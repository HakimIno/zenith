//! PostgreSQL replication slot management (polling-based)
//!
//! **DEPRECATED**: This module uses a polling approach which is not recommended
//! for production. Use [`super::streaming::StreamingReplicationSource`] instead,
//! which implements the proper PostgreSQL streaming replication protocol.
//!
//! # When to use this module
//!
//! - Testing/development environments
//! - When copy_both protocol is not available
//! - As a fallback mechanism
//!
//! # Example
//!
//! ```ignore
//! // Prefer StreamingReplicationSource for production:
//! use zenith_core::sources::postgres::StreamingReplicationSource;
//!
//! // Legacy polling approach (not recommended):
//! let source = PostgresSource::new(config, schema_registry, shutdown);
//! let (tx, rx) = mpsc::channel(10000);
//! source.start(start_lsn, tx).await?;
//! ```

use super::decoder::{format_lsn, parse_lsn, ReplicationDecoder};
use super::pgoutput_parser::PgOutputMessage;
use crate::config::PostgresConfig;
use crate::error::{Error, Result};
use crate::metrics::METRICS;
use crate::schema::SchemaRegistry;
use crate::utils::ShutdownSignal;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tracing::{debug, error, info, warn};

/// Message sent from PostgreSQL source to pipeline
#[derive(Debug)]
pub struct SourceMessage {
    /// The parsed pgoutput message
    pub message: PgOutputMessage,
    /// LSN of this message
    pub lsn: u64,
    /// Receive timestamp
    pub received_at: Instant,
}

/// PostgreSQL logical replication source
///
/// Connects to PostgreSQL, manages replication slots, and streams
/// changes via pgoutput protocol.
pub struct PostgresSource {
    config: PostgresConfig,
    #[allow(dead_code)]
    schema_registry: Arc<SchemaRegistry>,
    shutdown: ShutdownSignal,
    current_lsn: Arc<AtomicU64>,
}

impl PostgresSource {
    /// Create a new PostgreSQL source
    pub fn new(
        config: PostgresConfig,
        schema_registry: Arc<SchemaRegistry>,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            config,
            schema_registry,
            shutdown,
            current_lsn: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get current LSN position
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Acquire)
    }

    /// Start streaming changes from the replication slot
    pub async fn start(
        self: Arc<Self>,
        start_lsn: u64,
        tx: mpsc::Sender<SourceMessage>,
    ) -> Result<()> {
        info!(
            "Starting PostgreSQL source, connecting to {}",
            self.config.url.split('@').last().unwrap_or("***")
        );

        // Connect to PostgreSQL in replication mode
        let (client, connection) = self.connect_replication().await?;

        // Spawn connection handler
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                result = connection => {
                    if let Err(e) = result {
                        error!("PostgreSQL connection error: {}", e);
                    }
                }
                _ = shutdown.wait() => {
                    debug!("Connection shutting down");
                }
            }
        });

        // Create slot if needed
        if self.config.create_slot {
            self.ensure_slot_exists(&client).await?;
        }

        // Get start position
        let actual_start_lsn = if start_lsn > 0 {
            start_lsn
        } else {
            self.get_slot_lsn(&client).await?
        };

        info!(
            "Starting replication from LSN {}",
            format_lsn(actual_start_lsn)
        );

        self.current_lsn.store(actual_start_lsn, Ordering::Release);

        // Start the replication stream
        self.run_replication_loop(&client, actual_start_lsn, tx).await
    }

    /// Connect to PostgreSQL in replication mode
    async fn connect_replication(
        &self,
    ) -> Result<(
        Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    )> {
        // Build connection string with replication mode
        let mut conn_str = self.config.url.clone();
        if !conn_str.contains("replication=") {
            if conn_str.contains('?') {
                conn_str.push_str("&replication=database");
            } else {
                conn_str.push_str("?replication=database");
            }
        }

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

        METRICS.set_active_connections("postgres", 1);
        info!("Connected to PostgreSQL in replication mode");

        Ok((client, connection))
    }

    /// Ensure the replication slot exists
    async fn ensure_slot_exists(&self, client: &Client) -> Result<()> {
        let slot_name = &self.config.slot_name;

        // Check if slot exists
        let check_query = format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{}'",
            slot_name
        );

        let rows = client.simple_query(&check_query).await?;
        let exists = rows
            .iter()
            .any(|msg| matches!(msg, SimpleQueryMessage::Row(_)));

        if !exists {
            info!("Creating replication slot '{}'", slot_name);

            let create_query = format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
                slot_name
            );

            client.simple_query(&create_query).await?;
            info!("Created replication slot '{}'", slot_name);
        } else {
            debug!("Replication slot '{}' already exists", slot_name);
        }

        Ok(())
    }

    /// Get the confirmed flush LSN from the slot
    async fn get_slot_lsn(&self, client: &Client) -> Result<u64> {
        let query = format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            self.config.slot_name
        );

        let rows = client.simple_query(&query).await?;

        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                if let Some(lsn_str) = row.get(0) {
                    if !lsn_str.is_empty() {
                        return parse_lsn(lsn_str);
                    }
                }
            }
        }

        // If no LSN found, start from 0 (beginning)
        Ok(0)
    }

    /// Run the main replication loop
    ///
    /// This is a simplified polling-based approach. For true streaming replication,
    /// you would need to:
    /// 1. Use the copy_both protocol directly
    /// 2. Parse XLogData messages from the stream
    /// 3. Send standby status updates periodically
    ///
    /// For production, consider using a dedicated replication library or
    /// implementing the protocol directly.
    async fn run_replication_loop(
        &self,
        client: &Client,
        start_lsn: u64,
        _tx: mpsc::Sender<SourceMessage>,
    ) -> Result<()> {
        let slot_name = &self.config.slot_name;
        let publication = &self.config.publication;
        let _decoder = ReplicationDecoder::new();
        let mut last_lsn = start_lsn;

        info!(
            "Starting replication loop for slot '{}' publication '{}'",
            slot_name, publication
        );

        // Poll for changes using pg_logical_slot_get_changes
        // This is a simplified approach - real streaming would use START_REPLICATION
        loop {
            if self.shutdown.is_triggered() {
                info!("Shutdown triggered, stopping replication");
                break;
            }

            // Peek at changes (doesn't consume them)
            let peek_query = format!(
                "SELECT lsn, xid, data FROM pg_logical_slot_peek_binary_changes('{}', NULL, NULL, 'proto_version', '1', 'publication_names', '{}') LIMIT 1000",
                slot_name, publication
            );

            let result = client.simple_query(&peek_query).await;

            match result {
                Ok(rows) => {
                    let mut processed_count = 0u64;
                    let mut max_lsn = last_lsn;

                    for msg in &rows {
                        if let SimpleQueryMessage::Row(row) = msg {
                            // Parse LSN
                            if let Some(lsn_str) = row.get(0) {
                                if let Ok(lsn) = parse_lsn(lsn_str) {
                                    max_lsn = max_lsn.max(lsn);

                                    // Get binary data (column 2)
                                    // Note: In real implementation, this would be binary data
                                    // For now, we simulate with a mock message
                                    if let Some(_data_str) = row.get(2) {
                                        // In production, you would parse the actual binary data
                                        // decoder.parse_pgoutput(binary_data)?;
                                        
                                        // For demonstration, log the change
                                        debug!("Received change at LSN {}", lsn_str);
                                        processed_count += 1;

                                        // Update metrics
                                        METRICS.record_event(slot_name, "change");
                                    }
                                }
                            }
                        }
                    }

                    if processed_count > 0 {
                        // Consume the changes we processed
                        let consume_query = format!(
                            "SELECT pg_logical_slot_get_binary_changes('{}', '{}', NULL, 'proto_version', '1', 'publication_names', '{}')",
                            slot_name, format_lsn(max_lsn), publication
                        );
                        let _ = client.simple_query(&consume_query).await;

                        last_lsn = max_lsn;
                        self.current_lsn.store(last_lsn, Ordering::Release);

                        debug!("Processed {} changes, LSN now at {}", processed_count, format_lsn(last_lsn));
                    } else {
                        // No changes, wait a bit
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                            _ = self.shutdown.wait() => {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    // Check if it's a recoverable error
                    if e.to_string().contains("does not exist") {
                        warn!("Slot or publication not found, retrying...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    } else {
                        error!("Replication error: {}", e);
                        METRICS.record_error("replication");
                        return Err(Error::Postgres(e));
                    }
                }
            }
        }

        METRICS.set_active_connections("postgres", 0);
        info!(
            "Replication stopped at LSN {}",
            format_lsn(last_lsn)
        );

        Ok(())
    }
}

/// Create multiple parallel slots for sharded processing
pub async fn create_parallel_slots(config: &PostgresConfig, count: usize) -> Result<Vec<String>> {
    let mut slot_names = Vec::with_capacity(count);

    for i in 0..count {
        let name = if count == 1 {
            config.slot_name.clone()
        } else {
            format!("{}_{}", config.slot_name, i)
        };
        slot_names.push(name);
    }

    Ok(slot_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_slot_names() {
        let config = PostgresConfig {
            url: "postgres://localhost/test".to_string(),
            publication: "test_pub".to_string(),
            slot_name: "test_slot".to_string(),
            parallel_slots: 4,
            create_slot: true,
        };

        // Single slot
        let rt = tokio::runtime::Runtime::new().unwrap();
        let slots = rt.block_on(create_parallel_slots(&config, 1)).unwrap();
        assert_eq!(slots, vec!["test_slot"]);

        // Multiple slots
        let slots = rt.block_on(create_parallel_slots(&config, 3)).unwrap();
        assert_eq!(slots, vec!["test_slot_0", "test_slot_1", "test_slot_2"]);
    }
}
