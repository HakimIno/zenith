//! PostgreSQL streaming replication using raw protocol
//!
//! This module implements real streaming replication using PostgreSQL's
//! replication protocol directly. This is the recommended approach for
//! production CDC systems.
//!
//! # Protocol Overview
//!
//! 1. Connect in replication mode (?replication=database)
//! 2. Create/ensure replication slot exists
//! 3. Send START_REPLICATION command
//! 4. Switch to CopyBoth mode for bidirectional streaming
//! 5. Receive XLogData and keepalive messages from server
//! 6. Send StandbyStatusUpdate messages periodically
//!
//! # Benefits over polling
//!
//! - Real-time delivery of changes (sub-millisecond latency)
//! - Lower database load (no repeated queries)
//! - Proper WAL position feedback to prevent WAL bloat
//! - Native PostgreSQL protocol compliance

use super::decoder::{
    current_pg_timestamp, format_lsn, parse_lsn, ReplicationDecoder, ReplicationMessage,
};
use super::pgoutput_parser::PgOutputMessage;
use crate::config::PostgresConfig;
use crate::error::{Error, Result};
use crate::metrics::METRICS;
use crate::schema::SchemaRegistry;
use crate::utils::ShutdownSignal;
use zenith_storage::WalPositionStore;

use bytes::{BufMut, BytesMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, trace, warn};

/// Configuration for standby status updates
#[derive(Debug, Clone)]
pub struct StatusUpdateConfig {
    /// Interval between automatic status updates
    pub interval: Duration,
    /// Send status update immediately after receiving reply_requested keepalive
    pub reply_to_keepalive: bool,
}

impl Default for StatusUpdateConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            reply_to_keepalive: true,
        }
    }
}

/// Message sent from streaming source to pipeline
#[derive(Debug)]
pub struct StreamingSourceMessage {
    /// The parsed pgoutput message
    pub message: PgOutputMessage,
    /// Start LSN of this message
    pub start_lsn: u64,
    /// End LSN of this message
    pub end_lsn: u64,
    /// Receive timestamp
    pub received_at: Instant,
}

/// LSN tracking for standby status updates
#[derive(Debug)]
pub struct LsnTracker {
    /// Last WAL position written to disk (received)
    write_lsn: AtomicU64,
    /// Last WAL position flushed to persistent storage
    flush_lsn: AtomicU64,
    /// Last WAL position applied (processed by application)
    apply_lsn: AtomicU64,
}

impl LsnTracker {
    pub fn new(initial_lsn: u64) -> Self {
        Self {
            write_lsn: AtomicU64::new(initial_lsn),
            flush_lsn: AtomicU64::new(initial_lsn),
            apply_lsn: AtomicU64::new(initial_lsn),
        }
    }

    pub fn update_write(&self, lsn: u64) {
        self.write_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    pub fn update_flush(&self, lsn: u64) {
        self.flush_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    pub fn update_apply(&self, lsn: u64) {
        self.apply_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    pub fn get_positions(&self) -> (u64, u64, u64) {
        (
            self.write_lsn.load(Ordering::SeqCst),
            self.flush_lsn.load(Ordering::SeqCst),
            self.apply_lsn.load(Ordering::SeqCst),
        )
    }

    pub fn current_lsn(&self) -> u64 {
        self.write_lsn.load(Ordering::Acquire)
    }
}

/// Connection parameters parsed from URL
#[derive(Debug, Clone)]
struct ConnectionParams {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
}

impl ConnectionParams {
    fn from_url(url: &str) -> Result<Self> {
        // Parse postgres://user:pass@host:port/database
        let url = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");
        
        let (userinfo, rest) = if let Some(at_pos) = url.find('@') {
            (&url[..at_pos], &url[at_pos + 1..])
        } else {
            ("", url)
        };

        let (user, password) = if let Some(colon_pos) = userinfo.find(':') {
            (
                userinfo[..colon_pos].to_string(),
                Some(userinfo[colon_pos + 1..].to_string()),
            )
        } else {
            (userinfo.to_string(), None)
        };

        // Remove query parameters
        let rest = rest.split('?').next().unwrap_or(rest);

        let (hostport, database) = if let Some(slash_pos) = rest.find('/') {
            (&rest[..slash_pos], rest[slash_pos + 1..].to_string())
        } else {
            (rest, "postgres".to_string())
        };

        let (host, port) = if let Some(colon_pos) = hostport.find(':') {
            (
                hostport[..colon_pos].to_string(),
                hostport[colon_pos + 1..].parse().unwrap_or(5432),
            )
        } else {
            (hostport.to_string(), 5432)
        };

        Ok(Self {
            host,
            port,
            user: if user.is_empty() { "postgres".to_string() } else { user },
            password,
            database,
        })
    }
}

/// PostgreSQL streaming replication source
///
/// Uses raw protocol for real-time WAL streaming.
pub struct StreamingReplicationSource {
    config: PostgresConfig,
    #[allow(dead_code)]
    schema_registry: Arc<SchemaRegistry>,
    shutdown: ShutdownSignal,
    status_config: StatusUpdateConfig,
    lsn_tracker: Arc<LsnTracker>,
    snapshot_store: Option<Arc<WalPositionStore>>,
}

impl StreamingReplicationSource {
    /// Create a new streaming replication source
    pub fn new(
        config: PostgresConfig,
        schema_registry: Arc<SchemaRegistry>,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            config: config.clone(),
            schema_registry,
            shutdown,
            status_config: StatusUpdateConfig {
                interval: config.status_interval(),
                reply_to_keepalive: true,
            },
            lsn_tracker: Arc::new(LsnTracker::new(0)),
            snapshot_store: None,
        }
    }

    /// Create with custom status update configuration
    pub fn with_status_config(mut self, config: StatusUpdateConfig) -> Self {
        self.status_config = config;
        self
    }

    /// Set a snapshot store for filtering (deduplication)
    pub fn with_snapshot_store(mut self, store: Arc<WalPositionStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Get current write LSN position
    pub fn current_lsn(&self) -> u64 {
        self.lsn_tracker.current_lsn()
    }

    /// Get the LSN tracker for external updates
    pub fn lsn_tracker(&self) -> Arc<LsnTracker> {
        Arc::clone(&self.lsn_tracker)
    }

    /// Update the apply LSN (called by downstream consumers)
    pub fn update_apply_lsn(&self, lsn: u64) {
        self.lsn_tracker.update_apply(lsn);
    }

    /// Start streaming changes from the replication slot
    pub async fn start(
        self: Arc<Self>,
        start_lsn: u64,
        tx: mpsc::Sender<StreamingSourceMessage>,
    ) -> Result<()> {
        info!(
            "Starting streaming replication source, connecting to {}",
            self.config.url.split('@').last().unwrap_or("***")
        );

        let params = ConnectionParams::from_url(&self.config.url)?;

        // Connect to PostgreSQL
        let addr = format!("{}:{}", params.host, params.port);
        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            Error::connection(format!("Failed to connect to {}: {}", addr, e))
        })?;

        stream.set_nodelay(true).ok();

        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Perform PostgreSQL startup
        self.perform_startup(&mut reader, &mut writer, &params).await?;

        info!("Connected to PostgreSQL in replication mode");
        METRICS.set_active_connections("postgres_streaming", 1);

        // Create slot if needed
        if self.config.create_slot {
            self.ensure_slot_exists(&mut reader, &mut writer).await?;
        }

        // Get start position
        let actual_start_lsn = if start_lsn > 0 {
            start_lsn
        } else {
            self.get_slot_lsn(&mut reader, &mut writer).await?
        };

        info!(
            "Starting streaming replication from LSN {}",
            format_lsn(actual_start_lsn)
        );

        // Initialize LSN tracker
        self.lsn_tracker
            .write_lsn
            .store(actual_start_lsn, Ordering::Release);
        self.lsn_tracker
            .flush_lsn
            .store(actual_start_lsn, Ordering::Release);
        self.lsn_tracker
            .apply_lsn
            .store(actual_start_lsn, Ordering::Release);

        // Start the streaming replication
        self.run_streaming_replication(&mut reader, &mut writer, actual_start_lsn, tx)
            .await
    }

    /// Perform PostgreSQL startup handshake
    async fn perform_startup<R, W>(
        &self,
        reader: &mut R,
        writer: &mut W,
        params: &ConnectionParams,
    ) -> Result<()>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let mut buf = BytesMut::new();

        // Build startup message with replication=database
        let startup_params: Vec<(&str, &str)> = vec![
            ("user", &params.user),
            ("database", &params.database),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
        ];

        // Calculate message length
        let mut msg_len = 4 + 4; // length + protocol version
        for (key, value) in &startup_params {
            msg_len += key.len() + 1 + value.len() + 1;
        }
        msg_len += 1; // null terminator

        buf.put_i32(msg_len as i32);
        buf.put_i32(196608); // Protocol version 3.0

        for (key, value) in &startup_params {
            buf.put_slice(key.as_bytes());
            buf.put_u8(0);
            buf.put_slice(value.as_bytes());
            buf.put_u8(0);
        }
        buf.put_u8(0); // terminator

        writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;

        // Read response
        loop {
            let msg_type = reader.read_u8().await.map_err(|e| Error::connection(e.to_string()))?;
            let msg_len = reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

            let mut msg_buf = vec![0u8; msg_len];
            reader.read_exact(&mut msg_buf).await.map_err(|e| Error::connection(e.to_string()))?;

            match msg_type {
                b'R' => {
                    // Authentication request
                    let auth_type = if msg_buf.len() >= 4 {
                        i32::from_be_bytes([msg_buf[0], msg_buf[1], msg_buf[2], msg_buf[3]])
                    } else {
                        0
                    };

                    match auth_type {
                        0 => {
                            // AuthenticationOk
                            debug!("Authentication successful");
                        }
                        3 => {
                            // CleartextPassword
                            if let Some(ref password) = params.password {
                                self.send_password(writer, password).await?;
                            } else {
                                return Err(Error::auth("Password required but not provided"));
                            }
                        }
                        5 => {
                            // MD5Password
                            if msg_buf.len() >= 8 {
                                let salt = &msg_buf[4..8];
                                if let Some(ref password) = params.password {
                                    self.send_md5_password(writer, &params.user, password, salt)
                                        .await?;
                                } else {
                                    return Err(Error::auth("Password required but not provided"));
                                }
                            }
                        }
                        10 => {
                            // SASL
                            return Err(Error::auth(
                                "SASL authentication not yet supported. Use md5 or trust.",
                            ));
                        }
                        _ => {
                            return Err(Error::auth(format!(
                                "Unsupported authentication type: {}",
                                auth_type
                            )));
                        }
                    }
                }
                b'K' => {
                    // BackendKeyData - we can ignore this
                    debug!("Received BackendKeyData");
                }
                b'S' => {
                    // ParameterStatus - we can ignore this
                    trace!("Received ParameterStatus");
                }
                b'Z' => {
                    // ReadyForQuery
                    debug!("Server ready for queries");
                    return Ok(());
                }
                b'E' => {
                    // ErrorResponse
                    let error_msg = self.parse_error_response(&msg_buf);
                    return Err(Error::connection(format!("Server error: {}", error_msg)));
                }
                _ => {
                    trace!("Unknown message type during startup: {}", msg_type as char);
                }
            }
        }
    }

    /// Send cleartext password
    async fn send_password<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
        password: &str,
    ) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32((4 + password.len() + 1) as i32);
        buf.put_slice(password.as_bytes());
        buf.put_u8(0);

        writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;
        Ok(())
    }

    /// Send MD5 password
    async fn send_md5_password<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
        user: &str,
        password: &str,
        salt: &[u8],
    ) -> Result<()> {
        // MD5(MD5(password + user) + salt)
        let inner = format!("{}{}", password, user);
        let inner_hash = md5::compute(inner.as_bytes());
        let inner_hex = format!("{:x}", inner_hash);

        let mut outer = inner_hex.as_bytes().to_vec();
        outer.extend_from_slice(salt);
        let outer_hash = md5::compute(&outer);
        let password_hash = format!("md5{:x}", outer_hash);

        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32((4 + password_hash.len() + 1) as i32);
        buf.put_slice(password_hash.as_bytes());
        buf.put_u8(0);

        writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;
        Ok(())
    }

    /// Parse error response message
    fn parse_error_response(&self, data: &[u8]) -> String {
        let mut message = String::new();
        let mut i = 0;
        while i < data.len() {
            let field_type = data[i];
            i += 1;
            if field_type == 0 {
                break;
            }
            let end = data[i..].iter().position(|&b| b == 0).unwrap_or(data.len() - i);
            let value = String::from_utf8_lossy(&data[i..i + end]);
            i += end + 1;

            match field_type {
                b'M' => message = value.to_string(),
                _ => {}
            }
        }
        message
    }

    /// Execute a simple query
    async fn simple_query<R, W>(
        &self,
        reader: &mut R,
        writer: &mut W,
        query: &str,
    ) -> Result<Vec<Vec<Option<String>>>>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((4 + query.len() + 1) as i32);
        buf.put_slice(query.as_bytes());
        buf.put_u8(0);

        writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;

        let mut rows = Vec::new();

        loop {
            let msg_type = reader.read_u8().await.map_err(|e| Error::connection(e.to_string()))?;
            let msg_len = reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

            let mut msg_buf = vec![0u8; msg_len];
            reader.read_exact(&mut msg_buf).await.map_err(|e| Error::connection(e.to_string()))?;

            match msg_type {
                b'T' => {
                    // RowDescription - we ignore column metadata for now
                }
                b'D' => {
                    // DataRow
                    let row = self.parse_data_row(&msg_buf)?;
                    rows.push(row);
                }
                b'C' => {
                    // CommandComplete
                }
                b'Z' => {
                    // ReadyForQuery
                    break;
                }
                b'E' => {
                    let error_msg = self.parse_error_response(&msg_buf);
                    return Err(Error::parse(format!("Query error: {}", error_msg)));
                }
                b'N' => {
                    // NoticeResponse - ignore
                }
                _ => {}
            }
        }

        Ok(rows)
    }

    /// Parse a DataRow message
    fn parse_data_row(&self, data: &[u8]) -> Result<Vec<Option<String>>> {
        if data.len() < 2 {
            return Ok(Vec::new());
        }

        let num_cols = i16::from_be_bytes([data[0], data[1]]) as usize;
        let mut cols = Vec::with_capacity(num_cols);
        let mut pos = 2;

        for _ in 0..num_cols {
            if pos + 4 > data.len() {
                break;
            }
            let col_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            if col_len == -1 {
                cols.push(None);
            } else {
                let col_len = col_len as usize;
                if pos + col_len > data.len() {
                    break;
                }
                let value = String::from_utf8_lossy(&data[pos..pos + col_len]).to_string();
                cols.push(Some(value));
                pos += col_len;
            }
        }

        Ok(cols)
    }

    /// Ensure the replication slot exists
    async fn ensure_slot_exists<R, W>(&self, reader: &mut R, writer: &mut W) -> Result<()>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let slot_name = &self.config.slot_name;

        // Check if slot exists
        let check_query = format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{}'",
            slot_name
        );

        let rows = self.simple_query(reader, writer, &check_query).await?;
        let exists = !rows.is_empty();

        if !exists {
            info!("Creating replication slot '{}'", slot_name);

            let create_query = format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
                slot_name
            );

            self.simple_query(reader, writer, &create_query).await?;
            info!("Created replication slot '{}'", slot_name);
        } else {
            debug!("Replication slot '{}' already exists", slot_name);
        }

        Ok(())
    }

    /// Get the confirmed flush LSN from the slot
    async fn get_slot_lsn<R, W>(&self, reader: &mut R, writer: &mut W) -> Result<u64>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let query = format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            self.config.slot_name
        );

        let rows = self.simple_query(reader, writer, &query).await?;

        for row in rows {
            if let Some(Some(lsn_str)) = row.first() {
                if !lsn_str.is_empty() {
                    return parse_lsn(lsn_str);
                }
            }
        }

        Ok(0)
    }

    /// Run the streaming replication
    async fn run_streaming_replication<R, W>(
        &self,
        reader: &mut R,
        writer: &mut W,
        start_lsn: u64,
        tx: mpsc::Sender<StreamingSourceMessage>,
    ) -> Result<()>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let slot_name = &self.config.slot_name;
        let publication = &self.config.publication;

        // Send START_REPLICATION command
        let start_cmd = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            slot_name,
            format_lsn(start_lsn),
            publication
        );

        info!("Starting replication: {}", start_cmd);

        // Send query
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((4 + start_cmd.len() + 1) as i32);
        buf.put_slice(start_cmd.as_bytes());
        buf.put_u8(0);

        writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;

        // Wait for CopyBothResponse
        let msg_type = reader.read_u8().await.map_err(|e| Error::connection(e.to_string()))?;
        let msg_len = reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

        let mut msg_buf = vec![0u8; msg_len];
        if msg_len > 0 {
            reader.read_exact(&mut msg_buf).await.map_err(|e| Error::connection(e.to_string()))?;
        }

        match msg_type {
            b'W' => {
                // CopyBothResponse - we're now in copy mode
                info!("Streaming replication started");
            }
            b'E' => {
                let error_msg = self.parse_error_response(&msg_buf);
                return Err(Error::connection(format!(
                    "Failed to start replication: {}",
                    error_msg
                )));
            }
            _ => {
                return Err(Error::connection(format!(
                    "Unexpected response type: {}",
                    msg_type as char
                )));
            }
        }

        // Now process CopyData messages
        self.process_copy_stream(reader, writer, tx).await
    }

    /// Process the CopyBoth stream
    async fn process_copy_stream<R, W>(
        &self,
        reader: &mut R,
        writer: &mut W,
        tx: mpsc::Sender<StreamingSourceMessage>,
    ) -> Result<()>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let mut decoder = ReplicationDecoder::new();
        let mut status_interval = interval(self.status_config.interval);

        loop {
            tokio::select! {
                biased;

                // Check for shutdown
                _ = self.shutdown.wait() => {
                    info!("Shutdown triggered, stopping streaming replication");
                    break;
                }

                // Periodic status update
                _ = status_interval.tick() => {
                    self.send_status_update(writer, false).await?;
                }

                // Receive messages
                result = self.read_copy_data(reader) => {
                    match result? {
                        Some(data) => {
                            match decoder.decode_replication_message(&data) {
                                Ok(ReplicationMessage::XLogData(xlog)) => {
                                    trace!(
                                        "XLogData: start={}, end={}, len={}",
                                        format_lsn(xlog.start_lsn),
                                        format_lsn(xlog.end_lsn),
                                        xlog.data.len()
                                    );

                                    self.lsn_tracker.update_write(xlog.end_lsn);

                                    match decoder.parse_pgoutput(&xlog.data) {
                                        Ok(msg) => {
                                            // Deduplication: Skip events already in snapshot (per table)
                                            if let Some(ref store) = self.snapshot_store {
                                                let should_skip = match &msg {
                                                    PgOutputMessage::Insert { relation_id, .. } |
                                                    PgOutputMessage::Update { relation_id, .. } |
                                                    PgOutputMessage::Delete { relation_id, .. } => {
                                                        if let Some(rel) = self.schema_registry.get(*relation_id) {
                                                            let table = rel.full_name();
                                                            if let Ok(Some(snap_lsn)) = store.get_table_snapshot_lsn(&table) {
                                                                xlog.end_lsn <= snap_lsn
                                                            } else {
                                                                false
                                                            }
                                                        } else {
                                                            false
                                                        }
                                                    },
                                                    _ => false
                                                };
                                                
                                                if should_skip {
                                                    self.lsn_tracker.update_flush(xlog.end_lsn);
                                                    self.lsn_tracker.update_apply(xlog.end_lsn);
                                                    continue;
                                                }
                                            }

                                            METRICS.record_event(&self.config.slot_name, msg.type_name());

                                            let source_msg = StreamingSourceMessage {
                                                message: msg,
                                                start_lsn: xlog.start_lsn,
                                                end_lsn: xlog.end_lsn,
                                                received_at: Instant::now(),
                                            };

                                            if tx.send(source_msg).await.is_err() {
                                                warn!("Channel closed");
                                                break;
                                            }

                                            self.lsn_tracker.update_flush(xlog.end_lsn);
                                        }
                                        Err(e) => {
                                            warn!("Parse error at {}: {}", format_lsn(xlog.start_lsn), e);
                                        }
                                    }
                                }
                                Ok(ReplicationMessage::PrimaryKeepalive(ka)) => {
                                    trace!("Keepalive: wal_end={}, reply={}", format_lsn(ka.wal_end), ka.reply_requested);
                                    self.lsn_tracker.update_write(ka.wal_end);

                                    if ka.reply_requested && self.status_config.reply_to_keepalive {
                                        self.send_status_update(writer, true).await?;
                                    }
                                }
                                Err(e) => {
                                    error!("Decode error: {}", e);
                                    METRICS.record_error("decode");
                                }
                            }
                        }
                        None => {
                            info!("Stream closed");
                            break;
                        }
                    }
                }
            }
        }

        // Send final status
        let _ = self.send_status_update(writer, false).await;

        METRICS.set_active_connections("postgres_streaming", 0);

        let (w, f, a) = self.lsn_tracker.get_positions();
        info!(
            "Stopped. LSN: write={}, flush={}, apply={}",
            format_lsn(w),
            format_lsn(f),
            format_lsn(a)
        );

        Ok(())
    }

    /// Read a CopyData message
    async fn read_copy_data<R: AsyncReadExt + Unpin>(&self, reader: &mut R) -> Result<Option<Vec<u8>>> {
        let msg_type = match reader.read_u8().await {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::connection(e.to_string())),
        };

        let msg_len = reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

        match msg_type {
            b'd' => {
                // CopyData
                let mut data = vec![0u8; msg_len];
                reader.read_exact(&mut data).await.map_err(|e| Error::connection(e.to_string()))?;
                Ok(Some(data))
            }
            b'c' => {
                // CopyDone
                Ok(None)
            }
            b'E' => {
                let mut data = vec![0u8; msg_len];
                reader.read_exact(&mut data).await.map_err(|e| Error::connection(e.to_string()))?;
                let error_msg = self.parse_error_response(&data);
                Err(Error::connection(format!("Server error: {}", error_msg)))
            }
            _ => {
                // Skip unknown messages
                let mut data = vec![0u8; msg_len];
                reader.read_exact(&mut data).await.map_err(|e| Error::connection(e.to_string()))?;
                Ok(Some(vec![]))
            }
        }
    }

    /// Send standby status update
    async fn send_status_update<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
        reply_requested: bool,
    ) -> Result<()> {
        let (write_lsn, flush_lsn, apply_lsn) = self.lsn_tracker.get_positions();
        let timestamp = current_pg_timestamp();

        let status_msg = ReplicationDecoder::create_standby_status_update(
            write_lsn,
            flush_lsn,
            apply_lsn,
            timestamp,
            reply_requested,
        );

        trace!(
            "Status update: write={}, flush={}, apply={}",
            format_lsn(write_lsn),
            format_lsn(flush_lsn),
            format_lsn(apply_lsn)
        );

        // Wrap in CopyData message
        let mut buf = BytesMut::new();
        buf.put_u8(b'd'); // CopyData
        buf.put_i32((4 + status_msg.len()) as i32);
        buf.put_slice(&status_msg);

        writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;

        METRICS.record_event(&self.config.slot_name, "status_update");

        Ok(())
    }
}

/// Builder for StreamingReplicationSource
pub struct StreamingReplicationSourceBuilder {
    config: PostgresConfig,
    schema_registry: Arc<SchemaRegistry>,
    shutdown: ShutdownSignal,
    status_config: StatusUpdateConfig,
}

impl StreamingReplicationSourceBuilder {
    pub fn new(
        config: PostgresConfig,
        schema_registry: Arc<SchemaRegistry>,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            config,
            schema_registry,
            shutdown,
            status_config: StatusUpdateConfig::default(),
        }
    }

    /// Set the status update interval
    pub fn status_interval(mut self, interval: Duration) -> Self {
        self.status_config.interval = interval;
        self
    }

    /// Set whether to reply to keepalive requests
    pub fn reply_to_keepalive(mut self, reply: bool) -> Self {
        self.status_config.reply_to_keepalive = reply;
        self
    }

    /// Build the streaming replication source
    pub fn build(self) -> StreamingReplicationSource {
        StreamingReplicationSource {
            config: self.config,
            schema_registry: self.schema_registry,
            shutdown: self.shutdown,
            status_config: self.status_config,
            lsn_tracker: Arc::new(LsnTracker::new(0)),
            snapshot_store: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_tracker() {
        let tracker = LsnTracker::new(100);
        assert_eq!(tracker.get_positions(), (100, 100, 100));

        tracker.update_write(200);
        tracker.update_flush(150);
        tracker.update_apply(120);

        assert_eq!(tracker.get_positions(), (200, 150, 120));

        // Test that it only increases (fetch_max)
        tracker.update_write(150);
        assert_eq!(tracker.write_lsn.load(Ordering::SeqCst), 200);
    }

    #[test]
    fn test_connection_params_parsing() {
        let params = ConnectionParams::from_url("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 5432);
        assert_eq!(params.user, "user");
        assert_eq!(params.password, Some("pass".to_string()));
        assert_eq!(params.database, "mydb");

        let params2 = ConnectionParams::from_url("postgres://localhost/testdb").unwrap();
        assert_eq!(params2.host, "localhost");
        assert_eq!(params2.port, 5432);
        assert_eq!(params2.database, "testdb");
    }

    #[test]
    fn test_status_update_config_default() {
        let config = StatusUpdateConfig::default();
        assert_eq!(config.interval, Duration::from_secs(10));
        assert!(config.reply_to_keepalive);
    }
}
