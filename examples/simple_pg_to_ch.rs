//! Simple example of PostgreSQL to ClickHouse CDC
//!
//! This example demonstrates basic usage of the Zenith CDC library.
//!
//! Run with:
//! ```bash
//! cargo run --example simple_pg_to_ch
//! ```

use std::sync::Arc;
use tokio::sync::mpsc;
use zenith_core::{
    config::{ClickHouseConfig, PostgresConfig, StorageConfig},
    metrics::METRICS,
    pipeline::{CommitQueue, TransactionBuffer, WalPosition},
    schema::SchemaRegistry,
    sinks::clickhouse::ClickHouseSink,
    sources::postgres::PostgresSource,
    utils::ShutdownSignal,
};
use zenith_storage::WalPositionStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("Zenith CDC Example - PostgreSQL to ClickHouse");
    println!("==============================================");

    // Configuration
    let postgres_config = PostgresConfig {
        url: std::env::var("POSTGRES_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/testdb".to_string()),
        publication: std::env::var("PUBLICATION_NAME")
            .unwrap_or_else(|_| "zenith_pub".to_string()),
        slot_name: std::env::var("SLOT_NAME")
            .unwrap_or_else(|_| "zenith_example".to_string()),
        parallel_slots: 1,
        create_slot: true,
    };

    let clickhouse_config = ClickHouseConfig {
        url: std::env::var("CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string()),
        database: "default".to_string(),
        table: "zenith_cdc_example".to_string(),
        user: std::env::var("CLICKHOUSE_USER").ok(),
        password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
        batch_size: 1000,
        batch_timeout_ms: 100,
        compression: true,
    };

    let storage_config = StorageConfig {
        path: std::path::PathBuf::from("./zenith_example_data"),
        flush_interval_ms: 1000,
        flush_rows: 10000,
    };

    // Initialize components
    let storage = WalPositionStore::open(&storage_config.path)?;
    println!("Storage initialized, starting LSN: {:X}", storage.confirmed_lsn());

    let schema_registry = Arc::new(SchemaRegistry::new());
    let wal_position = Arc::new(WalPosition::new(storage.clone()));
    let transaction_buffer = Arc::new(TransactionBuffer::new(schema_registry.clone()));
    let commit_queue = Arc::new(CommitQueue::new(1000));

    let shutdown = ShutdownSignal::new();
    shutdown.install_ctrl_c_handler();

    // Initialize ClickHouse sink
    let sink = Arc::new(ClickHouseSink::new(clickhouse_config).await?);
    sink.init_table().await?;
    println!("ClickHouse sink initialized");

    // Create channel
    let (tx, mut rx) = mpsc::channel(10000);

    // Start source
    let source = Arc::new(PostgresSource::new(
        postgres_config,
        schema_registry.clone(),
        shutdown.clone(),
    ));

    let start_lsn = wal_position.start_lsn();
    let source_shutdown = shutdown.clone();
    let source_handle = tokio::spawn(async move {
        if let Err(e) = source.start(start_lsn, tx).await {
            if !source_shutdown.is_triggered() {
                eprintln!("Source error: {}", e);
            }
        }
    });

    println!("Streaming changes from PostgreSQL...");
    println!("Press Ctrl+C to stop");

    // Main loop
    let mut total_events = 0u64;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(source_msg) => {
                        wal_position.update_received(source_msg.lsn);

                        if let Some(txn) = transaction_buffer.process_message(
                            source_msg.message,
                            source_msg.lsn,
                        ) {
                            let event_count = txn.event_count();
                            let commit_lsn = txn.commit_lsn;

                            let mut txn = txn;
                            let events = txn.take_events();

                            match sink.flush_events(events).await {
                                Ok(flushed) => {
                                    total_events += flushed;
                                    if let Some(lsn) = commit_lsn {
                                        wal_position.update_confirmed(lsn, flushed, false)?;
                                    }
                                    println!("Flushed {} events (total: {})", event_count, total_events);
                                }
                                Err(e) => {
                                    eprintln!("Flush error: {}", e);
                                }
                            }
                        }
                    }
                    None => break,
                }
            }
            _ = shutdown.wait() => {
                println!("\nShutting down...");
                break;
            }
        }
    }

    // Cleanup
    wal_position.flush()?;
    let _ = source_handle.await;

    println!("Example complete. Total events: {}", total_events);
    Ok(())
}

