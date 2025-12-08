//! Zenith CDC CLI - High-performance PostgreSQL to ClickHouse CDC
//!
//! This binary provides the main entry point for the Zenith CDC engine.

use anyhow::{Context, Result};
use clap::Parser;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use zenith_core::{
    config::Config,
    metrics::METRICS,
    pipeline::{CommitQueue, TransactionBuffer, WalPosition},
    schema::SchemaRegistry,
    sinks::clickhouse::ClickHouseSink,
    sources::postgres::PostgresSource,
    utils::ShutdownSignal,
};
use zenith_storage::WalPositionStore;

/// Zenith CDC - High-performance Change Data Capture
#[derive(Parser, Debug)]
#[command(name = "zenith")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// PostgreSQL connection URL
    #[arg(long, env = "POSTGRES_URL")]
    postgres_url: Option<String>,

    /// ClickHouse URL
    #[arg(long, env = "CLICKHOUSE_URL")]
    clickhouse_url: Option<String>,

    /// Publication name
    #[arg(long, env = "PUBLICATION_NAME", default_value = "zenith_pub")]
    publication: String,

    /// Replication slot name
    #[arg(long, env = "SLOT_NAME", default_value = "zenith_slot")]
    slot_name: String,

    /// Metrics port
    #[arg(long, env = "METRICS_PORT", default_value = "9090")]
    metrics_port: u16,

    /// Storage path
    #[arg(long, env = "STORAGE_PATH", default_value = "./zenith_data")]
    storage_path: PathBuf,

    /// Log level
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Initialize ClickHouse table and exit
    #[arg(long)]
    init_table: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present
    let _ = dotenvy::dotenv();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialize logging
    init_logging(&args.log_level)?;

    info!("╔═══════════════════════════════════════════════════════════════╗");
    info!("║                     Zenith CDC Engine                         ║");
    info!("║         High-performance PostgreSQL → ClickHouse CDC          ║");
    info!("╚═══════════════════════════════════════════════════════════════╝");

    // Load configuration
    let config = load_config(&args)?;

    info!("Configuration loaded:");
    info!("  PostgreSQL: {}", config.postgres.url.split('@').last().unwrap_or("***"));
    info!("  ClickHouse: {}", config.clickhouse.url);
    info!("  Publication: {}", config.postgres.publication);
    info!("  Slot: {}", config.postgres.slot_name);
    info!("  Storage: {:?}", config.storage.path);

    // Initialize storage
    let storage = WalPositionStore::open(&config.storage.path)
        .context("Failed to open WAL position store")?;

    info!("Storage initialized, confirmed_lsn: {}", storage.confirmed_lsn());

    // Initialize ClickHouse sink
    let sink = Arc::new(
        ClickHouseSink::new(config.clickhouse.clone())
            .await
            .context("Failed to create ClickHouse sink")?,
    );

    // Initialize table if requested
    if args.init_table {
        info!("Initializing ClickHouse table...");
        sink.init_table().await?;
        info!("Table initialized successfully");
        return Ok(());
    }

    // Initialize table (create if not exists)
    sink.init_table().await.context("Failed to initialize ClickHouse table")?;

    // Setup shutdown signal
    let shutdown = ShutdownSignal::new();
    shutdown.install_signal_handlers();

    // Start metrics server
    if config.metrics.enabled {
        let metrics_shutdown = shutdown.clone();
        let metrics_addr = format!("{}:{}", config.metrics.host, config.metrics.port);
        let metrics_addr_log = metrics_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(&metrics_addr, metrics_shutdown).await {
                error!("Metrics server error: {}", e);
            }
        });
        info!("Metrics server started at http://{}/metrics", metrics_addr_log);
    }

    // Run the CDC pipeline
    run_pipeline(config, storage, sink, shutdown).await?;

    info!("Zenith CDC shutdown complete");
    Ok(())
}

/// Initialize logging with tracing
fn init_logging(level: &str) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    Ok(())
}

/// Load configuration from file or environment
fn load_config(args: &Args) -> Result<Config> {
    if let Some(ref path) = args.config {
        Config::from_file(path.to_str().unwrap())
            .context("Failed to load config file")
    } else {
        let mut config = Config::from_env()
            .context("Failed to load config from environment")?;

        // Override with CLI args
        if let Some(ref url) = args.postgres_url {
            config.postgres.url = url.clone();
        }
        if let Some(ref url) = args.clickhouse_url {
            config.clickhouse.url = url.clone();
        }
        config.postgres.publication = args.publication.clone();
        config.postgres.slot_name = args.slot_name.clone();
        config.metrics.port = args.metrics_port;
        config.storage.path = args.storage_path.clone();

        Ok(config)
    }
}

/// Run the main CDC pipeline
async fn run_pipeline(
    config: Config,
    storage: Arc<WalPositionStore>,
    sink: Arc<ClickHouseSink>,
    shutdown: ShutdownSignal,
) -> Result<()> {
    // Initialize components
    let schema_registry = Arc::new(SchemaRegistry::new());
    let wal_position = Arc::new(WalPosition::new(storage.clone()));
    let transaction_buffer = Arc::new(TransactionBuffer::new(schema_registry.clone()));
    let commit_queue = Arc::new(CommitQueue::new(config.pipeline.max_buffered_transactions));

    // Create channel for source -> pipeline communication
    let (source_tx, mut source_rx) = mpsc::channel(config.pipeline.channel_size);

    // Start PostgreSQL source
    let source = Arc::new(PostgresSource::new(
        config.postgres.clone(),
        schema_registry.clone(),
        shutdown.clone(),
    ));

    let start_lsn = wal_position.start_lsn();
    info!("Starting replication from LSN: {:X}", start_lsn);

    let source_handle = {
        let source = source.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = source.start(start_lsn, source_tx).await {
                if !shutdown.is_triggered() {
                    error!("Source error: {}", e);
                }
            }
        })
    };

    // Statistics tracking
    let total_events = Arc::new(AtomicU64::new(0));
    let last_stats_time = Arc::new(parking_lot::Mutex::new(Instant::now()));

    // Main processing loop
    let flush_interval = Duration::from_millis(config.clickhouse.batch_timeout_ms);
    let mut flush_ticker = tokio::time::interval(flush_interval);
    let mut stats_ticker = tokio::time::interval(Duration::from_secs(10));

    info!("CDC pipeline started, processing events...");

    loop {
        tokio::select! {
            // Process messages from source
            msg = source_rx.recv() => {
                match msg {
                    Some(source_msg) => {
                        wal_position.update_received(source_msg.lsn);

                        // Process through transaction buffer
                        if let Some(committed_txn) = transaction_buffer.process_message(
                            source_msg.message,
                            source_msg.lsn,
                        ) {
                            // Add to commit queue
                            if !commit_queue.push(committed_txn) {
                                warn!("Commit queue full, applying backpressure");
                                // In production, we'd wait or slow down
                            }
                        }

                        // Update buffer metrics
                        METRICS.set_buffer_size(transaction_buffer.len());
                    }
                    None => {
                        info!("Source channel closed");
                        break;
                    }
                }
            }

            // Periodic flush
            _ = flush_ticker.tick() => {
                // Pop ready transactions and flush
                let ready_txns = commit_queue.pop_ready(u64::MAX);

                if !ready_txns.is_empty() {
                    let mut all_events = Vec::new();
                    let mut max_lsn = 0u64;

                    for txn in ready_txns {
                        if let Some(lsn) = txn.commit_lsn {
                            max_lsn = max_lsn.max(lsn);
                        }
                        let mut txn = txn;
                        all_events.extend(txn.take_events());
                        METRICS.record_commit(&config.postgres.slot_name);
                    }

                    if !all_events.is_empty() {
                        let _event_count = all_events.len() as u64;

                        match sink.flush_events(all_events).await {
                            Ok(flushed) => {
                                total_events.fetch_add(flushed, Ordering::Relaxed);
                                wal_position.update_confirmed(max_lsn, flushed, false)?;
                                commit_queue.update_confirmed_lsn(max_lsn);
                            }
                            Err(e) => {
                                error!("Failed to flush events: {}", e);
                                METRICS.record_error("flush");
                            }
                        }
                    }
                }

                // Also flush any pending batch
                if sink.should_flush_by_timeout() {
                    if let Err(e) = sink.flush().await {
                        error!("Failed to flush pending batch: {}", e);
                    }
                }

                // Update throughput metric
                let total = total_events.load(Ordering::Relaxed);
                METRICS.update_throughput(total);
            }

            // Periodic stats logging
            _ = stats_ticker.tick() => {
                let (started, committed, _buffered) = transaction_buffer.stats();
                let queue_stats = commit_queue.stats();
                let pos = wal_position.snapshot();
                let total = total_events.load(Ordering::Relaxed);

                let mut last_time = last_stats_time.lock();
                let elapsed = last_time.elapsed();
                *last_time = Instant::now();

                let _rps = if elapsed.as_secs() > 0 {
                    total / elapsed.as_secs()
                } else {
                    0
                };

                info!(
                    "Stats: events={}, txns_started={}, txns_committed={}, \
                     queue_size={}, lag={}, confirmed_lsn={:X}",
                    total, started, committed,
                    queue_stats.size, pos.lag(), pos.confirmed_lsn
                );
            }

            // Shutdown signal
            _ = shutdown.wait() => {
                info!("Shutdown signal received, flushing remaining data...");
                break;
            }
        }
    }

    // Graceful shutdown: flush all remaining transactions
    info!("Flushing remaining transactions...");

    let remaining = commit_queue.drain_all();
    if !remaining.is_empty() {
        info!("Flushing {} remaining transactions", remaining.len());

        let mut all_events = Vec::new();
        let mut max_lsn = 0u64;

        for txn in remaining {
            if let Some(lsn) = txn.commit_lsn {
                max_lsn = max_lsn.max(lsn);
            }
            let mut txn = txn;
            all_events.extend(txn.take_events());
        }

        if !all_events.is_empty() {
            match sink.flush_events(all_events).await {
                Ok(flushed) => {
                    info!("Flushed {} remaining events", flushed);
                    wal_position.update_confirmed(max_lsn, flushed, true)?;
                }
                Err(e) => {
                    error!("Failed to flush remaining events: {}", e);
                }
            }
        }
    }

    // Final position flush
    wal_position.flush()?;

    // Wait for source to finish
    let _ = source_handle.await;

    let total = total_events.load(Ordering::Relaxed);
    info!("Pipeline shutdown complete. Total events processed: {}", total);

    Ok(())
}

/// Run the Prometheus metrics HTTP server
async fn run_metrics_server(addr: &str, shutdown: ShutdownSignal) -> Result<()> {
    let addr: SocketAddr = addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("Metrics server listening on {}", addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let io = TokioIo::new(stream);

                tokio::spawn(async move {
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(io, service_fn(handle_metrics_request))
                        .await
                    {
                        debug!("Metrics connection error: {}", e);
                    }
                });
            }
            _ = shutdown.wait() => {
                info!("Metrics server shutting down");
                break;
            }
        }
    }

    Ok(())
}

/// Handle metrics HTTP requests
async fn handle_metrics_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let response = match req.uri().path() {
        "/metrics" => {
            let metrics = METRICS.gather();
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from(metrics)))
                .unwrap()
        }
        "/health" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from_static(b"OK")))
            .unwrap(),
        "/ready" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from_static(b"Ready")))
            .unwrap(),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"Not Found")))
            .unwrap(),
    };

    Ok(response)
}

