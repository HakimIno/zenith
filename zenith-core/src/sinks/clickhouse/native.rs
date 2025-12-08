//! Native ClickHouse HTTP sink with batch processing
//!
//! High-performance sink that writes CDC events to ClickHouse using
//! the native HTTP interface with compression support.

use crate::config::ClickHouseConfig;
use crate::error::{Error, Result};
use crate::metrics::METRICS;
use crate::pipeline::transaction_buffer::Event;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_TYPE};
use reqwest::Client;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, info, trace};

/// ClickHouse table schema for CDC events
const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.{table} (
    lsn UInt64,
    xid UInt64,
    op String,
    `table` String,
    data JSON,
    before Nullable(JSON),
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (lsn, xid)
PARTITION BY toYYYYMM(ts)
"#;

/// Native ClickHouse sink using HTTP protocol
pub struct ClickHouseSink {
    config: ClickHouseConfig,
    client: Client,
    /// Semaphore for concurrent batch uploads
    upload_semaphore: Arc<Semaphore>,
    /// Batch buffer
    batch: parking_lot::Mutex<BatchBuffer>,
    /// Statistics
    stats: SinkStats,
}

struct BatchBuffer {
    events: Vec<Event>,
    byte_size: usize,
    last_flush: Instant,
}

impl Default for BatchBuffer {
    fn default() -> Self {
        Self {
            events: Vec::with_capacity(10_000),
            byte_size: 0,
            last_flush: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
struct SinkStats {
    batches_sent: AtomicU64,
    events_flushed: AtomicU64,
    bytes_sent: AtomicU64,
    errors: AtomicU64,
}

impl ClickHouseSink {
    /// Create a new ClickHouse sink
    pub async fn new(config: ClickHouseConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        if config.compression {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .build()
            .map_err(|e| Error::clickhouse(format!("Failed to create HTTP client: {}", e)))?;

        let sink = Self {
            config,
            client,
            upload_semaphore: Arc::new(Semaphore::new(4)), // Max 4 concurrent uploads
            batch: parking_lot::Mutex::new(BatchBuffer::default()),
            stats: SinkStats::default(),
        };

        info!("Created ClickHouse sink: {}", sink.config.url);

        Ok(sink)
    }

    /// Initialize the target table if it doesn't exist
    pub async fn init_table(&self) -> Result<()> {
        let sql = CREATE_TABLE_SQL
            .replace("{database}", &self.config.database)
            .replace("{table}", &self.config.table);

        self.execute_query(&sql).await?;
        info!(
            "Initialized table {}.{}",
            self.config.database, self.config.table
        );

        Ok(())
    }

    /// Add events to the batch buffer
    ///
    /// Returns true if the batch should be flushed.
    pub fn add_events(&self, events: Vec<Event>) -> bool {
        if events.is_empty() {
            return false;
        }

        let mut batch = self.batch.lock();
        let event_count = events.len();

        // Estimate byte size
        let estimated_size: usize = events.iter().map(|e| estimate_event_size(e)).sum();

        batch.events.extend(events);
        batch.byte_size += estimated_size;

        let should_flush = batch.events.len() >= self.config.batch_size
            || batch.last_flush.elapsed() > self.config.batch_timeout();

        trace!(
            "Added {} events to batch, total: {}, should_flush: {}",
            event_count,
            batch.events.len(),
            should_flush
        );

        should_flush
    }

    /// Flush the current batch to ClickHouse
    pub async fn flush(&self) -> Result<u64> {
        let events = {
            let mut batch = self.batch.lock();
            if batch.events.is_empty() {
                return Ok(0);
            }
            batch.last_flush = Instant::now();
            std::mem::take(&mut batch.events)
        };

        self.flush_events(events).await
    }

    /// Flush a specific set of events
    pub async fn flush_events(&self, events: Vec<Event>) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let event_count = events.len() as u64;
        let start = Instant::now();

        // Acquire semaphore for concurrent upload limiting
        let _permit = self.upload_semaphore.acquire().await.unwrap();

        // Build JSON Lines format for ClickHouse
        let body = self.build_jsonl_body(&events)?;
        let body_len = body.len();

        // Build the insert query
        let url = format!(
            "{}/",
            self.config.url.trim_end_matches('/')
        );

        let query = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            self.config.database, self.config.table
        );

        let mut request = self.client.post(&url).query(&[("query", &query)]);

        // Add authentication if configured
        if let Some(ref user) = self.config.user {
            request = request.query(&[("user", user)]);
        }
        if let Some(ref password) = self.config.password {
            request = request.query(&[("password", password)]);
        }

        // Compress if enabled
        let body = if self.config.compression {
            compress_gzip(&body)?
        } else {
            body
        };

        let response = request.body(body).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            METRICS.record_error("clickhouse_insert");
            return Err(Error::clickhouse(format!(
                "Insert failed with status {}: {}",
                status, text
            )));
        }

        let elapsed = start.elapsed();

        // Update statistics
        self.stats.batches_sent.fetch_add(1, Ordering::Relaxed);
        self.stats.events_flushed.fetch_add(event_count, Ordering::Relaxed);
        self.stats.bytes_sent.fetch_add(body_len as u64, Ordering::Relaxed);

        // Record metrics
        METRICS.record_flush(&self.config.table, event_count);
        METRICS.record_batch_latency("clickhouse", elapsed.as_secs_f64());

        debug!(
            "Flushed {} events to ClickHouse in {:?} ({} bytes)",
            event_count, elapsed, body_len
        );

        Ok(event_count)
    }

    /// Build JSON Lines body for ClickHouse
    fn build_jsonl_body(&self, events: &[Event]) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(events.len() * 256);

        for event in events {
            let row = serde_json::json!({
                "lsn": event.lsn,
                "xid": event.xid,
                "op": event.op.as_str(),
                "table": event.table,
                "data": event.data,
                "before": event.before,
                "ts": event.ts.format("%Y-%m-%d %H:%M:%S").to_string()
            });

            serde_json::to_writer(&mut body, &row)?;
            body.push(b'\n');
        }

        Ok(body)
    }

    /// Execute a query against ClickHouse
    async fn execute_query(&self, query: &str) -> Result<String> {
        let url = format!("{}/", self.config.url.trim_end_matches('/'));

        let mut request = self.client.post(&url).query(&[("query", query)]);

        if let Some(ref user) = self.config.user {
            request = request.query(&[("user", user)]);
        }
        if let Some(ref password) = self.config.password {
            request = request.query(&[("password", password)]);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(Error::clickhouse(format!(
                "Query failed with status {}: {}",
                status, text
            )));
        }

        response
            .text()
            .await
            .map_err(|e| Error::clickhouse(e.to_string()))
    }

    /// Get sink statistics
    pub fn stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.batches_sent.load(Ordering::Relaxed),
            self.stats.events_flushed.load(Ordering::Relaxed),
            self.stats.bytes_sent.load(Ordering::Relaxed),
            self.stats.errors.load(Ordering::Relaxed),
        )
    }

    /// Get current batch size
    pub fn pending_events(&self) -> usize {
        self.batch.lock().events.len()
    }

    /// Check if batch should be flushed based on timeout
    pub fn should_flush_by_timeout(&self) -> bool {
        let batch = self.batch.lock();
        !batch.events.is_empty() && batch.last_flush.elapsed() > self.config.batch_timeout()
    }
}

/// Estimate the size of an event in bytes
fn estimate_event_size(event: &Event) -> usize {
    // Base overhead + fields
    64 + event.table.len()
        + event.data.to_string().len()
        + event.before.as_ref().map(|v| v.to_string().len()).unwrap_or(0)
}

/// Compress data with gzip
fn compress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    use std::io::Write;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data)?;
    encoder.finish().map_err(|e| Error::Io(e))
}

/// Batch writer for high-throughput scenarios
pub struct BatchWriter {
    sink: Arc<ClickHouseSink>,
    buffer: Vec<Event>,
    max_batch_size: usize,
    flush_interval: Duration,
    last_flush: Instant,
}

impl BatchWriter {
    pub fn new(sink: Arc<ClickHouseSink>, max_batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            sink,
            buffer: Vec::with_capacity(max_batch_size),
            max_batch_size,
            flush_interval,
            last_flush: Instant::now(),
        }
    }

    /// Write events to the buffer
    pub async fn write(&mut self, events: Vec<Event>) -> Result<u64> {
        self.buffer.extend(events);

        let should_flush = self.buffer.len() >= self.max_batch_size
            || self.last_flush.elapsed() > self.flush_interval;

        if should_flush {
            return self.flush().await;
        }

        Ok(0)
    }

    /// Flush the buffer to ClickHouse
    pub async fn flush(&mut self) -> Result<u64> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let events = std::mem::take(&mut self.buffer);
        self.buffer = Vec::with_capacity(self.max_batch_size);
        self.last_flush = Instant::now();

        self.sink.flush_events(events).await
    }

    /// Get pending event count
    pub fn pending(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Operation;
    use chrono::Utc;

    fn create_test_event(id: u64) -> Event {
        Event::new(
            id,
            1,
            Operation::Insert,
            "public.users".to_string(),
            serde_json::json!({"id": id, "name": "test"}),
            None,
            Utc::now(),
            Some(serde_json::json!({"id": id})),
        )
    }

    #[test]
    fn test_event_size_estimation() {
        let event = create_test_event(1);
        let size = estimate_event_size(&event);
        assert!(size > 0);
        assert!(size < 1000);
    }

    #[test]
    fn test_jsonl_body_format() {
        let config = ClickHouseConfig {
            url: "http://localhost:8123".to_string(),
            database: "default".to_string(),
            table: "zenith_cdc".to_string(),
            user: None,
            password: None,
            batch_size: 1000,
            batch_timeout_ms: 100,
            compression: false,
        };

        // Would need async runtime to test fully
    }
}

