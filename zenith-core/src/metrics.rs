//! Prometheus metrics for Zenith CDC

use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    CounterVec, Encoder, Gauge, GaugeVec, HistogramVec, TextEncoder,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Global metrics instance
pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::new);

/// Metrics collector for Zenith CDC
pub struct Metrics {
    /// Total events received from PostgreSQL
    pub events_received: CounterVec,
    /// Total events flushed to ClickHouse
    pub events_flushed: CounterVec,
    /// Total transactions committed
    pub transactions_committed: CounterVec,
    /// Current confirmed LSN position
    pub confirmed_lsn: Gauge,
    /// Replication lag in bytes
    pub lag_bytes: GaugeVec,
    /// Current transaction buffer size
    pub buffer_size: Gauge,
    /// Commit queue size
    pub commit_queue_size: Gauge,
    /// Current throughput (rows per second)
    pub throughput: Gauge,
    /// Batch insert latency histogram
    pub batch_latency: HistogramVec,
    /// Parse latency histogram
    pub parse_latency: HistogramVec,
    /// Active connections
    pub active_connections: GaugeVec,
    /// Errors by type
    pub errors: CounterVec,

    // Internal tracking for throughput calculation
    last_count: AtomicU64,
    last_time: parking_lot::Mutex<Instant>,
}

impl Metrics {
    pub fn new() -> Self {
        let events_received = register_counter_vec!(
            "zenith_events_received_total",
            "Total number of events received from PostgreSQL",
            &["slot", "type"]
        )
        .expect("Failed to register events_received metric");

        let events_flushed = register_counter_vec!(
            "zenith_events_flushed_total",
            "Total number of events flushed to ClickHouse",
            &["table"]
        )
        .expect("Failed to register events_flushed metric");

        let transactions_committed = register_counter_vec!(
            "zenith_transactions_committed_total",
            "Total committed transactions",
            &["slot"]
        )
        .expect("Failed to register transactions_committed metric");

        let confirmed_lsn = register_gauge!(
            "zenith_confirmed_lsn",
            "Current confirmed LSN position"
        )
        .expect("Failed to register confirmed_lsn metric");

        let lag_bytes = register_gauge_vec!(
            "zenith_lag_bytes",
            "Replication lag in bytes",
            &["slot"]
        )
        .expect("Failed to register lag_bytes metric");

        let buffer_size = register_gauge!(
            "zenith_buffer_size",
            "Current transaction buffer size"
        )
        .expect("Failed to register buffer_size metric");

        let commit_queue_size = register_gauge!(
            "zenith_commit_queue_size",
            "Current commit queue size"
        )
        .expect("Failed to register commit_queue_size metric");

        let throughput = register_gauge!(
            "zenith_throughput_rows_per_sec",
            "Current throughput in rows per second"
        )
        .expect("Failed to register throughput metric");

        let batch_latency = register_histogram_vec!(
            "zenith_batch_latency_seconds",
            "Batch insert latency in seconds",
            &["sink"],
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        )
        .expect("Failed to register batch_latency metric");

        let parse_latency = register_histogram_vec!(
            "zenith_parse_latency_seconds",
            "Parse latency in seconds",
            &["message_type"],
            vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01]
        )
        .expect("Failed to register parse_latency metric");

        let active_connections = register_gauge_vec!(
            "zenith_active_connections",
            "Number of active connections",
            &["type"]
        )
        .expect("Failed to register active_connections metric");

        let errors = register_counter_vec!(
            "zenith_errors_total",
            "Total errors by type",
            &["type"]
        )
        .expect("Failed to register errors metric");

        Self {
            events_received,
            events_flushed,
            transactions_committed,
            confirmed_lsn,
            lag_bytes,
            buffer_size,
            commit_queue_size,
            throughput,
            batch_latency,
            parse_latency,
            active_connections,
            errors,
            last_count: AtomicU64::new(0),
            last_time: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// Record an event received
    #[inline]
    pub fn record_event(&self, slot: &str, event_type: &str) {
        self.events_received
            .with_label_values(&[slot, event_type])
            .inc();
    }

    /// Record events flushed
    #[inline]
    pub fn record_flush(&self, table: &str, count: u64) {
        self.events_flushed
            .with_label_values(&[table])
            .inc_by(count as f64);
    }

    /// Record a committed transaction
    #[inline]
    pub fn record_commit(&self, slot: &str) {
        self.transactions_committed
            .with_label_values(&[slot])
            .inc();
    }

    /// Update confirmed LSN
    #[inline]
    pub fn set_confirmed_lsn(&self, lsn: u64) {
        self.confirmed_lsn.set(lsn as f64);
    }

    /// Update replication lag
    #[inline]
    pub fn set_lag(&self, slot: &str, bytes: u64) {
        self.lag_bytes.with_label_values(&[slot]).set(bytes as f64);
    }

    /// Update buffer size
    #[inline]
    pub fn set_buffer_size(&self, size: usize) {
        self.buffer_size.set(size as f64);
    }

    /// Update commit queue size
    #[inline]
    pub fn set_commit_queue_size(&self, size: usize) {
        self.commit_queue_size.set(size as f64);
    }

    /// Update throughput calculation
    pub fn update_throughput(&self, total_events: u64) {
        let now = Instant::now();
        let mut last_time = self.last_time.lock();
        let elapsed = now.duration_since(*last_time);

        if elapsed.as_secs_f64() >= 1.0 {
            let last_count = self.last_count.swap(total_events, Ordering::Relaxed);
            let diff = total_events.saturating_sub(last_count);
            let rps = diff as f64 / elapsed.as_secs_f64();
            self.throughput.set(rps);
            *last_time = now;
        }
    }

    /// Record batch latency
    #[inline]
    pub fn record_batch_latency(&self, sink: &str, seconds: f64) {
        self.batch_latency
            .with_label_values(&[sink])
            .observe(seconds);
    }

    /// Record parse latency
    #[inline]
    pub fn record_parse_latency(&self, msg_type: &str, seconds: f64) {
        self.parse_latency
            .with_label_values(&[msg_type])
            .observe(seconds);
    }

    /// Update active connections
    #[inline]
    pub fn set_active_connections(&self, conn_type: &str, count: i64) {
        self.active_connections
            .with_label_values(&[conn_type])
            .set(count as f64);
    }

    /// Record an error
    #[inline]
    pub fn record_error(&self, error_type: &str) {
        self.errors.with_label_values(&[error_type]).inc();
    }

    /// Gather all metrics as Prometheus text format
    pub fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer guard for measuring operation duration
pub struct Timer {
    start: Instant,
    metric_fn: Option<Box<dyn FnOnce(f64) + Send>>,
}

impl Timer {
    pub fn new<F: FnOnce(f64) + Send + 'static>(metric_fn: F) -> Self {
        Self {
            start: Instant::now(),
            metric_fn: Some(Box::new(metric_fn)),
        }
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(f) = self.metric_fn.take() {
            f(self.start.elapsed().as_secs_f64());
        }
    }
}

/// Macro for timing operations
#[macro_export]
macro_rules! time_operation {
    ($name:expr, $block:expr) => {{
        let _timer = $crate::metrics::Timer::new(|elapsed| {
            $crate::metrics::METRICS.record_parse_latency($name, elapsed);
        });
        $block
    }};
}

