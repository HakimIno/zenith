//! Configuration for Zenith CDC

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub postgres: PostgresConfig,
    pub clickhouse: ClickHouseConfig,
    pub storage: StorageConfig,
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
}

impl Config {
    /// Load configuration from file
    pub fn from_file(path: &str) -> crate::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(config::Environment::with_prefix("ZENITH").separator("__"))
            .build()
            .map_err(|e| crate::Error::config(e.to_string()))?;

        settings
            .try_deserialize()
            .map_err(|e| crate::Error::config(e.to_string()))
    }

    /// Create configuration from environment variables
    pub fn from_env() -> crate::Result<Self> {
        Ok(Self {
            postgres: PostgresConfig {
                url: std::env::var("POSTGRES_URL")
                    .unwrap_or_else(|_| "postgres://localhost:5432/postgres".to_string()),
                publication: std::env::var("PUBLICATION_NAME")
                    .unwrap_or_else(|_| "zenith_pub".to_string()),
                slot_name: std::env::var("SLOT_NAME")
                    .unwrap_or_else(|_| "zenith_slot".to_string()),
                parallel_slots: std::env::var("PARALLEL_SLOTS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1),
                create_slot: std::env::var("CREATE_SLOT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(true),
                status_update_interval_ms: std::env::var("POSTGRES_STATUS_INTERVAL_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10_000),
            },
            clickhouse: ClickHouseConfig {
                url: std::env::var("CLICKHOUSE_URL")
                    .unwrap_or_else(|_| "http://localhost:8123".to_string()),
                database: std::env::var("CLICKHOUSE_DATABASE")
                    .unwrap_or_else(|_| "default".to_string()),
                table: std::env::var("CLICKHOUSE_TABLE")
                    .unwrap_or_else(|_| "zenith_cdc".to_string()),
                user: std::env::var("CLICKHOUSE_USER").ok(),
                password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
                batch_size: std::env::var("CLICKHOUSE_BATCH_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10_000),
                batch_timeout_ms: std::env::var("CLICKHOUSE_BATCH_TIMEOUT_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(100),
                compression: std::env::var("CLICKHOUSE_COMPRESSION")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(true),
            },
            storage: StorageConfig {
                path: PathBuf::from(
                    std::env::var("STORAGE_PATH").unwrap_or_else(|_| "./zenith_data".to_string()),
                ),
                flush_interval_ms: std::env::var("STORAGE_FLUSH_INTERVAL_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000),
                flush_rows: std::env::var("STORAGE_FLUSH_ROWS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(100_000),
            },
            metrics: MetricsConfig {
                enabled: std::env::var("METRICS_ENABLED")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(true),
                port: std::env::var("METRICS_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(9090),
                host: std::env::var("METRICS_HOST")
                    .unwrap_or_else(|_| "0.0.0.0".to_string()),
            },
            pipeline: PipelineConfig::default(),
        })
    }
}

/// PostgreSQL source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL connection URL
    pub url: String,
    /// Publication name to subscribe to
    pub publication: String,
    /// Replication slot name
    pub slot_name: String,
    /// Number of parallel replication slots (for sharded setups)
    #[serde(default = "default_parallel_slots")]
    pub parallel_slots: usize,
    /// Create slot if it doesn't exist
    #[serde(default = "default_true")]
    pub create_slot: bool,
    /// Status update interval in milliseconds
    #[serde(default = "default_status_update_interval")]
    pub status_update_interval_ms: u64,
}

impl PostgresConfig {
    pub fn status_interval(&self) -> Duration {
        Duration::from_millis(self.status_update_interval_ms)
    }
}

/// ClickHouse sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP URL
    pub url: String,
    /// Target database
    pub database: String,
    /// Target table
    pub table: String,
    /// Optional username
    pub user: Option<String>,
    /// Optional password
    pub password: Option<String>,
    /// Batch size for inserts
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,
    /// Enable LZ4 compression
    #[serde(default = "default_true")]
    pub compression: bool,
}

impl ClickHouseConfig {
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.batch_timeout_ms)
    }
}

/// Storage configuration for WAL position persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to storage directory
    pub path: PathBuf,
    /// Flush interval in milliseconds
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Flush after this many rows
    #[serde(default = "default_flush_rows")]
    pub flush_rows: u64,
}

impl StorageConfig {
    pub fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms)
    }
}

/// Prometheus metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics endpoint
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Port for metrics endpoint
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    /// Host to bind to
    #[serde(default = "default_host")]
    pub host: String,
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Channel buffer size
    #[serde(default = "default_channel_size")]
    pub channel_size: usize,
    /// Maximum buffered transactions
    #[serde(default = "default_max_buffered_txns")]
    pub max_buffered_transactions: usize,
    /// Worker threads for processing
    #[serde(default = "default_workers")]
    pub workers: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_size: default_channel_size(),
            max_buffered_transactions: default_max_buffered_txns(),
            workers: default_workers(),
        }
    }
}

// Default value functions
fn default_parallel_slots() -> usize {
    1
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    10_000
}

fn default_batch_timeout() -> u64 {
    100
}

fn default_flush_interval() -> u64 {
    1000
}

fn default_flush_rows() -> u64 {
    100_000
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_channel_size() -> usize {
    100_000
}

fn default_max_buffered_txns() -> usize {
    10_000
}

fn default_workers() -> usize {
    num_cpus::get().max(4)
}

fn default_status_update_interval() -> u64 {
    10_000
}

// Add num_cpus as inline
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        std::env::set_var("POSTGRES_URL", "postgres://test:test@localhost/test");
        std::env::set_var("CLICKHOUSE_URL", "http://localhost:8123");

        let config = Config::from_env().unwrap();
        assert_eq!(config.postgres.url, "postgres://test:test@localhost/test");
        assert_eq!(config.clickhouse.url, "http://localhost:8123");
    }
}

