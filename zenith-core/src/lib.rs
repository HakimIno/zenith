//! Zenith Core - High-performance CDC engine for PostgreSQL to ClickHouse
//!
//! This crate provides the core functionality for change data capture with:
//! - Complete pgoutput v1 parser
//! - Transactional awareness with exactly-once semantics
//! - High-throughput ClickHouse sink
//! - Prometheus metrics
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL -> pgoutput parser -> Transaction Buffer -> Commit Queue -> ClickHouse
//!                                        |
//!                                   Schema Registry
//! ```

pub mod config;
pub mod dlq;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod schema;
pub mod sinks;
pub mod sources;
pub mod utils;

pub use config::Config;
pub use error::{Error, Result};

// Re-export commonly used types
pub use pipeline::commit_queue::CommitQueue;
pub use pipeline::transaction_buffer::{Event, Operation, Transaction, TransactionBuffer};
pub use pipeline::wal_position::WalPosition;
pub use schema::SchemaRegistry;
pub use sinks::clickhouse::ClickHouseSink;
pub use sources::postgres::{PostgresSource, PgOutputMessage};

// Streaming replication (recommended for production)
pub use sources::postgres::{
    StreamingReplicationSource,
    StreamingReplicationSourceBuilder,
    StreamingSourceMessage,
};

