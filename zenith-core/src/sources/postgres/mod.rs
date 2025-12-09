//! PostgreSQL logical replication source
//!
//! This module provides two approaches to PostgreSQL CDC:
//!
//! 1. **Streaming Replication (recommended)** - Uses `START_REPLICATION` command
//!    with copy_both protocol for real-time WAL streaming. This is the production-ready
//!    approach with sub-millisecond latency.
//!
//! 2. **Polling-based (fallback)** - Uses `pg_logical_slot_get_changes()` for
//!    environments where streaming is not available.
//!
//! # Example
//!
//! ```ignore
//! use zenith_core::sources::postgres::{StreamingReplicationSource, StreamingReplicationSourceBuilder};
//!
//! let source = StreamingReplicationSourceBuilder::new(config, schema_registry, shutdown)
//!     .status_interval(Duration::from_secs(5))
//!     .build();
//!
//! let (tx, rx) = mpsc::channel(10000);
//! Arc::new(source).start(0, tx).await?;
//! ```

pub mod binary_copy;
pub mod connection;
pub mod decoder;
pub mod pgoutput_parser;
pub mod slot;
pub mod snapshot;
pub mod streaming;

pub use decoder::{ReplicationDecoder, ReplicationMessage, XLogData, PrimaryKeepalive};
pub use pgoutput_parser::{PgOutputMessage, PgOutputParser, TupleData};
pub use self::snapshot::{SnapshotCopier, SnapshotProgress};
pub use slot::PostgresSource;  // Legacy polling-based source
pub use streaming::{
    StreamingReplicationSource,
    StreamingReplicationSourceBuilder,
    StreamingSourceMessage,
    StatusUpdateConfig,
};
