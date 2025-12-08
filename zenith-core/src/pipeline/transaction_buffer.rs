//! In-memory transaction buffer with DashMap
//!
//! Buffers events by transaction ID until commit is received,
//! ensuring transactional consistency in the output.

use crate::schema::SchemaRegistry;
use crate::sources::postgres::{PgOutputMessage, TupleData};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, trace, warn};

/// Operation type for CDC events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Insert => "INSERT",
            Operation::Update => "UPDATE",
            Operation::Delete => "DELETE",
        }
    }
}

/// A single CDC event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// LSN of this event
    pub lsn: u64,
    /// Transaction ID
    pub xid: u64,
    /// Operation type
    pub op: Operation,
    /// Fully qualified table name (schema.table)
    pub table: String,
    /// New data (for INSERT and UPDATE)
    pub data: Value,
    /// Old data (for UPDATE with REPLICA IDENTITY FULL, DELETE with FULL)
    pub before: Option<Value>,
    /// Event timestamp
    pub ts: DateTime<Utc>,
    /// Primary key values
    pub pk: Option<Value>,
}

impl Event {
    /// Create a new event
    pub fn new(
        lsn: u64,
        xid: u64,
        op: Operation,
        table: String,
        data: Value,
        before: Option<Value>,
        ts: DateTime<Utc>,
        pk: Option<Value>,
    ) -> Self {
        Self {
            lsn,
            xid,
            op,
            table,
            data,
            before,
            ts,
            pk,
        }
    }

    /// Convert to JSON format for ClickHouse
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "lsn": self.lsn,
            "xid": self.xid,
            "op": self.op.as_str(),
            "table": self.table,
            "data": self.data,
            "before": self.before,
            "ts": self.ts.format("%Y-%m-%d %H:%M:%S").to_string()
        })
    }

    /// Convert to ClickHouse row format
    pub fn to_clickhouse_row(&self) -> String {
        format!(
            "({}, {}, '{}', '{}', '{}', {}, '{}')",
            self.lsn,
            self.xid,
            self.op.as_str(),
            self.table.replace('\'', "''"),
            self.data.to_string().replace('\'', "''"),
            self.before
                .as_ref()
                .map(|v| format!("'{}'", v.to_string().replace('\'', "''")))
                .unwrap_or_else(|| "NULL".to_string()),
            self.ts.format("%Y-%m-%d %H:%M:%S")
        )
    }
}

/// A transaction containing multiple events
#[derive(Debug)]
pub struct Transaction {
    /// Transaction ID
    pub xid: u64,
    /// Commit LSN (set when COMMIT received)
    pub commit_lsn: Option<u64>,
    /// Events in this transaction
    pub events: Vec<Event>,
    /// Whether the transaction is committed
    pub committed: bool,
    /// Begin timestamp
    pub begin_time: i64,
    /// Commit timestamp
    pub commit_time: Option<i64>,
    /// When this transaction was created
    pub created_at: Instant,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(xid: u64, begin_time: i64) -> Self {
        Self {
            xid,
            commit_lsn: None,
            events: Vec::with_capacity(16),
            committed: false,
            begin_time,
            commit_time: None,
            created_at: Instant::now(),
        }
    }

    /// Add an event to this transaction
    pub fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    /// Mark transaction as committed
    pub fn commit(&mut self, commit_lsn: u64, commit_time: i64) {
        self.commit_lsn = Some(commit_lsn);
        self.commit_time = Some(commit_time);
        self.committed = true;
    }

    /// Get the number of events
    #[inline]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Check if transaction is empty (no events)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Take ownership of events
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
}

/// Thread-safe transaction buffer
///
/// Uses DashMap for concurrent access from multiple reader threads.
pub struct TransactionBuffer {
    /// Active transactions (xid → Transaction)
    transactions: DashMap<u64, Transaction>,
    /// Schema registry for relation metadata
    schema_registry: Arc<SchemaRegistry>,
    /// Current transaction ID (for context)
    current_xid: AtomicU64,
    /// Statistics
    stats: BufferStats,
}

#[derive(Debug, Default)]
pub struct BufferStats {
    pub transactions_started: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub events_buffered: AtomicU64,
}

impl TransactionBuffer {
    /// Create a new transaction buffer
    pub fn new(schema_registry: Arc<SchemaRegistry>) -> Self {
        Self {
            transactions: DashMap::with_capacity(1000),
            schema_registry,
            current_xid: AtomicU64::new(0),
            stats: BufferStats::default(),
        }
    }

    /// Get current buffer size (number of active transactions)
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Process a pgoutput message
    ///
    /// Returns Some(Transaction) if a transaction was committed and is ready to flush.
    pub fn process_message(
        &self,
        message: PgOutputMessage,
        lsn: u64,
    ) -> Option<Transaction> {
        match message {
            PgOutputMessage::Begin {
                xid,
                commit_time,
                final_lsn: _,
            } => {
                trace!("BEGIN xid={}", xid);
                let txn = Transaction::new(xid, commit_time);
                self.transactions.insert(xid, txn);
                self.current_xid.store(xid, Ordering::Release);
                self.stats.transactions_started.fetch_add(1, Ordering::Relaxed);
                None
            }

            PgOutputMessage::Commit {
                commit_lsn,
                commit_time,
                end_lsn: _,
                ..
            } => {
                let xid = self.current_xid.load(Ordering::Acquire);
                trace!("COMMIT xid={}, lsn={}", xid, commit_lsn);

                if let Some((_, mut txn)) = self.transactions.remove(&xid) {
                    txn.commit(commit_lsn, commit_time);
                    self.stats.transactions_committed.fetch_add(1, Ordering::Relaxed);

                    // Return non-empty committed transactions
                    if !txn.is_empty() {
                        return Some(txn);
                    }
                }
                None
            }

            PgOutputMessage::Insert {
                relation_id,
                new_tuple,
            } => {
                self.process_insert(relation_id, new_tuple, lsn);
                None
            }

            PgOutputMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
            } => {
                self.process_update(relation_id, old_tuple, new_tuple, lsn);
                None
            }

            PgOutputMessage::Delete {
                relation_id,
                old_tuple,
                key_only,
            } => {
                self.process_delete(relation_id, old_tuple, key_only, lsn);
                None
            }

            // Relation messages are handled at the source level
            PgOutputMessage::Relation { .. } => None,

            // Streaming transaction messages
            PgOutputMessage::StreamStart { xid, first_segment } => {
                if first_segment {
                    let txn = Transaction::new(xid, 0);
                    self.transactions.insert(xid, txn);
                }
                self.current_xid.store(xid, Ordering::Release);
                None
            }

            PgOutputMessage::StreamCommit {
                xid,
                commit_lsn,
                commit_time,
                ..
            } => {
                if let Some((_, mut txn)) = self.transactions.remove(&xid) {
                    txn.commit(commit_lsn, commit_time);
                    self.stats.transactions_committed.fetch_add(1, Ordering::Relaxed);
                    if !txn.is_empty() {
                        return Some(txn);
                    }
                }
                None
            }

            PgOutputMessage::StreamAbort { xid, .. } => {
                self.transactions.remove(&xid);
                None
            }

            PgOutputMessage::StreamStop => None,

            // Log and ignore other messages
            PgOutputMessage::Type { .. }
            | PgOutputMessage::Origin { .. }
            | PgOutputMessage::Truncate { .. }
            | PgOutputMessage::Message { .. } => {
                debug!("Ignoring message: {:?}", message.type_name());
                None
            }
        }
    }

    /// Process INSERT message
    fn process_insert(&self, relation_id: u32, new_tuple: TupleData, lsn: u64) {
        let xid = self.current_xid.load(Ordering::Acquire);

        if let Some(relation) = self.schema_registry.get(relation_id) {
            let table = relation.full_name();
            let data = new_tuple.to_json(&relation);
            let pk = new_tuple.extract_pk(&relation);
            let ts = Utc::now();

            let event = Event::new(
                lsn,
                xid,
                Operation::Insert,
                table,
                data,
                None,
                ts,
                Some(pk),
            );

            self.add_event_to_transaction(xid, event);
        } else {
            warn!("Unknown relation {} for INSERT", relation_id);
        }
    }

    /// Process UPDATE message
    fn process_update(
        &self,
        relation_id: u32,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
        lsn: u64,
    ) {
        let xid = self.current_xid.load(Ordering::Acquire);

        if let Some(relation) = self.schema_registry.get(relation_id) {
            let table = relation.full_name();
            let data = new_tuple.to_json(&relation);
            let before = old_tuple.as_ref().map(|t| t.to_json(&relation));
            let pk = new_tuple.extract_pk(&relation);
            let ts = Utc::now();

            let event = Event::new(
                lsn,
                xid,
                Operation::Update,
                table,
                data,
                before,
                ts,
                Some(pk),
            );

            self.add_event_to_transaction(xid, event);
        } else {
            warn!("Unknown relation {} for UPDATE", relation_id);
        }
    }

    /// Process DELETE message
    fn process_delete(
        &self,
        relation_id: u32,
        old_tuple: TupleData,
        key_only: bool,
        lsn: u64,
    ) {
        let xid = self.current_xid.load(Ordering::Acquire);

        if let Some(relation) = self.schema_registry.get(relation_id) {
            let table = relation.full_name();
            
            // For DELETE, data contains the key (or full old row if REPLICA IDENTITY FULL)
            let data = if key_only {
                old_tuple.extract_pk(&relation)
            } else {
                old_tuple.to_json(&relation)
            };
            
            let pk = old_tuple.extract_pk(&relation);
            let ts = Utc::now();

            let event = Event::new(
                lsn,
                xid,
                Operation::Delete,
                table,
                data,
                None,
                ts,
                Some(pk),
            );

            self.add_event_to_transaction(xid, event);
        } else {
            warn!("Unknown relation {} for DELETE", relation_id);
        }
    }

    /// Add an event to the current transaction
    fn add_event_to_transaction(&self, xid: u64, event: Event) {
        if let Some(mut txn) = self.transactions.get_mut(&xid) {
            txn.add_event(event);
            self.stats.events_buffered.fetch_add(1, Ordering::Relaxed);
        } else {
            warn!("No transaction found for xid {}", xid);
        }
    }

    /// Get buffer statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.transactions_started.load(Ordering::Relaxed),
            self.stats.transactions_committed.load(Ordering::Relaxed),
            self.stats.events_buffered.load(Ordering::Relaxed),
        )
    }

    /// Get total buffered events across all transactions
    pub fn total_events(&self) -> usize {
        self.transactions.iter().map(|t| t.event_count()).sum()
    }

    /// Remove stale transactions (older than timeout)
    pub fn cleanup_stale(&self, max_age_secs: u64) {
        let cutoff = Instant::now() - std::time::Duration::from_secs(max_age_secs);
        let stale: Vec<u64> = self
            .transactions
            .iter()
            .filter(|t| t.created_at < cutoff && !t.committed)
            .map(|t| *t.key())
            .collect();

        for xid in stale {
            warn!("Removing stale transaction xid={}", xid);
            self.transactions.remove(&xid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, Relation, ReplicaIdentity};
    use crate::sources::postgres::TupleData;
    use crate::sources::postgres::pgoutput_parser::ColumnValue;

    fn create_test_registry() -> Arc<SchemaRegistry> {
        let registry = SchemaRegistry::new();
        registry.register(Relation {
            id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                Column {
                    name: "id".to_string(),
                    flags: 0x01,
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    name: "name".to_string(),
                    flags: 0x00,
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
            primary_key_indices: vec![0],
        });
        Arc::new(registry)
    }

    #[test]
    fn test_transaction_lifecycle() {
        let registry = create_test_registry();
        let buffer = TransactionBuffer::new(registry);

        // Begin transaction
        buffer.process_message(
            PgOutputMessage::Begin {
                xid: 100,
                commit_time: 0,
                final_lsn: 1000,
            },
            1000,
        );

        assert_eq!(buffer.len(), 1);

        // Insert event
        buffer.process_message(
            PgOutputMessage::Insert {
                relation_id: 16384,
                new_tuple: TupleData {
                    columns: vec![
                        ColumnValue::Text("1".to_string()),
                        ColumnValue::Text("John".to_string()),
                    ],
                },
            },
            1001,
        );

        // Commit transaction
        let committed = buffer.process_message(
            PgOutputMessage::Commit {
                flags: 0,
                commit_lsn: 1002,
                end_lsn: 1002,
                commit_time: 0,
            },
            1002,
        );

        assert!(committed.is_some());
        let txn = committed.unwrap();
        assert_eq!(txn.xid, 100);
        assert_eq!(txn.events.len(), 1);
        assert!(txn.committed);
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_event_to_json() {
        let event = Event::new(
            1000,
            100,
            Operation::Insert,
            "public.users".to_string(),
            serde_json::json!({"id": 1, "name": "John"}),
            None,
            Utc::now(),
            Some(serde_json::json!({"id": 1})),
        );

        let json = event.to_json();
        assert_eq!(json["op"], "INSERT");
        assert_eq!(json["table"], "public.users");
        assert_eq!(json["data"]["id"], 1);
    }
}

