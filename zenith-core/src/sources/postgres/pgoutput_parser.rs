//! Complete pgoutput v1 protocol parser
//!
//! Parses all message types from PostgreSQL logical replication:
//! - Begin (B): Transaction start
//! - Commit (C): Transaction commit
//! - Relation (R): Table schema
//! - Insert (I): New row
//! - Update (U): Row update with optional old tuple
//! - Delete (D): Row deletion
//! - Type (Y): Custom type definition
//! - Origin (O): Replication origin
//! - Truncate (T): Table truncation
//! - Message (M): Logical decoding message
//! - StreamStart (S): Streaming transaction start
//! - StreamStop (E): Streaming transaction stop
//! - StreamCommit (c): Streaming transaction commit
//! - StreamAbort (A): Streaming transaction abort

use crate::error::{Error, Result};
use crate::schema::{Column, Relation, ReplicaIdentity};
use bytes::Buf;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::{debug, trace, warn};

/// Message type identifiers in pgoutput protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Begin = b'B',
    Commit = b'C',
    Relation = b'R',
    Insert = b'I',
    Update = b'U',
    Delete = b'D',
    Type = b'Y',
    Origin = b'O',
    Truncate = b'T',
    Message = b'M',
    StreamStart = b'S',
    StreamStop = b'E',
    StreamCommit = b'c',
    StreamAbort = b'A',
}

impl TryFrom<u8> for MessageType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            b'B' => Ok(MessageType::Begin),
            b'C' => Ok(MessageType::Commit),
            b'R' => Ok(MessageType::Relation),
            b'I' => Ok(MessageType::Insert),
            b'U' => Ok(MessageType::Update),
            b'D' => Ok(MessageType::Delete),
            b'Y' => Ok(MessageType::Type),
            b'O' => Ok(MessageType::Origin),
            b'T' => Ok(MessageType::Truncate),
            b'M' => Ok(MessageType::Message),
            b'S' => Ok(MessageType::StreamStart),
            b'E' => Ok(MessageType::StreamStop),
            b'c' => Ok(MessageType::StreamCommit),
            b'A' => Ok(MessageType::StreamAbort),
            _ => Err(Error::invalid_message(format!(
                "Unknown message type: {} (0x{:02x})",
                value as char, value
            ))),
        }
    }
}

/// Tuple data column format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TupleDataFormat {
    /// Null value
    Null,
    /// Unchanged TOAST value (not sent)
    Unchanged,
    /// Text format value
    Text,
    /// Binary format value
    Binary,
}

impl TryFrom<u8> for TupleDataFormat {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            b'n' => Ok(TupleDataFormat::Null),
            b'u' => Ok(TupleDataFormat::Unchanged),
            b't' => Ok(TupleDataFormat::Text),
            b'b' => Ok(TupleDataFormat::Binary),
            _ => Err(Error::invalid_message(format!(
                "Unknown tuple data format: {} (0x{:02x})",
                value as char, value
            ))),
        }
    }
}

/// Single column value in a tuple
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnValue {
    Null,
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

impl ColumnValue {
    /// Convert to JSON value with type information
    pub fn to_json_value(&self, type_oid: u32) -> Value {
        match self {
            ColumnValue::Null => Value::Null,
            ColumnValue::Unchanged => Value::Null, // Or we could use a sentinel
            ColumnValue::Text(s) => {
                // Try to parse based on type OID
                match type_oid {
                    // Boolean
                    16 => Value::Bool(s == "t" || s == "true" || s == "1"),
                    // Integer types
                    20 | 21 | 23 => s.parse::<i64>().map(Value::from).unwrap_or(Value::String(s.clone())),
                    // Float types
                    700 | 701 => s.parse::<f64>().map(Value::from).unwrap_or(Value::String(s.clone())),
                    // Numeric (keep as string for precision)
                    1700 => Value::String(s.clone()),
                    // JSON/JSONB
                    114 | 3802 => serde_json::from_str(s).unwrap_or(Value::String(s.clone())),
                    // UUID
                    2950 => Value::String(s.clone()),
                    // Arrays - keep as string for now
                    _ if s.starts_with('{') && s.ends_with('}') => Value::String(s.clone()),
                    // Default: string
                    _ => Value::String(s.clone()),
                }
            }
            ColumnValue::Binary(b) => {
                // For binary, encode as base64 or hex
                Value::String(format!("\\x{}", hex::encode(b)))
            }
        }
    }

    /// Check if this is a null value
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }

    /// Check if unchanged
    #[inline]
    pub fn is_unchanged(&self) -> bool {
        matches!(self, ColumnValue::Unchanged)
    }

    /// Get as text if available
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ColumnValue::Text(s) => Some(s),
            _ => None,
        }
    }
}

// Simple hex encoding
mod hex {
    pub fn encode(data: &[u8]) -> String {
        let mut s = String::with_capacity(data.len() * 2);
        for byte in data {
            s.push_str(&format!("{:02x}", byte));
        }
        s
    }
}

/// Tuple data (row)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TupleData {
    pub columns: Vec<ColumnValue>,
}

impl TupleData {
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    /// Convert tuple to JSON object using column metadata
    pub fn to_json(&self, relation: &Relation) -> Value {
        let mut map = Map::new();
        for (i, col_value) in self.columns.iter().enumerate() {
            if let Some(col_meta) = relation.columns.get(i) {
                let value: Value = col_value.to_json_value(col_meta.type_oid);
                map.insert(col_meta.name.clone(), value);
            }
        }
        Value::Object(map)
    }

    /// Extract primary key values
    pub fn extract_pk(&self, relation: &Relation) -> Value {
        let mut map = Map::new();
        for &idx in &relation.primary_key_indices {
            if let (Some(col_meta), Some(col_value)) = (relation.columns.get(idx), self.columns.get(idx)) {
                let value = col_value.to_json_value(col_meta.type_oid);
                map.insert(col_meta.name.clone(), value);
            }
        }
        Value::Object(map)
    }

    /// Check if tuple is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get column count
    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

/// Parsed pgoutput message
#[derive(Debug, Clone)]
pub enum PgOutputMessage {
    /// Transaction begin
    Begin {
        /// Final LSN of the transaction
        final_lsn: u64,
        /// Commit timestamp (microseconds since 2000-01-01)
        commit_time: i64,
        /// Transaction ID
        xid: u64,
    },

    /// Transaction commit
    Commit {
        /// Flags (currently unused, always 0)
        flags: u8,
        /// LSN of the commit
        commit_lsn: u64,
        /// End LSN of the transaction
        end_lsn: u64,
        /// Commit timestamp (microseconds since 2000-01-01)
        commit_time: i64,
    },

    /// Relation (table) definition
    Relation {
        id: u32,
        namespace: String,
        name: String,
        replica_identity: ReplicaIdentity,
        columns: Vec<Column>,
    },

    /// Insert operation
    Insert {
        relation_id: u32,
        new_tuple: TupleData,
    },

    /// Update operation
    Update {
        relation_id: u32,
        /// Old tuple (only if REPLICA IDENTITY FULL or key changed)
        old_tuple: Option<TupleData>,
        /// New tuple
        new_tuple: TupleData,
    },

    /// Delete operation
    Delete {
        relation_id: u32,
        /// Old tuple (primary key only if REPLICA IDENTITY DEFAULT, full row if FULL)
        old_tuple: TupleData,
        /// Whether old_tuple contains full row or just key
        key_only: bool,
    },

    /// Type definition
    Type {
        id: u32,
        namespace: String,
        name: String,
    },

    /// Origin (for cascaded replication)
    Origin {
        origin_lsn: u64,
        origin_name: String,
    },

    /// Truncate operation
    Truncate {
        /// Truncate options
        options: u8,
        /// List of relation IDs being truncated
        relation_ids: Vec<u32>,
    },

    /// Logical decoding message
    Message {
        /// Transactional flag
        transactional: bool,
        /// Message prefix
        prefix: String,
        /// LSN of the message
        lsn: u64,
        /// Message content
        content: Vec<u8>,
    },

    /// Streaming replication: transaction start
    StreamStart {
        xid: u64,
        first_segment: bool,
    },

    /// Streaming replication: transaction stop
    StreamStop,

    /// Streaming replication: commit
    StreamCommit {
        xid: u64,
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_time: i64,
    },

    /// Streaming replication: abort
    StreamAbort {
        xid: u64,
        subxid: u64,
    },
}

impl PgOutputMessage {
    /// Get the message type name for metrics
    pub fn type_name(&self) -> &'static str {
        match self {
            PgOutputMessage::Begin { .. } => "begin",
            PgOutputMessage::Commit { .. } => "commit",
            PgOutputMessage::Relation { .. } => "relation",
            PgOutputMessage::Insert { .. } => "insert",
            PgOutputMessage::Update { .. } => "update",
            PgOutputMessage::Delete { .. } => "delete",
            PgOutputMessage::Type { .. } => "type",
            PgOutputMessage::Origin { .. } => "origin",
            PgOutputMessage::Truncate { .. } => "truncate",
            PgOutputMessage::Message { .. } => "message",
            PgOutputMessage::StreamStart { .. } => "stream_start",
            PgOutputMessage::StreamStop => "stream_stop",
            PgOutputMessage::StreamCommit { .. } => "stream_commit",
            PgOutputMessage::StreamAbort { .. } => "stream_abort",
        }
    }

    /// Check if this is a data-modifying message
    #[inline]
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            PgOutputMessage::Insert { .. }
                | PgOutputMessage::Update { .. }
                | PgOutputMessage::Delete { .. }
        )
    }

    /// Get the relation ID if this is a DML message
    pub fn relation_id(&self) -> Option<u32> {
        match self {
            PgOutputMessage::Insert { relation_id, .. } => Some(*relation_id),
            PgOutputMessage::Update { relation_id, .. } => Some(*relation_id),
            PgOutputMessage::Delete { relation_id, .. } => Some(*relation_id),
            _ => None,
        }
    }
}

/// pgoutput protocol parser
///
/// Efficiently parses binary pgoutput messages into structured data.
#[derive(Debug, Clone)]
pub struct PgOutputParser {
    /// Protocol version (currently always 1)
    pub version: u8,
}

impl Default for PgOutputParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PgOutputParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self { version: 1 }
    }

    /// Parse a pgoutput message from raw bytes
    pub fn parse(&self, data: &[u8]) -> Result<PgOutputMessage> {
        if data.is_empty() {
            return Err(Error::invalid_message("Empty message"));
        }

        let msg_type = MessageType::try_from(data[0])?;
        let mut buf = &data[1..];

        match msg_type {
            MessageType::Begin => self.parse_begin(&mut buf),
            MessageType::Commit => self.parse_commit(&mut buf),
            MessageType::Relation => self.parse_relation(&mut buf),
            MessageType::Insert => self.parse_insert(&mut buf),
            MessageType::Update => self.parse_update(&mut buf),
            MessageType::Delete => self.parse_delete(&mut buf),
            MessageType::Type => self.parse_type(&mut buf),
            MessageType::Origin => self.parse_origin(&mut buf),
            MessageType::Truncate => self.parse_truncate(&mut buf),
            MessageType::Message => self.parse_message(&mut buf),
            MessageType::StreamStart => self.parse_stream_start(&mut buf),
            MessageType::StreamStop => Ok(PgOutputMessage::StreamStop),
            MessageType::StreamCommit => self.parse_stream_commit(&mut buf),
            MessageType::StreamAbort => self.parse_stream_abort(&mut buf),
        }
    }

    /// Parse Begin message
    fn parse_begin(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 20)?;

        let final_lsn = buf.get_u64();
        let commit_time = buf.get_i64();
        let xid = buf.get_u32() as u64;

        trace!("Parsed BEGIN: xid={}, final_lsn={}, commit_time={}", xid, final_lsn, commit_time);

        Ok(PgOutputMessage::Begin {
            final_lsn,
            commit_time,
            xid,
        })
    }

    /// Parse Commit message
    fn parse_commit(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 25)?;

        let flags = buf.get_u8();
        let commit_lsn = buf.get_u64();
        let end_lsn = buf.get_u64();
        let commit_time = buf.get_i64();

        trace!("Parsed COMMIT: commit_lsn={}, end_lsn={}", commit_lsn, end_lsn);

        Ok(PgOutputMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            commit_time,
        })
    }

    /// Parse Relation message
    fn parse_relation(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 6)?;

        let id = buf.get_u32();
        let namespace = read_cstring(buf)?;
        let name = read_cstring(buf)?;

        ensure_remaining(buf, 2)?;
        let replica_identity = ReplicaIdentity::from(buf.get_u8());
        let num_columns = buf.get_u16() as usize;

        let mut columns = Vec::with_capacity(num_columns);
        let mut primary_key_indices = Vec::new();

        for i in 0..num_columns {
            ensure_remaining(buf, 1)?;
            let flags = buf.get_u8();
            let col_name = read_cstring(buf)?;

            ensure_remaining(buf, 8)?;
            let type_oid = buf.get_u32();
            let type_modifier = buf.get_i32();

            let column = Column {
                name: col_name,
                flags,
                type_oid,
                type_modifier,
            };

            if column.is_key() {
                primary_key_indices.push(i);
            }

            columns.push(column);
        }

        debug!(
            "Parsed RELATION: {}.{} (id={}, columns={}, pk_indices={:?})",
            namespace, name, id, columns.len(), primary_key_indices
        );

        Ok(PgOutputMessage::Relation {
            id,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }

    /// Parse Insert message
    fn parse_insert(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;

        let relation_id = buf.get_u32();
        let tuple_type = buf.get_u8();

        if tuple_type != b'N' {
            return Err(Error::invalid_message(format!(
                "Expected 'N' for INSERT new tuple, got '{}'",
                tuple_type as char
            )));
        }

        let new_tuple = self.parse_tuple_data(buf)?;

        trace!("Parsed INSERT: relation_id={}, columns={}", relation_id, new_tuple.len());

        Ok(PgOutputMessage::Insert {
            relation_id,
            new_tuple,
        })
    }

    /// Parse Update message
    fn parse_update(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;

        let relation_id = buf.get_u32();
        let mut old_tuple = None;

        // Check for old tuple indicator
        ensure_remaining(buf, 1)?;
        let indicator = buf.get_u8();

        match indicator {
            b'O' | b'K' => {
                // Old tuple present (O = full old tuple, K = key only)
                old_tuple = Some(self.parse_tuple_data(buf)?);

                // Now read the new tuple indicator
                ensure_remaining(buf, 1)?;
                let new_indicator = buf.get_u8();
                if new_indicator != b'N' {
                    return Err(Error::invalid_message(format!(
                        "Expected 'N' for UPDATE new tuple, got '{}'",
                        new_indicator as char
                    )));
                }
            }
            b'N' => {
                // No old tuple, indicator is for new tuple
            }
            _ => {
                return Err(Error::invalid_message(format!(
                    "Invalid UPDATE tuple indicator: '{}'",
                    indicator as char
                )));
            }
        }

        let new_tuple = self.parse_tuple_data(buf)?;

        trace!(
            "Parsed UPDATE: relation_id={}, has_old={}, new_columns={}",
            relation_id,
            old_tuple.is_some(),
            new_tuple.len()
        );

        Ok(PgOutputMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
        })
    }

    /// Parse Delete message
    fn parse_delete(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;

        let relation_id = buf.get_u32();
        let tuple_type = buf.get_u8();

        let key_only = match tuple_type {
            b'K' => true,  // Key tuple only
            b'O' => false, // Full old tuple
            _ => {
                return Err(Error::invalid_message(format!(
                    "Expected 'K' or 'O' for DELETE, got '{}'",
                    tuple_type as char
                )));
            }
        };

        let old_tuple = self.parse_tuple_data(buf)?;

        trace!(
            "Parsed DELETE: relation_id={}, key_only={}, columns={}",
            relation_id,
            key_only,
            old_tuple.len()
        );

        Ok(PgOutputMessage::Delete {
            relation_id,
            old_tuple,
            key_only,
        })
    }

    /// Parse Type message
    fn parse_type(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 4)?;

        let id = buf.get_u32();
        let namespace = read_cstring(buf)?;
        let name = read_cstring(buf)?;

        debug!("Parsed TYPE: {}.{} (id={})", namespace, name, id);

        Ok(PgOutputMessage::Type { id, namespace, name })
    }

    /// Parse Origin message
    fn parse_origin(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 8)?;

        let origin_lsn = buf.get_u64();
        let origin_name = read_cstring(buf)?;

        debug!("Parsed ORIGIN: {} at {}", origin_name, origin_lsn);

        Ok(PgOutputMessage::Origin {
            origin_lsn,
            origin_name,
        })
    }

    /// Parse Truncate message
    fn parse_truncate(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;

        let num_relations = buf.get_u32() as usize;
        let options = buf.get_u8();

        let mut relation_ids = Vec::with_capacity(num_relations);
        for _ in 0..num_relations {
            ensure_remaining(buf, 4)?;
            relation_ids.push(buf.get_u32());
        }

        warn!("Parsed TRUNCATE: {} relations, options={}", num_relations, options);

        Ok(PgOutputMessage::Truncate {
            options,
            relation_ids,
        })
    }

    /// Parse Message (logical decoding message)
    fn parse_message(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 10)?;

        let transactional = buf.get_u8() != 0;
        let lsn = buf.get_u64();
        let prefix = read_cstring(buf)?;

        ensure_remaining(buf, 4)?;
        let content_len = buf.get_u32() as usize;

        ensure_remaining(buf, content_len)?;
        let content = buf[..content_len].to_vec();
        buf.advance(content_len);

        debug!(
            "Parsed MESSAGE: prefix={}, lsn={}, len={}",
            prefix, lsn, content_len
        );

        Ok(PgOutputMessage::Message {
            transactional,
            prefix,
            lsn,
            content,
        })
    }

    /// Parse StreamStart message
    fn parse_stream_start(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;

        let xid = buf.get_u32() as u64;
        let first_segment = buf.get_u8() != 0;

        trace!("Parsed STREAM_START: xid={}, first={}", xid, first_segment);

        Ok(PgOutputMessage::StreamStart { xid, first_segment })
    }

    /// Parse StreamCommit message
    fn parse_stream_commit(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 29)?;

        let xid = buf.get_u32() as u64;
        let flags = buf.get_u8();
        let commit_lsn = buf.get_u64();
        let end_lsn = buf.get_u64();
        let commit_time = buf.get_i64();

        trace!("Parsed STREAM_COMMIT: xid={}, commit_lsn={}", xid, commit_lsn);

        Ok(PgOutputMessage::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            commit_time,
        })
    }

    /// Parse StreamAbort message
    fn parse_stream_abort(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 8)?;

        let xid = buf.get_u32() as u64;
        let subxid = buf.get_u32() as u64;

        trace!("Parsed STREAM_ABORT: xid={}, subxid={}", xid, subxid);

        Ok(PgOutputMessage::StreamAbort { xid, subxid })
    }

    /// Parse tuple data (column values)
    fn parse_tuple_data(&self, buf: &mut &[u8]) -> Result<TupleData> {
        ensure_remaining(buf, 2)?;

        let num_columns = buf.get_u16() as usize;
        let mut columns = Vec::with_capacity(num_columns);

        for _ in 0..num_columns {
            ensure_remaining(buf, 1)?;
            let format = TupleDataFormat::try_from(buf.get_u8())?;

            let value = match format {
                TupleDataFormat::Null => ColumnValue::Null,
                TupleDataFormat::Unchanged => ColumnValue::Unchanged,
                TupleDataFormat::Text => {
                    ensure_remaining(buf, 4)?;
                    let len = buf.get_u32() as usize;
                    ensure_remaining(buf, len)?;

                    let text = String::from_utf8_lossy(&buf[..len]).to_string();
                    buf.advance(len);
                    ColumnValue::Text(text)
                }
                TupleDataFormat::Binary => {
                    ensure_remaining(buf, 4)?;
                    let len = buf.get_u32() as usize;
                    ensure_remaining(buf, len)?;

                    let data = buf[..len].to_vec();
                    buf.advance(len);
                    ColumnValue::Binary(data)
                }
            };

            columns.push(value);
        }

        Ok(TupleData { columns })
    }
}

/// Helper function to ensure buffer has enough remaining bytes
#[inline]
fn ensure_remaining(buf: &[u8], required: usize) -> Result<()> {
    if buf.len() < required {
        return Err(Error::invalid_message(format!(
            "Buffer underflow: need {} bytes, have {}",
            required,
            buf.len()
        )));
    }
    Ok(())
}

/// Read a null-terminated C string from the buffer
fn read_cstring(buf: &mut &[u8]) -> Result<String> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| Error::invalid_message("Missing null terminator in string"))?;

    let s = String::from_utf8_lossy(&buf[..pos]).to_string();
    *buf = &buf[pos + 1..]; // Skip the null terminator
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_begin_message(xid: u32, final_lsn: u64, commit_time: i64) -> Vec<u8> {
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&final_lsn.to_be_bytes());
        msg.extend_from_slice(&commit_time.to_be_bytes());
        msg.extend_from_slice(&xid.to_be_bytes());
        msg
    }

    fn create_commit_message(flags: u8, commit_lsn: u64, end_lsn: u64, commit_time: i64) -> Vec<u8> {
        let mut msg = vec![b'C'];
        msg.push(flags);
        msg.extend_from_slice(&commit_lsn.to_be_bytes());
        msg.extend_from_slice(&end_lsn.to_be_bytes());
        msg.extend_from_slice(&commit_time.to_be_bytes());
        msg
    }

    #[test]
    fn test_parse_begin() {
        let parser = PgOutputParser::new();
        let msg = create_begin_message(12345, 0x123456789, 1700000000000000);

        match parser.parse(&msg).unwrap() {
            PgOutputMessage::Begin {
                xid,
                final_lsn,
                commit_time,
            } => {
                assert_eq!(xid, 12345);
                assert_eq!(final_lsn, 0x123456789);
                assert_eq!(commit_time, 1700000000000000);
            }
            _ => panic!("Expected Begin message"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let parser = PgOutputParser::new();
        let msg = create_commit_message(0, 0x123456789, 0x123456790, 1700000000000000);

        match parser.parse(&msg).unwrap() {
            PgOutputMessage::Commit {
                flags,
                commit_lsn,
                end_lsn,
                commit_time,
            } => {
                assert_eq!(flags, 0);
                assert_eq!(commit_lsn, 0x123456789);
                assert_eq!(end_lsn, 0x123456790);
                assert_eq!(commit_time, 1700000000000000);
            }
            _ => panic!("Expected Commit message"),
        }
    }

    #[test]
    fn test_column_value_to_json() {
        // Test integer
        let int_val = ColumnValue::Text("42".to_string());
        assert_eq!(int_val.to_json_value(23), Value::from(42i64));

        // Test boolean
        let bool_val = ColumnValue::Text("t".to_string());
        assert_eq!(bool_val.to_json_value(16), Value::Bool(true));

        // Test null
        let null_val = ColumnValue::Null;
        assert_eq!(null_val.to_json_value(0), Value::Null);

        // Test text
        let text_val = ColumnValue::Text("hello".to_string());
        assert_eq!(text_val.to_json_value(25), Value::String("hello".to_string()));
    }

    #[test]
    fn test_tuple_data_to_json() {
        let relation = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                Column {
                    name: "id".to_string(),
                    flags: 1,
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    name: "name".to_string(),
                    flags: 0,
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
            primary_key_indices: vec![0],
        };

        let tuple = TupleData {
            columns: vec![
                ColumnValue::Text("1".to_string()),
                ColumnValue::Text("John".to_string()),
            ],
        };

        let json = tuple.to_json(&relation);
        assert_eq!(json["id"], 1);
        assert_eq!(json["name"], "John");
    }
}

