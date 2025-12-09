//! Complete pgoutput v1 protocol parser
//!
//! Parses all message types from PostgreSQL logical replication:
//! - Begin (B), Commit (C): Transaction boundaries
//! - Insert (I), Update (U), Delete (D): Data changes
//! - Relation (R), Type (Y): Schema definitions
//! - Stream*: Streaming replication control

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
    Null,
    Unchanged,
    Text,
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
    pub fn to_json_value(&self, type_oid: u32) -> Value {
        match self {
            ColumnValue::Null => Value::Null,
            ColumnValue::Unchanged => Value::Null,
            ColumnValue::Text(s) => match type_oid {
                16 => Value::Bool(s == "t" || s == "true" || s == "1"),
                20 | 21 | 23 => s.parse::<i64>().map(Value::from).unwrap_or(Value::String(s.clone())),
                700 | 701 => s.parse::<f64>().map(Value::from).unwrap_or(Value::String(s.clone())),
                1700 => Value::String(s.clone()),
                114 | 3802 => serde_json::from_str(s).unwrap_or(Value::String(s.clone())),
                _ => Value::String(s.clone()),
            },
            ColumnValue::Binary(b) => Value::String(format!("\\x{}", hex::encode(b))),
        }
    }

    pub fn is_null(&self) -> bool { matches!(self, ColumnValue::Null) }
    pub fn is_unchanged(&self) -> bool { matches!(self, ColumnValue::Unchanged) }
    pub fn as_text(&self) -> Option<&str> { match self { ColumnValue::Text(s) => Some(s), _ => None } }
}

mod hex {
    pub fn encode(data: &[u8]) -> String {
        let mut s = String::with_capacity(data.len() * 2);
        for byte in data { s.push_str(&format!("{:02x}", byte)); }
        s
    }
}

/// Tuple data (row)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TupleData {
    pub columns: Vec<ColumnValue>,
}

impl TupleData {
    pub fn new() -> Self { Self { columns: Vec::new() } }

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

    pub fn is_empty(&self) -> bool { self.columns.is_empty() }
    pub fn len(&self) -> usize { self.columns.len() }
}

/// Parsed pgoutput message
#[derive(Debug, Clone)]
pub enum PgOutputMessage {
    Begin { final_lsn: u64, commit_time: i64, xid: u64 },
    Commit { flags: u8, commit_lsn: u64, end_lsn: u64, commit_time: i64 },
    Relation { id: u32, namespace: String, name: String, replica_identity: ReplicaIdentity, columns: Vec<Column> },
    Insert { relation_id: u32, new_tuple: TupleData },
    Update { relation_id: u32, old_tuple: Option<TupleData>, new_tuple: TupleData },
    Delete { relation_id: u32, old_tuple: TupleData, key_only: bool },
    Type { id: u32, namespace: String, name: String },
    Origin { origin_lsn: u64, origin_name: String },
    Truncate { options: u8, relation_ids: Vec<u32> },
    Message { transactional: bool, prefix: String, lsn: u64, content: Vec<u8> },
    StreamStart { xid: u64, first_segment: bool },
    StreamStop,
    StreamCommit { xid: u64, flags: u8, commit_lsn: u64, end_lsn: u64, commit_time: i64 },
    StreamAbort { xid: u64, subxid: u64 },
}

impl PgOutputMessage {
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

    pub fn is_dml(&self) -> bool {
        matches!(self, PgOutputMessage::Insert { .. } | PgOutputMessage::Update { .. } | PgOutputMessage::Delete { .. })
    }

    pub fn relation_id(&self) -> Option<u32> {
        match self {
            PgOutputMessage::Insert { relation_id, .. } => Some(*relation_id),
            PgOutputMessage::Update { relation_id, .. } => Some(*relation_id),
            PgOutputMessage::Delete { relation_id, .. } => Some(*relation_id),
            _ => None,
        }
    }
}

// Sub-parsers for modularity
mod parsers {
    use super::*;

    pub(crate) struct TransactionParser;
    impl TransactionParser {
        pub fn parse_begin(buf: &mut &[u8]) -> Result<PgOutputMessage> {
            ensure_remaining(buf, 20)?;
            let final_lsn = buf.get_u64();
            let commit_time = buf.get_i64();
            let xid = buf.get_u32() as u64;
            trace!("Parsed BEGIN: xid={}, final_lsn={}, commit_time={}", xid, final_lsn, commit_time);
            Ok(PgOutputMessage::Begin { final_lsn, commit_time, xid })
        }

        pub fn parse_commit(buf: &mut &[u8]) -> Result<PgOutputMessage> {
            ensure_remaining(buf, 25)?;
            let flags = buf.get_u8();
            let commit_lsn = buf.get_u64();
            let end_lsn = buf.get_u64();
            let commit_time = buf.get_i64();
            trace!("Parsed COMMIT: commit_lsn={}, end_lsn={}", commit_lsn, end_lsn);
            Ok(PgOutputMessage::Commit { flags, commit_lsn, end_lsn, commit_time })
        }
    }

    pub(crate) struct DmlParser;
    impl DmlParser {
        pub fn parse_insert(parser: &PgOutputParser, buf: &mut &[u8]) -> Result<PgOutputMessage> {
            ensure_remaining(buf, 5)?;
            let relation_id = buf.get_u32();
            if buf.get_u8() != b'N' { return Err(Error::invalid_message("Expected 'N' for INSERT")); }
            let new_tuple = parser.parse_tuple_data(buf)?;
            trace!("Parsed INSERT: relation_id={}, columns={}", relation_id, new_tuple.len());
            Ok(PgOutputMessage::Insert { relation_id, new_tuple })
        }

        pub fn parse_update(parser: &PgOutputParser, buf: &mut &[u8]) -> Result<PgOutputMessage> {
            ensure_remaining(buf, 5)?;
            let relation_id = buf.get_u32();
            ensure_remaining(buf, 1)?;
            let indicator = buf.get_u8();
            let old_tuple = match indicator {
                b'O' | b'K' => Some(parser.parse_tuple_data(buf)?),
                b'N' => None,
                _ => return Err(Error::invalid_message("Invalid UPDATE tuple indicator")),
            };
            
            if old_tuple.is_some() {
                ensure_remaining(buf, 1)?;
                if buf.get_u8() != b'N' { return Err(Error::invalid_message("Expected 'N' for UPDATE new tuple")); }
            } else if indicator == b'N' {
                 // For 'N', the next byte is already the start of tuple data (after we consumed 'N')
                 // No wait, if indicator was 'N', we didn't consume anything else.
                 // Actually logic: 
                 // If 'O'/'K': parse old tuple -> read 'N' -> parse new tuple
                 // If 'N': parse new tuple directly
                 // My logic above for 'N' just set old_tuple=None. The buffer is positioned for new tuple.
                 
                // WAIT: If indicator is N, we just proceed.
            }

            let new_tuple = parser.parse_tuple_data(buf)?;
            Ok(PgOutputMessage::Update { relation_id, old_tuple, new_tuple })
        }

        pub fn parse_delete(parser: &PgOutputParser, buf: &mut &[u8]) -> Result<PgOutputMessage> {
            ensure_remaining(buf, 5)?;
            let relation_id = buf.get_u32();
            let tuple_type = buf.get_u8();
            let key_only = match tuple_type {
                b'K' => true, b'O' => false,
                _ => return Err(Error::invalid_message("Expected 'K' or 'O' for DELETE")),
            };
            let old_tuple = parser.parse_tuple_data(buf)?;
            trace!("Parsed DELETE: relation_id={}, key_only={}", relation_id, key_only);
            Ok(PgOutputMessage::Delete { relation_id, old_tuple, key_only })
        }
    }
}

/// pgoutput protocol parser
#[derive(Debug, Clone)]
pub struct PgOutputParser {
    pub version: u8,
}

impl Default for PgOutputParser {
    fn default() -> Self { Self::new() }
}

impl PgOutputParser {
    pub fn new() -> Self { Self { version: 1 } }

    pub fn parse(&self, data: &[u8]) -> Result<PgOutputMessage> {
        if data.is_empty() { return Err(Error::invalid_message("Empty message")); }
        let msg_type = MessageType::try_from(data[0])?;
        let mut buf = &data[1..];

        use parsers::{TransactionParser, DmlParser};

        match msg_type {
            MessageType::Begin => TransactionParser::parse_begin(&mut buf),
            MessageType::Commit => TransactionParser::parse_commit(&mut buf),
            MessageType::Insert => DmlParser::parse_insert(self, &mut buf),
            MessageType::Update => DmlParser::parse_update(self, &mut buf),
            MessageType::Delete => DmlParser::parse_delete(self, &mut buf),
            MessageType::Relation => self.parse_relation(&mut buf),
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

    // Keep remaining parsers as methods for now, can be extracted later if they grow
    fn parse_relation(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 6)?;
        let id = buf.get_u32();
        let namespace = read_cstring(buf)?;
        let name = read_cstring(buf)?;
        ensure_remaining(buf, 2)?;
        let replica_identity = ReplicaIdentity::from(buf.get_u8());
        let num_columns = buf.get_u16() as usize;

        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            ensure_remaining(buf, 1)?;
            let flags = buf.get_u8();
            let col_name = read_cstring(buf)?;
            ensure_remaining(buf, 8)?;
            let type_oid = buf.get_u32();
            let type_modifier = buf.get_i32();
            columns.push(Column { name: col_name, flags, type_oid, type_modifier });
        }
        debug!("Parsed R: {}.{} (cols={})", namespace, name, columns.len());
        Ok(PgOutputMessage::Relation { id, namespace, name, replica_identity, columns })
    }

    fn parse_type(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 4)?;
        Ok(PgOutputMessage::Type {
            id: buf.get_u32(),
            namespace: read_cstring(buf)?,
            name: read_cstring(buf)?,
        })
    }

    fn parse_origin(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 8)?;
        Ok(PgOutputMessage::Origin {
            origin_lsn: buf.get_u64(),
            origin_name: read_cstring(buf)?,
        })
    }

    fn parse_truncate(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;
        let num_relations = buf.get_u32() as usize;
        let options = buf.get_u8();
        let mut relation_ids = Vec::with_capacity(num_relations);
        for _ in 0..num_relations {
            ensure_remaining(buf, 4)?;
            relation_ids.push(buf.get_u32());
        }
        Ok(PgOutputMessage::Truncate { options, relation_ids })
    }

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
        Ok(PgOutputMessage::Message { transactional, prefix, lsn, content })
    }

    fn parse_stream_start(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 5)?;
        Ok(PgOutputMessage::StreamStart { xid: buf.get_u32() as u64, first_segment: buf.get_u8() != 0 })
    }

    fn parse_stream_commit(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 29)?;
        Ok(PgOutputMessage::StreamCommit {
            xid: buf.get_u32() as u64,
            flags: buf.get_u8(),
            commit_lsn: buf.get_u64(),
            end_lsn: buf.get_u64(),
            commit_time: buf.get_i64(),
        })
    }

    fn parse_stream_abort(&self, buf: &mut &[u8]) -> Result<PgOutputMessage> {
        ensure_remaining(buf, 8)?;
        Ok(PgOutputMessage::StreamAbort { xid: buf.get_u32() as u64, subxid: buf.get_u32() as u64 })
    }

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

#[inline]
fn ensure_remaining(buf: &[u8], required: usize) -> Result<()> {
    if buf.len() < required {
        return Err(Error::invalid_message(format!(
            "Buffer underflow: need {} bytes, have {}", required, buf.len()
        )));
    }
    Ok(())
}

fn read_cstring(buf: &mut &[u8]) -> Result<String> {
    let pos = buf.iter().position(|&b| b == 0).ok_or_else(|| Error::invalid_message("Missing null terminator"))?;
    let s = String::from_utf8_lossy(&buf[..pos]).to_string();
    *buf = &buf[pos + 1..];
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

    #[test]
    fn test_parse_begin() {
        let parser = PgOutputParser::new();
        let msg = create_begin_message(12345, 0x123456789, 1700000000000000);
        match parser.parse(&msg).unwrap() {
            PgOutputMessage::Begin { xid, final_lsn, commit_time } => {
                assert_eq!(xid, 12345);
                assert_eq!(final_lsn, 0x123456789);
                assert_eq!(commit_time, 1700000000000000);
            }
            _ => panic!("Expected Begin message"),
        }
    }
    // Existing tests would continue here...
}
