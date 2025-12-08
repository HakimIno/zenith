//! Integration tests for pgoutput parser

use zenith_core::schema::{Column, Relation, ReplicaIdentity, SchemaRegistry};
use zenith_core::sources::postgres::pgoutput_parser::{
    ColumnValue, PgOutputMessage, PgOutputParser, TupleData,
};
use zenith_core::pipeline::transaction_buffer::{TransactionBuffer, Operation};
use std::sync::Arc;

/// Helper to create test messages
mod message_builder {
    pub fn begin(xid: u32, final_lsn: u64, commit_time: i64) -> Vec<u8> {
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&final_lsn.to_be_bytes());
        msg.extend_from_slice(&commit_time.to_be_bytes());
        msg.extend_from_slice(&xid.to_be_bytes());
        msg
    }

    pub fn commit(flags: u8, commit_lsn: u64, end_lsn: u64, commit_time: i64) -> Vec<u8> {
        let mut msg = vec![b'C'];
        msg.push(flags);
        msg.extend_from_slice(&commit_lsn.to_be_bytes());
        msg.extend_from_slice(&end_lsn.to_be_bytes());
        msg.extend_from_slice(&commit_time.to_be_bytes());
        msg
    }

    pub fn relation(
        id: u32,
        namespace: &str,
        name: &str,
        replica_identity: u8,
        columns: &[(&str, u8, u32)], // (name, flags, type_oid)
    ) -> Vec<u8> {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&id.to_be_bytes());

        // Namespace
        msg.extend_from_slice(namespace.as_bytes());
        msg.push(0);

        // Name
        msg.extend_from_slice(name.as_bytes());
        msg.push(0);

        // Replica identity
        msg.push(replica_identity);

        // Columns
        msg.extend_from_slice(&(columns.len() as u16).to_be_bytes());

        for (col_name, flags, type_oid) in columns {
            msg.push(*flags);
            msg.extend_from_slice(col_name.as_bytes());
            msg.push(0);
            msg.extend_from_slice(&type_oid.to_be_bytes());
            msg.extend_from_slice(&(-1i32).to_be_bytes());
        }

        msg
    }

    pub fn insert(relation_id: u32, values: &[&str]) -> Vec<u8> {
        let mut msg = vec![b'I'];
        msg.extend_from_slice(&relation_id.to_be_bytes());
        msg.push(b'N');

        // Column count
        msg.extend_from_slice(&(values.len() as u16).to_be_bytes());

        for value in values {
            msg.push(b't'); // Text format
            msg.extend_from_slice(&(value.len() as u32).to_be_bytes());
            msg.extend_from_slice(value.as_bytes());
        }

        msg
    }

    pub fn update_with_old(
        relation_id: u32,
        old_values: &[&str],
        new_values: &[&str],
    ) -> Vec<u8> {
        let mut msg = vec![b'U'];
        msg.extend_from_slice(&relation_id.to_be_bytes());

        // Old tuple
        msg.push(b'O');
        msg.extend_from_slice(&(old_values.len() as u16).to_be_bytes());
        for value in old_values {
            msg.push(b't');
            msg.extend_from_slice(&(value.len() as u32).to_be_bytes());
            msg.extend_from_slice(value.as_bytes());
        }

        // New tuple
        msg.push(b'N');
        msg.extend_from_slice(&(new_values.len() as u16).to_be_bytes());
        for value in new_values {
            msg.push(b't');
            msg.extend_from_slice(&(value.len() as u32).to_be_bytes());
            msg.extend_from_slice(value.as_bytes());
        }

        msg
    }

    pub fn delete(relation_id: u32, key_only: bool, values: &[&str]) -> Vec<u8> {
        let mut msg = vec![b'D'];
        msg.extend_from_slice(&relation_id.to_be_bytes());
        msg.push(if key_only { b'K' } else { b'O' });

        msg.extend_from_slice(&(values.len() as u16).to_be_bytes());
        for value in values {
            msg.push(b't');
            msg.extend_from_slice(&(value.len() as u32).to_be_bytes());
            msg.extend_from_slice(value.as_bytes());
        }

        msg
    }
}

#[test]
fn test_full_transaction_flow() {
    let parser = PgOutputParser::new();

    // Parse BEGIN
    let begin = message_builder::begin(12345, 0x100000000, 1700000000000000);
    let result = parser.parse(&begin).unwrap();
    match result {
        PgOutputMessage::Begin { xid, final_lsn, commit_time } => {
            assert_eq!(xid, 12345);
            assert_eq!(final_lsn, 0x100000000);
            assert_eq!(commit_time, 1700000000000000);
        }
        _ => panic!("Expected Begin message"),
    }

    // Parse RELATION
    let relation = message_builder::relation(
        16384,
        "public",
        "users",
        b'f',
        &[
            ("id", 0x01, 23),
            ("name", 0x00, 25),
            ("email", 0x00, 25),
        ],
    );
    let result = parser.parse(&relation).unwrap();
    match result {
        PgOutputMessage::Relation { id, namespace, name, replica_identity, columns } => {
            assert_eq!(id, 16384);
            assert_eq!(namespace, "public");
            assert_eq!(name, "users");
            assert_eq!(replica_identity, ReplicaIdentity::Full);
            assert_eq!(columns.len(), 3);
            assert!(columns[0].is_key());
            assert!(!columns[1].is_key());
        }
        _ => panic!("Expected Relation message"),
    }

    // Parse INSERT
    let insert = message_builder::insert(16384, &["1", "John", "john@example.com"]);
    let result = parser.parse(&insert).unwrap();
    match result {
        PgOutputMessage::Insert { relation_id, new_tuple } => {
            assert_eq!(relation_id, 16384);
            assert_eq!(new_tuple.columns.len(), 3);
            assert_eq!(new_tuple.columns[0].as_text(), Some("1"));
            assert_eq!(new_tuple.columns[1].as_text(), Some("John"));
        }
        _ => panic!("Expected Insert message"),
    }

    // Parse UPDATE with old tuple
    let update = message_builder::update_with_old(
        16384,
        &["1", "John", "john@example.com"],
        &["1", "Jane", "jane@example.com"],
    );
    let result = parser.parse(&update).unwrap();
    match result {
        PgOutputMessage::Update { relation_id, old_tuple, new_tuple } => {
            assert_eq!(relation_id, 16384);
            assert!(old_tuple.is_some());
            let old = old_tuple.unwrap();
            assert_eq!(old.columns[1].as_text(), Some("John"));
            assert_eq!(new_tuple.columns[1].as_text(), Some("Jane"));
        }
        _ => panic!("Expected Update message"),
    }

    // Parse DELETE
    let delete = message_builder::delete(16384, false, &["1", "Jane", "jane@example.com"]);
    let result = parser.parse(&delete).unwrap();
    match result {
        PgOutputMessage::Delete { relation_id, old_tuple, key_only } => {
            assert_eq!(relation_id, 16384);
            assert!(!key_only);
            assert_eq!(old_tuple.columns.len(), 3);
        }
        _ => panic!("Expected Delete message"),
    }

    // Parse COMMIT
    let commit = message_builder::commit(0, 0x100000100, 0x100000100, 1700000000001000);
    let result = parser.parse(&commit).unwrap();
    match result {
        PgOutputMessage::Commit { flags, commit_lsn, end_lsn, commit_time } => {
            assert_eq!(flags, 0);
            assert_eq!(commit_lsn, 0x100000100);
            assert_eq!(end_lsn, 0x100000100);
            assert_eq!(commit_time, 1700000000001000);
        }
        _ => panic!("Expected Commit message"),
    }
}

#[test]
fn test_transaction_buffer_integration() {
    // Setup schema registry
    let registry = Arc::new(SchemaRegistry::new());
    registry.register(Relation {
        id: 16384,
        namespace: "public".to_string(),
        name: "users".to_string(),
        replica_identity: ReplicaIdentity::Full,
        columns: vec![
            Column { name: "id".to_string(), flags: 0x01, type_oid: 23, type_modifier: -1 },
            Column { name: "name".to_string(), flags: 0x00, type_oid: 25, type_modifier: -1 },
        ],
        primary_key_indices: vec![0],
    });

    let buffer = TransactionBuffer::new(registry);
    let parser = PgOutputParser::new();

    // Process transaction
    let begin = parser.parse(&message_builder::begin(100, 1000, 0)).unwrap();
    assert!(buffer.process_message(begin, 1000).is_none());

    let insert = parser.parse(&message_builder::insert(16384, &["1", "Test"])).unwrap();
    assert!(buffer.process_message(insert, 1001).is_none());

    let insert2 = parser.parse(&message_builder::insert(16384, &["2", "Test2"])).unwrap();
    assert!(buffer.process_message(insert2, 1002).is_none());

    // Commit should return the transaction
    let commit = parser.parse(&message_builder::commit(0, 1003, 1003, 0)).unwrap();
    let txn = buffer.process_message(commit, 1003);

    assert!(txn.is_some());
    let txn = txn.unwrap();
    assert_eq!(txn.xid, 100);
    assert_eq!(txn.events.len(), 2);
    assert!(txn.committed);
    assert_eq!(txn.commit_lsn, Some(1003));

    // Verify events
    assert_eq!(txn.events[0].op, Operation::Insert);
    assert_eq!(txn.events[0].table, "public.users");
    assert_eq!(txn.events[0].data["id"], 1);
    assert_eq!(txn.events[1].data["id"], 2);
}

#[test]
fn test_null_and_unchanged_values() {
    let parser = PgOutputParser::new();

    // Create INSERT with null value
    let mut msg = vec![b'I'];
    msg.extend_from_slice(&16384u32.to_be_bytes());
    msg.push(b'N');
    msg.extend_from_slice(&3u16.to_be_bytes()); // 3 columns

    // Column 1: text value
    msg.push(b't');
    msg.extend_from_slice(&1u32.to_be_bytes());
    msg.push(b'1');

    // Column 2: null
    msg.push(b'n');

    // Column 3: text value
    msg.push(b't');
    msg.extend_from_slice(&4u32.to_be_bytes());
    msg.extend_from_slice(b"test");

    let result = parser.parse(&msg).unwrap();
    match result {
        PgOutputMessage::Insert { new_tuple, .. } => {
            assert_eq!(new_tuple.columns.len(), 3);
            assert!(!new_tuple.columns[0].is_null());
            assert!(new_tuple.columns[1].is_null());
            assert!(!new_tuple.columns[2].is_null());
        }
        _ => panic!("Expected Insert message"),
    }
}

#[test]
fn test_schema_registry() {
    let registry = SchemaRegistry::new();

    let relation = Relation {
        id: 16384,
        namespace: "public".to_string(),
        name: "users".to_string(),
        replica_identity: ReplicaIdentity::Full,
        columns: vec![
            Column { name: "id".to_string(), flags: 0x01, type_oid: 23, type_modifier: -1 },
            Column { name: "name".to_string(), flags: 0x00, type_oid: 25, type_modifier: -1 },
        ],
        primary_key_indices: vec![0],
    };

    registry.register(relation);

    assert!(registry.contains(16384));
    assert!(!registry.contains(99999));

    let retrieved = registry.get(16384).unwrap();
    assert_eq!(retrieved.name, "users");
    assert_eq!(retrieved.primary_key_columns(), vec!["id"]);

    let by_name = registry.get_by_name("public.users").unwrap();
    assert_eq!(by_name.id, 16384);
}

#[test]
fn test_tuple_json_conversion() {
    let relation = Relation {
        id: 1,
        namespace: "public".to_string(),
        name: "test".to_string(),
        replica_identity: ReplicaIdentity::Full,
        columns: vec![
            Column { name: "id".to_string(), flags: 0x01, type_oid: 23, type_modifier: -1 },
            Column { name: "name".to_string(), flags: 0x00, type_oid: 25, type_modifier: -1 },
            Column { name: "active".to_string(), flags: 0x00, type_oid: 16, type_modifier: -1 },
            Column { name: "score".to_string(), flags: 0x00, type_oid: 701, type_modifier: -1 },
        ],
        primary_key_indices: vec![0],
    };

    let tuple = TupleData {
        columns: vec![
            ColumnValue::Text("42".to_string()),
            ColumnValue::Text("Test User".to_string()),
            ColumnValue::Text("t".to_string()),
            ColumnValue::Text("95.5".to_string()),
        ],
    };

    let json = tuple.to_json(&relation);

    // Integer conversion
    assert_eq!(json["id"], 42);

    // String as-is
    assert_eq!(json["name"], "Test User");

    // Boolean conversion
    assert_eq!(json["active"], true);

    // Float conversion
    assert_eq!(json["score"], 95.5);

    // PK extraction
    let pk = tuple.extract_pk(&relation);
    assert_eq!(pk["id"], 42);
    assert!(pk.get("name").is_none());
}

