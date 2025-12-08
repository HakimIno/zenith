//! Benchmarks for pgoutput parser performance
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use zenith_core::sources::postgres::pgoutput_parser::{PgOutputParser, TupleData, ColumnValue};

/// Create a BEGIN message
fn create_begin_message(xid: u32, final_lsn: u64, commit_time: i64) -> Vec<u8> {
    let mut msg = vec![b'B'];
    msg.extend_from_slice(&final_lsn.to_be_bytes());
    msg.extend_from_slice(&commit_time.to_be_bytes());
    msg.extend_from_slice(&xid.to_be_bytes());
    msg
}

/// Create a COMMIT message
fn create_commit_message(flags: u8, commit_lsn: u64, end_lsn: u64, commit_time: i64) -> Vec<u8> {
    let mut msg = vec![b'C'];
    msg.push(flags);
    msg.extend_from_slice(&commit_lsn.to_be_bytes());
    msg.extend_from_slice(&end_lsn.to_be_bytes());
    msg.extend_from_slice(&commit_time.to_be_bytes());
    msg
}

/// Create an INSERT message with tuple data
fn create_insert_message(relation_id: u32, columns: &[&str]) -> Vec<u8> {
    let mut msg = vec![b'I'];
    msg.extend_from_slice(&relation_id.to_be_bytes());
    msg.push(b'N'); // New tuple indicator
    
    // Number of columns
    msg.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    
    // Column values
    for col in columns {
        msg.push(b't'); // Text format
        msg.extend_from_slice(&(col.len() as u32).to_be_bytes());
        msg.extend_from_slice(col.as_bytes());
    }
    
    msg
}

/// Create a RELATION message
fn create_relation_message(
    id: u32,
    namespace: &str,
    name: &str,
    column_names: &[&str],
) -> Vec<u8> {
    let mut msg = vec![b'R'];
    msg.extend_from_slice(&id.to_be_bytes());
    
    // Namespace (null-terminated)
    msg.extend_from_slice(namespace.as_bytes());
    msg.push(0);
    
    // Table name (null-terminated)
    msg.extend_from_slice(name.as_bytes());
    msg.push(0);
    
    // Replica identity
    msg.push(b'f'); // FULL
    
    // Number of columns
    msg.extend_from_slice(&(column_names.len() as u16).to_be_bytes());
    
    // Columns
    for (i, col_name) in column_names.iter().enumerate() {
        msg.push(if i == 0 { 0x01 } else { 0x00 }); // First column is PK
        msg.extend_from_slice(col_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(&25u32.to_be_bytes()); // TEXT type OID
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // Type modifier
    }
    
    msg
}

fn parser_benchmarks(c: &mut Criterion) {
    let parser = PgOutputParser::new();
    
    // Benchmark BEGIN message parsing
    let begin_msg = create_begin_message(12345, 0x123456789, 1700000000000000);
    c.bench_function("parse_begin", |b| {
        b.iter(|| parser.parse(black_box(&begin_msg)))
    });
    
    // Benchmark COMMIT message parsing
    let commit_msg = create_commit_message(0, 0x123456789, 0x123456790, 1700000000000000);
    c.bench_function("parse_commit", |b| {
        b.iter(|| parser.parse(black_box(&commit_msg)))
    });
    
    // Benchmark INSERT message parsing (small)
    let small_insert = create_insert_message(16384, &["1", "John", "john@example.com"]);
    c.bench_function("parse_insert_small", |b| {
        b.iter(|| parser.parse(black_box(&small_insert)))
    });
    
    // Benchmark INSERT message parsing (large - 20 columns)
    let large_columns: Vec<String> = (0..20)
        .map(|i| format!("column_value_{}_with_some_data", i))
        .collect();
    let large_refs: Vec<&str> = large_columns.iter().map(|s| s.as_str()).collect();
    let large_insert = create_insert_message(16384, &large_refs);
    c.bench_function("parse_insert_large", |b| {
        b.iter(|| parser.parse(black_box(&large_insert)))
    });
    
    // Benchmark RELATION message parsing
    let relation_msg = create_relation_message(
        16384,
        "public",
        "users",
        &["id", "name", "email", "created_at", "updated_at"],
    );
    c.bench_function("parse_relation", |b| {
        b.iter(|| parser.parse(black_box(&relation_msg)))
    });
    
    // Throughput benchmark for INSERT parsing
    let mut group = c.benchmark_group("insert_throughput");
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("small_insert", |b| {
        b.iter(|| parser.parse(black_box(&small_insert)))
    });
    
    group.bench_function("large_insert", |b| {
        b.iter(|| parser.parse(black_box(&large_insert)))
    });
    
    group.finish();
}

fn tuple_benchmarks(c: &mut Criterion) {
    use zenith_core::schema::{Column, Relation, ReplicaIdentity};
    
    // Create test relation
    let relation = Relation {
        id: 16384,
        namespace: "public".to_string(),
        name: "users".to_string(),
        replica_identity: ReplicaIdentity::Full,
        columns: vec![
            Column { name: "id".to_string(), flags: 0x01, type_oid: 23, type_modifier: -1 },
            Column { name: "name".to_string(), flags: 0x00, type_oid: 25, type_modifier: -1 },
            Column { name: "email".to_string(), flags: 0x00, type_oid: 25, type_modifier: -1 },
            Column { name: "active".to_string(), flags: 0x00, type_oid: 16, type_modifier: -1 },
            Column { name: "score".to_string(), flags: 0x00, type_oid: 23, type_modifier: -1 },
        ],
        primary_key_indices: vec![0],
    };
    
    // Create test tuple
    let tuple = TupleData {
        columns: vec![
            ColumnValue::Text("12345".to_string()),
            ColumnValue::Text("John Doe".to_string()),
            ColumnValue::Text("john.doe@example.com".to_string()),
            ColumnValue::Text("t".to_string()),
            ColumnValue::Text("95".to_string()),
        ],
    };
    
    c.bench_function("tuple_to_json", |b| {
        b.iter(|| tuple.to_json(black_box(&relation)))
    });
    
    c.bench_function("tuple_extract_pk", |b| {
        b.iter(|| tuple.extract_pk(black_box(&relation)))
    });
}

criterion_group!(benches, parser_benchmarks, tuple_benchmarks);
criterion_main!(benches);

