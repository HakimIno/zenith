mod common;

use common::*;
use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

/// Test 1: Basic Snapshot and Streaming
#[tokio::test]
#[ignore]
async fn test_basic_snapshot_and_streaming() -> Result<()> {
    println!("\n🧪 Test: Basic Snapshot and Streaming");
    
    let pg_client = setup_postgres().await?;
    let ch_client = setup_clickhouse();
    
    // Create test table
    pg_client.batch_execute("
        DROP TABLE IF EXISTS test_basic;
        CREATE TABLE test_basic (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER
        );
        INSERT INTO test_basic (name, value) VALUES ('Alice', 100), ('Bob', 200);
    ").await?;
    
    // Start Zenith
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    // Wait for snapshot
    sleep(Duration::from_secs(5)).await;
    
    // Verify snapshot data
    verify_clickhouse_count(&ch_client, "test_basic_v1", 2).await?;
    
    // Insert streaming data
    pg_client.execute("INSERT INTO test_basic (name, value) VALUES ($1, $2)", &[&"Charlie", &300]).await?;
    
    // Wait for replication
    wait_for_replication(&ch_client, "test_basic_v1", 3, 10).await?;
    
    // Cleanup
    kill_zenith(zenith)?;
    cleanup_test_data();
    
    println!("✅ Basic test passed");
    Ok(())
}

/// Test 2: Parallel Snapshot Performance
#[tokio::test]
#[ignore]
async fn test_parallel_snapshot() -> Result<()> {
    println!("\n🧪 Test: Parallel Snapshot");
    
    let pg_client = setup_postgres().await?;
    let ch_client = setup_clickhouse();
    
    // Create multiple large tables
    for i in 1..=5 {
        let table_name = format!("test_parallel_{}", i);
        pg_client.batch_execute(&format!("
            DROP TABLE IF EXISTS {};
            CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                data TEXT,
                value INTEGER
            );
        ", table_name, table_name)).await?;
        
        // Insert 10K rows per table
        let data = generate_large_dataset(10_000);
        insert_large_dataset(&pg_client, &table_name, &data).await?;
    }
    
    let start = std::time::Instant::now();
    
    // Start Zenith with parallel snapshots
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    // Wait for all snapshots to complete
    sleep(Duration::from_secs(30)).await;
    
    let duration = start.elapsed();
    
    // Verify all tables
    for i in 1..=5 {
        let table_name = format!("test_parallel_{}_v1", i);
        verify_clickhouse_count(&ch_client, &table_name, 10_000).await?;
    }
    
    println!("✅ Parallel snapshot completed in {:?}", duration);
    println!("   Throughput: ~{} rows/sec", (50_000 as f64 / duration.as_secs_f64()) as u64);
    
    kill_zenith(zenith)?;
    cleanup_test_data();
    
    Ok(())
}

/// Test 3: Row-level Resumability
#[tokio::test]
#[ignore]
async fn test_resumability() -> Result<()> {
    println!("\n🧪 Test: Row-level Resumability");
    
    let pg_client = setup_postgres().await?;
    let ch_client = setup_clickhouse();
    
    // Create large table
    pg_client.batch_execute("
        DROP TABLE IF EXISTS test_resumable;
        CREATE TABLE test_resumable (
            id SERIAL PRIMARY KEY,
            data TEXT,
            value INTEGER
        );
    ").await?;
    
    // Insert 50K rows
    let data = generate_large_dataset(50_000);
    insert_large_dataset(&pg_client, "test_resumable", &data).await?;
    
    // Start Zenith
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    // Let it snapshot partially (5 seconds should process ~10-20K rows)
    sleep(Duration::from_secs(5)).await;
    
    // Kill process mid-snapshot
    println!("   Killing process mid-snapshot...");
    kill_zenith(zenith)?;
    
    // Check partial progress
    let partial_count = query_clickhouse(&ch_client, "SELECT count() FROM test_resumable_v1")
        .await?
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    
    println!("   Partial progress: {} rows", partial_count);
    assert!(partial_count > 0 && partial_count < 50_000, "Should have partial data");
    
    // Restart Zenith
    println!("   Restarting to resume...");
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    // Wait for completion
    wait_for_replication(&ch_client, "test_resumable_v1", 50_000, 60).await?;
    
    // Verify no duplicates (count should be exactly 50K)
    verify_clickhouse_count(&ch_client, "test_resumable_v1", 50_000).await?;
    
    println!("✅ Resumability test passed - resumed from {} rows", partial_count);
    
    kill_zenith(zenith)?;
    cleanup_test_data();
    
    Ok(())
}

/// Test 4: Dead Letter Queue
#[tokio::test]
#[ignore]
async fn test_dead_letter_queue() -> Result<()> {
    println!("\n🧪 Test: Dead Letter Queue");
    
    let pg_client = setup_postgres().await?;
    let ch_client = setup_clickhouse();
    
    // Create test table
    pg_client.batch_execute("
        DROP TABLE IF EXISTS test_dlq;
        CREATE TABLE test_dlq (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER
        );
    ").await?;
    
    // Start Zenith with DLQ enabled
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    sleep(Duration::from_secs(3)).await;
    
    // Insert valid data
    pg_client.execute("INSERT INTO test_dlq (name, value) VALUES ($1, $2)", &[&"Valid", &100]).await?;
    
    // Wait for replication
    wait_for_replication(&ch_client, "test_dlq_v1", 1, 10).await?;
    
    // Manually create ClickHouse table with wrong schema to trigger DLQ
    ch_client.post(format!("{}/?query=DROP TABLE IF EXISTS test_dlq_v1", CLICKHOUSE_URL))
        .send().await?;
    
    ch_client.post(format!("{}/?query=CREATE TABLE test_dlq_v1 (id UInt32, wrong_column String) ENGINE = MergeTree() ORDER BY id", CLICKHOUSE_URL))
        .send().await?;
    
    // Insert data that will fail
    pg_client.execute("INSERT INTO test_dlq (name, value) VALUES ($1, $2)", &[&"WillFail", &200]).await?;
    
    // Wait for DLQ to catch the error
    sleep(Duration::from_secs(5)).await;
    
    // Verify DLQ file exists
    let dlq_exists = verify_dlq_file_exists("./dlq/failed_events.jsonl")?;
    assert!(dlq_exists, "DLQ file should exist");
    
    let dlq_content = read_dlq_file("./dlq/failed_events.jsonl")?;
    println!("   DLQ content preview: {}", &dlq_content[..dlq_content.len().min(200)]);
    
    assert!(dlq_content.contains("error"), "DLQ should contain error information");
    
    println!("✅ DLQ test passed");
    
    kill_zenith(zenith)?;
    cleanup_test_data();
    
    Ok(())
}

/// Test 5: Composite Primary Key Resumability
#[tokio::test]
#[ignore]
async fn test_composite_pk_resumability() -> Result<()> {
    println!("\n🧪 Test: Composite PK Resumability");
    
    let pg_client = setup_postgres().await?;
    let ch_client = setup_clickhouse();
    
    // Create composite PK table
    create_composite_pk_table(&pg_client).await?;
    
    // Insert 20K rows
    insert_composite_pk_data(&pg_client, 20_000).await?;
    
    // Start Zenith
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    // Partial snapshot
    sleep(Duration::from_secs(3)).await;
    kill_zenith(zenith)?;
    
    // Restart and complete
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    wait_for_replication(&ch_client, "test_composite_pk_v1", 20_000, 60).await?;
    
    println!("✅ Composite PK resumability test passed");
    
    kill_zenith(zenith)?;
    cleanup_test_data();
    
    Ok(())
}

/// Test 6: High-Volume Stress Test
#[tokio::test]
#[ignore]
async fn test_high_volume_stress() -> Result<()> {
    println!("\n🧪 Test: High-Volume Stress");
    
    let pg_client = setup_postgres().await?;
    let ch_client = setup_clickhouse();
    
    // Create test table
    pg_client.batch_execute("
        DROP TABLE IF EXISTS test_stress;
        CREATE TABLE test_stress (
            id SERIAL PRIMARY KEY,
            data TEXT,
            value INTEGER
        );
    ").await?;
    
    // Start Zenith
    let mut zenith = start_zenith(&[
        "--postgres-url", POSTGRES_URL,
        "--clickhouse-url", CLICKHOUSE_URL,
        "--storage-path", "./test_zenith_data",
    ])?;
    
    sleep(Duration::from_secs(3)).await;
    
    let start = std::time::Instant::now();
    
    // Rapidly insert 100K rows
    println!("   Inserting 100K rows...");
    for batch in 0..100 {
        let mut query = "INSERT INTO test_stress (data, value) VALUES ".to_string();
        let values: Vec<String> = (0..1000)
            .map(|i| format!("('data_{}', {})", batch * 1000 + i, i))
            .collect();
        query.push_str(&values.join(", "));
        
        pg_client.execute(&query, &[]).await?;
    }
    
    let insert_duration = start.elapsed();
    println!("   Insert completed in {:?}", insert_duration);
    
    // Wait for replication
    wait_for_replication(&ch_client, "test_stress_v1", 100_000, 120).await?;
    
    let total_duration = start.elapsed();
    let throughput = 100_000.0 / total_duration.as_secs_f64();
    
    println!("✅ Stress test passed");
    println!("   Total time: {:?}", total_duration);
    println!("   Throughput: {:.0} events/sec", throughput);
    
    kill_zenith(zenith)?;
    cleanup_test_data();
    
    Ok(())
}
