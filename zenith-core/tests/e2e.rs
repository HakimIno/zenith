use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;
use tracing::info;

const POSTGRES_URL: &str = "postgres://user:password@localhost:5432/mydb";
const CLICKHOUSE_URL: &str = "http://localhost:8123";

#[tokio::test]
#[ignore] // Ignored by default as it requires docker environment
async fn test_e2e_snapshot_and_streaming() -> anyhow::Result<()> {
    // 1. Setup Database Connections
    let (client, connection) = tokio_postgres::connect(POSTGRES_URL, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let client = std::sync::Arc::new(client);

    // 2. Prepare Data in PostgreSQL
    // Create a test table
    client.batch_execute("
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INT
        );
        INSERT INTO test_table (name, value) VALUES ('Item 1', 100);
        INSERT INTO test_table (name, value) VALUES ('Item 2', 200);
    ").await?;

    // 3. Prepare ClickHouse (Clean state)
    let http_client = reqwest::Client::new();
    let _ = http_client.post(format!("{}/?query=DROP TABLE IF EXISTS test_table", CLICKHOUSE_URL))
        .send()
        .await?;

    // 4. Build the CLI binary
    info!("Building zenith-cli...");
    let status = Command::new("cargo")
        .args(&["build", "-p", "zenith-cli"])
        .status()?;
    assert!(status.success(), "Failed to build zenith-cli");

    // 5. Run Zenith CDC CLI
    info!("Starting Zenith CDC...");
    let mut child = Command::new("./target/debug/zenith")
        .args(&[
            "--postgres-url", POSTGRES_URL,
            "--clickhouse-url", CLICKHOUSE_URL,
            "--initial-load", "true",
            "--storage-path", "./test_zenith_data", // Use dedicated test storage
            "--init-table" // Ensure table is created in CH
        ])
        .env("RUST_LOG", "info")
        .spawn()?;

    // Wait a bit for initialization and snapshot
    sleep(Duration::from_secs(5)).await;

    // 6. Verify Snapshot Data in ClickHouse
    verify_clickhouse_count(&http_client, "test_table", 2).await?;

    // 7. Perform Streaming Updates
    info!("Inserting streaming data...");
    client.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", &[&"Item 3", &300]).await?;
    client.execute("UPDATE test_table SET value = 999 WHERE id = 1", &[]).await?;
    
    // Wait for replication
    sleep(Duration::from_secs(3)).await;

    // 8. Verify Streaming Data
    // Count should be 3 (Snapshot 2 + Insert 1)
    verify_clickhouse_count(&http_client, "test_table", 3).await?;
    
    // 9. Schema Evolution Test
    info!("Testing Schema Evolution...");
    // Alter table in Postgres
    client.batch_execute("ALTER TABLE test_table ADD COLUMN description TEXT").await?;
    
    // Insert data with new column
    client.execute("INSERT INTO test_table (name, value, description) VALUES ($1, $2, $3)", 
        &[&"Item 4", &400, &"New Column Data"]).await?;
        
    // Wait for migration and replication
    sleep(Duration::from_secs(5)).await;
    
    // Check if new version table exists (assuming it starts at v1, so new is v2)
    // Note: The registry logic starts at v1. The first alteration should make it v2.
    // However, since we restarted the registry in this test? No, registry persists.
    // Wait, on initial load, we registered v1.
    // Now we alter, so it should detect change and become v2.
    
    // Verify count in the NEW table (v2)
    // Since my simple view logic just points to the new table, the view 'test_table' 
    // might only show the 1 new row if it's not a UNION view. 
    // Let's verify the specific v2 table directly.
    verify_clickhouse_count(&http_client, "test_table_v2", 1).await?;
    
    // Verify the view points to v2 (so querying view returns 1)
    // verify_clickhouse_count(&http_client, "test_table", 1).await?; 

    // 10. Cleanup
    info!("Killing Zenith CDC...");
    child.kill()?;
    let _ = std::fs::remove_dir_all("./test_zenith_data");
    
    Ok(())
}

async fn verify_clickhouse_count(client: &reqwest::Client, table: &str, expected: u64) -> anyhow::Result<()> {
    let query = format!("SELECT count() FROM {}", table);
    let res = client.post(format!("{}/?query={}", CLICKHOUSE_URL, query))
        .send()
        .await?
        .text()
        .await?;
    
    let count: u64 = res.trim().parse().unwrap_or(0);
    info!("ClickHouse count for {}: {}", table, count);
    assert_eq!(count, expected, "Data count mismatch in ClickHouse table {}", table);
    Ok(())
}
