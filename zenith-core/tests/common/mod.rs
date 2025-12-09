use anyhow::Result;
use reqwest::Client;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::{NoTls, Client as PgClient};

pub const POSTGRES_URL: &str = "postgres://user:password@localhost:5432/mydb";
pub const CLICKHOUSE_URL: &str = "http://localhost:8123";

/// Setup PostgreSQL client
pub async fn setup_postgres() -> Result<PgClient> {
    let (client, connection) = tokio_postgres::connect(POSTGRES_URL, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });
    
    Ok(client)
}

/// Setup ClickHouse HTTP client
pub fn setup_clickhouse() -> Client {
    Client::new()
}

/// Generate test data for large tables
pub fn generate_large_dataset(rows: usize) -> Vec<(String, i32)> {
    (0..rows)
        .map(|i| (format!("data_{}", i), i as i32))
        .collect()
}

/// Insert large dataset into PostgreSQL
pub async fn insert_large_dataset(
    client: &PgClient,
    table: &str,
    data: &[(String, i32)],
) -> Result<()> {
    for chunk in data.chunks(1000) {
        let mut query = format!("INSERT INTO {} (data, value) VALUES ", table);
        let values: Vec<String> = chunk
            .iter()
            .enumerate()
            .map(|(i, (data, value))| format!("('{}', {})", data, value))
            .collect();
        query.push_str(&values.join(", "));
        
        client.execute(&query, &[]).await?;
    }
    Ok(())
}

/// Verify row count in ClickHouse
pub async fn verify_clickhouse_count(
    client: &Client,
    table: &str,
    expected: u64,
) -> Result<()> {
    let query = format!("SELECT count() FROM {}", table);
    let res = client
        .post(format!("{}/?query={}", CLICKHOUSE_URL, query))
        .send()
        .await?
        .text()
        .await?;
    
    let count: u64 = res.trim().parse().unwrap_or(0);
    
    if count != expected {
        anyhow::bail!(
            "Count mismatch in {}: expected {}, got {}",
            table,
            expected,
            count
        );
    }
    
    Ok(())
}

/// Wait for data to be replicated to ClickHouse
pub async fn wait_for_replication(
    client: &Client,
    table: &str,
    expected_count: u64,
    timeout_secs: u64,
) -> Result<()> {
    let start = std::time::Instant::now();
    
    loop {
        if start.elapsed().as_secs() > timeout_secs {
            anyhow::bail!("Timeout waiting for replication");
        }
        
        match verify_clickhouse_count(client, table, expected_count).await {
            Ok(_) => return Ok(()),
            Err(_) => sleep(Duration::from_millis(500)).await,
        }
    }
}

/// Query ClickHouse and return results
pub async fn query_clickhouse(client: &Client, query: &str) -> Result<String> {
    let res = client
        .post(format!("{}/?query={}", CLICKHOUSE_URL, query))
        .send()
        .await?
        .text()
        .await?;
    
    Ok(res)
}

/// Start Zenith CDC process
pub fn start_zenith(args: &[&str]) -> Result<Child> {
    let mut cmd = Command::new("./target/release/zenith");
    cmd.args(args);
    cmd.env("RUST_LOG", "info");
    
    let child = cmd.spawn()?;
    Ok(child)
}

/// Kill Zenith process gracefully
pub fn kill_zenith(mut child: Child) -> Result<()> {
    child.kill()?;
    child.wait()?;
    Ok(())
}

/// Clean up test data
pub fn cleanup_test_data() {
    let _ = std::fs::remove_dir_all("./test_zenith_data");
    let _ = std::fs::remove_dir_all("./dlq");
}

/// Check if DLQ file exists and has content
pub fn verify_dlq_file_exists(path: &str) -> Result<bool> {
    if let Ok(metadata) = std::fs::metadata(path) {
        Ok(metadata.len() > 0)
    } else {
        Ok(false)
    }
}

/// Read DLQ file content
pub fn read_dlq_file(path: &str) -> Result<String> {
    Ok(std::fs::read_to_string(path)?)
}

/// Create a table with composite primary key
pub async fn create_composite_pk_table(client: &PgClient) -> Result<()> {
    client
        .batch_execute(
            "
            DROP TABLE IF EXISTS test_composite_pk;
            CREATE TABLE test_composite_pk (
                user_id INTEGER,
                order_id INTEGER,
                amount DECIMAL(10,2),
                PRIMARY KEY (user_id, order_id)
            );
            ",
        )
        .await?;
    Ok(())
}

/// Insert data into composite PK table
pub async fn insert_composite_pk_data(
    client: &PgClient,
    rows: usize,
) -> Result<()> {
    for i in 0..rows {
        let user_id = (i % 100) as i32;
        let order_id = i as i32;
        let amount = (i as f64 * 10.5) as f64;
        
        client
            .execute(
                "INSERT INTO test_composite_pk (user_id, order_id, amount) VALUES ($1, $2, $3)",
                &[&user_id, &order_id, &amount],
            )
            .await?;
    }
    Ok(())
}
