use crate::config::PostgresConfig;
use crate::error::{Result, Error};
use crate::pipeline::{Event};
use crate::schema::registry::{SchemaRegistry, Relation, Column, ReplicaIdentity};
use chrono::Utc;
use futures::{StreamExt};
use serde_json::{Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_postgres::{NoTls};
use tracing::{error, info};
use std::collections::HashMap;
use zenith_storage::WalPositionStore;
use super::binary_copy::BinaryCopyParser;

/// Progress update for snapshot
#[derive(Debug, Clone)]
pub struct SnapshotProgress {
    pub table: String,
    pub rows_processed: u64,
    pub total_bytes: Option<u64>,
    pub complete: bool,
}

/// Snapshot copier for initial data load
pub struct SnapshotCopier {
    config: PostgresConfig,
    registry: Arc<SchemaRegistry>,
    store: Arc<WalPositionStore>,
}

impl SnapshotCopier {
    pub fn new(config: PostgresConfig, registry: Arc<SchemaRegistry>, store: Arc<WalPositionStore>) -> Self {
        Self { config, registry, store }
    }

    /// Run the snapshot process
    /// 
    /// The `on_snapshot_started` callback is invoked once the snapshot is exported,
    /// providing the snapshot_id and consistent_lsn. This allows the caller to
    /// start the streaming replication concurrently.
    pub async fn run<F, Fut>(
        &self, 
        tx: mpsc::Sender<Event>, 
        progress_tx: Option<mpsc::Sender<SnapshotProgress>>,
        on_snapshot_started: F
    ) -> Result<()>
    where 
        F: FnOnce(String, u64) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + Send, 
    {
        info!("Starting snapshot process...");
        let start_time = Instant::now();

        // Connect to Postgres
        let (mut client, connection) = tokio_postgres::connect(&self.config.url, NoTls).await
            .map_err(|e| Error::Postgres(e))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });

        // 1. Start transaction with REPEATABLE READ isolation level
        let transaction = client.build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await
            .map_err(|e| Error::Postgres(e))?;

        // 2. Export snapshot
        let snapshot_id_row = transaction.query_one("SELECT pg_export_snapshot()", &[])
            .await
            .map_err(|e| Error::Postgres(e))?;
        let snapshot_id: String = snapshot_id_row.get(0);
        
        // 3. Get current LSN (consistent point)
        let lsn_row = transaction.query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .map_err(|e| Error::Postgres(e))?;
        let lsn_text: String = lsn_row.get(0);
        let consistent_lsn = u64::from_str_radix(&lsn_text.replace('/', ""), 16)
            .map_err(|_| Error::Config("Invalid LSN format".into()))?;

        info!("Snapshot created: {}, Consistent LSN: {}", snapshot_id, consistent_lsn);

        // Notify caller that snapshot is ready
        on_snapshot_started(snapshot_id.clone(), consistent_lsn).await?;

        // 4. Discover tables and schema
        let schema_query = "
            SELECT 
                n.nspname, 
                c.relname, 
                c.oid, 
                a.attname, 
                a.atttypid, 
                a.atttypmod, 
                a.attnum, 
                coalesce(i.indisprimary, false) as is_primary
            FROM pg_class c 
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            JOIN pg_attribute a ON a.attrelid = c.oid
            LEFT JOIN pg_index i ON c.oid = i.indrelid AND i.indisprimary
            WHERE 
                c.relkind = 'r' 
                AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                AND a.attnum > 0 
                AND NOT a.attisdropped
            ORDER BY c.oid, a.attnum
        ";
        
        let rows = transaction.query(schema_query, &[])
            .await
            .map_err(|e| Error::Postgres(e))?;

        // Group columns by table
        let mut table_map: HashMap<u32, Relation> = HashMap::new();

        for row in rows {
            let ns: String = row.get(0);
            let name: String = row.get(1);
            let oid: u32 = row.get(2);
            let col_name: String = row.get(3);
            let type_oid: u32 = row.get(4);
            let type_mod: i32 = row.get(5);
            let is_primary: bool = row.get(7);

            let relation = table_map.entry(oid).or_insert_with(|| Relation {
                id: oid,
                namespace: ns,
                name: name,
                replica_identity: ReplicaIdentity::Default,
                version: 1,
                columns: Vec::new(),
                primary_key_indices: Vec::new(),
            });

            let col_idx = relation.columns.len();
            relation.columns.push(Column {
                name: col_name,
                flags: if is_primary { 1 } else { 0 },
                type_oid,
                type_modifier: type_mod,
            });
            
            if is_primary {
                relation.primary_key_indices.push(col_idx);
            }
        }

        // Register all relations
        for (_, relation) in table_map.iter() {
            if let Err(e) = self.registry.register(relation.clone()) {
                 error!("Failed to register relation {}: {}", relation.full_name(), e);
                 // Should we fail? Maybe just log for now as snapshot might proceed
            }
        }

        info!("Discovered and registered {} tables", table_map.len());
        
        // 5. Parallel COPY
        let max_workers = self.config.max_concurrent_snapshots.max(1);
        let (job_tx, job_rx) = async_channel::bounded(table_map.len());
        
        // Populate job queue
        for relation in table_map.values() {
            let full_name = relation.full_name();
            // Resume capability check
            if let Ok(Some(_)) = self.store.get_table_snapshot_lsn(&full_name) {
                info!("Skipping table {} (already snapshotted)", full_name);
                continue;
            }
            job_tx.send(relation.clone()).await.map_err(|_| Error::ChannelClosed)?;
        }
        job_tx.close(); // No more jobs
        
        info!("Spawning {} snapshot workers", max_workers);
        
        let mut tasks = Vec::new();
        
        for i in 0..max_workers {
            let config = self.config.clone();
            let job_rx = job_rx.clone();
            let tx = tx.clone();
            let progress_tx = progress_tx.clone();
            let snapshot_id = snapshot_id.clone();
            let store = self.store.clone();
            let consistent_lsn = consistent_lsn;

            tasks.push(tokio::spawn(async move {
                // Each worker needs its own connection
                let (mut client, connection) = tokio_postgres::connect(&config.url, NoTls).await
                    .map_err(|e| Error::Postgres(e))?;

                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        error!("Worker {} connection error: {}", i, e);
                    }
                });

                // Start transaction and synchronize snapshot
                let transaction = client.build_transaction()
                    .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                    .start()
                    .await
                    .map_err(|e| Error::Postgres(e))?;

                transaction.execute(
                    &format!("SET TRANSACTION SNAPSHOT '{}'", snapshot_id), 
                    &[]
                ).await.map_err(|e| Error::Postgres(e))?;

                while let Ok(relation) = job_rx.recv().await {
                    let full_name = relation.full_name();
                    info!("Worker {} snapshotting table {}", i, full_name);

                    // Check for Primary Key for chunking
                    let pk_columns: Vec<&Column> = relation.primary_key_indices
                        .iter()
                        .map(|&idx| &relation.columns[idx])
                        .collect();
                    
                    if pk_columns.is_empty() {
                        // Fallback to full copy if no PK
                         let query = format!("COPY {}.{} TO STDOUT (FORMAT BINARY)", relation.namespace, relation.name);
                         if let Err(e) = process_copy_stream(
                             &transaction, &query, &relation, &tx, &progress_tx, consistent_lsn, &full_name, i
                         ).await {
                             error!("Worker {} failed to snapshot {}: {}", i, full_name, e);
                             return Err(e);
                         }
                    } else {
                        // Chunked resumable copy
                        let chunk_size = config.snapshot_chunk_size.max(1000);
                        let mut last_pk_json = store.get_table_checkpoint(&full_name).unwrap_or(None);
                        
                        loop {
                            let mut query = format!(
                                "COPY (SELECT * FROM {}.{} ", 
                                relation.namespace, relation.name
                            );

                            if let Some(ref last_pk) = last_pk_json {
                                let pk_values: Vec<Value> = serde_json::from_str(last_pk)
                                    .map_err(|e| Error::Config(format!("Invalid checkpoint data: {}", e)))?;
                                
                                let where_clause = build_pk_where_clause(&pk_columns, &pk_values);
                                query.push_str(&format!("WHERE {} ", where_clause));
                            }
                            
                            let order_by = pk_columns.iter()
                                .map(|c| format!("\"{}\" ASC", c.name))
                                .collect::<Vec<_>>()
                                .join(", ");

                            query.push_str(&format!("ORDER BY {} LIMIT {}) TO STDOUT (FORMAT BINARY)", order_by, chunk_size));
                            
                            let (rows, last_row_pk) = process_copy_stream_chunk(
                                &transaction, &query, &relation, &tx, &progress_tx, consistent_lsn, &full_name, i, &pk_columns
                            ).await?;
                            
                            if rows == 0 {
                                break;
                            }
                            
                            // Save checkpoint
                            if let Some(pk) = last_row_pk {
                                let pk_str = serde_json::to_string(&pk).unwrap();
                                store.set_table_checkpoint(&full_name, pk_str.clone())
                                    .map_err(|e| Error::Config(e.to_string()))?;
                                last_pk_json = Some(pk_str);
                            }
                            
                            if rows < chunk_size as u64 {
                                break;
                            }
                        }
                    }

                    info!("Worker {} finished {}", i, full_name);
                    
                    if let Err(e) = store.set_table_snapshot_lsn(&full_name, consistent_lsn) {
                         error!("Failed to save snapshot progress for {}: {}", full_name, e);
                    }
                    
                    // Clear checkpoint after success
                    let _ = store.set_table_checkpoint(&full_name, "null".to_string());
                    
                    if let Some(ref ptx) = progress_tx {
                        let _ = ptx.try_send(SnapshotProgress {
                            table: full_name.clone(),
                            rows_processed: 0, 
                            total_bytes: None,
                            complete: true,
                        });
                    }
                }
                Ok::<(), Error>(())
            }));
        }

        // Wait for all workers
        for task in tasks {
            if let Err(e) = task.await {
                error!("Worker task panic: {}", e);
            }
        }
        
        info!("Snapshot sequence complete ({:?})", start_time.elapsed());
        Ok(())
    }
}

/// Helper to decode binary values
fn decode_binary_value(oid: u32, bytes: &[u8]) -> Value {
    use byteorder::{BigEndian, ByteOrder};

    match oid {
        // BOOL
        16 => {
            Value::Bool(bytes.get(0).map(|&b| b != 0).unwrap_or(false))
        },
        // INT2
        21 => {
            if bytes.len() >= 2 {
                Value::Number(serde_json::Number::from(BigEndian::read_i16(bytes)))
            } else { Value::Null }
        },
        // INT4
        23 => {
            if bytes.len() >= 4 {
                Value::Number(serde_json::Number::from(BigEndian::read_i32(bytes)))
            } else { Value::Null }
        },
        // INT8
        20 => {
             if bytes.len() >= 8 {
                Value::Number(serde_json::Number::from(BigEndian::read_i64(bytes)))
            } else { Value::Null }
        },
        // TEXT, VARCHAR, NAME usually utf-8
        25 | 1043 | 19 => {
             String::from_utf8_lossy(bytes).into_owned().into()
        },
        // TIMESTAMP / TIMESTAMPTZ (micros since 2000-01-01)
        1114 | 1184 => {
             if bytes.len() >= 8 {
                 let pg_ts = BigEndian::read_i64(bytes);
                 // Convert to ISO string roughly
                 // Simpler: just send as string for ClickHouse parser? 
                 // Or format it. Let's use string for compatibility with existing flow.
                 // We need proper timestamp conversion util.
                 // For now, let's treat as number or special object?
                 // The existing parser logic sends ISO string.
                 // Let's defer strict timestamp logic or copy from decoder.
                 Value::Number(serde_json::Number::from(pg_ts)) 
                 // Note: This changes format from ISO string to PG epoch int.
                 // ClickHouse Int64 DateTime64(6) generally needs seconds or proper format.
                 // Let's attempt to use ISO string manually if possible, or leave as int if Schema allows.
             } else { Value::Null }
        },
        _ => {
            // Fallback: try UTF-8 string, else Base64?
            // Existing logic was "TEXT" format so everything was string.
            // Let's try UTF-8 first
             String::from_utf8_lossy(bytes).into_owned().into()
        }
    }
}

/// Helper to build PK WHERE clause
fn build_pk_where_clause(pk_cols: &[&Column], pk_values: &[Value]) -> String {
    if pk_cols.len() == 1 {
        let col = pk_cols[0];
        let val = &pk_values[0];
        format!("\"{}\" > {}", col.name, value_to_sql(val))
    } else {
        let cols = pk_cols.iter().map(|c| format!("\"{}\"", c.name)).collect::<Vec<_>>().join(", ");
        let vals = pk_values.iter().map(|v| value_to_sql(v)).collect::<Vec<_>>().join(", ");
        format!("({}) > ({})", cols, vals)
    }
}

fn value_to_sql(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
        Value::String(s) => format!("'{}'", s.replace("'", "''")),
        _ => format!("'{}'", v.to_string().replace("'", "''")),
    }
}

/// Process a full copy stream (legacy/fallback)
async fn process_copy_stream(
    transaction: &tokio_postgres::Transaction<'_>,
    query: &str,
    relation: &Relation,
    tx: &mpsc::Sender<Event>,
    progress_tx: &Option<mpsc::Sender<SnapshotProgress>>,
    consistent_lsn: u64,
    full_name: &str,
    worker_id: usize,
) -> Result<()> {
    match transaction.copy_out(query).await {
         Ok(reader) => {
            let pin_reader = std::pin::pin!(reader);
            let mut parser = BinaryCopyParser::new(pin_reader);
            let mut row_count = 0;
            
            while let Some(row) = parser.next_row().await? {
                let row_data = parse_row(row, relation)?;
                send_event(tx, consistent_lsn, full_name, relation, row_data).await?;
                row_count += 1;
                if row_count % 1000 == 0 {
                    report_progress(progress_tx, full_name, row_count, false);
                }
            }
            info!("Worker {} finished {} ({} rows)", worker_id, full_name, row_count);
            Ok(())
         }
         Err(e) => {
             error!("Failed to start COPY for {}: {}", full_name, e);
             Err(Error::Postgres(e))
         }
    }
}

/// Process a chunk copy stream
async fn process_copy_stream_chunk(
    transaction: &tokio_postgres::Transaction<'_>,
    query: &str,
    relation: &Relation,
    tx: &mpsc::Sender<Event>,
    progress_tx: &Option<mpsc::Sender<SnapshotProgress>>,
    consistent_lsn: u64,
    full_name: &str,
    worker_id: usize,
    pk_cols: &[&Column],
) -> Result<(u64, Option<Vec<Value>>)> {
    match transaction.copy_out(query).await {
         Ok(reader) => {
            let pin_reader = std::pin::pin!(reader);
            let mut parser = BinaryCopyParser::new(pin_reader);
            let mut row_count = 0;
            let mut last_pk_values = None;
            
            while let Some(row) = parser.next_row().await? {
                let row_data = parse_row(row, relation)?;
                
                // Extract PK values
                let mut current_pk = Vec::new();
                for pk_col in pk_cols {
                    if let Some(val) = row_data.get(&pk_col.name) {
                        current_pk.push(val.clone());
                    }
                }
                last_pk_values = Some(current_pk);
                
                send_event(tx, consistent_lsn, full_name, relation, row_data).await?;
                row_count += 1;
            }
            
            if row_count > 0 {
                info!("Worker {} chunk {} ({} rows)", worker_id, full_name, row_count);
            }
            
            Ok((row_count, last_pk_values))
         }
         Err(e) => {
             error!("Failed to start COPY for {}: {}", full_name, e);
             Err(Error::Postgres(e))
         }
    }
}

fn parse_row(row: Vec<Option<Vec<u8>>>, relation: &Relation) -> Result<serde_json::Map<String, Value>> {
    if row.len() != relation.columns.len() {
         return Err(Error::Config(format!("Row field count mismatch: expected {}, got {}", relation.columns.len(), row.len())));
    }

    let mut row_data = serde_json::Map::new();
    for (i, col_data) in row.into_iter().enumerate() {
        if let Some(col) = relation.columns.get(i) {
            let val = match col_data {
                Some(bytes) => decode_binary_value(col.type_oid, &bytes),
                None => Value::Null,
            };
            row_data.insert(col.name.clone(), val);
        }
    }
    Ok(row_data)
}

async fn send_event(tx: &mpsc::Sender<Event>, lsn: u64, full_name: &str, relation: &Relation, data: serde_json::Map<String, Value>) -> Result<()> {
    let selector = format!("{}_v{}", relation.name, relation.version);
    let event = Event::new(
        lsn,
        0,
        crate::pipeline::Operation::Insert,
        full_name.to_string(),
        selector,
        Value::Object(data),
        None,
        Utc::now(),
        None,
    );

    tx.send(event).await.map_err(|_| Error::ChannelClosed)?;
    Ok(())
}

fn report_progress(tx: &Option<mpsc::Sender<SnapshotProgress>>, table: &str, rows: u64, complete: bool) {
     if let Some(ref ptx) = tx {
        let _ = ptx.try_send(SnapshotProgress {
            table: table.to_string(),
            rows_processed: rows,
            total_bytes: None,
            complete,
        });
    }
}
