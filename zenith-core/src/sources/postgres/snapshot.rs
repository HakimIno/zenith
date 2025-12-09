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
        
        // 5. COPY each table
        for relation in table_map.values() {
            let full_name = relation.full_name();
            
            // Resume capability: Check if already snapshotted
            if let Ok(Some(_)) = self.store.get_table_snapshot_lsn(&full_name) {
                info!("Skipping table {} (already snapshotted)", full_name);
                continue;
            }

            info!("Snapshotting table {}", full_name);
            
            // Format TEXT is roughly TSV with \N for nulls
            let query = format!("COPY {}.{} TO STDOUT (FORMAT TEXT)", relation.namespace, relation.name);
            
            match transaction.copy_out(&query).await {
                Ok(reader) => {
                    let pin_reader = std::pin::pin!(reader);
                    // Use LinesStream or similar if available, or just read chunks and split
                    // Since we don't have a framed reader easily handy without more deps,
                    // we will implement a simple line buffer.
                 
                    let mut reader_stream = pin_reader;
                    let mut buffer = Vec::new(); // Incomplete line buffer
                    let mut row_count = 0;
                    
                    while let Some(chunk_res) = reader_stream.next().await {
                        let chunk = chunk_res.map_err(|e: tokio_postgres::Error| Error::Postgres(e))?;
                        buffer.extend_from_slice(&chunk);
                        
                        // Process complete lines
                        while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                            let line_bytes: Vec<u8> = buffer.drain(..=pos).collect();
                            let line = std::str::from_utf8(&line_bytes[..line_bytes.len()-1]) // remove \n
                                .unwrap_or(""); 
                                
                            // Parse fields
                            let fields: Vec<&str> = line.split('\t').collect();
                            
                            if fields.len() != relation.columns.len() {
                                // Mismatch
                                continue;
                            }
                            
                            let mut row_data = serde_json::Map::new();
                            for (i, field) in fields.iter().enumerate() {
                                if let Some(col) = relation.columns.get(i) {
                                    let val = if *field == "\\N" {
                                        Value::Null
                                    } else {
                                        // TODO: Parse types correctly
                                        Value::String(field.to_string())
                                    };
                                    row_data.insert(col.name.clone(), val);
                                }
                            }
                            
                            // Send Event
                            let selector = format!("{}_v{}", relation.name, relation.version);

                             let event = Event::new(
                                consistent_lsn, // lsn
                                0, // xid
                                crate::pipeline::Operation::Insert, // op
                                full_name.clone(), // table
                                selector, // selector
                                Value::Object(row_data), // data
                                None, // before
                                Utc::now(), // ts
                                None, // pk
                            );

                            if let Err(e) = tx.send(event).await {
                                error!("Failed to send snapshot event: {}", e);
                                return Err(Error::ChannelClosed);
                            }
                            row_count += 1;
                        }
                        
                        // Report progress per chunk
                        if let Some(ref ptx) = progress_tx {
                            let _ = ptx.try_send(SnapshotProgress {
                                table: full_name.clone(),
                                rows_processed: row_count,
                                total_bytes: None, // COPY doesn't give total easy
                                complete: false,
                            });
                        }
                    }
                    info!("Finished snapshot for {} ({} rows)", full_name, row_count);
                    
                    // Mark as completed
                    if let Err(e) = self.store.set_table_snapshot_lsn(&full_name, consistent_lsn) {
                         error!("Failed to save snapshot progress for {}: {}", full_name, e);
                         // Don't fail the whole process, but warn
                    }
                    
                    // Report completion
                    if let Some(ref ptx) = progress_tx {
                        let _ = ptx.try_send(SnapshotProgress {
                            table: full_name.clone(),
                            rows_processed: row_count,
                            total_bytes: None,
                            complete: true,
                        });
                    }
                }
                Err(e) => {
                     error!("Failed to start COPY for {}: {}", full_name, e);
                     return Err(Error::Postgres(e));
                }
            }
        }
        
        info!("Snapshot sequence complete ({:?})", start_time.elapsed());
        Ok(())
    }
}
