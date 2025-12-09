//! Schema registry for tracking relation metadata

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::{debug, info};
use rusqlite::{params, Connection};
use anyhow::{Result, Context};

/// Column metadata from PostgreSQL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column flags (part of key, etc.)
    pub flags: u8,
    /// PostgreSQL type OID
    pub type_oid: u32,
    /// Type modifier (e.g., varchar length)
    pub type_modifier: i32,
}

impl Column {
    /// Check if this column is part of the primary key
    #[inline]
    pub fn is_key(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Replica identity mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    /// Default: primary key columns for UPDATE/DELETE
    Default,
    /// Nothing: only key columns, no old tuple
    Nothing,
    /// Full: entire old row
    Full,
    /// Index: specific index columns
    Index,
}

impl From<u8> for ReplicaIdentity {
    fn from(value: u8) -> Self {
        match value {
            b'd' | 0 => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::Nothing,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            _ => ReplicaIdentity::Default,
        }
    }
}

/// Relation (table) metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    /// Relation OID
    pub id: u32,
    /// Schema version (monotonically increasing)
    #[serde(default)]
    pub version: u32,
    /// Schema/namespace name
    pub namespace: String,
    /// Table name
    pub name: String,
    /// Replica identity mode
    pub replica_identity: ReplicaIdentity,
    /// Column definitions
    pub columns: Vec<Column>,
    /// Primary key column indices
    pub primary_key_indices: Vec<usize>,
}

impl Relation {
    /// Get the fully qualified table name
    #[inline]
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    /// Get primary key column names
    pub fn primary_key_columns(&self) -> Vec<&str> {
        self.primary_key_indices
            .iter()
            .filter_map(|&i| self.columns.get(i).map(|c| c.name.as_str()))
            .collect()
    }

    /// Check if this relation has full replica identity
    #[inline]
    pub fn has_full_replica_identity(&self) -> bool {
        self.replica_identity == ReplicaIdentity::Full
    }

    /// Get column by index
    #[inline]
    pub fn get_column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Get column count
    #[inline]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Schema change type result
#[derive(Debug, PartialEq, Eq)]
pub enum SchemaChange {
    None,
    Created,
    Updated { old_version: u32, new_version: u32 },
}

/// Schema registry for tracking relation metadata
///
/// Thread-safe registry that maps relation IDs to their metadata.
/// Updated as RELATION messages are received from pgoutput.
#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    relations: Arc<DashMap<u32, Relation>>,
    /// Index by full name for reverse lookups
    by_name: Arc<DashMap<String, u32>>,
    /// SQLite connection for persistence
    db: Arc<Mutex<Connection>>,
}

impl SchemaRegistry {
    /// Create a new schema registry backed by SQLite
    pub fn new(storage_path: &std::path::Path) -> Result<Self> {
        let db_path = storage_path.join("schema_registry.sqlite");
        let conn = Connection::open(&db_path).context("Failed to open schema registry DB")?;
        
        // Initialize tables
        conn.execute(
            "CREATE TABLE IF NOT EXISTS relations (
                id INTEGER PRIMARY KEY,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                metadata JSON NOT NULL
            )",
            [],
        )?;
        
        let registry = Self {
            relations: Arc::new(DashMap::new()),
            by_name: Arc::new(DashMap::new()),
            db: Arc::new(Mutex::new(conn)),
        };
        
        // Load existing relations
        registry.load_from_disk()?;
        
        Ok(registry)
    }

    fn load_from_disk(&self) -> Result<()> {
        let conn = self.db.lock().map_err(|_| anyhow::anyhow!("Lock poisoned"))?;
        let mut stmt = conn.prepare("SELECT id, metadata FROM relations")?;
        
        let rows = stmt.query_map([], |row| {
            let id: u32 = row.get(0)?;
            let metadata: String = row.get(1)?;
            Ok((id, metadata))
        })?;
        
        for row in rows {
            let (id, metadata) = row?;
            let relation: Relation = serde_json::from_str(&metadata)?;
            self.relations.insert(id, relation.clone());
            self.by_name.insert(relation.full_name(), id);
        }
        
        info!("Loaded {} relations from disk", self.relations.len());
        Ok(())
    }

    /// Register or update a relation
    /// Returns the type of change (None, Created, Updated)
    pub fn register(&self, mut relation: Relation) -> Result<SchemaChange> {
        let full_name = relation.full_name();
        let id = relation.id;

        // Check for existing
        if let Some(mut existing) = self.relations.get_mut(&id) {
            // Check if schema matches
            // We compare columns and primary keys.
            // Note: We ignore flags for now as they might flutter, but ideally should verify type_oid/mod
            let schema_changed = existing.columns.len() != relation.columns.len() 
                || existing.columns.iter().zip(relation.columns.iter()).any(|(a, b)| {
                    a.name != b.name || a.type_oid != b.type_oid
                });

            if !schema_changed {
                return Ok(SchemaChange::None);
            }

            // Update version
            relation.version = existing.version + 1;
            let old_version = existing.version;
            
            // Persist
            {
                let conn = self.db.lock().map_err(|_| anyhow::anyhow!("Lock poisoned"))?;
                let metadata = serde_json::to_string(&relation)?;
                conn.execute(
                    "INSERT OR REPLACE INTO relations (id, namespace, name, metadata) VALUES (?1, ?2, ?3, ?4)",
                    params![id, relation.namespace, relation.name, metadata],
                )?;
            }

            // Update memory
            *existing = relation.clone();
            
            info!("Schema updated for {} (v{} -> v{})", full_name, old_version, relation.version);
            return Ok(SchemaChange::Updated { old_version, new_version: relation.version });
        }

        // New relation
        relation.version = 1; // Start at v1
        debug!(
            "Registered new relation {} ({}) v1 with {} columns",
            full_name, id, relation.columns.len()
        );

        // Persist
        {
            let conn = self.db.lock().map_err(|_| anyhow::anyhow!("Lock poisoned"))?;
            let metadata = serde_json::to_string(&relation)?;
            conn.execute(
                "INSERT OR REPLACE INTO relations (id, namespace, name, metadata) VALUES (?1, ?2, ?3, ?4)",
                params![id, relation.namespace, relation.name, metadata],
            )?;
        }

        self.by_name.insert(full_name.clone(), id);
        self.relations.insert(id, relation);
        
        Ok(SchemaChange::Created)
    }

    /// Get a relation by ID
    #[inline]
    pub fn get(&self, id: u32) -> Option<dashmap::mapref::one::Ref<'_, u32, Relation>> {
        self.relations.get(&id)
    }

    /// Get a relation by full name
    pub fn get_by_name(&self, name: &str) -> Option<dashmap::mapref::one::Ref<'_, u32, Relation>> {
        self.by_name.get(name).and_then(|id| self.relations.get(&*id))
    }

    /// Check if a relation exists
    #[inline]
    pub fn contains(&self, id: u32) -> bool {
        self.relations.contains_key(&id)
    }

    /// Get the number of registered relations
    #[inline]
    pub fn len(&self) -> usize {
        self.relations.len()
    }

    /// Check if the registry is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Get all relation IDs
    pub fn relation_ids(&self) -> Vec<u32> {
        self.relations.iter().map(|r| *r.key()).collect()
    }

    /// Clear the registry (memory only, primarily for tests)
    pub fn clear(&self) {
        self.relations.clear();
        self.by_name.clear();
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        // In-memory default for tests if needed, or panic?
        // Ideally should assume temp dir. 
        // For simplicity in Default trait, we panic or use a temp file
        panic!("SchemaRegistry requires a path. Use SchemaRegistry::new(path)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_relation() -> Relation {
        Relation {
            id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            version: 1,
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                Column {
                    name: "id".to_string(),
                    flags: 0x01, // Part of key
                    type_oid: 23, // INT4
                    type_modifier: -1,
                },
                Column {
                    name: "name".to_string(),
                    flags: 0x00,
                    type_oid: 25, // TEXT
                    type_modifier: -1,
                },
                Column {
                    name: "email".to_string(),
                    flags: 0x00,
                    type_oid: 25, // TEXT
                    type_modifier: -1,
                },
            ],
            primary_key_indices: vec![0],
        }
    }

    #[test]
    fn test_relation_full_name() {
        let relation = create_test_relation();
        assert_eq!(relation.full_name(), "public.users");
    }

    #[test]
    fn test_relation_primary_key() {
        let relation = create_test_relation();
        assert_eq!(relation.primary_key_columns(), vec!["id"]);
    }

    #[test]
    fn test_registry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let registry = SchemaRegistry::new(temp_dir.path()).unwrap();

        let relation = create_test_relation();
        registry.register(relation.clone()).unwrap();

        assert!(registry.contains(16384));
        assert_eq!(registry.len(), 1);

        let retrieved = registry.get(16384).unwrap();
        assert_eq!(retrieved.name, "users");

        let by_name = registry.get_by_name("public.users").unwrap();
        assert_eq!(by_name.id, 16384);
    }

    #[test]
    fn test_column_is_key() {
        let col1 = Column {
            name: "id".to_string(),
            flags: 0x01,
            type_oid: 23,
            type_modifier: -1,
        };
        let col2 = Column {
            name: "name".to_string(),
            flags: 0x00,
            type_oid: 25,
            type_modifier: -1,
        };

        assert!(col1.is_key());
        assert!(!col2.is_key());
    }
}

