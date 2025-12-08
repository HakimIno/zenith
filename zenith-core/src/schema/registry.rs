//! Schema registry for tracking relation metadata

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::debug;

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

/// Schema registry for tracking relation metadata
///
/// Thread-safe registry that maps relation IDs to their metadata.
/// Updated as RELATION messages are received from pgoutput.
#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    relations: Arc<DashMap<u32, Relation>>,
    /// Index by full name for reverse lookups
    by_name: Arc<DashMap<String, u32>>,
}

impl SchemaRegistry {
    /// Create a new empty schema registry
    pub fn new() -> Self {
        Self {
            relations: Arc::new(DashMap::new()),
            by_name: Arc::new(DashMap::new()),
        }
    }

    /// Register or update a relation
    pub fn register(&self, relation: Relation) {
        let full_name = relation.full_name();
        let id = relation.id;

        debug!(
            "Registered relation {} ({}) with {} columns, PK: {:?}",
            full_name,
            id,
            relation.columns.len(),
            relation.primary_key_columns()
        );

        self.by_name.insert(full_name, id);
        self.relations.insert(id, relation);
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

    /// Clear the registry
    pub fn clear(&self) {
        self.relations.clear();
        self.by_name.clear();
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
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
    fn test_schema_registry() {
        let registry = SchemaRegistry::new();
        let relation = create_test_relation();

        registry.register(relation.clone());

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

