use crate::config::ClickHouseConfig;
use crate::error::Result;
use crate::schema::{Relation, Column};
use reqwest::Client;
use tracing::{info, debug};

pub struct ClickHouseMigrator {
    client: Client,
    config: ClickHouseConfig,
}

impl ClickHouseMigrator {
    pub fn new(config: ClickHouseConfig) -> Self {
        Self {
            client: Client::new(),
            config,
        }
    }

    /// Migrate schema for a relation:
    /// 1. Create table version if not exists
    /// 2. Update the Unified View
    pub async fn migrate(&self, relation: &Relation) -> Result<()> {
        let table_name = format!("{}_v{}", relation.name, relation.version);
        let full_table_name = format!("{}.{}", self.config.database, table_name);

        // 1. Create Table Version
        let create_table_query = self.generate_create_table_sql(relation, &full_table_name);
        debug!("Creating table: {}", full_table_name);
        self.execute(&create_table_query).await?;

        // 2. Update Unified View
        let view_name = format!("{}.{}", self.config.database, relation.name);
        let create_view_query = self.generate_create_view_sql(relation, &view_name);
        info!("Updating view: {}", view_name);
        self.execute(&create_view_query).await?;

        Ok(())
    }

    async fn execute(&self, query: &str) -> Result<()> {
        let url = format!("{}/", self.config.url);
        let res = self.client.post(&url)
            .query(&[("query", query)])
            .send()
            .await?;
            
        if !res.status().is_success() {
             let error = res.text().await?;
             return Err(crate::error::Error::Config(format!("ClickHouse error: {}", error)));
        }
        
        Ok(())
    }

    fn generate_create_table_sql(&self, relation: &Relation, full_table_name: &str) -> String {
        let columns: Vec<String> = relation.columns.iter()
            .map(|c| format!("`{}` {}", c.name, self.map_type(c)))
            .collect();
            
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                _zenith_offset UInt64,
                _zenith_ts DateTime64(6),
                _zenith_op String,
                {}
            ) ENGINE = ReplacingMergeTree(_zenith_offset) ",
            full_table_name,
            columns.join(", ")
        );

        if !relation.primary_key_indices.is_empty() {
             let pk_cols: Vec<String> = relation.primary_key_indices.iter()
                .filter_map(|&i| relation.columns.get(i).map(|c| c.name.clone()))
                .collect();
             sql.push_str(&format!("ORDER BY ({})", pk_cols.join(", ")));
        } else {
             sql.push_str("ORDER BY tuple()");
        }

        sql
    }

    fn generate_create_view_sql(&self, relation: &Relation, view_name: &str) -> String {
        let table_name = format!("{}.{}_v{}", self.config.database, relation.name, relation.version);
        
        format!(
            "CREATE OR REPLACE VIEW {} AS SELECT * FROM {} FINAL WHERE _zenith_op != 'DELETE'",
            view_name,
            table_name
        )
    }

    fn map_type(&self, column: &Column) -> String {
        match column.type_oid {
            16 => "Bool".to_string(),
            20 => "Int64".to_string(), // INT8
            21 => "Int16".to_string(), // INT2
            23 => "Int32".to_string(), // INT4
            25 | 1043 => "String".to_string(), // TEXT | VARCHAR
            700 | 701 => "Float64".to_string(),
            1114 => "DateTime64(6)".to_string(), // TIMESTAMP
            1184 => "DateTime64(6)".to_string(), // TIMESTAMPTZ
            1700 => self.map_decimal(column.type_modifier), // NUMERIC
            2950 => "UUID".to_string(),
            1082 => "Date".to_string(), // DATE
            _ => "String".to_string(), // Fallback
        }
    }

    /// Parse Postgres type modifier for NUMERIC(p, s)
    /// precision = (mod - 4) >> 16
    /// scale = (mod - 4) & 0xFFFF
    fn map_decimal(&self, type_modifier: i32) -> String {
        if type_modifier == -1 {
            // No precision specified, use ClickHouse default safe max or specific fallback
            // ClickHouse Decimal256 can hold very large numbers, but typically Decimal(38, S) is common standard
            return "Decimal(38, 9)".to_string(); 
        }

        // The modifier in postgres is encoded as: ((precision << 16) | scale) + 4
        let tmp = type_modifier - 4;
        let precision = (tmp >> 16) & 0xFFFF;
        let scale = tmp & 0xFFFF;

        // ClickHouse max precision is 76 (Decimal256), but commonly 38 (Decimal128)
        let ch_precision = precision.max(1).min(76);
        let ch_scale = scale.min(ch_precision); // Scale cannot exceed precision

        format!("Decimal({}, {})", ch_precision, ch_scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClickHouseConfig;

    fn test_config() -> ClickHouseConfig {
        ClickHouseConfig {
            url: "http://localhost:8123".to_string(),
            database: "test".to_string(),
            table: "test".to_string(),
            user: None,
            password: None,
            batch_size: 1000,
            batch_timeout_ms: 100,
            compression: false,
            async_insert: false,
            dlq: crate::config::DlqConfig::default(),
        }
    }

    #[test]
    fn test_generate_create_view_sql() {
        let config = test_config();
        let migrator = ClickHouseMigrator::new(config);

        let relation = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            version: 1,
            replica_identity: crate::schema::ReplicaIdentity::Default,
            columns: vec![],
            primary_key_indices: vec![],
        };

        let sql = migrator.generate_create_view_sql(&relation, "test.users");
        assert_eq!(
            sql,
            "CREATE OR REPLACE VIEW test.users AS SELECT * FROM test.users_v1 FINAL WHERE _zenith_op != 'DELETE'"
        );
    }

    #[test]
    fn test_type_mapping() {
        let migrator = ClickHouseMigrator::new(test_config());

        // Test Integers
        let col_int4 = Column { name: "id".into(), flags: 0, type_oid: 23, type_modifier: -1 };
        assert_eq!(migrator.map_type(&col_int4), "Int32");

        let col_int8 = Column { name: "id".into(), flags: 0, type_oid: 20, type_modifier: -1 };
        assert_eq!(migrator.map_type(&col_int8), "Int64");

        // Test Decimals
        // Modifier for (18, 4) -> ((18 << 16) | 4) + 4 = 1179652
        let mod_18_4 = ((18 << 16) | 4) + 4;
        let col_decimal = Column { name: "price".into(), flags: 0, type_oid: 1700, type_modifier: mod_18_4 };
        assert_eq!(migrator.map_type(&col_decimal), "Decimal(18, 4)");

        // Test Decimal Fallback
        let col_decimal_fallback = Column { name: "price".into(), flags: 0, type_oid: 1700, type_modifier: -1 };
        assert_eq!(migrator.map_type(&col_decimal_fallback), "Decimal(38, 9)");
    }
}
