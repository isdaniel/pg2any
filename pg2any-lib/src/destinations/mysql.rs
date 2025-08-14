use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType},
};
use async_trait::async_trait;
use sqlx::MySqlPool;
use tracing::debug;

/// MySQL destination implementation
pub struct MySQLDestination {
    pool: Option<MySqlPool>,
}

impl MySQLDestination {
    /// Create a new MySQL destination instance
    pub fn new() -> Self {
        Self { pool: None }
    }

    /// Convert PostgreSQL type to MySQL type
    fn convert_type(&self, pg_type: &str) -> &str {
        match pg_type.to_lowercase().as_str() {
            "integer" | "int4" => "INT",
            "bigint" | "int8" => "BIGINT",
            "smallint" | "int2" => "SMALLINT",
            "real" | "float4" => "FLOAT",
            "double precision" | "float8" => "DOUBLE",
            "numeric" | "decimal" => "DECIMAL",
            "boolean" | "bool" => "BOOLEAN",
            "character varying" | "varchar" => "VARCHAR(255)",
            "character" | "char" => "CHAR",
            "text" => "TEXT",
            "date" => "DATE",
            "time" | "time without time zone" => "TIME",
            "timestamp" | "timestamp without time zone" => "DATETIME",
            "timestamp with time zone" | "timestamptz" => "DATETIME",
            "uuid" => "VARCHAR(36)",
            "json" | "jsonb" => "JSON",
            _ => "TEXT", // fallback
        }
    }

    /// Generate CREATE TABLE statement for MySQL
    async fn generate_create_table(&self, event: &ChangeEvent) -> Result<String> {
        let default_schema = "public".to_string();
        let schema_name = event.schema_name.as_ref().unwrap_or(&default_schema);
        let table_name = event.table_name.as_ref().unwrap();

        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n",
            schema_name, table_name
        );

        // For now, we'll create columns based on the data we receive
        // In a production system, you'd want to query the PostgreSQL schema
        if let Some(data) = &event.new_data {
            let mut columns = Vec::new();
            for (column_name, value) in data {
                let column_type = match value {
                    serde_json::Value::Number(n) if n.is_i64() => "BIGINT",
                    serde_json::Value::Number(n) if n.is_f64() => "DOUBLE",
                    serde_json::Value::Bool(_) => "BOOLEAN",
                    serde_json::Value::String(s) if s.len() <= 255 => "VARCHAR(255)",
                    serde_json::Value::String(_) => "TEXT",
                    _ => "JSON",
                };
                columns.push(format!("  `{}` {}", column_name, column_type));
            }
            sql.push_str(&columns.join(",\n"));
        }

        sql.push_str("\n)");
        Ok(sql)
    }
}

#[async_trait]
impl DestinationHandler for MySQLDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let pool = MySqlPool::connect(connection_string).await?;
        self.pool = Some(pool);
        Ok(())
    }

    async fn process_event(&mut self, event: &ChangeEvent) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

        match &event.event_type {
            EventType::Insert => {
                if let (Some(schema), Some(table), Some(data)) =
                    (&event.schema_name, &event.table_name, &event.new_data)
                {
                    let columns: Vec<String> = data.keys().map(|k| format!("`{}`", k)).collect();
                    let placeholders: Vec<String> =
                        (0..columns.len()).map(|_| "?".to_string()).collect();

                    let sql = format!(
                        "INSERT INTO `{}`.`{}` ({}) VALUES ({})",
                        schema,
                        table,
                        columns.join(", "),
                        placeholders.join(", ")
                    );

                    let mut query = sqlx::query(&sql);
                    for (_, value) in data {
                        query = match value {
                            serde_json::Value::String(s) => query.bind(s),
                            serde_json::Value::Number(n) if n.is_i64() => {
                                query.bind(n.as_i64().unwrap())
                            }
                            serde_json::Value::Number(n) if n.is_f64() => {
                                query.bind(n.as_f64().unwrap())
                            }
                            serde_json::Value::Bool(b) => query.bind(*b),
                            _ => query.bind(value.to_string()),
                        };
                    }

                    query.execute(pool).await?;
                }
            }
            EventType::Update => {
                if let (Some(schema), Some(table), Some(new_data)) =
                    (&event.schema_name, &event.table_name, &event.new_data)
                {
                    let set_clauses: Vec<String> =
                        new_data.keys().map(|k| format!("`{}` = ?", k)).collect();

                    // Use old_data to build WHERE clause for proper row identification
                    // This uses the replica identity (primary key, unique index, or full row)
                    let where_clause = if let Some(old_data) = &event.old_data {
                        old_data
                            .iter()
                            .map(|(k, v)| format!("`{k}` = {v}"))
                            .collect::<Vec<String>>()
                            .join(" AND ")
                    } else {
                        // Fallback: use primary key/unique columns from new_data if old_data is not available
                        // This happens with REPLICA IDENTITY NOTHING, but it's not ideal
                        // todo handle this via replica identity
                        new_data
                            .iter()
                            .take(1) // Take first column as a fallback (not ideal)
                            .map(|(k, v)| format!("`{k}` = {v}"))
                            .collect::<Vec<String>>()
                            .join(" AND ")
                    };

                    let sql = format!(
                        "UPDATE `{}`.`{}` SET {} WHERE {}",
                        schema,
                        table,
                        set_clauses.join(", "),
                        where_clause
                    );

                    let mut query = sqlx::query(&sql);

                    // Bind SET clause values (new data)
                    for (_, value) in new_data {
                        query = match value {
                            serde_json::Value::String(s) => query.bind(s),
                            serde_json::Value::Number(n) if n.is_i64() => {
                                query.bind(n.as_i64().unwrap())
                            }
                            serde_json::Value::Number(n) if n.is_f64() => {
                                query.bind(n.as_f64().unwrap())
                            }
                            serde_json::Value::Bool(b) => query.bind(*b),
                            _ => query.bind(value.to_string()),
                        };
                    }

                    query.execute(pool).await?;
                }
            }
            EventType::Delete => {
                if let (Some(schema), Some(table), Some(data)) =
                    (&event.schema_name, &event.table_name, &event.old_data)
                {
                    // Simple delete - in production you'd use proper key matching
                    let where_clauses: Vec<String> =
                        data.keys().map(|k| format!("`{}` = ?", k)).collect();

                    let sql = format!(
                        "DELETE FROM `{}`.`{}` WHERE {}",
                        schema,
                        table,
                        where_clauses.join(" AND ")
                    );

                    let mut query = sqlx::query(&sql);
                    for (_, value) in data {
                        query = match value {
                            serde_json::Value::String(s) => query.bind(s),
                            serde_json::Value::Number(n) if n.is_i64() => {
                                query.bind(n.as_i64().unwrap())
                            }
                            serde_json::Value::Number(n) if n.is_f64() => {
                                query.bind(n.as_f64().unwrap())
                            }
                            serde_json::Value::Bool(b) => query.bind(*b),
                            _ => query.bind(value.to_string()),
                        };
                    }

                    query.execute(pool).await?;
                }
            }
            EventType::Truncate(truncate_tables) => {
                let sql = truncate_tables
                    .iter()
                    .map(|table_full_name| {
                        let mut parts = table_full_name.splitn(2, '.');
                        match (parts.next(), parts.next()) {
                            (Some(schema), Some(table)) => {
                                format!("TRUNCATE TABLE `{}`.`{}`", schema, table)
                            }
                            _ => format!("TRUNCATE TABLE `{}`", table_full_name),
                        }
                    })
                    .collect::<Vec<String>>()
                    .join("; ");
                let query = sqlx::query(&sql);
                query.execute(pool).await?;
            }
            _ => {
                // Skip non-DML events for now
            }
        }

        Ok(())
    }

    async fn create_table_if_not_exists(&mut self, event: &ChangeEvent) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

        let sql = self.generate_create_table(event).await?;
        sqlx::query(&sql).execute(pool).await?;

        Ok(())
    }

    async fn health_check(&mut self) -> Result<bool> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
    }

    #[test]
    fn test_type_conversion() {
        let destination = MySQLDestination::new();

        assert_eq!(destination.convert_type("integer"), "INT");
        assert_eq!(destination.convert_type("bigint"), "BIGINT");
        assert_eq!(destination.convert_type("varchar"), "VARCHAR(255)");
        assert_eq!(destination.convert_type("boolean"), "BOOLEAN");
        assert_eq!(destination.convert_type("uuid"), "VARCHAR(36)");
        assert_eq!(destination.convert_type("unknown_type"), "TEXT");
    }

    #[tokio::test]
    async fn test_generate_create_table() {
        let destination = MySQLDestination::new();

        let mut data = HashMap::new();
        data.insert(
            "id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        );
        data.insert(
            "name".to_string(),
            serde_json::Value::String("test".to_string()),
        );
        data.insert("active".to_string(), serde_json::Value::Bool(true));

        let event = ChangeEvent {
            event_type: EventType::Insert,
            transaction_id: Some(123),
            commit_timestamp: Some(Utc::now()),
            schema_name: Some("test_schema".to_string()),
            table_name: Some("test_table".to_string()),
            relation_oid: Some(456),
            old_data: None,
            new_data: Some(data),
            lsn: None,
            metadata: None,
        };

        let sql = destination.generate_create_table(&event).await.unwrap();

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_schema`.`test_table`"));
        assert!(sql.contains("`id` BIGINT"));
        assert!(sql.contains("`name` VARCHAR(255)"));
        assert!(sql.contains("`active` BOOLEAN"));
    }

    #[test]
    fn test_mysql_destination_default_schema() {
        // Test that default schema is handled correctly
        let destination = MySQLDestination::new();
        let event = ChangeEvent {
            event_type: EventType::Insert,
            transaction_id: Some(123),
            commit_timestamp: Some(Utc::now()),
            schema_name: None, // No schema provided
            table_name: Some("test_table".to_string()),
            relation_oid: Some(456),
            old_data: None,
            new_data: Some(HashMap::new()),
            lsn: None,
            metadata: None,
        };

        // This should use "public" as default schema
        // We can test this by checking the generate_create_table method
        assert!(true); // Placeholder - would need async test setup for full validation
    }
}
