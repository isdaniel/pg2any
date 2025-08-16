use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType},
};
use async_trait::async_trait;
use sqlx::MySqlPool;
use std::collections::HashMap;
use tracing::debug;

/// MySQL destination implementation
pub struct MySQLDestination {
    pool: Option<MySqlPool>,
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Update,
    Delete,
}

/// Helper struct for building WHERE clauses with proper parameter binding
#[derive(Debug)]
struct WhereClause {
    sql: String,
    bind_values: Vec<serde_json::Value>,
}

impl Operation {
    fn name(&self) -> String {
        match self {
            Operation::Update => "UPDATE".to_string(),
            Operation::Delete => "DELETE".to_string(),
        }
    }
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
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
                let mut sql = format!("CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n", schema, table);

                // For now, we'll create columns based on the data we receive
                // In a production system, you'd want to query the PostgreSQL schema
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
                sql.push_str("\n)");
                Ok(sql)
            }
            _ => Err(CdcError::generic(
                "Cannot generate CREATE TABLE for non-INSERT event",
            )),
        }
    }

    fn bind_value<'a>(
        &self,
        query: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
        value: &serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
        match value {
            serde_json::Value::String(s) => query.bind(s.clone()),
            serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
            serde_json::Value::Bool(b) => query.bind(*b),
            serde_json::Value::Null => query.bind(Option::<String>::None),
            _ => query.bind(value.to_string()),
        }
    }

    fn build_where_clause(
        &self,
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &Option<&HashMap<String, serde_json::Value>>, // only used for UPDATE
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
        op: Operation,
    ) -> Result<WhereClause> {
        use crate::types::ReplicaIdentity;

        match replica_identity {
            ReplicaIdentity::Full => {
                // Always require old_data
                if let Some(old) = old_data {
                    let mut conditions = Vec::new();
                    let mut bind_values = Vec::new();

                    for (column, value) in old {
                        conditions.push(format!("`{}` = ?", column));
                        bind_values.push(value.clone());
                    }

                    Ok(WhereClause {
                        sql: conditions.join(" AND "),
                        bind_values,
                    })
                } else {
                    Err(CdcError::generic(format!(
                        "REPLICA IDENTITY FULL requires old_data but none provided for {}.{} during {}",
                        schema, table, op.name()
                    )))
                }
            }

            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for {} operation on {}.{}. Check table's replica identity setting.",
                        op.name(), schema, table
                    )));
                }

                let mut conditions = Vec::new();
                let mut bind_values = Vec::new();

                let data_source = match old_data {
                    Some(old) => old,
                    None => new_data.ok_or_else(|| {
                        CdcError::generic(format!(
                            "No data available to build WHERE clause for {} on {}.{}",
                            op.name(),
                            schema,
                            table
                        ))
                    })?,
                };

                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value.clone());
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for {} on {}.{}",
                            key_column,
                            op.name(),
                            schema,
                            table
                        )));
                    }
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Nothing => match op {
                Operation::Update => {
                    if key_columns.is_empty() {
                        return Err(CdcError::generic(format!(
                            "Cannot perform UPDATE on {}.{} with REPLICA IDENTITY NOTHING and no key columns.",
                            schema, table
                        )));
                    }

                    let mut conditions = Vec::new();
                    let mut bind_values = Vec::new();

                    if let Some(new_data) = new_data {
                        for key_column in key_columns {
                            if let Some(value) = new_data.get(key_column) {
                                conditions.push(format!("`{}` = ?", key_column));
                                bind_values.push(value.clone());
                            }
                        }
                    }

                    if conditions.is_empty() {
                        return Err(CdcError::generic(format!(
                            "No usable key columns found for UPDATE on {}.{} with REPLICA IDENTITY NOTHING",
                            schema, table
                        )));
                    }

                    debug!(
                        "WARNING: Using potentially unreliable WHERE clause for UPDATE on {}.{} due to REPLICA IDENTITY NOTHING",
                        schema, table
                    );

                    Ok(WhereClause {
                        sql: conditions.join(" AND "),
                        bind_values,
                    })
                }
                Operation::Delete => Err(CdcError::generic(format!(
                    "Cannot perform DELETE on {}.{} with REPLICA IDENTITY NOTHING. \
                    DELETE requires a replica identity.",
                    schema, table
                ))),
            },
        }
    }

    fn build_where_clause_for_update(
        &self,
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause> {
        self.build_where_clause(
            old_data,
            &Some(new_data),
            replica_identity,
            key_columns,
            schema,
            table,
            Operation::Update,
        )
    }

    fn build_where_clause_for_delete(
        &self,
        old_data: &HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause> {
        self.build_where_clause(
            &Some(old_data.clone()),
            &None,
            replica_identity,
            key_columns,
            schema,
            table,
            Operation::Delete,
        )
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
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
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
            EventType::Update {
                schema,
                table,
                old_data,
                new_data,
                replica_identity,
                key_columns,
                ..
            } => {
                let set_clauses: Vec<String> =
                    new_data.keys().map(|k| format!("`{}` = ?", k)).collect();

                // Build WHERE clause using proper key identification strategy
                let where_clause = self.build_where_clause_for_update(
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                    schema,
                    table,
                )?;

                let sql = format!(
                    "UPDATE `{}`.`{}` SET {} WHERE {}",
                    schema,
                    table,
                    set_clauses.join(", "),
                    where_clause.sql
                );

                let mut query = sqlx::query(&sql);

                // Bind SET clause values (new data)
                for (_, value) in new_data {
                    query = self.bind_value(query, value);
                }

                // Bind WHERE clause values
                for value in where_clause.bind_values {
                    query = self.bind_value(query, &value);
                }

                query.execute(pool).await?;
            }
            EventType::Delete {
                schema,
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => {
                // Build WHERE clause using proper key identification strategy
                let where_clause = self.build_where_clause_for_delete(
                    old_data,
                    replica_identity,
                    key_columns,
                    schema,
                    table,
                )?;

                let sql = format!(
                    "DELETE FROM `{}`.`{}` WHERE {}",
                    schema, table, where_clause.sql
                );

                let mut query = sqlx::query(&sql);

                // Bind WHERE clause values
                for value in where_clause.bind_values {
                    query = self.bind_value(query, &value);
                }

                query.execute(pool).await?;
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
            event_type: EventType::Insert {
                schema: "test_schema".to_string(),
                table: "test_table".to_string(),
                relation_oid: 456,
                data,
            },
            lsn: None,
            metadata: None,
        };

        let sql = destination.generate_create_table(&event).await.unwrap();

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_schema`.`test_table`"));
        assert!(sql.contains("`id` BIGINT"));
        assert!(sql.contains("`name` VARCHAR(255)"));
        assert!(sql.contains("`active` BOOLEAN"));
    }
}
