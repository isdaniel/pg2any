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
    /// Schema mappings: maps source schema to destination database
    /// e.g., "public" -> "cdc_db"
    schema_mappings: HashMap<String, String>,
}

/// Helper struct for building WHERE clauses with proper parameter binding
/// Uses references to avoid unnecessary cloning of JSON values
#[derive(Debug)]
struct WhereClause<'a> {
    sql: String,
    bind_values: Vec<&'a serde_json::Value>,
}

impl MySQLDestination {
    /// Create a new MySQL destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            schema_mappings: HashMap::new(),
        }
    }

    /// Map a source schema to the destination schema/database
    /// If no mapping exists, returns the original schema name
    fn map_schema(&self, source_schema: &str) -> String {
        self.schema_mappings
            .get(source_schema)
            .cloned()
            .unwrap_or_else(|| source_schema.to_string())
    }

    fn bind_value<'a>(
        &self,
        query: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
        value: &'a serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
        match value {
            serde_json::Value::String(s) => query.bind(s.as_str()),
            serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
            serde_json::Value::Bool(b) => query.bind(*b),
            serde_json::Value::Null => query.bind(Option::<String>::None),
            _ => query.bind(value.to_string()),
        }
    }

    fn build_where_clause_for_update<'a>(
        &self,
        old_data: &'a Option<HashMap<String, serde_json::Value>>,
        new_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause<'a>> {
        use crate::types::ReplicaIdentity;

        match replica_identity {
            ReplicaIdentity::Full => {
                // Always require old_data
                if let Some(old) = old_data {
                    let mut conditions = Vec::with_capacity(old.len());
                    let mut bind_values = Vec::with_capacity(old.len());

                    for (column, value) in old {
                        conditions.push(format!("`{}` = ?", column));
                        bind_values.push(value);
                    }

                    Ok(WhereClause {
                        sql: conditions.join(" AND "),
                        bind_values,
                    })
                } else {
                    Err(CdcError::generic(format!(
                        "REPLICA IDENTITY FULL requires old_data but none provided for {}.{} during UPDATE",
                        schema, table
                    )))
                }
            }

            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for UPDATE operation on {}.{}. Check table's replica identity setting.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::with_capacity(key_columns.len());
                let mut bind_values = Vec::with_capacity(key_columns.len());

                // Use old_data if available, otherwise use new_data
                let data_source: &HashMap<String, serde_json::Value> = match old_data {
                    Some(old) => old,
                    None => new_data,
                };

                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value);
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for UPDATE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Nothing => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "Cannot perform UPDATE on {}.{} with REPLICA IDENTITY NOTHING and no key columns.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::with_capacity(key_columns.len());
                let mut bind_values = Vec::with_capacity(key_columns.len());

                for key_column in key_columns {
                    if let Some(value) = new_data.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value);
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
        }
    }

    fn build_where_clause_for_delete<'a>(
        &self,
        old_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause<'a>> {
        use crate::types::ReplicaIdentity;

        // For DELETE, old_data is always provided, handle each replica identity case directly
        match replica_identity {
            ReplicaIdentity::Full => {
                let mut conditions = Vec::with_capacity(old_data.len());
                let mut bind_values = Vec::with_capacity(old_data.len());

                for (column, value) in old_data {
                    conditions.push(format!("`{}` = ?", column));
                    bind_values.push(value);
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for DELETE operation on {}.{}. Check table's replica identity setting.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::with_capacity(key_columns.len());
                let mut bind_values = Vec::with_capacity(key_columns.len());

                for key_column in key_columns {
                    if let Some(value) = old_data.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value);
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for DELETE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Nothing => Err(CdcError::generic(format!(
                "Cannot perform DELETE on {}.{} with REPLICA IDENTITY NOTHING. \
                DELETE requires a replica identity.",
                schema, table
            ))),
        }
    }
}

#[async_trait]
impl DestinationHandler for MySQLDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let pool = MySqlPool::connect(connection_string).await?;
        self.pool = Some(pool);
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
        if !self.schema_mappings.is_empty() {
            debug!(
                "MySQL destination schema mappings set: {:?}",
                self.schema_mappings
            );
        }
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
                let dest_schema = self.map_schema(schema);
                let columns: Vec<String> = data.keys().map(|k| format!("`{}`", k)).collect();
                let placeholders: Vec<String> =
                    (0..columns.len()).map(|_| "?".to_string()).collect();

                let sql = format!(
                    "INSERT INTO `{}`.`{}` ({}) VALUES ({})",
                    dest_schema,
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
                let dest_schema = self.map_schema(schema);
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
                    dest_schema,
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
                    query = self.bind_value(query, value);
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
                let dest_schema = self.map_schema(schema);
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
                    dest_schema, table, where_clause.sql
                );

                let mut query = sqlx::query(&sql);

                // Bind WHERE clause values
                for value in where_clause.bind_values {
                    query = self.bind_value(query, value);
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
                                let dest_schema = self.map_schema(schema);
                                format!("TRUNCATE TABLE `{}`.`{}`", dest_schema, table)
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

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
    }
}
