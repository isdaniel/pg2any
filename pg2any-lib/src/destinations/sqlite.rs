use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, ReplicaIdentity},
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use tracing::{debug, error, info};

/// SQLite destination implementation
///
/// This implementation uses sqlx for database operations with async support.
/// SQLite connections are managed through sqlx's connection pool.
pub struct SQLiteDestination {
    pool: Option<SqlitePool>,
    database_path: Option<String>,
}

impl SQLiteDestination {
    /// Create a new SQLite destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            database_path: None,
        }
    }

    /// Helper function to bind serde_json::Value to sqlx query
    fn bind_value<'a>(
        &self,
        query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
        value: &serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
        match value {
            serde_json::Value::String(s) => query.bind(s.clone()),
            serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
            serde_json::Value::Bool(b) => query.bind(*b),
            serde_json::Value::Null => query.bind(Option::<String>::None),
            _ => query.bind(value.to_string()), // Store complex types as JSON strings
        }
    }

    /// Build WHERE clause for UPDATE and DELETE operations
    fn build_where_clause(
        &self,
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &Option<&HashMap<String, serde_json::Value>>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
        operation: &str,
    ) -> Result<(String, Vec<serde_json::Value>)> {
        match replica_identity {
            ReplicaIdentity::Full => {
                // Use all old data for WHERE clause
                if let Some(old) = old_data {
                    let mut conditions = Vec::new();
                    let mut bind_values = Vec::new();

                    for (column, value) in old {
                        conditions.push(format!("\"{}\" = ?", column));
                        bind_values.push(value.clone());
                    }

                    Ok((conditions.join(" AND "), bind_values))
                } else {
                    Err(CdcError::generic(format!(
                        "REPLICA IDENTITY FULL requires old_data but none provided for {}.{} during {}",
                        schema, table, operation
                    )))
                }
            }

            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for {} operation on {}.{}. Check table's replica identity setting.",
                        operation, schema, table
                    )));
                }

                let mut conditions = Vec::new();
                let mut bind_values = Vec::new();

                // Use old_data if available, otherwise fall back to new_data
                let data_source = match old_data {
                    Some(old) => old,
                    None => new_data.ok_or_else(|| {
                        CdcError::generic(format!(
                            "No data available to build WHERE clause for {} on {}.{}",
                            operation, schema, table
                        ))
                    })?,
                };

                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!("\"{}\" = ?", key_column));
                        bind_values.push(value.clone());
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for {} on {}.{}",
                            key_column, operation, schema, table
                        )));
                    }
                }

                Ok((conditions.join(" AND "), bind_values))
            }

            ReplicaIdentity::Nothing => {
                if operation == "DELETE" {
                    return Err(CdcError::generic(format!(
                        "Cannot perform DELETE on {}.{} with REPLICA IDENTITY NOTHING. \
                        DELETE requires a replica identity.",
                        schema, table
                    )));
                }

                // For UPDATE with NOTHING, try to use key columns if available
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
                            conditions.push(format!("\"{}\" = ?", key_column));
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

                Ok((conditions.join(" AND "), bind_values))
            }
        }
    }

    /// Execute INSERT operation
    async fn execute_insert(
        &self,
        pool: &SqlitePool,
        schema: &str,
        table: &str,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let columns: Vec<String> = data.keys().map(|k| format!("\"{}\"", k)).collect();
        let placeholders: Vec<String> = (0..data.len()).map(|_| "?".to_string()).collect();

        let sql = format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})",
            schema,
            table,
            columns.join(", "),
            placeholders.join(", ")
        );

        debug!("Executing SQLite INSERT: {}", sql);

        let mut query = sqlx::query(&sql);
        for (_, value) in data {
            query = self.bind_value(query, value);
        }

        query
            .execute(pool)
            .await
            .map_err(|e| CdcError::generic(format!("SQLite INSERT failed: {}", e)))?;

        Ok(())
    }

    /// Execute UPDATE operation
    async fn execute_update(
        &self,
        pool: &SqlitePool,
        schema: &str,
        table: &str,
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
    ) -> Result<()> {
        let set_clauses: Vec<String> = new_data.keys().map(|k| format!("\"{}\" = ?", k)).collect();

        let (where_clause, where_values) = self.build_where_clause(
            old_data,
            &Some(new_data),
            replica_identity,
            key_columns,
            schema,
            table,
            "UPDATE",
        )?;

        let sql = format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE {}",
            schema,
            table,
            set_clauses.join(", "),
            where_clause
        );

        debug!("Executing SQLite UPDATE: {}", sql);

        let mut query = sqlx::query(&sql);

        // Bind SET values first
        for (_, value) in new_data {
            query = self.bind_value(query, value);
        }

        // Then bind WHERE values
        for value in &where_values {
            query = self.bind_value(query, value);
        }

        let result = query
            .execute(pool)
            .await
            .map_err(|e| CdcError::generic(format!("SQLite UPDATE failed: {}", e)))?;

        if result.rows_affected() == 0 {
            error!(
                "UPDATE operation affected 0 rows for table {}.{}",
                schema, table
            );
        } else {
            debug!("UPDATE operation affected {} rows", result.rows_affected());
        }

        Ok(())
    }

    /// Execute DELETE operation
    async fn execute_delete(
        &self,
        pool: &SqlitePool,
        schema: &str,
        table: &str,
        old_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
    ) -> Result<()> {
        let (where_clause, where_values) = self.build_where_clause(
            &Some(old_data.clone()),
            &None,
            replica_identity,
            key_columns,
            schema,
            table,
            "DELETE",
        )?;

        let sql = format!(
            "DELETE FROM \"{}\".\"{}\" WHERE {}",
            schema, table, where_clause
        );

        debug!("Executing SQLite DELETE: {}", sql);

        let mut query = sqlx::query(&sql);
        for value in &where_values {
            query = self.bind_value(query, value);
        }

        let result = query
            .execute(pool)
            .await
            .map_err(|e| CdcError::generic(format!("SQLite DELETE failed: {}", e)))?;

        if result.rows_affected() == 0 {
            error!(
                "DELETE operation affected 0 rows for table {}.{}",
                schema, table
            );
        } else {
            debug!("DELETE operation affected {} rows", result.rows_affected());
        }

        Ok(())
    }
}

#[async_trait]
impl DestinationHandler for SQLiteDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        // Parse connection string - for SQLite, this is typically just a file path
        // Support formats like:
        // - "file:///path/to/database.db"
        // - "sqlite:///path/to/database.db"
        // - "/path/to/database.db"
        // - "database.db"
        let db_path = if connection_string.starts_with("file://") {
            connection_string
                .strip_prefix("file://")
                .unwrap_or(connection_string)
        } else if connection_string.starts_with("sqlite://") {
            connection_string
                .strip_prefix("sqlite://")
                .unwrap_or(connection_string)
        } else {
            connection_string
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    CdcError::generic(format!(
                        "Failed to create directory for SQLite database: {}",
                        e
                    ))
                })?;
            }
        }

        // Create connection options
        let mut options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path))
            .map_err(|e| {
                CdcError::generic(format!(
                    "Invalid SQLite connection string '{}': {}",
                    db_path, e
                ))
            })?;

        // Configure options
        options = options
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .foreign_keys(true);

        // Create connection pool
        let pool = SqlitePool::connect_with(options).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to connect to SQLite database '{}': {}",
                db_path, e
            ))
        })?;

        self.pool = Some(pool);
        self.database_path = Some(db_path.to_string());

        info!("Connected to SQLite database: {}", db_path);
        Ok(())
    }

    async fn process_event(&mut self, event: &ChangeEvent) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
                self.execute_insert(pool, schema, table, data).await?;
                debug!("Successfully inserted record into {}.{}", schema, table);
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
                self.execute_update(
                    pool,
                    schema,
                    table,
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                )
                .await?;
                debug!("Successfully updated record in {}.{}", schema, table);
            }

            EventType::Delete {
                schema,
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => {
                self.execute_delete(pool, schema, table, old_data, replica_identity, key_columns)
                    .await?;
                debug!("Successfully deleted record from {}.{}", schema, table);
            }

            EventType::Truncate(tables) => {
                for table_ref in tables {
                    // table_ref format is typically "schema.table"
                    let parts: Vec<&str> = table_ref.split('.').collect();
                    let (schema, table) = if parts.len() >= 2 {
                        (parts[0], parts[1])
                    } else {
                        ("main", parts[0]) // Default schema in SQLite is "main"
                    };

                    let sql = format!("DELETE FROM \"{}\".\"{}\"", schema, table);

                    sqlx::query(&sql).execute(pool).await.map_err(|e| {
                        CdcError::generic(format!(
                            "SQLite TRUNCATE failed for {}.{}: {}",
                            schema, table, e
                        ))
                    })?;

                    info!("Successfully truncated table {}.{}", schema, table);
                }
            }

            EventType::Begin { .. } => {
                // SQLite with WAL mode handles transactions automatically
                debug!("Transaction BEGIN received (SQLite with WAL mode handles transactions automatically)");
            }

            EventType::Commit { .. } => {
                // SQLite with WAL mode handles transactions automatically
                debug!("Transaction COMMIT received (SQLite with WAL mode handles transactions automatically)");
            }

            EventType::Relation | EventType::Type | EventType::Origin | EventType::Message => {
                // These are metadata events that don't require action for SQLite
                debug!("Received metadata event: {:?}", event.event_type);
            }
        }

        Ok(())
    }

    async fn health_check(&mut self) -> Result<bool> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

        // Try a simple query to check if the connection is working
        match sqlx::query("SELECT 1").execute(pool).await {
            Ok(_) => {
                debug!("SQLite health check passed");
                Ok(true)
            }
            Err(e) => {
                error!("SQLite health check failed: {}", e);
                Ok(false)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!("SQLite connection pool closed successfully");
        }

        self.database_path = None;
        Ok(())
    }
}

impl Default for SQLiteDestination {
    fn default() -> Self {
        Self::new()
    }
}
