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
        value: &'a serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
        match value {
            serde_json::Value::String(s) => query.bind(s.as_str()),
            serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
            serde_json::Value::Bool(b) => query.bind(*b),
            serde_json::Value::Null => query.bind(Option::<String>::None),
            _ => query.bind(value.to_string()), // Store complex types as JSON strings
        }
    }

    /// Build WHERE clause for UPDATE operations
    /// Returns the SQL WHERE clause string and a vector of references to the bind values
    fn build_where_clause_for_update<'a>(
        &self,
        old_data: &'a Option<HashMap<String, serde_json::Value>>,
        new_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<(String, Vec<&'a serde_json::Value>)> {
        match replica_identity {
            ReplicaIdentity::Full => {
                // Use all old data for WHERE clause
                if let Some(old) = old_data {
                    let mut conditions = Vec::with_capacity(old.len());
                    let mut bind_values = Vec::with_capacity(old.len());

                    for (column, value) in old {
                        conditions.push(format!("\"{}\" = ?", column));
                        bind_values.push(value);
                    }

                    Ok((conditions.join(" AND "), bind_values))
                } else {
                    Err(CdcError::generic(format!(
                        "REPLICA IDENTITY FULL requires old_data but none provided for {} during UPDATE",
                        table
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

                // Use old_data if available, otherwise fall back to new_data
                let data_source: &HashMap<String, serde_json::Value> = match old_data {
                    Some(old) => old,
                    None => new_data,
                };

                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!("\"{}\" = ?", key_column));
                        bind_values.push(value);
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for UPDATE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok((conditions.join(" AND "), bind_values))
            }

            ReplicaIdentity::Nothing => {
                // For UPDATE with NOTHING, try to use key columns if available
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
                        conditions.push(format!("\"{}\" = ?", key_column));
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

                Ok((conditions.join(" AND "), bind_values))
            }
        }
    }

    /// Build WHERE clause specifically for DELETE operations (where old_data is always provided)
    fn build_where_clause_for_delete<'a>(
        &self,
        old_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<(String, Vec<&'a serde_json::Value>)> {
        match replica_identity {
            ReplicaIdentity::Full => {
                let mut conditions = Vec::with_capacity(old_data.len());
                let mut bind_values = Vec::with_capacity(old_data.len());

                for (column, value) in old_data {
                    conditions.push(format!("\"{}\" = ?", column));
                    bind_values.push(value);
                }

                Ok((conditions.join(" AND "), bind_values))
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
                        conditions.push(format!("\"{}\" = ?", key_column));
                        bind_values.push(value);
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for DELETE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok((conditions.join(" AND "), bind_values))
            }

            ReplicaIdentity::Nothing => Err(CdcError::generic(format!(
                "Cannot perform DELETE on {}.{} with REPLICA IDENTITY NOTHING. \
                DELETE requires a replica identity.",
                schema, table
            ))),
        }
    }

    /// Execute INSERT operation
    async fn execute_insert(
        &self,
        pool: &SqlitePool,
        table: &str,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let columns: Vec<String> = data.keys().map(|k| format!("\"{}\"", k)).collect();
        let placeholders: Vec<String> = (0..data.len()).map(|_| "?".to_string()).collect();

        // Use only table name without schema
        let table_ref = format!("\"{}\"", table);

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_ref,
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

        let (where_clause, where_values) = self.build_where_clause_for_update(
            old_data,
            new_data,
            replica_identity,
            key_columns,
            schema,
            table,
        )?;

        // Use only table name without schema
        let table_ref = format!("\"{}\"", table);

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            table_ref,
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
        for value in where_values {
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
        // Build WHERE clause directly from old_data without wrapping in Option
        let (where_clause, where_values) = self.build_where_clause_for_delete(
            old_data,
            replica_identity,
            key_columns,
            schema,
            table,
        )?;

        // Use only table name without schema
        let table_ref = format!("\"{}\"", table);

        let sql = format!("DELETE FROM {} WHERE {}", table_ref, where_clause);

        debug!("Executing SQLite DELETE: {}", sql);

        let mut query = sqlx::query(&sql);
        for value in where_values {
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

    /// Execute TRUNCATE operation (implemented as DELETE FROM table)
    /// SQLite doesn't have a native TRUNCATE command, so we use DELETE FROM table
    /// with optimizations for better performance
    async fn execute_truncate(&self, pool: &SqlitePool, tables: &[String]) -> Result<()> {
        for table_ref in tables {
            // Parse table reference: "schema.table" or just "table"
            let parts: Vec<&str> = table_ref.split('.').collect();
            let (_schema, table) = if parts.len() >= 2 {
                (parts[0], parts[1])
            } else {
                ("main", parts[0]) // Default schema in SQLite is "main"
            };

            // Use only table name without schema
            let table_name = format!("\"{}\"", table);

            // SQLite doesn't have TRUNCATE, but DELETE without WHERE clause is equivalent
            // We could also use: DELETE FROM table; DELETE FROM sqlite_sequence WHERE name='table';
            // to reset auto-increment counters, but we'll keep it simple for now
            let sql = format!("DELETE FROM {}", table_name);

            debug!("Executing SQLite TRUNCATE (DELETE): {}", sql);

            let result = sqlx::query(&sql).execute(pool).await.map_err(|e| {
                CdcError::generic(format!("SQLite TRUNCATE failed for {}: {}", table_name, e))
            })?;

            info!(
                "Successfully truncated table {} (deleted {} rows)",
                table_name,
                result.rows_affected()
            );
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

    // SQLite does not use schema/database namespacing like MySQL, so schema mappings are not needed - tables are referenced by name only
    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn process_event(&mut self, event: &ChangeEvent) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

        match &event.event_type {
            EventType::Insert { table, data, .. } => {
                self.execute_insert(pool, table, data).await?;
                debug!("Successfully inserted record into {}", table);
            }

            EventType::Update {
                table,
                old_data,
                new_data,
                replica_identity,
                key_columns,
                ..
            } => {
                self.execute_update(
                    pool,
                    "main",
                    table,
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                )
                .await?;
                debug!("Successfully updated record in main.{}", table);
            }

            EventType::Delete {
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => {
                self.execute_delete(pool, "main", table, old_data, replica_identity, key_columns)
                    .await?;
                debug!("Successfully deleted record from main.{}", table);
            }

            EventType::Truncate(tables) => {
                self.execute_truncate(pool, tables).await?;
                debug!("Successfully truncated {} table(s)", tables.len());
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
