use super::destination_factory::DestinationHandler;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

// ============================================================================
// SQLite Destination Implementation
// ============================================================================

/// SQLite destination implementation with batch INSERT/UPDATE/DELETE support
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
}

impl Default for SQLiteDestination {
    fn default() -> Self {
        Self::new()
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
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    CdcError::generic(format!(
                        "Failed to create directory for SQLite database: {e}"
                    ))
                })?;
            }
        }

        // Create connection options
        let mut options =
            SqliteConnectOptions::from_str(&format!("sqlite://{db_path}")).map_err(|e| {
                CdcError::generic(format!(
                    "Failed to parse SQLite connection string '{db_path}': {e}"
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
                "Failed to connect to SQLite database '{db_path}': {e}"
            ))
        })?;

        self.pool = Some(pool);
        self.database_path = Some(db_path.to_string());

        info!("Connected to SQLite database: {}", db_path);
        Ok(())
    }

    // SQLite does not use schema/database namespacing like MySQL, so schema mappings are not needed
    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch(&mut self, commands: &[String]) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite pool not initialized"))?;

        // Begin a transaction
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("SQLite BEGIN transaction failed: {e}")))?;

        // Execute all commands in the transaction
        for (idx, sql) in commands.iter().enumerate() {
            if let Err(e) = sqlx::query(sql).execute(&mut *tx).await {
                // Rollback on error
                if let Err(rollback_err) = tx.rollback().await {
                    tracing::error!(
                        "SQLite ROLLBACK failed after execution error: {}",
                        rollback_err
                    );
                }
                return Err(CdcError::generic(format!(
                    "SQLite execute_sql_batch failed at command {}/{}: {}",
                    idx + 1,
                    commands.len(),
                    e
                )));
            }
        }

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| CdcError::generic(format!("SQLite COMMIT transaction failed: {e}")))?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        self.database_path = None;
        info!("SQLite connection closed successfully");
        Ok(())
    }
}
