use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use sqlx::MySqlPool;
use std::collections::HashMap;
use tracing::{debug, info};

// ============================================================================
// MySQL Destination Implementation
// ============================================================================

pub struct MySQLDestination {
    pool: Option<MySqlPool>,
    /// Schema mappings: maps source schema to destination database
    schema_mappings: HashMap<String, String>,
    /// MySQL max_allowed_packet value in bytes (default 64MB, max 1GB), Used to calculate safe batch sizes dynamically
    max_allowed_packet: u64,
}

impl MySQLDestination {
    /// Create a new MySQL destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            schema_mappings: HashMap::new(),
            max_allowed_packet: 67108864, // Default 64MB
        }
    }
}

impl Default for MySQLDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DestinationHandler for MySQLDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let pool = MySqlPool::connect(connection_string).await?;

        // Query max_allowed_packet to calculate optimal batch sizes
        let row: (String, String) = sqlx::query_as("SHOW VARIABLES LIKE 'max_allowed_packet'")
            .fetch_one(&pool)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to query max_allowed_packet: {e}")))?;

        // Parse the string value to u64
        self.max_allowed_packet = row.1.parse::<u64>().unwrap_or(67108864); // Default to 64MB if parse fails

        info!(
            "MySQL max_allowed_packet: {} bytes ({:.2} MB)",
            self.max_allowed_packet,
            self.max_allowed_packet as f64 / 1_048_576.0
        );

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

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL pool not initialized"))?;

        super::common::execute_sqlx_batch_with_hook(pool, commands, pre_commit_hook, "MySQL").await
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        info!("MySQL connection closed successfully");
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
    }
}
