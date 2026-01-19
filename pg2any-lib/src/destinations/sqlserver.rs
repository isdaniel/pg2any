use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info};

/// SQL Server destination implementation
pub struct SqlServerDestination {
    client: Option<Client<Compat<TcpStream>>>,
    /// Schema mappings: maps source schema to destination schema
    schema_mappings: HashMap<String, String>,
}

impl SqlServerDestination {
    /// Create a new SQL Server destination instance
    pub fn new() -> Self {
        Self {
            client: None,
            schema_mappings: HashMap::new(),
        }
    }
}

#[async_trait]
impl DestinationHandler for SqlServerDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let config = Config::from_ado_string(connection_string)
            .map_err(|e| CdcError::generic(format!("Invalid SQL Server connection string: {e}")))?;

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| CdcError::generic(format!("Failed to connect to SQL Server: {e}")))?;
        tcp.set_nodelay(true)
            .map_err(|e| CdcError::generic(format!("Failed to set TCP_NODELAY: {e}")))?;

        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to establish SQL Server connection: {e}"))
            })?;

        self.client = Some(client);
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
        if !self.schema_mappings.is_empty() {
            debug!(
                "SQL Server destination schema mappings set: {:?}",
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

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server client not initialized"))?;

        // Begin a transaction
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("SQL Server BEGIN TRANSACTION failed: {e}")))?;

        // Execute all commands in the transaction
        let mut execution_result = Ok(());
        for (idx, sql) in commands.iter().enumerate() {
            if let Err(e) = client.simple_query(sql).await {
                execution_result = Err(CdcError::generic(format!(
                    "SQL Server execute_sql_batch failed at command {}/{}: {}",
                    idx + 1,
                    commands.len(),
                    e
                )));
                break;
            }
        }

        // If any command failed, rollback
        if execution_result.is_err() {
            if let Err(rollback_err) = client.simple_query("ROLLBACK TRANSACTION").await {
                tracing::error!(
                    "SQL Server ROLLBACK failed after execution error: {}",
                    rollback_err
                );
            }
            return execution_result;
        }

        // Execute pre-commit hook BEFORE transaction COMMIT
        if let Some(hook) = pre_commit_hook {
            if let Err(e) = hook().await {
                // Rollback transaction if hook fails
                if let Err(rollback_err) = client.simple_query("ROLLBACK TRANSACTION").await {
                    tracing::error!(
                        "SQL Server ROLLBACK failed after pre-commit hook error: {}",
                        rollback_err
                    );
                }
                return Err(CdcError::generic(format!(
                    "SQL Server pre-commit hook failed, transaction rolled back: {}",
                    e
                )));
            }
        }

        // Commit the transaction
        client
            .simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("SQL Server COMMIT TRANSACTION failed: {e}")))?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            let _ = client.close().await;
        }
        self.client = None;
        info!("SQL Server connection closed successfully");
        Ok(())
    }
}

impl Default for SqlServerDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlserver_destination_creation() {
        let destination = SqlServerDestination::new();
        assert!(destination.client.is_none());
    }
}
