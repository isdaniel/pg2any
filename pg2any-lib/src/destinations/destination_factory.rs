use crate::{
    error::{CdcError, Result},
    types::{DestinationType, Transaction},
};
use async_trait::async_trait;
use std::collections::HashMap;

// Import destination implementations
#[cfg(feature = "mysql")]
use super::mysql::MySQLDestination;

#[cfg(feature = "sqlserver")]
use super::sqlserver::SqlServerDestination;

#[cfg(feature = "sqlite")]
use super::sqlite::SQLiteDestination;

/// Trait for database destination handlers
///
/// Each worker has its own destination handler with its own connection.
/// Workers process complete transactions atomically to ensure data consistency.
///
/// ## File-Based Transaction Processing
///
/// The current architecture uses file-based transaction processing:
/// - Transactions are written to files in sql_received_tx/ as events arrive
/// - Completed transactions are moved to sql_pending_tx/ on COMMIT/StreamCommit
/// - Consumer reads files and executes SQL via `execute_sql_batch()` in atomic transactions
/// - No database transactions are kept open between batches
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    /// Initialize the destination connection
    async fn connect(&mut self, connection_string: &str) -> Result<()>;

    /// Set schema mappings for translating source schemas to destination schemas/databases
    /// Maps source schema (e.g., PostgreSQL "public") to destination schema/database (e.g., MySQL "cdc_db")
    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>);

    /// Process a transaction batch
    ///
    /// In the file-based architecture, this method processes batches read from transaction files.
    /// Each batch is executed atomically in its own database transaction and committed immediately.
    /// The distinction between streaming and normal transactions is no longer relevant at this level.
    ///
    /// # Arguments
    /// * `transaction` - Transaction batch to process
    ///
    /// # Returns
    /// * `Ok(())` - Transaction batch was successfully processed and committed
    /// * `Err(...)` - Processing failed
    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()>;

    /// Execute a batch of SQL commands within a single transaction
    ///
    /// This is the primary method used by the consumer to execute transaction files.
    /// All commands are executed atomically - if any command fails, the entire batch is rolled back.
    ///
    /// # Arguments
    /// * `commands` - Vector of SQL commands to execute in a single transaction
    ///
    /// # Returns
    /// * `Ok(())` - All SQL commands were successfully executed and committed
    /// * `Err(...)` - Execution failed, transaction was rolled back
    async fn execute_sql_batch(&mut self, commands: &[String]) -> Result<()>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;
}

/// Factory for creating destination handlers
pub struct DestinationFactory;

impl DestinationFactory {
    /// Create a new destination handler for the specified type
    pub fn create(destination_type: &DestinationType) -> Result<Box<dyn DestinationHandler>> {
        match *destination_type {
            #[cfg(feature = "mysql")]
            DestinationType::MySQL => Ok(Box::new(MySQLDestination::new())),

            #[cfg(feature = "sqlserver")]
            DestinationType::SqlServer => Ok(Box::new(SqlServerDestination::new())),

            #[cfg(feature = "sqlite")]
            DestinationType::SQLite => Ok(Box::new(SQLiteDestination::new())),

            #[allow(unreachable_patterns)]
            _ => Err(CdcError::unsupported(format!(
                "Destination type {:?} is not supported or not enabled",
                destination_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destination_factory_create() {
        // Test factory creation for different destination types
        #[cfg(feature = "mysql")]
        {
            let result = DestinationFactory::create(&DestinationType::MySQL);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let result = DestinationFactory::create(&DestinationType::SqlServer);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlite")]
        {
            let result = DestinationFactory::create(&DestinationType::SQLite);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_destination_types_serialization() {
        use serde_json;

        let mysql_type = DestinationType::MySQL;
        let json = serde_json::to_string(&mysql_type).unwrap();
        assert_eq!(json, "\"MySQL\"");

        let deserialized: DestinationType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, mysql_type);

        let sqlite_type = DestinationType::SQLite;
        let sqlite_json = serde_json::to_string(&sqlite_type).unwrap();
        assert_eq!(sqlite_json, "\"SQLite\"");

        let sqlite_deserialized: DestinationType = serde_json::from_str(&sqlite_json).unwrap();
        assert_eq!(sqlite_deserialized, sqlite_type);
    }
}
