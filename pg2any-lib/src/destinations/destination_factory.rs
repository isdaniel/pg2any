use crate::{
    error::{CdcError, Result},
    types::DestinationType,
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
/// - Transactions are written to files in sql_data_tx/ as events arrive
/// - Metadata for completed transactions are stored in sql_pending_tx/ on COMMIT/StreamCommit
/// - Consumer reads metadata and executes SQL via `execute_sql_batch()` in atomic transactions
/// - No database transactions are kept open between batches
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    /// Initialize the destination connection
    async fn connect(&mut self, connection_string: &str) -> Result<()>;

    /// Set schema mappings for translating source schemas to destination schemas/databases
    /// Maps source schema (e.g., PostgreSQL "public") to destination schema/database (e.g., MySQL "cdc_db")
    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>);

    /// Execute a batch of SQL commands within a single transaction with optional pre-commit hook
    ///
    /// This is the primary method used by the consumer to execute transaction files.
    /// All commands are executed atomically - if any command fails, the entire batch is rolled back.
    ///
    /// ## Transactional Checkpoint Strategy
    ///
    /// The `pre_commit_hook` enables true atomicity between data changes and checkpoint updates:
    /// ```ignore
    /// BEGIN;
    ///   INSERT INTO users ...;  // Your CDC data
    ///   UPDATE products ...;
    ///   
    ///   pre_commit_hook();      // Update checkpoint (in-memory + persist to file)
    /// COMMIT;                   // Both data AND checkpoint committed atomically
    /// ```
    ///
    /// If transaction fails or crashes:
    /// - Before COMMIT: Both data and checkpoint rolled back → Safe
    /// - After COMMIT: Both data and checkpoint committed → Safe
    /// - No race condition possible!
    ///
    /// # Arguments
    /// * `commands` - Vector of SQL commands to execute in a single transaction
    /// * `pre_commit_hook` - Optional boxed async function called BEFORE COMMIT within the transaction
    ///
    /// # Returns
    /// * `Ok(())` - All SQL commands and pre-commit hook executed successfully, transaction committed
    /// * `Err(...)` - Execution or hook failed, entire transaction was rolled back
    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<
            Box<
                dyn FnOnce() -> std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<()>> + Send>,
                    > + Send,
            >,
        >,
    ) -> Result<()>;

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
                "Destination type {destination_type:?} is not supported or not enabled"
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
