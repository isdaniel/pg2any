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
/// ## Streaming Transaction Support
///
/// For streaming transactions (protocol v2+), the handler supports keeping a database
/// transaction open across multiple batches from the same PostgreSQL transaction:
///
/// - `process_transaction()` - Unified method that handles both normal and streaming transactions
/// - `rollback_streaming_transaction()` - Rollback an open streaming transaction
/// - `get_active_streaming_transaction_id()` - Check if there's an open streaming transaction
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    /// Initialize the destination connection
    async fn connect(&mut self, connection_string: &str) -> Result<()>;

    /// Set schema mappings for translating source schemas to destination schemas/databases
    /// Maps source schema (e.g., PostgreSQL "public") to destination schema/database (e.g., MySQL "cdc_db")
    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>);

    /// Process a transaction (both normal and streaming)
    ///
    /// This unified method handles all transaction types:
    /// - **Normal transactions** (is_streaming=false): BEGIN → process → COMMIT atomically
    /// - **Streaming transactions** (is_streaming=true): Process in batches, commit on final batch
    ///
    /// For streaming transactions:
    /// - Opens a DB transaction on first batch (is_final_batch=false)
    /// - Processes intermediate batches without committing
    /// - Commits DB transaction on final batch (is_final_batch=true)
    ///
    /// # Arguments
    /// * `transaction` - Transaction or batch with flags indicating type and finality
    ///
    /// # Returns
    /// * `Ok(())` - Transaction/batch was successfully processed
    /// * `Err(...)` - Processing failed (streaming transactions should be rolled back)
    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()>;

    /// Check if there's an active streaming transaction
    ///
    /// Returns the transaction_id of the active streaming transaction, if any.
    /// This is used to ensure proper cleanup during shutdown.
    fn get_active_streaming_transaction_id(&self) -> Option<u32>;

    /// Rollback an active streaming transaction
    ///
    /// This is called when a StreamAbort is received or during error recovery/shutdown.
    /// Default implementation does nothing (for handlers that don't maintain open transactions).
    async fn rollback_streaming_transaction(&mut self) -> Result<()>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;
}

/// Factory for creating destination handlers
pub struct DestinationFactory;

impl DestinationFactory {
    /// Create a new destination handler for the specified type
    pub fn create(destination_type: DestinationType) -> Result<Box<dyn DestinationHandler>> {
        match destination_type {
            #[cfg(feature = "mysql")]
            DestinationType::MySQL => Ok(Box::new(MySQLDestination::new())),

            #[cfg(feature = "sqlserver")]
            DestinationType::SqlServer => Ok(Box::new(SqlServerDestination::new())),

            #[cfg(feature = "sqlite")]
            DestinationType::SQLite => Ok(Box::new(SQLiteDestination::new())),

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
            let result = DestinationFactory::create(DestinationType::MySQL);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let result = DestinationFactory::create(DestinationType::SqlServer);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlite")]
        {
            let result = DestinationFactory::create(DestinationType::SQLite);
            assert!(result.is_ok());
        }

        // Test unsupported destination type
        let result = DestinationFactory::create(DestinationType::PostgreSQL);
        assert!(result.is_err());
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
