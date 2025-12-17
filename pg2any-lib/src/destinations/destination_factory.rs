use crate::{
    error::{CdcError, Result},
    types::{DestinationType, Transaction},
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

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
/// - `process_streaming_batch()` - Process a batch, keeping the DB transaction open if not final
/// - `commit_streaming_transaction()` - Explicitly commit an open streaming transaction  
/// - `rollback_streaming_transaction()` - Rollback an open streaming transaction
/// - `has_active_streaming_transaction()` - Check if there's an open streaming transaction
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    /// Initialize the destination connection
    async fn connect(&mut self, connection_string: &str) -> Result<()>;

    /// Set schema mappings for translating source schemas to destination schemas/databases
    /// Maps source schema (e.g., PostgreSQL "public") to destination schema/database (e.g., MySQL "cdc_db")
    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>);

    /// Process a complete transaction atomically
    ///
    /// This method processes all events within a transaction as a single atomic unit.
    /// The destination should use database transactions to ensure all-or-nothing semantics.
    ///
    /// # Arguments
    /// * `transaction` - A complete transaction containing all events from BEGIN to COMMIT
    ///
    /// # Returns
    /// * `Ok(())` - Transaction was successfully applied
    /// * `Err(...)` - Transaction failed and was rolled back
    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()>;

    /// Process a streaming transaction batch
    ///
    /// For streaming transactions, this method processes events while keeping the database
    /// transaction open until the final batch is received. This ensures atomicity across
    /// all batches from the same PostgreSQL streaming transaction.
    ///
    /// # Arguments
    /// * `transaction` - A transaction batch (may be intermediate or final)
    ///
    /// # Behavior
    /// - If `transaction.is_streaming` is false, behaves like `process_transaction()`
    /// - If `transaction.is_streaming` is true and `is_final_batch` is false:
    ///   - Opens a new DB transaction if none is active for this transaction_id
    ///   - Processes events but does NOT commit
    /// - If `transaction.is_streaming` is true and `is_final_batch` is true:
    ///   - Processes events and commits the DB transaction
    ///
    /// Default implementation falls back to `process_transaction()` for compatibility.
    async fn process_streaming_batch(&mut self, transaction: &Transaction) -> Result<()> {
        self.process_transaction(transaction).await
    }

    /// Check if there's an active streaming transaction
    ///
    /// Returns the transaction_id of the active streaming transaction, if any.
    /// This is used to ensure proper cleanup during shutdown.
    fn get_active_streaming_transaction_id(&self) -> Option<u32>;

    /// Commit an active streaming transaction
    ///
    /// This is called when a StreamCommit is received and all batches have been processed.
    /// Default implementation does nothing (for handlers that don't maintain open transactions).
    async fn commit_streaming_transaction(&mut self) -> Result<()>;

    /// Rollback an active streaming transaction
    ///
    /// This is called when a StreamAbort is received or during error recovery/shutdown.
    /// Default implementation does nothing (for handlers that don't maintain open transactions).
    async fn rollback_streaming_transaction(&mut self) -> Result<()>;

    /// Check if the destination is healthy
    async fn health_check(&mut self) -> Result<bool>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;
}

/// Type alias for a factory function that creates destination handlers
pub type DestinationHandlerFactory = Arc<dyn Fn() -> Box<dyn DestinationHandler> + Send + Sync>;

/// Global registry for custom destination handlers
static CUSTOM_HANDLERS: OnceLock<RwLock<HashMap<String, DestinationHandlerFactory>>> = OnceLock::new();

/// Get or initialize the custom handlers registry
fn get_custom_handlers() -> &'static RwLock<HashMap<String, DestinationHandlerFactory>> {
    CUSTOM_HANDLERS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Factory for creating destination handlers
pub struct DestinationFactory;

impl DestinationFactory {
    /// Register a custom destination handler factory
    ///
    /// This allows users to register their own destination handlers that can be
    /// instantiated by name using `DestinationType::Custom(name)`.
    ///
    /// # Arguments
    ///
    /// * `name` - A unique name for this custom destination type
    /// * `factory` - A function that creates new instances of the handler
    ///
    /// # Example
    ///
    /// ```rust
    /// use pg2any_lib::destinations::{DestinationFactory, FunctionDestination};
    ///
    /// DestinationFactory::register_custom(
    ///     "my-api",
    ///     Box::new(|| {
    ///         Box::new(FunctionDestination::new(|transaction| async move {
    ///             // Custom processing logic
    ///             Ok(())
    ///         }))
    ///     })
    /// );
    /// ```
    pub fn register_custom<F>(name: impl Into<String>, factory: F)
    where
        F: Fn() -> Box<dyn DestinationHandler> + Send + Sync + 'static,
    {
        let name = name.into();
        let mut registry = get_custom_handlers()
            .write()
            .expect("Failed to acquire write lock on custom handlers registry");
        registry.insert(name, Arc::new(factory));
    }

    /// Register a custom destination handler instance directly
    ///
    /// This is a convenience method for registering a pre-configured handler.
    /// Note that the handler will be cloned for each worker, so it must be cloneable
    /// or you should use `register_custom` with a factory function instead.
    ///
    /// # Arguments
    ///
    /// * `name` - A unique name for this custom destination type
    /// * `_handler` - The handler instance to register (currently unused - use register_custom instead)
    pub fn register_handler(name: impl Into<String>, _handler: Box<dyn DestinationHandler>) {
        let name = name.into();
        let mut registry = get_custom_handlers()
            .write()
            .expect("Failed to acquire write lock on custom handlers registry");
        
        // Store the handler in an Arc so we can clone it
        // Note: This requires the handler to be cloneable or recreatable
        registry.insert(
            name.clone(),
            Arc::new(move || {
                // For now, we can't clone arbitrary trait objects
                // Users should use register_custom with a factory instead
                panic!("Handler for '{}' must be registered with register_custom factory function", name)
            }),
        );
    }

    /// Create a new destination handler for the specified type
    pub fn create(destination_type: DestinationType) -> Result<Box<dyn DestinationHandler>> {
        match destination_type {
            #[cfg(feature = "mysql")]
            DestinationType::MySQL => Ok(Box::new(MySQLDestination::new())),

            #[cfg(feature = "sqlserver")]
            DestinationType::SqlServer => Ok(Box::new(SqlServerDestination::new())),

            #[cfg(feature = "sqlite")]
            DestinationType::SQLite => Ok(Box::new(SQLiteDestination::new())),

            DestinationType::Custom(ref name) => {
                let registry = get_custom_handlers()
                    .read()
                    .expect("Failed to acquire read lock on custom handlers registry");
                
                if let Some(factory) = registry.get(name) {
                    Ok(factory())
                } else {
                    Err(CdcError::unsupported(format!(
                        "Custom destination type '{}' is not registered. Use DestinationFactory::register_custom() to register it.",
                        name
                    )))
                }
            }

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

        let custom_type = DestinationType::Custom("my-api".to_string());
        let custom_json = serde_json::to_string(&custom_type).unwrap();
        assert_eq!(custom_json, r#"{"Custom":"my-api"}"#);

        let custom_deserialized: DestinationType = serde_json::from_str(&custom_json).unwrap();
        assert_eq!(custom_deserialized, custom_type);
    }

    #[test]
    fn test_custom_destination_registration() {
        use super::super::FunctionDestination;

        // Register a custom destination
        DestinationFactory::register_custom("test-handler", || {
            Box::new(FunctionDestination::new(|_transaction| async move {
                Ok(())
            }))
        });

        // Try to create it
        let result = DestinationFactory::create(DestinationType::Custom("test-handler".to_string()));
        assert!(result.is_ok());

        // Try to create non-existent custom handler
        let result = DestinationFactory::create(DestinationType::Custom("nonexistent".to_string()));
        assert!(result.is_err());
    }
}
