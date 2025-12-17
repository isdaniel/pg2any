//! Function-based destination handler
//!
//! This module provides a generic destination handler that wraps async functions,
//! allowing users to implement custom CDC event processing without implementing
//! a full database driver. This is ideal for use cases like:
//!
//! - Calling external REST APIs with CDC events
//! - Writing to message queues or event streams
//! - Custom data transformations or filtering
//! - Webhook notifications
//! - Audit logging to specialized systems
//!
//! # Example
//!
//! ```rust
//! use pg2any_lib::destinations::{FunctionDestination, DestinationHandler};
//! use pg2any_lib::types::Transaction;
//! use pg2any_lib::error::Result;
//!
//! // Create a custom handler that calls an API
//! let handler = FunctionDestination::new(|transaction: Transaction| async move {
//!     // Call your API with the transaction data
//!     let client = reqwest::Client::new();
//!     client.post("https://api.example.com/cdc-events")
//!         .json(&transaction)
//!         .send()
//!         .await
//!         .map_err(|e| pg2any_lib::error::CdcError::generic(e.to_string()))?;
//!     Ok(())
//! });
//! ```

use crate::{
    error::{CdcError, Result},
    types::Transaction,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Type alias for the async function that processes transactions
pub type TransactionProcessorFn =
    dyn Fn(Transaction) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync;

/// A destination handler that wraps an async function
///
/// This allows users to implement custom CDC event processing by providing
/// an async function that receives transactions and processes them however needed.
///
/// The function receives complete `Transaction` objects with all events and can:
/// - Make HTTP API calls
/// - Write to message queues
/// - Transform and forward data
/// - Implement custom business logic
/// - Combine multiple downstream operations
pub struct FunctionDestination {
    /// The async function that processes transactions
    processor: Arc<TransactionProcessorFn>,
    /// Schema mappings (stored but not used by function destination)
    schema_mappings: HashMap<String, String>,
    /// Connection state flag
    connected: bool,
    /// Active streaming transaction ID (for streaming transaction support)
    active_streaming_transaction_id: Option<u32>,
}

impl FunctionDestination {
    /// Create a new function-based destination handler
    ///
    /// # Arguments
    ///
    /// * `processor` - An async function that takes a `Transaction` and returns `Result<()>`
    ///
    /// # Example
    ///
    /// ```rust
    /// use pg2any_lib::destinations::FunctionDestination;
    /// use pg2any_lib::types::Transaction;
    ///
    /// let handler = FunctionDestination::new(|transaction| async move {
    ///     println!("Processing transaction with {} events", transaction.events.len());
    ///     Ok(())
    /// });
    /// ```
    pub fn new<F, Fut>(processor: F) -> Self
    where
        F: Fn(Transaction) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let processor = Arc::new(move |transaction: Transaction| {
            Box::pin(processor(transaction)) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        });

        Self {
            processor,
            schema_mappings: HashMap::new(),
            connected: false,
            active_streaming_transaction_id: None,
        }
    }

    /// Create a new function-based destination with an Arc-wrapped processor
    ///
    /// This is useful when you want to share the same processor across multiple instances.
    pub fn with_arc_processor(processor: Arc<TransactionProcessorFn>) -> Self {
        Self {
            processor,
            schema_mappings: HashMap::new(),
            connected: false,
            active_streaming_transaction_id: None,
        }
    }
}

#[async_trait]
impl super::DestinationHandler for FunctionDestination {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        // Function destinations don't need traditional connections
        // We just mark as connected and optionally validate the connection string
        self.connected = true;
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
        if !self.connected {
            return Err(CdcError::generic(
                "Function destination not connected. Call connect() first.",
            ));
        }

        // Call the user-provided async function
        (self.processor)(transaction.clone()).await
    }

    async fn process_streaming_batch(&mut self, transaction: &Transaction) -> Result<()> {
        if !self.connected {
            return Err(CdcError::generic(
                "Function destination not connected. Call connect() first.",
            ));
        }

        // For streaming transactions, track the active transaction ID
        if transaction.is_streaming {
            self.active_streaming_transaction_id = Some(transaction.transaction_id);
        }

        // Process the batch
        (self.processor)(transaction.clone()).await?;

        // If this is the final batch, clear the active transaction ID
        if transaction.is_streaming && transaction.is_final_batch {
            self.active_streaming_transaction_id = None;
        }

        Ok(())
    }

    fn get_active_streaming_transaction_id(&self) -> Option<u32> {
        self.active_streaming_transaction_id
    }

    async fn commit_streaming_transaction(&mut self) -> Result<()> {
        // Function destinations handle commits within the processor function
        // Clear the active transaction ID
        self.active_streaming_transaction_id = None;
        Ok(())
    }

    async fn rollback_streaming_transaction(&mut self) -> Result<()> {
        // Function destinations handle rollbacks within the processor function
        // Clear the active transaction ID
        self.active_streaming_transaction_id = None;
        Ok(())
    }

    async fn health_check(&mut self) -> Result<bool> {
        Ok(self.connected)
    }

    async fn close(&mut self) -> Result<()> {
        self.connected = false;
        self.active_streaming_transaction_id = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destinations::DestinationHandler;
    use crate::types::{ChangeEvent, EventType};
    use chrono::Utc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_function_destination_basic() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let mut handler = FunctionDestination::new(move |transaction: Transaction| {
            let call_count = call_count_clone.clone();
            async move {
                call_count.fetch_add(1, Ordering::SeqCst);
                assert!(!transaction.events.is_empty());
                Ok(())
            }
        });

        // Connect
        handler.connect("").await.unwrap();

        // Create a test transaction
        let commit_time = Utc::now();
        let transaction = Transaction {
            transaction_id: 1,
            commit_timestamp: commit_time,
            commit_lsn: None,
            events: vec![ChangeEvent {
                event_type: EventType::Begin {
                    transaction_id: 1,
                    commit_timestamp: commit_time,
                },
                lsn: None,
                metadata: None,
            }],
            is_streaming: false,
            is_final_batch: true,
        };

        // Process transaction
        handler.process_transaction(&transaction).await.unwrap();

        // Verify the function was called
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_function_destination_streaming() {
        let mut handler = FunctionDestination::new(|_transaction: Transaction| async move {
            Ok(())
        });

        handler.connect("").await.unwrap();

        // Create a streaming transaction batch
        let commit_time = Utc::now();
        let transaction = Transaction {
            transaction_id: 42,
            commit_timestamp: commit_time,
            commit_lsn: None,
            events: vec![],
            is_streaming: true,
            is_final_batch: false,
        };

        // Process batch
        handler.process_streaming_batch(&transaction).await.unwrap();

        // Verify active transaction is tracked
        assert_eq!(handler.get_active_streaming_transaction_id(), Some(42));

        // Process final batch
        let final_transaction = Transaction {
            transaction_id: 42,
            commit_timestamp: commit_time,
            commit_lsn: None,
            events: vec![],
            is_streaming: true,
            is_final_batch: true,
        };

        handler
            .process_streaming_batch(&final_transaction)
            .await
            .unwrap();

        // Verify transaction is cleared
        assert_eq!(handler.get_active_streaming_transaction_id(), None);
    }

    #[tokio::test]
    async fn test_function_destination_error_handling() {
        let mut handler = FunctionDestination::new(|_transaction: Transaction| async move {
            Err(CdcError::generic("Test error"))
        });

        handler.connect("").await.unwrap();

        let transaction = Transaction {
            transaction_id: 1,
            commit_timestamp: Utc::now(),
            commit_lsn: None,
            events: vec![],
            is_streaming: false,
            is_final_batch: true,
        };

        // Verify error is propagated
        let result = handler.process_transaction(&transaction).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_function_destination_not_connected() {
        let mut handler = FunctionDestination::new(|_transaction: Transaction| async move {
            Ok(())
        });

        // Don't call connect()

        let transaction = Transaction {
            transaction_id: 1,
            commit_timestamp: Utc::now(),
            commit_lsn: None,
            events: vec![],
            is_streaming: false,
            is_final_batch: true,
        };

        // Verify error when not connected
        let result = handler.process_transaction(&transaction).await;
        assert!(result.is_err());
    }
}
