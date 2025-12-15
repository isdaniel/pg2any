use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::types::{EventType, Lsn, Transaction};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Main CDC client for coordinating replication and destination writes
///
/// # Transaction-Based Processing Architecture
///
/// This client uses a producer-consumer pattern where workers process complete
/// PostgreSQL transactions atomically:
///
/// ## Single Producer (PostgreSQL Reader)
/// - Reads from PostgreSQL logical replication stream
/// - Collects events from BEGIN to COMMIT into Transaction objects
/// - Sends complete transactions to a bounded channel
/// - Handles LSN tracking and heartbeats
///
/// ## Multiple Consumers (Destination Writers)
/// - Configurable number of worker threads (CDC_CONSUMER_WORKERS)
/// - **Each worker has its own destination database connection**
/// - **Each worker processes complete transactions atomically**
/// - Workers use a work-stealing pattern via shared channel
/// - TRUE parallelism: workers can write to destination simultaneously
/// - Transaction consistency: all events in a transaction succeed or fail together
///
/// ## Performance Characteristics
///
/// ### With 1 Worker (default):
/// - Sequential transaction processing
/// - Single connection overhead
/// - Predictable order of transactions
///
/// ### With Multiple Workers (e.g., 8):
/// - Parallel transaction writes to destination database
/// - Different transactions can be processed concurrently
/// - Transaction order within a single transaction is preserved
/// - Best for destinations that support concurrent connections (MySQL, PostgreSQL)
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    transaction_sender: Option<mpsc::Sender<Transaction>>,
    transaction_receiver: Option<mpsc::Receiver<Transaction>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    metrics_collector: Arc<MetricsCollector>,
}

impl CdcClient {
    /// Create a new CDC client
    pub async fn new(config: Config) -> Result<Self> {
        info!("Creating CDC client");

        // Create destination handler
        let destination_handler = DestinationFactory::create(config.destination_type.clone())?;

        let replication_manager = ReplicationManager::new(config.clone());

        let (transaction_sender, transaction_receiver) = mpsc::channel(config.buffer_size);

        Ok(Self {
            config,
            replication_manager: Some(replication_manager),
            destination_handler: Some(destination_handler),
            transaction_sender: Some(transaction_sender),
            transaction_receiver: Some(transaction_receiver),
            cancellation_token: CancellationToken::new(),
            producer_handle: None,
            consumer_handles: Vec::new(),
            metrics_collector: Arc::new(MetricsCollector::new()),
        })
    }

    /// Initialize the CDC client
    pub async fn init(&mut self) -> Result<()> {
        info!("Initializing CDC client");

        // Connect to destination database
        if let Some(ref mut handler) = self.destination_handler {
            handler
                .connect(&self.config.destination_connection_string)
                .await?;

            // Set schema mappings if any are configured
            if !self.config.schema_mappings.is_empty() {
                handler.set_schema_mappings(self.config.schema_mappings.clone());
                info!("Schema mappings applied: {:?}", self.config.schema_mappings);
            }
        }

        info!("CDC client initialized successfully");
        Ok(())
    }

    /// Start CDC replication from a specific LSN
    pub async fn start_replication_from_lsn(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication");

        // Ensure we're initialized
        self.init().await?;

        // Create replication stream using async method
        let replication_manager = self
            .replication_manager
            .take()
            .ok_or_else(|| CdcError::generic("Replication manager not available"))?;

        let mut replication_stream = replication_manager.create_stream_async().await?;

        // Start the replication stream
        replication_stream.start(start_lsn).await?;

        // Start the producer task (reads from PostgreSQL and builds transactions)
        let transaction_sender = self
            .transaction_sender
            .take()
            .ok_or_else(|| CdcError::generic("Transaction sender not available"))?;

        let producer_handle = {
            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let start_lsn = start_lsn.unwrap_or_else(|| Lsn::new(0));

            tokio::spawn(Self::run_producer(
                replication_stream,
                transaction_sender,
                token,
                start_lsn,
                metrics,
            ))
        };

        // Start consumer worker tasks with per-worker destination connections
        let num_workers = self.config.consumer_workers;
        let transaction_receiver = self
            .transaction_receiver
            .take()
            .ok_or_else(|| CdcError::generic("Transaction receiver not available"))?;

        // // Discard the pre-created destination handler from init() since we'll create per-worker handlers
        // let _ = self.destination_handler.take();

        let dest_type = &self.config.destination_type;
        let dest_connection_string = &self.config.destination_connection_string;
        let schema_mappings = self.config.schema_mappings.clone();

        info!(
            "Starting {} consumer worker(s) for transaction-based parallel processing",
            num_workers
        );

        // Share the receiver among all workers for work-stealing pattern
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(transaction_receiver));

        for worker_id in 0..num_workers {
            // Create a new destination handler for each worker
            let mut worker_destination = DestinationFactory::create(dest_type.clone())?;

            // Connect the worker's destination handler
            worker_destination.connect(dest_connection_string).await?;

            // Apply schema mappings to the worker's handler
            if !schema_mappings.is_empty() {
                worker_destination.set_schema_mappings(schema_mappings.clone());
            }

            info!("Worker {} destination connection established", worker_id);

            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let dest_type_str = dest_type.to_string();
            let shared_recv = shared_receiver.clone();

            let consumer_handle = tokio::spawn(Self::run_consumer_loop(
                worker_id,
                shared_recv,
                worker_destination,
                token,
                metrics,
                dest_type_str,
            ));
            self.consumer_handles.push(consumer_handle);
        }

        // Update metrics for active connections
        self.metrics_collector
            .update_active_connections(num_workers, "consumer");

        self.producer_handle = Some(producer_handle);

        self.start_server_uptime();

        info!(
            "CDC replication started successfully with {} consumer worker(s)",
            num_workers
        );
        self.cancellation_token.cancelled().await;
        Ok(())
    }

    // Start metrics update task
    fn start_server_uptime(&mut self) {
        let metrics = self.metrics_collector.clone();
        let token = self.cancellation_token.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = interval.tick() => {
                        metrics.update_uptime();
                        metrics.update_events_rate();
                    }
                }
            }
        });
    }

    /// Producer task: reads events from PostgreSQL replication stream and builds transactions
    ///
    /// This producer collects events between BEGIN and COMMIT into complete Transaction objects,
    /// ensuring that each worker processes an entire transaction atomically.
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        transaction_sender: mpsc::Sender<Transaction>,
        cancellation_token: CancellationToken,
        start_lsn: Lsn,
        metrics_collector: Arc<MetricsCollector>,
    ) -> Result<()> {
        info!("Starting replication producer (transaction mode)");

        // Initialize connection status
        metrics_collector.update_source_connection_status(true);

        // Current transaction being built
        let mut current_transaction: Option<Transaction> = None;

        while !cancellation_token.is_cancelled() {
            match replication_stream.next_event(&cancellation_token).await {
                Ok(Some(event)) => {
                    if let Some(current_lsn) = event.lsn {
                        if current_lsn <= start_lsn {
                            debug!("Skipping event with LSN {} <= {}", current_lsn, start_lsn);
                            continue;
                        }

                        // Record current LSN
                        metrics_collector.record_received_lsn(current_lsn.0);
                    }

                    // Handle transaction boundaries
                    match &event.event_type {
                        EventType::Begin {
                            transaction_id,
                            commit_timestamp,
                        } => {
                            // Start a new transaction
                            if current_transaction.is_some() {
                                warn!("Received BEGIN while transaction is in progress, discarding incomplete transaction");
                            }
                            debug!("Starting transaction {} commit_timestamp: {}", transaction_id, commit_timestamp);
                            current_transaction =
                                Some(Transaction::new(*transaction_id, *commit_timestamp));
                        }

                        EventType::Commit { .. } => {
                            // Complete and send the transaction
                            if let Some(mut tx) = current_transaction.take() {
                                if let Some(lsn) = event.lsn {
                                    tx.set_commit_lsn(lsn);
                                }

                                // Send the completed transaction
                                if let Err(e) = transaction_sender.send(tx).await {
                                    error!("Failed to send transaction to consumer: {}", e);
                                    metrics_collector
                                        .record_error("transaction_send_failed", "producer");
                                    break;
                                }
                            } else {
                                warn!("Received COMMIT without BEGIN, ignoring");
                            }
                        }

                        // Add DML events to the current transaction
                        EventType::Insert { .. }
                        | EventType::Update { .. }
                        | EventType::Delete { .. }
                        | EventType::Truncate(_) => {
                            if let Some(ref mut tx) = current_transaction {
                                tx.add_event(event);
                            } else {
                                // Event outside of transaction - create implicit transaction, this handles edge cases where we might receive events without BEGIN
                                warn!("Received DML event outside of transaction context, creating implicit transaction");
                                let mut implicit_tx = Transaction::new(0, chrono::Utc::now());
                                if let Some(lsn) = event.lsn {
                                    implicit_tx.set_commit_lsn(lsn);
                                }
                                implicit_tx.add_event(event);

                                if let Err(e) = transaction_sender.send(implicit_tx).await {
                                    error!("Failed to send implicit transaction: {}", e);
                                    metrics_collector
                                        .record_error("transaction_send_failed", "producer");
                                    break;
                                }
                            }
                        }

                        // Skip metadata events (Relation, Type, Origin, Message)
                        _ => {
                            debug!("Skipping metadata event: {:?}", event.event_type);
                        }
                    }
                }
                Ok(None) => {
                    // No event available
                    if cancellation_token.is_cancelled() {
                        info!("Producer received cancellation signal");
                        break;
                    }
                }
                Err(e) => {
                    error!("Error reading from replication stream: {}", e);
                    metrics_collector.record_error("replication_stream_error", "producer");
                    break;
                }
            }
        }

        info!("Producer shutting down");

        // If there's an incomplete transaction, log a warning
        if let Some(tx) = current_transaction.take() {
            warn!(
                "Discarding incomplete transaction {} with {} events due to shutdown",
                tx.transaction_id,
                tx.event_count()
            );
        }

        // Update connection status on shutdown
        metrics_collector.update_source_connection_status(false);

        // Gracefully stop the replication stream
        replication_stream.stop().await?;

        info!("Replication producer stopped gracefully");
        Ok(())
    }

    /// Unified consumer loop for transaction processing (work-stealing pattern with per-worker connections)
    ///
    /// Each worker receives complete transactions and processes them atomically.
    /// This ensures that all events within a transaction are applied together.
    ///
    /// # Arguments
    /// * `worker_id` - Identifier for this worker (0 for single worker mode)
    /// * `transaction_receiver` - Shared receiver for transactions (work-stealing)
    /// * `destination_handler` - Per-worker destination handler (no mutex needed!)
    /// * `cancellation_token` - Token for graceful shutdown
    /// * `metrics_collector` - Metrics collector
    /// * `destination_type` - Type of destination for metrics labeling
    async fn run_consumer_loop(
        worker_id: usize,
        transaction_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<Transaction>>>,
        mut destination_handler: Box<dyn DestinationHandler>,
        cancellation_token: CancellationToken,
        metrics_collector: Arc<MetricsCollector>,
        destination_type: String,
    ) -> Result<()> {
        info!("Starting consumer worker {}", worker_id);

        // Update destination connection status for this worker
        metrics_collector.update_destination_connection_status(&format!("{}:worker_{}", destination_type, worker_id), true);

        let mut queue_size_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                biased;

                // Handle graceful shutdown
                _ = cancellation_token.cancelled() => {
                    info!("Consumer worker {} received cancellation signal", worker_id);

                    Self::drain_remaining_transactions(
                        &transaction_receiver,
                        &mut destination_handler,
                        &metrics_collector,
                        &destination_type,
                        worker_id,
                    ).await;
                    break;
                }

                // Periodic queue size reporting for this worker
                _ = queue_size_interval.tick() => {
                    let receiver = transaction_receiver.lock().await;
                    let queue_length = receiver.len();
                    debug!("Consumer worker {} transaction queue length: {}", worker_id, queue_length);
                    metrics_collector.update_consumer_queue_length(queue_length);
                }

                // Efficiently receive and process transactions (blocking recv, not polling!)
                transaction = async {
                    let mut receiver = transaction_receiver.lock().await;
                    receiver.recv().await
                } => {
                    match transaction {
                        Some(tx) => {
                            debug!(
                                "Consumer worker {} processing transaction {} with {} events",
                                worker_id, tx.transaction_id, tx.event_count()
                            );
                            Self::process_transaction_per_worker(
                                tx,
                                &mut destination_handler,
                                &metrics_collector,
                                &destination_type,
                                worker_id,
                            ).await;
                        }
                        None => {
                            // Channel closed, break loop
                            info!("Consumer worker {} detected channel closure", worker_id);
                            break;
                        }
                    }
                }
            }
        }

        // Close the worker's destination connection
        if let Err(e) = destination_handler.close().await {
            error!(
                "Worker {} failed to close destination connection: {}",
                worker_id, e
            );
        }

        metrics_collector.update_destination_connection_status(&format!("{}:worker_{}", destination_type, worker_id), false);

        info!("Consumer worker {} stopped gracefully", worker_id);
        Ok(())
    }

    /// Drain any remaining transactions from the channel during shutdown
    async fn drain_remaining_transactions(
        transaction_receiver: &Arc<tokio::sync::Mutex<mpsc::Receiver<Transaction>>>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
        worker_id: usize,
    ) {
        let mut count = 0;
        loop {
            let tx = {
                let mut receiver = transaction_receiver.lock().await;
                receiver.try_recv().ok()
            };

            match tx {
                Some(transaction) => {
                    debug!(
                        "Processing remaining transaction {} during shutdown",
                        transaction.transaction_id
                    );
                    Self::process_transaction_per_worker(
                        transaction,
                        destination_handler,
                        metrics_collector,
                        destination_type,
                        worker_id,
                    )
                    .await;
                    count += 1;
                }
                None => break,
            }
        }

        info!(
            "Worker {} processed {} remaining transactions during graceful shutdown",
            worker_id, count
        );
    }

    /// Process a complete transaction with per-worker destination handler
    async fn process_transaction_per_worker(
        transaction: Transaction,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
        worker_id: usize,
    ) {
        let start_time = std::time::Instant::now();
        let event_count = transaction.event_count();

        // Process transaction atomically - each worker has its own handler!
        match destination_handler.process_transaction(&transaction).await {
            Ok(_) => {
                let duration = start_time.elapsed();
                metrics_collector.record_transaction_processed(&transaction, destination_type);
                debug!(
                    "Worker {} successfully processed transaction {} ({} events) in {:?}",
                    worker_id, transaction.transaction_id, event_count, duration
                );
            }
            Err(e) => {
                error!(
                    "Worker {} failed to process transaction {}: {}",
                    worker_id, transaction.transaction_id, e
                );
                metrics_collector.record_error("transaction_processing_failed", "consumer");
            }
        }
    }

    /// Stop the CDC replication process gracefully
    pub async fn stop(&mut self) -> Result<()> {
        info!("Initiating graceful shutdown of CDC replication");

        // Signal cancellation to all tasks
        self.cancellation_token.cancel();

        // Wait for both tasks to complete gracefully
        self.wait_for_tasks_completion().await?;

        // Close destination connection
        if let Some(ref mut handler) = self.destination_handler {
            handler.close().await?;
        }

        info!("CDC replication stopped gracefully");
        Ok(())
    }

    async fn wait_handle(handle: Option<JoinHandle<Result<()>>>, name: &str) -> Result<()> {
        if let Some(h) = handle {
            h.await.expect(&format!("{} task panicked", name))?;
            info!("{} task completed successfully", name);
        }
        Ok(())
    }

    /// Wait for producer and consumer tasks to complete gracefully
    pub async fn wait_for_tasks_completion(&mut self) -> Result<()> {
        let producer_task = Self::wait_handle(self.producer_handle.take(), "Producer");

        // Wait for all consumer workers
        let consumer_handles: Vec<_> = self.consumer_handles.drain(..).collect();
        let num_workers = consumer_handles.len();

        let consumer_tasks = async {
            let mut results = Vec::new();
            for (idx, handle) in consumer_handles.into_iter().enumerate() {
                let result = handle.await;
                match result {
                    Ok(Ok(_)) => {
                        debug!("Consumer worker {} completed successfully", idx);
                        results.push(Ok(()));
                    }
                    Ok(Err(e)) => {
                        error!("Consumer worker {} failed: {}", idx, e);
                        results.push(Err(e));
                    }
                    Err(e) => {
                        error!("Consumer worker {} panicked: {}", idx, e);
                        results.push(Err(CdcError::generic(format!(
                            "Consumer worker {} panicked",
                            idx
                        ))));
                    }
                }
            }
            // Return first error if any
            for result in results {
                result?;
            }
            Ok::<(), CdcError>(())
        };

        match tokio::join!(producer_task, consumer_tasks) {
            (Ok(_), Ok(_)) => {
                info!(
                    "All CDC tasks completed successfully!! ({} consumer workers)",
                    num_workers.max(1)
                );
            }
            (Err(e), _) | (_, Err(e)) => {
                error!("Task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Check if the CDC client is currently running
    #[inline]
    pub fn is_running(&self) -> bool {
        !self.cancellation_token.is_cancelled()
    }

    /// Get the cancellation token for external shutdown coordination
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Get the current configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get metrics collector for accessing metrics
    pub fn metrics_collector(&self) -> Arc<MetricsCollector> {
        self.metrics_collector.clone()
    }

    /// Get metrics in Prometheus text format
    pub fn get_metrics(&self) -> Result<String> {
        self.metrics_collector.get_metrics()
    }

    /// Initialize build information in metrics
    pub fn init_build_info(&self, version: &str) {
        self.metrics_collector.init_build_info(version);
    }

    /// Health check for all components
    pub async fn health_check(&mut self) -> Result<bool> {
        // Check destination connection
        if let Some(ref mut handler) = self.destination_handler {
            if !handler.health_check().await? {
                self.metrics_collector.update_destination_connection_status(
                    &self.config.destination_type.to_string(),
                    false,
                );
                return Ok(false);
            } else {
                self.metrics_collector.update_destination_connection_status(
                    &self.config.destination_type.to_string(),
                    true,
                );
            }
        }

        Ok(true)
    }

    /// Get replication statistics
    pub fn get_stats(&self) -> ReplicationStats {
        ReplicationStats {
            is_running: self.is_running(),
            events_processed: 0, // In a real implementation, you'd track this
            last_processed_lsn: None,
            lag_seconds: None,
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub is_running: bool,
    pub events_processed: u64,
    pub last_processed_lsn: Option<Lsn>,
    pub lag_seconds: Option<f64>,
}

impl Drop for CdcClient {
    fn drop(&mut self) {
        // Note: This is a synchronous drop, so we can't call async methods here
        // In a production system, you might want to ensure graceful shutdown
        debug!("CDC client dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigBuilder;
    use crate::types::{ChangeEvent, Transaction};
    use std::time::Duration;
    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Clone, PartialEq)]
    pub struct ProcessedTransactionInfo {
        pub transaction_id: u32,
        pub commit_lsn: Option<Lsn>,
        pub event_count: usize,
    }

    impl From<&Transaction> for ProcessedTransactionInfo {
        fn from(tx: &Transaction) -> Self {
            Self {
                transaction_id: tx.transaction_id,
                commit_lsn: tx.commit_lsn,
                event_count: tx.event_count(),
            }
        }
    }

    // Mock destination handler for testing
    pub struct MockDestinationHandler {
        pub transactions_processed: std::sync::Arc<std::sync::Mutex<Vec<ProcessedTransactionInfo>>>,
        pub should_fail: bool,
        pub processing_delay: Duration,
    }

    #[async_trait::async_trait]
    impl DestinationHandler for MockDestinationHandler {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: std::collections::HashMap<String, String>) {
            // Mock implementation - no-op
        }

        async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
            if self.processing_delay > Duration::ZERO {
                sleep(self.processing_delay).await;
            }

            if self.should_fail {
                return Err(CdcError::generic("Mock error"));
            }

            let mut transactions = self.transactions_processed.lock().unwrap();
            transactions.push(ProcessedTransactionInfo::from(transaction));
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        async fn health_check(&mut self) -> Result<bool> {
            Ok(true)
        }
    }

    fn create_test_config() -> Config {
        ConfigBuilder::default()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(crate::DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .protocol_version(2)
            .binary_format(false)
            .streaming(true)
            .connection_timeout(Duration::from_secs(10))
            .query_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .buffer_size(500)
            .build()
            .expect("Failed to build test config")
    }

    fn create_test_event() -> ChangeEvent {
        ChangeEvent::insert(
            "public".to_string(),
            "test_table".to_string(),
            12345,
            std::collections::HashMap::new(),
        )
    }

    fn create_test_transaction() -> Transaction {
        let mut tx = Transaction::new(1, chrono::Utc::now());
        tx.add_event(create_test_event());
        tx
    }

    fn create_test_transaction_with_lsn(lsn: u64) -> Transaction {
        let mut tx = Transaction::new(1, chrono::Utc::now());
        let mut event = create_test_event();
        event.lsn = Some(Lsn::new(lsn));
        tx.add_event(event);
        tx.set_commit_lsn(Lsn::new(lsn));
        tx
    }

    #[tokio::test]
    async fn test_client_creation_and_basic_properties() {
        let config = create_test_config();
        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Test that the client is initially not running (not cancelled)
        assert!(client.is_running());

        // Test that we can get a cancellation token
        let token = client.cancellation_token();
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_token_cancellation() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        let token = client.cancellation_token();
        assert!(!token.is_cancelled());

        // Cancel the token
        client.stop().await.expect("Failed to stop client");

        // The token should be cancelled
        assert!(token.is_cancelled());
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_cancellation_token_propagation() {
        let config = create_test_config();
        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        let token1 = client.cancellation_token();
        let token2 = client.cancellation_token();

        // Both tokens should not be cancelled initially
        assert!(!token1.is_cancelled());
        assert!(!token2.is_cancelled());

        // Cancel the first token
        token1.cancel();

        // Both tokens should be cancelled since they're clones
        assert!(token1.is_cancelled());
        assert!(token2.is_cancelled());
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_producer_task_cancellation() {
        let (_tx_sender, _tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();

        let token_clone = cancellation_token.clone();

        let producer_task = tokio::spawn(async move {
            // Simulate the producer loop structure
            loop {
                tokio::select! {
                    biased;
                    _ = token_clone.cancelled() => {
                        info!("Producer received cancellation signal");
                        break;
                    }
                    _ = sleep(Duration::from_millis(10)) => {
                        // Simulate waiting for events
                        continue;
                    }
                }
            }
            Ok::<(), CdcError>(())
        });

        // Let the producer run for a bit
        sleep(Duration::from_millis(50)).await;

        // Cancel the token
        cancellation_token.cancel();

        // The producer should complete quickly after cancellation
        let result = timeout(Duration::from_millis(100), producer_task)
            .await
            .expect("Producer task should complete quickly after cancellation")
            .expect("Producer task should not panic");

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consumer_task_cancellation() {
        let (_tx_sender, tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        let token_clone = cancellation_token.clone();
        let metrics_collector = Arc::new(MetricsCollector::new());

        // Use new per-worker API - pass Box directly, not Arc<Mutex>
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));
        let boxed_handler: Box<dyn DestinationHandler> = mock_handler;

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                0,
                shared_receiver,
                boxed_handler,
                token_clone,
                metrics_collector,
                "test".to_string(),
            )
            .await
        });

        // Let the consumer run for a bit
        sleep(Duration::from_millis(50)).await;

        // Cancel the token
        cancellation_token.cancel();

        // The consumer should complete quickly after cancellation
        let result = timeout(Duration::from_millis(100), consumer_task)
            .await
            .expect("Consumer task should complete quickly after cancellation")
            .expect("Consumer task should not panic");

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consumer_processes_remaining_transactions_on_shutdown() {
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: false,
            processing_delay: Duration::from_millis(10), // Small delay to simulate processing
        });

        // Send some test transactions
        for _ in 0..3 {
            tx_sender
                .send(create_test_transaction())
                .await
                .expect("Failed to send transaction");
        }

        let token_clone = cancellation_token.clone();
        let tx_clone = transactions_processed.clone();
        let metrics_collector = Arc::new(MetricsCollector::new());

        // Use new per-worker API - pass Box directly, not Arc<Mutex>
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));
        let boxed_handler: Box<dyn DestinationHandler> = mock_handler;

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                0,
                shared_receiver,
                boxed_handler,
                token_clone,
                metrics_collector,
                "test".to_string(),
            )
            .await
        });

        // Give the consumer time to start processing
        sleep(Duration::from_millis(50)).await;

        // Cancel the token
        cancellation_token.cancel();

        // Wait for the consumer to complete
        let result = timeout(Duration::from_secs(1), consumer_task)
            .await
            .expect("Consumer task should complete within timeout")
            .expect("Consumer task should not panic");

        assert!(result.is_ok());

        // Check that some transactions were processed
        let processed_transactions = tx_clone.lock().unwrap();
        assert!(
            !processed_transactions.is_empty(),
            "Consumer should have processed some transactions before shutdown"
        );
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_task_handles() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Initially no task handles should be set
        assert!(client.producer_handle.is_none());
        assert!(client.consumer_handles.is_empty());

        // Test graceful shutdown without starting tasks
        client
            .stop()
            .await
            .expect("Stop should succeed even without tasks");
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_wait_for_tasks_completion_with_no_tasks() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Should not fail when no tasks are running
        client
            .wait_for_tasks_completion()
            .await
            .expect("Should succeed with no tasks");
    }

    #[tokio::test]
    async fn test_multiple_shutdown_calls_are_safe() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // First stop call
        client.stop().await.expect("First stop call should succeed");
        assert!(!client.is_running());

        // Second stop call should also succeed and not panic
        client
            .stop()
            .await
            .expect("Second stop call should succeed");
        assert!(!client.is_running());

        // Third stop call should also succeed
        client.stop().await.expect("Third stop call should succeed");
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_client_stats_reflect_cancellation_state() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Initially running (not cancelled)
        let stats = client.get_stats();
        assert!(stats.is_running);

        // Stop the client
        client.stop().await.expect("Failed to stop client");

        // Stats should reflect stopped state
        let stats = client.get_stats();
        assert!(!stats.is_running);
    }

    #[tokio::test]
    async fn test_cancellation_token_from_external_source() {
        let config = create_test_config();
        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Get the client's token
        let client_token = client.cancellation_token();

        // Create an external cancellation token
        let external_token = CancellationToken::new();

        // Create a task that links external cancellation to client cancellation
        let client_token_clone = client_token.clone();
        let external_token_clone = external_token.clone();
        let linking_task = tokio::spawn(async move {
            external_token_clone.cancelled().await;
            client_token_clone.cancel();
        });

        // Initially, neither should be cancelled
        assert!(!client_token.is_cancelled());
        assert!(!external_token.is_cancelled());
        assert!(client.is_running());

        // Cancel the external token
        external_token.cancel();

        // Wait for the linking to complete
        linking_task.await.expect("Linking task should complete");

        // Client token should now be cancelled
        assert!(client_token.is_cancelled());
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_per_worker_connection_architecture() {
        // Test that multiple workers each get their own destination handler
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(100);
        let cancellation_token = CancellationToken::new();
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));

        // Create multiple workers with their own handlers
        let num_workers = 3;
        let mut worker_handles = Vec::new();
        let mut tx_trackers = Vec::new();

        for worker_id in 0..num_workers {
            let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            tx_trackers.push(transactions_processed.clone());

            let mock_handler = Box::new(MockDestinationHandler {
                transactions_processed,
                should_fail: false,
                processing_delay: Duration::from_millis(5),
            });

            let token = cancellation_token.clone();
            let metrics = Arc::new(MetricsCollector::new());
            let receiver = shared_receiver.clone();

            let handle = tokio::spawn(async move {
                CdcClient::run_consumer_loop(
                    worker_id,
                    receiver,
                    mock_handler,
                    token,
                    metrics,
                    "test".to_string(),
                )
                .await
            });
            worker_handles.push(handle);
        }

        // Send test transactions
        let num_transactions = 30;
        for i in 0..num_transactions {
            let tx = create_test_transaction_with_lsn(i as u64);
            tx_sender
                .send(tx)
                .await
                .expect("Failed to send transaction");
        }

        // Let workers process
        sleep(Duration::from_millis(200)).await;

        // Cancel and wait
        cancellation_token.cancel();
        for handle in worker_handles {
            let result = timeout(Duration::from_secs(1), handle)
                .await
                .expect("Worker should complete")
                .expect("Worker should not panic");
            assert!(result.is_ok());
        }

        // Verify all transactions were processed
        let mut total_processed = 0;
        for tracker in &tx_trackers {
            let count = tracker.lock().unwrap().len();
            total_processed += count;
            println!("Worker processed {} transactions", count);
        }

        assert_eq!(
            total_processed, num_transactions,
            "All transactions should be processed by workers"
        );

        // Verify work was distributed (at least 2 workers should have processed transactions)
        let workers_with_transactions = tx_trackers
            .iter()
            .filter(|tracker| !tracker.lock().unwrap().is_empty())
            .count();
        assert!(
            workers_with_transactions >= 2,
            "Work should be distributed across multiple workers"
        );
    }

    #[tokio::test]
    async fn test_worker_independence_no_mutex_contention() {
        // Test that workers can process transactions independently without blocking each other
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(100);
        let cancellation_token = CancellationToken::new();
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));

        let num_workers = 4;
        let mut worker_handles = Vec::new();
        let processing_times = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        for worker_id in 0..num_workers {
            let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let times = processing_times.clone();

            let mock_handler = Box::new(MockDestinationHandler {
                transactions_processed,
                should_fail: false,
                processing_delay: Duration::from_millis(20), // Simulate slow writes
            });

            let token = cancellation_token.clone();
            let metrics = Arc::new(MetricsCollector::new());
            let receiver = shared_receiver.clone();

            let handle = tokio::spawn(async move {
                let start = std::time::Instant::now();
                let result = CdcClient::run_consumer_loop(
                    worker_id,
                    receiver,
                    mock_handler,
                    token,
                    metrics,
                    "test".to_string(),
                )
                .await;
                let elapsed = start.elapsed();
                times.lock().unwrap().push(elapsed);
                result
            });
            worker_handles.push(handle);
        }

        // Send transactions
        let num_transactions = 20;
        for _ in 0..num_transactions {
            tx_sender
                .send(create_test_transaction())
                .await
                .expect("Failed to send");
        }

        // Let workers process
        sleep(Duration::from_millis(150)).await;

        cancellation_token.cancel();
        for handle in worker_handles {
            timeout(Duration::from_secs(1), handle)
                .await
                .expect("Worker should complete")
                .expect("Worker should not panic")
                .expect("Worker should succeed");
        }

        // With 4 workers processing 20 transactions (each taking 20ms),
        // if they were sequential (mutex bottleneck), it would take 400ms+
        // With parallel processing, it should take much less (around 100-150ms)
        let times = processing_times.lock().unwrap();
        let max_time = times.iter().max().unwrap();
        println!("Max worker time: {:?}", max_time);

        // The key test: parallel processing should complete faster than sequential
        // Sequential would be: 20 transactions * 20ms = 400ms minimum
        // Parallel with 4 workers: ~100-150ms expected
        assert!(
            max_time.as_millis() < 300,
            "Parallel processing should be faster than sequential (got {}ms)",
            max_time.as_millis()
        );
    }

    #[tokio::test]
    async fn test_process_transaction_per_worker_direct_no_mutex() {
        // Test that process_transaction_per_worker doesn't use mutex (direct mutable access)
        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler: Box<dyn DestinationHandler> = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        // Need to use a mutable variable for the trait object
        let mut handler = mock_handler;
        let metrics = Arc::new(MetricsCollector::new());
        let transaction = create_test_transaction_with_lsn(12345);
        let expected_lsn = transaction.commit_lsn;

        // This should work with direct mutable reference (no mutex)
        CdcClient::process_transaction_per_worker(transaction, &mut handler, &metrics, "test", 0).await;

        // Verify transaction was processed
        let processed = transactions_processed.lock().unwrap();
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].commit_lsn, expected_lsn);
    }

    #[tokio::test]
    async fn test_drain_remaining_transactions_per_worker() {
        // Test that primary worker drains remaining transactions on shutdown
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(10);
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler: Box<dyn DestinationHandler> = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        // Need mutable variable for trait object
        let mut handler = mock_handler;

        // Send transactions before draining
        let num_transactions = 5;
        for _ in 0..num_transactions {
            tx_sender
                .send(create_test_transaction())
                .await
                .expect("Send failed");
        }
        drop(tx_sender); // Close sender

        let metrics = Arc::new(MetricsCollector::new());

        // Drain transactions
        CdcClient::drain_remaining_transactions(
            &shared_receiver,
            &mut handler,
            &metrics,
            "test",
            0,
        )
        .await;

        // Verify all transactions were drained and processed
        let processed = transactions_processed.lock().unwrap();
        assert_eq!(
            processed.len(),
            num_transactions,
            "All remaining transactions should be drained"
        );
    }

    #[tokio::test]
    async fn test_worker_error_handling() {
        // Test that worker continues processing even if some transactions fail
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(tx_receiver));

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: true, // Simulate errors
            processing_delay: Duration::ZERO,
        });

        let token = cancellation_token.clone();
        let metrics = Arc::new(MetricsCollector::new());

        let worker_handle = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                0,
                shared_receiver,
                mock_handler,
                token,
                metrics,
                "test".to_string(),
            )
            .await
        });

        // Send transactions (they will fail but worker should continue)
        for _ in 0..5 {
            tx_sender
                .send(create_test_transaction())
                .await
                .expect("Send failed");
        }

        sleep(Duration::from_millis(50)).await;
        cancellation_token.cancel();

        let result = timeout(Duration::from_secs(1), worker_handle)
            .await
            .expect("Worker should complete")
            .expect("Worker should not panic");

        assert!(result.is_ok(), "Worker should handle errors gracefully");
    }

    #[tokio::test]
    async fn test_configurable_buffer_size() {
        // Test that buffer_size configuration is respected
        let custom_buffer_size = 2000;
        let config = ConfigBuilder::default()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(crate::DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .buffer_size(custom_buffer_size)
            .build()
            .expect("Failed to build config");

        assert_eq!(config.buffer_size, custom_buffer_size);

        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Verify the client was created successfully with custom buffer
        assert_eq!(client.config().buffer_size, custom_buffer_size);
    }
}
