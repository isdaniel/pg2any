use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::transaction_manager::{TransactionContext, TransactionManager};
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
/// This client uses a producer-consumer pattern where a single consumer processes complete
/// PostgreSQL transactions atomically:
///
/// ## Single Producer (PostgreSQL Reader)
/// - Reads from PostgreSQL logical replication stream
/// - Collects events from BEGIN to COMMIT into Transaction objects
/// - Sends complete transactions to bounded channels
/// - Handles LSN tracking and heartbeats
///
/// ## Single Consumer (Destination Writer)
/// - Dedicated destination database connection
/// - Processes complete transactions atomically
/// - Streaming transactions maintain database transaction open across batches
/// - Normal transactions use work-stealing pattern via shared channel
/// - Transaction consistency: all events in a transaction succeed or fail together
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    transaction_sender: Option<mpsc::Sender<Transaction>>,
    transaction_receiver: Option<mpsc::Receiver<Transaction>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
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
            consumer_handle: None,
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
            let batch_size = self.config.batch_size;

            tokio::spawn(Self::run_producer(
                replication_stream,
                transaction_sender,
                token,
                start_lsn,
                metrics,
                batch_size,
            ))
        };

        // Start consumer task with destination connection
        let transaction_receiver = self
            .transaction_receiver
            .take()
            .ok_or_else(|| CdcError::generic("Transaction receiver not available"))?;

        let dest_type = &self.config.destination_type;
        let dest_connection_string = &self.config.destination_connection_string;
        let schema_mappings = self.config.schema_mappings.clone();

        info!("Starting consumer for transaction processing");

        // Create destination handler for the consumer
        let mut consumer_destination = DestinationFactory::create(dest_type.clone())?;

        // Connect the consumer's destination handler
        consumer_destination.connect(dest_connection_string).await?;

        // Apply schema mappings to the handler
        if !schema_mappings.is_empty() {
            consumer_destination.set_schema_mappings(schema_mappings.clone());
        }

        info!("Consumer destination connection established");

        let token = self.cancellation_token.clone();
        let metrics = self.metrics_collector.clone();
        let dest_type_str = dest_type.to_string();

        let consumer_handle = tokio::spawn(Self::run_consumer_loop(
            transaction_receiver,
            consumer_destination,
            token,
            metrics,
            dest_type_str,
        ));
        self.consumer_handle = Some(consumer_handle);

        // Update metrics for active connections
        self.metrics_collector
            .update_active_connections(1, "consumer");

        self.producer_handle = Some(producer_handle);

        self.start_server_uptime();

        info!("CDC replication started successfully");
        self.cancellation_token.cancelled().await;
        Ok(())
    }

    // Start metrics update task
    fn start_server_uptime(&mut self) {
        let metrics = self.metrics_collector.clone();
        let token = self.cancellation_token.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

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
    /// ensuring that the consumer processes an entire transaction atomically.
    ///
    /// ## Batch Processing
    ///
    /// When the number of events in a transaction reaches batch_size, a batch is sent
    /// to the consumer immediately. This enables:
    /// - Incremental LSN updates for graceful shutdown recovery
    /// - Prevention of double consumption after restart
    /// - Better memory management for large transactions
    ///
    /// ## Transaction Types
    ///
    /// - Normal transactions (BEGIN...COMMIT): Single transaction processed atomically
    /// - Streaming transactions (StreamStart...StreamCommit): Large transactions sent in batches
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        transaction_sender: mpsc::Sender<Transaction>,
        cancellation_token: CancellationToken,
        start_lsn: Lsn,
        metrics_collector: Arc<MetricsCollector>,
        batch_size: usize,
    ) -> Result<()> {
        info!("Starting replication producer (batch_size={})", batch_size);

        // Initialize connection status
        metrics_collector.update_source_connection_status(true);

        // Unified transaction manager for both normal and streaming transactions
        let mut tx_manager = TransactionManager::new(batch_size);
        let mut current_context = TransactionContext::None;

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
                    metrics_collector.record_event(&event);
                    // Handle transaction boundaries
                    match &event.event_type {
                        EventType::Begin {
                            transaction_id,
                            commit_timestamp,
                        } => {
                            // Start a new normal transaction
                            if !matches!(current_context, TransactionContext::None) {
                                warn!("Received BEGIN while transaction is in progress, discarding incomplete transaction");
                            }
                            debug!(
                                "Starting transaction {} commit_timestamp: {}",
                                transaction_id, commit_timestamp
                            );
                            tx_manager.handle_begin(*transaction_id, *commit_timestamp, event.lsn);
                            current_context = TransactionContext::Normal(*transaction_id);
                        }

                        EventType::Commit { .. } => {
                            // Complete and send the normal transaction
                            if let TransactionContext::Normal(tx_id) = current_context {
                                if let Some(tx) = tx_manager.handle_commit(tx_id, event.lsn) {
                                    let event_count = tx.event_count();
                                    info!(
                                        "Producer: Sending normal transaction {} with {} events",
                                        tx_id, event_count
                                    );

                                    if let Err(e) = transaction_sender.send(tx).await {
                                        error!(
                                            "Producer: Failed to send transaction to consumer: {}",
                                            e
                                        );
                                        metrics_collector
                                            .record_error("transaction_send_failed", "producer");
                                        break;
                                    }
                                    info!(
                                        "Producer: Successfully sent transaction {} with {} events",
                                        tx_id, event_count
                                    );
                                }
                                current_context = TransactionContext::None;
                            } else {
                                warn!("Received COMMIT without BEGIN, ignoring");
                            }
                        }

                        EventType::StreamStart {
                            transaction_id,
                            first_segment,
                        } => {
                            debug!(
                                "StreamStart: transaction_id={}, first_segment={}",
                                transaction_id, first_segment
                            );
                            tx_manager.handle_stream_start(
                                *transaction_id,
                                *first_segment,
                                event.lsn,
                            );
                            current_context = TransactionContext::Streaming(*transaction_id);
                        }

                        EventType::StreamStop => {
                            if let TransactionContext::Streaming(xid) = current_context {
                                // Check if we should send a batch
                                if let Some(batch_tx) =
                                    tx_manager.handle_stream_stop(xid, event.lsn)
                                {
                                    let batch_count = batch_tx.event_count();

                                    info!(
                                        "Producer: StreamStop - sending batch with {} events for transaction {}",
                                        batch_count, xid
                                    );

                                    if let Err(e) = transaction_sender.send(batch_tx).await {
                                        error!("Producer: Failed to send StreamStop batch: {}", e);
                                        metrics_collector
                                            .record_error("batch_send_failed", "producer");
                                        break;
                                    }
                                    info!(
                                        "Producer: Successfully sent StreamStop batch for transaction {}",
                                        xid
                                    );
                                }
                            }
                        }

                        EventType::StreamCommit {
                            transaction_id,
                            commit_timestamp,
                        } => {
                            info!("Producer: StreamCommit for transaction {}", transaction_id);
                            if let Some(final_tx) = tx_manager.handle_stream_commit(
                                *transaction_id,
                                *commit_timestamp,
                                event.lsn,
                            ) {
                                let final_count = final_tx.event_count();

                                info!(
                                    "Producer: Sending final streaming transaction {} with {} events",
                                    transaction_id, final_count
                                );

                                if let Err(e) = transaction_sender.send(final_tx).await {
                                    error!(
                                        "Producer: Failed to send final streaming transaction: {}",
                                        e
                                    );
                                    metrics_collector
                                        .record_error("transaction_send_failed", "producer");
                                    break;
                                }
                                info!(
                                    "Producer: Successfully sent final transaction {} with {} events",
                                    transaction_id, final_count
                                );
                            }
                            if let TransactionContext::Streaming(xid) = current_context {
                                if xid == *transaction_id {
                                    current_context = TransactionContext::None;
                                }
                            }
                        }

                        EventType::StreamAbort { transaction_id } => {
                            debug!("StreamAbort: transaction_id={}", transaction_id);
                            tx_manager.handle_stream_abort(*transaction_id);
                            if let TransactionContext::Streaming(xid) = current_context {
                                if xid == *transaction_id {
                                    current_context = TransactionContext::None;
                                }
                            }
                        }

                        EventType::Insert { .. }
                        | EventType::Update { .. }
                        | EventType::Delete { .. }
                        | EventType::Truncate(_) => {
                            match &current_context {
                                TransactionContext::Streaming(xid) => {
                                    let xid = *xid;
                                    // Add to streaming transaction
                                    tx_manager.add_event(xid, event);

                                    // Check if we should send a batch (accumulated enough events)
                                    if tx_manager.should_send_batch(xid) {
                                        debug!(
                                            "Producer: Batch threshold reached for streaming transaction {}",
                                            xid
                                        );
                                        if let Some(batch_tx) = tx_manager.take_batch(xid) {
                                            let batch_count = batch_tx.event_count();

                                            info!(
                                                "Producer: Sending mid-stream batch with {} events for transaction {}",
                                                batch_count, xid
                                            );

                                            if let Err(e) = transaction_sender.send(batch_tx).await
                                            {
                                                error!(
                                                    "Producer: Failed to send mid-stream batch: {}",
                                                    e
                                                );
                                                metrics_collector
                                                    .record_error("batch_send_failed", "producer");
                                                break;
                                            }
                                            info!(
                                                "Producer: Successfully sent batch of {} events for transaction {}",
                                                batch_count, xid
                                            );
                                        }
                                    }
                                }
                                TransactionContext::Normal(tx_id) => {
                                    let tx_id = *tx_id;
                                    // Add event to normal transaction
                                    tx_manager.add_event(tx_id, event);

                                    // Check if we should send a batch for normal transaction too
                                    if tx_manager.should_send_batch(tx_id) {
                                        debug!(
                                            "Producer: Batch threshold reached for normal transaction {}",
                                            tx_id
                                        );
                                        if let Some(batch_tx) = tx_manager.take_batch(tx_id) {
                                            let batch_count = batch_tx.event_count();

                                            info!(
                                                "Producer: Sending batch with {} events for normal transaction {}",
                                                batch_count, tx_id
                                            );

                                            if let Err(e) = transaction_sender.send(batch_tx).await
                                            {
                                                error!("Producer: Failed to send batch: {}", e);
                                                metrics_collector
                                                    .record_error("batch_send_failed", "producer");
                                                break;
                                            }
                                            info!(
                                                "Producer: Successfully sent batch of {} events for transaction {}",
                                                batch_count, tx_id
                                            );
                                        }
                                    }
                                }
                                TransactionContext::None => {
                                    // Event outside of transaction - create implicit transaction
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
        match current_context {
            TransactionContext::Normal(tx_id) => {
                let event_count = tx_manager.event_count(tx_id);
                warn!(
                    "Discarding incomplete normal transaction {} with {} events due to shutdown",
                    tx_id, event_count
                );
            }
            TransactionContext::Streaming(xid) => {
                warn!(
                    "Discarding incomplete streaming transaction {} due to shutdown",
                    xid
                );
            }
            TransactionContext::None => {}
        }

        // Clear any incomplete transactions from the manager
        if tx_manager.has_active_transactions() {
            let count = tx_manager.clear();
            warn!(
                "Discarding {} incomplete transaction(s) due to shutdown",
                count
            );
        }

        // Update connection status on shutdown
        metrics_collector.update_source_connection_status(false);

        // Gracefully stop the replication stream
        replication_stream.stop().await?;

        info!("Replication producer stopped gracefully");
        Ok(())
    }

    /// Consumer loop for transaction processing
    ///
    /// The consumer receives complete transactions and processes them atomically.
    /// This ensures that all events within a transaction are applied together.
    ///
    /// # Arguments
    /// * `transaction_receiver` - Receiver for all transactions (both normal and streaming)
    /// * `destination_handler` - Destination handler (no mutex needed!)
    /// * `cancellation_token` - Token for graceful shutdown
    /// * `metrics_collector` - Metrics collector
    /// * `destination_type` - Type of destination for metrics labeling
    async fn run_consumer_loop(
        mut transaction_receiver: mpsc::Receiver<Transaction>,
        mut destination_handler: Box<dyn DestinationHandler>,
        cancellation_token: CancellationToken,
        metrics_collector: Arc<MetricsCollector>,
        destination_type: String,
    ) -> Result<()> {
        info!("Starting consumer loop for transaction processing");

        // Update destination connection status
        metrics_collector.update_destination_connection_status(&destination_type, true);

        let mut queue_size_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                biased;

                // Handle graceful shutdown
                _ = cancellation_token.cancelled() => {
                    info!("Consumer received cancellation signal");

                    // First, rollback any active streaming transaction
                    if let Some(active_tx_id) = destination_handler.get_active_streaming_transaction_id() {
                        warn!("Consumer: Rolling back active streaming transaction {} due to shutdown",
                              active_tx_id);
                        if let Err(e) = destination_handler.rollback_streaming_transaction().await {
                            error!("Consumer: Failed to rollback streaming transaction: {}", e);
                        }
                    }

                    // Then drain remaining transactions
                    Self::drain_remaining_transactions(
                        &mut transaction_receiver,
                        &mut destination_handler,
                        &metrics_collector,
                        &destination_type,
                    ).await;
                    break;
                }

                // Process transactions from single channel
                transaction = transaction_receiver.recv() => {
                    match transaction {
                        Some(tx) => {
                            let tx_id = tx.transaction_id;
                            let event_count = tx.event_count();
                            let is_final = tx.is_final_batch;

                            info!(
                                "Consumer received transaction {} with {} events (final={})",
                                tx_id, event_count, is_final
                            );

                            Self::process_transaction(
                                tx,
                                &mut destination_handler,
                                &metrics_collector,
                                &destination_type,
                            ).await;

                            info!("Consumer completed transaction {}", tx_id);
                        }
                        None => {
                            // Channel closed - producer has finished
                            info!("Consumer: Transaction channel closed");
                            break;
                        }
                    }
                }

                // Periodic queue size reporting
                _ = queue_size_interval.tick() => {
                    let queue_length = transaction_receiver.len();
                    debug!("Consumer queue length: {}", queue_length);
                    metrics_collector.update_consumer_queue_length(queue_length);
                }
            }
        }

        // Close the destination connection
        if let Err(e) = destination_handler.close().await {
            error!("Failed to close destination connection: {}", e);
        }

        metrics_collector.update_destination_connection_status(&destination_type, false);

        info!("Consumer stopped gracefully");
        Ok(())
    }

    /// Drain any remaining transactions from the channel during shutdown
    async fn drain_remaining_transactions(
        transaction_receiver: &mut mpsc::Receiver<Transaction>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
    ) {
        let mut count = 0;

        // Drain all remaining transactions
        loop {
            match transaction_receiver.try_recv() {
                Ok(transaction) => {
                    debug!(
                        "Consumer: Processing remaining transaction {} during shutdown",
                        transaction.transaction_id
                    );
                    Self::process_transaction(
                        transaction,
                        destination_handler,
                        metrics_collector,
                        destination_type,
                    )
                    .await;
                    count += 1;
                }
                Err(_) => break,
            }
        }

        info!(
            "Consumer processed {} remaining transactions during graceful shutdown",
            count
        );
    }

    /// Process a transaction batch
    ///
    /// Executes events batch by batch. Destination handler commits only when
    /// is_final_batch=true (triggered by COMMIT or StreamCommit events).
    async fn process_transaction(
        transaction: Transaction,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
    ) {
        let start_time = std::time::Instant::now();
        let event_count = transaction.event_count();
        let tx_id = transaction.transaction_id;
        let is_final = transaction.is_final_batch;

        let result = destination_handler.process_transaction(&transaction).await;

        match result {
            Ok(_) => {
                let duration = start_time.elapsed();

                // Only record metrics when transaction is complete (final batch)
                if is_final {
                    metrics_collector.record_transaction_processed(&transaction, destination_type);
                }

                debug!(
                    "Successfully processed transaction {} batch ({} events, final={}) in {:?}",
                    tx_id, event_count, is_final, duration
                );
            }
            Err(e) => {
                error!("Failed to process transaction {}: {}", tx_id, e);
                metrics_collector.record_error("transaction_processing_failed", "consumer");

                // Attempt to rollback on error
                if let Err(rollback_err) =
                    destination_handler.rollback_streaming_transaction().await
                {
                    error!("Failed to rollback transaction {}: {}", tx_id, rollback_err);
                }
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
        let consumer_task = Self::wait_handle(self.consumer_handle.take(), "Consumer");

        match tokio::join!(producer_task, consumer_task) {
            (Ok(_), Ok(_)) => {
                info!("All CDC tasks completed successfully");
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

        fn get_active_streaming_transaction_id(&self) -> Option<u32> {
            None
        }

        async fn rollback_streaming_transaction(&mut self) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
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
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        let token_clone = cancellation_token.clone();
        let metrics_collector = Arc::new(MetricsCollector::new());

        // Use new API - pass Receiver directly
        let boxed_handler: Box<dyn DestinationHandler> = mock_handler;

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                tx_receiver,
                boxed_handler,
                token_clone,
                metrics_collector,
                "test".to_string(),
            )
            .await
        });

        // Let the consumer run for a bit
        sleep(Duration::from_millis(50)).await;

        // Drop the sender to close the channel properly, This prevents the drain logic from waiting with timeout
        drop(tx_sender);

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

        // Use new API - pass Receiver directly
        let boxed_handler: Box<dyn DestinationHandler> = mock_handler;

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                tx_receiver,
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
        assert!(client.consumer_handle.is_none());

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
    async fn test_single_consumer_processes_all_transactions() {
        // Test that the single consumer processes all transactions
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(100);
        let cancellation_token = CancellationToken::new();

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let tracker = transactions_processed.clone();

        let mock_handler = Box::new(MockDestinationHandler {
            transactions_processed,
            should_fail: false,
            processing_delay: Duration::from_millis(5),
        });

        let token = cancellation_token.clone();
        let metrics = Arc::new(MetricsCollector::new());

        let consumer_handle = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                tx_receiver,
                mock_handler,
                token,
                metrics,
                "test".to_string(),
            )
            .await
        });

        // Send test transactions
        let num_transactions = 30;
        for i in 0..num_transactions {
            let tx = create_test_transaction_with_lsn(i as u64);
            tx_sender
                .send(tx)
                .await
                .expect("Failed to send transaction");
        }

        // Give consumer time to process all transactions
        sleep(Duration::from_millis(500)).await;

        // Cancel and wait
        cancellation_token.cancel();
        let result = timeout(Duration::from_secs(1), consumer_handle)
            .await
            .expect("Consumer should complete")
            .expect("Consumer should not panic");
        assert!(result.is_ok());

        // Verify all transactions were processed
        let total_processed = tracker.lock().unwrap().len();
        assert_eq!(
            total_processed, num_transactions,
            "All transactions should be processed by consumer"
        );
    }

    #[tokio::test]
    async fn test_process_transaction_direct_no_mutex() {
        // Test that process_transaction doesn't use mutex (direct mutable access)
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
        CdcClient::process_transaction(transaction, &mut handler, &metrics, "test").await;

        // Verify transaction was processed
        let processed = transactions_processed.lock().unwrap();
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].commit_lsn, expected_lsn);
    }

    #[tokio::test]
    async fn test_drain_remaining_transactions() {
        // Test that consumer drains remaining transactions on shutdown
        let (tx_sender, mut tx_receiver) = mpsc::channel::<Transaction>(10);

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
        CdcClient::drain_remaining_transactions(&mut tx_receiver, &mut handler, &metrics, "test")
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
    async fn test_consumer_error_handling() {
        // Test that consumer continues processing even if some transactions fail
        let (tx_sender, tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();

        let transactions_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            transactions_processed: transactions_processed.clone(),
            should_fail: true, // Simulate errors
            processing_delay: Duration::ZERO,
        });

        let token = cancellation_token.clone();
        let metrics = Arc::new(MetricsCollector::new());

        let consumer_handle = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                tx_receiver,
                mock_handler,
                token,
                metrics,
                "test".to_string(),
            )
            .await
        });

        // Send transactions (they will fail but consumer should continue)
        for _ in 0..5 {
            tx_sender
                .send(create_test_transaction())
                .await
                .expect("Send failed");
        }

        sleep(Duration::from_millis(50)).await;
        cancellation_token.cancel();

        let result = timeout(Duration::from_secs(1), consumer_handle)
            .await
            .expect("Consumer should complete")
            .expect("Consumer should not panic");

        assert!(result.is_ok(), "Consumer should handle errors gracefully");
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
