use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::transaction_manager::{
    PendingTransactionFile, TransactionFileMetadata, TransactionManager,
};
use crate::types::{EventType, Lsn, TransactionContext};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Main CDC client for coordinating replication and destination writes
///
/// # File-Based Transaction Processing Architecture
///
/// This client uses a file-based producer-consumer pattern for reliable transaction processing:
///
/// ## Producer (PostgreSQL Reader)
/// - Reads from PostgreSQL logical replication stream
/// - Collects events from BEGIN to COMMIT
/// - Writes transaction files to sql_received_tx/ directory
/// - Moves completed transactions to sql_pending_tx/ on COMMIT
/// - Handles LSN tracking and heartbeats
/// - Sends feedback to PostgreSQL
///
/// ## Consumer (Destination Writer)
/// - Receives notifications via mpsc channel when transactions are committed
/// - Reads and executes SQL commands from transaction files
/// - Processes complete transactions atomically
/// - Deletes transaction files after successful execution
/// - Transaction consistency: all events in a transaction succeed or fail together
/// - Updates flush_lsn after successful commits
///
/// ## LSN Tracking
///
/// The client uses simplified LSN tracking with a single flush_lsn value:
/// - flush_lsn: Updated by consumer when transaction is committed to destination
/// - This LSN serves as the start_lsn for recovery on restart
/// - Persisted to disk periodically for graceful shutdown support
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    metrics_collector: Arc<MetricsCollector>,
    /// LSN tracker for tracking last committed LSN to destination (for file persistence)
    lsn_tracker: Option<Arc<LsnTracker>>,
    /// Shared LSN feedback for replication protocol (write/flush/replay separation)
    shared_lsn_feedback: Arc<SharedLsnFeedback>,
    /// Transaction file manager for file-based workflow
    transaction_file_manager: Option<Arc<TransactionManager>>,
}

impl CdcClient {
    /// Create a new CDC client
    pub async fn new(config: Config) -> Result<Self> {
        info!("Creating CDC client");

        // Create destination handler
        let destination_handler = DestinationFactory::create(&config.destination_type)?;

        let replication_manager = ReplicationManager::new(config.clone());

        // Create shared LSN feedback for proper replication protocol handling
        let shared_lsn_feedback = SharedLsnFeedback::new_shared();

        // Create transaction file manager (always enabled for data safety)
        info!(
            "Transaction file persistence enabled at: {}",
            config.transaction_file_base_path
        );
        let mut manager = TransactionManager::new(
            &config.transaction_file_base_path,
            config.destination_type.clone(),
        )
        .await?;
        manager.set_schema_mappings(config.schema_mappings.clone());

        Ok(Self {
            config,
            replication_manager: Some(replication_manager),
            destination_handler: Some(destination_handler),
            cancellation_token: CancellationToken::new(),
            producer_handle: None,
            consumer_handle: None,
            metrics_collector: Arc::new(MetricsCollector::new()),
            lsn_tracker: None,
            shared_lsn_feedback,
            transaction_file_manager: Some(Arc::new(manager)),
        })
    }

    /// Set the LSN tracker for tracking committed LSN
    ///
    /// This should be called before starting replication to enable
    /// LSN persistence after each successful commit to the destination.
    pub fn set_lsn_tracker(&mut self, tracker: Arc<LsnTracker>) {
        self.lsn_tracker = Some(tracker);
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

            // Process pending transaction files from previous run (recovery)
            if let Some(ref file_mgr) = self.transaction_file_manager {
                info!("Transaction file persistence - checking for pending files");
                if let Err(e) = Self::process_pending_transaction_files(
                    file_mgr,
                    handler,
                    &self.cancellation_token,
                    &self.lsn_tracker,
                    &self.metrics_collector,
                    self.config.batch_size,
                )
                .await
                {
                    error!(
                        "Failed to process pending transaction files during recovery: {}",
                        e
                    );
                    return Err(e);
                }
            }
        }

        info!("CDC client initialized successfully");
        Ok(())
    }

    /// Start CDC replication from a specific LSN
    pub async fn start_replication_from_lsn(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication");

        info!("Performing CDC client initialization (including recovery)");
        // Ensure we're initialized
        self.init().await?;
        info!("CDC client initialized successfully");

        // Create replication stream using async method
        let replication_manager = self
            .replication_manager
            .take()
            .ok_or_else(|| CdcError::generic("Replication manager not available"))?;

        let mut replication_stream = replication_manager.create_stream_async().await?;

        // Start the replication stream
        replication_stream.start(start_lsn).await?;

        // Start file-based workflow
        self.start_file_based_workflow(replication_stream, start_lsn)
            .await?;

        self.start_server_uptime();

        info!("CDC replication started successfully");
        self.cancellation_token.cancelled().await;
        Ok(())
    }

    async fn start_file_based_workflow(
        &mut self,
        replication_stream: ReplicationStream,
        start_lsn: Option<Lsn>,
    ) -> Result<()> {
        let transaction_file_manager = self
            .transaction_file_manager
            .clone()
            .ok_or_else(|| CdcError::generic("Transaction file manager not available"))?;

        let (tx_commit_notifier, rx_commit_notifier) =
            mpsc::channel::<PendingTransactionFile>(1000);
        info!("Created transaction commit notification channel with buffer size 1000");

        // Start producer (writes to files only)
        let producer_handle = {
            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let start_lsn = start_lsn.unwrap_or_else(|| Lsn::new(0));
            let file_mgr = transaction_file_manager.clone();
            let lsn_feedback = self.shared_lsn_feedback.clone();
            let lsn_tracker = self.lsn_tracker.clone();

            tokio::spawn(Self::run_producer(
                replication_stream,
                token,
                start_lsn,
                metrics,
                file_mgr,
                lsn_feedback,
                lsn_tracker,
                tx_commit_notifier,
            ))
        };

        // Process any pending transactions from previous run before starting consumer
        info!("Checking for pending transaction files from previous run...");
        let pending_count = transaction_file_manager
            .list_pending_transactions()
            .await?
            .len();
        if pending_count > 0 {
            info!(
                "Found {} pending transaction files to process before starting consumer",
                pending_count
            );
        }

        // Start consumer (reads from files only)
        let dest_type = &self.config.destination_type;
        let dest_connection_string = &self.config.destination_connection_string;
        let schema_mappings = self.config.schema_mappings.clone();

        info!("Starting file-based consumer for transaction processing");

        // Create destination handler for the consumer
        let mut consumer_destination = DestinationFactory::create(&dest_type)?;

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
        let lsn_tracker = self.lsn_tracker.clone();
        let shared_lsn_feedback = self.shared_lsn_feedback.clone();

        let consumer_handle = tokio::spawn(Self::run_consumer_loop(
            transaction_file_manager,
            consumer_destination,
            token,
            metrics,
            dest_type_str,
            lsn_tracker,
            shared_lsn_feedback,
            self.config.batch_size,
            rx_commit_notifier,
        ));

        self.consumer_handle = Some(consumer_handle);
        self.producer_handle = Some(producer_handle);

        // Update metrics for active connections
        self.metrics_collector
            .update_active_connections(1, "consumer");

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

    /// Helper function to handle transaction commit logic (shared between Commit and StreamCommit)
    ///
    /// This function encapsulates the common logic for committing a transaction:
    /// 1. Move transaction file from sql_received_tx/ to sql_pending_tx/
    /// 2. Update flush_lsn for PostgreSQL feedback
    /// 3. Update lsn_tracker for persistence
    /// 4. Read metadata from pending file
    /// 5. Notify consumer via mpsc channel
    /// 6. Handle errors with metrics recording
    ///
    /// # Arguments
    /// * `transaction_id` - Transaction ID being committed
    /// * `timestamp` - Commit timestamp
    /// * `event_lsn` - LSN of the commit event
    /// * `transaction_type` - Type of transaction ("normal" or "streaming") for logging
    /// * `transaction_file_manager` - File manager for moving transaction files
    /// * `shared_lsn_feedback` - Shared LSN feedback for PostgreSQL protocol
    /// * `lsn_tracker` - Optional LSN tracker for persistence
    /// * `commit_notifier` - Channel sender for notifying consumer
    /// * `metrics_collector` - Metrics collector for error recording
    async fn handle_transaction_commit(
        transaction_id: u32,
        timestamp: chrono::DateTime<chrono::Utc>,
        event_lsn: Option<Lsn>,
        transaction_type: &str,
        transaction_file_manager: &Arc<TransactionManager>,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
        lsn_tracker: &Option<Arc<LsnTracker>>,
        commit_notifier: &mpsc::Sender<PendingTransactionFile>,
        metrics_collector: &Arc<MetricsCollector>,
    ) -> Result<()> {
        match transaction_file_manager
            .commit_transaction(transaction_id, timestamp, event_lsn)
            .await
        {
            Ok(pending_path) => {
                info!(
                    "Committed {} transaction file to: {:?}",
                    transaction_type, pending_path
                );

                // Update flush_lsn - transaction file is now durably persisted to disk
                if let Some(lsn) = event_lsn {
                    shared_lsn_feedback.update_flushed_lsn(lsn.0);
                    debug!(
                    "Updated flush LSN to {} for {} transaction {} (file persisted to sql_pending_tx/)",
                        lsn, transaction_type, transaction_id
                );

                    if let Some(ref tracker) = lsn_tracker {
                        tracker.update_if_greater(lsn.0);
                    }
                }

                // Notify consumer with transaction details for immediate processing
                match tokio::fs::read_to_string(&pending_path).await {
                    Ok(content) => {
                        match serde_json::from_str::<TransactionFileMetadata>(&content) {
                            Ok(metadata) => {
                                let notification = PendingTransactionFile {
                                    file_path: pending_path.clone(),
                                    metadata,
                                };
                                if let Err(e) = commit_notifier.send(notification).await {
                                    warn!(
                                    "Failed to send commit notification to consumer: {}. Consumer may have stopped.",
                                    e
                                );
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse metadata from {:?}: {}", pending_path, e);
                                metrics_collector.record_error("metadata_parse_failed", "producer");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read metadata from {:?}: {}", pending_path, e);
                        metrics_collector.record_error("metadata_read_failed", "producer");
                    }
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to commit {} transaction file for tx {}: {}",
                    transaction_type, transaction_id, e
                );
                metrics_collector.record_error("transaction_file_commit_failed", "producer");
                Err(e)
            }
        }
    }

    /// File-based producer task: reads events from PostgreSQL replication stream and writes to transaction files
    ///
    /// This producer collects events between BEGIN and COMMIT, writing them to transaction files.
    /// Files are moved from sql_received_tx/ to sql_pending_tx/ on COMMIT, making them available
    /// for the consumer to process. The producer notifies the consumer via mpsc channel on each commit,
    /// sending the exact file path and transaction metadata for immediate processing.
    ///
    /// ## Transaction Types
    ///
    /// - Normal transactions (BEGIN...COMMIT): Single transaction file
    /// - Streaming transactions (StreamStart...StreamCommit): Large transactions in one file
    ///
    /// ## LSN Tracking
    ///
    /// The producer updates shared_lsn_feedback with flush_lsn when transaction files are moved to sql_pending_tx/
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        cancellation_token: CancellationToken,
        start_lsn: Lsn,
        metrics_collector: Arc<MetricsCollector>,
        transaction_file_manager: Arc<TransactionManager>,
        shared_lsn_feedback: Arc<SharedLsnFeedback>,
        lsn_tracker: Option<Arc<LsnTracker>>,
        commit_notifier: mpsc::Sender<PendingTransactionFile>,
    ) -> Result<()> {
        info!(
            "Starting replication producer, last time start_lsn: {}",
            start_lsn
        );

        // Initialize connection status
        metrics_collector.update_source_connection_status(true);

        // Track current transaction context
        let mut current_context = TransactionContext::None;

        // Track active transaction files: tx_id -> (file_path, timestamp)
        let mut active_tx_files: std::collections::HashMap<
            u32,
            (std::path::PathBuf, chrono::DateTime<chrono::Utc>),
        > = std::collections::HashMap::new();

        while !cancellation_token.is_cancelled() {
            match replication_stream.next_event(&cancellation_token).await {
                Ok(event) => {
                    if event.lsn <= start_lsn {
                        debug!("Skipping event with LSN {} <= {}", event.lsn, start_lsn);
                        continue;
                    }

                    // Record current LSN for metrics
                    metrics_collector.record_received_lsn(event.lsn.0);

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
                            current_context = TransactionContext::Normal(*transaction_id);

                            // Create transaction file
                            match transaction_file_manager
                                .begin_transaction(*transaction_id, *commit_timestamp)
                                .await
                            {
                                Ok(file_path) => {
                                    debug!("Created transaction file: {:?}", file_path);
                                    active_tx_files
                                        .insert(*transaction_id, (file_path, *commit_timestamp));
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to create transaction file for tx {}: {}",
                                        transaction_id, e
                                    );
                                    metrics_collector
                                        .record_error("transaction_file_create_failed", "producer");
                                }
                            }
                        }

                        EventType::Commit { .. } => {
                            // Complete and commit the normal transaction file
                            if let TransactionContext::Normal(tx_id) = current_context {
                                info!("Producer: Committing normal transaction {}", tx_id);

                                // Move transaction file to pending and notify consumer
                                if let Some((_, timestamp)) = active_tx_files.remove(&tx_id) {
                                    // Use helper function to handle commit logic
                                    if let Err(e) = Self::handle_transaction_commit(
                                        tx_id,
                                        timestamp,
                                        Some(event.lsn),
                                        "normal",
                                        &transaction_file_manager,
                                        &shared_lsn_feedback,
                                        &lsn_tracker,
                                        &commit_notifier,
                                        &metrics_collector,
                                    )
                                    .await
                                    {
                                        error!(
                                            "Failed to handle normal transaction commit for tx {}: {}",
                                            tx_id, e
                                        );
                                    }
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
                            current_context = TransactionContext::Streaming(*transaction_id);

                            // Create transaction file on first segment
                            if *first_segment {
                                let timestamp = chrono::Utc::now();
                                match transaction_file_manager
                                    .begin_transaction(*transaction_id, timestamp)
                                    .await
                                {
                                    Ok(file_path) => {
                                        debug!(
                                            "Created streaming transaction file: {:?}",
                                            file_path
                                        );
                                        active_tx_files
                                            .insert(*transaction_id, (file_path, timestamp));
                                    }
                                    Err(e) => {
                                        error!("Failed to create streaming transaction file for tx {}: {}", transaction_id, e);
                                        metrics_collector.record_error(
                                            "transaction_file_create_failed",
                                            "producer",
                                        );
                                    }
                                }
                            }
                        }

                        EventType::StreamStop => {
                            // StreamStop marks the end of a segment in streaming transactions
                            if let TransactionContext::Streaming(xid) = current_context {
                                debug!("Producer: StreamStop for transaction {}", xid);
                            }
                        }

                        EventType::StreamCommit {
                            transaction_id,
                            commit_timestamp: _,
                        } => {
                            info!("Producer: StreamCommit for transaction {}", transaction_id);

                            // Move streaming transaction file to pending and notify consumer
                            if let Some((_, timestamp)) = active_tx_files.remove(transaction_id) {
                                // Use helper function to handle commit logic
                                if let Err(e) = Self::handle_transaction_commit(
                                    *transaction_id,
                                    timestamp,
                                    Some(event.lsn),
                                    "streaming",
                                    &transaction_file_manager,
                                    &shared_lsn_feedback,
                                    &lsn_tracker,
                                    &commit_notifier,
                                    &metrics_collector,
                                )
                                .await
                                {
                                    error!(
                                        "Failed to handle streaming transaction commit for tx {}: {}",
                                        transaction_id, e
                                    );
                                }
                            }

                            if let TransactionContext::Streaming(xid) = current_context {
                                if xid == *transaction_id {
                                    current_context = TransactionContext::None;
                                }
                            }
                        }

                        EventType::StreamAbort { transaction_id } => {
                            debug!("StreamAbort: transaction_id={}", transaction_id);

                            // Delete transaction file
                            if let Some((_, timestamp)) = active_tx_files.remove(transaction_id) {
                                match transaction_file_manager
                                    .abort_transaction(*transaction_id, timestamp)
                                    .await
                                {
                                    Ok(_) => {
                                        debug!(
                                            "Aborted transaction file for tx {}",
                                            transaction_id
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to abort transaction file for tx {}: {}",
                                            transaction_id, e
                                        );
                                        metrics_collector.record_error(
                                            "transaction_file_abort_failed",
                                            "producer",
                                        );
                                    }
                                }
                            }

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
                            // Append event to transaction file
                            let tx_id = match &current_context {
                                TransactionContext::Streaming(xid) => Some(*xid),
                                TransactionContext::Normal(tx_id) => Some(*tx_id),
                                TransactionContext::None => {
                                    warn!("Received DML event outside of transaction context");
                                    None
                                }
                            };

                            if let Some(tx_id) = tx_id {
                                if let Some((file_path, _)) = active_tx_files.get(&tx_id) {
                                    if let Err(e) = transaction_file_manager
                                        .append_event(file_path, &event)
                                        .await
                                    {
                                        error!("Failed to append event to transaction file for tx {}: {}", tx_id, e);
                                        metrics_collector.record_error(
                                            "transaction_file_append_failed",
                                            "producer",
                                        );
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
                Err(e) => {
                    error!("Error reading from replication stream: {}", e);
                    metrics_collector.record_error("replication_stream_error", "producer");
                    break;
                }
            }
        }

        info!("File-based producer shutting down");

        if let Err(e) = transaction_file_manager.flush_all_buffers().await {
            error!("Failed to flush buffers during shutdown: {}", e);
            metrics_collector.record_error("buffer_flush_failed", "producer");
        } else {
            info!("Successfully flushed all buffers");
        }

        // If there's an incomplete transaction, log a warning
        match current_context {
            TransactionContext::Normal(tx_id) => {
                warn!(
                    "Discarding incomplete normal transaction {} due to shutdown",
                    tx_id
                );
                // File will remain in sql_received_tx/ and can be cleaned up on restart
            }
            TransactionContext::Streaming(xid) => {
                warn!(
                    "Discarding incomplete streaming transaction {} due to shutdown",
                    xid
                );
            }
            TransactionContext::None => {}
        }

        // Update connection status on shutdown
        metrics_collector.update_source_connection_status(false);

        // Gracefully stop the replication stream
        replication_stream.stop().await?;

        info!("File-based replication producer stopped gracefully");
        Ok(())
    }

    /// Process pending transaction files on startup (recovery)
    ///
    /// This function processes all committed transaction files from sql_pending_tx/
    /// in commit timestamp order before starting normal replication processing.
    ///
    /// This method now respects cancellation token for graceful shutdown during recovery.
    ///
    async fn process_pending_transaction_files(
        file_mgr: &TransactionManager,
        destination: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        lsn_tracker: &Option<Arc<LsnTracker>>,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
    ) -> Result<()> {
        info!("Checking for pending transaction files from previous run...");

        let pending_txs = file_mgr.list_pending_transactions().await?;

        if pending_txs.is_empty() {
            info!("No pending transaction files found");
            return Ok(());
        }

        let total_count = pending_txs.len();
        info!(
            "Found {} pending transaction file(s) to process",
            total_count
        );

        for (idx, pending_tx) in pending_txs.iter().enumerate() {
            // Check for cancellation before processing each file
            if cancellation_token.is_cancelled() {
                info!(
                    "Cancellation detected during recovery, processed {} of {} files",
                    idx, total_count
                );
                return Err(CdcError::cancelled("Recovery cancelled by shutdown signal"));
            }

            info!(
                "Processing pending transaction file {} of {}: {} (tx_id: {}, lsn: {:?})",
                idx + 1,
                total_count,
                pending_tx.file_path.display(),
                pending_tx.metadata.transaction_id,
                pending_tx.metadata.commit_lsn
            );

            // Call the core processing logic directly
            if let Err(e) = Self::process_transaction_file(
                pending_tx,
                file_mgr,
                destination,
                cancellation_token,
                lsn_tracker,
                metrics_collector,
                batch_size,
            )
            .await
            {
                error!(
                    "Failed to process pending transaction file {}: {}",
                    pending_tx.file_path.display(),
                    e
                );
                metrics_collector.record_error("transaction_file_execution_failed", "consumer");
                return Err(e);
            }
        }

        info!(
            "Successfully processed all {} pending transaction file(s)",
            total_count
        );
        Ok(())
    }

    /// Consumer loop for file-based transaction processing
    ///
    /// The consumer waits for notifications from the producer via mpsc channel.
    /// Each notification contains the exact transaction file path and metadata,
    /// allowing the consumer to process transactions in the precise order they were committed
    /// without needing to scan the directory.
    ///
    /// LSN tracking: After each successful transaction file execution,
    /// the flush_lsn is updated and persisted to ensure graceful shutdown doesn't lose data.
    ///
    /// # Arguments
    /// * `transaction_file_manager` - File manager for reading pending transaction files
    /// * `destination_handler` - Destination handler for executing SQL
    /// * `cancellation_token` - Token for graceful shutdown
    /// * `metrics_collector` - Metrics collector
    /// * `destination_type` - Type of destination for metrics labeling
    /// * `lsn_tracker` - Optional LSN tracker for persisting committed LSN
    /// * `shared_lsn_feedback` - Shared feedback for updating applied LSN for replication protocol
    /// * `batch_size` - Batch size for SQL log truncation control
    /// * `mut commit_receiver` - Channel receiver for transaction commit notifications with file details
    async fn run_consumer_loop(
        transaction_file_manager: Arc<TransactionManager>,
        mut destination_handler: Box<dyn DestinationHandler>,
        cancellation_token: CancellationToken,
        metrics_collector: Arc<MetricsCollector>,
        destination_type: String,
        lsn_tracker: Option<Arc<LsnTracker>>,
        shared_lsn_feedback: Arc<SharedLsnFeedback>,
        batch_size: usize,
        mut commit_receiver: mpsc::Receiver<PendingTransactionFile>,
    ) -> Result<()> {
        info!("Starting file-based consumer loop for transaction processing");

        // Update destination connection status
        metrics_collector.update_destination_connection_status(&destination_type, true);

        loop {
            tokio::select! {
                biased;

                // Handle graceful shutdown
                _ = cancellation_token.cancelled() => {
                    info!("Consumer received cancellation signal");

                    // Process any remaining pending transaction files before shutdown
                    Self::drain_remaining_files(
                        &transaction_file_manager,
                        &mut destination_handler,
                        &metrics_collector,
                        &lsn_tracker,
                        batch_size,
                        &cancellation_token,
                    ).await;

                    // Log final LSN state
                    shared_lsn_feedback.log_state("Consumer shutdown - final LSN state");
                    break;
                }

                // Wait for transaction commit notification from producer
                result = commit_receiver.recv() => {
                    match result {
                        Some(notification) => {
                            // Received notification with exact transaction details
                            debug!(
                                "Consumer received commit notification for transaction {} with file {:?}",
                                notification.metadata.transaction_id, notification.file_path
                            );

                            // Check for cancellation before processing
                            if cancellation_token.is_cancelled() {
                                debug!("Consumer: Cancellation detected, not processing transaction {}", notification.metadata.transaction_id);
                                break;
                            }

                            // Process the transaction directly from notification (already a PendingTransactionFile)
                            if let Err(e) = Self::process_transaction_file(
                                &notification,
                                &transaction_file_manager,
                                &mut destination_handler,
                                &cancellation_token,
                                &lsn_tracker,
                                &metrics_collector,
                                batch_size,
                            ).await {
                                error!(
                                    "Failed to process transaction {} from file {:?}: {}",
                                    notification.metadata.transaction_id, notification.file_path, e
                                );
                                metrics_collector.record_error("transaction_file_processing_failed", "consumer");
                                // Continue to next transaction rather than failing completely
                            }
                        }
                        None => {
                            // Channel closed - producer has stopped
                            info!("Consumer: Commit notification channel closed, producer has stopped");

                            // Process any remaining files before exiting
                            Self::drain_remaining_files(
                                &transaction_file_manager,
                                &mut destination_handler,
                                &metrics_collector,
                                &lsn_tracker,
                                batch_size,
                                &cancellation_token,
                            ).await;

                            break;
                        }
                    }
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

    /// Drain any remaining transaction files from sql_pending_tx/ during shutdown
    async fn drain_remaining_files(
        transaction_file_manager: &TransactionManager,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        lsn_tracker: &Option<Arc<LsnTracker>>,
        batch_size: usize,
        cancellation_token: &CancellationToken,
    ) {
        // Get all pending files
        match transaction_file_manager.list_pending_transactions().await {
            Ok(pending_files) => {
                info!(
                    "Consumer: Draining {} remaining transaction files during shutdown",
                    pending_files.len()
                );
                for pending_tx in pending_files {
                    // Check for cancellation before processing each file during drain
                    if cancellation_token.is_cancelled() {
                        break;
                    }

                    debug!(
                        "Consumer: Processing remaining file {:?} during shutdown (tx_id: {})",
                        pending_tx.file_path, pending_tx.metadata.transaction_id
                    );

                    match Self::process_transaction_file(
                        &pending_tx,
                        transaction_file_manager,
                        destination_handler,
                        cancellation_token,
                        lsn_tracker,
                        metrics_collector,
                        batch_size,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            error!(
                                "Failed to process remaining file {:?}: {}",
                                pending_tx.file_path, e
                            );
                        }
                    };
                }
            }
            Err(e) => {
                error!("Failed to list remaining files during shutdown: {}", e);
            }
        }
    }

    /// Process a single transaction file
    /// Reads SQL commands from the file, executes them via the destination handler in batches,
    /// updates LSN tracking, and deletes the file upon success.
    /// This method supports resumable processing: it tracks the position after each batch
    /// and can resume from where it left off if interrupted.
    ///
    /// Checks cancellation token between batches to support graceful shutdown.
    async fn process_transaction_file(
        pending_tx: &PendingTransactionFile,
        file_manager: &TransactionManager,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        lsn_tracker: &Option<Arc<LsnTracker>>,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
    ) -> Result<()> {
        let start_time = Instant::now();
        let tx_id = pending_tx.metadata.transaction_id;
        let file_path_str = pending_tx.file_path.to_string_lossy().to_string();

        // Check if we're resuming from a saved position for THIS specific file
        let start_index = if let Some(ref tracker) = lsn_tracker {
            if let Some((saved_file_path, saved_start_index)) = tracker.get_resume_position() {
                // Only use the saved index if it's for the current file
                if saved_file_path == file_path_str {
                    saved_start_index
                } else {
                    info!(
                        "Saved position is for different file ('{}' vs '{}'). Starting from index 0",
                        saved_file_path, file_path_str
                    );
                    0
                }
            } else {
                info!("No saved consumer position found, starting from index 0");
                0
            }
        } else {
            info!("No LSN tracker available, starting from index 0");
            0
        };

        info!(
            "Processing transaction file: {} (tx_id: {}, lsn: {:?}, start_index: {})",
            pending_tx.file_path.display(),
            tx_id,
            pending_tx.metadata.commit_lsn,
            start_index
        );

        // Read SQL commands from file starting from the saved position
        let commands = file_manager
            .read_sql_commands_from_index(&pending_tx.file_path, start_index)
            .await?;
        let command_count = commands.len();
        let total_commands = start_index + command_count;

        if command_count == 0 {
            info!(
                "All commands already executed for transaction file: {} (tx_id: {})",
                pending_tx.file_path.display(),
                tx_id
            );

            // Clear position and delete file
            if let Some(ref tracker) = lsn_tracker {
                tracker.clear_consumer_position();
            }

            // Update LSN and delete file
            Self::finalize_transaction_file(
                pending_tx,
                file_manager,
                lsn_tracker,
                metrics_collector,
                total_commands,
            )
            .await?;

            return Ok(());
        }

        info!(
            "Executing {} remaining SQL command(s) from file in batches of {} (total: {}, skipped: {})",
            command_count, batch_size, total_commands, start_index
        );

        // Execute commands in batches within transactions for better performance
        let mut batch_count = 0;
        let mut current_command_index = start_index;

        for (batch_idx, chunk) in commands.chunks(batch_size).enumerate() {
            // Check for cancellation before processing each batch
            if cancellation_token.is_cancelled() {
                info!(
                    "Cancellation detected during transaction file processing at batch {}/{}",
                    batch_idx,
                    (command_count + batch_size - 1) / batch_size
                );

                warn!(
                    "Transaction file processing cancelled by shutdown signal (tx_id: {})",
                    tx_id
                );

                return Ok(());
            }

            // CRITICAL: Transactional checkpoint update for atomicity
            // The pre-commit hook executes WITHIN the database transaction, BEFORE COMMIT.
            // This provides true atomicity:
            //
            // Timeline:
            //   BEGIN;
            //     INSERT INTO users ...;       ← Execute batch commands
            //     UPDATE products ...;
            //
            //     [pre_commit_hook()]          ← Update checkpoint (in-memory + persist)
            //   COMMIT;                        ← Both data and checkpoint committed atomically
            //
            // If crash/rollback occurs:
            //   - Before COMMIT: Both data and checkpoint rolled back → Safe, resume from old position
            //   - After COMMIT: Both data and checkpoint persisted → Safe, resume from new position
            //   - No race condition possible!
            let next_command_index = current_command_index + chunk.len();
            let pre_commit_hook = if let Some(ref tracker) = lsn_tracker {
                let tracker_clone = tracker.clone();
                let file_path_clone = file_path_str.clone();
                let last_executed_index = next_command_index - 1;

                Some(Box::new(move || {
                    Box::pin(async move {
                        tracker_clone
                            .update_consumer_position(file_path_clone, last_executed_index);
                        Ok(())
                    })
                        as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
                })
                    as Box<
                        dyn FnOnce() -> std::pin::Pin<
                                Box<dyn std::future::Future<Output = Result<()>> + Send>,
                            > + Send,
                    >)
            } else {
                None
            };

            let batch_start_time = Instant::now();
            debug!(
                "Executing batch {}/{} with {} commands (tx_id: {}, global indices: {}-{})",
                batch_idx + 1,
                (command_count + batch_size - 1) / batch_size,
                chunk.len(),
                tx_id,
                current_command_index,
                next_command_index - 1
            );

            // Execute the batch with transactional checkpoint hook
            if let Err(e) = destination_handler
                .execute_sql_batch_with_hook(chunk, pre_commit_hook)
                .await
            {
                error!(
                    "Failed to execute SQL batch {} from file {}: {}",
                    batch_idx + 1,
                    pending_tx.file_path.display(),
                    e
                );
                metrics_collector.record_error("transaction_file_execution_failed", "consumer");

                info!(
                    "Batch and checkpoint rolled back together, will retry from last committed position on restart"
                );

                return Err(e);
            }

            // Update current position after successful batch
            current_command_index = next_command_index;

            let batch_duration = batch_start_time.elapsed();
            debug!(
                "Successfully executed batch {}/{} with {} commands in {:?}",
                batch_idx + 1,
                (command_count + batch_size - 1) / batch_size,
                chunk.len(),
                batch_duration
            );
            batch_count += 1;
        }

        let duration = start_time.elapsed();
        info!(
            "Successfully executed all {} remaining commands ({} total) in {} batches in {:?} (tx_id: {}, avg: {:?}/batch)",
            command_count,
            total_commands,
            batch_count,
            duration,
            tx_id,
            duration / batch_count.max(1) as u32
        );

        // Clear consumer position since file is fully processed and persist immediately
        if let Some(ref tracker) = lsn_tracker {
            if let Err(e) = tracker
                .clear_consumer_position_and_persist_immediately()
                .await
            {
                warn!("Failed to clear and persist consumer position: {}", e);
            }
        }

        // Finalize: update LSN, record metrics, delete file
        Self::finalize_transaction_file(
            pending_tx,
            file_manager,
            lsn_tracker,
            metrics_collector,
            total_commands,
        )
        .await?;

        Ok(())
    }

    /// Core logic for finalizing transaction file processing
    async fn finalize_transaction_file(
        pending_tx: &PendingTransactionFile,
        file_manager: &TransactionManager,
        lsn_tracker: &Option<Arc<LsnTracker>>,
        metrics_collector: &Arc<MetricsCollector>,
        total_commands: usize,
    ) -> Result<()> {
        let tx_id = pending_tx.metadata.transaction_id;

        // Update LSN tracking (replay_lsn only, pending_file_count will be updated after deletion)
        if let Some(commit_lsn) = pending_tx.metadata.commit_lsn {
            // Note: We don't have shared_lsn_feedback here, which is only used by the consumer loop
            // For recovery processing, we only update the lsn_tracker
            debug!("Transaction {} commit LSN: {}", tx_id, commit_lsn);

            if let Some(ref tracker) = lsn_tracker {
                // Update flush LSN (last committed to destination)
                tracker.commit_lsn(commit_lsn.0);
            }
        }

        // Record metrics - create a transaction object for metrics recording
        let destination_type_str = pending_tx.metadata.destination_type.to_string();

        // Create a transaction object for metrics (events are already executed, so we use empty vec)
        // The event_count is derived from the number of SQL commands executed
        let mut transaction = crate::types::Transaction::new(
            pending_tx.metadata.transaction_id,
            pending_tx.metadata.commit_timestamp,
        );
        transaction.commit_lsn = pending_tx.metadata.commit_lsn;

        // Record transaction processed metrics
        metrics_collector.record_transaction_processed(&transaction, &destination_type_str);

        // Since file-based processing always processes complete transactions,
        // we also record this as a full transaction
        metrics_collector.record_full_transaction_processed(&transaction, &destination_type_str);

        debug!(
            "Successfully processed transaction file with {} commands and recorded metrics",
            total_commands
        );

        // Delete the file after successful processing
        if let Err(e) = file_manager
            .delete_pending_transaction(&pending_tx.file_path)
            .await
        {
            error!(
                "Failed to delete processed transaction file {}: {}",
                pending_tx.file_path.display(),
                e
            );
        }

        if pending_tx.metadata.commit_lsn.is_some() {
            if let Some(ref tracker) = lsn_tracker {
                let pending_count = file_manager.list_pending_transactions().await?.len();
                tracker.update_consumer_state(
                    tx_id,
                    pending_tx.metadata.commit_timestamp,
                    pending_count,
                );

                debug!(
                "Updated LSN tracker consumer state: tx_id={}, pending_count={} (after deletion)",
                tx_id, pending_count
            );
            }
        }

        Ok(())
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

        // Shutdown LSN tracker to persist final state
        if let Some(ref tracker) = self.lsn_tracker {
            info!("Shutting down LSN tracker and persisting final state");

            // Shutdown LSN tracker, which includes a final state persistence.
            tracker.shutdown_async().await;

            // Log final state after shutdown
            let post_shutdown_metadata = tracker.get_metadata();
            info!(
                "Post-shutdown state - flush_lsn={}, pending_files={}, current_file={:?}, last_executed_index={:?}",
                pg_walstream::format_lsn(post_shutdown_metadata.lsn_tracking.flush_lsn),
                post_shutdown_metadata.consumer_state.pending_file_count,
                post_shutdown_metadata.consumer_state.current_file_path,
                post_shutdown_metadata.consumer_state.last_executed_command_index
            );
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
    use crate::types::Transaction;
    use std::time::Duration;
    use tokio::sync::mpsc;
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
        pub should_fail: bool,
    }

    #[async_trait::async_trait]
    impl DestinationHandler for MockDestinationHandler {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: std::collections::HashMap<String, String>) {
            // Mock implementation - no-op
        }

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
        ) -> Result<()> {
            if self.should_fail {
                return Err(CdcError::generic("Mock execute_sql_batch error"));
            }
            // Mock implementation - just count the commands
            for _ in commands {
                // No-op
            }

            // Execute pre-commit hook if provided
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }

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
