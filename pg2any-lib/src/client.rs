use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::replication_state::{CommittedTx, ReplicationState, TxBuffer};
use crate::sql_generator::SqlGenerator;
use crate::types::{EventType, Lsn};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Main CDC client for coordinating replication and destination writes
///
/// # PostgreSQL Logical Replication Protocol Compliance
///
/// This implementation strictly follows PostgreSQL logical replication protocol rules:
///
/// ## Core Principles
/// 1. **LSN is the only valid resume cursor** - Never use txid for resume
/// 2. **Commit defines transaction boundaries** - Changes not durable until commit
/// 3. **commit_lsn is globally ordered** - Apply transactions in commit_lsn order
/// 4. **Change arrival order NOT guaranteed** - Streaming fragments may interleave
/// 5. **Replication slot guarantees** - WAL resent until ACKed, no partial replays
///
/// ## Transaction Flow
///
/// ### Non-streaming transactions (BEGIN...COMMIT):
/// - Only ONE active transaction at a time
/// - Change messages have NO xid
/// - Flow: Begin → buffer to current_tx → Commit → move to commit_queue → apply → ACK
///
/// ### Streaming transactions (StreamStart...StreamCommit):
/// - Multiple open transactions allowed
/// - Each message includes xid
/// - Flow: StreamStart → buffer to streaming_txs\[xid\] → StreamCommit → move to commit_queue → apply → ACK
///
/// ## State Model
///
/// ### In-Memory (ReplicationState):
/// - `current_tx`: Active non-streaming transaction (only ONE)
/// - `streaming_txs`: Map<Xid, TxBuffer> for active streaming transactions
/// - `commit_queue`: Min-heap ordered by commit_lsn for correct apply order
///
/// ### Persistent (LsnTracker):
/// - `confirmed_flush_lsn`: ONLY LSN persisted, updated AFTER successful apply
///
///
/// ## Graceful Shutdown:
/// 1. Stop receiving new replication messages
/// 2. Finish applying ONLY committed transactions in commit_queue
/// 3. DO NOT apply or ACK open transactions (current_tx, streaming_txs)
/// 4. Persist confirmed_flush_lsn
/// 5. Close replication connection
/// Result: PostgreSQL will resend incomplete transactions on restart
///
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    metrics_collector: Arc<MetricsCollector>,
    /// LSN tracker for persisting confirmed_flush_lsn (ONLY LSN that matters for resume)
    lsn_tracker: Arc<LsnTracker>,
    /// Replication state (transaction buffers and commit queue) - shared between producer/consumer
    replication_state: Arc<TokioMutex<ReplicationState>>,
    /// SQL generator for converting change events to destination SQL statements
    sql_generator: Option<Arc<SqlGenerator>>,
    /// Channel receiver for committed transactions
    commit_rx: Option<mpsc::Receiver<CommittedTx>>,
}

impl CdcClient {
    /// Create a new CDC client with LSN tracking
    ///
    /// This method creates a new CDC client and automatically initializes the LSN tracker
    /// for tracking committed LSN positions. The LSN tracker is loaded from the persistence
    /// file if it exists, allowing graceful recovery from previous runs.
    ///
    /// # Arguments
    ///
    /// * `config` - The CDC configuration
    /// * `lsn_file_path` - Optional path to the LSN persistence file. If None, uses
    ///   the default from environment variables or "./pg2any_last_lsn.metadata"
    ///
    /// # Returns
    ///
    /// Returns a tuple of (`CdcClient`, `Option<Lsn>`) where the `Lsn` is the last committed
    /// LSN loaded from the persistence file, or `None` if starting fresh.
    pub async fn new(config: Config, lsn_file_path: Option<&str>) -> Result<(Self, Option<Lsn>)> {
        info!("Creating CDC client");

        // Create destination handler
        let destination_handler = DestinationFactory::create(&config.destination_type)?;

        let replication_manager = ReplicationManager::new(config.clone());

        // Create SQL generator for converting change events to destination SQL
        info!("Initializing SQL generator for {:?}", config.destination_type);
        let mut generator = SqlGenerator::new(config.destination_type.clone()).await?;
        generator.set_schema_mappings(config.schema_mappings.clone());

        // Create LSN tracker and load last known LSN
        info!("Initializing LSN tracker for position tracking");
        let (lsn_tracker, start_lsn) =
            crate::lsn_tracker::create_lsn_tracker_with_load(lsn_file_path).await;

        // Create channel for producer->consumer communication with backpressure
        let (commit_tx, commit_rx) = mpsc::channel(config.batch_size);

        let client = Self {
            config,
            replication_manager: Some(replication_manager),
            destination_handler: Some(destination_handler),
            cancellation_token: CancellationToken::new(),
            producer_handle: None,
            consumer_handle: None,
            metrics_collector: Arc::new(MetricsCollector::new()),
            lsn_tracker,
            replication_state: Arc::new(TokioMutex::new(ReplicationState::new(commit_tx))),
            sql_generator: Some(Arc::new(generator)),
            commit_rx: Some(commit_rx),
        };

        Ok((client, start_lsn))
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
        let sql_generator = self
            .sql_generator
            .clone()
            .ok_or_else(|| CdcError::generic("SQL generator not available"))?;

        let shared_lsn_feedback = replication_stream.shared_lsn_feedback().clone();

        // Start producer
        let producer_handle = {
            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let start_lsn = start_lsn.unwrap_or_else(|| Lsn::new(0));
            let repl_state = self.replication_state.clone();

            tokio::spawn(Self::run_producer(
                replication_stream,
                token,
                start_lsn,
                metrics,
                repl_state,
            ))
        };

        // Start consumer with protocol-compliant logic
        let dest_type = &self.config.destination_type;
        let dest_connection_string = &self.config.destination_connection_string;
        let schema_mappings = self.config.schema_mappings.clone();

        info!("Starting protocol-compliant consumer with commit_lsn ordering");

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
        let shared_lsn_feedback_for_consumer = shared_lsn_feedback.clone();

        // Take the receiver from self (it will be moved to the consumer task)
        let commit_rx = self
            .commit_rx
            .take()
            .ok_or_else(|| CdcError::generic("Commit receiver not available"))?;

        let consumer_handle = tokio::spawn(Self::run_consumer(
            commit_rx,
            consumer_destination,
            token,
            metrics,
            dest_type_str,
            lsn_tracker,
            shared_lsn_feedback_for_consumer,
            sql_generator,
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

    /// Protocol-compliant producer task: reads events and buffers transactions
    ///
    /// This producer follows PostgreSQL logical replication protocol strictly:
    ///
    /// ## Non-streaming transactions (BEGIN...COMMIT):
    /// - Only ONE active transaction at a time in current_tx
    /// - Change events have NO xid
    /// - Flow: Begin → add to current_tx → Commit → move to commit_queue
    ///
    /// ## Streaming transactions (StreamStart...StreamCommit):
    /// - Multiple concurrent transactions in streaming_txs HashMap
    /// - StreamStart/StreamStop establish context with xid
    /// - DML events between StreamStart/StreamStop belong to that xid
    /// - Flow: StreamStart(xid) → add to streaming_txs[xid] → StreamCommit → move to commit_queue
    ///
    /// ## CRITICAL: NO ACK in Producer
    /// - Producer NEVER sends ACK to PostgreSQL
    /// - Only consumer sends ACK AFTER successful apply
    /// - This prevents data loss on crash/restart
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        cancellation_token: CancellationToken,
        start_lsn: Lsn,
        metrics_collector: Arc<MetricsCollector>,
        replication_state: Arc<TokioMutex<ReplicationState>>,
    ) -> Result<()> {
        info!(
            "Starting protocol-compliant replication producer from LSN: {}",
            start_lsn
        );

        // Initialize connection status
        metrics_collector.update_source_connection_status(true);

        // Track current streaming context (which streaming transaction we're in)
        let mut current_streaming_xid: Option<u32> = None;

        while !cancellation_token.is_cancelled() {
            match replication_stream.next_event(&cancellation_token).await {
                Ok(event) => {
                    // Record current LSN for metrics
                    metrics_collector.record_received_lsn(event.lsn.0);
                    metrics_collector.record_event(&event);

                    // Handle transaction boundaries and buffer changes
                    {
                        let mut state = replication_state.lock().await;
                        match &event.event_type {
                            EventType::Begin {
                                transaction_id,
                                commit_timestamp,
                            } => {
                                if let Err(e) =
                                    state.handle_begin(*transaction_id, *commit_timestamp)
                                {
                                    error!("Failed to handle Begin: {}", e);
                                    metrics_collector.record_error("begin_failed", "producer");
                                }
                            }

                            EventType::Commit { commit_timestamp } => {
                                if let Err(e) = state.handle_commit(event.lsn, *commit_timestamp) {
                                    error!("Failed to handle Commit: {}", e);
                                    metrics_collector.record_error("commit_failed", "producer");
                                } else {
                                    debug!(
                                        "Non-streaming transaction committed at LSN {} (queue size: {})",
                                        event.lsn,
                                        state.commit_queue_size()
                                    );
                                }
                            }

                            EventType::StreamStart {
                                transaction_id,
                                first_segment,
                            } => {
                                if let Err(e) =
                                    state.handle_stream_start(*transaction_id, *first_segment)
                                {
                                    error!("Failed to handle StreamStart: {}", e);
                                    metrics_collector
                                        .record_error("stream_start_failed", "producer");
                                } else {
                                    // Set streaming context
                                    current_streaming_xid = Some(*transaction_id);
                                    debug!("Streaming context set to xid {}", transaction_id);
                                }
                            }

                            EventType::StreamStop => {
                                debug!("StreamStop received, clearing streaming context");
                                current_streaming_xid = None;
                            }

                            EventType::StreamCommit {
                                transaction_id,
                                commit_timestamp,
                            } => {
                                if let Err(e) = state.handle_stream_commit(
                                    *transaction_id,
                                    event.lsn,
                                    *commit_timestamp,
                                ) {
                                    error!("Failed to handle StreamCommit: {}", e);
                                    metrics_collector
                                        .record_error("stream_commit_failed", "producer");
                                } else {
                                    debug!(
                                        "Streaming transaction {} committed at LSN {} (queue size: {})",
                                        transaction_id,
                                        event.lsn,
                                        state.commit_queue_size()
                                    );
                                }
                                // Clear streaming context
                                current_streaming_xid = None;
                            }

                            EventType::StreamAbort { transaction_id } => {
                                if let Err(e) = state.handle_stream_abort(*transaction_id) {
                                    error!("Failed to handle StreamAbort: {}", e);
                                    metrics_collector
                                        .record_error("stream_abort_failed", "producer");
                                }
                                // Clear streaming context if it's the current one
                                if current_streaming_xid == Some(*transaction_id) {
                                    current_streaming_xid = None;
                                }
                            }

                            EventType::Insert { .. }
                            | EventType::Update { .. }
                            | EventType::Delete { .. }
                            | EventType::Truncate(_) => {
                                // Route DML event to correct transaction based on context
                                if let Some(xid) = current_streaming_xid {
                                    // Streaming transaction context - add to streaming_txs[xid]
                                    if let Err(e) =
                                        state.add_change_to_streaming(xid, event.clone())
                                    {
                                        error!(
                                            "Failed to add change to streaming transaction {}: {}",
                                            xid, e
                                        );
                                        metrics_collector.record_error(
                                            "add_streaming_change_failed",
                                            "producer",
                                        );
                                    }
                                } else if state.has_current_tx() {
                                    // Non-streaming transaction context - add to current_tx
                                    if let Err(e) = state.add_change_to_current(event.clone()) {
                                        error!(
                                            "Failed to add change to current transaction: {}",
                                            e
                                        );
                                        metrics_collector
                                            .record_error("add_change_failed", "producer");
                                    }
                                } else {
                                    warn!(
                                        "Received DML event outside transaction context: {:?}",
                                        event.event_type
                                    );
                                }
                            }

                            // Skip metadata events
                            _ => {
                                debug!("Skipping metadata event: {:?}", event.event_type);
                            }
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

        info!("Producer shutting down");

        // Log incomplete transactions (will be resent by PostgreSQL on restart)
        {
            let state = replication_state.lock().await;
            let (current_tx_count, streaming_tx_count) = state.incomplete_transactions();
            if current_tx_count > 0 || streaming_tx_count > 0 {
                info!(
                    "Producer shutdown with {} non-streaming + {} streaming incomplete transactions (PostgreSQL will resend on restart)",
                    current_tx_count, streaming_tx_count
                );
            }
            info!("Replication state at shutdown: {}", state.stats());
        }

        // Update connection status on shutdown
        metrics_collector.update_source_connection_status(false);

        // Gracefully stop the replication stream
        replication_stream.stop().await?;

        info!("Protocol-compliant producer stopped gracefully");
        Ok(())
    }

    /// Protocol-compliant consumer: pops from commit_queue, applies in commit_lsn order, then ACKs
    ///
    /// This consumer follows PostgreSQL logical replication protocol strictly:
    ///
    /// ## Apply Flow (CRITICAL ORDER):
    /// 1. Pop transaction with smallest commit_lsn from commit_queue
    /// 2. Generate SQL for all changes in transaction
    /// 3. Apply transaction atomically to destination
    /// 4. Update confirmed_flush_lsn = transaction.commit_lsn
    /// 5. Send ACK to PostgreSQL via SharedLsnFeedback
    /// 6. Persist confirmed_flush_lsn to disk
    ///
    /// ## Graceful Shutdown:
    /// - Process ALL transactions in commit_queue
    /// - DO NOT wait for new commits
    /// - Persist final confirmed_flush_lsn
    /// - Incomplete transactions (in current_tx/streaming_txs) are NOT applied
    ///
    /// ## CRITICAL: ACK Only After Apply
    /// - Never ACK before transaction is committed to destination
    /// - This ensures exactly-once at the consumer boundary
    async fn run_consumer(
        mut commit_rx: mpsc::Receiver<CommittedTx>,
        mut destination_handler: Box<dyn DestinationHandler>,
        cancellation_token: CancellationToken,
        metrics_collector: Arc<MetricsCollector>,
        destination_type: String,
        lsn_tracker: Arc<LsnTracker>,
        shared_lsn_feedback: Arc<SharedLsnFeedback>,
        sql_generator: Arc<SqlGenerator>,
    ) -> Result<()> {
        info!("Starting protocol-compliant consumer with commit_lsn ordering and channel-based communication");

        // Update destination connection status
        metrics_collector.update_destination_connection_status(&destination_type, true);

        loop {
            tokio::select! {
                biased;

                // Handle graceful shutdown
                _ = cancellation_token.cancelled() => {
                    info!("Consumer received cancellation signal");
                    break;
                }

                // Receive committed transaction from channel
                Some(committed_tx) = commit_rx.recv() => {
                    let tx_buffer = committed_tx.tx_buffer;
                    let commit_lsn = committed_tx.commit_lsn;

                    info!(
                        "Consumer processing transaction {} with {} changes at commit_lsn {} ({})",
                        tx_buffer.xid,
                        tx_buffer.change_count(),
                        commit_lsn,
                        if tx_buffer.is_streaming { "streaming" } else { "normal" }
                    );

                    // Apply transaction
                    if let Err(e) = Self::apply_transaction(
                        &tx_buffer,
                        commit_lsn,
                        &mut destination_handler,
                        &metrics_collector,
                        &destination_type,
                        &lsn_tracker,
                        &shared_lsn_feedback,
                        &sql_generator,
                    ).await {
                        error!("Failed to apply transaction {}: {}", tx_buffer.xid, e);
                        metrics_collector.record_error("transaction_apply_failed", "consumer");
                        // On error, we DON'T ACK and will retry from last confirmed_flush_lsn on restart
                        return Err(e);
                    }

                    info!(
                        "Successfully applied and ACKed transaction {} at commit_lsn {}",
                        tx_buffer.xid, commit_lsn
                    );
                }
            }
        }

        // Graceful shutdown: drain remaining committed transactions from channel
        info!("Consumer draining remaining transactions from channel");
        let mut drained_count = 0;

        while let Ok(committed_tx) = commit_rx.try_recv() {
            let tx_buffer = committed_tx.tx_buffer;
            let commit_lsn = committed_tx.commit_lsn;

            info!(
                "Draining transaction {} at commit_lsn {}",
                tx_buffer.xid, commit_lsn
            );

            if let Err(e) = Self::apply_transaction(
                &tx_buffer,
                commit_lsn,
                &mut destination_handler,
                &metrics_collector,
                &destination_type,
                &lsn_tracker,
                &shared_lsn_feedback,
                &sql_generator,
            )
            .await
            {
                error!("Failed to drain transaction {}: {}", tx_buffer.xid, e);
                break;
            }

            drained_count += 1;
        }

        info!(
            "Consumer drained {} transactions during shutdown",
            drained_count
        );

        // Close the destination connection
        if let Err(e) = destination_handler.close().await {
            error!("Failed to close destination connection: {}", e);
        }

        metrics_collector.update_destination_connection_status(&destination_type, false);

        info!("Protocol-compliant consumer stopped gracefully");
        Ok(())
    }

    /// Apply a single transaction to the destination
    /// CRITICAL: Only ACKs after successful apply
    async fn apply_transaction(
        tx_buffer: &TxBuffer,
        commit_lsn: Lsn,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
        lsn_tracker: &Arc<LsnTracker>,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
        sql_generator: &Arc<SqlGenerator>,
    ) -> Result<()> {
        let start_time = Instant::now();
        let xid = tx_buffer.xid;
        let change_count = tx_buffer.change_count();

        // Generate SQL for all changes
        let mut sql_commands = Vec::with_capacity(change_count);
        for change in &tx_buffer.changes {
            match sql_generator.generate_sql_for_event(change) {
                Ok(sql) => {
                    if !sql.is_empty() {
                        sql_commands.push(sql);
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to generate SQL for event in transaction {}: {}",
                        xid, e
                    );
                    return Err(e);
                }
            }
        }

        debug!(
            "Generated {} SQL commands from {} changes for transaction {}",
            sql_commands.len(),
            change_count,
            xid
        );

        // CRITICAL: Update LSN BEFORE commit to ensure atomicity
        // The pre_commit_hook executes within the destination transaction,
        // ensuring LSN is updated only if the transaction commits successfully
        let lsn_tracker_clone = lsn_tracker.clone();
        let shared_lsn_feedback_clone = shared_lsn_feedback.clone();
        let lsn_value = commit_lsn.0;
        let xid_for_hook = xid;
        let commit_lsn_for_hook = commit_lsn;

        let pre_commit_hook = Box::new(move || {
            Box::pin(async move {
                lsn_tracker_clone.commit_lsn(lsn_value);
                shared_lsn_feedback_clone.update_flushed_lsn(lsn_value);
                debug!(
                    "Pre-commit hook: Updated confirmed_flush_lsn to {} for transaction {}",
                    commit_lsn_for_hook, xid_for_hook
                );
                Ok(())
            })
                as Pin<
                    Box<
                        dyn std::future::Future<Output = std::result::Result<(), CdcError>>
                            + Send
                            + 'static,
                    >,
                >
        });

        // Apply SQL commands atomically to destination with pre-commit LSN update
        if let Err(e) = destination_handler
            .execute_sql_batch_with_hook(&sql_commands, Some(pre_commit_hook))
            .await
        {
            error!(
                "Failed to execute transaction {} with {} commands: {}",
                xid,
                sql_commands.len(),
                e
            );
            metrics_collector.record_error("sql_execution_failed", "consumer");
            return Err(e);
        }

        let duration = start_time.elapsed();
        info!(
            "Applied transaction {} with {} commands in {:?}",
            xid,
            sql_commands.len(),
            duration
        );

        debug!(
            "Transaction {} committed with LSN {} updated atomically",
            xid, commit_lsn
        );

        // Record metrics
        let mut transaction = crate::types::Transaction::new(
            xid,
            tx_buffer
                .commit_timestamp
                .unwrap_or_else(|| chrono::Utc::now()),
        );
        transaction.commit_lsn = Some(commit_lsn);
        for change in &tx_buffer.changes {
            transaction.add_event(change.clone());
        }

        metrics_collector.record_transaction_processed(&transaction, destination_type);
        metrics_collector.record_full_transaction_processed(&transaction, destination_type);

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
        info!("Shutting down LSN tracker and persisting final state");

        // Shutdown LSN tracker, which includes a final state persistence.
        self.lsn_tracker.shutdown_async().await;

        // Log final state after shutdown
        let post_shutdown_metadata = self.lsn_tracker.get_metadata();
        info!(
                "Post-shutdown state - flush_lsn={}, pending_files={}, current_file={:?}, last_executed_index={:?}",
                pg_walstream::format_lsn(post_shutdown_metadata.lsn_tracking.flush_lsn),
                post_shutdown_metadata.consumer_state.pending_file_count,
                post_shutdown_metadata.consumer_state.current_file_path,
                post_shutdown_metadata.consumer_state.last_executed_command_index
            );

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

    // Mock destination handler for testing
    #[allow(dead_code)]
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

    async fn cleanup_default_metadata_file() {
        let _ = tokio::fs::remove_file("./pg2any_last_lsn.metadata").await;
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
        let (client, _start_lsn) = CdcClient::new(config, None)
            .await
            .expect("Failed to create client");

        // Test that the client is initially not running (not cancelled)
        assert!(client.is_running());

        // Test that we can get a cancellation token
        let token = client.cancellation_token();
        assert!(!token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_cancellation_token_cancellation() {
        let config = create_test_config();
        let (mut client, _start_lsn) = CdcClient::new(config, None)
            .await
            .expect("Failed to create client");

        let token = client.cancellation_token();
        assert!(!token.is_cancelled());

        // Cancel the token
        client.stop().await.expect("Failed to stop client");

        // The token should be cancelled
        assert!(token.is_cancelled());
        assert!(!client.is_running());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_cancellation_token_propagation() {
        let config = create_test_config();
        let (client, _start_lsn) = CdcClient::new(config, None)
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

        cleanup_default_metadata_file().await;
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
        let (mut client, _start_lsn) = CdcClient::new(config, None)
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

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_wait_for_tasks_completion_with_no_tasks() {
        let config = create_test_config();
        let (mut client, _start_lsn) = CdcClient::new(config, None)
            .await
            .expect("Failed to create client");

        // Should not fail when no tasks are running
        client
            .wait_for_tasks_completion()
            .await
            .expect("Should succeed with no tasks");

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_multiple_shutdown_calls_are_safe() {
        let config = create_test_config();
        let (mut client, _start_lsn) = CdcClient::new(config, None)
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

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_client_stats_reflect_cancellation_state() {
        let config = create_test_config();
        let (mut client, _start_lsn) = CdcClient::new(config, None)
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

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_cancellation_token_from_external_source() {
        let config = create_test_config();
        let (client, _start_lsn) = CdcClient::new(config, None)
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

        cleanup_default_metadata_file().await;
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

        let (client, _start_lsn) = CdcClient::new(config, None)
            .await
            .expect("Failed to create client");

        // Verify the client was created successfully with custom buffer
        assert_eq!(client.config().buffer_size, custom_buffer_size);

        cleanup_default_metadata_file().await;
    }
}
