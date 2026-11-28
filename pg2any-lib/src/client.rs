use crate::config::Config;
use crate::consumer::{self, GROUP_COMMIT_MAX_TXNS};
use crate::destinations::DestinationHandler;
use crate::error::{CdcError, Result};
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::producer;
use crate::slot::SlotStatus;
use crate::snapshot::decision::{decide, DecisionInput, SnapshotAction};
use crate::snapshot::marker::{MarkerStore, SnapshotMarker};
use crate::snapshot::{run_snapshot_phase, SnapshotContext};
use crate::transaction_manager::{
    PendingTransactionFile, TransactionManager, MEMORY_FAST_PATH_BUDGET_BYTES,
};
use crate::types::Lsn;
use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Waits for both tasks and returns the first failure, the producer's before the consumer's. A
/// task that panicked is an error too: `stop()` must go on to ACK, close and persist the position.
async fn join_tasks(
    producer: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer: Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    async fn join(name: &str, handle: Option<tokio::task::JoinHandle<Result<()>>>) -> Result<()> {
        match handle {
            None => Ok(()),
            Some(handle) => handle
                .await
                .map_err(|e| CdcError::generic(format!("{name} task panicked: {e}")))?,
        }
    }

    match tokio::join!(join("producer", producer), join("consumer", consumer)) {
        (Ok(()), Ok(())) => {
            info!("All CDC tasks completed successfully");
            Ok(())
        }
        (Err(producer_err), Ok(())) => {
            error!("Producer task failed: {}", producer_err);
            Err(producer_err)
        }
        (Ok(()), Err(consumer_err)) => {
            error!("Consumer task failed: {}", consumer_err);
            Err(consumer_err)
        }
        (Err(producer_err), Err(consumer_err)) => {
            error!("Producer task failed: {}", producer_err);
            error!("Consumer task failed: {}", consumer_err);
            Err(producer_err)
        }
    }
}

/// Batches after the startup recovery that may skip a duplicate key: a transaction the previous
/// run committed at the destination but did not record is sent again by PostgreSQL from the slot's
/// position (it has no file to replay). A slot created now (first start, deleted slot) starts
/// behind everything and re-sends nothing, and after a snapshot nothing is in doubt either: a
/// duplicate key there is a real conflict.
fn live_replay_grace(slot_known_absent: bool, after_snapshot: bool) -> usize {
    if slot_known_absent || after_snapshot {
        0
    } else {
        GROUP_COMMIT_MAX_TXNS + 1
    }
}

/// Decides whether this start runs, skips, recovers or refuses the initial snapshot.
fn plan_snapshot(
    config: &Config,
    marker: Option<&SnapshotMarker>,
    slot: std::result::Result<Option<&SlotStatus>, ()>,
    has_disk_lsn: bool,
) -> Result<Option<SnapshotAction>> {
    let destination =
        crate::config::sanitize_connection_string(&config.destination_connection_string);
    decide(&DecisionInput {
        enabled: config.initial_snapshot,
        policy: config.snapshot_target_policy,
        slot_name: &config.replication_slot_name,
        destination: &destination,
        marker,
        slot,
        has_disk_lsn,
    })
    .into_result()
}

/// Main CDC client for coordinating replication and destination writes.
///
/// # File-Based Transaction Processing Architecture
///
/// Uses a producer-consumer pattern with file-based persistence:
///
/// - **Producer** (`producer.rs`): Reads PostgreSQL WAL, writes transaction files.
/// - **Consumer** (`consumer.rs`): Reads transaction files, applies to destination,
///   updates LSN.
///
/// ## Graceful Shutdown Flow
///
/// 1. `stop()` cancels the `CancellationToken`
/// 2. Producer exits loop, flushes buffers, drops mpsc sender, sends oneshot
/// 3. Consumer drains remaining queue, processes all transactions
/// 4. Consumer persists final LSN to disk
/// 5. `stop()` sends final ACK to PostgreSQL and closes destination
///
/// On restart, `process_pending_transaction_files` replays any committed-but-unexecuted
/// files from `sql_pending_tx/`. Transactions whose `commit_lsn <= flush_lsn` are
/// skipped (position-tracking deduplication).
pub struct CdcClient {
    config: Config,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    metrics_collector: Arc<MetricsCollector>,
    lsn_tracker: Arc<LsnTracker>,
    transaction_file_manager: Arc<TransactionManager>,
    /// `None` only after an initial snapshot consumed the stream and failed; the snapshot
    /// library discards its slot and connection then, and `stop()` has nothing to ACK.
    replication_stream: Option<Arc<Mutex<LogicalReplicationStream>>>,
    snapshot_action: Option<SnapshotAction>,
    /// The startup query found no slot (a failed query leaves this `false`).
    slot_known_absent: bool,
}

impl CdcClient {
    /// Create a new CDC client with LSN tracking.
    ///
    /// Returns `(CdcClient, Option<Lsn>)` where the `Lsn` is the last committed
    /// position loaded from disk, or `None` if starting fresh.
    pub async fn new(config: Config, lsn_file_path: Option<&str>) -> Result<(Self, Option<Lsn>)> {
        info!("Creating CDC client");

        let destination_handler = config.create_destination()?;

        let dialect_override = Some(destination_handler.dialect());

        info!(
            "Transaction file persistence enabled at: {}",
            config.transaction_file_base_path
        );
        let mut manager = TransactionManager::new(
            &config.transaction_file_base_path,
            config.destination_type.clone(),
            dialect_override,
            config.transaction_segment_size_bytes,
        )
        .await?;
        manager.set_schema_mappings(config.schema_mappings.clone());
        manager.set_bulk_insert_config(config.bulk_insert_threshold);
        manager.set_memory_fast_path(MEMORY_FAST_PATH_BUDGET_BYTES, config.batch_size);

        if destination_handler.supports_event_mode() {
            info!(
                "Destination supports event mode, enabling event-mode for transaction processing"
            );
            manager.set_event_mode(true);
        }

        info!("Initializing LSN tracker for position tracking");
        let (lsn_tracker, disk_lsn) =
            crate::lsn_tracker::create_lsn_tracker_with_load(lsn_file_path).await;

        // Slot-first recovery: query pg_replication_slots for the server-side resume position, falling back to the on-disk LSN if the slot is gone or the query fails.
        let slot_result = crate::slot::query_replication_slot(
            &config.source_connection_string,
            &config.replication_slot_name,
            config.connection_timeout,
            config.initial_snapshot,
        )
        .await;
        let query_err = slot_result.as_ref().err().map(|e| e.to_string());
        let slot_known_absent = matches!(slot_result, Ok(None));

        let decision = crate::slot::reconcile_resume(
            disk_lsn.map(|l| l.0),
            slot_result.as_ref().map(Option::as_ref).map_err(|_| ()),
        );

        match decision.source {
            crate::slot::ResumeSource::Slot => info!(
                "Resuming from replication slot '{}': confirmed_flush={}, dedup_boundary={} (disk={})",
                config.replication_slot_name,
                pg_walstream::format_lsn(decision.start_lsn.unwrap_or(0)),
                pg_walstream::format_lsn(decision.dedup_boundary),
                disk_lsn
                    .map(|l| pg_walstream::format_lsn(l.0))
                    .unwrap_or_else(|| "none".to_string()),
            ),
            crate::slot::ResumeSource::SlotDeletedFallback => warn!(
                "Replication slot '{}' not found; resuming from on-disk LSN {}. If the slot was \
                 deleted, WAL changes after this LSN up to now may be unrecoverable (a new slot \
                 will be created at the current WAL position).",
                config.replication_slot_name,
                pg_walstream::format_lsn(decision.dedup_boundary),
            ),
            crate::slot::ResumeSource::QueryFailedFallback => warn!(
                "Replication slot '{}' query failed ({}); resuming from on-disk LSN {}. The slot \
                 likely still exists, so no WAL gap is expected.",
                config.replication_slot_name,
                query_err.as_deref().unwrap_or("unknown error"),
                pg_walstream::format_lsn(decision.dedup_boundary),
            ),
            crate::slot::ResumeSource::Fresh => info!(
                "No replication slot and no on-disk LSN; starting fresh from the current WAL position"
            ),
        }

        if matches!(slot_result.as_ref(), Ok(Some(s)) if s.active) {
            warn!(
                "Replication slot '{}' is currently active (held by another connection); \
                 starting replication may fail if a second consumer holds it",
                config.replication_slot_name
            );
        }

        // A leftover marker is checked even with the flag off, so a half-done snapshot is never
        // silently ignored; a corrupt marker aborts startup.
        let marker = MarkerStore::new(&config.transaction_file_base_path)
            .load()
            .await?;
        if matches!(marker, Some(SnapshotMarker::Completed { .. })) && slot_result.is_err() {
            warn!(
                "Initial snapshot is marked completed but replication slot '{}' could not be \
                 queried; resuming from the on-disk LSN without checking that the slot exists",
                config.replication_slot_name
            );
        }
        let snapshot_action = plan_snapshot(
            &config,
            marker.as_ref(),
            slot_result.as_ref().map(Option::as_ref).map_err(|_| ()),
            disk_lsn.is_some(),
        )?;
        if let Some(action) = &snapshot_action {
            info!("Initial snapshot planned: {:?}", action);
        }

        // Seed the de-dup boundary (monotonic raise). shared_lsn_feedback is seeded from lsn_tracker.get() later in start_file_based_workflow.
        lsn_tracker.commit_lsn(decision.dedup_boundary);

        let start_lsn = decision.start_lsn.map(Lsn::new);

        info!("Creating replication stream");
        let stream_config = ReplicationStreamConfig::from(&config);
        let replication_stream =
            LogicalReplicationStream::new(&config.source_connection_string, stream_config).await?;

        let client = Self {
            config,
            destination_handler: Some(destination_handler),
            cancellation_token: CancellationToken::new(),
            producer_handle: None,
            consumer_handle: None,
            metrics_collector: Arc::new(MetricsCollector::new()),
            lsn_tracker,
            transaction_file_manager: Arc::new(manager),
            replication_stream: Some(Arc::new(Mutex::new(replication_stream))),
            snapshot_action,
            slot_known_absent,
        };

        Ok((client, start_lsn))
    }

    fn stream(&self) -> Result<&Arc<Mutex<LogicalReplicationStream>>> {
        self.replication_stream.as_ref().ok_or_else(|| {
            CdcError::generic("replication stream is unavailable after an unfinished snapshot")
        })
    }

    /// Hands the stream to the snapshot phase and takes the returned one back.
    ///
    /// `snapshot()` takes the stream by value. The producer is not spawned yet, so the client
    /// holds the only `Arc` and `try_unwrap` succeeds. On any failure the stream stays consumed
    /// (`None`): the library has dropped it and discarded its slot, and `stop()` skips the ACK.
    async fn run_initial_snapshot(&mut self, action: SnapshotAction) -> Result<Lsn> {
        let shared = self.replication_stream.take().ok_or_else(|| {
            CdcError::generic("replication stream is unavailable for the snapshot")
        })?;
        let stream = match Arc::try_unwrap(shared) {
            Ok(mutex) => mutex.into_inner(),
            Err(shared) => {
                self.replication_stream = Some(shared);
                return Err(CdcError::generic(
                    "replication stream is already in use; cannot run the initial snapshot",
                ));
            }
        };
        let ctx = SnapshotContext {
            config: self.config.clone(),
            lsn_tracker: self.lsn_tracker.clone(),
            marker_store: MarkerStore::new(&self.config.transaction_file_base_path),
            metrics: self.metrics_collector.clone(),
            cancel: self.cancellation_token.clone(),
        };
        let (stream, consistent_point) = run_snapshot_phase(&ctx, action, stream).await?;
        self.replication_stream = Some(Arc::new(Mutex::new(stream)));
        Ok(consistent_point)
    }

    /// Initialize the CDC client (connect to destination).
    pub async fn init(&mut self) -> Result<()> {
        info!("Initializing CDC client");

        if let Some(ref mut handler) = self.destination_handler {
            self.config.connect_destination(handler.as_mut()).await?;
            if !self.config.schema_mappings.is_empty() {
                info!("Schema mappings applied: {:?}", self.config.schema_mappings);
            }
        }

        Ok(())
    }

    /// Start CDC replication from a specific LSN.
    pub async fn start_replication_from_lsn(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication");

        // The snapshot phase opens its own destination handlers, so it runs before `init()`:
        // the main handler would otherwise sit idle for the whole (possibly hours-long) copy.
        let consistent_point = match self.snapshot_action.take() {
            None => None,
            Some(action) => match self.run_initial_snapshot(action).await {
                Ok(consistent_point) => Some(consistent_point),
                Err(e) if self.cancellation_token.is_cancelled() => {
                    warn!(
                        "Initial snapshot interrupted by shutdown ({}); it will restart from the \
                         beginning on the next start",
                        e
                    );
                    return Ok(());
                }
                Err(e) => return Err(e),
            },
        };

        info!("Performing CDC client initialization (including recovery)");
        self.init().await?;
        info!("CDC client initialized successfully");

        // Right after a snapshot the slot resumes at its consistent point, so `None`.
        // The tracker was seeded with that point, and the de-dup boundary only drops
        // `commit_lsn <= flush_lsn` (see `is_already_applied`). The commit LSN the producer
        // stores is the Commit event's `lsn`, the commit record's END position, so every
        // transaction streamed after the snapshot is above the consistent point and is
        // delivered; a transaction at or below it is already in the snapshot.
        let start_xlog = match consistent_point {
            Some(_) => None,
            None => start_lsn.map(|lsn| lsn.0),
        };
        let after_snapshot = consistent_point.is_some();
        let start_lsn = consistent_point.or(start_lsn);

        self.start_file_based_workflow(start_xlog, start_lsn, after_snapshot)
            .await?;
        self.start_server_uptime();

        info!("CDC replication started successfully");
        self.cancellation_token.cancelled().await;
        Ok(())
    }

    async fn start_file_based_workflow(
        &mut self,
        start_xlog: Option<u64>,
        start_lsn: Option<Lsn>,
        after_snapshot: bool,
    ) -> Result<()> {
        let transaction_file_manager = self.transaction_file_manager.clone();

        let (tx_commit_notifier, rx_commit_notifier) =
            mpsc::channel::<PendingTransactionFile>(self.config.buffer_size);
        info!(
            "Created transaction commit notification channel with buffer size {}",
            self.config.buffer_size
        );

        let (producer_shutdown_tx, producer_shutdown_rx) = oneshot::channel::<()>();
        info!("Created producer shutdown notification channel");

        let replication_stream = self.stream()?.clone();
        let shared_lsn_feedback = {
            let stream_guard = replication_stream.lock().await;
            stream_guard.shared_lsn_feedback.clone()
        };

        // Seed shared_lsn_feedback with persisted LSN so standby status updates
        // report the correct position after restart
        let persisted_lsn = self.lsn_tracker.get();
        if persisted_lsn > 0 {
            shared_lsn_feedback.update_flushed_lsn(persisted_lsn);
            shared_lsn_feedback.update_applied_lsn(persisted_lsn);
            info!(
                "Initialized shared_lsn_feedback with persisted LSN: {}",
                pg_walstream::format_lsn(persisted_lsn)
            );
        }

        // One handler serves the recovery and then the live consumer, which closes it.
        let identity_keys = self.publication_identity_keys().await;
        let mut handler = self.destination_handler.take().ok_or_else(|| {
            CdcError::generic("the destination handler was already handed to the consumer")
        })?;
        Self::apply_live_settings(&mut handler, &identity_keys);

        // The slot must exist before the recovery, the stream must not be started before it ends.
        // A slot created only afterwards (it was deleted) starts behind everything committed in
        // the meantime, which would be lost; a started stream that nothing polls is terminated by
        // the server after `wal_sender_timeout`. A no-op when the slot exists.
        let ensured = replication_stream
            .lock()
            .await
            .ensure_replication_slot()
            .await;
        if let Err(e) = ensured {
            self.destination_handler = Some(handler);
            return Err(CdcError::from(e));
        }

        // Recovery: process committed-but-unexecuted transaction files from previous run
        info!("Processing pending transaction files from previous run (recovery)...");
        // The batch a crash left committed but unrecorded hits duplicate keys when it is
        // replayed. Only the batches that can have been in flight skip them; a later file or
        // batch with a duplicate key is a genuine conflict and still fails the recovery.
        // A crash can leave one whole group committed but unrecorded; replayed from its files each
        // transaction of it is a single batch.
        transaction_file_manager.set_replay_grace(GROUP_COMMIT_MAX_TXNS);
        let recovered = Self::process_pending_transaction_files(
            &transaction_file_manager,
            &mut handler,
            &self.cancellation_token,
            &self.lsn_tracker,
            &self.metrics_collector,
            self.config.batch_size,
            &shared_lsn_feedback,
        )
        .await;
        // Whatever the recovery did not spend must not reach the live path.
        transaction_file_manager.set_replay_grace(0);
        if let Err(e) = recovered {
            error!(
                "Failed to process pending transaction files during recovery: {}",
                e
            );
            self.destination_handler = Some(handler);
            return Err(e);
        }
        // One group (or one transaction) can have been in flight, plus the group's own attempt.
        transaction_file_manager
            .set_replay_grace(live_replay_grace(self.slot_known_absent, after_snapshot));

        // Only now: nothing answers the walsender's keepalives until the producer polls the
        // stream, so a recovery that outlasts `wal_sender_timeout` would get the connection cut.
        if let Err(e) = Self::start_stream(&replication_stream, &self.config, start_xlog).await {
            self.destination_handler = Some(handler);
            return Err(e);
        }

        // Spawn producer
        let producer_handle = {
            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let start_lsn = start_lsn.unwrap_or_else(|| Lsn::new(0));
            let file_mgr = transaction_file_manager.clone();
            let replication_stream = replication_stream.clone();

            tokio::spawn(producer::run_producer(
                replication_stream,
                token,
                start_lsn,
                metrics,
                file_mgr,
                tx_commit_notifier,
                producer_shutdown_tx,
            ))
        };

        info!("Starting file-based consumer for transaction processing");

        let consumer_handle = tokio::spawn(consumer::run_consumer_loop(
            transaction_file_manager,
            handler,
            self.cancellation_token.clone(),
            self.metrics_collector.clone(),
            self.config.destination_type.to_string(),
            self.lsn_tracker.clone(),
            shared_lsn_feedback.clone(),
            consumer::ConsumerSettings {
                batch_size: self.config.batch_size,
                group_commit_max_txns: consumer::GROUP_COMMIT_MAX_TXNS,
            },
            rx_commit_notifier,
            producer_shutdown_rx,
        ));

        self.consumer_handle = Some(consumer_handle);
        self.producer_handle = Some(producer_handle);

        self.metrics_collector
            .update_active_connections(1, "consumer");

        Ok(())
    }

    /// Starts the stream. Its connection sat idle through the recovery, which can be long: when it
    /// died meanwhile, the stream is replaced by a fresh one that keeps the same LSN feedback (the
    /// consumer holds it), as the producer's reads replace a connection that dropped.
    async fn start_stream(
        stream: &Arc<Mutex<LogicalReplicationStream>>,
        config: &Config,
        start_xlog: Option<u64>,
    ) -> Result<()> {
        let first = stream.lock().await.start(start_xlog).await;
        let Err(e) = first else {
            return Ok(());
        };
        if !e.is_transient() {
            return Err(CdcError::from(e));
        }
        warn!(
            "Starting the replication stream failed ({}); connecting again",
            e
        );
        let mut fresh = LogicalReplicationStream::new(
            &config.source_connection_string,
            ReplicationStreamConfig::from(config),
        )
        .await?;
        let mut guard = stream.lock().await;
        fresh.shared_lsn_feedback = guard.shared_lsn_feedback.clone();
        *guard = fresh;
        guard.start(start_xlog).await?;
        Ok(())
    }

    /// Replica-identity keys of the publication's tables for a destination that keys its records
    /// (Kafka): it would otherwise learn a table's key only from its first UPDATE/DELETE and
    /// publish earlier inserts unkeyed.
    async fn publication_identity_keys(&self) -> Vec<(String, Vec<Arc<str>>)> {
        if !self
            .destination_handler
            .as_ref()
            .is_some_and(|handler| handler.supports_event_mode())
        {
            return Vec::new();
        }
        match crate::slot::query_publication_identity_keys(
            &self.config.source_connection_string,
            &self.config.publication_name,
            self.config.connection_timeout,
        )
        .await
        {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    "Could not read the replica identity keys of publication '{}': {}. INSERT \
                     messages stay unkeyed until a table's first UPDATE/DELETE",
                    self.config.publication_name, e
                );
                Vec::new()
            }
        }
    }

    fn apply_live_settings(
        handler: &mut Box<dyn DestinationHandler>,
        identity_keys: &[(String, Vec<Arc<str>>)],
    ) {
        for (table_key, columns) in identity_keys {
            handler.set_snapshot_key_columns(table_key, columns.clone());
        }
    }

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

    /// Process pending transaction files on startup (recovery).
    ///
    /// Processes all committed files from `sql_pending_tx/` in commit_lsn (WAL) order
    /// before starting normal replication. Respects cancellation token.
    async fn process_pending_transaction_files(
        file_mgr: &Arc<TransactionManager>,
        destination: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        lsn_tracker: &Arc<LsnTracker>,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    ) -> Result<()> {
        info!("Checking for pending transaction files from previous run...");

        let pending_txs = file_mgr.list_pending_transactions().await?;

        // Seed the in-memory pending counter from the authoritative dir scan.
        // The recovery loop below finalizes each of these, decrementing the
        // counter back toward 0 (dedup-skipped duplicates decrement on delete too).
        file_mgr.seed_pending_count(pending_txs.len());

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

            if let Err(e) = file_mgr
                .clone()
                .process_transaction_file(
                    pending_tx,
                    destination,
                    cancellation_token,
                    lsn_tracker,
                    metrics_collector,
                    batch_size,
                    shared_lsn_feedback,
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

    /// Stop the CDC replication process gracefully.
    ///
    /// Shutdown ordering (each step depends on the previous):
    /// 1. Cancel token → producer and consumer detect shutdown
    /// 2. Join handles → wait for both tasks to complete (consumer drains queue)
    /// 3. Final ACK → send confirmed_flush_lsn to PostgreSQL so WAL slot advances
    /// 4. Close destination → release connection pool
    /// 5. Persist LSN → final atomic write to disk (idempotent, consumer already persisted)
    pub async fn stop(&mut self) -> Result<()> {
        info!("Initiating graceful shutdown of CDC replication");

        self.cancellation_token.cancel();

        // Every step runs whatever an earlier one reported: a failed task still leaves everything
        // applied so far to be ACKed, closed and persisted (the unapplied files replay on
        // restart). The first error is returned at the end.
        let mut first_error = join_tasks(self.producer_handle.take(), self.consumer_handle.take())
            .await
            .err();
        match &first_error {
            Some(e) => error!("Shutdown did not complete cleanly: {}", e),
            None => info!("Both producer and consumer completed gracefully"),
        }

        if let Some(replication_stream) = &self.replication_stream {
            info!("Sending final ACK to PostgreSQL before shutdown");
            let mut stream = replication_stream.lock().await;
            stream
                .shared_lsn_feedback
                .log_state("Final shutdown - LSN state before ACK");

            match stream.stop().await {
                Ok(()) => info!("Final ACK sent successfully to PostgreSQL"),
                Err(e) => {
                    error!("Failed to stop replication stream: {}", e);
                    first_error.get_or_insert(CdcError::from(e));
                }
            }
        } else {
            info!("No replication stream to stop (an unfinished initial snapshot consumed it)");
        }

        // `None` once the consumer took it: the consumer closes its handler itself.
        if let Some(ref mut handler) = self.destination_handler {
            if let Err(e) = handler.close().await {
                error!("Failed to close the destination handler: {}", e);
                first_error.get_or_insert(e);
            }
        }

        self.lsn_tracker.shutdown_async().await;

        let post_shutdown_metadata = self.lsn_tracker.get_metadata();
        info!(
            "Post-shutdown state - flush_lsn={}, pending_files={}",
            pg_walstream::format_lsn(post_shutdown_metadata.lsn_tracking.flush_lsn),
            post_shutdown_metadata.consumer_state.pending_file_count
        );

        match first_error {
            Some(e) => Err(e),
            None => {
                info!("CDC replication stopped gracefully");
                Ok(())
            }
        }
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        !self.cancellation_token.is_cancelled()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn metrics_collector(&self) -> Arc<MetricsCollector> {
        self.metrics_collector.clone()
    }

    pub fn get_metrics(&self) -> Result<String> {
        self.metrics_collector.get_metrics()
    }

    pub fn init_build_info(&self, version: &str) {
        self.metrics_collector.init_build_info(version);
    }

    pub fn get_stats(&self) -> ReplicationStats {
        ReplicationStats {
            is_running: self.is_running(),
            events_processed: 0,
            last_processed_lsn: None,
            lag_seconds: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub is_running: bool,
    pub events_processed: u64,
    pub last_processed_lsn: Option<Lsn>,
    pub lag_seconds: Option<f64>,
}

impl Drop for CdcClient {
    fn drop(&mut self) {
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

    fn snapshot_config(enabled: bool) -> Config {
        Config {
            initial_snapshot: enabled,
            replication_slot_name: "slot_a".to_string(),
            ..Config::default()
        }
    }

    fn slot(confirmed: Option<u64>) -> SlotStatus {
        SlotStatus {
            confirmed_flush_lsn: confirmed,
            restart_lsn: None,
            active: false,
            wal_lost: false,
        }
    }

    #[test]
    fn plan_snapshot_maps_fail_to_config_error() {
        // Enabled, no marker, but the slot already exists: no baseline, refuse.
        let existing = slot(Some(0x100));
        let err =
            plan_snapshot(&snapshot_config(true), None, Ok(Some(&existing)), false).unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "got {err:?}");
    }

    #[test]
    fn plan_snapshot_disabled_returns_none() {
        assert_eq!(
            plan_snapshot(&snapshot_config(false), None, Ok(None), false).unwrap(),
            None
        );
    }

    #[test]
    fn plan_snapshot_enabled_fresh_returns_run() {
        assert_eq!(
            plan_snapshot(&snapshot_config(true), None, Ok(None), false).unwrap(),
            Some(SnapshotAction::Run)
        );
    }

    #[test]
    fn plan_snapshot_disabled_in_progress_marker_fails() {
        let marker = SnapshotMarker::InProgress {
            slot_name: "slot_a".to_string(),
            consistent_point: None,
            tables: vec![],
            copy_started: false,
            destination: None,
        };
        let err =
            plan_snapshot(&snapshot_config(false), Some(&marker), Ok(None), false).unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "got {err:?}");
    }

    async fn cleanup_default_metadata_file() {
        let _ = tokio::fs::remove_file("./pg2any_last_lsn.metadata").await;
    }

    #[tokio::test]
    async fn test_client_creation_and_basic_properties() {
        let cancellation_token = CancellationToken::new();
        assert!(!cancellation_token.is_cancelled());

        let token_clone = cancellation_token.clone();
        assert!(!token_clone.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_cancellation_token_cancellation() {
        let cancellation_token = CancellationToken::new();
        let token_clone = cancellation_token.clone();

        assert!(!token_clone.is_cancelled());

        cancellation_token.cancel();

        assert!(token_clone.is_cancelled());
        assert!(cancellation_token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_cancellation_token_propagation() {
        let cancellation_token = CancellationToken::new();

        let token1 = cancellation_token.clone();
        let token2 = cancellation_token.clone();

        assert!(!token1.is_cancelled());
        assert!(!token2.is_cancelled());

        token1.cancel();

        assert!(token1.is_cancelled());
        assert!(token2.is_cancelled());
        assert!(cancellation_token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_producer_task_cancellation() {
        let (_tx_sender, _tx_receiver) = mpsc::channel::<Transaction>(10);
        let cancellation_token = CancellationToken::new();

        let token_clone = cancellation_token.clone();

        let producer_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token_clone.cancelled() => {
                        info!("Producer received cancellation signal");
                        break;
                    }
                    _ = sleep(Duration::from_millis(10)) => {
                        continue;
                    }
                }
            }
            Ok::<(), CdcError>(())
        });

        sleep(Duration::from_millis(50)).await;

        cancellation_token.cancel();

        let result = timeout(Duration::from_millis(100), producer_task)
            .await
            .expect("Producer task should complete quickly after cancellation")
            .expect("Producer task should not panic");

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_task_handles() {
        let cancellation_token = CancellationToken::new();

        let token_clone = cancellation_token.clone();
        let task = tokio::spawn(async move {
            token_clone.cancelled().await;
            Ok::<(), CdcError>(())
        });

        cancellation_token.cancel();
        let result = task.await.expect("Task should complete");
        assert!(result.is_ok());
        assert!(cancellation_token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_wait_for_tasks_completion_with_no_tasks() {
        let task1 = tokio::spawn(async { Ok::<(), CdcError>(()) });
        let task2 = tokio::spawn(async { Ok::<(), CdcError>(()) });

        let (result1, result2) = tokio::join!(task1, task2);
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result1.unwrap().is_ok());
        assert!(result2.unwrap().is_ok());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_multiple_shutdown_calls_are_safe() {
        let cancellation_token = CancellationToken::new();

        assert!(!cancellation_token.is_cancelled());

        cancellation_token.cancel();
        assert!(cancellation_token.is_cancelled());

        cancellation_token.cancel();
        assert!(cancellation_token.is_cancelled());

        cancellation_token.cancel();
        assert!(cancellation_token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_client_stats_reflect_cancellation_state() {
        let cancellation_token = CancellationToken::new();

        assert!(!cancellation_token.is_cancelled());

        cancellation_token.cancel();

        assert!(cancellation_token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_cancellation_token_from_external_source() {
        let client_token = CancellationToken::new();
        let external_token = CancellationToken::new();

        let client_token_clone = client_token.clone();
        let external_token_clone = external_token.clone();
        let linking_task = tokio::spawn(async move {
            external_token_clone.cancelled().await;
            client_token_clone.cancel();
        });

        assert!(!client_token.is_cancelled());
        assert!(!external_token.is_cancelled());

        external_token.cancel();

        linking_task.await.expect("Linking task should complete");

        assert!(client_token.is_cancelled());

        cleanup_default_metadata_file().await;
    }

    #[tokio::test]
    async fn test_configurable_buffer_size() {
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

        cleanup_default_metadata_file().await;
    }
    #[derive(Default)]
    struct Settings {
        keys: Vec<String>,
    }

    struct SettingsRecorder(Arc<std::sync::Mutex<Settings>>);

    #[async_trait::async_trait]
    impl DestinationHandler for SettingsRecorder {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }
        fn set_schema_mappings(&mut self, _mappings: std::collections::HashMap<String, String>) {}
        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            _hook: Option<crate::destinations::PreCommitHook>,
        ) -> Result<()> {
            Ok(())
        }
        fn set_snapshot_key_columns(&mut self, table: &str, columns: Vec<Arc<str>>) {
            self.0
                .lock()
                .unwrap()
                .keys
                .push(format!("{table}:{}", columns.join(",")));
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// The handler that applies the recovery and the live path gets the live path's settings.
    #[test]
    fn live_settings_reach_the_handler_they_are_applied_to() {
        let keys = vec![("public.t".to_string(), vec![Arc::<str>::from("id")])];
        let seen = Arc::new(std::sync::Mutex::new(Settings::default()));
        let mut handler: Box<dyn DestinationHandler> = Box::new(SettingsRecorder(seen.clone()));
        CdcClient::apply_live_settings(&mut handler, &keys);
        assert_eq!(seen.lock().unwrap().keys, vec!["public.t:id"]);
    }

    fn task(result: Result<()>) -> Option<tokio::task::JoinHandle<Result<()>>> {
        Some(tokio::spawn(async move { result }))
    }

    #[tokio::test]
    async fn join_tasks_without_failures_is_ok() {
        assert!(join_tasks(task(Ok(())), task(Ok(()))).await.is_ok());
        assert!(join_tasks(None, None).await.is_ok());
    }

    #[tokio::test]
    async fn join_tasks_reports_the_producer_error_before_the_consumers() {
        let err = join_tasks(
            task(Err(CdcError::generic("producer failed"))),
            task(Err(CdcError::generic("consumer failed"))),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("producer failed"), "{err}");

        let err = join_tasks(
            task(Ok(())),
            task(Err(CdcError::generic("consumer failed"))),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("consumer failed"), "{err}");
    }

    #[tokio::test]
    async fn join_tasks_turns_a_panic_into_an_error() {
        let panicking: Option<tokio::task::JoinHandle<Result<()>>> =
            Some(tokio::spawn(async { panic!("boom") }));
        let err = join_tasks(panicking, task(Ok(()))).await.unwrap_err();
        assert!(err.to_string().contains("producer task panicked"), "{err}");
    }

    #[test]
    fn live_replay_grace_is_only_for_a_slot_that_may_resend_work() {
        assert_eq!(live_replay_grace(false, false), GROUP_COMMIT_MAX_TXNS + 1);
        // A slot created now starts behind everything: nothing is re-sent, a duplicate is real.
        assert_eq!(live_replay_grace(true, false), 0);
        // Right after a snapshot no live commit is in doubt either.
        assert_eq!(live_replay_grace(false, true), 0);
        assert_eq!(live_replay_grace(true, true), 0);
    }
}
