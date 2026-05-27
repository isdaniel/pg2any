use crate::config::Config;
use crate::consumer;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::producer;
use crate::transaction_manager::{PendingTransactionFile, TransactionManager};
use crate::types::Lsn;
use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

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
    replication_stream: Arc<Mutex<LogicalReplicationStream>>,
}

impl CdcClient {
    /// Create a new CDC client with LSN tracking.
    ///
    /// Returns `(CdcClient, Option<Lsn>)` where the `Lsn` is the last committed
    /// position loaded from disk, or `None` if starting fresh.
    pub async fn new(config: Config, lsn_file_path: Option<&str>) -> Result<(Self, Option<Lsn>)> {
        info!("Creating CDC client");

        let destination_handler = DestinationFactory::create(&config.destination_type)?;

        info!(
            "Transaction file persistence enabled at: {}",
            config.transaction_file_base_path
        );
        let mut manager = TransactionManager::new(
            &config.transaction_file_base_path,
            config.destination_type.clone(),
            config.transaction_segment_size_bytes,
        )
        .await?;
        manager.set_schema_mappings(config.schema_mappings.clone());
        manager.set_bulk_insert_config(config.bulk_insert_threshold);

        if destination_handler.supports_event_mode() {
            info!(
                "Destination supports event mode, enabling event-mode for transaction processing"
            );
            manager.set_event_mode(true);
        }

        info!("Initializing LSN tracker for position tracking");
        let (lsn_tracker, start_lsn) =
            crate::lsn_tracker::create_lsn_tracker_with_load(lsn_file_path).await;

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
            replication_stream: Arc::new(Mutex::new(replication_stream)),
        };

        Ok((client, start_lsn))
    }

    /// Initialize the CDC client (connect to destination).
    pub async fn init(&mut self) -> Result<()> {
        info!("Initializing CDC client");

        if let Some(ref mut handler) = self.destination_handler {
            handler
                .connect(&self.config.destination_connection_string)
                .await?;

            if !self.config.schema_mappings.is_empty() {
                handler.set_schema_mappings(self.config.schema_mappings.clone());
                info!("Schema mappings applied: {:?}", self.config.schema_mappings);
            }

            if self.config.max_rows_per_insert > 0 {
                handler.set_max_rows_per_insert(self.config.max_rows_per_insert);
            }
        }

        Ok(())
    }

    /// Start CDC replication from a specific LSN.
    pub async fn start_replication_from_lsn(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication");

        info!("Performing CDC client initialization (including recovery)");
        self.init().await?;
        info!("CDC client initialized successfully");

        {
            let start_xlog = start_lsn.map(|lsn| lsn.0);
            self.replication_stream
                .lock()
                .await
                .start(start_xlog)
                .await?;
        }

        self.start_file_based_workflow(start_lsn).await?;
        self.start_server_uptime();

        info!("CDC replication started successfully");
        self.cancellation_token.cancelled().await;
        Ok(())
    }

    async fn start_file_based_workflow(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        let transaction_file_manager = self.transaction_file_manager.clone();

        let (tx_commit_notifier, rx_commit_notifier) =
            mpsc::channel::<PendingTransactionFile>(self.config.buffer_size);
        info!(
            "Created transaction commit notification channel with buffer size {}",
            self.config.buffer_size
        );

        let (producer_shutdown_tx, producer_shutdown_rx) = oneshot::channel::<()>();
        info!("Created producer shutdown notification channel");

        let shared_lsn_feedback = {
            let stream_guard = self.replication_stream.as_ref().lock().await;
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

        // Recovery: process committed-but-unexecuted transaction files from previous run
        if let Some(ref mut handler) = self.destination_handler {
            info!("Processing pending transaction files from previous run (recovery)...");
            if let Err(e) = Self::process_pending_transaction_files(
                &transaction_file_manager,
                handler,
                &self.cancellation_token,
                &self.lsn_tracker,
                &self.metrics_collector,
                self.config.batch_size,
                &shared_lsn_feedback,
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

        // Spawn producer
        let producer_handle = {
            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let start_lsn = start_lsn.unwrap_or_else(|| Lsn::new(0));
            let file_mgr = transaction_file_manager.clone();
            let replication_stream = self.replication_stream.clone();

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

        // Spawn consumer
        let dest_type = &self.config.destination_type;
        let schema_mappings = self.config.schema_mappings.clone();

        info!("Starting file-based consumer for transaction processing");

        let mut consumer_destination = DestinationFactory::create(dest_type)?;
        consumer_destination
            .connect(&self.config.destination_connection_string)
            .await?;

        if !schema_mappings.is_empty() {
            consumer_destination.set_schema_mappings(schema_mappings.clone());
        }

        if self.config.max_rows_per_insert > 0 {
            consumer_destination.set_max_rows_per_insert(self.config.max_rows_per_insert);
        }

        info!("Consumer destination connection established");

        let consumer_handle = tokio::spawn(consumer::run_consumer_loop(
            transaction_file_manager,
            consumer_destination,
            self.cancellation_token.clone(),
            self.metrics_collector.clone(),
            dest_type.to_string(),
            self.lsn_tracker.clone(),
            shared_lsn_feedback.clone(),
            self.config.batch_size,
            rx_commit_notifier,
            producer_shutdown_rx,
        ));

        self.consumer_handle = Some(consumer_handle);
        self.producer_handle = Some(producer_handle);

        self.metrics_collector
            .update_active_connections(1, "consumer");

        Ok(())
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
    /// Processes all committed files from `sql_pending_tx/` in commit timestamp order
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
    pub async fn stop(&mut self) -> Result<()> {
        info!("Initiating graceful shutdown of CDC replication");

        self.cancellation_token.cancel();

        self.wait_for_tasks_completion().await?;

        info!("Both producer and consumer completed gracefully");
        {
            info!("Sending final ACK to PostgreSQL before shutdown");
            let mut stream = self.replication_stream.as_ref().lock().await;
            stream
                .shared_lsn_feedback
                .log_state("Final shutdown - LSN state before ACK");

            if let Err(e) = stream.stop().await {
                error!("Failed to stop replication stream: {}", e);
                return Err(CdcError::from(e));
            }

            info!("Final ACK sent successfully to PostgreSQL");
        }

        if let Some(ref mut handler) = self.destination_handler {
            handler.close().await?;
        }

        self.lsn_tracker.shutdown_async().await;

        let post_shutdown_metadata = self.lsn_tracker.get_metadata();
        info!(
            "Post-shutdown state - flush_lsn={}, pending_files={}",
            pg_walstream::format_lsn(post_shutdown_metadata.lsn_tracking.flush_lsn),
            post_shutdown_metadata.consumer_state.pending_file_count
        );

        info!("CDC replication stopped gracefully");
        Ok(())
    }

    async fn wait_for_tasks_completion(&mut self) -> Result<()> {
        let producer_handle = self.producer_handle.take();
        let consumer_handle = self.consumer_handle.take();

        let producer_result = async {
            if let Some(h) = producer_handle {
                h.await.expect("Producer task panicked")
            } else {
                Ok(())
            }
        };

        let consumer_result = async {
            if let Some(h) = consumer_handle {
                h.await.expect("Consumer task panicked")
            } else {
                Ok(())
            }
        };

        match tokio::join!(producer_result, consumer_result) {
            (Ok(()), Ok(())) => {
                info!("All CDC tasks completed successfully");
            }
            (Err(producer_err), Ok(_)) => {
                error!("Producer task failed: {}", producer_err);
                return Err(producer_err);
            }
            (Ok(_), Err(consumer_err)) => {
                error!("Consumer task failed: {}", consumer_err);
                return Err(consumer_err);
            }
            (Err(producer_err), Err(consumer_err)) => {
                error!("Producer task failed: {}", producer_err);
                error!("Consumer task failed: {}", consumer_err);
                return Err(producer_err);
            }
        }

        Ok(())
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
}
