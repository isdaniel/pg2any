use crate::destinations::DestinationHandler;
use crate::error::Result;
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::transaction_manager::{PendingTransactionFile, TransactionManager};
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Consumer loop for file-based transaction processing.
///
/// Maintains a priority queue (min-heap) ordered by `commit_lsn` and processes
/// transactions in ascending LSN order. ACKs to PostgreSQL are sent only after
/// successful apply to the destination.
///
/// ## Shutdown Coordination
///
/// The consumer detects shutdown via two signals:
/// - **Producer oneshot**: producer has flushed and stopped — drain remaining work.
/// - **mpsc channel closed**: producer dropped the sender — wait for oneshot, then drain.
///
/// After the main loop exits (for any reason), `drain_and_shutdown` is called once
/// to process all remaining queued transactions and persist the final position.
pub(crate) async fn run_consumer_loop(
    transaction_file_manager: Arc<TransactionManager>,
    mut destination_handler: Box<dyn DestinationHandler>,
    cancellation_token: CancellationToken,
    metrics_collector: Arc<MetricsCollector>,
    destination_type: String,
    lsn_tracker: Arc<LsnTracker>,
    shared_lsn_feedback: Arc<SharedLsnFeedback>,
    batch_size: usize,
    mut commit_receiver: mpsc::Receiver<PendingTransactionFile>,
    mut producer_shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    info!("Starting file-based consumer loop with commit_lsn ordering (protocol compliant)");

    metrics_collector.update_destination_connection_status(&destination_type, true);

    let mut commit_queue: BinaryHeap<std::cmp::Reverse<PendingTransactionFile>> = BinaryHeap::new();

    let mut retry_deadline: Option<tokio::time::Instant> = None;
    let mut retry_count: u32 = 0;

    loop {
        let sleep_deadline = retry_deadline.unwrap_or_else(tokio::time::Instant::now);

        tokio::select! {
            biased;

            _ = &mut producer_shutdown_rx => {
                info!("Consumer: Received producer shutdown signal, draining queue");
                break;
            }

            result = commit_receiver.recv() => {
                match result {
                    Some(notification) => {
                        debug!(
                            "Consumer received commit notification for transaction {} (commit_lsn: {:?}) with file {:?}",
                            notification.metadata.transaction_id, notification.metadata.commit_lsn, notification.file_path
                        );
                        commit_queue.push(std::cmp::Reverse(notification));
                    }
                    None => {
                        warn!("Consumer: mpsc channel closed, waiting for producer shutdown signal");
                        let _ = producer_shutdown_rx.await;
                        break;
                    }
                }
            }

            _ = tokio::time::sleep_until(sleep_deadline), if retry_deadline.is_some() => {
                retry_deadline = None;
            }
        }

        // Process queue when not in backoff and there's work to do
        if retry_deadline.is_none() && !commit_queue.is_empty() {
            let cancelled = process_commit_queue(
                &mut commit_queue,
                &transaction_file_manager,
                &mut destination_handler,
                &cancellation_token,
                &lsn_tracker,
                &metrics_collector,
                batch_size,
                &shared_lsn_feedback,
                &mut retry_deadline,
                &mut retry_count,
            )
            .await;

            if cancelled {
                info!("Consumer: cancellation detected, draining remaining queue");
                break;
            }
        }
    }

    drain_and_shutdown(
        &mut commit_queue,
        &mut commit_receiver,
        &transaction_file_manager,
        &mut destination_handler,
        &lsn_tracker,
        &metrics_collector,
        batch_size,
        &shared_lsn_feedback,
    )
    .await;

    metrics_collector.update_destination_connection_status(&destination_type, false);

    info!("Consumer stopped gracefully");
    Ok(())
}

/// Drain remaining channel messages, process all queued transactions, then
/// persist position and exit.
///
/// Uses a 90-second timeout to leave headroom within the Docker stop grace
/// period (typically 120s). If the timeout fires, processing stops and the
/// current position is persisted — unprocessed files remain in `sql_pending_tx/`
/// for recovery on the next restart.
async fn drain_and_shutdown(
    commit_queue: &mut BinaryHeap<std::cmp::Reverse<PendingTransactionFile>>,
    commit_receiver: &mut mpsc::Receiver<PendingTransactionFile>,
    transaction_file_manager: &Arc<TransactionManager>,
    destination_handler: &mut Box<dyn DestinationHandler>,
    lsn_tracker: &Arc<LsnTracker>,
    metrics_collector: &Arc<MetricsCollector>,
    batch_size: usize,
    shared_lsn_feedback: &Arc<SharedLsnFeedback>,
) {
    if let Err(e) = transaction_file_manager
        .flush_staged_pending_progress()
        .await
    {
        warn!("Failed to flush staged progress before drain: {}", e);
    }

    while let Ok(notification) = commit_receiver.try_recv() {
        commit_queue.push(std::cmp::Reverse(notification));
    }

    if commit_queue.is_empty() {
        flush_and_persist_on_shutdown(transaction_file_manager, lsn_tracker).await;
        info!("Consumer: Shutdown complete, no pending transactions, position persisted");
        shared_lsn_feedback.log_state("Consumer shutdown - final LSN state");
        return;
    }

    info!(
        "Consumer: Processing {} queued transaction(s) before shutdown",
        commit_queue.len()
    );

    let drain_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);

    while let Some(std::cmp::Reverse(next_tx)) = commit_queue.pop() {
        if tokio::time::Instant::now() >= drain_deadline {
            warn!(
                "Consumer: Drain timeout reached with {} transaction(s) remaining, persisting position",
                commit_queue.len() + 1
            );
            commit_queue.push(std::cmp::Reverse(next_tx));
            break;
        }

        info!(
            "Consumer drain: processing transaction {} (LSN: {:?})",
            next_tx.metadata.transaction_id, next_tx.metadata.commit_lsn
        );

        let drain_token = CancellationToken::new();

        if let Err(e) = transaction_file_manager
            .clone()
            .process_transaction_file(
                &next_tx,
                destination_handler,
                &drain_token,
                lsn_tracker,
                metrics_collector,
                batch_size,
                shared_lsn_feedback,
            )
            .await
        {
            warn!(
                "Consumer drain: failed to process transaction {}: {}. File remains in sql_pending_tx/ for recovery.",
                next_tx.metadata.transaction_id, e
            );
            break;
        }
    }

    flush_and_persist_on_shutdown(transaction_file_manager, lsn_tracker).await;
    info!("Consumer: Shutdown complete, position persisted");
    shared_lsn_feedback.log_state("Consumer shutdown - final LSN state");
}

/// Process all transactions in the commit queue in `commit_lsn` order.
///
/// On failure, re-inserts the failed transaction and sets a retry deadline
/// with exponential backoff (2s, 4s, 8s, 16s, capped at 30s).
///
/// Returns `true` if a transaction was cancelled by the shutdown signal.
async fn process_commit_queue(
    commit_queue: &mut BinaryHeap<std::cmp::Reverse<PendingTransactionFile>>,
    transaction_file_manager: &Arc<TransactionManager>,
    destination_handler: &mut Box<dyn DestinationHandler>,
    cancellation_token: &CancellationToken,
    lsn_tracker: &Arc<LsnTracker>,
    metrics_collector: &Arc<MetricsCollector>,
    batch_size: usize,
    shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    retry_deadline: &mut Option<tokio::time::Instant>,
    retry_count: &mut u32,
) -> bool {
    if commit_queue.is_empty() {
        return false;
    }

    while let Some(std::cmp::Reverse(next_tx)) = commit_queue.pop() {
        info!(
            "Consumer processing transaction {} in commit_lsn order (LSN: {:?})",
            next_tx.metadata.transaction_id, next_tx.metadata.commit_lsn
        );

        if let Err(e) = transaction_file_manager
            .clone()
            .process_transaction_file(
                &next_tx,
                destination_handler,
                cancellation_token,
                lsn_tracker,
                metrics_collector,
                batch_size,
                shared_lsn_feedback,
            )
            .await
        {
            if e.is_cancelled() {
                info!(
                    "Consumer: transaction {} cancelled by shutdown, will be recovered on restart",
                    next_tx.metadata.transaction_id
                );
                commit_queue.push(std::cmp::Reverse(next_tx));
                return true;
            }

            error!(
                "Failed to process transaction {} from file {:?}: {}. Will retry after backoff.",
                next_tx.metadata.transaction_id, next_tx.file_path, e
            );
            metrics_collector.record_error("transaction_file_processing_failed", "consumer");
            commit_queue.push(std::cmp::Reverse(next_tx));

            *retry_count = retry_count.saturating_add(1);
            let backoff_secs = 2u64.saturating_pow(*retry_count).min(30);
            *retry_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_secs(backoff_secs));
            info!(
                "Consumer: scheduling retry in {}s for {} queued transaction(s)",
                backoff_secs,
                commit_queue.len()
            );
            break;
        }
    }

    if commit_queue.is_empty() {
        *retry_deadline = None;
        *retry_count = 0;
    }
    false
}

#[inline]
async fn flush_and_persist_on_shutdown(
    transaction_file_manager: &Arc<TransactionManager>,
    lsn_tracker: &Arc<LsnTracker>,
) {
    if let Err(e) = transaction_file_manager
        .flush_staged_pending_progress()
        .await
    {
        warn!("Failed to flush staged pending progress on shutdown: {}", e);
    }

    if let Err(e) = lsn_tracker.persist_async().await {
        warn!("Failed to persist LSN on consumer shutdown: {}", e);
    }
}
