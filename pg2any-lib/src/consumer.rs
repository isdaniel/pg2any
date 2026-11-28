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

type CommitQueue = BinaryHeap<std::cmp::Reverse<PendingTransactionFile>>;

/// Most queued source transactions merged into one destination transaction (group commit).
///
/// A group is only what is ALREADY queued when the consumer picks up work, it never waits for more:
/// three transactions followed by silence are committed at once, whatever the cap. The cap bounds
/// the work lost to a failing group (the transactions are then applied one by one) and the number
/// of transactions a crash can leave committed-but-unrecorded, which startup recovery has to
/// tolerate (see `client.rs`). A group is also limited to one destination batch of statements.
pub const GROUP_COMMIT_MAX_TXNS: usize = 128;

/// Most transactions the consumer queues while the destination is failing and it waits for its
/// retry. Past it the consumer stops reading the channel, which fills and blocks the producer.
const MAX_QUEUED_TXNS_IN_BACKOFF: usize = 10_000;

/// Whether the consumer may take another notification off the channel.
fn may_receive(in_backoff: bool, queued: usize) -> bool {
    !in_backoff || queued < MAX_QUEUED_TXNS_IN_BACKOFF
}

/// Tuning knobs of the consumer task.
#[derive(Debug, Clone)]
pub struct ConsumerSettings {
    /// Commands per destination batch.
    pub batch_size: usize,
    /// Merge up to this many consecutive queued small transactions into one destination
    /// transaction (`GROUP_COMMIT_MAX_TXNS`; `1` applies every transaction on its own). Never waits
    /// for more work, only merges what is already queued.
    pub group_commit_max_txns: usize,
}

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
/// If cancellation interrupts the consumer while it is applying, it first waits for the
/// producer to finish (still receiving, so the producer can never block on a full channel) and
/// only then drains, so a transaction the producer committed during shutdown is not left behind.
///
/// After the main loop exits (for any reason), `drain_and_shutdown` is called once
/// to process all remaining queued transactions and persist the final position. It has no time
/// limit of its own: the process manager's stop timeout (SIGKILL) is what ends a drain against a
/// destination that never recovers, and the files left in `sql_pending_tx/` replay on the next start.
pub(crate) async fn run_consumer_loop(
    transaction_file_manager: Arc<TransactionManager>,
    mut destination_handler: Box<dyn DestinationHandler>,
    cancellation_token: CancellationToken,
    metrics_collector: Arc<MetricsCollector>,
    destination_type: String,
    lsn_tracker: Arc<LsnTracker>,
    shared_lsn_feedback: Arc<SharedLsnFeedback>,
    settings: ConsumerSettings,
    mut commit_receiver: mpsc::Receiver<PendingTransactionFile>,
    mut producer_shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    info!("Starting file-based consumer loop with commit_lsn ordering (protocol compliant)");

    metrics_collector.update_destination_connection_status(&destination_type, true);

    let mut commit_queue: CommitQueue = BinaryHeap::new();

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

            // A producer blocked on a full channel (the consumer stopped reading it while it backs
            // off) only reaches its shutdown once the consumer receives again.
            _ = cancellation_token.cancelled() => {
                info!("Consumer: cancellation detected, waiting for the producer before draining");
                wait_for_producer(
                    &mut commit_queue,
                    &mut commit_receiver,
                    producer_shutdown_rx,
                )
                .await;
                break;
            }

            _ = tokio::time::sleep_until(sleep_deadline), if retry_deadline.is_some() => {
                retry_deadline = None;
            }

            result = commit_receiver.recv(), if may_receive(retry_deadline.is_some(), commit_queue.len()) => {
                match result {
                    Some(notification) => {
                        debug!(
                            "Consumer received commit notification for transaction {} (commit_lsn: {:?}) with file {:?}",
                            notification.metadata.transaction_id, notification.metadata.commit_lsn, notification.file_path
                        );
                        commit_queue.push(std::cmp::Reverse(notification));
                        if settings.group_commit_max_txns > 1 {
                            while let Ok(more) = commit_receiver.try_recv() {
                                commit_queue.push(std::cmp::Reverse(more));
                            }
                        }
                    }
                    None => {
                        warn!("Consumer: mpsc channel closed, waiting for producer shutdown signal");
                        let _ = producer_shutdown_rx.await;
                        break;
                    }
                }
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
                &settings,
                &shared_lsn_feedback,
                &mut retry_deadline,
                &mut retry_count,
            )
            .await;

            if cancelled {
                info!("Consumer: cancellation detected, waiting for the producer before draining");
                wait_for_producer(
                    &mut commit_queue,
                    &mut commit_receiver,
                    producer_shutdown_rx,
                )
                .await;
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
        &settings,
        &shared_lsn_feedback,
    )
    .await;

    if let Err(e) = destination_handler.close().await {
        error!("Failed to close consumer destination handler: {}", e);
    }

    metrics_collector.update_destination_connection_status(&destination_type, false);

    info!("Consumer stopped gracefully");
    Ok(())
}

/// After a cancellation mid-apply: keep receiving until the producer reports it has stopped,
/// so the final drain sees every transaction the producer committed. Receiving continues while
/// waiting because the producer's `send().await` would otherwise block on a full channel.
/// A producer that never stops is ended by the process manager's stop timeout, not here.
async fn wait_for_producer(
    commit_queue: &mut CommitQueue,
    commit_receiver: &mut mpsc::Receiver<PendingTransactionFile>,
    mut producer_shutdown_rx: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;

            _ = &mut producer_shutdown_rx => return,

            result = commit_receiver.recv() => match result {
                Some(notification) => commit_queue.push(std::cmp::Reverse(notification)),
                None => {
                    let _ = producer_shutdown_rx.await;
                    return;
                }
            },
        }
    }
}

/// Drain remaining channel messages, process all queued transactions, then
/// persist position and exit.
///
/// **Shutdown correctness guarantee:** This function uses an uncancellable
/// `CancellationToken` (one that is never cancelled) when processing queued
/// transactions. This ensures all in-progress work completes to maintain
/// position-tracking correctness — if a transaction is partially applied
/// without LSN advancement, the next restart would replay it.
///
/// A failing destination is retried with the live path's backoff until everything is applied:
/// there is no time limit here, the process manager's stop timeout (SIGKILL) ends a drain against
/// a destination that never recovers. The files of unapplied transactions remain in
/// `sql_pending_tx/` and are replayed on the next start (in-memory ones are re-sent by PostgreSQL).
pub async fn drain_and_shutdown(
    commit_queue: &mut BinaryHeap<std::cmp::Reverse<PendingTransactionFile>>,
    commit_receiver: &mut mpsc::Receiver<PendingTransactionFile>,
    transaction_file_manager: &Arc<TransactionManager>,
    destination_handler: &mut Box<dyn DestinationHandler>,
    lsn_tracker: &Arc<LsnTracker>,
    metrics_collector: &Arc<MetricsCollector>,
    settings: &ConsumerSettings,
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
        "Consumer: Processing {} queued transaction(s) before shutdown (retrying until applied)",
        commit_queue.len()
    );

    let uncancellable_token = CancellationToken::new();
    let mut retry_deadline: Option<tokio::time::Instant> = None;
    let mut retry_count: u32 = 0;

    loop {
        process_commit_queue(
            commit_queue,
            transaction_file_manager,
            destination_handler,
            &uncancellable_token,
            lsn_tracker,
            metrics_collector,
            settings,
            shared_lsn_feedback,
            &mut retry_deadline,
            &mut retry_count,
        )
        .await;

        if commit_queue.is_empty() {
            break;
        }

        let retry_at = retry_deadline
            .take()
            .unwrap_or_else(tokio::time::Instant::now);
        tokio::time::sleep_until(retry_at).await;
    }

    flush_and_persist_on_shutdown(transaction_file_manager, lsn_tracker).await;
    shared_lsn_feedback.log_state("Consumer shutdown - final LSN state");
    info!("Consumer: Shutdown complete, position persisted");
}

/// Take the transactions that may be applied together with `work[0]`: queued successors,
/// in `commit_lsn` order, that are fresh SQL-mode transactions, until `max_txns` or
/// `max_statements` is reached.
fn extend_group(
    commit_queue: &mut CommitQueue,
    manager: &TransactionManager,
    work: &mut Vec<PendingTransactionFile>,
    max_txns: usize,
    max_statements: usize,
) {
    let Some(mut statements) = manager.groupable_statement_count(&work[0]) else {
        return;
    };
    while work.len() < max_txns {
        let Some(next) = commit_queue.peek() else {
            break;
        };
        match manager.groupable_statement_count(&next.0) {
            Some(n) if statements + n <= max_statements => statements += n,
            _ => break,
        }
        if let Some(std::cmp::Reverse(tx)) = commit_queue.pop() {
            work.push(tx);
        }
    }
}

/// Process all transactions in the commit queue in `commit_lsn` order.
///
/// On failure, re-inserts the failed transaction and sets a retry deadline
/// with exponential backoff (2s, 4s, 8s, 16s, capped at 30s).
///
/// Returns `true` if a transaction was cancelled by the shutdown signal.
async fn process_commit_queue(
    commit_queue: &mut CommitQueue,
    transaction_file_manager: &Arc<TransactionManager>,
    destination_handler: &mut Box<dyn DestinationHandler>,
    cancellation_token: &CancellationToken,
    lsn_tracker: &Arc<LsnTracker>,
    metrics_collector: &Arc<MetricsCollector>,
    settings: &ConsumerSettings,
    shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    retry_deadline: &mut Option<tokio::time::Instant>,
    retry_count: &mut u32,
) -> bool {
    if commit_queue.is_empty() {
        return false;
    }

    while let Some(std::cmp::Reverse(first)) = commit_queue.pop() {
        let mut work = vec![first];
        extend_group(
            commit_queue,
            transaction_file_manager,
            &mut work,
            settings.group_commit_max_txns,
            settings.batch_size,
        );

        if work.len() > 1 {
            match transaction_file_manager
                .clone()
                .process_transaction_group(
                    &work,
                    destination_handler,
                    cancellation_token,
                    lsn_tracker,
                    metrics_collector,
                    shared_lsn_feedback,
                )
                .await
            {
                Ok(()) => continue,
                Err(e) if e.is_cancelled() => {
                    info!(
                        "Consumer: group of {} transactions cancelled by shutdown, will be recovered on restart",
                        work.len()
                    );
                    commit_queue.extend(work.into_iter().map(std::cmp::Reverse));
                    return true;
                }
                // Nothing was committed or changed: apply the same transactions one by one so
                // an error is attributed to (and retried for) the transaction that causes it.
                Err(e) => warn!(
                    "Consumer: group commit of {} transactions failed ({}), applying them one by one",
                    work.len(),
                    e
                ),
            }
        }

        let mut remaining = work.into_iter();
        while let Some(next_tx) = remaining.next() {
            info!(
                "Consumer processing transaction {} in commit_lsn order (LSN: {:?})",
                next_tx.metadata.transaction_id, next_tx.metadata.commit_lsn
            );

            let Err(e) = transaction_file_manager
                .clone()
                .process_transaction_file(
                    &next_tx,
                    destination_handler,
                    cancellation_token,
                    lsn_tracker,
                    metrics_collector,
                    settings.batch_size,
                    shared_lsn_feedback,
                )
                .await
            else {
                continue;
            };

            // The pre-commit hook may have staged a checkpoint for a batch whose commit then
            // failed. Persisting it (the shutdown flush does) would make a restart skip a batch
            // the destination never applied.
            transaction_file_manager
                .clear_staged_pending_progress(&next_tx.file_path)
                .await;

            let cancelled = e.is_cancelled();
            if cancelled {
                info!(
                    "Consumer: transaction {} cancelled by shutdown, will be recovered on restart",
                    next_tx.metadata.transaction_id
                );
            } else {
                error!(
                    "Failed to process transaction {} from file {:?}: {}. Will retry after backoff.",
                    next_tx.metadata.transaction_id, next_tx.file_path, e
                );
                metrics_collector.record_error("transaction_file_processing_failed", "consumer");
            }
            commit_queue.push(std::cmp::Reverse(next_tx));
            commit_queue.extend(remaining.map(std::cmp::Reverse));

            if cancelled {
                return true;
            }

            *retry_count = retry_count.saturating_add(1);
            let backoff_secs = 2u64.saturating_pow(*retry_count).min(30);
            *retry_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_secs(backoff_secs));
            info!(
                "Consumer: scheduling retry in {}s for {} queued transaction(s)",
                backoff_secs,
                commit_queue.len()
            );
            return false;
        }
    }

    *retry_deadline = None;
    *retry_count = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destinations::PreCommitHook;
    use crate::error::CdcError;
    use crate::types::{DestinationType, Lsn};
    use async_trait::async_trait;
    use chrono::Utc;
    use pg_walstream::{ChangeEvent, ColumnValue, RowData};
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::Notify;

    /// Blocks inside its first batch until released, then applies everything.
    struct BlockingFirstBatch {
        calls: usize,
        entered: Arc<Notify>,
        release: Arc<Notify>,
        applied: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl DestinationHandler for BlockingFirstBatch {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

        async fn execute_sql_batch_with_hook(
            &mut self,
            commands: &[String],
            pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            self.calls += 1;
            if self.calls == 1 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }
            self.applied
                .lock()
                .unwrap()
                .extend(commands.iter().cloned());
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    async fn commit_tx(
        manager: &Arc<TransactionManager>,
        tx_id: u32,
        lsn: u64,
        rows: usize,
    ) -> PendingTransactionFile {
        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();
        for id in 0..rows {
            let data = RowData::from_pairs(vec![
                ("id", ColumnValue::text(&format!("{tx_id}{id}"))),
                ("name", ColumnValue::text(&format!("tx{tx_id}-row{id}"))),
            ]);
            let event = ChangeEvent::insert("public", "t", 1, data, Lsn::from(lsn));
            manager.append_event(tx_id, &event).await.unwrap();
        }
        manager.flush_all_buffers().await.unwrap();
        let (file_path, metadata) = manager
            .commit_transaction(tx_id, Some(Lsn(lsn)))
            .await
            .unwrap();
        PendingTransactionFile {
            file_path,
            metadata,
        }
    }

    /// Shutdown arrives while the consumer is applying; the producer commits one more
    /// transaction after that. The consumer must wait for the producer and apply it too.
    #[tokio::test]
    async fn cancellation_mid_apply_still_drains_what_the_producer_commits_afterwards() {
        let dir = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(
            TransactionManager::new(dir.path(), DestinationType::MySQL, None, 64 * 1024 * 1024)
                .await
                .unwrap(),
        );
        let first = commit_tx(&manager, 1, 1000, 3).await;
        let late = commit_tx(&manager, 2, 2000, 1).await;

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let applied = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dest: Box<dyn DestinationHandler> = Box::new(BlockingFirstBatch {
            calls: 0,
            entered: entered.clone(),
            release: release.clone(),
            applied: applied.clone(),
        });

        let lsn_path = dir.path().join("lsn");
        let lsn_tracker = LsnTracker::new(Some(lsn_path.to_str().unwrap())).await;
        let token = CancellationToken::new();
        let (commit_tx_sender, commit_rx) = mpsc::channel(8);
        let (producer_done_tx, producer_done_rx) = oneshot::channel();

        let consumer = tokio::spawn(run_consumer_loop(
            manager.clone(),
            dest,
            token.clone(),
            Arc::new(MetricsCollector::new()),
            "mysql".to_string(),
            lsn_tracker.clone(),
            SharedLsnFeedback::new_shared(),
            ConsumerSettings {
                batch_size: 1, // one statement per batch: a cancel is seen at a batch boundary
                group_commit_max_txns: 1,
            },
            commit_rx,
            producer_done_rx,
        ));

        commit_tx_sender.send(first).await.unwrap();
        entered.notified().await;
        token.cancel();
        release.notify_one();

        // The consumer has now observed the cancellation and must be waiting for the producer.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !consumer.is_finished(),
            "consumer left before the producer stopped"
        );
        commit_tx_sender.send(late).await.unwrap();
        drop(commit_tx_sender);
        producer_done_tx.send(()).unwrap();

        consumer.await.unwrap().unwrap();
        assert_eq!(
            lsn_tracker.get(),
            2000,
            "the late transaction was applied before exit"
        );
        let applied = applied.lock().unwrap();
        assert_eq!(
            applied.len(),
            4,
            "3 statements of the first + 1 of the late transaction"
        );
        assert!(applied[3].contains("tx2-row0"));
    }

    /// Runs the pre-commit hook (which stages the resume checkpoint) and then fails the COMMIT.
    struct FailAfterHook;

    #[async_trait]
    impl DestinationHandler for FailAfterHook {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }
            Err(CdcError::generic("simulated COMMIT failure"))
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A batch whose COMMIT fails after the pre-commit hook staged its checkpoint must not leave
    /// that checkpoint behind: the shutdown flush would persist it and a restart would skip a
    /// batch the destination never applied.
    #[tokio::test]
    async fn a_failed_commit_leaves_no_staged_checkpoint_for_the_shutdown_flush() {
        let dir = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(
            TransactionManager::new(dir.path(), DestinationType::MySQL, None, 64 * 1024 * 1024)
                .await
                .unwrap(),
        );
        let mut queue: CommitQueue = BinaryHeap::new();
        queue.push(std::cmp::Reverse(commit_tx(&manager, 1, 1000, 3).await));
        let mut destination: Box<dyn DestinationHandler> = Box::new(FailAfterHook);
        let lsn_path = dir.path().join("lsn");
        let lsn_tracker = LsnTracker::new(Some(lsn_path.to_str().unwrap())).await;
        let (mut retry_deadline, mut retry_count) = (None, 0);

        let cancelled = process_commit_queue(
            &mut queue,
            &manager,
            &mut destination,
            &CancellationToken::new(),
            &lsn_tracker,
            &Arc::new(MetricsCollector::new()),
            &ConsumerSettings {
                batch_size: 1000,
                group_commit_max_txns: 1,
            },
            &SharedLsnFeedback::new_shared(),
            &mut retry_deadline,
            &mut retry_count,
        )
        .await;

        assert!(!cancelled);
        assert_eq!(queue.len(), 1, "the failed transaction waits for its retry");
        assert!(retry_deadline.is_some());
        manager.flush_staged_pending_progress().await.unwrap();
        let pending = manager.list_pending_transactions().await.unwrap();
        assert_eq!(
            pending[0].metadata.last_executed_command_index, None,
            "a batch that was not committed must not be recorded as executed"
        );
    }

    /// Records the size of every destination commit.
    struct CommitLog {
        commits: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl DestinationHandler for CommitLog {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

        async fn execute_sql_batch_with_hook(
            &mut self,
            commands: &[String],
            pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }
            self.commits.lock().unwrap().push(commands.len());
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A destination batch of 128 statements and a few transactions that are followed by silence
    /// (the channel and the producer stay open): they are committed at once, nothing waits for
    /// more transactions to fill a group or the batch.
    #[tokio::test]
    async fn three_transactions_then_silence_are_committed_at_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(
            TransactionManager::new(dir.path(), DestinationType::MySQL, None, 64 * 1024 * 1024)
                .await
                .unwrap(),
        );
        let commits = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dest: Box<dyn DestinationHandler> = Box::new(CommitLog {
            commits: commits.clone(),
        });
        let lsn_path = dir.path().join("lsn");
        let lsn_tracker = LsnTracker::new(Some(lsn_path.to_str().unwrap())).await;
        let (sender, commit_rx) = mpsc::channel(8);
        let (_producer_done_tx, producer_done_rx) = oneshot::channel::<()>();
        let consumer = tokio::spawn(run_consumer_loop(
            manager.clone(),
            dest,
            CancellationToken::new(),
            Arc::new(MetricsCollector::new()),
            "mysql".to_string(),
            lsn_tracker.clone(),
            SharedLsnFeedback::new_shared(),
            ConsumerSettings {
                batch_size: 128,
                group_commit_max_txns: GROUP_COMMIT_MAX_TXNS,
            },
            commit_rx,
            producer_done_rx,
        ));

        for (id, lsn) in [(1u32, 1000u64), (2, 2000), (3, 3000)] {
            sender
                .send(commit_tx(&manager, id, lsn, 1).await)
                .await
                .unwrap();
        }
        // Nothing else arrives and nothing closes: the position must still reach the third and
        // the files must be gone (they are deleted just after the position moves).
        tokio::time::timeout(Duration::from_secs(5), async {
            while lsn_tracker.get() < 3000
                || !manager
                    .list_pending_transactions()
                    .await
                    .unwrap()
                    .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("three transactions followed by silence were held back");

        let commits = commits.lock().unwrap().clone();
        assert_eq!(commits.iter().sum::<usize>(), 3, "{commits:?}");
        consumer.abort();
    }

    #[test]
    fn receiving_stops_in_backoff_once_the_queue_is_full() {
        assert!(may_receive(false, usize::MAX));
        assert!(may_receive(true, 0));
        assert!(may_receive(true, MAX_QUEUED_TXNS_IN_BACKOFF - 1));
        assert!(!may_receive(true, MAX_QUEUED_TXNS_IN_BACKOFF));
    }

    /// Every batch fails, so the consumer keeps backing off.
    struct AlwaysFails;

    #[async_trait]
    impl DestinationHandler for AlwaysFails {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            _pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            Err(CdcError::generic("destination is down"))
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct BackedOff {
        _dir: tempfile::TempDir,
        template: PendingTransactionFile,
        sender: mpsc::Sender<PendingTransactionFile>,
        token: CancellationToken,
        producer_done: oneshot::Sender<()>,
        consumer: tokio::task::JoinHandle<Result<()>>,
        next_lsn: u64,
    }

    impl BackedOff {
        /// A consumer whose destination is down and that has failed its first transaction, so it
        /// now waits for its retry.
        async fn start() -> Self {
            let dir = tempfile::TempDir::new().unwrap();
            let manager = Arc::new(
                TransactionManager::new(dir.path(), DestinationType::MySQL, None, 64 * 1024 * 1024)
                    .await
                    .unwrap(),
            );
            let template = commit_tx(&manager, 1, 1000, 1).await;
            let lsn_path = dir.path().join("lsn");
            let lsn_tracker = LsnTracker::new(Some(lsn_path.to_str().unwrap())).await;
            let token = CancellationToken::new();
            let (sender, commit_rx) = mpsc::channel(8);
            let (producer_done, producer_done_rx) = oneshot::channel();
            let consumer = tokio::spawn(run_consumer_loop(
                manager,
                Box::new(AlwaysFails),
                token.clone(),
                Arc::new(MetricsCollector::new()),
                "mysql".to_string(),
                lsn_tracker,
                SharedLsnFeedback::new_shared(),
                ConsumerSettings {
                    batch_size: 1000,
                    group_commit_max_txns: 1,
                },
                commit_rx,
                producer_done_rx,
            ));
            sender.send(template.clone()).await.unwrap();
            // The first attempt fails at once; the retry is 2 s away.
            tokio::time::sleep(Duration::from_millis(300)).await;
            Self {
                _dir: dir,
                template,
                sender,
                token,
                producer_done,
                consumer,
                next_lsn: 2000,
            }
        }

        fn next(&mut self) -> PendingTransactionFile {
            let mut tx = self.template.clone();
            tx.metadata.commit_lsn = Some(Lsn(self.next_lsn));
            self.next_lsn += 1;
            tx
        }

        /// Sends until the channel stays full (the producer would block); returns how many went.
        async fn send_until_blocked(&mut self, give_up_after: usize) -> usize {
            let mut sent = 0;
            loop {
                let tx = self.next();
                match tokio::time::timeout(Duration::from_millis(100), self.sender.send(tx)).await {
                    Ok(result) => result.unwrap(),
                    Err(_) => return sent,
                }
                sent += 1;
                assert!(sent <= give_up_after, "the channel never filled up");
            }
        }
    }

    /// A destination that stays down must not let the consumer's queue grow with the source: past
    /// the cap the channel fills and the producer is held back (the WAL stays in the slot).
    #[tokio::test]
    async fn a_failing_destination_cannot_grow_the_queue_without_bound() {
        let mut consumer = BackedOff::start().await;
        let sent = consumer
            .send_until_blocked(MAX_QUEUED_TXNS_IN_BACKOFF + 64)
            .await;
        assert!(
            sent >= MAX_QUEUED_TXNS_IN_BACKOFF - 1,
            "the queue should take {MAX_QUEUED_TXNS_IN_BACKOFF} transactions first, took {sent}"
        );
        consumer.consumer.abort();
    }

    /// A producer blocked on the full channel must be released by a shutdown, without waiting out
    /// the retry delay, or it would never reach the point where it reports that it stopped.
    #[tokio::test]
    async fn shutdown_releases_a_producer_blocked_on_a_backed_off_consumer() {
        let mut consumer = BackedOff::start().await;
        consumer
            .send_until_blocked(MAX_QUEUED_TXNS_IN_BACKOFF + 64)
            .await;

        let blocked = consumer.next();
        let sender = consumer.sender.clone();
        let send = tokio::spawn(async move { sender.send(blocked).await });
        consumer.token.cancel();
        let released = tokio::time::timeout(Duration::from_secs(1), send)
            .await
            .expect("the producer stayed blocked until the retry delay ended");
        released.unwrap().unwrap();

        drop(consumer.producer_done);
        consumer.consumer.abort();
    }
}
