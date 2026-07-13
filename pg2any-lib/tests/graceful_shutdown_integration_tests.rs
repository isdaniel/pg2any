use async_trait::async_trait;
use chrono::Utc;
use pg2any_lib::destinations::{DestinationHandler, PreCommitHook};
use pg2any_lib::error::{CdcError, Result};
use pg2any_lib::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use pg2any_lib::transaction_manager::{PendingTransactionFile, TransactionManager};
use pg2any_lib::types::{DestinationType, Lsn};
use pg2any_lib::MetricsCollectorTrait;
use pg_walstream::{ChangeEvent, ColumnValue, RowData};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

struct MockDestination {
    executed_batches: Arc<AtomicUsize>,
    fail_on_batch: Option<usize>,
    batch_counter: usize,
}

impl MockDestination {
    fn new() -> Self {
        Self {
            executed_batches: Arc::new(AtomicUsize::new(0)),
            fail_on_batch: None,
            batch_counter: 0,
        }
    }

    fn new_failing_at(batch_num: usize) -> Self {
        Self {
            executed_batches: Arc::new(AtomicUsize::new(0)),
            fail_on_batch: Some(batch_num),
            batch_counter: 0,
        }
    }
}

#[async_trait]
impl DestinationHandler for MockDestination {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        self.batch_counter += 1;

        if let Some(fail_at) = self.fail_on_batch {
            if self.batch_counter == fail_at {
                return Err(CdcError::generic("Simulated batch failure"));
            }
        }

        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }

        self.executed_batches.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn create_insert_event(lsn: u64) -> ChangeEvent {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("test")),
    ]);
    ChangeEvent::insert("public", "test", 12345, data, Lsn::from(lsn))
}

async fn setup_pending_transaction_with_events(
    temp_dir: &TempDir,
    tx_id: u32,
    commit_lsn: u64,
    event_count: usize,
) -> (Arc<TransactionManager>, PendingTransactionFile) {
    let manager = Arc::new(
        TransactionManager::new(
            temp_dir.path(),
            DestinationType::MySQL,
            None,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    let timestamp = Utc::now();
    manager
        .begin_transaction(tx_id, timestamp, "normal")
        .await
        .unwrap();

    for _ in 0..event_count {
        let event = create_insert_event(commit_lsn);
        manager.append_event(tx_id, &event).await.unwrap();
    }

    manager.flush_all_buffers().await.unwrap();

    let (pending_path, metadata) = manager
        .commit_transaction(tx_id, Some(Lsn(commit_lsn)))
        .await
        .unwrap();

    let pending_tx = PendingTransactionFile {
        file_path: pending_path,
        metadata,
    };

    (manager, pending_tx)
}

#[tokio::test]
async fn test_shutdown_persists_lsn_and_skips_on_restart() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn");
    let lsn_path = lsn_file.to_str().unwrap();

    let (manager, pending_tx) =
        setup_pending_transaction_with_events(&temp_dir, 100, 50000, 3).await;

    let mut dest: Box<dyn DestinationHandler> = Box::new(MockDestination::new());
    let token = CancellationToken::new();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest,
            &token,
            &lsn_tracker,
            &metrics,
            100,
            &shared_feedback,
        )
        .await
        .unwrap();

    assert_eq!(lsn_tracker.get(), 50000);

    // Simulate restart: load LSN from file
    let (tracker2, loaded_lsn) = LsnTracker::new_with_load(Some(lsn_path)).await;
    assert_eq!(loaded_lsn, Some(Lsn(50000)));

    // Create another transaction with commit_lsn <= flush_lsn — should be skipped
    let temp_dir2 = TempDir::new().unwrap();
    let (manager2, pending_tx2) =
        setup_pending_transaction_with_events(&temp_dir2, 101, 50000, 2).await;

    let mock = MockDestination::new();
    let executed = mock.executed_batches.clone();
    let mut dest2: Box<dyn DestinationHandler> = Box::new(mock);

    manager2
        .clone()
        .process_transaction_file(
            &pending_tx2,
            &mut dest2,
            &token,
            &tracker2,
            &metrics,
            100,
            &shared_feedback,
        )
        .await
        .unwrap();

    // Transaction was skipped — no batches executed
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    // LSN unchanged
    assert_eq!(tracker2.get(), 50000);
}

#[tokio::test]
async fn test_lsn_not_advanced_on_failed_batch() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_fail");
    let lsn_path = lsn_file.to_str().unwrap();

    let (manager, pending_tx) =
        setup_pending_transaction_with_events(&temp_dir, 200, 60000, 2).await;

    let mut dest: Box<dyn DestinationHandler> = Box::new(MockDestination::new_failing_at(1));
    let token = CancellationToken::new();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    let result = manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest,
            &token,
            &lsn_tracker,
            &metrics,
            100,
            &shared_feedback,
        )
        .await;

    assert!(result.is_err());
    assert_eq!(lsn_tracker.get(), 0);
}

#[tokio::test]
async fn test_incomplete_transaction_not_in_pending() {
    let temp_dir = TempDir::new().unwrap();

    let manager = Arc::new(
        TransactionManager::new(
            temp_dir.path(),
            DestinationType::MySQL,
            None,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    let timestamp = Utc::now();
    manager
        .begin_transaction(300, timestamp, "normal")
        .await
        .unwrap();

    // Not committed — should NOT appear in pending
    let pending = manager.list_pending_transactions().await.unwrap();
    assert!(pending.is_empty());

    // Should be in received (recoverable on restart)
    let received = manager.restore_received_transactions().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].transaction_id, 300);
}

#[tokio::test]
async fn test_partial_batch_resumes_from_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_partial");
    let lsn_path = lsn_file.to_str().unwrap();

    let (manager, pending_tx) =
        setup_pending_transaction_with_events(&temp_dir, 400, 70000, 5).await;

    // Simulate partial execution: mark first 3 commands as already executed
    let metadata_content = tokio::fs::read_to_string(&pending_tx.file_path)
        .await
        .unwrap();
    let mut metadata: pg2any_lib::transaction_manager::TransactionFileMetadata =
        serde_json::from_str(&metadata_content).unwrap();
    metadata.last_executed_command_index = Some(2);
    let updated_json = serde_json::to_string(&metadata).unwrap();
    tokio::fs::write(&pending_tx.file_path, updated_json)
        .await
        .unwrap();

    let mock = MockDestination::new();
    let executed = mock.executed_batches.clone();
    let mut dest: Box<dyn DestinationHandler> = Box::new(mock);
    let token = CancellationToken::new();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest,
            &token,
            &lsn_tracker,
            &metrics,
            100,
            &shared_feedback,
        )
        .await
        .unwrap();

    // Should have executed at least 1 batch with the remaining commands
    assert!(executed.load(Ordering::SeqCst) >= 1);
    assert_eq!(lsn_tracker.get(), 70000);
}

#[tokio::test]
async fn test_cancellation_prevents_lsn_advance() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_cancel");
    let lsn_path = lsn_file.to_str().unwrap();

    let (manager, pending_tx) =
        setup_pending_transaction_with_events(&temp_dir, 500, 80000, 2).await;

    let mut dest: Box<dyn DestinationHandler> = Box::new(MockDestination::new());
    let token = CancellationToken::new();
    token.cancel();

    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    let result = manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest,
            &token,
            &lsn_tracker,
            &metrics,
            100,
            &shared_feedback,
        )
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().is_cancelled());
    assert_eq!(lsn_tracker.get(), 0);
}

#[tokio::test]
async fn test_mid_batch_cancellation_completes_drain() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_drain");
    let lsn_path = lsn_file.to_str().unwrap();

    let manager = Arc::new(
        TransactionManager::new(
            temp_dir.path(),
            DestinationType::MySQL,
            None,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    let mut pending_txs = Vec::new();
    for (i, lsn) in [(1u32, 10000u64), (2, 20000), (3, 30000)].iter() {
        let timestamp = Utc::now();
        manager
            .begin_transaction(*i, timestamp, "normal")
            .await
            .unwrap();
        let event = create_insert_event(*lsn);
        manager.append_event(*i, &event).await.unwrap();
        manager.flush_all_buffers().await.unwrap();
        let (pending_path, metadata) = manager
            .commit_transaction(*i, Some(Lsn(*lsn)))
            .await
            .unwrap();
        pending_txs.push(PendingTransactionFile {
            file_path: pending_path,
            metadata,
        });
    }

    let mut commit_queue: std::collections::BinaryHeap<std::cmp::Reverse<PendingTransactionFile>> =
        std::collections::BinaryHeap::new();
    for tx in pending_txs {
        commit_queue.push(std::cmp::Reverse(tx));
    }

    let (_tx_sender, mut rx_receiver) = tokio::sync::mpsc::channel::<PendingTransactionFile>(10);
    drop(_tx_sender);

    let mock = MockDestination::new();
    let executed = mock.executed_batches.clone();
    let mut dest: Box<dyn DestinationHandler> = Box::new(mock);
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    pg2any_lib::drain_and_shutdown(
        &mut commit_queue,
        &mut rx_receiver,
        &manager,
        &mut dest,
        &lsn_tracker,
        &metrics,
        100,
        &shared_feedback,
    )
    .await;

    assert_eq!(executed.load(Ordering::SeqCst), 3);
    assert_eq!(lsn_tracker.get(), 30000);
    let (_, loaded_lsn) = LsnTracker::new_with_load(Some(lsn_path)).await;
    assert_eq!(loaded_lsn, Some(Lsn(30000)));
}

#[tokio::test]
async fn test_committed_but_undelivered_recovered_on_restart() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_undelivered");
    let lsn_path = lsn_file.to_str().unwrap();

    let manager = Arc::new(
        TransactionManager::new(
            temp_dir.path(),
            DestinationType::MySQL,
            None,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    let timestamp = Utc::now();
    manager
        .begin_transaction(42, timestamp, "normal")
        .await
        .unwrap();
    let event = create_insert_event(55000);
    manager.append_event(42, &event).await.unwrap();
    manager.flush_all_buffers().await.unwrap();
    let _pending = manager
        .commit_transaction(42, Some(Lsn(55000)))
        .await
        .unwrap();

    let pending_txs = manager.list_pending_transactions().await.unwrap();
    assert_eq!(pending_txs.len(), 1);
    assert_eq!(pending_txs[0].metadata.transaction_id, 42);

    let mock = MockDestination::new();
    let executed = mock.executed_batches.clone();
    let mut dest: Box<dyn DestinationHandler> = Box::new(mock);
    let token = CancellationToken::new();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    for pending_tx in &pending_txs {
        manager
            .clone()
            .process_transaction_file(
                pending_tx,
                &mut dest,
                &token,
                &lsn_tracker,
                &metrics,
                100,
                &shared_feedback,
            )
            .await
            .unwrap();
    }

    assert!(executed.load(Ordering::SeqCst) >= 1);
    assert_eq!(lsn_tracker.get(), 55000);
    let remaining = manager.list_pending_transactions().await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn test_final_ack_failure_still_persists_local_lsn() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_ack_fail");
    let lsn_path = lsn_file.to_str().unwrap();

    let (manager, pending_tx) =
        setup_pending_transaction_with_events(&temp_dir, 600, 90000, 3).await;

    let mut dest: Box<dyn DestinationHandler> = Box::new(MockDestination::new());
    let token = CancellationToken::new();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest,
            &token,
            &lsn_tracker,
            &metrics,
            100,
            &shared_feedback,
        )
        .await
        .unwrap();

    assert_eq!(lsn_tracker.get(), 90000);

    let (tracker_after_restart, loaded_lsn) = LsnTracker::new_with_load(Some(lsn_path)).await;
    assert_eq!(loaded_lsn, Some(Lsn(90000)));

    let temp_dir2 = TempDir::new().unwrap();
    let (manager2, pending_tx_replay) =
        setup_pending_transaction_with_events(&temp_dir2, 601, 90000, 2).await;

    let mock2 = MockDestination::new();
    let executed2 = mock2.executed_batches.clone();
    let mut dest2: Box<dyn DestinationHandler> = Box::new(mock2);

    manager2
        .clone()
        .process_transaction_file(
            &pending_tx_replay,
            &mut dest2,
            &token,
            &tracker_after_restart,
            &metrics,
            100,
            &shared_feedback,
        )
        .await
        .unwrap();

    assert_eq!(executed2.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_drain_on_error_preserves_file_for_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_drain_err");
    let lsn_path = lsn_file.to_str().unwrap();

    let manager = Arc::new(
        TransactionManager::new(
            temp_dir.path(),
            DestinationType::MySQL,
            None,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    let mut pending_txs = Vec::new();
    for (i, lsn) in [(10u32, 40000u64), (11, 50000)].iter() {
        let timestamp = Utc::now();
        manager
            .begin_transaction(*i, timestamp, "normal")
            .await
            .unwrap();
        let event = create_insert_event(*lsn);
        manager.append_event(*i, &event).await.unwrap();
        manager.flush_all_buffers().await.unwrap();
        let (pending_path, metadata) = manager
            .commit_transaction(*i, Some(Lsn(*lsn)))
            .await
            .unwrap();
        pending_txs.push(PendingTransactionFile {
            file_path: pending_path,
            metadata,
        });
    }

    let mut commit_queue: std::collections::BinaryHeap<std::cmp::Reverse<PendingTransactionFile>> =
        std::collections::BinaryHeap::new();
    for tx in pending_txs {
        commit_queue.push(std::cmp::Reverse(tx));
    }

    let (_tx_sender, mut rx_receiver) = tokio::sync::mpsc::channel::<PendingTransactionFile>(10);
    drop(_tx_sender);

    let mut dest: Box<dyn DestinationHandler> = Box::new(MockDestination::new_failing_at(2));
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();

    pg2any_lib::drain_and_shutdown(
        &mut commit_queue,
        &mut rx_receiver,
        &manager,
        &mut dest,
        &lsn_tracker,
        &metrics,
        100,
        &shared_feedback,
    )
    .await;

    assert_eq!(lsn_tracker.get(), 40000);

    let remaining = manager.list_pending_transactions().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].metadata.transaction_id, 11);

    let (tracker_restart, loaded_lsn) = LsnTracker::new_with_load(Some(lsn_path)).await;
    assert_eq!(loaded_lsn, Some(Lsn(40000)));
    assert!(50000 > tracker_restart.get());
}

/// Handler that records the exact SQL commands it executes and cancels a shared
/// token after `cancel_after_batches` batches — simulating SIGTERM arriving
/// mid-transaction. Runs the pre-commit hook (like a real destination) so the
/// resume checkpoint (`last_executed_command_index`) is staged and flushed.
struct CancellingRecorder {
    executed: Arc<std::sync::Mutex<Vec<String>>>,
    token: CancellationToken,
    cancel_after_batches: usize,
    batch_counter: usize,
}

#[async_trait]
impl DestinationHandler for CancellingRecorder {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        // Record what actually got applied, in order.
        {
            let mut log = self.executed.lock().unwrap();
            log.extend(commands.iter().cloned());
        }

        // Stage + (via the manager) flush the resume checkpoint, exactly as a
        // real destination does. Must happen for THIS batch before we cancel,
        // so the next-batch cancellation check resumes from the right index.
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }

        self.batch_counter += 1;
        if self.batch_counter == self.cancel_after_batches {
            self.token.cancel();
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn create_distinct_insert_event(id: usize, lsn: u64) -> ChangeEvent {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text(&id.to_string())),
        ("name", ColumnValue::text(&format!("row-{id}"))),
    ]);
    ChangeEvent::insert("public", "test", 12345, data, Lsn::from(lsn))
}

/// Finding 1 verification: a large transaction that is cancelled mid-processing
/// (SIGTERM) and then resumed (drain/restart) must apply every statement
/// EXACTLY ONCE — no replay (duplicate key) and no loss.
///
/// This exercises the full producer-count -> consumer-skip path end to end with
/// DISTINCT statements, so a mismatch between the producer's tracked
/// `segment_statement_count` and the consumer's `SqlStreamParser` read count
/// (the crash-resume skip arithmetic) surfaces as a duplicated or missing
/// statement. The pre-existing resume tests use identical events and only count
/// batches, so they cannot catch this class of position-tracking bug.
#[tokio::test]
async fn test_cancel_and_resume_applies_each_statement_exactly_once() {
    const N: usize = 12;
    const COMMIT_LSN: u64 = 123_000;

    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_exactly_once");
    let lsn_path = lsn_file.to_str().unwrap();

    // Small segment size forces the producer to rotate across MULTIPLE segments,
    // exercising both whole-segment skip and within-segment index skip on resume.
    let manager = Arc::new(
        TransactionManager::new(temp_dir.path(), DestinationType::MySQL, None, 160)
            .await
            .unwrap(),
    );

    manager
        .begin_transaction(700, Utc::now(), "normal")
        .await
        .unwrap();
    for id in 0..N {
        manager
            .append_event(700, &create_distinct_insert_event(id, COMMIT_LSN))
            .await
            .unwrap();
    }
    manager.flush_all_buffers().await.unwrap();
    let (pending_path, metadata) = manager
        .commit_transaction(700, Some(Lsn(COMMIT_LSN)))
        .await
        .unwrap();
    // Sanity: the transaction really did span more than one segment.
    assert!(
        metadata.segments.len() > 1,
        "test needs a multi-segment transaction, got {} segment(s)",
        metadata.segments.len()
    );
    let pending_tx = PendingTransactionFile {
        file_path: pending_path,
        metadata,
    };

    let executed: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let batch_size = 3;

    // --- Run 1: process, but cancel after 2 batches (SIGTERM mid-transaction) ---
    let token = CancellationToken::new();
    let mut dest: Box<dyn DestinationHandler> = Box::new(CancellingRecorder {
        executed: executed.clone(),
        token: token.clone(),
        cancel_after_batches: 2,
        batch_counter: 0,
    });

    let result = manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest,
            &token,
            &lsn_tracker,
            &metrics,
            batch_size,
            &shared_feedback,
        )
        .await;
    assert!(
        result.is_err() && result.unwrap_err().is_cancelled(),
        "run 1 must stop with a cancellation error"
    );
    // LSN must NOT advance for a partially-applied transaction.
    assert_eq!(lsn_tracker.get(), 0, "flush_lsn must not advance mid-tx");

    // --- Run 2: drain/restart with a fresh, uncancelled token ---
    let drain_token = CancellationToken::new();
    let mut dest2: Box<dyn DestinationHandler> = Box::new(CancellingRecorder {
        executed: executed.clone(),
        token: drain_token.clone(),
        cancel_after_batches: usize::MAX, // never cancel
        batch_counter: 0,
    });
    manager
        .clone()
        .process_transaction_file(
            &pending_tx,
            &mut dest2,
            &drain_token,
            &lsn_tracker,
            &metrics,
            batch_size,
            &shared_feedback,
        )
        .await
        .expect("run 2 (drain) must complete");

    // Transaction fully applied -> flush_lsn advances, file removed.
    assert_eq!(lsn_tracker.get(), COMMIT_LSN);
    assert!(manager
        .list_pending_transactions()
        .await
        .unwrap()
        .is_empty());

    // The core assertion: every distinct statement applied EXACTLY ONCE, in order.
    let applied = executed.lock().unwrap().clone();
    assert_eq!(
        applied.len(),
        N,
        "expected {N} statements applied exactly once, got {} (replay or loss): {applied:#?}",
        applied.len()
    );
    for (id, stmt) in applied.iter().enumerate() {
        assert!(
            stmt.contains(&format!("row-{id}")),
            "statement {id} out of order / replayed / lost: {stmt:?}"
        );
    }
}

/// Build a transaction whose events include a multi-table TRUNCATE (one event
/// that renders to MULTIPLE `;`-statements). Returns the committed pending file.
async fn build_mixed_tx(
    temp_dir: &TempDir,
    tx_id: u32,
    commit_lsn: u64,
) -> (Arc<TransactionManager>, PendingTransactionFile) {
    let manager = Arc::new(
        TransactionManager::new(temp_dir.path(), DestinationType::MySQL, None, 160)
            .await
            .unwrap(),
    );
    manager
        .begin_transaction(tx_id, Utc::now(), "normal")
        .await
        .unwrap();

    // 5 distinct inserts, then a 3-table TRUNCATE (renders to 3 statements in a
    // single appended event), then 6 more distinct inserts.
    for id in 0..5 {
        manager
            .append_event(tx_id, &create_distinct_insert_event(id, commit_lsn))
            .await
            .unwrap();
    }
    let tables: Vec<std::sync::Arc<str>> = vec![
        std::sync::Arc::from("public.ta"),
        std::sync::Arc::from("public.tb"),
        std::sync::Arc::from("public.tc"),
    ];
    manager
        .append_event(tx_id, &ChangeEvent::truncate(tables, Lsn::from(commit_lsn)))
        .await
        .unwrap();
    for id in 5..11 {
        manager
            .append_event(tx_id, &create_distinct_insert_event(id, commit_lsn))
            .await
            .unwrap();
    }

    manager.flush_all_buffers().await.unwrap();
    let (pending_path, metadata) = manager
        .commit_transaction(tx_id, Some(Lsn(commit_lsn)))
        .await
        .unwrap();
    (
        manager,
        PendingTransactionFile {
            file_path: pending_path,
            metadata,
        },
    )
}

async fn process_recording(
    manager: &Arc<TransactionManager>,
    pending_tx: &PendingTransactionFile,
    lsn_path: &str,
    cancel_after_batches: usize,
    batch_size: usize,
) -> (Vec<String>, u64) {
    let executed: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let shared_feedback = SharedLsnFeedback::new_shared();
    let lsn_tracker = LsnTracker::new(Some(lsn_path)).await;
    let token = CancellationToken::new();
    let mut dest: Box<dyn DestinationHandler> = Box::new(CancellingRecorder {
        executed: executed.clone(),
        token: token.clone(),
        cancel_after_batches,
        batch_counter: 0,
    });

    // Loop process -> resume, mirroring drain: a cancellation re-runs with a
    // fresh uncancelled token until the transaction is fully applied.
    let mut cancelled = true;
    let mut first = true;
    while cancelled {
        let tok = if first {
            token.clone()
        } else {
            CancellationToken::new()
        };
        first = false;
        match manager
            .clone()
            .process_transaction_file(
                pending_tx,
                &mut dest,
                &tok,
                &lsn_tracker,
                &metrics,
                batch_size,
                &shared_feedback,
            )
            .await
        {
            Ok(()) => cancelled = false,
            Err(e) if e.is_cancelled() => {
                // swap in a non-cancelling recorder for the drain pass
                dest = Box::new(CancellingRecorder {
                    executed: executed.clone(),
                    token: CancellationToken::new(),
                    cancel_after_batches: usize::MAX,
                    batch_counter: 0,
                });
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    let applied = executed.lock().unwrap().clone();
    (applied, lsn_tracker.get())
}

/// Finding 1 (multi-statement event): a transaction containing a multi-table
/// TRUNCATE — one event that renders to several `;`-statements — must, after a
/// mid-transaction cancel + resume, apply the EXACT same statements as a clean
/// uninterrupted pass. If the producer's `count_rendered_statements` (which must
/// count the TRUNCATE as 3, not 1) disagrees with the consumer's parser, the
/// resume-skip lands on the wrong statement and this oracle comparison fails.
#[tokio::test]
async fn test_cancel_resume_with_multi_statement_truncate_matches_clean_pass() {
    const COMMIT_LSN: u64 = 456_000;
    let batch_size = 3;

    // Oracle: one clean, uninterrupted pass.
    let baseline_dir = TempDir::new().unwrap();
    let (mgr_a, tx_a) = build_mixed_tx(&baseline_dir, 800, COMMIT_LSN).await;
    assert!(
        tx_a.metadata.segments.len() > 1,
        "need a multi-segment tx to exercise segment skip"
    );
    let baseline_lsn = baseline_dir.path().join("lsn_a");
    let (baseline, lsn_a) = process_recording(
        &mgr_a,
        &tx_a,
        baseline_lsn.to_str().unwrap(),
        usize::MAX, // never cancel
        batch_size,
    )
    .await;
    assert_eq!(lsn_a, COMMIT_LSN);
    // Producer-tracked statement count must equal what a clean pass executes.
    let producer_total: usize = tx_a
        .metadata
        .segments
        .iter()
        .map(|s| s.statement_count)
        .sum();
    assert_eq!(
        baseline.len(),
        producer_total,
        "producer segment_statement_count sum ({producer_total}) != statements a clean pass executes ({})",
        baseline.len()
    );
    // The TRUNCATE contributed 3 statements (not 1).
    assert_eq!(baseline.len(), 5 + 3 + 6);

    // Actual: cancel mid-transaction, then resume to completion.
    for cancel_after in [1usize, 2, 3] {
        let dir = TempDir::new().unwrap();
        let (mgr_b, tx_b) = build_mixed_tx(&dir, 801, COMMIT_LSN).await;
        let lsn_b = dir.path().join("lsn_b");
        let (actual, lsn) = process_recording(
            &mgr_b,
            &tx_b,
            lsn_b.to_str().unwrap(),
            cancel_after,
            batch_size,
        )
        .await;
        assert_eq!(
            lsn, COMMIT_LSN,
            "flush_lsn must reach commit_lsn after drain"
        );
        assert_eq!(
            actual, baseline,
            "cancel_after={cancel_after}: resume applied a different statement sequence than a clean pass (replay/loss/misorder)"
        );
    }
}
