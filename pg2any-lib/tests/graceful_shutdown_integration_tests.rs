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

/// INSERT event carrying a distinguishing `id` so the consumer's rendered SQL
/// can be matched back to the exact source record. Used by the multi-segment
/// crash-resume test to prove WHICH records were applied.
fn create_insert_event_with_id(lsn: u64, id: usize) -> ChangeEvent {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text(&id.to_string())),
        ("name", ColumnValue::text(&format!("row-{id}"))),
    ]);
    ChangeEvent::insert("public", "test", 12345, data, Lsn::from(lsn))
}

/// Destination mock that CAPTURES every rendered SQL command it receives, so a
/// test can assert exactly which records were applied (no duplicates, no skips).
struct CapturingDestination {
    commands: Arc<std::sync::Mutex<Vec<String>>>,
}

impl CapturingDestination {
    fn new() -> Self {
        Self {
            commands: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl DestinationHandler for CapturingDestination {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        {
            let mut captured = self.commands.lock().unwrap();
            captured.extend_from_slice(commands);
        }
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
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

/// Covers the most safety-critical resume path: a crash-resume checkpoint that
/// lands INSIDE a later `.mpk` segment, exercising the per-segment record-unit
/// skip arithmetic (`remaining_start_index -= segment.statement_count`, then the
/// in-segment remainder skip) in `process_transaction_file`.
///
/// A tiny `segment_size_bytes` forces segment rotation so 10 INSERT events span >= 2
/// segments. The checkpoint is set so resume must apply ONLY the un-applied
/// suffix. We capture the rendered SQL and assert EXACTLY which records ran,
/// proving no duplicate, no skip, and no off-by-one at the segment boundary.
#[tokio::test]
async fn test_multi_segment_crash_resume_applies_exact_suffix() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn_multiseg");
    let lsn_path = lsn_file.to_str().unwrap();

    const COMMIT_LSN: u64 = 120_000;
    const TX_ID: u32 = 700;
    const RECORD_COUNT: usize = 10;

    // Tiny segment size forces rotation roughly every record, yielding many
    // segments. Each encoded INSERT record is well over 32 bytes.
    let manager = Arc::new(
        TransactionManager::new(temp_dir.path(), DestinationType::MySQL, None, 32)
            .await
            .unwrap(),
    );

    let timestamp = Utc::now();
    manager
        .begin_transaction(TX_ID, timestamp, "normal")
        .await
        .unwrap();

    for id in 0..RECORD_COUNT {
        let event = create_insert_event_with_id(COMMIT_LSN, id);
        manager.append_event(TX_ID, &event).await.unwrap();
    }
    manager.flush_all_buffers().await.unwrap();

    let (pending_path, metadata) = manager
        .commit_transaction(TX_ID, Some(Lsn(COMMIT_LSN)))
        .await
        .unwrap();

    // (1) Confirm rotation actually produced multiple segments AND that the
    // total record count is split across them (so the cross-segment subtraction
    // genuinely runs during resume).
    assert!(
        metadata.segments.len() >= 2,
        "expected >= 2 segments to exercise cross-segment skip, got {}",
        metadata.segments.len()
    );
    let total_records: usize = metadata.segments.iter().map(|s| s.statement_count).sum();
    assert_eq!(
        total_records, RECORD_COUNT,
        "segment statement_counts must sum to the number of appended records"
    );
    // The checkpoint must land in a LATER segment (not segment 0) for this test
    // to cover the subtraction. With last_executed_command_index = 6, records
    // 0..=6 are already applied; only 7, 8, 9 must be applied on resume.
    const LAST_EXECUTED: usize = 6;
    assert!(
        metadata.segments[0].statement_count <= LAST_EXECUTED,
        "checkpoint {LAST_EXECUTED} must fall beyond segment 0 (len {}) so the \
         per-segment subtraction runs",
        metadata.segments[0].statement_count
    );

    // (2) Seed the resume checkpoint into the pending metadata on disk.
    let pending_tx = PendingTransactionFile {
        file_path: pending_path,
        metadata,
    };
    let metadata_content = tokio::fs::read_to_string(&pending_tx.file_path)
        .await
        .unwrap();
    let mut disk_metadata: pg2any_lib::transaction_manager::TransactionFileMetadata =
        serde_json::from_str(&metadata_content).unwrap();
    disk_metadata.last_executed_command_index = Some(LAST_EXECUTED);
    tokio::fs::write(
        &pending_tx.file_path,
        serde_json::to_string(&disk_metadata).unwrap(),
    )
    .await
    .unwrap();

    // (3) Capture the rendered SQL the resume actually executes.
    let mock = CapturingDestination::new();
    let captured = mock.commands.clone();
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

    // (4) Assert EXACTLY records 7, 8, 9 were applied, in order. We match on the
    // unique `name` value (`row-<id>`) rendered into each INSERT so records
    // 0..=6 are provably skipped and 7..=9 provably applied with no duplicates.
    // Scan every record marker across all captured SQL (the consumer may coalesce
    // the homogeneous suffix into a single multi-value INSERT), preserving order.
    let commands = captured.lock().unwrap();
    let joined = commands.join("\n");
    let mut found: Vec<(usize, usize)> = Vec::new();
    for id in 0..RECORD_COUNT {
        if let Some(pos) = joined.find(&format!("row-{id}")) {
            found.push((pos, id));
        }
    }
    found.sort_unstable();
    let applied_ids: Vec<usize> = found.into_iter().map(|(_, id)| id).collect();

    assert_eq!(
        applied_ids,
        vec![7, 8, 9],
        "resume must apply exactly the un-applied suffix [7,8,9] in order; \
         captured SQL: {commands:?}"
    );

    assert_eq!(lsn_tracker.get(), COMMIT_LSN);
}
