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
        TransactionManager::new(temp_dir.path(), DestinationType::MySQL, 64 * 1024 * 1024)
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

    let pending_path = manager
        .commit_transaction(tx_id, Some(Lsn(commit_lsn)))
        .await
        .unwrap();

    let content = tokio::fs::read_to_string(&pending_path).await.unwrap();
    let metadata = serde_json::from_str(&content).unwrap();

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
        TransactionManager::new(temp_dir.path(), DestinationType::MySQL, 64 * 1024 * 1024)
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
