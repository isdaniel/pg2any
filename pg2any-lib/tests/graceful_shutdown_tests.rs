use pg2any_lib::lsn_tracker::LsnTracker;
use pg2any_lib::transaction_manager::TransactionManager;
use pg2any_lib::types::DestinationType;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_lsn_persists_and_recovers() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn.metadata");
    let lsn_path = lsn_file.to_str().unwrap();

    // Create tracker, commit an LSN, persist
    let tracker = LsnTracker::new(Some(lsn_path)).await;
    tracker.commit_lsn(12345);
    tracker.persist_async().await.unwrap();

    assert_eq!(tracker.get(), 12345);

    // Simulate restart: create new tracker from same file
    let (tracker2, loaded_lsn) = LsnTracker::new_with_load(Some(lsn_path)).await;
    assert_eq!(tracker2.get(), 12345);
    assert!(loaded_lsn.is_some());
    assert_eq!(loaded_lsn.unwrap().0, 12345);
}

#[tokio::test]
async fn test_lsn_only_advances_forward() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn.metadata");
    let lsn_path = lsn_file.to_str().unwrap();

    let tracker = LsnTracker::new(Some(lsn_path)).await;

    tracker.commit_lsn(100);
    assert_eq!(tracker.get(), 100);

    // Trying to go backwards should be ignored
    tracker.commit_lsn(50);
    assert_eq!(tracker.get(), 100);

    // Going forward works
    tracker.commit_lsn(200);
    assert_eq!(tracker.get(), 200);
}

#[tokio::test]
async fn test_lsn_zero_is_ignored() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn.metadata");
    let lsn_path = lsn_file.to_str().unwrap();

    let tracker = LsnTracker::new(Some(lsn_path)).await;
    tracker.commit_lsn(0);
    assert_eq!(tracker.get(), 0);
}

#[tokio::test]
async fn test_transaction_file_lifecycle() {
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

    // Directories should have been created
    assert!(temp_dir.path().join("sql_received_tx").exists());
    assert!(temp_dir.path().join("sql_pending_tx").exists());
    assert!(temp_dir.path().join("sql_data_tx").exists());

    // Verify no pending transactions at start
    let pending = manager.list_pending_transactions().await.unwrap();
    assert!(pending.is_empty());
}

#[tokio::test]
async fn test_multiple_lsn_persists_latest_wins() {
    let temp_dir = TempDir::new().unwrap();
    let lsn_file = temp_dir.path().join("test_lsn.metadata");
    let lsn_path = lsn_file.to_str().unwrap();

    let tracker = LsnTracker::new(Some(lsn_path)).await;
    tracker.commit_lsn(100);
    tracker.persist_async().await.unwrap();
    tracker.commit_lsn(500);
    tracker.persist_async().await.unwrap();
    tracker.commit_lsn(1000);
    tracker.persist_async().await.unwrap();

    // Reload and verify latest value
    let (tracker2, loaded_lsn) = LsnTracker::new_with_load(Some(lsn_path)).await;
    assert_eq!(tracker2.get(), 1000);
    assert_eq!(loaded_lsn.unwrap().0, 1000);
}
