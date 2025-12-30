/// Tests for resumable position tracking in the CDC consumer
///
/// These tests verify that the consumer can:
/// 1. Save position after each batch execution
/// 2. Resume from saved position on restart
/// 3. Handle various edge cases (empty files, single command, etc.)

#[cfg(test)]
mod position_tracking_tests {
    use chrono::Utc;
    use pg2any_lib::lsn_tracker::{CdcMetadata, ConsumerState, LsnTracker, LsnTracking};
    use tokio::fs;

    fn get_test_file_path(test_name: &str) -> String {
        format!("/tmp/pg2any_test_{}_{}", test_name, std::process::id())
    }

    async fn cleanup_test_file(path: &str) {
        let metadata_path = format!("{}.metadata", path);
        let _ = fs::remove_file(path).await;
        let _ = fs::remove_file(metadata_path).await;
    }

    #[tokio::test]
    async fn test_update_consumer_position() {
        let lsn_file = get_test_file_path("update_position");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // Update position
        tracker.update_consumer_position(
            "/path/to/file.meta".to_string(),
            499, // Last executed command index (0-based)
        );

        // Verify position is set
        let metadata = tracker.get_metadata();
        assert_eq!(
            metadata.consumer_state.current_file_path,
            Some("/path/to/file.meta".to_string())
        );
        assert_eq!(
            metadata.consumer_state.last_executed_command_index,
            Some(499)
        );

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_get_resume_position() {
        let lsn_file = get_test_file_path("resume_position");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // Update position to command 499 (0-indexed)
        tracker.update_consumer_position("/path/to/file.meta".to_string(), 499);

        // Get resume position - should return index 500 (next command to execute)
        let resume_position = tracker.get_resume_position();
        assert!(resume_position.is_some());

        let (file_path, start_index) = resume_position.unwrap();
        assert_eq!(file_path, "/path/to/file.meta");
        assert_eq!(start_index, 500); // Resume from command 500

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_clear_consumer_position() {
        let lsn_file = get_test_file_path("clear_position");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // Set position
        tracker.update_consumer_position("/path/to/file.meta".to_string(), 499);

        // Clear position
        tracker.clear_consumer_position();

        // Verify position is cleared
        let resume_position = tracker.get_resume_position();
        assert!(resume_position.is_none());

        let metadata = tracker.get_metadata();
        assert_eq!(metadata.consumer_state.current_file_path, None);
        assert_eq!(metadata.consumer_state.last_executed_command_index, None);

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_position_persistence_across_restarts() {
        let lsn_file = get_test_file_path("persistence");

        // First session: save position
        {
            let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);
            tracker.update_consumer_position("/path/to/transaction_123.meta".to_string(), 2499);
            tracker.commit_lsn(123456789);

            tracker.shutdown_async().await;
        }

        // Second session: load position
        {
            let (tracker, lsn) =
                LsnTracker::new_with_load_and_interval(Some(&lsn_file), 1000).await;

            // Verify LSN was restored
            assert!(lsn.is_some());
            assert_eq!(lsn.unwrap().0, 123456789);

            // Verify position was restored
            let resume_position = tracker.get_resume_position();
            assert!(resume_position.is_some());

            let (file_path, start_index) = resume_position.unwrap();
            assert_eq!(file_path, "/path/to/transaction_123.meta");
            assert_eq!(start_index, 2500); // Should resume from 2500 (next after 2499)

            tracker.shutdown_async().await;
        }

        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_position_at_file_boundary() {
        let lsn_file = get_test_file_path("boundary");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // Simulate processing all commands (last_executed = total - 1)
        tracker.update_consumer_position(
            "/path/to/file.meta".to_string(),
            999, // Last command in a 1000-command file (0-indexed)
        );

        // Get resume position
        let resume_position = tracker.get_resume_position();
        assert!(resume_position.is_some());

        let (_, start_index) = resume_position.unwrap();
        assert_eq!(start_index, 1000); // Would resume from index 1000 (past end of file)

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_metadata_backward_compatibility() {
        let lsn_file = get_test_file_path("compat");
        let metadata_file = format!("{}.metadata", lsn_file);

        // Write old format metadata (without position fields)
        let old_metadata = CdcMetadata {
            version: "1.0".to_string(),
            last_updated: Utc::now(),
            lsn_tracking: LsnTracking { flush_lsn: 100 },
            consumer_state: ConsumerState {
                last_processed_tx_id: Some(123),
                last_processed_timestamp: Some(Utc::now()),
                pending_file_count: 0,
                current_file_path: None,
                last_executed_command_index: None,
            },
        };

        let json = serde_json::to_string_pretty(&old_metadata).unwrap();
        fs::write(&metadata_file, json).await.unwrap();

        // Load with new tracker
        let (tracker, lsn) = LsnTracker::new_with_load_and_interval(Some(&lsn_file), 1000).await;

        // Verify LSN was loaded
        assert!(lsn.is_some());
        assert_eq!(lsn.unwrap().0, 100);

        // Verify position fields default to None (safe fallback)
        let resume_position = tracker.get_resume_position();
        assert!(resume_position.is_none());

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_position_update_sequence() {
        let lsn_file = get_test_file_path("sequence");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // Simulate processing batches of 100 commands
        let file_path = "/path/to/file.meta".to_string();

        // Batch 1: commands 0-99 (last_executed_index = 99)
        tracker.update_consumer_position(file_path.clone(), 99);
        assert_eq!(tracker.get_resume_position().unwrap().1, 100);

        // Batch 2: commands 100-199 (last_executed_index = 199)
        tracker.update_consumer_position(file_path.clone(), 199);
        assert_eq!(tracker.get_resume_position().unwrap().1, 200);

        // Batch 3: commands 200-299 (last_executed_index = 299)
        tracker.update_consumer_position(file_path.clone(), 299);
        assert_eq!(tracker.get_resume_position().unwrap().1, 300);

        // ... process continues

        // Final batch: commands 900-999 (last_executed_index = 999)
        tracker.update_consumer_position(file_path.clone(), 999);
        assert_eq!(tracker.get_resume_position().unwrap().1, 1000);

        // Clear position after file completion
        tracker.clear_consumer_position();
        assert!(tracker.get_resume_position().is_none());

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_multiple_files_with_position() {
        let lsn_file = get_test_file_path("multiple_files");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // Process file 1 completely
        tracker.update_consumer_position("/path/to/file1.meta".to_string(), 999);
        tracker.clear_consumer_position();

        // Start processing file 2, interrupt mid-way
        tracker.update_consumer_position("/path/to/file2.meta".to_string(), 499);

        // Verify current position is for file 2
        let resume_position = tracker.get_resume_position();
        assert!(resume_position.is_some());

        let (file_path, start_index) = resume_position.unwrap();
        assert_eq!(file_path, "/path/to/file2.meta");
        assert_eq!(start_index, 500);

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_position_with_single_command_file() {
        let lsn_file = get_test_file_path("single_command");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 100);

        // File with single command
        tracker.update_consumer_position("/path/to/single.meta".to_string(), 0);

        // Resume position should be 1 (past the end)
        let resume_position = tracker.get_resume_position();
        assert!(resume_position.is_some());
        assert_eq!(resume_position.unwrap().1, 1);

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_position_dirty_flag() {
        let lsn_file = get_test_file_path("dirty_flag");

        let tracker = LsnTracker::new_with_interval(Some(&lsn_file), 10000); // Long interval

        // Initially not dirty
        assert!(!tracker.is_dirty());

        // Update position - should mark as dirty
        tracker.update_consumer_position("/path/to/file.meta".to_string(), 99);
        assert!(tracker.is_dirty());

        // Force persist and shutdown (which guarantees persistence)
        tracker.shutdown_async().await;

        // After shutdown, the final persistence has completed
        // We can't check is_dirty() here as the tracker is shut down

        cleanup_test_file(&lsn_file).await;
    }
}
