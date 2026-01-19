/// Tests for consumer state persistence in the CDC metadata
#[cfg(test)]
mod consumer_state_tests {
    use chrono::Utc;
    use pg2any_lib::lsn_tracker::LsnTracker;
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
    async fn test_update_consumer_state() {
        let lsn_file = get_test_file_path("consumer_state_update");

        let tracker = LsnTracker::new(Some(&lsn_file)).await;

        tracker.update_consumer_state(123, Utc::now(), 7);

        let metadata = tracker.get_metadata();
        assert_eq!(metadata.consumer_state.last_processed_tx_id, Some(123));
        assert_eq!(metadata.consumer_state.pending_file_count, 7);

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_consumer_state_persistence_across_restarts() {
        let lsn_file = get_test_file_path("consumer_state_persistence");

        {
            let tracker = LsnTracker::new(Some(&lsn_file)).await;
            tracker.update_consumer_state(321, Utc::now(), 2);
            tracker.commit_lsn(123456789);
            tracker.shutdown_async().await;
        }

        {
            let (tracker, lsn) = LsnTracker::new_with_load(Some(&lsn_file)).await;

            assert!(lsn.is_some());
            assert_eq!(lsn.unwrap().0, 123456789);

            let metadata = tracker.get_metadata();
            assert_eq!(metadata.consumer_state.last_processed_tx_id, Some(321));
            assert_eq!(metadata.consumer_state.pending_file_count, 2);

            tracker.shutdown_async().await;
        }

        cleanup_test_file(&lsn_file).await;
    }

    #[tokio::test]
    async fn test_metadata_backward_compatibility_with_extra_fields() {
        let lsn_file = get_test_file_path("consumer_state_compat");
        let metadata_file = format!("{}.metadata", lsn_file);

        let legacy_json = serde_json::json!({
            "version": "1.0",
            "last_updated": Utc::now(),
            "lsn_tracking": { "flush_lsn": 100 },
            "consumer_state": {
                "last_processed_tx_id": 123,
                "last_processed_timestamp": Utc::now(),
                "pending_file_count": 0,
                "current_file_path": "/path/to/legacy.meta",
                "last_executed_command_index": 42
            }
        });

        fs::write(&metadata_file, legacy_json.to_string())
            .await
            .unwrap();

        let (tracker, lsn) = LsnTracker::new_with_load(Some(&lsn_file)).await;

        assert!(lsn.is_some());
        assert_eq!(lsn.unwrap().0, 100);

        let metadata = tracker.get_metadata();
        assert_eq!(metadata.consumer_state.last_processed_tx_id, Some(123));

        tracker.shutdown_async().await;
        cleanup_test_file(&lsn_file).await;
    }
}
