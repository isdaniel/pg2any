//! Unit tests for metrics logical functionality
//!
//! This module tests the metrics abstraction layer, including both real metrics
//! (when the `metrics` feature is enabled) and no-op implementations (when disabled).
//! Tests cover event recording, duration tracking, LSN handling, rate calculations,
//! connection status, error recording, and metrics gathering.

use pg2any_lib::monitoring::{
    gather_metrics, init_metrics, MetricsCollector, MetricsCollectorTrait, ProcessingTimer,
    ProcessingTimerTrait,
};
use pg2any_lib::types::{ChangeEvent, ReplicaIdentity};
use pg_walstream::ColumnValue;
use pg_walstream::Lsn;
use pg_walstream::RowData;
use std::sync::Arc;
use std::time::Duration;

/// Helper function to create test change events
fn create_test_insert_event() -> ChangeEvent {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("test")),
    ]);

    ChangeEvent::insert("public", "users", 12345, data, Lsn::from(100))
}

fn create_test_update_event() -> ChangeEvent {
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("old_name")),
    ]);

    let new_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("new_name")),
    ]);

    ChangeEvent::update(
        "public",
        "users",
        12345,
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(300),
    )
}

fn create_test_delete_event() -> ChangeEvent {
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("deleted_name")),
    ]);

    ChangeEvent::delete(
        "public",
        "users",
        12345,
        old_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(200),
    )
}

fn create_test_truncate_event() -> ChangeEvent {
    ChangeEvent::truncate(
        vec![Arc::from("users"), Arc::from("orders")],
        Lsn::from(400),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();

        // The collector should be created successfully regardless of feature flags
        // This tests the basic trait implementation
        assert!(std::format!("{:?}", collector).contains("MetricsCollector"));
    }

    #[test]
    fn test_event_recording() {
        let collector = MetricsCollector::new();

        // Test recording different types of events
        let insert_event = create_test_insert_event();
        collector.record_event(&insert_event);

        let update_event = create_test_update_event();
        collector.record_event(&update_event);

        let delete_event = create_test_delete_event();
        collector.record_event(&delete_event);

        let truncate_event = create_test_truncate_event();
        collector.record_event(&truncate_event);

        // All events should be recorded without panicking
        // This tests the event recording logic for different event types
    }

    #[test]
    fn test_processing_duration_recording() {
        let collector = MetricsCollector::new();

        // Test recording processing durations for different operations
        collector.record_processing_duration(Duration::from_millis(100), "insert", "mysql");

        collector.record_processing_duration(Duration::from_millis(250), "update", "postgresql");

        collector.record_processing_duration(Duration::from_millis(50), "delete", "sqlite");

        // Durations should be recorded without panicking
        // This tests the duration recording logic
    }

    #[test]
    fn test_processing_timer() {
        let collector = MetricsCollector::new();

        // Test timer creation and finishing
        let timer = ProcessingTimer::start("insert", "mysql");

        // Simulate some processing time
        std::thread::sleep(Duration::from_millis(1));

        timer.finish(&collector);

        // Timer should complete without panicking
        // This tests the timer functionality
    }

    #[test]
    fn test_processing_timer_multiple_operations() {
        let collector = MetricsCollector::new();

        // Test multiple timers for different operations
        let timer1 = ProcessingTimer::start("insert", "mysql");
        let timer2 = ProcessingTimer::start("update", "postgresql");

        std::thread::sleep(Duration::from_millis(1));

        timer1.finish(&collector);
        timer2.finish(&collector);

        // Multiple timers should work independently
    }

    #[test]
    fn test_error_recording() {
        let collector = MetricsCollector::new();

        // Test recording different types of errors
        collector.record_error("connection_timeout", "database");
        collector.record_error("parse_error", "replication");
        collector.record_error("authentication_failed", "source");
        collector.record_error("destination_unavailable", "destination");

        // Error recording should work without panicking
        // This tests the error recording logic
    }

    #[test]
    fn test_connection_status_updates() {
        let collector = MetricsCollector::new();

        // Test source connection status updates
        collector.update_source_connection_status(true);
        collector.update_source_connection_status(false);
        collector.update_source_connection_status(true);

        // Test destination connection status updates
        collector.update_destination_connection_status("mysql", true);
        collector.update_destination_connection_status("postgresql", false);
        collector.update_destination_connection_status("sqlite", true);

        // Connection status updates should work without panicking
        // This tests the connection status tracking logic
    }

    #[test]
    fn test_active_connections_tracking() {
        let collector = MetricsCollector::new();

        // Test tracking active connections
        collector.update_active_connections(5, "source");
        collector.update_active_connections(3, "destination");
        collector.update_active_connections(0, "source");
        collector.update_active_connections(10, "destination");

        // Active connections tracking should work without panicking
        // This tests the active connections counting logic
    }

    #[test]
    fn test_consumer_queue_length_updates() {
        let collector = MetricsCollector::new();

        // Test queue length updates
        collector.update_consumer_queue_length(0);
        collector.update_consumer_queue_length(100);
        collector.update_consumer_queue_length(50);
        collector.update_consumer_queue_length(1000);

        // Queue length updates should work without panicking
        // This tests the queue length tracking logic
    }

    #[test]
    fn test_uptime_updates() {
        let collector = MetricsCollector::new();

        // Test uptime updates
        collector.update_uptime();

        // Sleep a bit and update again
        std::thread::sleep(Duration::from_millis(1));
        collector.update_uptime();

        // Uptime updates should work without panicking
        // This tests the uptime tracking logic
    }

    #[test]
    fn test_events_rate_updates() {
        let collector = MetricsCollector::new();

        // Test events rate updates
        collector.update_events_rate();

        // Record some events
        let event = create_test_insert_event();
        collector.record_event(&event);
        collector.record_event(&event);

        // Update rate again
        collector.update_events_rate();

        // Rate updates should work without panicking
        // This tests the rate calculation logic
    }

    #[test]
    fn test_lsn_recording() {
        let collector = MetricsCollector::new();

        // Test LSN recording
        collector.record_received_lsn(12345);
        collector.record_received_lsn(23456);
        collector.record_received_lsn(34567);

        // LSN recording should work without panicking
        // This tests the LSN tracking logic
    }

    #[test]
    fn test_event_with_lsn() {
        let collector = MetricsCollector::new();

        // Create event with LSN
        let mut event = create_test_insert_event();
        event.lsn = Lsn::from(54321);

        // Record event with LSN
        collector.record_event(&event);

        // Event with LSN should be recorded properly
        // This tests the LSN extraction from events
    }

    #[test]
    fn test_metrics_gathering() {
        let collector = MetricsCollector::new();

        // Record some data
        let event = create_test_insert_event();
        collector.record_event(&event);
        collector.record_processing_duration(Duration::from_millis(100), "insert", "mysql");
        collector.record_error("test_error", "test_component");
        collector.update_source_connection_status(true);

        // Get metrics
        let metrics_result = collector.get_metrics();

        // Metrics should be gatherable (either real metrics or disabled message)
        assert!(metrics_result.is_ok());
        let metrics_text = metrics_result.unwrap();
        assert!(!metrics_text.is_empty());

        // This tests the metrics gathering functionality
    }

    #[test]
    fn test_build_info_initialization() {
        let collector = MetricsCollector::new();

        // Initialize build info
        collector.init_build_info("1.0.0-test");

        // Build info initialization should work without panicking
        // This tests the build info initialization logic
    }

    #[test]
    fn test_metrics_initialization() {
        // Test metrics initialization
        let result = init_metrics();

        // Initialization should succeed regardless of feature flags
        // Note: May fail if metrics are already initialized in other tests
        // This is expected behavior as metrics should only be initialized once
        match result {
            Ok(_) => {
                // Successfully initialized
            }
            Err(e) => {
                // If initialization fails, it should be because metrics are already registered
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("already")
                        || error_msg.contains("duplicate")
                        || error_msg.contains("register"),
                    "Unexpected error during metrics initialization: {}",
                    error_msg
                );
            }
        }

        // This tests the metrics initialization function
    }

    #[test]
    fn test_global_metrics_gathering() {
        // Initialize metrics first
        let _ = init_metrics();

        // Test global metrics gathering
        let result = gather_metrics();

        // Gathering should succeed regardless of feature flags
        assert!(result.is_ok());
        let metrics_text = result.unwrap();
        assert!(!metrics_text.is_empty());

        // This tests the global metrics gathering function
    }

    #[test]
    fn test_event_type_string_conversion() {
        // Test that event type strings are correct
        let insert_event = create_test_insert_event();
        assert_eq!(insert_event.event_type_str(), "insert");

        let update_event = create_test_update_event();
        assert_eq!(update_event.event_type_str(), "update");

        let delete_event = create_test_delete_event();
        assert_eq!(delete_event.event_type_str(), "delete");

        let truncate_event = create_test_truncate_event();
        assert_eq!(truncate_event.event_type_str(), "truncate");

        // This tests the event type string conversion logic
    }

    #[test]
    fn test_concurrent_metrics_operations() {
        use std::sync::Arc;
        use std::thread;

        let collector = Arc::new(MetricsCollector::new());
        let mut handles = vec![];

        // Spawn multiple threads to test thread safety
        for i in 0..10 {
            let collector_clone = Arc::clone(&collector);
            let handle = thread::spawn(move || {
                let event = create_test_insert_event();
                collector_clone.record_event(&event);
                collector_clone.record_processing_duration(
                    Duration::from_millis(i * 10),
                    "insert",
                    "mysql",
                );
                collector_clone.record_error("concurrent_test", "thread");
                collector_clone.update_source_connection_status(i % 2 == 0);
                collector_clone.record_received_lsn(i as u64 * 1000);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // All operations should complete successfully
        // This tests thread safety of the metrics collector
    }

    #[test]
    fn test_comprehensive_workflow() {
        let collector = MetricsCollector::new();

        // Initialize build info
        collector.init_build_info("1.0.0-test");

        // Update connection status
        collector.update_source_connection_status(true);
        collector.update_destination_connection_status("mysql", true);

        // Record some events with timers
        let timer1 = ProcessingTimer::start("insert", "mysql");
        let insert_event = create_test_insert_event();
        collector.record_event(&insert_event);
        timer1.finish(&collector);

        let timer2 = ProcessingTimer::start("update", "mysql");
        let update_event = create_test_update_event();
        collector.record_event(&update_event);
        timer2.finish(&collector);

        // Record LSNs
        collector.record_received_lsn(12345);

        // Update queue and connections
        collector.update_consumer_queue_length(50);
        collector.update_active_connections(3, "source");

        // Update rates and uptime
        collector.update_events_rate();
        collector.update_uptime();

        // Record an error
        collector.record_error("test_error", "workflow");

        // Get final metrics
        let metrics_result = collector.get_metrics();
        assert!(metrics_result.is_ok());

        // This tests a comprehensive workflow with all metrics operations
    }
}
