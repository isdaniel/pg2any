//! Metrics Abstraction Layer
//!
//! This module provides an abstraction layer for metrics that allows the application
//! to work with or without the metrics feature enabled. When metrics are disabled,
//! no-op implementations are provided to maintain the same API surface.

use std::time::SystemTime;

use crate::CdcResult;

/// Abstract metrics collector trait
///
/// This trait provides a uniform interface for metrics collection that can be
/// implemented either with real metrics (when the `metrics` feature is enabled)
/// or with no-op implementations (when the feature is disabled).
pub trait MetricsCollectorTrait: Send + Sync {
    /// Create a new metrics collector instance
    fn new() -> Self
    where
        Self: Sized;

    /// Record an event being processed
    fn record_event(&self, event: &crate::types::ChangeEvent, destination_type: &str);

    /// Record event processing duration
    fn record_processing_duration(&self, duration: std::time::Duration, event_type: &str, destination_type: &str);

    /// Record replication lag
    fn record_replication_lag(&self, lag_seconds: f64);

    /// Record an error
    fn record_error(&self, error_type: &str, component: &str);

    /// Update connection status
    fn update_source_connection_status(&self, connected: bool);

    /// Update destination connection status
    fn update_destination_connection_status(&self, destination_type: &str, connected: bool);

    /// Update queue depth
    fn update_queue_depth(&self, depth: usize);

    /// Record batch size
    fn record_batch_size(&self, size: usize);

    /// Record network bytes received
    fn record_bytes_received(&self, bytes: u64);

    /// Record network bytes sent
    fn record_bytes_sent(&self, bytes: u64, destination_type: &str);

    /// Update buffer memory usage
    fn update_buffer_memory_usage(&self, bytes: u64);

    /// Update active connections count
    fn update_active_connections(&self, count: usize, connection_type: &str);

    /// Update uptime
    fn update_uptime(&self);

    /// Record received LSN
    fn record_received_lsn(&self, lsn: u64);

    /// Get metrics in Prometheus text format
    fn get_metrics(&self) -> CdcResult<String>;

    /// Calculate replication lag from timestamps
    fn calculate_lag_from_timestamp(&self, event_timestamp: SystemTime) -> Option<f64>;

    /// Initialize build information
    fn init_build_info(&self, version: &str);
}

/// Abstract processing timer trait
pub trait ProcessingTimerTrait {
    /// Start a new processing timer
    fn start(event_type: &str, destination_type: &str) -> Self
    where
        Self: Sized;

    /// Finish timing and record the metric with the duration
    fn finish(self, collector: &dyn MetricsCollectorTrait);
}

// =============================================================================
// Real metrics implementations (when metrics feature is enabled)
// =============================================================================

#[cfg(feature = "metrics")]
pub use real_metrics::*;

#[cfg(feature = "metrics")]
mod real_metrics {
    use super::*;
    use crate::metrics::{MetricsCollector as RealMetricsCollector};
    use std::sync::{Arc, Mutex};

    /// Real metrics collector that wraps the actual prometheus-based collector
    #[derive(Debug)]
    pub struct MetricsCollector {
        inner: Arc<Mutex<RealMetricsCollector>>,
    }

    impl MetricsCollectorTrait for MetricsCollector {
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(RealMetricsCollector::new())),
            }
        }

        fn record_event(&self, event: &crate::types::ChangeEvent, destination_type: &str) {
            if let Ok(mut collector) = self.inner.lock() {
                collector.record_event(event, destination_type);
            }
        }

        fn record_processing_duration(&self, duration: std::time::Duration, event_type: &str, destination_type: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_processing_duration(duration, event_type, destination_type);
            }
        }

        fn record_replication_lag(&self, lag_seconds: f64) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_replication_lag(lag_seconds);
            }
        }

        fn record_error(&self, error_type: &str, component: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_error(error_type, component);
            }
        }

        fn update_source_connection_status(&self, connected: bool) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_source_connection_status(connected);
            }
        }

        fn update_destination_connection_status(&self, destination_type: &str, connected: bool) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_destination_connection_status(destination_type, connected);
            }
        }

        fn update_queue_depth(&self, depth: usize) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_queue_depth(depth);
            }
        }

        fn record_batch_size(&self, size: usize) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_batch_size(size);
            }
        }

        fn record_bytes_received(&self, bytes: u64) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_bytes_received(bytes);
            }
        }

        fn record_bytes_sent(&self, bytes: u64, destination_type: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_bytes_sent(bytes, destination_type);
            }
        }

        fn update_buffer_memory_usage(&self, bytes: u64) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_buffer_memory_usage(bytes);
            }
        }

        fn update_active_connections(&self, count: usize, connection_type: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_active_connections(count, connection_type);
            }
        }

        fn update_uptime(&self) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_uptime();
            }
        }

        fn record_received_lsn(&self, lsn: u64) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_received_lsn(lsn);
            }
        }

        fn get_metrics(&self) -> CdcResult<String> {
            if let Ok(collector) = self.inner.lock() {
                collector.get_metrics().map_err(|e| crate::CdcError::generic(&e.to_string()))
            } else {
                Err(crate::CdcError::generic("Failed to acquire metrics collector lock"))
            }
        }

        fn calculate_lag_from_timestamp(&self, event_timestamp: SystemTime) -> Option<f64> {
            if let Ok(collector) = self.inner.lock() {
                collector.calculate_lag_from_timestamp(event_timestamp)
            } else {
                None
            }
        }

        fn init_build_info(&self, version: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.init_build_info(version);
            }
        }
    }

    /// Real processing timer that wraps the actual timer
    pub struct ProcessingTimer {
        start_time: std::time::Instant,
        event_type: String,
        destination_type: String,
    }

    impl ProcessingTimerTrait for ProcessingTimer {
        fn start(event_type: &str, destination_type: &str) -> Self {
            Self {
                start_time: std::time::Instant::now(),
                event_type: event_type.to_string(),
                destination_type: destination_type.to_string(),
            }
        }

        fn finish(self, collector: &dyn MetricsCollectorTrait) {
            let duration = self.start_time.elapsed();
            collector.record_processing_duration(duration, &self.event_type, &self.destination_type);
        }
    }
}

// =============================================================================
// No-op metrics implementations (when metrics feature is disabled)
// =============================================================================

#[cfg(not(feature = "metrics"))]
pub use noop_metrics::*;

#[cfg(not(feature = "metrics"))]
mod noop_metrics {
    use super::*;

    /// No-op metrics collector that does nothing
    #[derive(Debug)]
    pub struct MetricsCollector;

    impl MetricsCollectorTrait for MetricsCollector {
        fn new() -> Self {
            Self
        }

        fn record_event(&self, _event: &crate::types::ChangeEvent, _destination_type: &str) {}

        fn record_processing_duration(&self, _duration: std::time::Duration, _event_type: &str, _destination_type: &str) {}

        fn record_replication_lag(&self, _lag_seconds: f64) {}

        fn record_error(&self, _error_type: &str, _component: &str) {}

        fn update_source_connection_status(&self, _connected: bool) {}

        fn update_destination_connection_status(&self, _destination_type: &str, _connected: bool) {}

        fn update_queue_depth(&self, _depth: usize) {}

        fn record_batch_size(&self, _size: usize) {}

        fn record_bytes_received(&self, _bytes: u64) {}

        fn record_bytes_sent(&self, _bytes: u64, _destination_type: &str) {}

        fn update_buffer_memory_usage(&self, _bytes: u64) {}

        fn update_active_connections(&self, _count: usize, _connection_type: &str) {}

        fn update_uptime(&self) {}

        fn record_received_lsn(&self, _lsn: u64) {}

        fn get_metrics(&self) -> CdcResult<String> {
            Ok("# Metrics not available - metrics feature disabled\n".to_string())
        }

        fn calculate_lag_from_timestamp(&self, _event_timestamp: SystemTime) -> Option<f64> {
            None
        }

        fn init_build_info(&self, _version: &str) {}
    }

    /// No-op processing timer that does nothing
    pub struct ProcessingTimer {
        _phantom: std::marker::PhantomData<()>,
    }

    impl ProcessingTimerTrait for ProcessingTimer {
        fn start(_event_type: &str, _destination_type: &str) -> Self {
            Self { _phantom: std::marker::PhantomData }
        }

        fn finish(self, _collector: &dyn MetricsCollectorTrait) {}
    }
}

// =============================================================================
// Convenience functions for easy metrics initialization
// =============================================================================

/// Initialize the global metrics registry (only when metrics feature is enabled)
#[cfg(feature = "metrics")]
pub fn init_metrics() -> CdcResult<()> {
    crate::metrics::init_metrics().map_err(|e| crate::CdcError::generic(&e.to_string()))
}

/// No-op metrics initialization when metrics feature is disabled
#[cfg(not(feature = "metrics"))]
pub fn init_metrics() -> CdcResult<()> {
    // No-op when metrics are disabled
    tracing::debug!("Metrics feature disabled - skipping metrics initialization");
    Ok(())
}

/// Gather all metrics from the global registry (only when metrics feature is enabled)
#[cfg(feature = "metrics")]
pub fn gather_metrics() -> CdcResult<String> {
    crate::metrics::gather_metrics().map_err(|e| crate::CdcError::generic(&e.to_string()))
}

/// Return a message indicating metrics are disabled
#[cfg(not(feature = "metrics"))]
pub fn gather_metrics() -> CdcResult<String> {
    Ok("# Metrics not available - metrics feature disabled\n".to_string())
}