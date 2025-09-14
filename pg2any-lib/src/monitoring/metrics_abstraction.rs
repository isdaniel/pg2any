//! Metrics Abstraction Layer
//!
//! This module provides an abstraction layer for metrics that allows the application
//! to work with or without the metrics feature enabled. When metrics are disabled,
//! no-op implementations are provided to maintain the same API surface.
//!
//! The trait implementations handle thread-safe access internally, eliminating the need
//! for external wrapper types and centralizing all locking logic.
use crate::CdcResult;

/// Abstract metrics collector trait with built-in thread safety
///
/// This trait provides a uniform interface for metrics collection that can be
/// implemented either with real metrics (when the `metrics` feature is enabled)
/// or with no-op implementations (when the feature is disabled).
///
/// All implementations handle thread-safe access internally.
pub trait MetricsCollectorTrait: Send + Sync {
    /// Create a new metrics collector instance
    fn new() -> Self
    where
        Self: Sized;

    /// Record an event being processed
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn record_event(&self, event: &crate::types::ChangeEvent, destination_type: &str);

    /// Record event processing duration
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn record_processing_duration(
        &self,
        duration: std::time::Duration,
        event_type: &str,
        destination_type: &str,
    );

    /// Record an error
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn record_error(&self, error_type: &str, component: &str);

    /// Update connection status
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn update_source_connection_status(&self, connected: bool);

    /// Update destination connection status
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn update_destination_connection_status(&self, destination_type: &str, connected: bool);

    /// Update active connections count
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn update_active_connections(&self, count: usize, connection_type: &str);

    /// Update consumer queue length (pending events in the consumer thread)
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn update_consumer_queue_length(&self, length: usize);

    /// Update uptime
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn update_uptime(&self);

    /// Update events rate - should be called periodically to ensure rate reflects current state
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    /// It checks if the current time window has expired and updates the rate accordingly.
    fn update_events_rate(&self);

    /// Record received LSN
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn record_received_lsn(&self, lsn: u64);

    /// Get metrics in Prometheus text format
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn get_metrics(&self) -> CdcResult<String>;

    /// Initialize build information
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn init_build_info(&self, version: &str);
}

/// Abstract processing timer trait
///
/// Provides timing functionality that works with the metrics collector abstraction.
/// The timer handles the locking and error handling internally when finishing.
pub trait ProcessingTimerTrait {
    /// Start a new processing timer
    fn start(event_type: &str, destination_type: &str) -> Self
    where
        Self: Sized;

    /// Finish timing and record the metric with the duration
    ///
    /// This method handles thread-safe access to the collector internally.
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
    use crate::monitoring::metrics::MetricsCollector as RealMetricsCollector;
    use std::sync::{Arc, Mutex};

    /// Real metrics collector that wraps the actual prometheus-based collector
    /// with built-in thread safety
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
            } else {
                self.handle_lock_error("record_event");
            }
        }

        fn record_processing_duration(
            &self,
            duration: std::time::Duration,
            event_type: &str,
            destination_type: &str,
        ) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_processing_duration(duration, event_type, destination_type);
            } else {
                self.handle_lock_error("record_processing_duration");
            }
        }

        fn record_error(&self, error_type: &str, component: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_error(error_type, component);
            } else {
                self.handle_lock_error("record_error");
            }
        }

        fn update_source_connection_status(&self, connected: bool) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_source_connection_status(connected);
            } else {
                self.handle_lock_error("update_source_connection_status");
            }
        }

        fn update_destination_connection_status(&self, destination_type: &str, connected: bool) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_destination_connection_status(destination_type, connected);
            } else {
                self.handle_lock_error("update_destination_connection_status");
            }
        }

        fn update_active_connections(&self, count: usize, connection_type: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_active_connections(count, connection_type);
            } else {
                self.handle_lock_error("update_active_connections");
            }
        }

        fn update_consumer_queue_length(&self, length: usize) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_consumer_queue_length(length);
            } else {
                self.handle_lock_error("update_consumer_queue_length");
            }
        }

        fn update_uptime(&self) {
            if let Ok(collector) = self.inner.lock() {
                collector.update_uptime();
            } else {
                self.handle_lock_error("update_uptime");
            }
        }

        fn update_events_rate(&self) {
            if let Ok(mut collector) = self.inner.lock() {
                collector.update_events_rate();
            } else {
                self.handle_lock_error("update_events_rate");
            }
        }

        fn record_received_lsn(&self, lsn: u64) {
            if let Ok(collector) = self.inner.lock() {
                collector.record_received_lsn(lsn);
            } else {
                self.handle_lock_error("record_received_lsn");
            }
        }

        fn get_metrics(&self) -> CdcResult<String> {
            match self.inner.lock() {
                Ok(collector) => collector
                    .get_metrics()
                    .map_err(|e| crate::CdcError::generic(&e.to_string())),
                Err(_) => {
                    self.handle_lock_error("get_metrics");
                    Err(crate::CdcError::generic(
                        "Failed to acquire metrics collector lock",
                    ))
                }
            }
        }

        fn init_build_info(&self, version: &str) {
            if let Ok(collector) = self.inner.lock() {
                collector.init_build_info(version);
            } else {
                self.handle_lock_error("init_build_info");
            }
        }
    }

    impl MetricsCollector {
        /// Handle lock acquisition errors with standard logging
        #[inline]
        fn handle_lock_error(&self, operation: &str) {
            tracing::warn!("Failed to lock metrics_collector for {}", operation);
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
            collector.record_processing_duration(
                duration,
                &self.event_type,
                &self.destination_type,
            );
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

        fn record_processing_duration(
            &self,
            _duration: std::time::Duration,
            _event_type: &str,
            _destination_type: &str,
        ) {
        }

        fn record_error(&self, _error_type: &str, _component: &str) {}

        fn update_source_connection_status(&self, _connected: bool) {}

        fn update_destination_connection_status(&self, _destination_type: &str, _connected: bool) {}

        fn update_active_connections(&self, _count: usize, _connection_type: &str) {}

        fn update_consumer_queue_length(&self, _size: usize) {}

        fn update_uptime(&self) {}

        fn update_events_rate(&self) {}

        fn record_received_lsn(&self, _lsn: u64) {}

        fn get_metrics(&self) -> CdcResult<String> {
            Ok("# Metrics not available - metrics feature disabled\n".to_string())
        }

        fn init_build_info(&self, _version: &str) {}
    }

    /// No-op processing timer that does nothing
    pub struct ProcessingTimer {
        _phantom: std::marker::PhantomData<()>,
    }

    impl ProcessingTimerTrait for ProcessingTimer {
        fn start(_event_type: &str, _destination_type: &str) -> Self {
            Self {
                _phantom: std::marker::PhantomData,
            }
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
    crate::monitoring::metrics::init_metrics().map_err(|e| crate::CdcError::generic(&e.to_string()))
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
    crate::monitoring::metrics::gather_metrics()
        .map_err(|e| crate::CdcError::generic(&e.to_string()))
}

/// Return a message indicating metrics are disabled
#[cfg(not(feature = "metrics"))]
pub fn gather_metrics() -> CdcResult<String> {
    Ok("# Metrics not available - metrics feature disabled\n".to_string())
}
