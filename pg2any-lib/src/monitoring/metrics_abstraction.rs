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
    fn record_event(&self, event: &crate::types::ChangeEvent);

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

    /// Record a transaction being successfully processed
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    fn record_transaction_processed(
        &self,
        transaction: &crate::types::Transaction,
        destination_type: &str,
    );

    /// Record a full transaction (final batch) being successfully processed
    ///
    /// This method is thread-safe and handles any necessary locking internally.
    /// Only called for complete transactions (is_final_batch = true).
    fn record_full_transaction_processed(
        &self,
        transaction: &crate::types::Transaction,
        destination_type: &str,
    );
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
    use crate::monitoring::metrics::*; // Import the static metrics
    use crate::types::EventType;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};
    use tracing::{debug, warn};

    /// Real metrics collector using atomic operations for lock-free performance
    #[derive(Debug)]
    pub struct MetricsCollector {
        start_time: Instant,
        last_event_time_nanos: AtomicU64, // 0 means None
        events_in_window: AtomicU64,
        window_start_nanos: AtomicU64,
        window_duration: Duration,
    }

    impl MetricsCollector {
        /// Helper method to convert Instant to nanoseconds since start_time
        #[inline]
        fn instant_to_nanos(&self, instant: Instant) -> u64 {
            instant.duration_since(self.start_time).as_nanos() as u64
        }

        /// Get the current time as nanoseconds since start_time
        #[inline]
        fn now_nanos(&self) -> u64 {
            self.instant_to_nanos(Instant::now())
        }
    }

    impl MetricsCollectorTrait for MetricsCollector {
        fn new() -> Self {
            let now = Instant::now();
            Self {
                start_time: now,
                last_event_time_nanos: AtomicU64::new(0), // 0 represents None
                events_in_window: AtomicU64::new(0),
                window_start_nanos: AtomicU64::new(0), // Will be set relative to start_time
                window_duration: Duration::from_secs(60), // 1-minute window for rate calculation
            }
        }

        fn record_event(&self, event: &crate::types::ChangeEvent) {
            let now = Instant::now();
            let now_nanos = self.instant_to_nanos(now);

            // Update counters
            EVENTS_PROCESSED_TOTAL.inc();

            // Track by event type and table
            let event_type = event.event_type_str();
            // Extract table name from event type
            let table_name = match &event.event_type {
                EventType::Insert { table, .. }
                | EventType::Update { table, .. }
                | EventType::Delete { table, .. } => table.as_str(),
                EventType::Truncate(tables) => &tables.join(","),
                _ => "unknown",
            };

            EVENTS_BY_TYPE
                .with_label_values(&[event_type, table_name])
                .inc();

            // Update LSN
            // In pg-walstream 0.1.0, event.lsn is Lsn (not Option)
            LAST_PROCESSED_LSN.set(event.lsn.value() as f64);

            // Track events for rate calculation
            self.events_in_window.fetch_add(1, Ordering::Relaxed);

            // Update last event time atomically
            self.last_event_time_nanos
                .store(now_nanos, Ordering::Relaxed);
        }

        fn record_processing_duration(
            &self,
            duration: std::time::Duration,
            event_type: &str,
            destination_type: &str,
        ) {
            EVENT_PROCESSING_DURATION
                .with_label_values(&[event_type, destination_type])
                .observe(duration.as_secs_f64());
        }

        fn record_error(&self, error_type: &str, component: &str) {
            ERRORS_TOTAL
                .with_label_values(&[error_type, component])
                .inc();
            warn!(
                "Error recorded: type={}, component={}",
                error_type, component
            );
        }

        fn update_source_connection_status(&self, connected: bool) {
            SOURCE_CONNECTION_STATUS.set(if connected { 1.0 } else { 0.0 });
        }

        fn update_destination_connection_status(&self, destination_type: &str, connected: bool) {
            DESTINATION_CONNECTION_STATUS
                .with_label_values(&[destination_type])
                .set(if connected { 1.0 } else { 0.0 });
        }

        fn update_active_connections(&self, count: usize, connection_type: &str) {
            ACTIVE_CONNECTIONS
                .with_label_values(&[connection_type])
                .set(count as f64);
        }

        fn update_consumer_queue_length(&self, length: usize) {
            CONSUMER_QUEUE_SIZE.set(length as f64);
            debug!("Updated consumer queue length: {}", length);
        }

        fn update_uptime(&self) {
            let uptime = self.start_time.elapsed().as_secs() as f64;
            UPTIME_SECONDS.set(uptime);
        }

        fn update_events_rate(&self) {
            let now_nanos = self.now_nanos();
            let window_start = self.window_start_nanos.load(Ordering::Relaxed);

            // Initialize window start if it's not set (first call)
            if window_start == 0 {
                self.window_start_nanos.store(now_nanos, Ordering::Relaxed);
                return;
            }

            let window_duration_nanos = self.window_duration.as_nanos() as u64;

            // Check if the current window has expired
            if now_nanos.saturating_sub(window_start) >= window_duration_nanos {
                // Calculate rate for the current window
                let events = self.events_in_window.swap(0, Ordering::Relaxed);
                let rate = events as f64 / self.window_duration.as_secs() as f64;
                EVENTS_RATE.set(rate);

                // Reset window for next period
                self.window_start_nanos.store(now_nanos, Ordering::Relaxed);

                debug!("Updated events rate: {} events/sec", rate);
            }
        }

        fn record_received_lsn(&self, lsn: u64) {
            CURRENT_RECEIVED_LSN.set(lsn as f64);
        }

        fn get_metrics(&self) -> CdcResult<String> {
            use prometheus::Encoder;
            let encoder = prometheus::TextEncoder::new();
            let metric_families = REGISTRY.gather();
            let mut buffer = Vec::new();
            encoder
                .encode(&metric_families, &mut buffer)
                .map_err(|e| crate::CdcError::generic(e.to_string()))?;
            String::from_utf8(buffer).map_err(|e| crate::CdcError::generic(e.to_string()))
        }

        fn init_build_info(&self, version: &str) {
            BUILD_INFO.with_label_values(&[version]).set(1.0);
        }

        fn record_transaction_processed(
            &self,
            transaction: &crate::types::Transaction,
            destination_type: &str,
        ) {
            TRANSACTIONS_PROCESSED_TOTAL.inc();

            // Track events for rate calculation (events_per_second)
            let event_count = transaction.event_count();
            self.events_in_window
                .fetch_add(event_count as u64, Ordering::Relaxed);

            // Update last event time for rate calculation
            let now_nanos = self.now_nanos();
            self.last_event_time_nanos
                .store(now_nanos, Ordering::Relaxed);

            // Update last processed LSN if available
            if let Some(lsn) = transaction.commit_lsn {
                LAST_PROCESSED_LSN.set(lsn.0 as f64);
            }

            debug!(
                "Recorded transaction processed: transaction_id={:?}, events={}, destination={}",
                transaction.transaction_id, event_count, destination_type
            );
        }

        fn record_full_transaction_processed(
            &self,
            transaction: &crate::types::Transaction,
            destination_type: &str,
        ) {
            FULL_TRANSACTIONS_PROCESSED_TOTAL.inc();

            debug!(
                "Recorded full transaction processed: transaction_id={:?}, events={}, destination={}",
                transaction.transaction_id, transaction.event_count(), destination_type
            );
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

        fn record_event(&self, _event: &crate::types::ChangeEvent) {}

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

        fn record_transaction_processed(
            &self,
            _transaction: &crate::types::Transaction,
            _destination_type: &str,
        ) {
        }

        fn record_full_transaction_processed(
            &self,
            _transaction: &crate::types::Transaction,
            _destination_type: &str,
        ) {
        }
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
    crate::monitoring::metrics::init_metrics().map_err(|e| crate::CdcError::generic(e.to_string()))
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
        .map_err(|e| crate::CdcError::generic(e.to_string()))
}

/// Return a message indicating metrics are disabled
#[cfg(not(feature = "metrics"))]
pub fn gather_metrics() -> CdcResult<String> {
    Ok("# Metrics not available - metrics feature disabled\n".to_string())
}
