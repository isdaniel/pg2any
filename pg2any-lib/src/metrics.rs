//! Metrics collection and monitoring for pg2any CDC replication
//!
//! This module provides comprehensive metrics collection for monitoring CDC replication
//! performance, lag, errors, and resource usage.

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram,
    register_histogram_vec, Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramVec,
    Registry,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

use crate::types::ChangeEvent;

lazy_static! {
    /// Global metrics registry for all CDC metrics
    pub static ref REGISTRY: Registry = Registry::new();

    // =============================================================================
    // Core Replication Metrics
    // =============================================================================

    /// Total number of CDC events processed
    pub static ref EVENTS_PROCESSED_TOTAL: Counter = register_counter!(
        "pg2any_events_processed_total",
        "Total number of CDC events processed"
    ).expect("metric can be created");

    /// Number of events processed by type (insert, update, delete)
    pub static ref EVENTS_BY_TYPE: CounterVec = register_counter_vec!(
        "pg2any_events_by_type_total",
        "Number of events processed by type",
        &["event_type", "table_name"]
    ).expect("metric can be created");

    /// Current replication lag in seconds
    pub static ref REPLICATION_LAG_SECONDS: Gauge = register_gauge!(
        "pg2any_replication_lag_seconds",
        "Current replication lag between source and target in seconds"
    ).expect("metric can be created");

    /// Events processing rate (events per second)
    pub static ref EVENTS_RATE: Gauge = register_gauge!(
        "pg2any_events_per_second",
        "Current rate of events being processed per second"
    ).expect("metric can be created");

    /// Last processed LSN from PostgreSQL WAL
    pub static ref LAST_PROCESSED_LSN: Gauge = register_gauge!(
        "pg2any_last_processed_lsn",
        "Last processed LSN from PostgreSQL WAL"
    ).expect("metric can be created");

    /// Current LSN received from PostgreSQL
    pub static ref CURRENT_RECEIVED_LSN: Gauge = register_gauge!(
        "pg2any_current_received_lsn",
        "Current LSN received from PostgreSQL replication stream"
    ).expect("metric can be created");

    // =============================================================================
    // Error and Health Metrics
    // =============================================================================

    /// Total number of errors encountered
    pub static ref ERRORS_TOTAL: CounterVec = register_counter_vec!(
        "pg2any_errors_total",
        "Total number of errors by type",
        &["error_type", "component"]
    ).expect("metric can be created");

    /// Connection status to source database (1 = connected, 0 = disconnected)
    pub static ref SOURCE_CONNECTION_STATUS: Gauge = register_gauge!(
        "pg2any_source_connection_status",
        "Connection status to source PostgreSQL database"
    ).expect("metric can be created");

    /// Connection status to destination database
    pub static ref DESTINATION_CONNECTION_STATUS: GaugeVec = register_gauge_vec!(
        "pg2any_destination_connection_status",
        "Connection status to destination database",
        &["destination_type"]
    ).expect("metric can be created");

    // =============================================================================
    // Performance Metrics
    // =============================================================================

    /// Event processing duration histogram
    pub static ref EVENT_PROCESSING_DURATION: HistogramVec = register_histogram_vec!(
        "pg2any_event_processing_duration_seconds",
        "Time taken to process CDC events",
        &["event_type", "destination_type"]
    ).expect("metric can be created");

    /// Queue depth - number of events waiting to be processed
    pub static ref QUEUE_DEPTH: Gauge = register_gauge!(
        "pg2any_queue_depth",
        "Number of events waiting to be processed"
    ).expect("metric can be created");

    /// Batch size for event processing
    pub static ref BATCH_SIZE: Histogram = register_histogram!(
        "pg2any_batch_size",
        "Size of event batches being processed"
    ).expect("metric can be created");

    /// Network I/O metrics
    pub static ref NETWORK_BYTES_RECEIVED: Counter = register_counter!(
        "pg2any_network_bytes_received_total",
        "Total bytes received from PostgreSQL replication stream"
    ).expect("metric can be created");

    /// Network I/O - bytes sent to destination
    pub static ref NETWORK_BYTES_SENT: CounterVec = register_counter_vec!(
        "pg2any_network_bytes_sent_total",
        "Total bytes sent to destination databases",
        &["destination_type"]
    ).expect("metric can be created");

    // =============================================================================
    // Resource Usage Metrics
    // =============================================================================

    /// Memory usage for event buffers
    pub static ref BUFFER_MEMORY_USAGE_BYTES: Gauge = register_gauge!(
        "pg2any_buffer_memory_usage_bytes",
        "Memory usage for event buffering in bytes"
    ).expect("metric can be created");

    /// Active connections count
    pub static ref ACTIVE_CONNECTIONS: GaugeVec = register_gauge_vec!(
        "pg2any_active_connections",
        "Number of active database connections",
        &["connection_type"] // source, destination
    ).expect("metric can be created");

    // =============================================================================
    // Application Metrics
    // =============================================================================

    /// Application uptime
    pub static ref UPTIME_SECONDS: Gauge = register_gauge!(
        "pg2any_uptime_seconds",
        "Application uptime in seconds"
    ).expect("metric can be created");

    /// Build information
    pub static ref BUILD_INFO: GaugeVec = register_gauge_vec!(
        "pg2any_build_info",
        "Build information",
        &["version"]
    ).expect("metric can be created");
}

/// Initialize all metrics with the global registry
pub fn init_metrics() -> Result<(), Box<dyn std::error::Error>> {
    REGISTRY
        .register(Box::new(EVENTS_PROCESSED_TOTAL.clone()))
        .map_err(|e| format!("Failed to register EVENTS_PROCESSED_TOTAL: {}", e))?;

    REGISTRY
        .register(Box::new(EVENTS_BY_TYPE.clone()))
        .map_err(|e| format!("Failed to register EVENTS_BY_TYPE: {}", e))?;

    REGISTRY
        .register(Box::new(REPLICATION_LAG_SECONDS.clone()))
        .map_err(|e| format!("Failed to register REPLICATION_LAG_SECONDS: {}", e))?;

    REGISTRY
        .register(Box::new(EVENTS_RATE.clone()))
        .map_err(|e| format!("Failed to register EVENTS_RATE: {}", e))?;

    REGISTRY
        .register(Box::new(LAST_PROCESSED_LSN.clone()))
        .map_err(|e| format!("Failed to register LAST_PROCESSED_LSN: {}", e))?;

    REGISTRY
        .register(Box::new(CURRENT_RECEIVED_LSN.clone()))
        .map_err(|e| format!("Failed to register CURRENT_RECEIVED_LSN: {}", e))?;

    REGISTRY
        .register(Box::new(ERRORS_TOTAL.clone()))
        .map_err(|e| format!("Failed to register ERRORS_TOTAL: {}", e))?;

    REGISTRY
        .register(Box::new(SOURCE_CONNECTION_STATUS.clone()))
        .map_err(|e| format!("Failed to register SOURCE_CONNECTION_STATUS: {}", e))?;

    REGISTRY
        .register(Box::new(DESTINATION_CONNECTION_STATUS.clone()))
        .map_err(|e| format!("Failed to register DESTINATION_CONNECTION_STATUS: {}", e))?;

    REGISTRY
        .register(Box::new(EVENT_PROCESSING_DURATION.clone()))
        .map_err(|e| format!("Failed to register EVENT_PROCESSING_DURATION: {}", e))?;

    REGISTRY
        .register(Box::new(QUEUE_DEPTH.clone()))
        .map_err(|e| format!("Failed to register QUEUE_DEPTH: {}", e))?;

    REGISTRY
        .register(Box::new(BATCH_SIZE.clone()))
        .map_err(|e| format!("Failed to register BATCH_SIZE: {}", e))?;

    REGISTRY
        .register(Box::new(NETWORK_BYTES_RECEIVED.clone()))
        .map_err(|e| format!("Failed to register NETWORK_BYTES_RECEIVED: {}", e))?;

    REGISTRY
        .register(Box::new(NETWORK_BYTES_SENT.clone()))
        .map_err(|e| format!("Failed to register NETWORK_BYTES_SENT: {}", e))?;

    REGISTRY
        .register(Box::new(BUFFER_MEMORY_USAGE_BYTES.clone()))
        .map_err(|e| format!("Failed to register BUFFER_MEMORY_USAGE_BYTES: {}", e))?;

    REGISTRY
        .register(Box::new(ACTIVE_CONNECTIONS.clone()))
        .map_err(|e| format!("Failed to register ACTIVE_CONNECTIONS: {}", e))?;

    REGISTRY
        .register(Box::new(UPTIME_SECONDS.clone()))
        .map_err(|e| format!("Failed to register UPTIME_SECONDS: {}", e))?;

    REGISTRY
        .register(Box::new(BUILD_INFO.clone()))
        .map_err(|e| format!("Failed to register BUILD_INFO: {}", e))?;

    debug!("All metrics registered successfully");
    Ok(())
}

/// Gather all metrics from the global registry
pub fn gather_metrics() -> Result<String, Box<dyn std::error::Error>> {
    let metric_families = REGISTRY.gather();
    let encoder = prometheus::TextEncoder::new();
    let mut output = Vec::new();
    encoder.encode(&metric_families, &mut output)?;
    Ok(String::from_utf8(output)?)
}

/// Metrics collector for tracking CDC performance and replication lag
#[derive(Debug)]
pub struct MetricsCollector {
    start_time: Instant,
    last_event_time: Option<Instant>,
    events_in_window: u64,
    window_start: Instant,
    window_duration: Duration,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_event_time: None,
            events_in_window: 0,
            window_start: now,
            window_duration: Duration::from_secs(60), // 1-minute window for rate calculation
        }
    }

    /// Initialize with build information
    pub fn init_build_info(&self, version: &str) {
        BUILD_INFO.with_label_values(&[version]).set(1.0);
    }

    /// Update uptime metric
    pub fn update_uptime(&self) {
        let uptime = self.start_time.elapsed().as_secs() as f64;
        UPTIME_SECONDS.set(uptime);
    }

    /// Record a processed CDC event
    pub fn record_event(&mut self, event: &ChangeEvent, destination_type: &str) {
        let now = Instant::now();

        // Update counters
        EVENTS_PROCESSED_TOTAL.inc();

        // Track by event type and table
        let event_type = match &event.event_type {
            crate::types::EventType::Insert { .. } => "insert",
            crate::types::EventType::Update { .. } => "update",
            crate::types::EventType::Delete { .. } => "delete",
            crate::types::EventType::Truncate(_) => "truncate",
            _ => "other",
        };

        // Extract table name from event type
        let table_name = match &event.event_type {
            crate::types::EventType::Insert { table, .. } => table.as_str(),
            crate::types::EventType::Update { table, .. } => table.as_str(),
            crate::types::EventType::Delete { table, .. } => table.as_str(),
            _ => "unknown",
        };
        EVENTS_BY_TYPE
            .with_label_values(&[event_type, table_name])
            .inc();

        // Update LSN if available
        if let Some(lsn) = event.lsn {
            LAST_PROCESSED_LSN.set(lsn.0 as f64);
        }

        // Calculate processing rate
        self.events_in_window += 1;
        if now.duration_since(self.window_start) >= self.window_duration {
            let rate = self.events_in_window as f64 / self.window_duration.as_secs() as f64;
            EVENTS_RATE.set(rate);

            // Reset window
            self.events_in_window = 0;
            self.window_start = now;
        }

        self.last_event_time = Some(now);
        debug!(
            "Recorded event: type={}, table={}, destination={}",
            event_type, table_name, destination_type
        );
    }

    /// Record event processing duration
    pub fn record_processing_duration(
        &self,
        duration: Duration,
        event_type: &str,
        destination_type: &str,
    ) {
        EVENT_PROCESSING_DURATION
            .with_label_values(&[event_type, destination_type])
            .observe(duration.as_secs_f64());
    }

    /// Record replication lag in seconds
    pub fn record_replication_lag(&self, lag_seconds: f64) {
        REPLICATION_LAG_SECONDS.set(lag_seconds);
        debug!("Replication lag recorded: {} seconds", lag_seconds);
    }

    /// Record current received LSN
    pub fn record_received_lsn(&self, lsn: u64) {
        CURRENT_RECEIVED_LSN.set(lsn as f64);
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str, component: &str) {
        ERRORS_TOTAL
            .with_label_values(&[error_type, component])
            .inc();
        warn!(
            "Error recorded: type={}, component={}",
            error_type, component
        );
    }

    /// Update connection status
    pub fn update_source_connection_status(&self, connected: bool) {
        SOURCE_CONNECTION_STATUS.set(if connected { 1.0 } else { 0.0 });
    }

    /// Update destination connection status
    pub fn update_destination_connection_status(&self, destination_type: &str, connected: bool) {
        DESTINATION_CONNECTION_STATUS
            .with_label_values(&[destination_type])
            .set(if connected { 1.0 } else { 0.0 });
    }

    /// Update queue depth
    pub fn update_queue_depth(&self, depth: usize) {
        QUEUE_DEPTH.set(depth as f64);
    }

    /// Record batch size
    pub fn record_batch_size(&self, size: usize) {
        BATCH_SIZE.observe(size as f64);
    }

    /// Record network bytes received
    pub fn record_bytes_received(&self, bytes: u64) {
        NETWORK_BYTES_RECEIVED.inc_by(bytes as f64);
    }

    /// Record network bytes sent to destination
    pub fn record_bytes_sent(&self, bytes: u64, destination_type: &str) {
        NETWORK_BYTES_SENT
            .with_label_values(&[destination_type])
            .inc_by(bytes as f64);
    }

    /// Update buffer memory usage
    pub fn update_buffer_memory_usage(&self, bytes: u64) {
        BUFFER_MEMORY_USAGE_BYTES.set(bytes as f64);
    }

    /// Update active connections count
    pub fn update_active_connections(&self, count: usize, connection_type: &str) {
        ACTIVE_CONNECTIONS
            .with_label_values(&[connection_type])
            .set(count as f64);
    }

    /// Get metrics in Prometheus text format
    pub fn get_metrics(&self) -> Result<String, Box<dyn std::error::Error>> {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = REGISTRY.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    /// Calculate replication lag from timestamps
    pub fn calculate_lag_from_timestamp(&self, event_timestamp: SystemTime) -> Option<f64> {
        match event_timestamp.duration_since(UNIX_EPOCH) {
            Ok(event_duration) => {
                match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(now_duration) => {
                        if now_duration > event_duration {
                            let lag = now_duration - event_duration;
                            Some(lag.as_secs_f64())
                        } else {
                            // Event is from the future, return 0 lag
                            Some(0.0)
                        }
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }
}

/// Timer for measuring event processing duration
pub struct ProcessingTimer {
    start: Instant,
    event_type: String,
    destination_type: String,
}

impl ProcessingTimer {
    /// Start a new processing timer
    pub fn start(event_type: &str, destination_type: &str) -> Self {
        Self {
            start: Instant::now(),
            event_type: event_type.to_string(),
            destination_type: destination_type.to_string(),
        }
    }

    /// Finish timing and record the duration
    pub fn finish(self, collector: &MetricsCollector) {
        let duration = self.start.elapsed();
        collector.record_processing_duration(duration, &self.event_type, &self.destination_type);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EventType, Lsn};

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();
        assert!(collector.last_event_time.is_none());
        assert_eq!(collector.events_in_window, 0);
    }

    #[test]
    fn test_build_info() {
        // Initialize the metrics registry first
        let _ = init_metrics();

        let collector = MetricsCollector::new();
        collector.init_build_info("1.0.0");

        // Check that build info is set
        let metrics = collector.get_metrics().unwrap();
        assert!(metrics.contains("pg2any_build_info"));
    }

    #[test]
    fn test_lag_calculation() {
        let collector = MetricsCollector::new();

        // Test with past timestamp
        let past_time = SystemTime::now() - Duration::from_secs(30);
        let lag = collector.calculate_lag_from_timestamp(past_time);
        assert!(lag.is_some());
        assert!(lag.unwrap() >= 29.0 && lag.unwrap() <= 31.0); // Allow some tolerance

        // Test with future timestamp
        let future_time = SystemTime::now() + Duration::from_secs(30);
        let lag = collector.calculate_lag_from_timestamp(future_time);
        assert!(lag.is_some());
        assert_eq!(lag.unwrap(), 0.0);
    }

    #[test]
    fn test_processing_timer() {
        // Initialize the metrics registry first
        let _ = init_metrics();

        let collector = MetricsCollector::new();
        let timer = ProcessingTimer::start("insert", "mysql");
        std::thread::sleep(Duration::from_millis(10));
        timer.finish(&collector);

        // Verify metrics were recorded
        let metrics = collector.get_metrics().unwrap();
        assert!(metrics.contains("pg2any_event_processing_duration_seconds"));
    }
}
