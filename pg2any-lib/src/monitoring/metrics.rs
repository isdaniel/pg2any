//! Metrics collection and monitoring for pg2any CDC replication
//!
//! This module provides comprehensive metrics collection for monitoring CDC replication
//! performance, lag, errors, and resource usage.

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec,
    register_histogram_vec, Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramVec, Registry,
};
use tracing::debug;

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

    /// Events processing rate (events per second)
    pub static ref EVENTS_RATE: Gauge = register_gauge!(
        "pg2any_events_per_second",
        "Current rate of events being processed per second"
    ).expect("metric can be created");

    /// Consumer queue size (pending events in the consumer thread)
    pub static ref CONSUMER_QUEUE_SIZE: Gauge = register_gauge!(
        "pg2any_consumer_queue_length",
        "Number of pending events in the consumer thread queue"
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

    // =============================================================================
    // Resource Usage Metrics
    // =============================================================================

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

    /// Total number of transactions processed by consumers
    pub static ref TRANSACTIONS_PROCESSED_TOTAL: Counter = register_counter!(
        "pg2any_transactions_processed_total",
        "Total number of transactions successfully processed"
    ).expect("metric can be created");

    /// Total number of full transactions (final batches) processed
    pub static ref FULL_TRANSACTIONS_PROCESSED_TOTAL: Counter = register_counter!(
        "pg2any_full_transactions_processed_total",
        "Total number of complete transactions (final batches) successfully processed"
    ).expect("metric can be created");

    // =============================================================================
    // Memory Metrics (jemalloc)
    // =============================================================================

    /// Total bytes allocated by the application (jemalloc)
    pub static ref MEMORY_ALLOCATED_BYTES: Gauge = register_gauge!(
        "pg2any_memory_allocated_bytes",
        "Total bytes allocated by the application (jemalloc)"
    ).expect("metric can be created");

    /// Total bytes in physically resident data pages (jemalloc)
    pub static ref MEMORY_RESIDENT_BYTES: Gauge = register_gauge!(
        "pg2any_memory_resident_bytes",
        "Total bytes in physically resident data pages (jemalloc)"
    ).expect("metric can be created");

    /// Total bytes in active pages (jemalloc)
    pub static ref MEMORY_ACTIVE_BYTES: Gauge = register_gauge!(
        "pg2any_memory_active_bytes",
        "Total bytes in active pages allocated by the application (jemalloc)"
    ).expect("metric can be created");

    /// Total bytes in mapped chunks (jemalloc)
    pub static ref MEMORY_MAPPED_BYTES: Gauge = register_gauge!(
        "pg2any_memory_mapped_bytes",
        "Total bytes in chunks mapped by the allocator (jemalloc)"
    ).expect("metric can be created");

    /// Total bytes dedicated to metadata (jemalloc)
    pub static ref MEMORY_METADATA_BYTES: Gauge = register_gauge!(
        "pg2any_memory_metadata_bytes",
        "Total bytes dedicated to jemalloc metadata"
    ).expect("metric can be created");

    /// Total bytes retained (not returned to OS) (jemalloc)
    pub static ref MEMORY_RETAINED_BYTES: Gauge = register_gauge!(
        "pg2any_memory_retained_bytes",
        "Total bytes retained by jemalloc (not returned to OS)"
    ).expect("metric can be created");

    /// Memory fragmentation bytes (jemalloc)
    pub static ref MEMORY_FRAGMENTATION_BYTES: Gauge = register_gauge!(
        "pg2any_memory_fragmentation_bytes",
        "Memory fragmentation in bytes (resident - active)"
    ).expect("metric can be created");

    /// Memory utilization percentage (jemalloc)
    pub static ref MEMORY_UTILIZATION_PERCENT: Gauge = register_gauge!(
        "pg2any_memory_utilization_percent",
        "Memory utilization percentage (allocated/resident * 100)"
    ).expect("metric can be created");
}

/// Initialize all metrics with the global registry
pub fn init_metrics() -> Result<(), Box<dyn std::error::Error>> {
    REGISTRY
        .register(Box::new(EVENTS_PROCESSED_TOTAL.clone()))
        .map_err(|e| format!("Failed to register EVENTS_PROCESSED_TOTAL: {e}"))?;

    REGISTRY
        .register(Box::new(EVENTS_BY_TYPE.clone()))
        .map_err(|e| format!("Failed to register EVENTS_BY_TYPE: {e}"))?;

    REGISTRY
        .register(Box::new(EVENTS_RATE.clone()))
        .map_err(|e| format!("Failed to register EVENTS_RATE: {e}"))?;

    REGISTRY
        .register(Box::new(CONSUMER_QUEUE_SIZE.clone()))
        .map_err(|e| format!("Failed to register CONSUMER_QUEUE_SIZE: {e}"))?;

    REGISTRY
        .register(Box::new(LAST_PROCESSED_LSN.clone()))
        .map_err(|e| format!("Failed to register LAST_PROCESSED_LSN: {e}"))?;

    REGISTRY
        .register(Box::new(CURRENT_RECEIVED_LSN.clone()))
        .map_err(|e| format!("Failed to register CURRENT_RECEIVED_LSN: {e}"))?;

    REGISTRY
        .register(Box::new(ERRORS_TOTAL.clone()))
        .map_err(|e| format!("Failed to register ERRORS_TOTAL: {e}"))?;

    REGISTRY
        .register(Box::new(SOURCE_CONNECTION_STATUS.clone()))
        .map_err(|e| format!("Failed to register SOURCE_CONNECTION_STATUS: {e}"))?;

    REGISTRY
        .register(Box::new(DESTINATION_CONNECTION_STATUS.clone()))
        .map_err(|e| format!("Failed to register DESTINATION_CONNECTION_STATUS: {e}"))?;

    REGISTRY
        .register(Box::new(EVENT_PROCESSING_DURATION.clone()))
        .map_err(|e| format!("Failed to register EVENT_PROCESSING_DURATION: {e}"))?;

    REGISTRY
        .register(Box::new(ACTIVE_CONNECTIONS.clone()))
        .map_err(|e| format!("Failed to register ACTIVE_CONNECTIONS: {e}"))?;

    REGISTRY
        .register(Box::new(UPTIME_SECONDS.clone()))
        .map_err(|e| format!("Failed to register UPTIME_SECONDS: {e}"))?;

    REGISTRY
        .register(Box::new(BUILD_INFO.clone()))
        .map_err(|e| format!("Failed to register BUILD_INFO: {e}"))?;

    REGISTRY
        .register(Box::new(TRANSACTIONS_PROCESSED_TOTAL.clone()))
        .map_err(|e| format!("Failed to register TRANSACTIONS_PROCESSED_TOTAL: {e}"))?;

    REGISTRY
        .register(Box::new(FULL_TRANSACTIONS_PROCESSED_TOTAL.clone()))
        .map_err(|e| format!("Failed to register FULL_TRANSACTIONS_PROCESSED_TOTAL: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_ALLOCATED_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_ALLOCATED_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_RESIDENT_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_RESIDENT_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_ACTIVE_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_ACTIVE_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_MAPPED_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_MAPPED_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_METADATA_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_METADATA_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_RETAINED_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_RETAINED_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_FRAGMENTATION_BYTES.clone()))
        .map_err(|e| format!("Failed to register MEMORY_FRAGMENTATION_BYTES: {e}"))?;

    REGISTRY
        .register(Box::new(MEMORY_UTILIZATION_PERCENT.clone()))
        .map_err(|e| format!("Failed to register MEMORY_UTILIZATION_PERCENT: {e}"))?;

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

/// Update jemalloc memory metrics
///
/// This function reads current memory statistics from jemalloc and updates
/// the corresponding Prometheus metrics. It should be called periodically
/// to keep memory metrics up to date.
///
/// This function is only available when the `jemalloc` feature is enabled.
#[cfg(feature = "metrics")]
pub fn update_jemalloc_metrics() {
    use crate::monitoring::jemalloc_stats::get_jemalloc_stats;

    let stats = get_jemalloc_stats();

    MEMORY_ALLOCATED_BYTES.set(stats.allocated as f64);
    MEMORY_RESIDENT_BYTES.set(stats.resident as f64);
    MEMORY_ACTIVE_BYTES.set(stats.active as f64);
    MEMORY_MAPPED_BYTES.set(stats.mapped as f64);
    MEMORY_METADATA_BYTES.set(stats.metadata as f64);
    MEMORY_RETAINED_BYTES.set(stats.retained as f64);
    MEMORY_FRAGMENTATION_BYTES.set(stats.fragmentation() as f64);
    MEMORY_UTILIZATION_PERCENT.set(stats.utilization_percent());
}

#[cfg(not(feature = "metrics"))]
pub fn update_jemalloc_metrics() {
    // No-op when jemalloc is not enabled
}
