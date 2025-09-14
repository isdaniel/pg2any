//! Monitoring and Metrics Module
//!
//! This module contains all monitoring-related functionality including metrics collection,
//! metrics server, and the metrics abstraction layer.
//!
//! The metrics system has been unified to eliminate duplicate locking logic. All thread
//! safety is handled within the trait implementations themselves, with configurable
//! error handling modes.

// Metrics abstraction layer - always available
pub mod metrics_abstraction;

// Real metrics implementation - only available when metrics feature is enabled
#[cfg(feature = "metrics")]
pub mod metrics;

// Metrics HTTP server - only available when metrics feature is enabled
#[cfg(feature = "metrics")]
pub mod metrics_server;

// Re-export key types and functions for convenience
pub use metrics_abstraction::{
    gather_metrics, init_metrics, MetricsCollector, MetricsCollectorTrait,
    ProcessingTimer, ProcessingTimerTrait,
};

#[cfg(feature = "metrics")]
pub use metrics_server::{
    create_metrics_server, create_metrics_server_with_config, MetricsServer, MetricsServerConfig,
};

// Conditionally export real metrics functionality when feature is enabled
#[cfg(feature = "metrics")]
pub use metrics_abstraction::init_metrics as init_real_metrics;
