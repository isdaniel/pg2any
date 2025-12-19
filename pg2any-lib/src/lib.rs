//! # PostgreSQL CDC Library
//!
//! A comprehensive Change Data Capture (CDC) library for PostgreSQL using logical replication.
//! This library allows you to stream database changes in real-time from PostgreSQL to other databases
//! such as SQL Server, MySQL, and more.
//!

//! ## Features
//!
//! - PostgreSQL logical replication support
//! - Real-time change streaming (INSERT, UPDATE, DELETE, TRUNCATE)
//! - Multiple destination database support (SQL Server, MySQL)
//! - Async/await support with Tokio
//! - Comprehensive error handling
//! - Thread-safe operations
//! - Built-in backpressure handling
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use pg2any_lib::{load_config_from_env, run_cdc_app};
//! use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
//!
//! #[tokio::main]
//!     async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize comprehensive logging
//!     init_logging();
//!     tracing::info!("Starting PostgreSQL CDC Application");
//!     // Load configuration from environment variables
//!     let config = load_config_from_env()?;
//!     // Run the CDC application with graceful shutdown handling
//!     run_cdc_app(config, None).await?;
//!     tracing::info!("CDC application stopped");
//!     Ok(())
//! }
//!
//! pub fn init_logging() {
//!     // Create a sophisticated logging setup
//!     let env_filter = EnvFilter::try_from_default_env()
//!         .unwrap_or_else(|_| EnvFilter::new("pg2any=debug,tokio_postgres=info,sqlx=info"));
//!
//!     let fmt_layer = fmt::layer()
//!         .with_target(true)
//!         .with_thread_ids(true)
//!         .with_level(true)
//!         .with_ansi(true)
//!         .compact();
//!
//!     tracing_subscriber::registry()
//!         .with(env_filter)
//!         .with(fmt_layer)
//!         .init();
//!
//!     tracing::info!("Logging initialized with level filtering");
//! }
//! ```

// Core modules
pub mod app;
pub mod config;
pub mod env;
pub mod error;

// Destination handlers
pub mod types;

// Low-level PostgreSQL replication using libpq-sys
pub mod buffer;
pub mod pg_replication;
pub mod retry;

// Unified replication protocol implementation
pub mod logical_stream;
pub mod replication_protocol;

// High-level client interface
pub mod client;

// Streaming transaction management for protocol v2+
mod transaction_manager;

// Monitoring and metrics
pub mod monitoring;

// Public API exports
pub use app::{run_cdc_app, CdcApp, CdcAppConfig};
pub use client::CdcClient;
pub use config::{Config, ConfigBuilder};
pub use env::load_config_from_env;
pub use error::CdcError;

/// Result type for CDC operations
pub type CdcResult<T> = Result<T, CdcError>;

pub mod destinations;

// Re-export implementations
#[cfg(feature = "mysql")]
pub use crate::destinations::MySQLDestination;

#[cfg(feature = "sqlserver")]
pub use crate::destinations::SqlServerDestination;

#[cfg(feature = "sqlite")]
pub use crate::destinations::SQLiteDestination;

pub use crate::destinations::{DestinationFactory, DestinationHandler};
pub use crate::replication_protocol::{
    ColumnData, ColumnInfo, LogicalReplicationMessage, LogicalReplicationParser, RelationInfo,
    ReplicationState, TupleData,
};
pub use crate::types::{DestinationType, Transaction};

// Conditionally export metrics server functionality
#[cfg(feature = "metrics")]
pub use crate::monitoring::{
    create_metrics_server, create_metrics_server_with_config, MetricsServer, MetricsServerConfig,
};

// Always export metrics abstraction layer
pub use crate::monitoring::{
    gather_metrics, init_metrics, MetricsCollector, MetricsCollectorTrait, ProcessingTimer,
    ProcessingTimerTrait,
};

// Conditionally export real metrics functionality when feature is enabled
#[cfg(feature = "metrics")]
pub use crate::monitoring::init_real_metrics;
