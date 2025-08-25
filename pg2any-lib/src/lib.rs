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
//! ```rust,no_run
//! use pg2any_lib::{CdcClient, Config, DestinationType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::builder()
//!         .source_connection_string("postgresql://user:password@localhost/source_db")
//!         .destination_type(DestinationType::MySQL)
//!         .destination_connection_string("mysql://user:password@localhost/target_db")
//!         .replication_slot_name("my_cdc_slot")
//!         .publication_name("my_publication")
//!         .build()?;
//!     
//!     let mut client = CdcClient::new(config).await?;
//!     client.start_replication_from_lsn(None).await?;
//!     
//!     Ok(())
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

// Unified replication protocol implementation
pub mod logical_stream;
pub mod replication_protocol;

// High-level client interface
pub mod client;
pub mod connection;

// Public API exports
pub use app::{run_cdc_app, CdcApp};
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

pub use crate::destinations::{DestinationFactory, DestinationHandler};
pub use crate::replication_protocol::{
    ColumnData, ColumnInfo, LogicalReplicationMessage, LogicalReplicationParser, RelationInfo,
    ReplicationState, TupleData,
};
pub use crate::types::DestinationType;
