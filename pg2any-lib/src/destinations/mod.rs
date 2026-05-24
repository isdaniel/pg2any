pub mod coalescing;
pub mod common;

/// Bulk insert utilities (TSV generation, INSERT detection)
pub mod bulk_insert;

/// MySQL destination implementation
#[cfg(feature = "mysql")]
pub mod mysql;

/// SQL Server destination implementation
#[cfg(feature = "sqlserver")]
pub mod sqlserver;

/// SQLite destination implementation
#[cfg(feature = "sqlite")]
pub mod sqlite;

/// Kafka destination implementation
#[cfg(feature = "kafka")]
pub mod kafka;

/// Destination factory and trait definitions
pub mod destination_factory;

// Re-export the implementations for easy access
#[cfg(feature = "mysql")]
pub use mysql::MySQLDestination;

#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerDestination;

#[cfg(feature = "sqlite")]
pub use sqlite::SQLiteDestination;

#[cfg(feature = "kafka")]
pub use kafka::KafkaDestination;

// Re-export factory and trait
pub use destination_factory::{DestinationFactory, DestinationHandler, PreCommitHook};
