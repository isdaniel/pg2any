pub mod common;

/// MySQL destination implementation
#[cfg(feature = "mysql")]
pub mod mysql;

/// SQL Server destination implementation
#[cfg(feature = "sqlserver")]
pub mod sqlserver;

/// SQLite destination implementation
#[cfg(feature = "sqlite")]
pub mod sqlite;

/// Destination factory and trait definitions
pub mod destination_factory;

// Re-export the implementations for easy access
#[cfg(feature = "mysql")]
pub use mysql::MySQLDestination;

#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerDestination;

#[cfg(feature = "sqlite")]
pub use sqlite::SQLiteDestination;

// Re-export factory and trait
pub use destination_factory::{DestinationFactory, DestinationHandler};
