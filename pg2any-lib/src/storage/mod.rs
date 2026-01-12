//! Storage abstraction module for transaction file operations
//!
//! This module provides a trait-based abstraction for different storage implementations,
//! eliminating the need for compression_enabled checks throughout the codebase.
//!
//! ## Storage Implementations
//!
//! - **UncompressedStorage**: Plain `.sql` files without compression
//! - **CompressedStorage**: `.sql.gz` files with sync points for efficient seeking
//!
//! ## Usage
//!
//! ```rust,ignore
//! use pg2any_lib::storage::{StorageFactory, TransactionStorage};
//!
//! // Create storage based on environment variable
//! let storage = StorageFactory::from_env();
//!
//! // Write transaction
//! let statements = vec!["INSERT INTO users VALUES (1, 'Alice');".to_string()];
//! let file_path = storage.write_transaction(&path, &statements).await?;
//!
//! // Read transaction
//! let statements = storage.read_transaction(&file_path, 0).await?;
//! ```

mod compressed;
pub mod sql_parser;
mod traits;
mod uncompressed;

pub use compressed::{CompressedStorage, CompressionIndex, StatementOffset};
pub use sql_parser::SqlStreamParser;
pub use traits::TransactionStorage;
pub use uncompressed::UncompressedStorage;

use std::sync::Arc;
use tracing::info;

/// Factory for creating storage implementations based on configuration
pub struct StorageFactory;

impl StorageFactory {
    /// Create a storage implementation based on the PG2ANY_ENABLE_COMPRESSION environment variable
    ///
    /// Returns:
    /// - `CompressedStorage` if PG2ANY_ENABLE_COMPRESSION=true or PG2ANY_ENABLE_COMPRESSION=1
    /// - `UncompressedStorage` otherwise (default)
    pub fn from_env() -> Arc<dyn TransactionStorage> {
        let compression_enabled = std::env::var("PG2ANY_ENABLE_COMPRESSION")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        Self::create(compression_enabled)
    }

    /// Create a storage implementation based on a boolean flag
    ///
    /// # Arguments
    /// * `compressed` - If true, creates CompressedStorage; otherwise UncompressedStorage
    ///
    /// # Returns
    /// Arc-wrapped storage implementation for thread-safe sharing
    pub fn create(compressed: bool) -> Arc<dyn TransactionStorage> {
        if compressed {
            info!("Using CompressedStorage: SQL files will be written as .sql.gz with index files");
            Arc::new(CompressedStorage::new())
        } else {
            info!(
                "Using UncompressedStorage: SQL files will be written as uncompressed .sql files"
            );
            Arc::new(UncompressedStorage::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_create_uncompressed() {
        let storage = StorageFactory::create(false);
        assert_eq!(storage.file_extension(), "sql");
    }

    #[test]
    fn test_factory_create_compressed() {
        let storage = StorageFactory::create(true);
        assert_eq!(storage.file_extension(), "sql.gz");
    }

    #[test]
    fn test_factory_from_env_default() {
        // Without setting env var, should default to uncompressed
        std::env::remove_var("PG2ANY_ENABLE_COMPRESSION");
        let storage = StorageFactory::from_env();
        assert_eq!(storage.file_extension(), "sql");
    }

    #[test]
    fn test_factory_from_env_true() {
        std::env::set_var("PG2ANY_ENABLE_COMPRESSION", "true");
        let storage = StorageFactory::from_env();
        assert_eq!(storage.file_extension(), "sql.gz");
        std::env::remove_var("PG2ANY_ENABLE_COMPRESSION");
    }

    #[test]
    fn test_factory_from_env_one() {
        std::env::set_var("PG2ANY_ENABLE_COMPRESSION", "1");
        let storage = StorageFactory::from_env();
        assert_eq!(storage.file_extension(), "sql.gz");
        std::env::remove_var("PG2ANY_ENABLE_COMPRESSION");
    }
}
