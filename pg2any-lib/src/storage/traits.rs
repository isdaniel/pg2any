//! Storage abstraction traits for transaction file operations
//!
//! This module defines the trait interface for different storage implementations
//! (compressed vs uncompressed) to avoid conditional logic throughout the codebase.

use crate::error::Result;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

/// Trait for transaction storage operations
///
/// Implementations handle reading and writing transaction files in different formats
/// (e.g., compressed vs uncompressed). This abstraction eliminates the need for
/// compression_enabled checks throughout the codebase.
#[async_trait]
pub trait TransactionStorage: Send + Sync {
    /// Write a transaction file to storage
    ///
    /// # Arguments
    /// * `file_path` - Uncompressed file path (implementation may transform this)
    /// * `data` - SQL statements to write
    ///
    /// # Returns
    /// * `PathBuf` - Actual path where data was written (may differ for compressed storage)
    async fn write_transaction(&self, file_path: &Path, data: &[String]) -> Result<PathBuf>;

    /// Write a transaction file from an existing uncompressed file
    ///
    /// Implementations may transform/compress the file. This avoids loading all
    /// statements into memory at once and can return a statement count for metadata.
    ///
    /// # Arguments
    /// * `file_path` - Uncompressed file path
    ///
    /// # Returns
    /// * `(PathBuf, usize)` - Actual path where data was written and total statements
    async fn write_transaction_from_file(&self, file_path: &Path) -> Result<(PathBuf, usize)>;

    /// Read SQL commands from a transaction file, starting from a specific index
    ///
    /// # Arguments
    /// * `file_path` - Path to the transaction file
    /// * `start_index` - Zero-based index of first command to return
    ///
    /// # Returns
    /// * `Vec<String>` - SQL statements starting from start_index
    async fn read_transaction(&self, file_path: &Path, start_index: usize) -> Result<Vec<String>>;

    /// Delete a transaction file
    ///
    /// # Arguments
    /// * `file_path` - Path to the transaction file to delete
    async fn delete_transaction(&self, file_path: &Path) -> Result<()>;

    /// Check if a file exists
    ///
    /// # Arguments
    /// * `file_path` - Path to check
    ///
    /// # Returns
    /// * `bool` - True if file exists
    async fn file_exists(&self, file_path: &Path) -> bool;

    /// Get the actual file extension used by this storage implementation
    ///
    /// # Returns
    /// * `&str` - File extension (e.g., "sql" or "sql.gz")
    fn file_extension(&self) -> &str;

    /// Transform a base file path to the actual storage path
    ///
    /// # Arguments
    /// * `base_path` - Base path without extension
    ///
    /// # Returns
    /// * `PathBuf` - Actual storage path with appropriate extension
    fn transform_path(&self, base_path: &Path) -> PathBuf;
}
