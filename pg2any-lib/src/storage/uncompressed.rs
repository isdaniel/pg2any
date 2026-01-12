//! Uncompressed transaction storage implementation
//!
//! This module provides a storage implementation that writes and reads
//! transaction files in plain SQL format without compression.

use crate::error::{CdcError, Result};
use crate::storage::sql_parser::SqlStreamParser;
use crate::storage::traits::TransactionStorage;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::debug;

/// Uncompressed storage handler for transaction files
///
/// Stores transactions as plain `.sql` files with no compression.
/// Simple and efficient for smaller transactions or when compression overhead
/// is not justified.
#[derive(Debug, Clone)]
pub struct UncompressedStorage;

impl UncompressedStorage {
    /// Create a new uncompressed storage handler
    pub fn new() -> Self {
        Self
    }
}

impl Default for UncompressedStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionStorage for UncompressedStorage {
    async fn write_transaction(&self, file_path: &Path, data: &[String]) -> Result<PathBuf> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to create file {file_path:?}: {e}")))?;

        let mut writer = BufWriter::new(file);

        for statement in data {
            let stmt = statement.trim();
            writer.write_all(stmt.as_bytes()).await?;
            // Ensure each statement ends with semicolon
            if !stmt.ends_with(';') {
                writer.write_all(b";").await?;
            }
            writer.write_all(b"\n").await?;
        }

        writer.flush().await?;

        debug!(
            "Wrote {} statements to uncompressed file: {:?}",
            data.len(),
            file_path
        );

        Ok(file_path.to_path_buf())
    }

    async fn read_transaction(&self, file_path: &Path, start_index: usize) -> Result<Vec<String>> {
        let mut parser = SqlStreamParser::new();
        let statements = parser
            .parse_file_from_index_collect(file_path, start_index)
            .await?;

        debug!(
            "Read {} statements from uncompressed file {:?} (starting from index {})",
            statements.len(),
            file_path,
            start_index
        );

        Ok(statements)
    }

    async fn delete_transaction(&self, file_path: &Path) -> Result<()> {
        if tokio::fs::metadata(file_path).await.is_ok() {
            fs::remove_file(file_path).await.map_err(|e| {
                CdcError::generic(format!("Failed to delete file {file_path:?}: {e}"))
            })?;
            debug!("Deleted uncompressed file: {:?}", file_path);
        }
        Ok(())
    }

    async fn file_exists(&self, file_path: &Path) -> bool {
        tokio::fs::metadata(file_path).await.is_ok()
    }

    fn file_extension(&self) -> &str {
        "sql"
    }

    fn transform_path(&self, base_path: &Path) -> PathBuf {
        base_path.with_extension("sql")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    async fn create_temp_dir() -> PathBuf {
        let temp_dir =
            std::env::temp_dir().join(format!("pg2any_uncomp_test_{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        temp_dir
    }

    #[tokio::test]
    async fn test_write_and_read_uncompressed() {
        let temp_dir = create_temp_dir().await;
        let file_path = temp_dir.join("test.sql");

        let storage = UncompressedStorage::new();

        // Write some statements
        let statements = vec![
            "INSERT INTO users VALUES (1, 'Alice');".to_string(),
            "INSERT INTO users VALUES (2, 'Bob');".to_string(),
        ];

        let written_path = storage
            .write_transaction(&file_path, &statements)
            .await
            .unwrap();

        assert_eq!(written_path, file_path);
        assert!(storage.file_exists(&file_path).await);

        // Read back
        let read_statements = storage.read_transaction(&file_path, 0).await.unwrap();

        assert_eq!(read_statements.len(), 2);
        assert_eq!(read_statements[0], "INSERT INTO users VALUES (1, 'Alice')");
        assert_eq!(read_statements[1], "INSERT INTO users VALUES (2, 'Bob')");

        // Clean up
        storage.delete_transaction(&file_path).await.unwrap();
        assert!(!storage.file_exists(&file_path).await);
    }

    #[tokio::test]
    async fn test_read_with_start_index() {
        let temp_dir = create_temp_dir().await;
        let file_path = temp_dir.join("test_index.sql");

        let storage = UncompressedStorage::new();

        // Write multiple statements
        let statements = vec![
            "INSERT INTO users VALUES (1, 'Alice');".to_string(),
            "INSERT INTO users VALUES (2, 'Bob');".to_string(),
            "INSERT INTO users VALUES (3, 'Charlie');".to_string(),
        ];

        storage
            .write_transaction(&file_path, &statements)
            .await
            .unwrap();

        // Read from index 1 (skip first statement)
        let read_statements = storage.read_transaction(&file_path, 1).await.unwrap();

        assert_eq!(read_statements.len(), 2);
        assert_eq!(read_statements[0], "INSERT INTO users VALUES (2, 'Bob')");
        assert_eq!(
            read_statements[1],
            "INSERT INTO users VALUES (3, 'Charlie')"
        );

        // Clean up
        storage.delete_transaction(&file_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_extension() {
        let storage = UncompressedStorage::new();
        assert_eq!(storage.file_extension(), "sql");
    }

    #[tokio::test]
    async fn test_transform_path() {
        let storage = UncompressedStorage::new();
        let base = PathBuf::from("/tmp/transaction_123");
        let transformed = storage.transform_path(&base);
        assert_eq!(transformed, PathBuf::from("/tmp/transaction_123.sql"));
    }
}
