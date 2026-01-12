//! Compressed transaction storage implementation
//!
//! This module provides a storage implementation that writes and reads
//! transaction files in compressed gzip format with sync points for efficient seeking.

use crate::error::{CdcError, Result};
use crate::storage::sql_parser::SqlStreamParser;
use crate::storage::traits::TransactionStorage;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use tracing::{debug, info};

/// Number of SQL statements per sync point
/// Each sync point starts a new gzip block, enabling seeking
const SYNC_POINT_INTERVAL: usize = 1000;

/// Offset information for a statement in compressed file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementOffset {
    /// Statement index (0-based)
    pub statement_index: usize,
    /// Byte offset in compressed file where this block starts
    pub compressed_offset: u64,
}

/// Index for a compressed SQL file
#[derive(Debug, Serialize, Deserialize)]
pub struct CompressionIndex {
    /// Total number of statements
    pub total_statements: usize,
    /// Sync points (every N statements)
    pub sync_points: Vec<StatementOffset>,
}

impl CompressionIndex {
    /// Create a new empty index
    pub fn new() -> Self {
        Self {
            total_statements: 0,
            sync_points: Vec::new(),
        }
    }

    /// Find the sync point to use for seeking to a given statement index
    pub fn find_sync_point_for_index(&self, target_index: usize) -> Option<&StatementOffset> {
        // Binary search for the largest sync point <= target_index
        let partition_idx = self
            .sync_points
            .partition_point(|sp| sp.statement_index <= target_index);
        self.sync_points.get(partition_idx.saturating_sub(1))
    }

    /// Save index to a file
    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| CdcError::generic(format!("Failed to serialize index: {e}")))?;

        tokio::fs::write(path, json)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to write index file: {e}")))?;

        Ok(())
    }

    /// Load index from a file
    pub async fn load_from_file(path: &Path) -> Result<Self> {
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to read index file: {e}")))?;

        let index: Self = serde_json::from_str(&content)
            .map_err(|e| CdcError::generic(format!("Failed to parse index: {e}")))?;

        Ok(index)
    }
}

impl Default for CompressionIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Compressed storage handler for transaction files
///
/// Stores transactions as `.sql.gz` files with an accompanying `.sql.gz.idx`
/// index file for efficient seeking. Uses multi-block gzip compression with
/// sync points to enable O(1) seeking to any statement position.
#[derive(Debug, Clone)]
pub struct CompressedStorage;

impl CompressedStorage {
    /// Create a new compressed storage handler
    pub fn new() -> Self {
        Self
    }

    /// Get the index file path for a compressed file
    fn index_path(compressed_path: &Path) -> PathBuf {
        compressed_path.with_extension("sql.gz.idx")
    }
}

impl Default for CompressedStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionStorage for CompressedStorage {
    async fn write_transaction(&self, file_path: &Path, data: &[String]) -> Result<PathBuf> {
        let compressed_path = file_path.with_extension("sql.gz");

        info!(
            "Compressing {:?} to {:?} with sync points (interval: {})",
            file_path, compressed_path, SYNC_POINT_INTERVAL
        );

        let total_statements = data.len();

        if total_statements == 0 {
            return Err(CdcError::generic("No statements to compress"));
        }

        // Add semicolons back to statements
        let statements_with_semicolons: Vec<String> = data
            .iter()
            .map(|stmt| {
                if stmt.trim().ends_with(';') {
                    stmt.clone()
                } else {
                    format!("{};", stmt.trim())
                }
            })
            .collect();

        // Create destination file
        let mut dest_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&compressed_path)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to create dest file: {e}")))?;

        let mut current_offset: u64 = 0;
        let mut index = CompressionIndex::new();
        index.total_statements = total_statements;

        // Process statements in chunks of SYNC_POINT_INTERVAL
        for (chunk_idx, chunk) in statements_with_semicolons
            .chunks(SYNC_POINT_INTERVAL)
            .enumerate()
        {
            let statement_index = chunk_idx * SYNC_POINT_INTERVAL;

            // Record sync point at start of this chunk
            index.sync_points.push(StatementOffset {
                statement_index,
                compressed_offset: current_offset,
            });

            // Compress this chunk as a separate gzip block
            let chunk_data = chunk.join("\n");

            let buffer = tokio::task::spawn_blocking({
                let chunk_data = chunk_data.clone();
                move || -> Result<Vec<u8>> {
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder
                        .write_all(chunk_data.as_bytes())
                        .map_err(|e| CdcError::generic(format!("Compression write failed: {e}")))?;
                    encoder
                        .finish()
                        .map_err(|e| CdcError::generic(format!("Compression finish failed: {e}")))
                }
            })
            .await
            .map_err(|e| CdcError::generic(format!("Compression task failed: {e}")))?;

            let buffer = buffer?;

            dest_file
                .write_all(&buffer)
                .await
                .map_err(|e| CdcError::generic(format!("Failed to write compressed data: {e}")))?;

            let compressed_size = buffer.len() as u64;
            current_offset += compressed_size;

            debug!(
                "Compressed chunk {} (statements {}-{}): {} bytes compressed",
                chunk_idx,
                statement_index,
                statement_index + chunk.len() - 1,
                compressed_size
            );
        }

        dest_file
            .flush()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to flush dest file: {e}")))?;

        // Save index file
        let index_path = Self::index_path(&compressed_path);
        index.save_to_file(&index_path).await?;

        info!(
            "Created compression index: {:?} ({} sync points, {} statements)",
            index_path,
            index.sync_points.len(),
            total_statements
        );

        Ok(compressed_path)
    }

    async fn read_transaction(&self, file_path: &Path, start_index: usize) -> Result<Vec<String>> {
        let index_path = Self::index_path(file_path);

        // Check if index file exists
        if tokio::fs::metadata(&index_path).await.is_err() {
            // Fall back to full decompression for v1 files
            debug!(
                "No index file found for {:?}, falling back to full decompression",
                file_path
            );
            return self.read_full(file_path, start_index).await;
        }

        // Load index
        let index = CompressionIndex::load_from_file(&index_path).await?;

        if start_index >= index.total_statements {
            return Ok(Vec::new());
        }

        // Find appropriate sync point
        let sync_point = index.find_sync_point_for_index(start_index);

        match sync_point {
            Some(sp) => {
                debug!(
                    "Using sync point at statement {} to read from index {}",
                    sp.statement_index, start_index
                );
                self.read_from_sync_point(file_path, sp, start_index).await
            }
            None => {
                debug!("No sync point found, reading from beginning");
                self.read_full(file_path, start_index).await
            }
        }
    }

    async fn delete_transaction(&self, file_path: &Path) -> Result<()> {
        // Delete main compressed file
        if tokio::fs::metadata(file_path).await.is_ok() {
            fs::remove_file(file_path).await.map_err(|e| {
                CdcError::generic(format!("Failed to delete file {file_path:?}: {e}"))
            })?;
            debug!("Deleted compressed file: {:?}", file_path);
        }

        // Delete index file
        let index_path = Self::index_path(file_path);
        if tokio::fs::metadata(&index_path).await.is_ok() {
            fs::remove_file(&index_path).await.map_err(|e| {
                CdcError::generic(format!("Failed to delete index {index_path:?}: {e}"))
            })?;
            debug!("Deleted index file: {:?}", index_path);
        }

        Ok(())
    }

    async fn file_exists(&self, file_path: &Path) -> bool {
        tokio::fs::metadata(file_path).await.is_ok()
    }

    fn file_extension(&self) -> &str {
        "sql.gz"
    }

    fn transform_path(&self, base_path: &Path) -> PathBuf {
        base_path.with_extension("sql.gz")
    }
}

impl CompressedStorage {
    /// Read from a specific sync point in the compressed file
    async fn read_from_sync_point(
        &self,
        compressed_path: &Path,
        sync_point: &StatementOffset,
        start_index: usize,
    ) -> Result<Vec<String>> {
        use async_compression::tokio::bufread::GzipDecoder;

        // Open the compressed file and seek to the sync point offset
        let mut file = tokio::fs::File::open(compressed_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to open compressed file {compressed_path:?}: {e}"
            ))
        })?;

        let offset = sync_point.compressed_offset;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| CdcError::generic(format!("Failed to seek to offset {offset}: {e}")))?;

        debug!(
            "Seeking to compressed offset {} (sync point at statement {})",
            offset, sync_point.statement_index
        );

        // Create a buffered reader and gzip decoder for streaming decompression
        let buf_reader = BufReader::new(file);
        let mut decoder = GzipDecoder::new(buf_reader);

        // Enable multi-member decoding to handle multiple concatenated gzip streams
        decoder.multiple_members(true);

        // Use SqlStreamParser to correctly parse SQL statements
        let mut parser = SqlStreamParser::new();

        let skip_count = start_index.saturating_sub(sync_point.statement_index);
        let result = parser.parse_stream_collect(decoder, skip_count).await?;

        Ok(result)
    }

    /// Read entire compressed file (fallback for v1 files or when no seeking needed)
    async fn read_full(&self, compressed_path: &Path, start_index: usize) -> Result<Vec<String>> {
        let file = tokio::fs::File::open(compressed_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to open compressed file {compressed_path:?}: {e}"
            ))
        })?;

        let buf_reader = BufReader::new(file);
        let mut decoder = GzipDecoder::new(buf_reader);

        // Enable multi-member decoding to handle multiple concatenated gzip streams
        decoder.multiple_members(true);

        let mut parser = SqlStreamParser::new();
        let statements = parser.parse_stream_collect(decoder, start_index).await?;

        debug!(
            "Read {} statements from compressed file {:?} (starting from index {})",
            statements.len(),
            compressed_path,
            start_index
        );

        Ok(statements)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    async fn create_temp_dir() -> PathBuf {
        let temp_dir =
            std::env::temp_dir().join(format!("pg2any_comp_test_{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        temp_dir
    }

    #[tokio::test]
    async fn test_write_and_read_compressed() {
        let temp_dir = create_temp_dir().await;
        let base_path = temp_dir.join("test");

        let storage = CompressedStorage::new();

        // Write some statements
        let statements = vec![
            "INSERT INTO users VALUES (1, 'Alice');".to_string(),
            "INSERT INTO users VALUES (2, 'Bob');".to_string(),
        ];

        let written_path = storage
            .write_transaction(&base_path, &statements)
            .await
            .unwrap();

        assert!(written_path.to_string_lossy().ends_with(".sql.gz"));
        assert!(storage.file_exists(&written_path).await);

        // Check index file exists
        let index_path = CompressedStorage::index_path(&written_path);
        assert!(index_path.exists());

        // Read back
        let read_statements = storage.read_transaction(&written_path, 0).await.unwrap();

        assert_eq!(read_statements.len(), 2);
        assert_eq!(read_statements[0], "INSERT INTO users VALUES (1, 'Alice')");
        assert_eq!(read_statements[1], "INSERT INTO users VALUES (2, 'Bob')");

        // Clean up
        storage.delete_transaction(&written_path).await.unwrap();
        assert!(!storage.file_exists(&written_path).await);
        assert!(!index_path.exists());
    }

    #[tokio::test]
    async fn test_compression_with_sync_points() {
        let temp_dir = create_temp_dir().await;
        let base_path = temp_dir.join("test_sync");

        let storage = CompressedStorage::new();

        // Create 2500 statements (3 sync points at 0, 1000, 2000)
        let statements: Vec<String> = (0..2500)
            .map(|i| format!("INSERT INTO test VALUES ({});", i))
            .collect();

        let written_path = storage
            .write_transaction(&base_path, &statements)
            .await
            .unwrap();

        // Check index
        let index_path = CompressedStorage::index_path(&written_path);
        let index = CompressionIndex::load_from_file(&index_path).await.unwrap();

        assert_eq!(index.total_statements, 2500);
        assert_eq!(index.sync_points.len(), 3); // 0, 1000, 2000

        // Read from middle using sync point
        let read_statements = storage.read_transaction(&written_path, 1100).await.unwrap();

        assert_eq!(read_statements.len(), 1400); // 2500 - 1100
        assert!(read_statements[0].contains("1100"));

        // Clean up
        storage.delete_transaction(&written_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_extension() {
        let storage = CompressedStorage::new();
        assert_eq!(storage.file_extension(), "sql.gz");
    }

    #[tokio::test]
    async fn test_transform_path() {
        let storage = CompressedStorage::new();
        let base = PathBuf::from("/tmp/transaction_123");
        let transformed = storage.transform_path(&base);
        assert_eq!(transformed, PathBuf::from("/tmp/transaction_123.sql.gz"));
    }
}
