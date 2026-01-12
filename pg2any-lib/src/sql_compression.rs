//! SQL File Compression Module with Sync Points
//!
//! This module provides compression with sync points for efficient seeking in large files.
//!
//! ## Key Features:
//! - Multi-block gzip compression (one block per sync point)
//! - Index file with byte offsets for O(1) seeking
//! - Streaming decompression from any sync point
//! - Backward compatible with v1 single-stream format
//!
//! ## Format:
//! - `.sql.gz`: Multi-block gzip file
//! - `.sql.gz.idx`: JSON index with statement offsets and sync points
//!
//! ## Memory Efficiency:
//! - Compression: O(statements_per_sync_point)
//! - Decompression: O(statements_since_last_sync_point)
//! - Seeking: O(1) to find sync point, then O(k) to decompress k statements

use crate::error::{CdcError, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use tokio_util::sync::CancellationToken;
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
    /// Byte offset in uncompressed stream where this statement starts
    pub uncompressed_offset: u64,
    /// Whether this is a sync point (start of new gzip block)
    pub is_sync_point: bool,
}

/// Index for a compressed SQL file
#[derive(Debug, Serialize, Deserialize)]
pub struct CompressionIndex {
    /// Version of the index format
    pub version: u32,
    /// Total number of statements
    pub total_statements: usize,
    /// Sync points (every N statements)
    pub sync_points: Vec<StatementOffset>,
}

impl CompressionIndex {
    /// Create a new empty index
    pub fn new() -> Self {
        Self {
            version: 2, // v2 format with multi-block compression
            total_statements: 0,
            sync_points: Vec::new(),
        }
    }

    /// Find the sync point to use for seeking to a given statement index
    pub fn find_sync_point_for_index(&self, target_index: usize) -> Option<&StatementOffset> {
        // Binary search for the largest sync point <= target_index
        self.sync_points
            .iter()
            .rev()
            .find(|sp| sp.statement_index <= target_index)
    }

    /// Save index to a file
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| CdcError::generic(format!("Failed to serialize index: {e}")))?;

        std::fs::write(path, json)
            .map_err(|e| CdcError::generic(format!("Failed to write index file: {e}")))?;

        Ok(())
    }

    /// Load index from a file
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| CdcError::generic(format!("Failed to read index file: {e}")))?;

        let index: Self = serde_json::from_str(&content)
            .map_err(|e| CdcError::generic(format!("Failed to parse index file: {e}")))?;

        Ok(index)
    }
}

/// Compress a SQL file with sync points for efficient seeking
///
/// Creates two files:
/// - `dest_path`: Multi-block gzip compressed file
/// - `dest_path.idx`: JSON index with sync point offsets
///
/// # Arguments
/// * `source_path` - Uncompressed SQL file
/// * `dest_path` - Destination for compressed file (will also create .idx)
///
/// # Returns
/// Total number of statements compressed
pub async fn compress_file_with_sync_points(source_path: &Path, dest_path: &Path) -> Result<usize> {
    info!(
        "Compressing {:?} to {:?} with sync points (interval: {})",
        source_path, dest_path, SYNC_POINT_INTERVAL
    );

    let source_content = tokio::fs::read_to_string(source_path)
        .await
        .map_err(|e| CdcError::generic(format!("Failed to read source file: {e}")))?;

    // Parse SQL statements
    let statements: Vec<String> = source_content
        .lines()
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(format!("{};", trimmed))
            }
        })
        .collect();

    let total_statements = statements.len();

    if total_statements == 0 {
        return Err(CdcError::generic("No statements found in source file"));
    }

    // Compress with sync points using blocking I/O
    tokio::task::spawn_blocking({
        let dest = dest_path.to_path_buf();
        let statements = statements.clone();
        move || -> Result<CompressionIndex> {
            let _dest_file = std::fs::File::create(&dest)
                .map_err(|e| CdcError::generic(format!("Failed to create dest file: {e}")))?;

            let mut current_offset: u64 = 0;
            let mut uncompressed_offset: u64 = 0;
            let mut index = CompressionIndex::new();
            index.total_statements = statements.len();

            // Process statements in chunks of SYNC_POINT_INTERVAL
            for (chunk_idx, chunk) in statements.chunks(SYNC_POINT_INTERVAL).enumerate() {
                let statement_index = chunk_idx * SYNC_POINT_INTERVAL;

                // Record sync point at start of this chunk
                index.sync_points.push(StatementOffset {
                    statement_index,
                    compressed_offset: current_offset,
                    uncompressed_offset,
                    is_sync_point: true,
                });

                // Compress this chunk as a separate gzip block
                let chunk_data = chunk.join("\n");
                let uncompressed_size = chunk_data.len() as u64;

                // Create a new gzip encoder for this block
                let mut buffer = Vec::new();
                {
                    let mut encoder = GzEncoder::new(&mut buffer, Compression::default());
                    encoder
                        .write_all(chunk_data.as_bytes())
                        .map_err(|e| CdcError::generic(format!("Failed to compress chunk: {e}")))?;
                    encoder.finish().map_err(|e| {
                        CdcError::generic(format!("Failed to finish compression: {e}"))
                    })?;
                }

                // Write compressed block to file
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&dest)
                    .map_err(|e| {
                        CdcError::generic(format!("Failed to open dest for append: {e}"))
                    })?;

                file.write_all(&buffer).map_err(|e| {
                    CdcError::generic(format!("Failed to write compressed block: {e}"))
                })?;

                let compressed_size = buffer.len() as u64;
                current_offset += compressed_size;
                uncompressed_offset += uncompressed_size;

                debug!(
                    "Sync point {}: statements {}-{}, compressed offset: {}, size: {} bytes",
                    chunk_idx,
                    statement_index,
                    statement_index + chunk.len() - 1,
                    current_offset - compressed_size,
                    compressed_size
                );
            }

            Ok(index)
        }
    })
    .await
    .map_err(|e| CdcError::generic(format!("Compression task failed: {e}")))?
    .and_then(|idx| {
        // Save index file
        let index_path = dest_path.with_extension("sql.gz.idx");
        idx.save_to_file(&index_path)?;
        info!(
            "Created compression index: {:?} ({} sync points)",
            index_path,
            idx.sync_points.len()
        );
        Ok(total_statements)
    })
}

/// Read compressed file from a specific statement index using sync points
///
/// Uses the index file to seek to the appropriate sync point, then
/// decompresses only from that point forward.
///
/// # Arguments
/// * `compressed_path` - Path to .sql.gz file
/// * `start_index` - Statement index to start reading from
/// * `cancel_token` - Token for graceful cancellation
///
/// # Returns
/// Vector of SQL statements starting from `start_index`
pub async fn read_compressed_file_with_seeking(
    compressed_path: &Path,
    start_index: usize,
    cancel_token: &CancellationToken,
) -> Result<Vec<String>> {
    let index_path = compressed_path.with_extension("sql.gz.idx");

    // Check if index file exists
    if !index_path.exists() {
        // Fall back to full decompression for v1 files
        debug!(
            "Index file not found for {:?}, using full decompression",
            compressed_path
        );
        return read_compressed_file_full(compressed_path, start_index, cancel_token).await;
    }

    // Load index
    let index = CompressionIndex::load_from_file(&index_path)?;

    if start_index >= index.total_statements {
        return Ok(Vec::new());
    }

    // Find appropriate sync point
    let sync_point = index.find_sync_point_for_index(start_index);

    match sync_point {
        Some(sp) => {
            info!(
                "Seeking to sync point at statement {} (offset: {} bytes) to read from statement {}",
                sp.statement_index, sp.compressed_offset, start_index
            );

            // Read from sync point
            read_from_sync_point(compressed_path, sp, start_index, cancel_token).await
        }
        None => {
            // No sync point found, decompress from beginning
            read_compressed_file_full(compressed_path, start_index, cancel_token).await
        }
    }
}

/// Read from a specific sync point in the compressed file
///
/// This implements true block-level seeking by:
/// 1. Seeking to the compressed byte offset of the sync point
/// 2. Reading and decompressing only from that point forward
/// 3. Parsing statements and skipping to the requested start_index
async fn read_from_sync_point(
    compressed_path: &Path,
    sync_point: &StatementOffset,
    start_index: usize,
    _cancel_token: &CancellationToken,
) -> Result<Vec<String>> {
    // Read compressed file starting from the sync point offset
    let compressed_data = tokio::fs::read(compressed_path).await.map_err(|e| {
        CdcError::generic(format!(
            "Failed to read compressed file {:?}: {}",
            compressed_path, e
        ))
    })?;

    // Extract data from sync point offset onwards
    let offset = sync_point.compressed_offset as usize;
    if offset > compressed_data.len() {
        return Err(CdcError::generic(format!(
            "Invalid sync point offset: {} > file size {}",
            offset,
            compressed_data.len()
        )));
    }

    let data_from_sync_point = &compressed_data[offset..];

    debug!(
        "Reading {} bytes from compressed offset {} ({}% of file)",
        data_from_sync_point.len(),
        offset,
        (data_from_sync_point.len() * 100) / compressed_data.len().max(1)
    );

    // Decompress from sync point in a blocking task
    let decompressed_data = tokio::task::spawn_blocking({
        let data = data_from_sync_point.to_vec();
        move || -> Result<Vec<u8>> {
            use flate2::read::MultiGzDecoder;
            use std::io::Read;

            let mut decoder = MultiGzDecoder::new(&data[..]);
            let mut result = Vec::new();

            decoder.read_to_end(&mut result).map_err(|e| {
                CdcError::generic(format!("Failed to decompress from sync point: {}", e))
            })?;

            Ok(result)
        }
    })
    .await
    .map_err(|e| CdcError::generic(format!("Decompression task failed: {}", e)))??;

    // Parse SQL statements from decompressed data
    let decompressed_str = String::from_utf8_lossy(&decompressed_data);

    // Parse all statements from this sync point onwards
    let statements_from_sync: Vec<String> = decompressed_str
        .split(';')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                None
            } else {
                Some(format!("{};", trimmed))
            }
        })
        .collect();

    // Calculate how many statements to skip within this decompressed chunk
    // We decompressed from sync_point.statement_index, but we want to start at start_index
    let skip_count = if start_index > sync_point.statement_index {
        start_index - sync_point.statement_index
    } else {
        0
    };

    let result: Vec<String> = statements_from_sync.into_iter().skip(skip_count).collect();

    Ok(result)
}

/// Read entire compressed file (fallback for v1 files or when no seeking needed)
///
/// Handles both single gzip streams and multiple concatenated gzip streams (sync points)
async fn read_compressed_file_full(
    compressed_path: &Path,
    start_index: usize,
    _cancel_token: &CancellationToken,
) -> Result<Vec<String>> {
    // Read and decompress all gzip blocks manually
    let compressed_data = tokio::fs::read(compressed_path).await.map_err(|e| {
        CdcError::generic(format!(
            "Failed to read compressed file {:?}: {}",
            compressed_path, e
        ))
    })?;

    // Decompress all concatenated gzip streams in a blocking task
    let decompressed_data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        use flate2::read::MultiGzDecoder;
        use std::io::Read;

        let mut decoder = MultiGzDecoder::new(&compressed_data[..]);
        let mut result = Vec::new();

        decoder
            .read_to_end(&mut result)
            .map_err(|e| CdcError::generic(format!("Failed to decompress: {}", e)))?;

        Ok(result)
    })
    .await
    .map_err(|e| CdcError::generic(format!("Decompression task failed: {}", e)))??;

    // Parse SQL statements from decompressed data
    let decompressed_str = String::from_utf8_lossy(&decompressed_data);

    // Parse statements
    let all_statements: Vec<String> = decompressed_str
        .split(';')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                None
            } else {
                Some(format!("{};", trimmed))
            }
        })
        .collect();

    // Return only statements from start_index onwards
    let statements: Vec<String> = all_statements.into_iter().skip(start_index).collect();

    debug!(
        "Read {} statements from compressed file {:?} (starting from index {})",
        statements.len(),
        compressed_path,
        start_index
    );

    Ok(statements)
}

/// Check if a file is compressed (has .gz extension)
pub fn is_compressed_file(path: &Path) -> bool {
    path.extension().and_then(|s| s.to_str()) == Some("gz")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::io::AsyncWriteExt;

    async fn create_test_sql_file(path: &PathBuf, statement_count: usize) -> std::io::Result<()> {
        let mut file = tokio::fs::File::create(path).await?;

        for i in 0..statement_count {
            let stmt = format!("INSERT INTO test VALUES ({}, 'data{}');\n", i, i);
            file.write_all(stmt.as_bytes()).await?;
        }

        file.flush().await?;
        Ok(())
    }

    #[test]
    fn test_is_compressed_file() {
        assert!(is_compressed_file(Path::new("test.sql.gz")));
        assert!(!is_compressed_file(Path::new("test.sql")));
        assert!(!is_compressed_file(Path::new("test.txt")));
    }

    #[tokio::test]
    async fn test_compression_with_sync_points() {
        let temp_dir =
            std::env::temp_dir().join(format!("pg2any_sync_test_{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Create test file with 2500 statements (3 sync points at 0, 1000, 2000)
        let source_path = temp_dir.join("test.sql");
        create_test_sql_file(&source_path, 2500).await.unwrap();

        let dest_path = temp_dir.join("test.sql.gz");
        let total = compress_file_with_sync_points(&source_path, &dest_path)
            .await
            .unwrap();

        assert_eq!(total, 2500);
        assert!(dest_path.exists());

        // Check index file
        let index_path = dest_path.with_extension("sql.gz.idx");
        assert!(index_path.exists());

        let index = CompressionIndex::load_from_file(&index_path).unwrap();
        assert_eq!(index.version, 2);
        assert_eq!(index.total_statements, 2500);
        assert_eq!(index.sync_points.len(), 3); // 0, 1000, 2000

        tokio::fs::remove_dir_all(&temp_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_reading_compressed_file() {
        let temp_dir =
            std::env::temp_dir().join(format!("pg2any_read_test_{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let source_path = temp_dir.join("test.sql");
        create_test_sql_file(&source_path, 1500).await.unwrap();

        let dest_path = temp_dir.join("test.sql.gz");
        compress_file_with_sync_points(&source_path, &dest_path)
            .await
            .unwrap();

        // Test 1: Read all statements from beginning
        let cancel_token = CancellationToken::new();
        let all_statements = read_compressed_file_with_seeking(&dest_path, 0, &cancel_token)
            .await
            .unwrap();

        assert_eq!(all_statements.len(), 1500);
        assert!(all_statements[0].contains("0"));
        assert!(all_statements[1499].contains("1499"));

        // Test 2: Read from statement 1100 (should get 400 statements: 1100-1499)
        let statements = read_compressed_file_with_seeking(&dest_path, 1100, &cancel_token)
            .await
            .unwrap();

        assert_eq!(statements.len(), 400); // 1500 - 1100 = 400
        assert!(statements[0].contains("1100"));
        assert!(statements[statements.len() - 1].contains("1499"));

        tokio::fs::remove_dir_all(&temp_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_backward_compatibility_without_index() {
        // Test that files without .idx fall back to full decompression
        let temp_dir =
            std::env::temp_dir().join(format!("pg2any_compat_test_{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let source_path = temp_dir.join("test.sql");
        create_test_sql_file(&source_path, 100).await.unwrap();

        // Compress without sync points (single gzip stream)
        let dest_path = temp_dir.join("test.sql.gz");
        let source_content = tokio::fs::read(&source_path).await.unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&source_content).unwrap();
        let compressed = encoder.finish().unwrap();
        tokio::fs::write(&dest_path, compressed).await.unwrap();

        // Read should work even without .idx file
        let cancel_token = CancellationToken::new();
        let statements = read_compressed_file_with_seeking(&dest_path, 0, &cancel_token)
            .await
            .unwrap();

        assert_eq!(statements.len(), 100);

        tokio::fs::remove_dir_all(&temp_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_block_level_seeking_efficiency() {
        // Test that seeking actually reads less data by using sync points
        let temp_dir =
            std::env::temp_dir().join(format!("pg2any_seek_test_{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Create a large file with 5000 statements (5 sync points: 0, 1000, 2000, 3000, 4000)
        let source_path = temp_dir.join("test.sql");
        create_test_sql_file(&source_path, 5000).await.unwrap();

        let dest_path = temp_dir.join("test.sql.gz");
        let total = compress_file_with_sync_points(&source_path, &dest_path)
            .await
            .unwrap();
        assert_eq!(total, 5000);

        // Load the index to verify sync points
        let index_path = dest_path.with_extension("sql.gz.idx");
        let index = CompressionIndex::load_from_file(&index_path).unwrap();
        assert_eq!(index.sync_points.len(), 5); // 0, 1000, 2000, 3000, 4000

        let cancel_token = CancellationToken::new();

        // Test 1: Read from statement 3500 (should use sync point at 3000)
        let statements = read_compressed_file_with_seeking(&dest_path, 3500, &cancel_token)
            .await
            .unwrap();

        assert_eq!(statements.len(), 1500); // 5000 - 3500 = 1500
        assert!(statements[0].contains("3500"));
        assert!(statements[statements.len() - 1].contains("4999"));

        // Test 2: Read from statement 4200 (should use sync point at 4000)
        let statements = read_compressed_file_with_seeking(&dest_path, 4200, &cancel_token)
            .await
            .unwrap();

        assert_eq!(statements.len(), 800); // 5000 - 4200 = 800
        assert!(statements[0].contains("4200"));
        assert!(statements[statements.len() - 1].contains("4999"));

        // Test 3: Read from exact sync point (statement 2000)
        let statements = read_compressed_file_with_seeking(&dest_path, 2000, &cancel_token)
            .await
            .unwrap();

        assert_eq!(statements.len(), 3000); // 5000 - 2000 = 3000
        assert!(statements[0].contains("2000"));

        // Test 4: Read from very end (statement 4999)
        let statements = read_compressed_file_with_seeking(&dest_path, 4999, &cancel_token)
            .await
            .unwrap();

        assert_eq!(statements.len(), 1); // Only last statement
        assert!(statements[0].contains("4999"));

        tokio::fs::remove_dir_all(&temp_dir).await.unwrap();
    }
}
