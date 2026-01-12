//! Transaction File Manager for CDC persistence
//!
//! This module implements file-based transaction persistence to ensure data consistency
//! even when long-running transactions might be aborted before commit.
//!
//! ## Architecture
//!
//! - **sql_data_tx/**: Stores actual SQL data (append-only, never moved)
//!   - File naming: `{txid}_{timestamp}.sql`
//!   - Events are appended as SQL commands as they arrive
//!   - Files are NOT moved or deleted until transaction is fully executed
//!
//! - **sql_received_tx/**: Stores metadata for in-progress transactions
//!   - File naming: `{txid}_{timestamp}.meta`
//!   - Contains JSON metadata: transaction_id, timestamp, data_file_path
//!   - Small files (~100 bytes) for fast operations
//!
//! - **sql_pending_tx/**: Stores metadata for committed transactions ready for execution
//!   - File naming: `{txid}_{timestamp}.meta`
//!   - Contains JSON metadata: transaction_id, timestamp, commit_lsn, data_file_path
//!   - Metadata moved here from sql_received_tx on COMMIT/StreamCommit
//!   - Consumer reads these to find which data files to execute
//!
//! ## Transaction Flow
//!
//! 1. BEGIN: Create data file in sql_data_tx/ and metadata in sql_received_tx/
//! 2. DML Events: Append SQL commands to the data file
//! 3. COMMIT: Move metadata from sql_received_tx/ to sql_pending_tx/ (data stays in sql_data_tx/)
//! 4. Consumer: Read metadata from sql_pending_tx/, execute SQL from sql_data_tx/
//! 5. Success: Delete metadata from sql_pending_tx/ and data file from sql_data_tx/
//! 6. ABORT: Delete metadata from sql_received_tx/ and data file from sql_data_tx/
//!
//! ## Recovery
//!
//! On startup:
//! - Metadata in sql_received_tx/ are incomplete transactions (can be cleaned up with data files)
//! - Metadata in sql_pending_tx/ are committed but not yet executed (must be processed)

use crate::error::{CdcError, Result};
use crate::sql_compression::{self, is_compressed_file};
use crate::sql_streaming::SqlStreamParser;
use crate::types::{ChangeEvent, DestinationType, EventType, Lsn, ReplicaIdentity};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

const RECEIVED_TX_DIR: &str = "sql_received_tx";
const PENDING_TX_DIR: &str = "sql_pending_tx";
const DATA_TX_DIR: &str = "sql_data_tx";

/// Default buffer size for event accumulation (1MB)
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

struct BufferedEventWriter {
    /// File path being written to
    file_path: PathBuf,
    /// In-memory buffer for accumulating SQL statements
    buffer: String,
    /// Maximum buffer size before forced flush
    max_buffer_size: usize,
}

impl BufferedEventWriter {
    /// Create a new buffered writer for a transaction file
    fn new(file_path: PathBuf, max_buffer_size: usize) -> Self {
        Self {
            file_path,
            buffer: String::with_capacity(max_buffer_size),
            max_buffer_size,
        }
    }

    /// Append SQL statement to the buffer
    /// Returns true if buffer should be flushed (reached capacity)
    fn append(&mut self, sql: &str) -> bool {
        self.buffer.push_str(sql);
        self.buffer.push('\n');

        // Check if we should flush
        self.buffer.len() >= self.max_buffer_size
    }

    /// Flush the buffer to disk
    /// Always writes uncompressed data - compression happens on commit if enabled
    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Always write uncompressed to avoid multiple gzip stream problem
        let file = fs::OpenOptions::new()
            .append(true)
            .open(&self.file_path)
            .await?;

        let mut writer = BufWriter::new(file);
        writer.write_all(self.buffer.as_bytes()).await?;
        writer.flush().await?;

        debug!(
            "Flushed {} bytes to {:?}",
            self.buffer.len(),
            self.file_path
        );

        // Clear buffer after successful flush
        self.buffer.clear();
        Ok(())
    }

    /// Get current buffer size
    fn buffer_size(&self) -> usize {
        self.buffer.len()
    }
}

/// Transaction file metadata stored in sql_received_tx/ and sql_pending_tx/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionFileMetadata {
    pub transaction_id: u32,
    pub commit_timestamp: DateTime<Utc>,
    pub commit_lsn: Option<Lsn>,
    pub destination_type: DestinationType,
    /// Path to the SQL data file in sql_data_tx/
    pub data_file_path: PathBuf,
    /// Transaction type: "normal" or "streaming"
    /// Used for recovery to correctly classify transactions on restart
    #[serde(default = "default_transaction_type")]
    pub transaction_type: String,
}

/// Default transaction type ("normal" for backward compatibility)
fn default_transaction_type() -> String {
    "normal".to_string()
}

/// A committed transaction file ready for execution
#[derive(Debug, Clone)]
pub struct PendingTransactionFile {
    pub file_path: PathBuf,
    pub metadata: TransactionFileMetadata,
}

// Ordering implementation for priority queue: order by commit_lsn (ascending)
impl Eq for PendingTransactionFile {}

impl PartialEq for PendingTransactionFile {
    fn eq(&self, other: &Self) -> bool {
        self.metadata.commit_lsn == other.metadata.commit_lsn
            && self.metadata.transaction_id == other.metadata.transaction_id
    }
}

impl Ord for PendingTransactionFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // For min-heap: smaller commit_lsn comes first.
        // `None` is treated as "infinity" (greater than any `Some`).
        // `transaction_id` is used as a tie-breaker for a stable, total ordering.
        match (self.metadata.commit_lsn, other.metadata.commit_lsn) {
            (Some(a), Some(b)) => a.cmp(&b).then_with(|| {
                self.metadata
                    .transaction_id
                    .cmp(&other.metadata.transaction_id)
            }),
            (Some(_), None) => std::cmp::Ordering::Less, // `Some` is smaller than `None`
            (None, Some(_)) => std::cmp::Ordering::Greater, // `None` is greater than `Some`
            (None, None) => self
                .metadata
                .transaction_id
                .cmp(&other.metadata.transaction_id),
        }
    }
}

impl PartialOrd for PendingTransactionFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Transaction File Manager for persisting and executing transactions
pub struct TransactionManager {
    base_path: PathBuf,
    destination_type: DestinationType,
    schema_mappings: HashMap<String, String>,
    /// Buffered writers for active transactions
    /// Key: transaction file path, Value: buffered writer
    active_writers: Arc<tokio::sync::Mutex<HashMap<PathBuf, BufferedEventWriter>>>,
    /// Maximum buffer size before forced flush
    buffer_size: usize,
    /// Enable gzip compression for SQL files (controlled by PG2ANY_ENABLE_COMPRESSION env var)
    compression_enabled: bool,
}

impl TransactionManager {
    /// Create a new transaction file manager
    pub async fn new(
        base_path: impl AsRef<Path>,
        destination_type: DestinationType,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create directories if they don't exist
        let received_tx_dir = base_path.join(RECEIVED_TX_DIR);
        let pending_tx_dir = base_path.join(PENDING_TX_DIR);
        let data_tx_dir = base_path.join(DATA_TX_DIR);

        fs::create_dir_all(&received_tx_dir).await?;
        fs::create_dir_all(&pending_tx_dir).await?;
        fs::create_dir_all(&data_tx_dir).await?;

        // Check if compression is enabled via environment variable
        let compression_enabled = std::env::var("PG2ANY_ENABLE_COMPRESSION")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        info!(
            "Transaction file manager initialized at {:?} for {:?}",
            base_path, destination_type
        );
        info!("Three-directory structure: sql_data_tx/ (data), sql_received_tx/ (in-progress metadata), sql_pending_tx/ (committed metadata)");

        if compression_enabled {
            info!("Compression ENABLED: SQL files will be written as .sql.gz with index files");
        } else {
            info!("Compression DISABLED: SQL files will be written as uncompressed .sql files");
        }

        Ok(Self {
            base_path,
            destination_type,
            schema_mappings: HashMap::new(),
            active_writers: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            buffer_size: DEFAULT_BUFFER_SIZE,
            compression_enabled,
        })
    }

    /// Set schema mappings for SQL generation
    pub fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    /// Flush all pending buffered writes
    /// Called during graceful shutdown to ensure no data is lost
    pub async fn flush_all_buffers(&self) -> Result<()> {
        let mut writers = self.active_writers.lock().await;

        let mut flush_count = 0;
        let mut total_bytes = 0;

        for (_, writer) in writers.iter_mut() {
            let buffer_size = writer.buffer_size();
            if buffer_size > 0 {
                writer.flush().await?;
                flush_count += 1;
                total_bytes += buffer_size;
            }
        }

        if flush_count > 0 {
            info!(
                "Flushed {} buffer(s) totaling {} bytes during shutdown",
                flush_count, total_bytes
            );
        }

        Ok(())
    }

    /// Get the file path for a received transaction metadata
    fn get_received_tx_path(&self, tx_id: u32) -> PathBuf {
        let filename = format!("{}.meta", tx_id);
        self.base_path.join(RECEIVED_TX_DIR).join(filename)
    }

    /// Get the file path for a pending transaction metadata
    fn get_pending_tx_path(&self, tx_id: u32) -> PathBuf {
        let filename = format!("{}.meta", tx_id);
        self.base_path.join(PENDING_TX_DIR).join(filename)
    }

    /// Get the file path for the SQL data file
    /// Always returns .sql - compression happens on commit if enabled
    fn get_data_file_path(&self, tx_id: u32) -> PathBuf {
        let filename = format!("{}.sql", tx_id);
        self.base_path.join(DATA_TX_DIR).join(filename)
    }

    /// Create a new transaction: data file in sql_data_tx/ and metadata in sql_received_tx/
    pub async fn begin_transaction(
        &self,
        tx_id: u32,
        timestamp: DateTime<Utc>,
        transaction_type: &str,
    ) -> Result<PathBuf> {
        let data_file_path = self.get_data_file_path(tx_id);
        let metadata_path = self.get_received_tx_path(tx_id);

        // Create the SQL data file in sql_data_tx/
        File::create(&data_file_path).await?;

        debug!("Created data file: {:?}", data_file_path);

        // Create metadata file in sql_received_tx/
        let metadata = TransactionFileMetadata {
            transaction_id: tx_id,
            commit_timestamp: timestamp,
            commit_lsn: None,
            destination_type: self.destination_type.clone(),
            data_file_path: data_file_path.clone(),
            transaction_type: transaction_type.to_string(),
        };

        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        let mut metadata_file = File::create(&metadata_path).await?;
        metadata_file.write_all(metadata_json.as_bytes()).await?;
        metadata_file.flush().await?;

        // Create a buffered writer for this transaction
        let mut writers = self.active_writers.lock().await;
        writers.insert(
            data_file_path.clone(),
            BufferedEventWriter::new(data_file_path.clone(), self.buffer_size),
        );

        info!(
            "Started transaction {}: data={:?}, metadata={:?}",
            tx_id, data_file_path, metadata_path
        );

        // Return the data file path for appending events
        Ok(data_file_path)
    }

    /// Append a change event to a running transaction file
    /// Uses buffered I/O to accumulate events in memory before flushing to disk
    /// Automatically flushes when buffer reaches capacity
    pub async fn append_event(&self, file_path: &Path, event: &ChangeEvent) -> Result<()> {
        let sql = self.generate_sql_for_event(event)?;

        // Skip empty SQL (metadata events)
        if sql.is_empty() {
            return Ok(());
        }

        let mut writers = self.active_writers.lock().await;

        if let Some(writer) = writers.get_mut(file_path) {
            // Append to buffer
            let should_flush = writer.append(&sql);

            // Flush if buffer is full
            if should_flush {
                writer.flush().await?;
            }
        }

        Ok(())
    }

    /// Move metadata from sql_received_tx to sql_pending_tx
    /// Flushes any pending buffered events before marking transaction as committed
    pub async fn commit_transaction(&self, tx_id: u32, commit_lsn: Option<Lsn>) -> Result<PathBuf> {
        let received_metadata_path = self.get_received_tx_path(tx_id);
        let pending_metadata_path = self.get_pending_tx_path(tx_id);

        // Read existing metadata from sql_received_tx/
        let metadata_content = fs::read_to_string(&received_metadata_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to read metadata from {received_metadata_path:?}: {e}"
                ))
            })?;

        let mut metadata: TransactionFileMetadata = serde_json::from_str(&metadata_content)
            .map_err(|e| CdcError::generic(format!("Failed to parse metadata: {e}")))?;

        let data_file_path = metadata.data_file_path.clone();

        // CRITICAL: Flush any pending buffered events before committing
        {
            let mut writers = self.active_writers.lock().await;
            if let Some(mut writer) = writers.remove(&data_file_path) {
                // Flush remaining buffer
                writer.flush().await?;
                debug!(
                    "Flushed final buffer for transaction {} ({} bytes)",
                    tx_id,
                    writer.buffer_size()
                );
            }
        }

        // Compress the data file if compression is enabled
        let _final_data_path = if self.compression_enabled {
            let compressed_path = data_file_path.with_extension("sql.gz");

            // Use compression with sync points for efficient seeking
            let total_statements =
                sql_compression::compress_file_with_sync_points(&data_file_path, &compressed_path)
                    .await?;

            info!(
                "Compressed transaction {} with sync points: {} statements, {} -> {}",
                tx_id,
                total_statements,
                data_file_path.display(),
                compressed_path.display()
            );

            // Delete the uncompressed file
            fs::remove_file(&data_file_path).await.map_err(|e| {
                CdcError::generic(format!("Failed to remove uncompressed file: {e}"))
            })?;

            // Update metadata to point to compressed file
            metadata.data_file_path = compressed_path.clone();
            compressed_path
        } else {
            data_file_path.clone()
        };

        // Update commit LSN
        metadata.commit_lsn = commit_lsn;

        // Write updated metadata to sql_pending_tx/
        let updated_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| CdcError::generic(format!("Failed to serialize metadata: {e}")))?;

        let mut pending_file = File::create(&pending_metadata_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to create pending metadata {pending_metadata_path:?}: {e}"
            ))
        })?;

        pending_file
            .write_all(updated_json.as_bytes())
            .await
            .map_err(|e| CdcError::generic(format!("Failed to write metadata: {e}")))?;

        pending_file
            .flush()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to flush metadata: {e}")))?;

        // Remove metadata from sql_received_tx/ only after successful write
        fs::remove_file(&received_metadata_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to remove received metadata {received_metadata_path:?}: {e}"
                ))
            })?;

        info!(
            "Committed transaction {}: moved metadata to sql_pending_tx/ (LSN: {:?}), data stays in sql_data_tx/",
            tx_id, commit_lsn
        );

        Ok(pending_metadata_path)
    }

    /// Delete transaction files (metadata and data) on abort
    pub async fn abort_transaction(&self, tx_id: u32, _timestamp: DateTime<Utc>) -> Result<()> {
        let received_metadata_path = self.get_received_tx_path(tx_id);
        let data_file_path = self.get_data_file_path(tx_id);

        // Read metadata to get data file path (in case it's different)
        let actual_data_path = if received_metadata_path.exists() {
            if let Ok(metadata_content) = fs::read_to_string(&received_metadata_path).await {
                if let Ok(metadata) =
                    serde_json::from_str::<TransactionFileMetadata>(&metadata_content)
                {
                    metadata.data_file_path
                } else {
                    data_file_path.clone()
                }
            } else {
                data_file_path.clone()
            }
        } else {
            data_file_path
        };

        // Remove buffered writer (discard any pending writes)
        {
            let mut writers = self.active_writers.lock().await;
            writers.remove(&actual_data_path);
        }

        // Delete metadata file
        if received_metadata_path.exists() {
            fs::remove_file(&received_metadata_path).await?;
            debug!("Deleted metadata file: {:?}", received_metadata_path);
        }

        // Delete data file
        if actual_data_path.exists() {
            fs::remove_file(&actual_data_path).await?;
            debug!("Deleted data file: {:?}", actual_data_path);
        }

        info!(
            "Aborted transaction {}, deleted metadata and data files",
            tx_id
        );
        Ok(())
    }

    /// List all pending transaction files ordered by commit timestamp
    pub async fn list_pending_transactions(&self) -> Result<Vec<PendingTransactionFile>> {
        let pending_dir = self.base_path.join(PENDING_TX_DIR);
        let mut entries = fs::read_dir(&pending_dir).await?;
        let mut files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Read .meta files from sql_pending_tx/
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                if let Ok(metadata) = self.read_metadata(&path).await {
                    files.push(PendingTransactionFile {
                        file_path: path,
                        metadata,
                    });
                }
            }
        }

        // Sort by commit timestamp
        files.sort_by_key(|f| f.metadata.commit_timestamp);

        Ok(files)
    }

    /// List all incomplete (received but not committed) transactions from sql_received_tx/
    ///
    /// Returns a HashMap mapping transaction_id -> (data_file_path, timestamp, transaction_type)
    /// This is used by the producer to restore its active_tx_files state on restart
    pub async fn list_received_transactions(
        &self,
    ) -> Result<HashMap<u32, (PathBuf, DateTime<Utc>, String)>> {
        let received_dir = self.base_path.join(RECEIVED_TX_DIR);
        let mut entries = fs::read_dir(&received_dir).await?;

        let mut metas = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                let meta = fs::metadata(&path).await?;
                let time = meta.created().or_else(|_| meta.modified())?;
                metas.push((path, DateTime::<Utc>::from(time)));
            }
        }

        metas.sort_by_key(|(_, t)| *t);

        let mut active_txs = HashMap::new();

        for (path, _file_time) in metas {
            if let Ok(metadata) = self.read_metadata(&path).await {
                active_txs.insert(
                    metadata.transaction_id,
                    (
                        metadata.data_file_path.clone(),
                        metadata.commit_timestamp,
                        metadata.transaction_type.clone(),
                    ),
                );
            }
        }

        Ok(active_txs)
    }

    /// Read metadata from a transaction metadata file (.meta)
    async fn read_metadata(&self, file_path: &Path) -> Result<TransactionFileMetadata> {
        let metadata_content = fs::read_to_string(file_path).await?;
        let metadata: TransactionFileMetadata = serde_json::from_str(&metadata_content)?;
        Ok(metadata)
    }

    /// Read SQL commands starting from a specific command index
    ///
    /// This method uses streaming parsing to efficiently handle large transaction files
    /// without loading the entire file into memory.
    ///
    /// Performance characteristics:
    /// - Memory: O(k) where k is the size of statements being collected (after start_index)
    /// - Time: O(n) where n is file size in bytes
    /// - Properly handles multi-line SQL statements with quote escaping
    /// - Can handle files of any size without memory issues
    ///
    /// # Arguments
    /// * `metadata_file_path` - Path to the metadata file in sql_pending_tx/
    /// * `start_index` - Zero-based index of the first command to return (0 = start from beginning)
    ///
    /// # Returns
    /// Returns a vector of SQL commands starting from start_index. If start_index >= total commands,
    /// returns an empty vector.
    ///
    /// # Example
    /// ```ignore
    /// // Resume from command 100 (skip first 100 commands that were already executed)
    /// let remaining_commands = manager.read_sql_commands_from_index(&file_path, 100).await?;
    /// ```
    pub async fn read_sql_commands_from_index(
        &self,
        metadata_file_path: &Path,
        start_index: usize,
    ) -> Result<Vec<String>> {
        // Read metadata to get the data file path
        let metadata = self.read_metadata(metadata_file_path).await?;
        let data_file_path = &metadata.data_file_path;

        debug!(
            "Reading SQL commands from data file: {:?} (referenced by metadata: {:?}), starting from index {}",
            data_file_path, metadata_file_path, start_index
        );

        // Check if file is compressed
        if is_compressed_file(data_file_path) {
            return self.read_compressed_file(data_file_path, start_index).await;
        }

        // Use streaming parser for uncompressed files
        let mut parser = SqlStreamParser::new();
        let cancel_token = CancellationToken::new();

        let collected_statements = parser
            .parse_file_from_index_collect(&data_file_path, start_index, &cancel_token)
            .await?;

        debug!(
            "Read {} SQL statements from {:?} (starting from index {})",
            collected_statements.len(),
            data_file_path,
            start_index
        );

        Ok(collected_statements)
    }

    /// Read commands from a compressed SQL file with streaming decompression
    ///
    /// Uses compression with sync points for efficient seeking in large files.
    /// Falls back to full streaming decompression for v1 files without index.
    ///
    /// Memory usage: O(buffer_size + statements_since_start_index)
    /// instead of O(entire_uncompressed_file_size)
    async fn read_compressed_file(
        &self,
        data_file_path: &Path,
        start_index: usize,
    ) -> Result<Vec<String>> {
        debug!(
            "Reading compressed file {:?} from statement index {}",
            data_file_path, start_index
        );

        let cancel_token = CancellationToken::new();

        // Use compression reader which automatically detects and uses sync points
        let statements = sql_compression::read_compressed_file_with_seeking(
            data_file_path,
            start_index,
            &cancel_token,
        )
        .await?;

        debug!(
            "Read {} SQL statements from compressed file {:?} (starting from index {})",
            statements.len(),
            data_file_path,
            start_index
        );

        Ok(statements)
    }

    /// Delete a pending transaction (metadata and data files) after successful execution
    pub async fn delete_pending_transaction(&self, metadata_file_path: &Path) -> Result<()> {
        // Read metadata to get data file path
        let metadata = self.read_metadata(metadata_file_path).await?;
        let data_file_path = &metadata.data_file_path;

        // Delete metadata file from sql_pending_tx/
        if data_file_path.exists() {
            fs::remove_file(metadata_file_path).await?;
        }

        // Delete data file from sql_data_tx/
        if data_file_path.exists() {
            fs::remove_file(data_file_path).await?;
        }

        info!(
            "Deleted executed transaction files: metadata={:?}, data={:?}",
            metadata_file_path, data_file_path
        );
        Ok(())
    }

    /// Generate SQL command for a change event
    fn generate_sql_for_event(&self, event: &ChangeEvent) -> Result<String> {
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => self.generate_insert_sql(schema, table, data),
            EventType::Update {
                schema,
                table,
                old_data,
                new_data,
                replica_identity,
                key_columns,
                ..
            } => self.generate_update_sql(
                schema,
                table,
                new_data,
                old_data,
                replica_identity,
                key_columns,
            ),
            EventType::Delete {
                schema,
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => self.generate_delete_sql(schema, table, old_data, replica_identity, key_columns),
            EventType::Truncate(tables) => self.generate_truncate_sql(tables),
            _ => {
                // Skip non-DML events
                Ok(String::new())
            }
        }
    }

    /// Generate INSERT SQL command
    fn generate_insert_sql(
        &self,
        schema: &str,
        table: &str,
        new_data: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let columns: Vec<String> = new_data.keys().cloned().collect();
        let values: Vec<String> = columns
            .iter()
            .map(|col| self.format_value(&new_data[col]))
            .collect();

        let sql = match self.destination_type {
            DestinationType::MySQL => {
                format!(
                    "INSERT INTO `{}`.`{}` ({}) VALUES ({});",
                    schema,
                    table,
                    columns
                        .iter()
                        .map(|c| format!("`{c}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                )
            }
            DestinationType::SqlServer => {
                format!(
                    "INSERT INTO [{}].[{}] ({}) VALUES ({});",
                    schema,
                    table,
                    columns
                        .iter()
                        .map(|c| format!("[{c}]"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                )
            }
            DestinationType::SQLite => {
                format!(
                    "INSERT INTO \"{}\" ({}) VALUES ({});",
                    table,
                    columns
                        .iter()
                        .map(|c| format!("\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                )
            }
        };

        Ok(sql)
    }

    /// Generate UPDATE SQL command
    fn generate_update_sql(
        &self,
        schema: &str,
        table: &str,
        new_data: &HashMap<String, serde_json::Value>,
        old_data: &Option<HashMap<String, serde_json::Value>>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let set_clause: Vec<String> = new_data
            .iter()
            .map(|(col, val)| {
                let formatted_col = match self.destination_type {
                    DestinationType::MySQL => format!("`{col}`"),
                    DestinationType::SqlServer => format!("[{col}]"),
                    DestinationType::SQLite => format!("\"{col}\""),
                };
                format!("{} = {}", formatted_col, self.format_value(val))
            })
            .collect();

        // Build WHERE clause based on replica identity
        let where_clause =
            self.build_where_clause(replica_identity, key_columns, old_data, new_data)?;

        let sql = match self.destination_type {
            DestinationType::MySQL => {
                format!(
                    "UPDATE `{}`.`{}` SET {} WHERE {};",
                    schema,
                    table,
                    set_clause.join(", "),
                    where_clause
                )
            }
            DestinationType::SqlServer => {
                format!(
                    "UPDATE [{}].[{}] SET {} WHERE {};",
                    schema,
                    table,
                    set_clause.join(", "),
                    where_clause
                )
            }
            DestinationType::SQLite => {
                format!(
                    "UPDATE \"{}\" SET {} WHERE {};",
                    table,
                    set_clause.join(", "),
                    where_clause
                )
            }
        };

        Ok(sql)
    }

    /// Generate DELETE SQL command
    fn generate_delete_sql(
        &self,
        schema: &str,
        table: &str,
        old_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let where_clause = self.build_where_clause(
            replica_identity,
            key_columns,
            &Some(old_data.clone()),
            old_data,
        )?;

        let sql = match self.destination_type {
            DestinationType::MySQL => {
                format!("DELETE FROM `{schema}`.`{table}` WHERE {where_clause};")
            }
            DestinationType::SqlServer => {
                format!("DELETE FROM [{schema}].[{table}] WHERE {where_clause};")
            }
            DestinationType::SQLite => {
                format!("DELETE FROM \"{table}\" WHERE {where_clause};")
            }
        };

        Ok(sql)
    }

    /// Generate TRUNCATE SQL command
    fn generate_truncate_sql(&self, tables: &[String]) -> Result<String> {
        // For simplicity, generate a TRUNCATE statement for each table
        let mut sqls = Vec::new();

        for table_spec in tables {
            // table_spec might include schema, parse it
            let parts: Vec<&str> = table_spec.split('.').collect();
            let (schema, table) = if parts.len() == 2 {
                (self.map_schema(Some(parts[0])), parts[1])
            } else {
                (self.map_schema(Some("public")), table_spec.as_str())
            };

            let sql = match self.destination_type {
                DestinationType::MySQL => {
                    format!("TRUNCATE TABLE `{schema}`.`{table}`;")
                }
                DestinationType::SqlServer => {
                    format!("TRUNCATE TABLE [{schema}].[{table}];")
                }
                DestinationType::SQLite => {
                    // SQLite doesn't have TRUNCATE, use DELETE
                    format!("DELETE FROM \"{table}\";")
                }
            };
            sqls.push(sql);
        }

        Ok(sqls.join("\n"))
    }

    /// Build WHERE clause for UPDATE/DELETE based on replica identity
    fn build_where_clause(
        &self,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let conditions: Vec<String> = match replica_identity {
            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                // Use key columns
                let data = old_data.as_ref().unwrap_or(new_data);
                key_columns
                    .iter()
                    .map(|col| {
                        let val = data.get(col).ok_or_else(|| {
                            CdcError::Generic(format!("Key column {col} not found"))
                        })?;
                        let formatted_col = match self.destination_type {
                            DestinationType::MySQL => format!("`{col}`"),
                            DestinationType::SqlServer => format!("[{col}]"),
                            DestinationType::SQLite => format!("\"{col}\""),
                        };
                        Ok(format!("{} = {}", formatted_col, self.format_value(val)))
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            ReplicaIdentity::Full => {
                // Use all columns from old data
                let data = old_data.as_ref().ok_or_else(|| {
                    CdcError::Generic("FULL replica identity requires old data".to_string())
                })?;
                data.iter()
                    .map(|(col, val)| {
                        let formatted_col = match self.destination_type {
                            DestinationType::MySQL => format!("`{col}`"),
                            DestinationType::SqlServer => format!("[{col}]"),
                            DestinationType::SQLite => format!("\"{col}\""),
                        };
                        if val.is_null() {
                            format!("{formatted_col} IS NULL")
                        } else {
                            format!("{} = {}", formatted_col, self.format_value(val))
                        }
                    })
                    .collect()
            }
            ReplicaIdentity::Nothing => {
                return Err(CdcError::Generic(
                    "Cannot generate WHERE clause with NOTHING replica identity".to_string(),
                ));
            }
        };

        Ok(conditions.join(" AND "))
    }

    /// Format a JSON value as SQL literal
    fn format_value(&self, value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => {
                if *b {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => {
                // Escape single quotes by doubling them (SQL standard)
                let escaped = s.replace('\'', "''");
                format!("'{escaped}'")
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                // For complex types, serialize as JSON string
                let json_str = value.to_string().replace('\'', "''");
                format!("'{json_str}'")
            }
        }
    }

    /// Map source schema to destination schema
    fn map_schema(&self, source_schema: Option<&str>) -> String {
        if let Some(schema) = source_schema {
            self.schema_mappings
                .get(schema)
                .cloned()
                .unwrap_or_else(|| schema.to_string())
        } else {
            "public".to_string()
        }
    }
}
