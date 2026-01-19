//! Transaction File Manager for CDC persistence
//!
//! This module implements file-based transaction persistence to ensure data consistency
//! even when long-running transactions might be aborted before commit.
//!
//! ## Architecture
//!
//! - **sql_data_tx/**: Stores actual SQL data (append-only, never moved)
//!   - File naming: `{txid}_{seq}.sql` (e.g., `774_000001.sql`, `774_000002.sql`)
//!   - Events are appended as SQL commands as they arrive
//!   - Files are NOT moved or deleted until transaction is fully executed
//!
//! - **sql_received_tx/**: Stores metadata for in-progress transactions
//!   - File naming: `{txid}.meta`
//!   - Contains JSON metadata: transaction_id, timestamp, segments, current_segment_index
//!   - Small files (~100 bytes) for fast operations
//!
//! - **sql_pending_tx/**: Stores metadata for committed transactions ready for execution
//!   - File naming: `{txid}.meta`
//!   - Contains JSON metadata: transaction_id, timestamp, commit_lsn, segments
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
use crate::storage::sql_parser::SqlStreamParser;
use crate::storage::{StorageFactory, TransactionStorage};
use crate::types::{ChangeEvent, DestinationType, EventType, Lsn, ReplicaIdentity};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::{debug, info};

const MB: usize = 1024 * 1024;
const RECEIVED_TX_DIR: &str = "sql_received_tx";
const PENDING_TX_DIR: &str = "sql_pending_tx";
const DATA_TX_DIR: &str = "sql_data_tx";
/// Default buffer size for event accumulation (1MB)
const DEFAULT_BUFFER_SIZE: usize = 1 * MB;
/// Default max segment size before rotating to a new data file (64MB)
const DEFAULT_SEGMENT_SIZE_BYTES: usize = 64 * MB;
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

/// Transaction segment metadata stored in sql_received_tx/ and sql_pending_tx/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionSegment {
    /// Path to the SQL data file for this segment
    pub path: PathBuf,
    /// Number of SQL statements in this segment (0 means unknown/uncomputed)
    #[serde(default)]
    pub statement_count: usize,
}

/// Transaction file metadata stored in sql_received_tx/ and sql_pending_tx/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionFileMetadata {
    pub transaction_id: u32,
    pub commit_timestamp: DateTime<Utc>,
    pub commit_lsn: Option<Lsn>,
    pub destination_type: DestinationType,
    /// Current segment path
    pub data_file_path: PathBuf,
    /// Ordered list of transaction segments
    #[serde(default)]
    pub segments: Vec<TransactionSegment>,
    /// Index of the current segment in `segments`
    #[serde(default)]
    pub current_segment_index: usize,
    /// Index of the last successfully executed SQL command for this transaction, Commands are 0-indexed. None means no commands have been executed yet.
    #[serde(default)]
    pub last_executed_command_index: Option<usize>,
    /// Timestamp of the last persisted progress update (pending transactions only)
    #[serde(default)]
    pub last_update_timestamp: Option<DateTime<Utc>>,
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

struct ActiveTransactionState {
    /// Ordered list of segment paths for this transaction
    segments: Vec<PathBuf>,
    /// Index of the current segment being written
    current_segment_index: usize,
    /// Current segment size on disk (bytes)
    current_segment_size_bytes: usize,
    /// Buffered writer for the current segment
    writer: BufferedEventWriter,
}

#[derive(Debug, Clone)]
struct PendingProgress {
    last_executed_command_index: usize,
    last_update_timestamp: DateTime<Utc>,
}

/// Transaction File Manager for persisting and executing transactions
pub struct TransactionManager {
    base_path: PathBuf,
    destination_type: DestinationType,
    schema_mappings: HashMap<String, String>,
    /// Active transactions and their current segment writers
    /// Key: transaction ID
    active_transactions: Arc<tokio::sync::Mutex<HashMap<u32, ActiveTransactionState>>>,
    /// Staged progress updates for pending metadata (persisted on shutdown)
    staged_pending_progress: Arc<tokio::sync::Mutex<HashMap<PathBuf, PendingProgress>>>,
    /// Maximum buffer size before forced flush
    buffer_size: usize,
    /// Maximum segment size before rotating to a new file
    segment_size_bytes: usize,
    /// Storage implementation (compressed or uncompressed)
    storage: Arc<dyn TransactionStorage>,
}

impl TransactionManager {
    /// Create a new transaction file manager
    pub async fn new(
        base_path: impl AsRef<Path>,
        destination_type: DestinationType,
        segment_size_bytes: usize,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create directories if they don't exist
        let received_tx_dir = base_path.join(RECEIVED_TX_DIR);
        let pending_tx_dir = base_path.join(PENDING_TX_DIR);
        let data_tx_dir = base_path.join(DATA_TX_DIR);

        fs::create_dir_all(&received_tx_dir).await?;
        fs::create_dir_all(&pending_tx_dir).await?;
        fs::create_dir_all(&data_tx_dir).await?;

        // Create storage based on environment variable
        let storage = StorageFactory::from_env();

        info!(
            "Transaction file manager initialized at {:?} for {:?}",
            base_path, destination_type
        );
        info!("Three-directory structure: sql_data_tx/ (data), sql_received_tx/ (in-progress metadata), sql_pending_tx/ (committed metadata)");

        let segment_size_bytes = if segment_size_bytes == 0 {
            DEFAULT_SEGMENT_SIZE_BYTES
        } else {
            segment_size_bytes
        };

        Ok(Self {
            base_path,
            destination_type,
            schema_mappings: HashMap::new(),
            active_transactions: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            staged_pending_progress: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            buffer_size: DEFAULT_BUFFER_SIZE,
            segment_size_bytes,
            storage,
        })
    }

    /// Set schema mappings for SQL generation
    pub fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    /// Flush all pending buffered writes
    /// Called during graceful shutdown to ensure no data is lost
    pub async fn flush_all_buffers(&self) -> Result<()> {
        let mut transactions = self.active_transactions.lock().await;

        let mut flush_count = 0;
        let mut total_bytes = 0;

        for (_, tx_state) in transactions.iter_mut() {
            let buffer_size = tx_state.writer.buffer_size();
            if buffer_size > 0 {
                tx_state.writer.flush().await?;
                tx_state.current_segment_size_bytes += buffer_size;
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

    /// Get the file path for a specific segment of a transaction
    /// Segment index is 0-based but file names are 1-based (txid_000001.sql)
    fn get_segment_data_file_path(&self, tx_id: u32, segment_index: usize) -> PathBuf {
        let filename = format!("{}_{:06}.sql", tx_id, segment_index + 1);
        self.base_path.join(DATA_TX_DIR).join(filename)
    }

    /// Create a new transaction: data file in sql_data_tx/ and metadata in sql_received_tx/
    pub async fn begin_transaction(
        &self,
        tx_id: u32,
        timestamp: DateTime<Utc>,
        transaction_type: &str,
    ) -> Result<PathBuf> {
        let data_file_path = self.get_segment_data_file_path(tx_id, 0);
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
            segments: vec![TransactionSegment {
                path: data_file_path.clone(),
                statement_count: 0,
            }],
            current_segment_index: 0,
            last_executed_command_index: None,
            last_update_timestamp: None,
            transaction_type: transaction_type.to_string(),
        };

        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        let mut metadata_file = File::create(&metadata_path).await?;
        metadata_file.write_all(metadata_json.as_bytes()).await?;
        metadata_file.flush().await?;

        // Create a buffered writer for this transaction
        let mut transactions = self.active_transactions.lock().await;
        transactions.insert(
            tx_id,
            ActiveTransactionState {
                segments: vec![data_file_path.clone()],
                current_segment_index: 0,
                current_segment_size_bytes: 0,
                writer: BufferedEventWriter::new(data_file_path.clone(), self.buffer_size),
            },
        );

        info!(
            "Started transaction {}: data={:?}, metadata={:?}",
            tx_id, data_file_path, metadata_path
        );

        // Return the data file path for appending events
        Ok(data_file_path)
    }

    /// Update metadata for an in-progress transaction with segment info
    async fn update_received_metadata_segments(
        &self,
        tx_id: u32,
        segments: &[PathBuf],
        current_segment_index: usize,
    ) -> Result<()> {
        let received_metadata_path = self.get_received_tx_path(tx_id);

        let metadata_content = fs::read_to_string(&received_metadata_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to read metadata from {received_metadata_path:?}: {e}"
                ))
            })?;

        let mut metadata: TransactionFileMetadata = serde_json::from_str(&metadata_content)
            .map_err(|e| CdcError::generic(format!("Failed to parse metadata: {e}")))?;

        metadata.segments = segments
            .iter()
            .map(|path| TransactionSegment {
                path: path.clone(),
                statement_count: 0,
            })
            .collect();
        metadata.current_segment_index = current_segment_index;
        metadata.data_file_path = segments
            .get(current_segment_index)
            .cloned()
            .unwrap_or_else(|| self.get_data_file_path(tx_id));

        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| CdcError::generic(format!("Failed to serialize metadata: {e}")))?;

        let mut metadata_file = File::create(&received_metadata_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to create received metadata {received_metadata_path:?}: {e}"
            ))
        })?;

        metadata_file.write_all(metadata_json.as_bytes()).await?;
        metadata_file.flush().await?;

        Ok(())
    }

    /// Append a change event to a running transaction file
    /// Uses buffered I/O to accumulate events in memory before flushing to disk
    /// Automatically flushes when buffer reaches capacity
    pub async fn append_event(&self, tx_id: u32, event: &ChangeEvent) -> Result<()> {
        let sql = self.generate_sql_for_event(event)?;

        // Skip empty SQL (metadata events)
        if sql.is_empty() {
            return Ok(());
        }

        let mut transactions = self.active_transactions.lock().await;

        let tx_state = transactions.get_mut(&tx_id).ok_or_else(|| {
            CdcError::generic(format!(
                "Active transaction {} not found for append_event",
                tx_id
            ))
        })?;

        let sql_bytes = sql.as_bytes().len() + 1; // include newline
        let estimated_size =
            tx_state.current_segment_size_bytes + tx_state.writer.buffer_size() + sql_bytes;

        let should_rotate = estimated_size > self.segment_size_bytes
            && (tx_state.current_segment_size_bytes > 0 || tx_state.writer.buffer_size() > 0);

        if should_rotate {
            let buffered_bytes = tx_state.writer.buffer_size();
            tx_state.writer.flush().await?;
            tx_state.current_segment_size_bytes += buffered_bytes;

            let next_segment_index = tx_state.current_segment_index + 1;
            let next_segment_path = self.get_segment_data_file_path(tx_id, next_segment_index);

            File::create(&next_segment_path).await?;

            tx_state.segments.push(next_segment_path.clone());
            tx_state.current_segment_index = next_segment_index;
            tx_state.current_segment_size_bytes = 0;
            tx_state.writer = BufferedEventWriter::new(next_segment_path.clone(), self.buffer_size);

            self.update_received_metadata_segments(
                tx_id,
                &tx_state.segments,
                tx_state.current_segment_index,
            )
            .await?;

            info!(
                "Rotated transaction {} to new segment {:?} ({} segments total)",
                tx_id,
                next_segment_path,
                tx_state.segments.len()
            );
        }

        let should_flush = tx_state.writer.append(&sql);
        if should_flush {
            let buffered_bytes = tx_state.writer.buffer_size();
            tx_state.writer.flush().await?;
            tx_state.current_segment_size_bytes += buffered_bytes;
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

        let mut segments = if !metadata.segments.is_empty() {
            metadata.segments.clone()
        } else {
            vec![TransactionSegment {
                path: metadata.data_file_path.clone(),
                statement_count: 0,
            }]
        };

        if segments.is_empty() {
            return Err(CdcError::generic(format!(
                "No transaction segments found for tx {}",
                tx_id
            )));
        }

        // CRITICAL: Flush any pending buffered events before committing
        {
            let mut transactions = self.active_transactions.lock().await;
            if let Some(tx_state) = transactions.remove(&tx_id) {
                let buffered_bytes = tx_state.writer.buffer_size();
                let mut writer = tx_state.writer;
                writer.flush().await?;
                if buffered_bytes > 0 {
                    debug!(
                        "Flushed final buffer for transaction {} ({} bytes)",
                        tx_id, buffered_bytes
                    );
                }
            }
        }

        let mut parser = SqlStreamParser::new();
        let mut final_segments: Vec<TransactionSegment> = Vec::new();

        for segment in segments.drain(..) {
            let statements = parser
                .parse_file_from_index_collect(&segment.path, 0)
                .await?;

            debug!(
                "Read {} statements from transaction {} segment {:?} for storage processing",
                statements.len(),
                tx_id,
                segment.path
            );

            let final_data_path = self
                .storage
                .write_transaction(&segment.path, &statements)
                .await?;

            if final_data_path != segment.path {
                fs::remove_file(&segment.path).await.map_err(|e| {
                    CdcError::generic(format!("Failed to remove uncompressed file: {e}"))
                })?;
                debug!("Removed original uncompressed file: {:?}", segment.path);
            }

            final_segments.push(TransactionSegment {
                path: final_data_path,
                statement_count: statements.len(),
            });
        }

        metadata.segments = final_segments;
        metadata.current_segment_index = 0;
        metadata.data_file_path = metadata
            .segments
            .first()
            .map(|segment| segment.path.clone())
            .unwrap_or_else(|| self.get_data_file_path(tx_id));
        metadata.last_executed_command_index = None;
        metadata.last_update_timestamp = None;
        metadata.commit_lsn = commit_lsn;

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

        let segment_paths = if tokio::fs::metadata(&received_metadata_path).await.is_ok() {
            if let Ok(metadata_content) = fs::read_to_string(&received_metadata_path).await {
                if let Ok(metadata) =
                    serde_json::from_str::<TransactionFileMetadata>(&metadata_content)
                {
                    if !metadata.segments.is_empty() {
                        metadata
                            .segments
                            .into_iter()
                            .map(|seg| seg.path)
                            .collect::<Vec<_>>()
                    } else {
                        vec![metadata.data_file_path]
                    }
                } else {
                    vec![data_file_path.clone()]
                }
            } else {
                vec![data_file_path.clone()]
            }
        } else {
            vec![data_file_path]
        };

        // Remove buffered writer (discard any pending writes)
        {
            let mut transactions = self.active_transactions.lock().await;
            transactions.remove(&tx_id);
        }

        // Delete metadata file
        if tokio::fs::metadata(&received_metadata_path).await.is_ok() {
            fs::remove_file(&received_metadata_path).await?;
            debug!("Deleted metadata file: {:?}", received_metadata_path);
        }

        // Delete data files using storage trait
        for path in segment_paths {
            self.storage.delete_transaction(&path).await?;
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

    /// Restore incomplete (received but not committed) transactions from sql_received_tx/
    ///
    /// Returns a list of metadata entries ordered by timestamp and seeds active writers
    /// for the current segment so producers can continue appending after restart.
    pub async fn restore_received_transactions(&self) -> Result<Vec<TransactionFileMetadata>> {
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

        let mut active_txs = Vec::new();

        for (path, _file_time) in metas {
            if let Ok(metadata) = self.read_metadata(&path).await {
                self.restore_active_transaction(&metadata).await?;
                active_txs.push(metadata);
            }
        }

        Ok(active_txs)
    }

    /// Read metadata from a transaction metadata file (.meta)
    pub(crate) async fn read_metadata(&self, file_path: &Path) -> Result<TransactionFileMetadata> {
        let metadata_content = fs::read_to_string(file_path).await?;
        let metadata: TransactionFileMetadata = serde_json::from_str(&metadata_content)?;
        Ok(metadata)
    }

    async fn write_pending_metadata(
        &self,
        metadata_file_path: &Path,
        metadata: &TransactionFileMetadata,
    ) -> Result<()> {
        let metadata_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| CdcError::generic(format!("Failed to serialize metadata: {e}")))?;

        let temp_path = metadata_file_path.with_extension("meta.tmp");

        let mut metadata_file = File::create(&temp_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to create pending metadata {temp_path:?}: {e}"
            ))
        })?;

        metadata_file.write_all(metadata_json.as_bytes()).await?;
        metadata_file.flush().await?;

        fs::rename(&temp_path, metadata_file_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to replace pending metadata {metadata_file_path:?}: {e}"
                ))
            })?;

        Ok(())
    }

    /// Stage progress updates for a pending transaction (persisted on shutdown)
    pub async fn stage_pending_metadata_progress(
        &self,
        metadata_file_path: &Path,
        last_executed_command_index: usize,
    ) -> Result<()> {
        let mut staged = self.staged_pending_progress.lock().await;
        staged.insert(
            metadata_file_path.to_path_buf(),
            PendingProgress {
                last_executed_command_index,
                last_update_timestamp: Utc::now(),
            },
        );
        Ok(())
    }

    /// Flush any staged pending progress updates to disk
    pub async fn flush_staged_pending_progress(&self) -> Result<()> {
        let staged_entries = {
            let mut staged = self.staged_pending_progress.lock().await;
            if staged.is_empty() {
                return Ok(());
            }
            staged.drain().collect::<Vec<_>>()
        };

        let mut last_error: Option<CdcError> = None;

        for (metadata_path, progress) in staged_entries {
            if fs::metadata(&metadata_path).await.is_err() {
                continue;
            }

            match self.read_metadata(&metadata_path).await {
                Ok(mut metadata) => {
                    metadata.last_executed_command_index =
                        Some(progress.last_executed_command_index);
                    metadata.last_update_timestamp = Some(progress.last_update_timestamp);

                    if let Err(e) = self.write_pending_metadata(&metadata_path, &metadata).await {
                        last_error = Some(e);
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        if let Some(err) = last_error {
            return Err(err);
        }

        Ok(())
    }

    /// Remove any staged progress update for a pending transaction
    pub async fn clear_staged_pending_progress(&self, metadata_file_path: &Path) {
        let mut staged = self.staged_pending_progress.lock().await;
        staged.remove(metadata_file_path);
    }

    /// Restore an active transaction writer from received metadata
    async fn restore_active_transaction(&self, metadata: &TransactionFileMetadata) -> Result<()> {
        let segments = if !metadata.segments.is_empty() {
            metadata
                .segments
                .iter()
                .map(|seg| seg.path.clone())
                .collect::<Vec<_>>()
        } else {
            vec![metadata.data_file_path.clone()]
        };

        let mut current_segment_index = metadata.current_segment_index;
        if current_segment_index >= segments.len() {
            current_segment_index = segments.len() - 1;
        }

        let current_segment_path = segments[current_segment_index].clone();

        // Ensure the current segment file exists without truncating
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_segment_path)
            .await?;

        let current_size = match tokio::fs::metadata(&current_segment_path).await {
            Ok(meta) => meta.len() as usize,
            Err(_) => 0,
        };

        let mut transactions = self.active_transactions.lock().await;
        transactions.insert(
            metadata.transaction_id,
            ActiveTransactionState {
                segments,
                current_segment_index,
                current_segment_size_bytes: current_size,
                writer: BufferedEventWriter::new(current_segment_path, self.buffer_size),
            },
        );

        Ok(())
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
        let mut segments = if !metadata.segments.is_empty() {
            metadata.segments.clone()
        } else {
            vec![TransactionSegment {
                path: metadata.data_file_path.clone(),
                statement_count: 0,
            }]
        };

        if segments.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Reading SQL commands from {} segment(s) (metadata: {:?}), starting from index {}",
            segments.len(),
            metadata_file_path,
            start_index
        );

        let mut remaining_start_index = start_index;
        let mut all_statements: Vec<String> = Vec::new();

        for segment in segments.drain(..) {
            if remaining_start_index == 0 {
                let statements = self.storage.read_transaction(&segment.path, 0).await?;
                all_statements.extend(statements);
                continue;
            }

            if segment.statement_count > 0 {
                if remaining_start_index >= segment.statement_count {
                    remaining_start_index -= segment.statement_count;
                    continue;
                }

                let statements = self
                    .storage
                    .read_transaction(&segment.path, remaining_start_index)
                    .await?;
                all_statements.extend(statements);
                remaining_start_index = 0;
                continue;
            }

            let statements = self.storage.read_transaction(&segment.path, 0).await?;
            if remaining_start_index >= statements.len() {
                remaining_start_index -= statements.len();
                continue;
            }

            all_statements.extend(statements.into_iter().skip(remaining_start_index));
            remaining_start_index = 0;
        }

        debug!(
            "Read {} SQL statements across segments (starting from index {})",
            all_statements.len(),
            start_index
        );

        Ok(all_statements)
    }

    /// Delete a pending transaction (metadata and data files) after successful execution
    pub async fn delete_pending_transaction(&self, metadata_file_path: &Path) -> Result<()> {
        // Read metadata to get data file path
        let metadata = self.read_metadata(metadata_file_path).await?;
        let data_file_paths = if !metadata.segments.is_empty() {
            metadata
                .segments
                .iter()
                .map(|seg| seg.path.clone())
                .collect::<Vec<_>>()
        } else {
            vec![metadata.data_file_path.clone()]
        };

        // Delete metadata file from sql_pending_tx/
        if tokio::fs::metadata(metadata_file_path).await.is_ok() {
            fs::remove_file(metadata_file_path).await?;
        }

        // Delete data files using storage trait (handles both compressed and uncompressed)
        for path in data_file_paths.iter() {
            self.storage.delete_transaction(path).await?;
        }

        info!(
            "Deleted executed transaction files: metadata={:?}, data_files={:?}",
            metadata_file_path, data_file_paths
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
