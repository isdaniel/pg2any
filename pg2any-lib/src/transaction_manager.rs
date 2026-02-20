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

use crate::destinations::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::storage::{CompressionIndex, SqlStreamParser, StorageFactory, TransactionStorage};
use crate::types::{ChangeEvent, DestinationType, EventType, Lsn, ReplicaIdentity, RowData};
use async_compression::tokio::bufread::GzipDecoder;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{self, File};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const MB: usize = 1024 * 1024;
const RECEIVED_TX_DIR: &str = "sql_received_tx";
const PENDING_TX_DIR: &str = "sql_pending_tx";
const DATA_TX_DIR: &str = "sql_data_tx";
/// Default buffer size for event accumulation (8MB)
const DEFAULT_BUFFER_SIZE: usize = 8 * MB;
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
    /// Statement counts per segment (aligned with `segments`)
    segment_statement_counts: Vec<usize>,
    /// Index of the current segment being written
    current_segment_index: usize,
    /// Current segment size on disk (bytes)
    current_segment_size_bytes: usize,
    /// Buffered writer for the current segment
    writer: BufferedEventWriter,
}

struct StatementProcessingState<'a> {
    batch: &'a mut Vec<String>,
    current_command_index: &'a mut usize,
    processed_count: &'a mut usize,
    batch_count: &'a mut usize,
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
    active_transactions: Arc<Mutex<HashMap<u32, ActiveTransactionState>>>,
    /// Staged progress updates for pending metadata (persisted on shutdown)
    staged_pending_progress: Arc<Mutex<HashMap<PathBuf, PendingProgress>>>,
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
            "Transaction file manager initialized at {:?} for {:?}, segment_size_bytes={:?}",
            base_path, destination_type, segment_size_bytes
        );

        Ok(Self {
            base_path,
            destination_type,
            schema_mappings: HashMap::new(),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            staged_pending_progress: Arc::new(Mutex::new(HashMap::new())),
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

    /// Get the file path for a specific segment of a transaction
    /// Segment index is 0-based but file names are 1-based (txid_000001.sql)
    fn get_segment_data_file_path(&self, tx_id: u32, segment_index: usize) -> PathBuf {
        let filename = format!("{}_{:06}.sql", tx_id, segment_index + 1);
        self.base_path.join(DATA_TX_DIR).join(filename)
    }

    fn get_final_segment_info(
        &self,
        tx_id: u32,
        metadata: &TransactionFileMetadata,
        tx_state: Option<&ActiveTransactionState>,
    ) -> (Vec<PathBuf>, Vec<usize>) {
        if let Some(state) = tx_state.filter(|state| !state.segments.is_empty()) {
            (
                state.segments.clone(),
                state.segment_statement_counts.clone(),
            )
        } else if !metadata.segments.is_empty() {
            let paths = metadata
                .segments
                .iter()
                .map(|seg| seg.path.clone())
                .collect::<Vec<_>>();
            let counts = metadata
                .segments
                .iter()
                .map(|seg| seg.statement_count)
                .collect::<Vec<_>>();
            (paths, counts)
        } else {
            (vec![self.get_segment_data_file_path(tx_id, 0)], vec![0])
        }
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
                segment_statement_counts: vec![0],
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
            tx_state.segment_statement_counts.push(0);
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
        if let Some(count) = tx_state
            .segment_statement_counts
            .get_mut(tx_state.current_segment_index)
        {
            *count += 1;
        }
        if should_flush {
            let buffered_bytes = tx_state.writer.buffer_size();
            tx_state.writer.flush().await?;
            tx_state.current_segment_size_bytes += buffered_bytes;
        }

        Ok(())
    }

    async fn read_received_metadata(
        &self,
        received_metadata_path: &Path,
    ) -> Result<TransactionFileMetadata> {
        let metadata_content = fs::read_to_string(received_metadata_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to read metadata from {received_metadata_path:?}: {e}"
                ))
            })?;

        serde_json::from_str(&metadata_content)
            .map_err(|e| CdcError::generic(format!("Failed to parse metadata: {e}")))
    }

    async fn take_and_flush_active_transaction(
        &self,
        tx_id: u32,
    ) -> Result<Option<ActiveTransactionState>> {
        let mut tx_state = {
            let mut transactions = self.active_transactions.lock().await;
            transactions.remove(&tx_id)
        };

        if let Some(state) = tx_state.as_mut() {
            let buffered_bytes = state.writer.buffer_size();
            state.writer.flush().await?;
            if buffered_bytes > 0 {
                debug!(
                    "Flushed final buffer for transaction {} ({} bytes)",
                    tx_id, buffered_bytes
                );
            }
        }

        Ok(tx_state)
    }

    async fn build_final_segments(
        &self,
        segment_paths: &[PathBuf],
        segment_counts: &[usize],
    ) -> Result<Vec<TransactionSegment>> {
        let mut final_segments = Vec::new();

        for (idx, segment_path) in segment_paths.iter().enumerate() {
            let (final_data_path, statement_count) = self
                .storage
                .write_transaction_from_file(segment_path)
                .await?;

            let fallback_count = segment_counts.get(idx).copied().unwrap_or(0);
            let final_count = if statement_count == 0 {
                fallback_count
            } else {
                statement_count
            };

            final_segments.push(TransactionSegment {
                path: final_data_path,
                statement_count: final_count,
            });
        }

        Ok(final_segments)
    }

    fn apply_commit_metadata(
        &self,
        metadata: &mut TransactionFileMetadata,
        final_segments: Vec<TransactionSegment>,
        commit_lsn: Option<Lsn>,
    ) {
        metadata.segments = final_segments;
        metadata.current_segment_index = 0;
        metadata.last_executed_command_index = None;
        metadata.last_update_timestamp = None;
        metadata.commit_lsn = commit_lsn;
    }

    async fn write_pending_metadata_file(
        &self,
        pending_metadata_path: &Path,
        metadata: &TransactionFileMetadata,
    ) -> Result<()> {
        let updated_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| CdcError::generic(format!("Failed to serialize metadata: {e}")))?;

        let mut pending_file = File::create(pending_metadata_path).await.map_err(|e| {
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

        Ok(())
    }

    async fn remove_received_metadata(&self, received_metadata_path: &Path) -> Result<()> {
        fs::remove_file(received_metadata_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to remove received metadata {received_metadata_path:?}: {e}"
            ))
        })
    }

    /// Move metadata from sql_received_tx to sql_pending_tx
    /// Flushes any pending buffered events before marking transaction as committed
    pub async fn commit_transaction(&self, tx_id: u32, commit_lsn: Option<Lsn>) -> Result<PathBuf> {
        let received_metadata_path = self.get_received_tx_path(tx_id);
        let pending_metadata_path = self.get_pending_tx_path(tx_id);

        let mut metadata = self.read_received_metadata(&received_metadata_path).await?;
        let tx_state = self.take_and_flush_active_transaction(tx_id).await?;

        let (segment_paths, segment_counts) =
            self.get_final_segment_info(tx_id, &metadata, tx_state.as_ref());

        if segment_paths.is_empty() {
            return Err(CdcError::generic(format!(
                "No transaction segments found for tx {}",
                tx_id
            )));
        }

        let final_segments = self
            .build_final_segments(&segment_paths, &segment_counts)
            .await?;

        self.apply_commit_metadata(&mut metadata, final_segments, commit_lsn);
        self.write_pending_metadata_file(&pending_metadata_path, &metadata)
            .await?;
        self.remove_received_metadata(&received_metadata_path)
            .await?;

        info!(
            "Committed transaction {}: moved metadata to sql_pending_tx/ (LSN: {:?}), data stays in sql_data_tx/",
            tx_id, commit_lsn
        );

        Ok(pending_metadata_path)
    }

    /// Delete transaction files (metadata and data) on abort
    pub async fn abort_transaction(&self, tx_id: u32, _timestamp: DateTime<Utc>) -> Result<()> {
        let received_metadata_path = self.get_received_tx_path(tx_id);
        let first_segment_path = self.get_segment_data_file_path(tx_id, 0);

        let segment_paths = 'paths: {
            if tokio::fs::metadata(&received_metadata_path).await.is_err() {
                break 'paths vec![first_segment_path.clone()];
            }

            let Ok(metadata_content) = fs::read_to_string(&received_metadata_path).await else {
                break 'paths vec![first_segment_path.clone()];
            };

            let Ok(metadata) = serde_json::from_str::<TransactionFileMetadata>(&metadata_content)
            else {
                break 'paths vec![first_segment_path.clone()];
            };

            if metadata.segments.is_empty() {
                break 'paths vec![first_segment_path.clone()];
            }

            metadata
                .segments
                .into_iter()
                .map(|seg| seg.path)
                .collect::<Vec<_>>()
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

        // Delete data files
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
            vec![self.get_segment_data_file_path(metadata.transaction_id, 0)]
        };

        let segment_statement_counts = if !metadata.segments.is_empty() {
            metadata
                .segments
                .iter()
                .map(|seg| seg.statement_count)
                .collect::<Vec<_>>()
        } else {
            vec![0]
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
                segment_statement_counts,
                current_segment_index,
                current_segment_size_bytes: current_size,
                writer: BufferedEventWriter::new(current_segment_path, self.buffer_size),
            },
        );

        Ok(())
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
            vec![self.get_segment_data_file_path(metadata.transaction_id, 0)]
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
                old_data.as_ref(),
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
    ///
    /// Accepts `&RowData` directly. Converts to HashMap internally because
    /// column iteration is required (RowData does not yet expose `iter()`).
    fn generate_insert_sql(&self, schema: &str, table: &str, new_data: &RowData) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let data_map = new_data.clone().into_hash_map();
        let columns: Vec<String> = data_map.keys().cloned().collect();
        let values: Vec<String> = columns
            .iter()
            .map(|col| self.format_value(&data_map[col]))
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
        new_data: &RowData,
        old_data: Option<&RowData>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[Arc<str>],
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        // Convert to HashMap for SET clause iteration (RowData lacks iter())
        let new_data_map = new_data.clone().into_hash_map();
        let set_clause: Vec<String> = new_data_map
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

        // Build WHERE clause — uses RowData::get() directly for Default/Index identity
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
    ///
    /// Accepts `&RowData` directly. For Default/Index replica identity (the most
    /// common case), this avoids cloning RowData entirely — only `RowData::get()`
    /// lookups are performed, with zero allocations.
    fn generate_delete_sql(
        &self,
        schema: &str,
        table: &str,
        old_data: &RowData,
        replica_identity: &ReplicaIdentity,
        key_columns: &[Arc<str>],
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let where_clause =
            self.build_where_clause(replica_identity, key_columns, Some(old_data), old_data)?;

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
    fn generate_truncate_sql(&self, tables: &[Arc<str>]) -> Result<String> {
        // For simplicity, generate a TRUNCATE statement for each table
        let mut sqls = Vec::new();

        for table_spec in tables {
            let table_spec: &str = table_spec;
            // table_spec might include schema, parse it
            let parts: Vec<&str> = table_spec.split('.').collect();
            let (schema, table) = if parts.len() == 2 {
                (self.map_schema(Some(parts[0])), parts[1])
            } else {
                (self.map_schema(Some("public")), table_spec)
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
        key_columns: &[Arc<str>],
        old_data: Option<&RowData>,
        new_data: &RowData,
    ) -> Result<String> {
        let conditions: Vec<String> = match replica_identity {
            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                // Use key columns with direct RowData::get() — no HashMap needed
                let data = old_data.unwrap_or(new_data);
                key_columns
                    .iter()
                    .map(|col| {
                        let col_str: &str = col;
                        let val = data.get(col_str).ok_or_else(|| {
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
                // Use all columns from old data — must convert to HashMap for iteration
                let data = old_data.ok_or_else(|| {
                    CdcError::Generic("FULL replica identity requires old data".to_string())
                })?;
                let data_map = data.clone().into_hash_map();
                data_map
                    .iter()
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

impl TransactionManager {
    async fn execute_sql_batch(
        self: &Arc<Self>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metadata_path: &Path,
        commands: &[String],
        last_executed_index: usize,
        batch_idx: usize,
        metrics_collector: &Arc<MetricsCollector>,
    ) -> Result<()> {
        let batch_start_time = Instant::now();
        let metadata_path = metadata_path.to_path_buf();
        let metadata_path_for_log = metadata_path.clone();
        let file_manager_for_hook = self.clone();
        let staged_index = last_executed_index;

        let pre_commit_hook: Option<PreCommitHook> = Some(Box::new(move || {
            let metadata_path = metadata_path.clone();
            let file_manager_for_hook = file_manager_for_hook.clone();
            Box::pin(async move {
                file_manager_for_hook
                    .stage_pending_metadata_progress(&metadata_path, staged_index)
                    .await?;
                Ok(())
            })
        }));

        if let Err(e) = destination_handler
            .execute_sql_batch_with_hook(commands, pre_commit_hook)
            .await
        {
            error!(
                "Failed to execute SQL batch {} from file {}: {}",
                batch_idx,
                metadata_path_for_log.display(),
                e
            );
            metrics_collector.record_error("transaction_file_execution_failed", "consumer");

            info!(
                "Batch and checkpoint rolled back together, will retry from last committed position on restart"
            );

            return Err(e);
        }

        let batch_duration = batch_start_time.elapsed();
        debug!(
            "Successfully executed batch {} with {} commands in {:?}",
            batch_idx,
            commands.len(),
            batch_duration
        );

        Ok(())
    }

    async fn process_reader_statements<R>(
        self: &Arc<Self>,
        reader: R,
        initial_statement_index: usize,
        start_index: usize,
        pending_tx: &PendingTransactionFile,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        state: &mut StatementProcessingState<'_>,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let mut parser = SqlStreamParser::new();
        let mut statement_index = initial_statement_index;

        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to read line: {e}")))?
        {
            let statements = parser.parse_line(&line)?;
            for stmt in statements {
                if statement_index >= start_index {
                    state.batch.push(stmt);

                    if state.batch.len() >= batch_size {
                        if cancellation_token.is_cancelled() {
                            return Err(CdcError::cancelled(
                                "Transaction file processing cancelled by shutdown signal",
                            ));
                        }

                        let batch_len = state.batch.len();
                        let next_command_index = *state.current_command_index + batch_len;
                        let last_executed_index = next_command_index - 1;
                        *state.batch_count += 1;

                        self.execute_sql_batch(
                            destination_handler,
                            &pending_tx.file_path,
                            state.batch,
                            last_executed_index,
                            *state.batch_count,
                            metrics_collector,
                        )
                        .await?;

                        *state.current_command_index = next_command_index;
                        *state.processed_count += batch_len;
                        state.batch.clear();
                    }
                }

                statement_index += 1;
            }
        }

        if let Some(stmt) = parser.finish_statement() {
            if statement_index >= start_index {
                state.batch.push(stmt);
            }
        }

        Ok(())
    }

    async fn process_segment_statements(
        self: &Arc<Self>,
        segment_path: &Path,
        start_index: usize,
        pending_tx: &PendingTransactionFile,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        state: &mut StatementProcessingState<'_>,
    ) -> Result<()> {
        let is_compressed = segment_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("gz"))
            .unwrap_or(false);

        if !is_compressed {
            let file = tokio::fs::File::open(segment_path).await.map_err(|e| {
                CdcError::generic(format!("Failed to open SQL file {segment_path:?}: {e}"))
            })?;

            return self
                .process_reader_statements(
                    file,
                    0,
                    start_index,
                    pending_tx,
                    destination_handler,
                    cancellation_token,
                    metrics_collector,
                    batch_size,
                    state,
                )
                .await;
        }

        let index_path = segment_path.with_extension("sql.gz.idx");
        let mut initial_statement_index = 0usize;
        let mut start_offset = 0u64;

        if tokio::fs::metadata(&index_path).await.is_ok() {
            if let Ok(index) = CompressionIndex::load_from_file(&index_path).await {
                if let Some(sync_point) = index.find_sync_point_for_index(start_index) {
                    initial_statement_index = sync_point.statement_index;
                    start_offset = sync_point.compressed_offset;
                }
            }
        }

        let mut file = tokio::fs::File::open(segment_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to open compressed file {segment_path:?}: {e}"
            ))
        })?;

        if start_offset > 0 {
            file.seek(SeekFrom::Start(start_offset))
                .await
                .map_err(|e| {
                    CdcError::generic(format!(
                        "Failed to seek compressed file {segment_path:?}: {e}"
                    ))
                })?;
        }

        let buf_reader = BufReader::new(file);
        let mut decoder = GzipDecoder::new(buf_reader);
        decoder.multiple_members(true);

        self.process_reader_statements(
            decoder,
            initial_statement_index,
            start_index,
            pending_tx,
            destination_handler,
            cancellation_token,
            metrics_collector,
            batch_size,
            state,
        )
        .await
    }

    /// Process a single transaction file
    /// Reads SQL commands from the file, executes them via the destination handler in batches,
    /// updates LSN tracking, and deletes the file upon success.
    /// This method supports resumable processing: it tracks the position after each batch
    /// and can resume from where it left off if interrupted.
    ///
    /// Checks cancellation token between batches to support graceful shutdown.
    pub(crate) async fn process_transaction_file(
        self: Arc<Self>,
        pending_tx: &PendingTransactionFile,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        lsn_tracker: &Arc<LsnTracker>,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    ) -> Result<()> {
        let start_time = Instant::now();
        let tx_id = pending_tx.metadata.transaction_id;

        let latest_metadata = self.read_metadata(&pending_tx.file_path).await?;
        let start_index = latest_metadata
            .last_executed_command_index
            .map(|idx| idx + 1)
            .unwrap_or(0);

        info!(
            "Processing transaction file: {} (tx_id: {}, lsn: {:?}, start_index: {})",
            pending_tx.file_path.display(),
            tx_id,
            pending_tx.metadata.commit_lsn,
            start_index
        );

        let mut segments = if !latest_metadata.segments.is_empty() {
            latest_metadata.segments.clone()
        } else {
            pending_tx.metadata.segments.clone()
        };

        if segments.is_empty() {
            return Err(CdcError::generic(format!(
                "No transaction segments found for tx {}",
                tx_id
            )));
        }

        let mut batch: Vec<String> = Vec::with_capacity(batch_size);
        let mut batch_count = 0usize;
        let mut processed_count = 0usize;
        let mut current_command_index = start_index;
        let mut remaining_start_index = start_index;

        let mut state = StatementProcessingState {
            batch: &mut batch,
            current_command_index: &mut current_command_index,
            processed_count: &mut processed_count,
            batch_count: &mut batch_count,
        };

        for segment in segments.drain(..) {
            if remaining_start_index > 0 && segment.statement_count > 0 {
                if remaining_start_index >= segment.statement_count {
                    remaining_start_index -= segment.statement_count;
                    continue;
                }
            }

            let segment_start_index = remaining_start_index;
            remaining_start_index = 0;

            let stream_result = self
                .process_segment_statements(
                    &segment.path,
                    segment_start_index,
                    pending_tx,
                    destination_handler,
                    cancellation_token,
                    metrics_collector,
                    batch_size,
                    &mut state,
                )
                .await;

            if let Err(e) = stream_result {
                if e.is_cancelled() {
                    warn!(
                        "Transaction file processing cancelled by shutdown signal (tx_id: {})",
                        tx_id
                    );
                    return Ok(());
                }

                return Err(e);
            }
        }

        if !batch.is_empty() {
            if cancellation_token.is_cancelled() {
                warn!(
                    "Transaction file processing cancelled by shutdown signal (tx_id: {})",
                    tx_id
                );
                return Ok(());
            }

            let batch_len = batch.len();
            let next_command_index = current_command_index + batch_len;
            let last_executed_index = next_command_index - 1;
            batch_count += 1;

            self.execute_sql_batch(
                destination_handler,
                &pending_tx.file_path,
                &batch,
                last_executed_index,
                batch_count,
                metrics_collector,
            )
            .await?;

            current_command_index = next_command_index;
            processed_count += batch_len;
            batch.clear();
        }

        let total_commands = current_command_index;

        if processed_count == 0 {
            info!(
                "All commands already executed for transaction file: {} (tx_id: {})",
                pending_tx.file_path.display(),
                tx_id
            );

            self.finalize_transaction_file(
                pending_tx,
                lsn_tracker,
                metrics_collector,
                total_commands,
                shared_lsn_feedback,
            )
            .await?;

            return Ok(());
        }

        let duration = start_time.elapsed();
        info!(
            "Successfully executed {} remaining commands ({} total) in {} batches in {:?} (tx_id: {}, avg: {:?}/batch)",
            processed_count,
            total_commands,
            batch_count,
            duration,
            tx_id,
            duration / batch_count.max(1) as u32
        );

        // Finalize: update LSN, record metrics, delete file
        self.finalize_transaction_file(
            pending_tx,
            lsn_tracker,
            metrics_collector,
            total_commands,
            shared_lsn_feedback,
        )
        .await?;

        Ok(())
    }

    /// Core logic for finalizing transaction file processing
    ///
    /// PROTOCOL COMPLIANCE - ACK AFTER APPLY:
    /// This function is called ONLY after successful execution of all SQL commands.
    /// It updates confirmed_flush_lsn and sends ACK to PostgreSQL.
    /// This ensures we never ACK a transaction that hasn't been successfully applied.
    async fn finalize_transaction_file(
        self: &Arc<Self>,
        pending_tx: &PendingTransactionFile,
        lsn_tracker: &Arc<LsnTracker>,
        metrics_collector: &Arc<MetricsCollector>,
        total_commands: usize,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    ) -> Result<()> {
        let tx_id = pending_tx.metadata.transaction_id;

        // PROTOCOL COMPLIANCE: Update LSN and send ACK ONLY after successful apply
        if let Some(commit_lsn) = pending_tx.metadata.commit_lsn {
            info!(
                "Transaction {} successfully applied to destination, commit_lsn: {}",
                tx_id, commit_lsn
            );

            // 1. Update confirmed_flush_lsn (last successfully applied LSN)
            lsn_tracker.commit_lsn(commit_lsn.0);

            // 2. Update apply_lsn - transaction is now applied to destination
            // (flush_lsn was already updated by producer when file was persisted)
            shared_lsn_feedback.update_applied_lsn(commit_lsn.0);

            info!(
                "Updated apply_lsn to {} (transaction {} applied to destination)",
                commit_lsn, tx_id
            );
        } else {
            warn!(
                "Transaction {} has no commit_lsn, cannot send ACK (this should not happen for committed transactions)",
                tx_id
            );
        }

        // Record metrics - create a transaction object for metrics recording
        let destination_type_str = pending_tx.metadata.destination_type.to_string();

        // Create a transaction object for metrics (events are already executed, so we use empty vec)
        // The event_count is derived from the number of SQL commands executed
        let mut transaction = crate::types::Transaction::new(
            pending_tx.metadata.transaction_id,
            pending_tx.metadata.commit_timestamp,
        );
        transaction.commit_lsn = pending_tx.metadata.commit_lsn;

        // Record transaction processed metrics
        metrics_collector.record_transaction_processed(&transaction, &destination_type_str);

        // Since file-based processing always processes complete transactions,
        // we also record this as a full transaction
        metrics_collector.record_full_transaction_processed(&transaction, &destination_type_str);

        debug!(
            "Successfully processed transaction file with {} commands and recorded metrics",
            total_commands
        );

        // Delete the file after successful processing
        if let Err(e) = self.delete_pending_transaction(&pending_tx.file_path).await {
            error!(
                "Failed to delete processed transaction file {}: {}",
                pending_tx.file_path.display(),
                e
            );
        }

        self.clear_staged_pending_progress(&pending_tx.file_path)
            .await;

        if pending_tx.metadata.commit_lsn.is_some() {
            let pending_count = self.list_pending_transactions().await?.len();
            lsn_tracker.update_consumer_state(
                tx_id,
                pending_tx.metadata.commit_timestamp,
                pending_count,
            );

            debug!(
                "Updated LSN tracker consumer state: tx_id={}, pending_count={} (after deletion)",
                tx_id, pending_count
            );
        }

        Ok(())
    }
}
