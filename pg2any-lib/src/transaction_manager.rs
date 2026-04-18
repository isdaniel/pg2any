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
use pg_walstream::ColumnValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// Persistent writer opened lazily on first flush. Reusing the handle
    /// across flushes avoids an open() syscall per 8MB of WAL.
    writer: Option<BufWriter<File>>,
}

impl BufferedEventWriter {
    /// Create a new buffered writer for a transaction file
    fn new(file_path: PathBuf, max_buffer_size: usize) -> Self {
        Self {
            file_path,
            buffer: String::with_capacity(max_buffer_size),
            max_buffer_size,
            writer: None,
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

        if self.writer.is_none() {
            let file = fs::OpenOptions::new()
                .append(true)
                .open(&self.file_path)
                .await?;
            self.writer = Some(BufWriter::with_capacity(64 * 1024, file));
        }

        let writer = self.writer.as_mut().unwrap();
        writer.write_all(self.buffer.as_bytes()).await?;

        debug!(
            "Flushed {} bytes to {:?}",
            self.buffer.len(),
            self.file_path
        );

        self.buffer.clear();
        Ok(())
    }

    /// Flush buffer and fsync the underlying file to disk.
    ///
    /// Used at segment rotation and commit boundaries: after this returns the data
    /// is guaranteed durable across power loss / abrupt container exit. A plain
    /// `flush()` only pushes the userspace BufWriter into the kernel — without
    /// `sync_all()` the bytes can still be lost if the process is killed and the
    /// kernel hasn't written them back yet.
    async fn sync(&mut self) -> Result<()> {
        self.flush().await?;
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
            writer.get_ref().sync_all().await?;
        }
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

/// Transaction File Manager for persisting and executing transactions
pub struct TransactionManager {
    base_path: PathBuf,
    destination_type: DestinationType,
    schema_mappings: HashMap<String, String>,
    /// Active transactions and their current segment writers
    /// Key: transaction ID
    active_transactions: Arc<Mutex<HashMap<u32, ActiveTransactionState>>>,
    /// Maximum buffer size before forced flush
    buffer_size: usize,
    /// Maximum segment size before rotating to a new file
    segment_size_bytes: usize,
    /// Storage implementation (compressed or uncompressed)
    storage: Arc<dyn TransactionStorage>,
    /// Atomic counter for pending transaction files (avoids directory scans)
    pending_count: AtomicUsize,
    /// Pre-computed: whether to escape backslashes in string literals (MySQL only)
    escape_backslash: bool,
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

        // Count existing pending transactions for the atomic counter
        let initial_pending_count = Self::count_pending_dir(&pending_tx_dir).await;

        info!(
            "Transaction file manager initialized at {:?} for {:?}, segment_size_bytes={:?}, pending_count={}",
            base_path, destination_type, segment_size_bytes, initial_pending_count
        );

        Ok(Self {
            escape_backslash: matches!(destination_type, DestinationType::MySQL),
            base_path,
            destination_type,
            schema_mappings: HashMap::new(),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            buffer_size: DEFAULT_BUFFER_SIZE,
            segment_size_bytes,
            storage,
            pending_count: AtomicUsize::new(initial_pending_count),
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
                tx_state.writer.sync().await?;
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

        let metadata_json = serde_json::to_string(&metadata)?;
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

        debug!(
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

        let metadata_json = serde_json::to_string(&metadata)
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

        if sql.is_empty() {
            return Ok(());
        }

        // Single lock: check rotation AND append in one scope.
        // The common path (no rotation) completes entirely within this lock,
        // cutting lock acquisitions from 2 to 1 per event.
        {
            let mut transactions = self.active_transactions.lock().await;
            let tx_state = transactions.get_mut(&tx_id).ok_or_else(|| {
                CdcError::generic(format!(
                    "Active transaction {} not found for append_event",
                    tx_id
                ))
            })?;

            let sql_bytes = sql.len() + 1;
            let estimated_size =
                tx_state.current_segment_size_bytes + tx_state.writer.buffer_size() + sql_bytes;
            let needs_rotation = estimated_size > self.segment_size_bytes
                && (tx_state.current_segment_size_bytes > 0 || tx_state.writer.buffer_size() > 0);

            if !needs_rotation {
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
                return Ok(());
            }
        }

        // Rotation path (rare): hold the lock for the whole rotation so a fault during
        // I/O cannot lose the transaction state. The producer is single-threaded, so
        // holding the mutex through I/O does not introduce real contention; the upside
        // is that any early return via `?` leaves the state in `active_transactions`
        // (still pointing at the *old* writer/segment) instead of being silently
        // dropped, which would cause every subsequent event for this tx to fail with
        // "Active transaction not found".
        let mut transactions = self.active_transactions.lock().await;
        let tx_state = transactions.get_mut(&tx_id).ok_or_else(|| {
            CdcError::generic(format!(
                "Active transaction {} not found for rotation",
                tx_id
            ))
        })?;

        let buffered_bytes = tx_state.writer.buffer_size();
        tx_state.writer.sync().await?;
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

        // Append the SQL after rotation
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
            state.writer.sync().await?;
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
        let updated_json = serde_json::to_string(metadata)
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
    pub async fn commit_transaction(
        &self,
        tx_id: u32,
        commit_lsn: Option<Lsn>,
    ) -> Result<(PathBuf, TransactionFileMetadata)> {
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

        self.pending_count.fetch_add(1, Ordering::Release);

        debug!(
            "Committed transaction {}: moved metadata to sql_pending_tx/ (LSN: {:?}), data stays in sql_data_tx/",
            tx_id, commit_lsn
        );

        Ok((pending_metadata_path, metadata))
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

    fn count_pending_transactions(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    async fn count_pending_dir(dir: &Path) -> usize {
        let mut count = 0usize;
        let Ok(mut entries) = fs::read_dir(dir).await else {
            return 0;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().extension().and_then(|s| s.to_str()) == Some("meta") {
                count += 1;
            }
        }
        count
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
        let metadata_json = serde_json::to_string(metadata)
            .map_err(|e| CdcError::generic(format!("Failed to serialize metadata: {e}")))?;

        let temp_path = metadata_file_path.with_extension("meta.tmp");

        let mut metadata_file = File::create(&temp_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to create pending metadata {temp_path:?}: {e}"
            ))
        })?;

        metadata_file.write_all(metadata_json.as_bytes()).await?;
        // fsync the temp file: without sync_all the kernel can lose these bytes if
        // the process is killed before writeback. This file is the source of truth
        // for `last_executed_command_index` on resume — losing it causes the whole
        // transaction file to replay, double-applying non-idempotent INSERTs.
        metadata_file.sync_all().await?;
        drop(metadata_file);

        fs::rename(&temp_path, metadata_file_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to replace pending metadata {metadata_file_path:?}: {e}"
                ))
            })?;

        // fsync the parent directory so the rename itself is durable.
        if let Some(parent) = metadata_file_path.parent() {
            if let Ok(dir) = File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

        Ok(())
    }

    /// Persist `last_executed_command_index` directly to the .meta file.
    ///
    /// Used as a pre-commit hook so progress is durable on disk atomically with the
    /// destination batch COMMIT: hook writes .meta → destination COMMITs. If the
    /// process is killed after COMMIT, restart reads the freshly-persisted index and
    /// resumes from the next batch instead of replaying the whole transaction file.
    pub async fn persist_pending_metadata_progress(
        &self,
        metadata_file_path: &Path,
        last_executed_command_index: usize,
    ) -> Result<()> {
        let mut metadata = self.read_metadata(metadata_file_path).await?;
        metadata.last_executed_command_index = Some(last_executed_command_index);
        metadata.last_update_timestamp = Some(Utc::now());
        self.write_pending_metadata(metadata_file_path, &metadata)
            .await?;
        Ok(())
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
            self.pending_count.fetch_sub(1, Ordering::Release);
        }

        // Delete data files using storage trait (handles both compressed and uncompressed)
        for path in data_file_paths.iter() {
            self.storage.delete_transaction(path).await?;
        }

        debug!(
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
    /// Accepts `&RowData` directly. Uses `iter()` for column iteration.
    fn generate_insert_sql(&self, schema: &str, table: &str, new_data: &RowData) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        // Pre-size: assume ~32 bytes per (column, value) pair + overhead
        let mut sql = String::with_capacity(64 + new_data.len() * 48);
        sql.push_str("INSERT INTO ");
        self.append_qualified_table(&mut sql, &schema, table);
        sql.push_str(" (");
        for (i, (k, _)) in new_data.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            self.append_quoted_identifier(&mut sql, k);
        }
        sql.push_str(") VALUES (");
        for (i, (_, v)) in new_data.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            self.append_value(&mut sql, v);
        }
        sql.push_str(");");

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

        let mut sql = String::with_capacity(64 + new_data.len() * 64);
        sql.push_str("UPDATE ");
        self.append_qualified_table(&mut sql, &schema, table);
        sql.push_str(" SET ");
        for (i, (col, val)) in new_data.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            self.append_quoted_identifier(&mut sql, col);
            sql.push_str(" = ");
            self.append_value(&mut sql, val);
        }
        sql.push_str(" WHERE ");
        self.append_where_clause(&mut sql, replica_identity, key_columns, old_data, new_data)?;
        sql.push(';');

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

        let mut sql = String::with_capacity(64 + key_columns.len() * 32);
        sql.push_str("DELETE FROM ");
        self.append_qualified_table(&mut sql, &schema, table);
        sql.push_str(" WHERE ");
        self.append_where_clause(
            &mut sql,
            replica_identity,
            key_columns,
            Some(old_data),
            old_data,
        )?;
        sql.push(';');

        Ok(sql)
    }

    /// Generate TRUNCATE SQL command
    fn generate_truncate_sql(&self, tables: &[Arc<str>]) -> Result<String> {
        // For simplicity, generate a TRUNCATE statement for each table
        let mut sqls = Vec::with_capacity(tables.len());

        for table_spec in tables {
            let table_spec: &str = table_spec;
            // table_spec might include schema, parse it
            let parts: Vec<&str> = table_spec.split('.').collect();
            let (schema, table) = if parts.len() == 2 {
                (self.map_schema(Some(parts[0])), parts[1])
            } else {
                (self.map_schema(Some("public")), table_spec)
            };

            let mut sql = String::with_capacity(32 + table.len() + schema.len());
            match self.destination_type {
                DestinationType::MySQL | DestinationType::SqlServer => {
                    sql.push_str("TRUNCATE TABLE ");
                    self.append_quoted_identifier(&mut sql, &schema);
                    sql.push('.');
                    self.append_quoted_identifier(&mut sql, table);
                    sql.push(';');
                }
                DestinationType::SQLite => {
                    sql.push_str("DELETE FROM ");
                    self.append_quoted_identifier(&mut sql, table);
                    sql.push(';');
                }
            }
            sqls.push(sql);
        }

        Ok(sqls.join("\n"))
    }

    /// Append WHERE clause conditions directly into the provided buffer
    fn append_where_clause(
        &self,
        out: &mut String,
        replica_identity: &ReplicaIdentity,
        key_columns: &[Arc<str>],
        old_data: Option<&RowData>,
        new_data: &RowData,
    ) -> Result<()> {
        match replica_identity {
            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                let data = old_data.unwrap_or(new_data);
                for (i, col) in key_columns.iter().enumerate() {
                    if i > 0 {
                        out.push_str(" AND ");
                    }
                    let col_str: &str = col;
                    let val = data.get(col_str).ok_or_else(|| {
                        CdcError::Generic(format!("Key column {col_str} not found"))
                    })?;
                    self.append_quoted_identifier(out, col_str);
                    out.push_str(" = ");
                    self.append_value(out, val);
                }
            }
            ReplicaIdentity::Full => {
                let data = old_data.ok_or_else(|| {
                    CdcError::Generic("FULL replica identity requires old data".to_string())
                })?;
                for (i, (col, val)) in data.iter().enumerate() {
                    if i > 0 {
                        out.push_str(" AND ");
                    }
                    self.append_quoted_identifier(out, col);
                    if val.is_null() {
                        out.push_str(" IS NULL");
                    } else {
                        out.push_str(" = ");
                        self.append_value(out, val);
                    }
                }
            }
            ReplicaIdentity::Nothing => {
                return Err(CdcError::Generic(
                    "Cannot generate WHERE clause with NOTHING replica identity".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Append a quoted qualified table `schema.table` (or just table for SQLite) to `out`.
    #[inline]
    fn append_qualified_table(&self, out: &mut String, schema: &str, table: &str) {
        match self.destination_type {
            DestinationType::MySQL | DestinationType::SqlServer => {
                self.append_quoted_identifier(out, schema);
                out.push('.');
                self.append_quoted_identifier(out, table);
            }
            DestinationType::SQLite => {
                self.append_quoted_identifier(out, table);
            }
        }
    }

    /// Append a quoted identifier (schema/table/column) to `out`, performing
    /// destination-specific escaping of embedded quote characters.
    #[inline]
    fn append_quoted_identifier(&self, out: &mut String, name: &str) {
        let (open, close, escape_ch) = match self.destination_type {
            DestinationType::MySQL => ('`', '`', '`'),
            DestinationType::SqlServer => ('[', ']', ']'),
            DestinationType::SQLite => ('"', '"', '"'),
        };
        out.reserve(name.len() + 2);
        out.push(open);
        if name.as_bytes().contains(&(escape_ch as u8)) {
            for ch in name.chars() {
                if ch == escape_ch {
                    out.push(escape_ch);
                }
                out.push(ch);
            }
        } else {
            out.push_str(name);
        }
        out.push(close);
    }

    /// Escape a database identifier (schema, table, or column name) for safe
    /// inclusion in generated SQL, preventing SQL injection via malicious names.
    ///
    /// - MySQL:      wraps in backticks, doubling any embedded backticks.
    /// - SQL Server: wraps in brackets, doubling any embedded closing brackets.
    /// - SQLite:     wraps in double quotes, doubling any embedded double quotes.
    #[cfg(test)]
    fn quote_identifier(&self, name: &str) -> String {
        let mut out = String::with_capacity(name.len() + 2);
        self.append_quoted_identifier(&mut out, name);
        out
    }

    /// Append a hex literal for raw bytes directly into `out`.
    fn append_hex_literal(&self, out: &mut String, bytes: &[u8]) {
        static HEX: &[u8; 16] = b"0123456789abcdef";
        let (prefix, suffix) = match self.destination_type {
            DestinationType::SqlServer => ("0x", ""),
            DestinationType::MySQL | DestinationType::SQLite => ("X'", "'"),
        };
        out.reserve(prefix.len() + bytes.len() * 2 + suffix.len());
        out.push_str(prefix);
        // Safety: we only push ASCII hex chars + prefix/suffix which are ASCII.
        let buf = unsafe { out.as_mut_vec() };
        for &b in bytes {
            buf.push(HEX[(b >> 4) as usize]);
            buf.push(HEX[(b & 0x0f) as usize]);
        }
        out.push_str(suffix);
    }

    /// Append a `ColumnValue` literal directly into `out`.
    #[inline]
    fn append_value(&self, out: &mut String, value: &ColumnValue) {
        match value {
            ColumnValue::Null => out.push_str("NULL"),
            ColumnValue::Text(_) => match value.as_str() {
                Some(s) => {
                    if s == "t" {
                        out.push('1');
                        return;
                    }
                    if s == "f" {
                        out.push('0');
                        return;
                    }
                    out.reserve(s.len() + 2);
                    out.push('\'');
                    let needs_escape = if self.escape_backslash {
                        s.as_bytes().iter().any(|&b| b == b'\'' || b == b'\\')
                    } else {
                        s.as_bytes().contains(&b'\'')
                    };
                    if needs_escape {
                        for ch in s.chars() {
                            match ch {
                                '\'' => out.push_str("''"),
                                '\\' if self.escape_backslash => out.push_str("\\\\"),
                                _ => out.push(ch),
                            }
                        }
                    } else {
                        out.push_str(s);
                    }
                    out.push('\'');
                }
                None => {
                    self.append_hex_literal(out, value.as_bytes());
                }
            },
            ColumnValue::Binary(_) => self.append_hex_literal(out, value.as_bytes()),
        }
    }

    /// Format a `ColumnValue` as a SQL literal.
    ///
    /// Handles the three upstream variants correctly:
    /// - `Null`   → SQL `NULL`
    /// - `Text`   → UTF-8 string; PostgreSQL pgoutput booleans (`"t"` / `"f"`)
    ///              are converted to `1` / `0`. All other text is always quoted
    ///              to preserve values like leading-zero strings (e.g. zip codes).
    ///              Falls back to a hex literal when the payload is not valid UTF-8.
    /// - `Binary` → destination-specific hex literal (`X'…'` or `0x…`).
    #[cfg(test)]
    fn format_value(&self, value: &ColumnValue) -> String {
        let mut out = String::new();
        self.append_value(&mut out, value);
        out
    }

    /// Map source schema to destination schema
    fn map_schema<'a>(&'a self, source_schema: Option<&'a str>) -> &'a str {
        if let Some(schema) = source_schema {
            self.schema_mappings
                .get(schema)
                .map(|s| s.as_str())
                .unwrap_or(schema)
        } else {
            "public"
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
        let owned_path = metadata_path.to_path_buf();
        let file_manager_for_hook = self.clone();
        let staged_index = last_executed_index;

        let pre_commit_hook: Option<PreCommitHook> = Some(Box::new(move || {
            Box::pin(async move {
                file_manager_for_hook
                    .persist_pending_metadata_progress(&owned_path, staged_index)
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
                metadata_path.display(),
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
        let mut line_stmts: Vec<String> = Vec::new();

        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to read line: {e}")))?
        {
            parser.parse_line(&line, &mut line_stmts)?;
            for stmt in line_stmts.drain(..) {
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
        self: &Arc<Self>,
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

        debug!(
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
            if remaining_start_index > 0
                && segment.statement_count > 0
                && remaining_start_index >= segment.statement_count
            {
                remaining_start_index -= segment.statement_count;
                continue;
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
        debug!(
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
            debug!("Transaction {} applied, commit_lsn: {}", tx_id, commit_lsn);

            lsn_tracker.commit_lsn(commit_lsn.0);

            shared_lsn_feedback.update_applied_lsn(commit_lsn.0);

            debug!(
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

        if pending_tx.metadata.commit_lsn.is_some() {
            let pending_count = self.count_pending_transactions();
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use pg_walstream::ColumnValue;

    /// Create a minimal `TransactionManager` for unit-testing `format_value`
    /// and SQL generation helpers without filesystem side-effects.
    async fn test_manager(dest: DestinationType) -> TransactionManager {
        let dir = std::env::temp_dir().join(format!(
            "pg2any_format_value_test_{}_{}",
            dest,
            std::process::id()
        ));
        let _ = tokio::fs::create_dir_all(&dir).await;
        TransactionManager::new(&dir, dest, 10 * 1024 * 1024)
            .await
            .expect("test manager creation should succeed")
    }

    // ── SQL injection prevention ──────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_backslash_injection_is_escaped() {
        let mgr = test_manager(DestinationType::MySQL).await;

        // Classic MySQL backslash injection: foo\' should NOT escape the quote
        let val = ColumnValue::text(r"foo\'; DROP TABLE users; --");
        let formatted = mgr.format_value(&val);
        // Backslash doubled, then single quote doubled: foo\\''  ; DROP TABLE users; --
        assert_eq!(formatted, r"'foo\\''; DROP TABLE users; --'");
        // The result is a safely quoted string literal — no breakout possible.
    }

    #[tokio::test]
    async fn test_mysql_backslash_at_end_of_string() {
        let mgr = test_manager(DestinationType::MySQL).await;

        let val = ColumnValue::text(r"trailing\");
        let formatted = mgr.format_value(&val);
        assert_eq!(formatted, r"'trailing\\'");
    }

    #[tokio::test]
    async fn test_sqlite_does_not_double_escape_backslashes() {
        let mgr = test_manager(DestinationType::SQLite).await;

        // SQLite does NOT treat backslashes as escape characters
        let val = ColumnValue::text(r"path\to\file");
        let formatted = mgr.format_value(&val);
        assert_eq!(formatted, r"'path\to\file'");
    }

    #[tokio::test]
    async fn test_sqlserver_does_not_double_escape_backslashes() {
        let mgr = test_manager(DestinationType::SqlServer).await;

        let val = ColumnValue::text(r"path\to\file");
        let formatted = mgr.format_value(&val);
        assert_eq!(formatted, r"'path\to\file'");
    }

    // ── Boolean & text value formatting ─────────────────────────────

    #[tokio::test]
    async fn test_numeric_text_is_always_quoted() {
        // Numeric-looking strings must be quoted to preserve values like
        // leading-zero zip codes and large numeric identifiers.
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(mgr.format_value(&ColumnValue::text("42")), "'42'");
        assert_eq!(mgr.format_value(&ColumnValue::text("-1")), "'-1'");
        assert_eq!(mgr.format_value(&ColumnValue::text("0")), "'0'");
        assert_eq!(mgr.format_value(&ColumnValue::text("3.14")), "'3.14'");
        assert_eq!(mgr.format_value(&ColumnValue::text("01234")), "'01234'");
    }

    #[tokio::test]
    async fn test_pgoutput_boolean_true() {
        // PostgreSQL pgoutput encodes boolean true as "t"
        for dest in [
            DestinationType::MySQL,
            DestinationType::SQLite,
            DestinationType::SqlServer,
        ] {
            let mgr = test_manager(dest.clone()).await;
            assert_eq!(mgr.format_value(&ColumnValue::text("t")), "1");
        }
    }

    #[tokio::test]
    async fn test_pgoutput_boolean_false() {
        // PostgreSQL pgoutput encodes boolean false as "f"
        for dest in [
            DestinationType::MySQL,
            DestinationType::SQLite,
            DestinationType::SqlServer,
        ] {
            let mgr = test_manager(dest.clone()).await;
            assert_eq!(mgr.format_value(&ColumnValue::text("f")), "0");
        }
    }

    #[tokio::test]
    async fn test_full_word_true_false_is_quoted() {
        // "true" and "false" (full words) are NOT boolean in pgoutput — they
        // should be treated as regular strings.
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(mgr.format_value(&ColumnValue::text("true")), "'true'");
        assert_eq!(mgr.format_value(&ColumnValue::text("false")), "'false'");
        assert_eq!(mgr.format_value(&ColumnValue::text("TRUE")), "'TRUE'");
    }

    #[tokio::test]
    async fn test_regular_string_is_quoted() {
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(mgr.format_value(&ColumnValue::text("hello")), "'hello'");
    }

    #[tokio::test]
    async fn test_string_with_single_quote_is_escaped() {
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(
            mgr.format_value(&ColumnValue::text("it's here")),
            "'it''s here'"
        );
    }

    // ── Null and binary ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_null_value() {
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(mgr.format_value(&ColumnValue::Null), "NULL");
    }

    #[tokio::test]
    async fn test_binary_value_hex_encoded_mysql_sqlite() {
        for dest in [DestinationType::MySQL, DestinationType::SQLite] {
            let mgr = test_manager(dest).await;
            let val = ColumnValue::Binary(Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]));
            assert_eq!(mgr.format_value(&val), "X'deadbeef'");
        }
    }

    #[tokio::test]
    async fn test_binary_value_hex_encoded_sqlserver() {
        let mgr = test_manager(DestinationType::SqlServer).await;
        let val = ColumnValue::Binary(Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(mgr.format_value(&val), "0xdeadbeef");
    }

    #[tokio::test]
    async fn test_binary_empty_bytes() {
        let mgr_mysql = test_manager(DestinationType::MySQL).await;
        let mgr_sqlserver = test_manager(DestinationType::SqlServer).await;
        let val = ColumnValue::Binary(Bytes::from_static(&[]));
        assert_eq!(mgr_mysql.format_value(&val), "X''");
        assert_eq!(mgr_sqlserver.format_value(&val), "0x");
    }

    #[tokio::test]
    async fn test_non_utf8_text_falls_back_to_hex_literal() {
        // Non-UTF-8 bytes in a Text variant → treated as binary literal
        let mgr_mysql = test_manager(DestinationType::MySQL).await;
        let mgr_sqlserver = test_manager(DestinationType::SqlServer).await;
        let val = ColumnValue::Text(Bytes::from_static(&[0x80, 0xFF, 0x01]));
        assert_eq!(mgr_mysql.format_value(&val), "X'80ff01'");
        assert_eq!(mgr_sqlserver.format_value(&val), "0x80ff01");
    }

    // ── Identifier escaping ──────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_identifier_escapes_backticks() {
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(mgr.quote_identifier("normal"), "`normal`");
        assert_eq!(mgr.quote_identifier("ta`ble"), "`ta``ble`");
        assert_eq!(mgr.quote_identifier("`inject`"), "```inject```");
    }

    #[tokio::test]
    async fn test_sqlserver_identifier_escapes_brackets() {
        let mgr = test_manager(DestinationType::SqlServer).await;
        assert_eq!(mgr.quote_identifier("normal"), "[normal]");
        assert_eq!(mgr.quote_identifier("ta]ble"), "[ta]]ble]");
    }

    #[tokio::test]
    async fn test_sqlite_identifier_escapes_double_quotes() {
        let mgr = test_manager(DestinationType::SQLite).await;
        assert_eq!(mgr.quote_identifier("normal"), "\"normal\"");
        assert_eq!(mgr.quote_identifier("ta\"ble"), "\"ta\"\"ble\"");
    }

    // ── Edge cases ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_empty_string_is_quoted() {
        let mgr = test_manager(DestinationType::MySQL).await;
        assert_eq!(mgr.format_value(&ColumnValue::text("")), "''");
    }

    // ── Performance benchmarks (run with `cargo test --lib bench_ -- --nocapture`) ──

    fn make_row_data(num_cols: usize) -> RowData {
        let mut row = RowData::with_capacity(num_cols);
        for i in 0..num_cols {
            row.push(
                Arc::from(format!("col_{i}").as_str()),
                ColumnValue::text(&format!("value_{i}_with_some_realistic_length_data")),
            );
        }
        row
    }

    #[tokio::test]
    async fn bench_generate_insert_sql() {
        let mgr = test_manager(DestinationType::MySQL).await;
        let row = make_row_data(10);
        let iterations = 100_000;

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let sql = mgr.generate_insert_sql("public", "users", &row).unwrap();
            std::hint::black_box(&sql);
        }
        let elapsed = start.elapsed();
        eprintln!(
            "generate_insert_sql (10 cols, MySQL): {iterations} iters in {:?} ({:.0} ns/iter)",
            elapsed,
            elapsed.as_nanos() as f64 / iterations as f64
        );
    }

    #[tokio::test]
    async fn bench_generate_update_sql() {
        let mgr = test_manager(DestinationType::MySQL).await;
        let new_data = make_row_data(10);
        let old_data = make_row_data(10);
        let key_columns: Vec<Arc<str>> = vec![Arc::from("col_0")];
        let iterations = 100_000;

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let sql = mgr
                .generate_update_sql(
                    "public",
                    "users",
                    &new_data,
                    Some(&old_data),
                    &ReplicaIdentity::Default,
                    &key_columns,
                )
                .unwrap();
            std::hint::black_box(&sql);
        }
        let elapsed = start.elapsed();
        eprintln!(
            "generate_update_sql (10 cols, MySQL): {iterations} iters in {:?} ({:.0} ns/iter)",
            elapsed,
            elapsed.as_nanos() as f64 / iterations as f64
        );
    }

    #[tokio::test]
    async fn bench_generate_delete_sql() {
        let mgr = test_manager(DestinationType::MySQL).await;
        let old_data = make_row_data(10);
        let key_columns: Vec<Arc<str>> = vec![Arc::from("col_0")];
        let iterations = 100_000;

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let sql = mgr
                .generate_delete_sql(
                    "public",
                    "users",
                    &old_data,
                    &ReplicaIdentity::Default,
                    &key_columns,
                )
                .unwrap();
            std::hint::black_box(&sql);
        }
        let elapsed = start.elapsed();
        eprintln!(
            "generate_delete_sql (10 cols, MySQL): {iterations} iters in {:?} ({:.0} ns/iter)",
            elapsed,
            elapsed.as_nanos() as f64 / iterations as f64
        );
    }

    #[tokio::test]
    async fn bench_map_schema_hit_miss() {
        let mut mgr = test_manager(DestinationType::MySQL).await;
        mgr.schema_mappings
            .insert("public".to_string(), "cdc_db".to_string());
        let iterations = 1_000_000;

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let s = mgr.map_schema(Some("public"));
            std::hint::black_box(s);
        }
        let hit_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let s = mgr.map_schema(Some("other_schema"));
            std::hint::black_box(s);
        }
        let miss_elapsed = start.elapsed();

        eprintln!(
            "map_schema hit:  {iterations} iters in {:?} ({:.1} ns/iter)",
            hit_elapsed,
            hit_elapsed.as_nanos() as f64 / iterations as f64
        );
        eprintln!(
            "map_schema miss: {iterations} iters in {:?} ({:.1} ns/iter)",
            miss_elapsed,
            miss_elapsed.as_nanos() as f64 / iterations as f64
        );
    }

    #[tokio::test]
    async fn bench_format_value_various_types() {
        let mgr = test_manager(DestinationType::MySQL).await;
        let iterations = 100_000;

        let text_val = ColumnValue::text("Hello, World! This is a test value with some length.");
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let s = mgr.format_value(&text_val);
            std::hint::black_box(&s);
        }
        let text_elapsed = start.elapsed();

        let null_val = ColumnValue::Null;
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let s = mgr.format_value(&null_val);
            std::hint::black_box(&s);
        }
        let null_elapsed = start.elapsed();

        let binary_val =
            ColumnValue::Binary(Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE]));
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let s = mgr.format_value(&binary_val);
            std::hint::black_box(&s);
        }
        let binary_elapsed = start.elapsed();

        eprintln!(
            "format_value text:   {iterations} iters in {:?} ({:.0} ns/iter)",
            text_elapsed,
            text_elapsed.as_nanos() as f64 / iterations as f64
        );
        eprintln!(
            "format_value null:   {iterations} iters in {:?} ({:.0} ns/iter)",
            null_elapsed,
            null_elapsed.as_nanos() as f64 / iterations as f64
        );
        eprintln!(
            "format_value binary: {iterations} iters in {:?} ({:.0} ns/iter)",
            binary_elapsed,
            binary_elapsed.as_nanos() as f64 / iterations as f64
        );
    }
}
