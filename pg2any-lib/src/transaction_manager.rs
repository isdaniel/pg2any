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

use crate::destinations::dialect::SqlDialect;
use crate::destinations::dialects::{
    AnsiDialect, KafkaDialect, MySqlDialect, SqlServerDialect, SqliteDialect,
};
use crate::destinations::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::types::{ChangeEvent, DestinationType, EventType, Lsn, RowData};
use chrono::{DateTime, Utc};
#[cfg(test)]
use pg_walstream::ColumnValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const MB: usize = 1024 * 1024;
const RECEIVED_TX_DIR: &str = "sql_received_tx";
const PENDING_TX_DIR: &str = "sql_pending_tx";
const DATA_TX_DIR: &str = "sql_data_tx";
/// Default buffer size for event accumulation (8MB)
const DEFAULT_BUFFER_SIZE: usize = 8 * MB;

fn dialect_for(dt: &DestinationType) -> &'static dyn SqlDialect {
    match dt {
        DestinationType::MySQL => {
            static D: MySqlDialect = MySqlDialect;
            &D
        }
        DestinationType::SqlServer => {
            static D: SqlServerDialect = SqlServerDialect;
            &D
        }
        DestinationType::SQLite => {
            static D: SqliteDialect = SqliteDialect;
            &D
        }
        DestinationType::Kafka => {
            static D: KafkaDialect = KafkaDialect;
            &D
        }
        DestinationType::Custom(_) => {
            static D: AnsiDialect = AnsiDialect;
            &D
        }
    }
}
struct BufferedEventWriter {
    /// File path being written to
    file_path: PathBuf,
    /// In-memory buffer accumulating length-framed MessagePack event records
    buffer: Vec<u8>,
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
            buffer: Vec::with_capacity(max_buffer_size),
            max_buffer_size,
            writer: None,
        }
    }

    /// Append a pre-encoded length-framed record to the buffer (the caller
    /// already encoded it once for the rotation-size check, so we reuse those
    /// bytes instead of re-encoding). Returns true if the buffer reached
    /// capacity and should be flushed.
    fn append_encoded(&mut self, encoded: &[u8]) -> bool {
        self.buffer.extend_from_slice(encoded);
        self.buffer.len() >= self.max_buffer_size
    }

    /// Flush the buffer to disk
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
        writer.write_all(&self.buffer).await?;
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

/// Mutable accumulator threaded through `process_segment_records` across all
/// segments of one transaction. Holds the SQL-render batch and the event-mode
/// batch (only one is used per transaction, chosen by `render_sql`), plus the
/// running record-unit indices shared with the caller.
struct RecordProcessingState<'a> {
    /// SQL-render batch (SQL destinations)
    sql_batch: &'a mut Vec<String>,
    /// Structured events aligned 1:1 with `sql_batch` (SQL destinations).
    /// Used to route homogeneous-INSERT runs through the structured bulk path
    /// (`execute_bulk_insert_rows_with_hook`), bypassing SQL render + reparse.
    sql_event_batch: &'a mut Vec<ChangeEvent>,
    /// Event batch (event-mode destinations)
    event_batch: &'a mut Vec<ChangeEvent>,
    current_command_index: &'a mut usize,
    processed_count: &'a mut usize,
    batch_count: &'a mut usize,
}

#[derive(Debug, Clone)]
struct PendingProgress {
    last_executed_command_index: usize,
    last_update_timestamp: DateTime<Utc>,
}

/// Delete a transaction data file, ignoring the case where it is already gone.
///
/// A missing file is not an error: recovery and finalize paths may both attempt
/// to remove the same segment, and an already-deleted file means the desired
/// post-condition already holds.
async fn delete_transaction_file(file_path: &Path) -> Result<()> {
    match fs::remove_file(file_path).await {
        Ok(()) => {
            debug!("Deleted transaction data file: {:?}", file_path);
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(CdcError::generic(format!(
            "Failed to delete file {file_path:?}: {e}"
        ))),
    }
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
    /// When true, stores raw ChangeEvent JSON instead of generated SQL
    event_mode: bool,
    /// Whether bulk insert optimization is enabled
    /// Minimum INSERT count to trigger bulk insert path
    bulk_insert_threshold: usize,
    /// SQL dialect for this destination (selected from `destination_type`).
    dialect: &'static dyn SqlDialect,
    /// In-memory count of transactions in sql_pending_tx/ (committed, not yet
    /// finalized). Seeded at startup from the dir scan; kept in sync on
    /// commit (+1) and finalize/delete (-1). Avoids a per-transaction dir scan.
    pending_tx_count: AtomicUsize,
}

impl TransactionManager {
    /// Create a new transaction file manager
    pub async fn new(
        base_path: impl AsRef<Path>,
        destination_type: DestinationType,
        dialect_override: Option<&'static dyn SqlDialect>,
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

        info!(
            "Transaction file manager initialized at {:?} for {:?}, segment_size_bytes={:?}",
            base_path, destination_type, segment_size_bytes
        );

        let dialect = dialect_override.unwrap_or_else(|| dialect_for(&destination_type));

        Ok(Self {
            base_path,
            destination_type,
            schema_mappings: HashMap::new(),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            staged_pending_progress: Arc::new(Mutex::new(HashMap::new())),
            buffer_size: DEFAULT_BUFFER_SIZE,
            segment_size_bytes,
            event_mode: false,
            bulk_insert_threshold: 500,
            dialect,
            pending_tx_count: AtomicUsize::new(0),
        })
    }

    /// Set schema mappings for SQL generation
    pub fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    /// Enable event-mode: stores raw ChangeEvent JSON instead of generated SQL
    pub fn set_event_mode(&mut self, enabled: bool) {
        self.event_mode = enabled;
    }

    /// Configure bulk insert optimization parameters
    pub fn set_bulk_insert_config(&mut self, threshold: usize) {
        self.bulk_insert_threshold = threshold;
    }

    /// Current in-memory count of committed-but-not-finalized pending transactions.
    pub fn pending_count(&self) -> usize {
        self.pending_tx_count.load(Ordering::Relaxed)
    }

    /// Seed the pending counter at startup from the authoritative dir scan.
    pub fn seed_pending_count(&self, n: usize) {
        self.pending_tx_count.store(n, Ordering::Relaxed);
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
    /// Segment index is 0-based but file names are 1-based (txid_000001.mpk)
    fn get_segment_data_file_path(&self, tx_id: u32, segment_index: usize) -> PathBuf {
        let filename = format!("{}_{:06}.mpk", tx_id, segment_index + 1);
        self.base_path.join(DATA_TX_DIR).join(filename)
    }

    fn get_final_segment_info(
        &self,
        tx_id: u32,
        metadata: &TransactionFileMetadata,
        tx_state: Option<ActiveTransactionState>,
    ) -> (Vec<PathBuf>, Vec<usize>) {
        if let Some(mut state) = tx_state.filter(|state| !state.segments.is_empty()) {
            (
                std::mem::take(&mut state.segments),
                std::mem::take(&mut state.segment_statement_counts),
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
    ///
    /// Each event is serialized as a length-framed MessagePack record (see
    /// `storage::binary_record`). Only DML/Truncate events are persisted; other
    /// event types are skipped. Exactly ONE record is written per persisted
    /// event, so `segment_statement_counts` counts records (1 per event), which
    /// is the unit the consumer's record-resume arithmetic uses.
    pub async fn append_event(&self, tx_id: u32, event: &ChangeEvent) -> Result<()> {
        // Only DML and Truncate events are persisted to disk. Other event types
        // (Begin/Commit/Relation/...) carry no row data and are skipped.
        match &event.event_type {
            EventType::Insert { .. }
            | EventType::Update { .. }
            | EventType::Delete { .. }
            | EventType::Truncate(_) => {}
            _ => return Ok(()),
        }

        // Encode the record once into a scratch buffer so we know its on-disk
        // size for the rotation decision (segments stay <= segment_size_bytes).
        let mut encoded = Vec::new();
        crate::storage::binary_record::encode_record(event, &mut encoded)?;
        let encoded_len = encoded.len();

        let mut transactions = self.active_transactions.lock().await;

        let tx_state = transactions.get_mut(&tx_id).ok_or_else(|| {
            CdcError::generic(format!(
                "Active transaction {} not found for append_event",
                tx_id
            ))
        })?;

        let estimated_size =
            tx_state.current_segment_size_bytes + tx_state.writer.buffer.len() + encoded_len;

        let should_rotate = estimated_size > self.segment_size_bytes
            && (tx_state.current_segment_size_bytes > 0 || !tx_state.writer.buffer.is_empty());

        if should_rotate {
            let buffered_bytes = tx_state.writer.buffer.len();
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

        // Append the already-encoded MessagePack record (reusing the bytes from
        // the rotation-size check above — no second encode) and count it as
        // exactly one record. The consumer reads back one ChangeEvent per
        // record, so this 1-per-event count is the authoritative unit for
        // crash-resume skip arithmetic.
        let should_flush = tx_state.writer.append_encoded(&encoded);
        if let Some(count) = tx_state
            .segment_statement_counts
            .get_mut(tx_state.current_segment_index)
        {
            *count += 1;
        }
        if should_flush {
            let buffered_bytes = tx_state.writer.buffer.len();
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
            let known_count = segment_counts.get(idx).copied().unwrap_or(0);
            // Binary MessagePack segments are never SQL-parsed at finalize: the
            // file is already final and the producer-tracked record count is
            // authoritative, so finalizing is a no-op passthrough.
            final_segments.push(TransactionSegment {
                path: segment_path.clone(),
                statement_count: known_count,
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
    ///
    /// If a pending metadata file already exists with `last_executed_command_index` set
    /// (indicating a previous run already applied this transaction to the destination),
    /// the existing file is preserved to prevent duplicate re-execution on recovery.
    pub async fn commit_transaction(
        &self,
        tx_id: u32,
        commit_lsn: Option<Lsn>,
    ) -> Result<(PathBuf, TransactionFileMetadata)> {
        let received_metadata_path = self.get_received_tx_path(tx_id);
        let pending_metadata_path = self.get_pending_tx_path(tx_id);

        // CRASH SAFETY: If pending file already exists with execution progress,
        // a previous run already applied this transaction. Don't overwrite — the
        // consumer's dedup check or recovery finalize will handle it correctly.
        if fs::metadata(&pending_metadata_path).await.is_ok() {
            if let Ok(existing) = self.read_metadata(&pending_metadata_path).await {
                if existing.last_executed_command_index.is_some() {
                    info!(
                        "Transaction {} already has pending file with execution progress, preserving existing state",
                        tx_id
                    );
                    // Clean up received file and flush active writer
                    let _ = self.remove_received_metadata(&received_metadata_path).await;
                    let tx_state = self
                        .take_and_flush_active_transaction(tx_id)
                        .await
                        .ok()
                        .flatten();
                    // Delete orphaned data segment files from this duplicate write
                    if let Some(state) = tx_state {
                        for seg_path in &state.segments {
                            let _ = fs::remove_file(seg_path).await;
                        }
                    }
                    return Ok((pending_metadata_path, existing));
                }
            }
        }

        let mut metadata = self.read_received_metadata(&received_metadata_path).await?;
        let tx_state = self.take_and_flush_active_transaction(tx_id).await?;

        let (segment_paths, segment_counts) =
            self.get_final_segment_info(tx_id, &metadata, tx_state);

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

        self.pending_tx_count.fetch_add(1, Ordering::Relaxed);

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
            delete_transaction_file(&path).await?;
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

        // Sort using the custom Ord impl: commit_lsn ascending (None treated as
        // infinity) with transaction_id tiebreaker for deterministic WAL-order recovery.
        files.sort_unstable();

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
        let metadata_json = serde_json::to_string(metadata)
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

        // Delete data files
        for path in data_file_paths.iter() {
            delete_transaction_file(path).await?;
        }

        info!(
            "Deleted executed transaction files: metadata={:?}, data_files={:?}",
            metadata_file_path, data_file_paths
        );
        Ok(())
    }

    /// Build a `RenderContext` borrowing this manager's dialect + schema mappings.
    #[inline]
    fn render_ctx(&self) -> crate::sql_renderer::RenderContext<'_> {
        crate::sql_renderer::RenderContext {
            dialect: self.dialect,
            schema_mappings: &self.schema_mappings,
        }
    }

    /// Generate TRUNCATE SQL command
    #[cfg(test)]
    fn generate_truncate_sql(&self, tables: &[Arc<str>]) -> Result<String> {
        crate::sql_renderer::generate_truncate_sql(&self.render_ctx(), tables)
    }

    /// Append a quoted qualified table `schema.table` (or just table for SQLite) to `out`.
    #[inline]
    #[cfg(test)]
    fn append_qualified_table(&self, out: &mut String, schema: &str, table: &str) {
        self.dialect.qualify_table(schema, table, out);
    }

    /// Append a quoted identifier (schema/table/column) to `out`, performing
    /// destination-specific escaping of embedded quote characters.
    #[inline]
    #[cfg(test)]
    fn append_quoted_identifier(&self, out: &mut String, name: &str) {
        self.dialect.quote_identifier(name, out);
    }

    /// Escape a database identifier (schema, table, or column name) for safe
    /// inclusion in generated SQL, preventing SQL injection via malicious names.
    #[cfg(test)]
    fn quote_identifier(&self, name: &str) -> String {
        let mut out = String::with_capacity(name.len() + 2);
        self.append_quoted_identifier(&mut out, name);
        out
    }

    /// Append a hex literal for raw bytes directly into `out`.
    #[cfg(test)]
    fn append_hex_literal(&self, out: &mut String, bytes: &[u8]) {
        self.dialect.render_hex_literal(bytes, out);
    }

    /// Append a `ColumnValue` literal directly into `out`.
    #[cfg(test)]
    fn append_value(&self, out: &mut String, value: &ColumnValue) {
        self.dialect.render_value(value, out);
    }

    /// Format a `ColumnValue` as a SQL literal.
    #[cfg(test)]
    fn format_value(&self, value: &ColumnValue) -> String {
        let mut out = String::new();
        self.append_value(&mut out, value);
        out
    }
}

/// Borrowed result of [`detect_homogeneous_insert_run`]:
/// `(schema, table, ordered column names, per-event row borrows)`.
type HomogeneousInsertRun<'a> = (&'a str, &'a str, Vec<Arc<str>>, Vec<&'a RowData>);

/// Detect a homogeneous INSERT run in a SQL-render batch's structured events.
///
/// Returns `Some((schema, table, columns, rows))` when EVERY event is an
/// `EventType::Insert` with the SAME `(schema, table)` and the SAME ordered
/// column-name set; `rows` borrows each event's `&RowData`. Returns `None`
/// otherwise (mixed event types, differing tables, or differing column sets),
/// so the caller falls back to the SQL-string path.
fn detect_homogeneous_insert_run(events: &[ChangeEvent]) -> Option<HomogeneousInsertRun<'_>> {
    let first = events.first()?;
    let (schema, table, data) = match &first.event_type {
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => (schema.as_ref(), table.as_ref(), data),
        _ => return None,
    };

    let columns: Vec<Arc<str>> = data.iter().map(|(name, _)| name.clone()).collect();

    let mut rows: Vec<&RowData> = Vec::with_capacity(events.len());
    rows.push(data);

    for event in &events[1..] {
        match &event.event_type {
            EventType::Insert {
                schema: s,
                table: t,
                data: d,
                ..
            } => {
                if s.as_ref() != schema || t.as_ref() != table {
                    return None;
                }
                // Column set (names + order) must match the first row exactly.
                if d.len() != columns.len() {
                    return None;
                }
                if d.iter()
                    .zip(columns.iter())
                    .any(|((name, _), expected)| name.as_ref() != expected.as_ref())
                {
                    return None;
                }
                rows.push(d);
            }
            _ => return None,
        }
    }

    Some((schema, table, columns, rows))
}

impl TransactionManager {
    #[allow(clippy::too_many_arguments)]
    async fn execute_batch_with_bulk_detection(
        self: &Arc<Self>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metadata_path: &Path,
        commands: &[String],
        events: &[ChangeEvent],
        last_executed_index: usize,
        batch_idx: usize,
        metrics_collector: &Arc<MetricsCollector>,
        bulk_insert_threshold: usize,
    ) -> Result<()> {
        if commands.len() >= bulk_insert_threshold && destination_handler.supports_bulk_insert() {
            // STRUCTURED FAST PATH (Phase 3 Stage 5): if the batch is a
            // homogeneous INSERT run, build the bulk load DIRECTLY from the
            // structured `RowData`, bypassing SQL render + reparse. `events` is
            // aligned 1:1 with `commands`, so the resume index is identical.
            if let Some((schema, table, columns, rows)) = detect_homogeneous_insert_run(events) {
                // Apply the SAME schema/table mapping the SQL renderer uses, so
                // the bulk target table matches the rendered INSERT exactly.
                let ctx = self.render_ctx();
                let mapped_schema = ctx.map_schema(Some(schema));
                let mut qualified_table = String::new();
                ctx.dialect
                    .qualify_table(mapped_schema, table, &mut qualified_table);

                debug!(
                    "Structured bulk insert detected: {} rows into {} (batch {})",
                    rows.len(),
                    qualified_table,
                    batch_idx
                );

                let batch_start_time = Instant::now();
                let metadata_path_owned = metadata_path.to_path_buf();
                let file_manager_for_hook = self.clone();
                let staged_index = last_executed_index;

                let pre_commit_hook: Option<PreCommitHook> = Some(Box::new(move || {
                    let metadata_path = metadata_path_owned;
                    let file_manager_for_hook = file_manager_for_hook;
                    Box::pin(async move {
                        file_manager_for_hook
                            .stage_pending_metadata_progress(&metadata_path, staged_index)
                            .await?;
                        Ok(())
                    })
                }));

                match destination_handler
                    .execute_bulk_insert_rows_with_hook(
                        &qualified_table,
                        &columns,
                        &rows,
                        pre_commit_hook,
                    )
                    .await
                {
                    Ok(()) => {
                        let duration = batch_start_time.elapsed();
                        debug!(
                            "Structured bulk insert batch {} complete: {} rows in {:?}",
                            batch_idx,
                            rows.len(),
                            duration
                        );
                        self.flush_staged_pending_progress().await?;
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(
                            "Structured bulk insert failed for batch {}, falling back to SQL batch: {}",
                            batch_idx, e
                        );
                    }
                }
            } else if let Some(parsed) =
                crate::destinations::bulk_insert::detect_bulk_insert_batch(commands)
            {
                debug!(
                    "Bulk insert detected: {} rows into {} (batch {})",
                    parsed.rows.len(),
                    parsed.table,
                    batch_idx
                );

                let batch_start_time = Instant::now();
                let metadata_path_owned = metadata_path.to_path_buf();
                let file_manager_for_hook = self.clone();
                let staged_index = last_executed_index;

                let pre_commit_hook: Option<PreCommitHook> = Some(Box::new(move || {
                    let metadata_path = metadata_path_owned;
                    let file_manager_for_hook = file_manager_for_hook;
                    Box::pin(async move {
                        file_manager_for_hook
                            .stage_pending_metadata_progress(&metadata_path, staged_index)
                            .await?;
                        Ok(())
                    })
                }));

                match destination_handler
                    .execute_bulk_insert_with_hook(
                        &parsed.table,
                        &parsed.columns,
                        &parsed.rows,
                        pre_commit_hook,
                    )
                    .await
                {
                    Ok(()) => {
                        let duration = batch_start_time.elapsed();
                        debug!(
                            "Bulk insert batch {} complete: {} rows in {:?}",
                            batch_idx,
                            parsed.rows.len(),
                            duration
                        );
                        self.flush_staged_pending_progress().await?;
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(
                            "Bulk insert failed for batch {}, falling back to SQL batch: {}",
                            batch_idx, e
                        );
                    }
                }
            }
        }

        self.execute_sql_batch(
            destination_handler,
            metadata_path,
            commands,
            last_executed_index,
            batch_idx,
            metrics_collector,
        )
        .await
    }

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

        self.flush_staged_pending_progress().await?;

        Ok(())
    }

    /// Read one binary `.mpk` segment file and dispatch its records.
    ///
    /// The whole segment is read into memory (bounded by `segment_size_bytes`,
    /// default 64MB) and decoded as a stream of length-framed MessagePack records via
    /// `binary_record::decode_record`. Records before `start_record_index` are
    /// skipped (record-unit crash resume). For each remaining record:
    ///   * SQL destinations (`render_sql = true`) render the event to SQL and
    ///     accumulate into the `Vec<String>` batch, flushing via
    ///     `execute_batch_with_bulk_detection` when the batch fills.
    ///   * Event-mode destinations (`render_sql = false`) accumulate the raw
    ///     `ChangeEvent` and flush via `execute_events_batch_with_hook`.
    #[allow(clippy::too_many_arguments)]
    async fn process_segment_records(
        self: &Arc<Self>,
        segment_path: &Path,
        start_record_index: usize,
        render_sql: bool,
        pending_tx: &PendingTransactionFile,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        state: &mut RecordProcessingState<'_>,
    ) -> Result<()> {
        // Binary segments are size-bounded; read-to-end is safe here. (Streaming
        // decode is a future refinement, see Phase 3 plan.)
        let bytes = tokio::fs::read(segment_path).await.map_err(|e| {
            CdcError::generic(format!("Failed to read segment {segment_path:?}: {e}"))
        })?;

        let mut cursor = std::io::Cursor::new(bytes);
        let mut record_index = 0usize;

        while let Some(event) =
            crate::storage::binary_record::decode_record(&mut cursor).map_err(|e| {
                CdcError::generic(format!(
                    "Failed to decode record {record_index} in segment {segment_path:?}: {e}"
                ))
            })?
        {
            if record_index < start_record_index {
                record_index += 1;
                continue;
            }

            if render_sql {
                let ctx = self.render_ctx();
                let mut sql = String::new();
                crate::sql_renderer::render_sql_for_event_into(&ctx, &event, &mut sql)?;
                if !sql.is_empty() {
                    state.sql_batch.push(sql);
                    // Keep the structured event aligned 1:1 with `sql_batch` so
                    // the flush can route homogeneous INSERT runs through the
                    // structured bulk path without desyncing the resume index.
                    state.sql_event_batch.push(event);
                }

                if state.sql_batch.len() >= batch_size {
                    if cancellation_token.is_cancelled() {
                        return Err(CdcError::cancelled(
                            "Transaction file processing cancelled by shutdown signal",
                        ));
                    }

                    let batch_len = state.sql_batch.len();
                    let next_command_index = *state.current_command_index + batch_len;
                    let last_executed_index = next_command_index - 1;
                    *state.batch_count += 1;

                    self.execute_batch_with_bulk_detection(
                        destination_handler,
                        &pending_tx.file_path,
                        state.sql_batch,
                        state.sql_event_batch,
                        last_executed_index,
                        *state.batch_count,
                        metrics_collector,
                        self.bulk_insert_threshold,
                    )
                    .await?;

                    *state.current_command_index = next_command_index;
                    *state.processed_count += batch_len;
                    state.sql_batch.clear();
                    state.sql_event_batch.clear();
                }
            } else {
                state.event_batch.push(event);

                if state.event_batch.len() >= batch_size {
                    if cancellation_token.is_cancelled() {
                        return Err(CdcError::cancelled(
                            "Transaction file processing cancelled by shutdown signal",
                        ));
                    }

                    let batch_len = state.event_batch.len();
                    let next_command_index = *state.current_command_index + batch_len;
                    let last_executed_index = next_command_index - 1;
                    *state.batch_count += 1;

                    self.execute_event_batch(
                        destination_handler,
                        pending_tx,
                        state.event_batch,
                        last_executed_index,
                    )
                    .await?;

                    *state.current_command_index = next_command_index;
                    *state.processed_count += batch_len;
                    state.event_batch.clear();
                }
            }

            record_index += 1;
        }

        Ok(())
    }

    /// Execute a batch of `ChangeEvent`s against an event-mode destination,
    /// staging resume progress (`last_executed_command_index` in record units)
    /// via the pre-commit hook so the on-disk index advances atomically with
    /// the destination commit.
    async fn execute_event_batch(
        self: &Arc<Self>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        pending_tx: &PendingTransactionFile,
        events: &[ChangeEvent],
        last_executed_index: usize,
    ) -> Result<()> {
        let metadata_path = pending_tx.file_path.clone();
        let file_manager = self.clone();
        let staged_index = last_executed_index;

        let pre_commit_hook: Option<PreCommitHook> = Some(Box::new(move || {
            let metadata_path = metadata_path.clone();
            let file_manager = file_manager.clone();
            Box::pin(async move {
                file_manager
                    .stage_pending_metadata_progress(&metadata_path, staged_index)
                    .await?;
                Ok(())
            })
        }));

        destination_handler
            .execute_events_batch_with_hook(
                events,
                pending_tx.metadata.transaction_id,
                pending_tx.metadata.commit_timestamp,
                pending_tx.metadata.commit_lsn,
                pre_commit_hook,
            )
            .await?;
        self.flush_staged_pending_progress().await?;
        Ok(())
    }

    /// Process a single transaction file
    /// Reads SQL commands from the file, executes them via the destination handler in batches,
    /// updates LSN tracking, and deletes the file upon success.
    /// This method supports resumable processing: it tracks the position after each batch
    /// and can resume from where it left off if interrupted.
    ///
    /// Checks cancellation token between batches to support graceful shutdown.
    pub async fn process_transaction_file(
        self: Arc<Self>,
        pending_tx: &PendingTransactionFile,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        lsn_tracker: &Arc<LsnTracker>,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    ) -> Result<()> {
        // Skip transactions already applied (position-tracking based deduplication)
        if let Some(commit_lsn) = pending_tx.metadata.commit_lsn {
            let current_flush_lsn = lsn_tracker.get();
            if commit_lsn.0 <= current_flush_lsn && current_flush_lsn > 0 {
                info!(
                    "Skipping already-applied transaction {} (commit_lsn {} <= flush_lsn {})",
                    pending_tx.metadata.transaction_id,
                    commit_lsn,
                    pg_walstream::format_lsn(current_flush_lsn)
                );
                if let Err(e) = self.delete_pending_transaction(&pending_tx.file_path).await {
                    warn!("Failed to delete duplicate pending file: {}", e);
                }
                // This file was counted at startup seed (`list_pending_transactions().len()`) but bypasses the normal finalize decrement path, so decrement here to keep the monitoring counter accurate. (Dedup-skip path only.)
                self.pending_tx_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                        Some(c.saturating_sub(1))
                    })
                    .ok();
                return Ok(());
            }
        }

        let start_time = Instant::now();
        let tx_id = pending_tx.metadata.transaction_id;

        // RENDER MODE (not storage format): SQL destinations render each
        // ChangeEvent to SQL lazily; event-mode destinations (Kafka) pass the
        // ChangeEvent through. Storage is uniformly binary MessagePack for both.
        let render_sql = !self.event_mode;

        let latest_metadata = self.read_metadata(&pending_tx.file_path).await?;
        // Resume is uniform in RECORD units: last_executed_command_index counts
        // records (1 per event), so the next record to apply is +1.
        let start_index = latest_metadata
            .last_executed_command_index
            .map(|idx| idx + 1)
            .unwrap_or(0);

        info!(
            "Processing transaction file: {} (tx_id: {}, lsn: {:?}, start_record_index: {}, render_sql: {})",
            pending_tx.file_path.display(),
            tx_id,
            pending_tx.metadata.commit_lsn,
            start_index,
            render_sql
        );

        let mut segments = if !latest_metadata.segments.is_empty() {
            latest_metadata.segments
        } else {
            pending_tx.metadata.segments.clone()
        };

        if segments.is_empty() {
            return Err(CdcError::generic(format!(
                "No transaction segments found for tx {}",
                tx_id
            )));
        }

        let mut sql_batch: Vec<String> = Vec::with_capacity(batch_size);
        let mut sql_event_batch: Vec<ChangeEvent> = Vec::with_capacity(batch_size);
        let mut event_batch: Vec<ChangeEvent> = Vec::with_capacity(batch_size);
        let mut batch_count = 0usize;
        let mut processed_count = 0usize;
        let mut current_command_index = start_index;
        let mut remaining_start_index = start_index;

        let mut state = RecordProcessingState {
            sql_batch: &mut sql_batch,
            sql_event_batch: &mut sql_event_batch,
            event_batch: &mut event_batch,
            current_command_index: &mut current_command_index,
            processed_count: &mut processed_count,
            batch_count: &mut batch_count,
        };

        for segment in segments.drain(..) {
            // Per-segment skip arithmetic, now in RECORD units (segment
            // statement_count == record count, 1 per event from the producer).
            // A fully-applied segment is skipped without opening the file.
            if remaining_start_index > 0
                && segment.statement_count > 0
                && remaining_start_index >= segment.statement_count
            {
                remaining_start_index -= segment.statement_count;
                continue;
            }

            let segment_start_index = remaining_start_index;
            remaining_start_index = 0;

            self.process_segment_records(
                &segment.path,
                segment_start_index,
                render_sql,
                pending_tx,
                destination_handler,
                cancellation_token,
                metrics_collector,
                batch_size,
                &mut state,
            )
            .await?;
        }

        // Flush the trailing partial batch (SQL or event mode).
        if !sql_batch.is_empty() || !event_batch.is_empty() {
            if cancellation_token.is_cancelled() {
                return Err(CdcError::cancelled(
                    "Transaction file processing cancelled by shutdown signal",
                ));
            }

            if render_sql {
                let batch_len = sql_batch.len();
                let next_command_index = current_command_index + batch_len;
                let last_executed_index = next_command_index - 1;
                batch_count += 1;

                self.execute_batch_with_bulk_detection(
                    destination_handler,
                    &pending_tx.file_path,
                    &sql_batch,
                    &sql_event_batch,
                    last_executed_index,
                    batch_count,
                    metrics_collector,
                    self.bulk_insert_threshold,
                )
                .await?;

                current_command_index = next_command_index;
                processed_count += batch_len;
                sql_batch.clear();
                sql_event_batch.clear();
            } else {
                let batch_len = event_batch.len();
                let next_command_index = current_command_index + batch_len;
                let last_executed_index = next_command_index - 1;
                batch_count += 1;

                self.execute_event_batch(
                    destination_handler,
                    pending_tx,
                    &event_batch,
                    last_executed_index,
                )
                .await?;

                current_command_index = next_command_index;
                processed_count += batch_len;
                event_batch.clear();
            }
        }

        let total_commands = current_command_index;

        if processed_count == 0 {
            info!(
                "All records already executed for transaction file: {} (tx_id: {})",
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
            "Successfully executed {} remaining records ({} total) in {} batches in {:?} (tx_id: {}, avg: {:?}/batch)",
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

            // 2. Update apply_lsn and flush_lsn - transaction is now delivered to destination
            shared_lsn_feedback.update_applied_lsn(commit_lsn.0);
            shared_lsn_feedback.update_flushed_lsn(commit_lsn.0);

            // 3. Persist LSN to disk immediately for crash safety
            if let Err(e) = lsn_tracker.persist_async().await {
                warn!("Failed to persist LSN after transaction {}: {}", tx_id, e);
            }

            info!(
                "Updated apply_lsn and flush_lsn to {} (transaction {} delivered to destination)",
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

        // Decrement unconditionally to match the unconditional increment in
        // `commit_transaction`: any transaction that reached the committed/pending
        // state was counted, so it must be uncounted here regardless of
        // `commit_lsn`. `fetch_update` applies a saturating decrement to the atomic itself, so a spurious finalize at count 0 cannot wrap the counter to `usize::MAX` in release builds (where `debug_assert!` is off).
        let prev = self
            .pending_tx_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                Some(c.saturating_sub(1))
            })
            .unwrap_or(0);
        debug_assert!(prev > 0, "pending counter underflow");
        let pending_count = prev.saturating_sub(1);

        if pending_tx.metadata.commit_lsn.is_some() {
            lsn_tracker.update_consumer_state(
                tx_id,
                pending_tx.metadata.commit_timestamp,
                pending_count,
            );

            debug!(
                "Updated LSN tracker consumer state: tx_id={}, pending_count={} (in-memory)",
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
    use pg_walstream::{ColumnValue, RowData};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Create a minimal `TransactionManager` for unit-testing `format_value`
    /// and SQL generation helpers without filesystem side-effects.
    async fn test_manager(dest: DestinationType) -> TransactionManager {
        let dir = std::env::temp_dir().join(format!(
            "pg2any_format_value_test_{}_{}",
            dest,
            std::process::id()
        ));
        let _ = tokio::fs::create_dir_all(&dir).await;
        TransactionManager::new(&dir, dest, None, 10 * 1024 * 1024)
            .await
            .expect("test manager creation should succeed")
    }

    /// Build a `TransactionManager` in a unique temp dir for tests that need
    /// real filesystem transaction lifecycle (begin/append/commit).
    async fn test_manager_fs() -> (TransactionManager, std::path::PathBuf) {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pg2any_tx_lifecycle_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = tokio::fs::create_dir_all(&dir).await;
        let mgr = TransactionManager::new(&dir, DestinationType::MySQL, None, 10 * 1024 * 1024)
            .await
            .expect("test manager creation should succeed");
        (mgr, dir)
    }

    /// Begin a normal transaction, append one INSERT event, and flush buffers so
    /// the transaction is ready to commit.
    async fn seed_simple_transaction(manager: &TransactionManager, tx_id: u32) {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("test")),
        ]);
        let event = ChangeEvent::insert("public", "test", 12345, data, crate::types::Lsn(1));
        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .expect("begin_transaction should succeed");
        manager
            .append_event(tx_id, &event)
            .await
            .expect("append_event should succeed");
        manager
            .flush_all_buffers()
            .await
            .expect("flush_all_buffers should succeed");
    }

    #[tokio::test]
    async fn test_commit_transaction_returns_inmemory_metadata() {
        let (manager, _tmp) = test_manager_fs().await;
        let tx_id = 42;
        seed_simple_transaction(&manager, tx_id).await;

        let (path, metadata) = manager
            .commit_transaction(tx_id, Some(crate::types::Lsn(100)))
            .await
            .unwrap();

        // Returned in-memory metadata must match what was persisted to disk.
        let on_disk = manager.read_metadata(&path).await.unwrap();
        assert_eq!(metadata.transaction_id, tx_id);
        assert_eq!(metadata.transaction_id, on_disk.transaction_id);
        assert_eq!(metadata.commit_lsn, on_disk.commit_lsn);
        assert_eq!(metadata.segments.len(), on_disk.segments.len());
    }

    #[tokio::test]
    async fn test_pending_counter_tracks_commit_and_finalize() {
        let (manager, _tmp) = test_manager_fs().await;
        assert_eq!(manager.pending_count(), 0);

        seed_simple_transaction(&manager, 1).await;
        manager
            .commit_transaction(1, Some(crate::types::Lsn(10)))
            .await
            .unwrap();
        assert_eq!(manager.pending_count(), 1, "commit should increment");

        // Counter must equal the authoritative dir-scan count.
        let scanned = manager.list_pending_transactions().await.unwrap().len();
        assert_eq!(manager.pending_count(), scanned);
    }

    /// Producer must write each persisted event as ONE decodable length-framed
    /// MessagePack record to a `.mpk` segment. A multi-table TRUNCATE is a single
    /// event => a single record (record-unit counting), unlike the old
    /// SQL-statement counting where it rendered N statements.
    #[tokio::test]
    async fn test_statement_count_is_record_count() {
        let (manager, _tmp) = test_manager_fs().await;
        let tx_id = 7;

        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();

        // An ordinary INSERT (1 record) ...
        let data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);
        let insert = ChangeEvent::insert("public", "test", 12345, data, crate::types::Lsn(1));
        manager.append_event(tx_id, &insert).await.unwrap();

        // ... alongside a multi-table TRUNCATE which is still ONE event/record.
        let truncate = ChangeEvent {
            event_type: EventType::Truncate(vec![
                std::sync::Arc::from("public.a"),
                std::sync::Arc::from("public.b"),
                std::sync::Arc::from("public.c"),
            ]),
            lsn: crate::types::Lsn(2),
            metadata: None,
        };
        manager.append_event(tx_id, &truncate).await.unwrap();

        manager.flush_all_buffers().await.unwrap();
        let (_path, metadata) = manager
            .commit_transaction(tx_id, Some(crate::types::Lsn(100)))
            .await
            .unwrap();

        // 2 events => 2 records (record-unit count).
        let stored: usize = metadata.segments.iter().map(|s| s.statement_count).sum();
        assert_eq!(stored, 2, "record count must be 1 per event (2 events)");

        // The segment must decode back to exactly 2 records, then EOF.
        let mut decoded = 0usize;
        for seg in &metadata.segments {
            let bytes = std::fs::read(&seg.path).unwrap();
            let mut cur = std::io::Cursor::new(bytes);
            while crate::storage::binary_record::decode_record(&mut cur)
                .unwrap()
                .is_some()
            {
                decoded += 1;
            }
        }
        assert_eq!(decoded, 2, "consumer must read back 2 binary records");
    }

    /// Producer writes a decodable `.mpk` segment: one INSERT event round-trips
    /// to one record, and a second decode at EOF returns None.
    #[tokio::test]
    async fn test_producer_writes_decodable_binary_records() {
        let (manager, _tmp) = test_manager_fs().await;
        let tx_id = 7;
        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let ev = ChangeEvent::insert("public", "users", 16384, data, crate::types::Lsn(5));
        manager.append_event(tx_id, &ev).await.unwrap();
        manager.flush_all_buffers().await.unwrap();
        let (_path, metadata) = manager
            .commit_transaction(tx_id, Some(crate::types::Lsn(10)))
            .await
            .unwrap();

        let seg = &metadata.segments[0].path;
        assert_eq!(
            seg.extension().and_then(|e| e.to_str()),
            Some("mpk"),
            "data segment must use .mpk extension"
        );
        let bytes = std::fs::read(seg).unwrap();
        let mut cur = std::io::Cursor::new(bytes);
        let got = crate::storage::binary_record::decode_record(&mut cur)
            .unwrap()
            .unwrap();
        assert!(matches!(
            got.event_type,
            crate::types::EventType::Insert { .. }
        ));
        assert!(crate::storage::binary_record::decode_record(&mut cur)
            .unwrap()
            .is_none());
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

    // ── Dialect snapshot safety net ─────────────────────────────────
    //
    // These five tests pin byte-exact output of the dialect-leaking helpers
    // (`append_quoted_identifier`, `append_value`, `append_qualified_table`,
    // `append_hex_literal`, `generate_truncate_sql`) across every
    // `DestinationType`. They exist to gate the upcoming `SqlDialect`
    // extraction refactor: any change in observable SQL output will fail
    // these tests. Expected strings were captured verbatim from the current
    // implementation — do not hand-edit, regenerate from the scratch test.

    fn all_dests() -> [(&'static str, DestinationType); 5] {
        [
            ("MySQL", DestinationType::MySQL),
            ("SqlServer", DestinationType::SqlServer),
            ("SQLite", DestinationType::SQLite),
            ("Kafka", DestinationType::Kafka),
            ("Custom", DestinationType::Custom("test".to_string())),
        ]
    }

    #[tokio::test]
    async fn snapshot_quote_identifier() {
        let ids = ["users", "back`tick", "bra]cket", "double\"quote"];
        let expected: &[(&str, [&str; 4])] = &[
            // MySQL — backtick quoting, only backticks escaped
            (
                "MySQL",
                ["`users`", "`back``tick`", "`bra]cket`", "`double\"quote`"],
            ),
            // SqlServer — bracket quoting, only `]` escaped
            (
                "SqlServer",
                ["[users]", "[back`tick]", "[bra]]cket]", "[double\"quote]"],
            ),
            // SQLite — double-quote quoting, only `"` escaped
            (
                "SQLite",
                [
                    "\"users\"",
                    "\"back`tick\"",
                    "\"bra]cket\"",
                    "\"double\"\"quote\"",
                ],
            ),
            // Kafka — double-quote quoting (same as SQLite)
            (
                "Kafka",
                [
                    "\"users\"",
                    "\"back`tick\"",
                    "\"bra]cket\"",
                    "\"double\"\"quote\"",
                ],
            ),
            // Custom — double-quote quoting (same as SQLite)
            (
                "Custom",
                [
                    "\"users\"",
                    "\"back`tick\"",
                    "\"bra]cket\"",
                    "\"double\"\"quote\"",
                ],
            ),
        ];
        for (name, dest) in all_dests().iter() {
            let mgr = test_manager(dest.clone()).await;
            let row = expected.iter().find(|(n, _)| n == name).unwrap();
            for (i, id) in ids.iter().enumerate() {
                assert_eq!(
                    mgr.quote_identifier(id),
                    row.1[i],
                    "quote_identifier({:?}) for {}",
                    id,
                    name
                );
            }
        }
    }

    #[tokio::test]
    async fn snapshot_format_value() {
        use bytes::Bytes;
        let vals: Vec<(&str, ColumnValue)> = vec![
            ("Null", ColumnValue::Null),
            ("text_t", ColumnValue::text("t")),
            ("text_f", ColumnValue::text("f")),
            ("text_hello", ColumnValue::text("hello")),
            ("text_oreilly", ColumnValue::text("o'reilly")),
            ("text_backslash", ColumnValue::text("back\\slash")),
            (
                "binary",
                ColumnValue::Binary(Bytes::from_static(&[0x00, 0xff, 0xab])),
            ),
        ];
        let expected: &[(&str, [&str; 7])] = &[
            // MySQL — pgoutput booleans → 1/0, single quotes doubled, backslash doubled, binary → X'…'
            (
                "MySQL",
                [
                    "NULL",
                    "1",
                    "0",
                    "'hello'",
                    "'o''reilly'",
                    "'back\\\\slash'",
                    "X'00ffab'",
                ],
            ),
            // SqlServer — booleans → 1/0, single quote doubled, backslash NOT doubled, binary → 0x…
            (
                "SqlServer",
                [
                    "NULL",
                    "1",
                    "0",
                    "'hello'",
                    "'o''reilly'",
                    "'back\\slash'",
                    "0x00ffab",
                ],
            ),
            // SQLite — booleans → 1/0, single quote doubled, backslash NOT doubled, binary → X'…'
            (
                "SQLite",
                [
                    "NULL",
                    "1",
                    "0",
                    "'hello'",
                    "'o''reilly'",
                    "'back\\slash'",
                    "X'00ffab'",
                ],
            ),
            // Kafka — same as SQLite (text formatting path is not dialect-specific outside MySQL)
            (
                "Kafka",
                [
                    "NULL",
                    "1",
                    "0",
                    "'hello'",
                    "'o''reilly'",
                    "'back\\slash'",
                    "X'00ffab'",
                ],
            ),
            // Custom — same as SQLite
            (
                "Custom",
                [
                    "NULL",
                    "1",
                    "0",
                    "'hello'",
                    "'o''reilly'",
                    "'back\\slash'",
                    "X'00ffab'",
                ],
            ),
        ];
        for (name, dest) in all_dests().iter() {
            let mgr = test_manager(dest.clone()).await;
            let row = expected.iter().find(|(n, _)| n == name).unwrap();
            for (i, (vname, val)) in vals.iter().enumerate() {
                assert_eq!(
                    mgr.format_value(val),
                    row.1[i],
                    "format_value({}) for {}",
                    vname,
                    name
                );
            }
        }
    }

    #[tokio::test]
    async fn snapshot_qualified_table() {
        let tables = [("public", "users"), ("custom", "items")];
        let expected: &[(&str, [&str; 2])] = &[
            // MySQL — schema.table both backticked
            ("MySQL", ["`public`.`users`", "`custom`.`items`"]),
            // SqlServer — schema.table both bracketed
            ("SqlServer", ["[public].[users]", "[custom].[items]"]),
            // SQLite — schema dropped, table-only
            ("SQLite", ["\"users\"", "\"items\""]),
            // Kafka — schema dropped, table-only (same as SQLite)
            ("Kafka", ["\"users\"", "\"items\""]),
            // Custom — schema.table both double-quoted (schema preserved)
            ("Custom", ["\"public\".\"users\"", "\"custom\".\"items\""]),
        ];
        for (name, dest) in all_dests().iter() {
            let mgr = test_manager(dest.clone()).await;
            let row = expected.iter().find(|(n, _)| n == name).unwrap();
            for (i, (s, t)) in tables.iter().enumerate() {
                let mut out = String::new();
                mgr.append_qualified_table(&mut out, s, t);
                assert_eq!(
                    out, row.1[i],
                    "append_qualified_table({}.{}) for {}",
                    s, t, name
                );
            }
        }
    }

    #[tokio::test]
    async fn snapshot_hex_literal() {
        let blobs: Vec<(&str, Vec<u8>)> = vec![
            ("empty", vec![]),
            ("deadbeef", vec![0xde, 0xad, 0xbe, 0xef]),
        ];
        let expected: &[(&str, [&str; 2])] = &[
            // MySQL — X'…' literal
            ("MySQL", ["X''", "X'deadbeef'"]),
            // SqlServer — 0x… literal (no prefix/suffix quotes)
            ("SqlServer", ["0x", "0xdeadbeef"]),
            // SQLite — X'…' literal
            ("SQLite", ["X''", "X'deadbeef'"]),
            // Kafka — X'…' literal
            ("Kafka", ["X''", "X'deadbeef'"]),
            // Custom — X'…' literal
            ("Custom", ["X''", "X'deadbeef'"]),
        ];
        for (name, dest) in all_dests().iter() {
            let mgr = test_manager(dest.clone()).await;
            let row = expected.iter().find(|(n, _)| n == name).unwrap();
            for (i, (bname, bytes)) in blobs.iter().enumerate() {
                let mut out = String::new();
                mgr.append_hex_literal(&mut out, bytes);
                assert_eq!(out, row.1[i], "append_hex_literal({}) for {}", bname, name);
            }
        }
    }

    #[tokio::test]
    async fn snapshot_truncate_sql() {
        let inputs: Vec<(&str, Vec<Arc<str>>)> = vec![
            ("public.users", vec![Arc::from("public.users")]),
            ("users_bare", vec![Arc::from("users")]),
            ("two_tables", vec![Arc::from("a.b"), Arc::from("c.d")]),
        ];
        let expected: &[(&str, [&str; 3])] = &[
            // MySQL — TRUNCATE TABLE `schema`.`table`; bare-name implicitly `public`
            (
                "MySQL",
                [
                    "TRUNCATE TABLE `public`.`users`;",
                    "TRUNCATE TABLE `public`.`users`;",
                    "TRUNCATE TABLE `a`.`b`;\nTRUNCATE TABLE `c`.`d`;",
                ],
            ),
            // SqlServer — TRUNCATE TABLE [schema].[table];
            (
                "SqlServer",
                [
                    "TRUNCATE TABLE [public].[users];",
                    "TRUNCATE TABLE [public].[users];",
                    "TRUNCATE TABLE [a].[b];\nTRUNCATE TABLE [c].[d];",
                ],
            ),
            // SQLite — DELETE FROM "table"; (schema dropped)
            (
                "SQLite",
                [
                    "DELETE FROM \"users\";",
                    "DELETE FROM \"users\";",
                    "DELETE FROM \"b\";\nDELETE FROM \"d\";",
                ],
            ),
            // Kafka — no-op: empty string per table (no statements emitted)
            ("Kafka", ["", "", ""]),
            // Custom — TRUNCATE TABLE "schema"."table";
            (
                "Custom",
                [
                    "TRUNCATE TABLE \"public\".\"users\";",
                    "TRUNCATE TABLE \"public\".\"users\";",
                    "TRUNCATE TABLE \"a\".\"b\";\nTRUNCATE TABLE \"c\".\"d\";",
                ],
            ),
        ];
        for (name, dest) in all_dests().iter() {
            let mgr = test_manager(dest.clone()).await;
            let row = expected.iter().find(|(n, _)| n == name).unwrap();
            for (i, (iname, tables)) in inputs.iter().enumerate() {
                let out = mgr.generate_truncate_sql(tables).unwrap();
                assert_eq!(
                    out, row.1[i],
                    "generate_truncate_sql({}) for {}",
                    iname, name
                );
            }
        }
    }

    // ── End-to-end binary format: producer → disk → consumer ──────────
    //
    // These exercise the full Phase 3 path: producer MessagePack-serializes events to
    // `.mpk` segments, the consumer decodes records and either renders SQL
    // (SQL destinations) or passes ChangeEvents through (event-mode), with
    // record-unit crash resume.

    use crate::lsn_tracker::{LsnTracker, SharedLsnFeedback};
    use std::sync::Mutex as StdMutex;

    /// Mock destination that captures SQL batches OR event batches, configurable
    /// for SQL-mode vs event-mode, used by the e2e consumer tests.
    struct CapturingDestination {
        event_mode: bool,
        sql: Arc<StdMutex<Vec<String>>>,
        events: Arc<StdMutex<Vec<ChangeEvent>>>,
    }

    impl CapturingDestination {
        fn sql_mode() -> (Self, Arc<StdMutex<Vec<String>>>) {
            let sql = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    event_mode: false,
                    sql: sql.clone(),
                    events: Arc::new(StdMutex::new(Vec::new())),
                },
                sql,
            )
        }

        fn event_mode() -> (Self, Arc<StdMutex<Vec<ChangeEvent>>>) {
            let events = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    event_mode: true,
                    sql: Arc::new(StdMutex::new(Vec::new())),
                    events: events.clone(),
                },
                events,
            )
        }
    }

    #[async_trait::async_trait]
    impl DestinationHandler for CapturingDestination {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

        async fn execute_sql_batch_with_hook(
            &mut self,
            commands: &[String],
            pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            self.sql.lock().unwrap().extend_from_slice(commands);
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }
            Ok(())
        }

        fn supports_event_mode(&self) -> bool {
            self.event_mode
        }

        async fn execute_events_batch_with_hook(
            &mut self,
            events: &[ChangeEvent],
            _transaction_id: u32,
            _commit_timestamp: DateTime<Utc>,
            _commit_lsn: Option<Lsn>,
            pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            self.events.lock().unwrap().extend_from_slice(events);
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    async fn run_consumer(
        manager: &Arc<TransactionManager>,
        pending: &PendingTransactionFile,
        handler: &mut Box<dyn DestinationHandler>,
        batch_size: usize,
    ) {
        let tracker = LsnTracker::new(None).await;
        let metrics = Arc::new(MetricsCollector::new());
        let feedback = SharedLsnFeedback::new_shared();
        let cancel = CancellationToken::new();
        manager
            .clone()
            .process_transaction_file(
                pending, handler, &cancel, &tracker, &metrics, batch_size, &feedback,
            )
            .await
            .expect("process_transaction_file should succeed");
    }

    fn sample_events() -> Vec<ChangeEvent> {
        vec![
            ChangeEvent::insert(
                "public",
                "users",
                100,
                RowData::from_pairs(vec![("id", ColumnValue::text("1"))]),
                crate::types::Lsn(1),
            ),
            ChangeEvent::insert(
                "public",
                "users",
                100,
                RowData::from_pairs(vec![("id", ColumnValue::text("2"))]),
                crate::types::Lsn(2),
            ),
            ChangeEvent::insert(
                "public",
                "users",
                100,
                RowData::from_pairs(vec![("id", ColumnValue::text("3"))]),
                crate::types::Lsn(3),
            ),
        ]
    }

    /// SQL destination: consumer renders the same SQL that `sql_renderer`
    /// produces for each event, in order.
    #[tokio::test]
    async fn test_e2e_sql_destination_renders_in_order() {
        let (mgr, dir) = test_manager_fs().await;
        let manager = Arc::new(mgr);
        let tx_id = 11;
        let events = sample_events();

        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();
        for ev in &events {
            manager.append_event(tx_id, ev).await.unwrap();
        }
        manager.flush_all_buffers().await.unwrap();
        let (path, metadata) = manager
            .commit_transaction(tx_id, Some(crate::types::Lsn(100)))
            .await
            .unwrap();
        let pending = PendingTransactionFile {
            file_path: path,
            metadata,
        };

        let (dest, captured) = CapturingDestination::sql_mode();
        let mut handler: Box<dyn DestinationHandler> = Box::new(dest);
        run_consumer(&manager, &pending, &mut handler, 2).await;

        // Expected SQL is exactly what sql_renderer produces for each event.
        let ctx = manager.render_ctx();
        let expected: Vec<String> = events
            .iter()
            .map(|ev| crate::sql_renderer::generate_sql_for_event(&ctx, ev).unwrap())
            .collect();
        assert_eq!(*captured.lock().unwrap(), expected);
        let _ = dir;
    }

    /// Event-mode destination: consumer passes the decoded ChangeEvents through
    /// to execute_events_batch_with_hook, in order.
    #[tokio::test]
    async fn test_e2e_event_mode_passes_events_in_order() {
        let (mgr, _dir) = test_manager_fs().await;
        let mut mgr = mgr;
        mgr.set_event_mode(true); // selects RENDER MODE = event pass-through
        let manager = Arc::new(mgr);
        let tx_id = 12;
        let events = sample_events();

        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();
        for ev in &events {
            manager.append_event(tx_id, ev).await.unwrap();
        }
        manager.flush_all_buffers().await.unwrap();
        let (path, metadata) = manager
            .commit_transaction(tx_id, Some(crate::types::Lsn(100)))
            .await
            .unwrap();
        let pending = PendingTransactionFile {
            file_path: path,
            metadata,
        };

        let (dest, captured) = CapturingDestination::event_mode();
        let mut handler: Box<dyn DestinationHandler> = Box::new(dest);
        run_consumer(&manager, &pending, &mut handler, 2).await;

        let got = captured.lock().unwrap();
        assert_eq!(got.len(), 3, "all 3 events passed through");
        for (g, e) in got.iter().zip(events.iter()) {
            assert_eq!(g.lsn, e.lsn);
        }
    }

    /// Crash resume: with last_executed_command_index = 0 (record units), the
    /// consumer skips exactly the first record and applies the remainder.
    #[tokio::test]
    async fn test_e2e_resume_skips_record_units() {
        let (mgr, _dir) = test_manager_fs().await;
        let manager = Arc::new(mgr);
        let tx_id = 13;
        let events = sample_events();

        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();
        for ev in &events {
            manager.append_event(tx_id, ev).await.unwrap();
        }
        manager.flush_all_buffers().await.unwrap();
        let (path, mut metadata) = manager
            .commit_transaction(tx_id, Some(crate::types::Lsn(100)))
            .await
            .unwrap();

        // Simulate a prior run that already applied record index 0.
        metadata.last_executed_command_index = Some(0);
        manager
            .write_pending_metadata(&path, &metadata)
            .await
            .unwrap();

        let pending = PendingTransactionFile {
            file_path: path,
            metadata,
        };

        let (dest, captured) = CapturingDestination::sql_mode();
        let mut handler: Box<dyn DestinationHandler> = Box::new(dest);
        run_consumer(&manager, &pending, &mut handler, 10).await;

        // Records 1 and 2 only (record 0 skipped).
        let ctx = manager.render_ctx();
        let expected: Vec<String> = events[1..]
            .iter()
            .map(|ev| crate::sql_renderer::generate_sql_for_event(&ctx, ev).unwrap())
            .collect();
        assert_eq!(*captured.lock().unwrap(), expected);
    }
}
