//! Relay log management for PostgreSQL logical replication
//!
//! This module implements MySQL-style relay logs for PostgreSQL logical replication,
//! providing persistent storage and async I/O for better performance and fault tolerance.

use crate::error::{CdcError, Result};
use crate::types::ChangeEvent;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info};

/// Relay log entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayLogEntry {
    /// Sequential ID for ordering
    pub sequence_id: u64,
    /// Source LSN from PostgreSQL
    pub source_lsn: Option<String>,
    /// Timestamp when written to relay log
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// The actual change event
    pub event: ChangeEvent,
}

/// Configuration for relay log management
#[derive(Debug, Clone)]
pub struct RelayLogConfig {
    /// Directory to store relay log files
    pub log_directory: PathBuf,
    /// Maximum size per log file (in bytes)
    pub max_file_size: u64,
    /// Maximum number of log files to keep
    pub max_files: u32,
    /// Buffer size for writing
    pub write_buffer_size: usize,
    /// Buffer size for reading
    pub read_buffer_size: usize,
    // /// Sync interval for flushing writes
    // pub sync_interval: std::time::Duration,
}

impl RelayLogConfig {
    pub fn new(relay_log_dir: String) -> RelayLogConfig {
        Self {
            log_directory: PathBuf::from(relay_log_dir),
            max_file_size: 100 * 1024 * 1024, // 100MB
            max_files: 100,
            write_buffer_size: 64 * 1024, // 64KB
            read_buffer_size: 64 * 1024,  // 64KB
        }
    }
}

/// Manages relay log files for replication
pub struct RelayLogManager {
    config: RelayLogConfig,
    current_file_index: AtomicU64,
    current_sequence: AtomicU64,
    log_directory: PathBuf,
    writer_state: Arc<Mutex<WriterState>>,
    reader_state: Arc<RwLock<ReaderState>>,
}

struct WriterState {
    current_file: Option<BufWriter<File>>,
    current_file_size: u64,
    current_file_index: u64,
}

struct ReaderState {
    current_file: Option<BufReader<File>>,
    current_file_index: u64,
    current_sequence: u64,
}

impl RelayLogManager {
    /// Create a new relay log manager
    pub async fn new(config: RelayLogConfig) -> Result<Self> {
        // Ensure log directory exists
        tokio::fs::create_dir_all(&config.log_directory).await?;

        // Find existing files and determine current index
        let current_file_index = Self::find_latest_file_index(&config.log_directory).await?;
        let current_sequence = Self::find_latest_sequence(&config.log_directory).await?;

        info!(
            "Initialized relay log manager at {} with file_index={}, sequence={}",
            config.log_directory.display(),
            current_file_index,
            current_sequence
        );

        Ok(Self {
            log_directory: config.log_directory.clone(),
            config,
            current_file_index: AtomicU64::new(current_file_index),
            current_sequence: AtomicU64::new(current_sequence),
            writer_state: Arc::new(Mutex::new(WriterState {
                current_file: None,
                current_file_size: 0,
                current_file_index,
            })),
            reader_state: Arc::new(RwLock::new(ReaderState {
                current_file: None,
                current_file_index: 0,
                current_sequence: 0,
            })),
        })
    }

    /// Find the latest file index in the log directory
    async fn find_latest_file_index(log_dir: &Path) -> Result<u64> {
        let mut entries = tokio::fs::read_dir(log_dir).await?;
        let mut max_index = 0u64;

        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            if let Some(file_str) = file_name.to_str() {
                if file_str.starts_with("relay-") && file_str.ends_with(".log") {
                    // Extract index from filename like "relay-000001.log"
                    let index_str = &file_str[6..file_str.len() - 4];
                    if let Ok(index) = index_str.parse::<u64>() {
                        max_index = max_index.max(index);
                    }
                }
            }
        }

        Ok(max_index)
    }

    /// Find the latest sequence number by reading the last entry
    async fn find_latest_sequence(log_dir: &Path) -> Result<u64> {
        let latest_file_index = Self::find_latest_file_index(log_dir).await?;
        if latest_file_index == 0 {
            return Ok(0);
        }

        let file_path = Self::get_log_file_path(log_dir, latest_file_index);
        if !file_path.exists() {
            return Ok(0);
        }

        let file = File::open(&file_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut last_sequence = 0u64;

        while let Some(line) = lines.next_line().await? {
            if !line.trim().is_empty() {
                if let Ok(entry) = serde_json::from_str::<RelayLogEntry>(&line) {
                    last_sequence = entry.sequence_id;
                }
            }
        }

        Ok(last_sequence)
    }

    /// Get the path for a relay log file
    fn get_log_file_path(log_dir: &Path, index: u64) -> PathBuf {
        log_dir.join(format!("relay-{:06}.log", index))
    }

    /// Create a new relay log writer
    pub async fn create_writer(&self) -> Result<RelayLogWriter> {
        RelayLogWriter::new(self.config.clone(), self.writer_state.clone()).await
    }

    /// Create a new relay log reader
    pub async fn create_reader(
        &self,
        file_name: String,
        start_sequence: u64,
    ) -> Result<RelayLogReader> {
        RelayLogReader::new(
            self.config.clone(),
            self.reader_state.clone(),
            start_sequence,
            file_name,
        )
        .await
    }

    /// Get current statistics
    pub fn get_stats(&self) -> RelayLogStats {
        RelayLogStats {
            current_file_index: self.current_file_index.load(Ordering::Relaxed),
            current_sequence: self.current_sequence.load(Ordering::Relaxed),
            log_directory: self.log_directory.clone(),
        }
    }
}

/// Statistics for relay log manager
#[derive(Debug, Clone)]
pub struct RelayLogStats {
    pub current_file_index: u64,
    pub current_sequence: u64,
    pub log_directory: PathBuf,
}

/// Writer for relay log files
pub struct RelayLogWriter {
    config: RelayLogConfig,
    writer_state: Arc<Mutex<WriterState>>,
    sequence_counter: AtomicU64,
}

impl RelayLogWriter {
    /// Create a new relay log writer
    async fn new(config: RelayLogConfig, writer_state: Arc<Mutex<WriterState>>) -> Result<Self> {
        Ok(Self {
            config,
            writer_state,
            sequence_counter: AtomicU64::new(0),
        })
    }

    /// Write an event to the relay log
    pub async fn write_event(&self, event: ChangeEvent, source_lsn: Option<String>) -> Result<u64> {
        let sequence_id = self.sequence_counter.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = RelayLogEntry {
            sequence_id,
            source_lsn,
            timestamp: chrono::Utc::now(),
            event,
        };

        let mut state = self.writer_state.lock().await;

        // Check if we need to rotate to a new file
        if state.current_file.is_none() || state.current_file_size >= self.config.max_file_size {
            self.rotate_file(&mut state).await?;
        }

        // Write the entry
        if let Some(ref mut writer) = state.current_file {
            let json_line = serde_json::to_string(&entry)?;
            let line_size = json_line.len() as u64 + 1;

            writer.write_all(json_line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;

            state.current_file_size += line_size;
        }

        debug!(
            "Wrote relay log entry: sequence={}, lsn={:?}",
            sequence_id, entry.source_lsn
        );
        Ok(sequence_id)
    }

    /// Rotate to a new log file
    async fn rotate_file(&self, state: &mut WriterState) -> Result<()> {
        // Close current file if exists
        if let Some(mut writer) = state.current_file.take() {
            writer.flush().await?;
            writer.shutdown().await?;
        }

        // Create new file
        state.current_file_index += 1;
        let file_path = RelayLogManager::get_log_file_path(
            &self.config.log_directory,
            state.current_file_index,
        );

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        let writer = BufWriter::with_capacity(self.config.write_buffer_size, file);
        state.current_file = Some(writer);
        state.current_file_size = 0;

        info!("Rotated to new relay log file: {}", file_path.display());
        Ok(())
    }

    /// Force flush all pending writes
    pub async fn flush(&self) -> Result<()> {
        let mut state = self.writer_state.lock().await;
        if let Some(ref mut writer) = state.current_file {
            writer.flush().await?;
        }
        Ok(())
    }
}

/// Reader for relay log files using async I/O
pub struct RelayLogReader {
    config: RelayLogConfig,
    reader_state: Arc<RwLock<ReaderState>>,
    log_directory: PathBuf,
    file_name: String,
}

impl RelayLogReader {
    /// Create a new relay log reader
    async fn new(
        config: RelayLogConfig,
        reader_state: Arc<RwLock<ReaderState>>,
        _start_sequence: u64,
        file_name: String,
    ) -> Result<Self> {
        let reader = Self {
            log_directory: config.log_directory.clone(),
            config,
            reader_state,
            file_name,
        };

        Ok(reader)
    }

    /// Read the next event from relay logs (non-blocking)
    pub async fn read_event(&self) -> Result<Option<RelayLogEntry>> {
        let mut state = self.reader_state.write().await;

        // Open current file if needed
        if state.current_file.is_none() {
            let file_path =
                RelayLogManager::get_log_file_path(&self.log_directory, state.current_file_index);
            if file_path.exists() {
                let file = File::open(&file_path).await?;
                let reader = BufReader::with_capacity(self.config.read_buffer_size, file);
                state.current_file = Some(reader);
            } else {
                // Try next file
                state.current_file_index += 1;
                return Ok(None);
            }
        }

        // Read from current file
        if let Some(ref mut reader) = state.current_file {
            let mut line = String::new();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF, try next file
                    state.current_file = None;
                    state.current_file_index += 1;
                    Ok(None)
                }
                Ok(_) => {
                    if line.trim().is_empty() {
                        return Ok(None);
                    }

                    match serde_json::from_str::<RelayLogEntry>(&line) {
                        Ok(entry) => {
                            state.current_sequence = entry.sequence_id;
                            debug!("Read relay log entry: sequence={}", entry.sequence_id);
                            Ok(Some(entry))
                        }
                        Err(e) => {
                            error!("Failed to parse relay log entry: {}", e);
                            Ok(None)
                        }
                    }
                }
                Err(e) => Err(CdcError::io(format!("Failed to read relay log: {}", e))),
            }
        } else {
            Ok(None)
        }
    }

    /// Wait for new events using tokio's async I/O
    pub async fn wait_for_events(&self) -> Result<Vec<RelayLogEntry>> {
        let mut events = Vec::new();

        // Use tokio's async file watching or polling
        // For now, implement simple polling
        loop {
            match self.read_event().await? {
                Some(event) => events.push(event),
                None => {
                    if !events.is_empty() {
                        break;
                    }
                    // Wait a bit before trying again
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChangeEvent, EventType};
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_relay_log_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig::new(temp_dir.path().to_string_lossy().to_string());

        let manager = RelayLogManager::new(config).await.unwrap();
        let writer = manager.create_writer().await.unwrap();
        let reader = manager.create_reader("".to_string(), 0).await.unwrap();

        // Create test event
        let mut data = HashMap::new();
        data.insert(
            "id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        );
        data.insert(
            "name".to_string(),
            serde_json::Value::String("test".to_string()),
        );

        let event = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".to_string(),
                table: "test_table".to_string(),
                relation_oid: 12345,
                data,
            },
            lsn: Some("0/1234567".to_string()),
            metadata: None,
        };

        // Write event
        let sequence = writer
            .write_event(event.clone(), event.lsn.clone())
            .await
            .unwrap();
        assert_eq!(sequence, 1);

        writer.flush().await.unwrap();

        // Drop the writer to ensure file is closed and data is flushed
        drop(writer);

        // Read event - try multiple times in case there's a timing issue
        let mut attempts = 0;
        let read_entry = loop {
            match reader.read_event().await.unwrap() {
                Some(entry) => break entry,
                None => {
                    attempts += 1;
                    if attempts > 10 {
                        panic!("Failed to read event after 10 attempts");
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        };
        assert_eq!(read_entry.sequence_id, 1);
        assert_eq!(read_entry.source_lsn, event.lsn);

        // Check event content
        match read_entry.event.event_type {
            EventType::Insert {
                schema,
                table,
                relation_oid,
                ..
            } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "test_table");
                assert_eq!(relation_oid, 12345);
            }
            _ => panic!("Expected Insert event"),
        }
    }

    #[tokio::test]
    async fn test_relay_log_file_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig {
            log_directory: temp_dir.path().to_path_buf(),
            max_file_size: 512, // Very small file size to trigger rotation
            max_files: 10,
            write_buffer_size: 1024,
            read_buffer_size: 1024,
        };

        let manager = RelayLogManager::new(config).await.unwrap();
        let writer = manager.create_writer().await.unwrap();

        // Write multiple events to trigger rotation
        for i in 1..=5 {
            let mut data = HashMap::new();
            data.insert(
                "id".to_string(),
                serde_json::Value::Number(serde_json::Number::from(i)),
            );

            let event = ChangeEvent {
                event_type: EventType::Insert {
                    schema: "public".to_string(),
                    table: "test_table".to_string(),
                    relation_oid: 12345,
                    data,
                },
                lsn: Some(format!("0/123456{}", i)),
                metadata: None,
            };

            writer.write_event(event, None).await.unwrap();
        }

        writer.flush().await.unwrap();

        // Check that multiple files were created
        let mut file_count = 0;
        let mut entries = tokio::fs::read_dir(&temp_dir).await.unwrap();
        while let Some(_entry) = entries.next_entry().await.unwrap() {
            file_count += 1;
        }

        assert!(
            file_count > 1,
            "Expected multiple relay log files due to rotation"
        );
    }
}
