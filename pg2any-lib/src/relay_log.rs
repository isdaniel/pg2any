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
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, error, info, warn};

/// Relay log entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayLogEntry {
    /// Sequential ID for ordering sequence_number
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
}

impl RelayLogConfig {
    pub fn new(relay_log_dir: String) -> RelayLogConfig {
        Self {
            log_directory: PathBuf::from(relay_log_dir),
            max_file_size: 500 * 1024 * 1024, // 500MB
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
    /// Notify mechanism to wake up SQL thread when new data is available
    new_data_notify: Arc<Notify>,
}

struct WriterState {
    current_file: Option<BufWriter<File>>,
    current_file_size: u64,
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

        // Find latest sequence from the single relay log file
        let relay_log_sequence = Self::find_latest_sequence(&config.log_directory).await?;

        // // Read SQL thread position if it exists
        // let sql_thread_sequence = Self::read_sql_thread_position(&config.log_directory).await?;

        // // Use the maximum of both sequences to ensure continuity
        // let current_sequence = match sql_thread_sequence {
        //     Some(sql_seq) => std::cmp::max(relay_log_sequence, sql_seq),
        //     None => relay_log_sequence,
        // };

        info!(
            "Initialized relay log manager at {} relay_log_seq={}",
            config.log_directory.display(),
            relay_log_sequence
        );

        Ok(Self {
            log_directory: config.log_directory.clone(),
            config,
            current_file_index: AtomicU64::new(1), // Always 1 for single file approach
            current_sequence: AtomicU64::new(relay_log_sequence),
            writer_state: Arc::new(Mutex::new(WriterState {
                current_file: None,
                current_file_size: 0,
            })),
            reader_state: Arc::new(RwLock::new(ReaderState {
                current_file: None,
                current_file_index: 1, // Always 1 for single file
                current_sequence: relay_log_sequence,
            })),
            new_data_notify: Arc::new(Notify::new()),
        })
    }

    /// Find the latest sequence number by reading the last entry from relay-main.log
    async fn find_latest_sequence(log_dir: &Path) -> Result<u64> {
        let file_path = Self::get_log_file_path(log_dir);
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

    /// Get the path for the single relay log file
    fn get_log_file_path(log_dir: &Path) -> PathBuf {
        log_dir.join("relay-main.log")
    }

    /// Read the SQL thread position from sql_thread_position.json if it exists
    // async fn read_sql_thread_position(log_dir: &Path) -> Result<Option<u64>> {
    //     let position_file_path = log_dir.join("sql_thread_position.json");
        
    //     if !position_file_path.exists() {
    //         return Ok(None);
    //     }

    //     let content = tokio::fs::read_to_string(&position_file_path).await?;
        
    //     match serde_json::from_str::<serde_json::Value>(&content) {
    //         Ok(json) => {
    //             if let Some(seq) = json.get("sequence_number").and_then(|v| v.as_u64()) {
    //                 Ok(Some(seq))
    //             } else {
    //                 Ok(None)
    //             }
    //         }
    //         Err(_) => Ok(None),
    //     }
    // }

    /// Create a new relay log writer  
    pub async fn create_writer(&self) -> Result<RelayLogWriter> {
        let current_sequence = self.current_sequence.load(Ordering::Relaxed);
        RelayLogWriter::new(
            self.config.clone(), 
            self.writer_state.clone(),
            self.new_data_notify.clone(),
            current_sequence,
        ).await
    }

    /// Create a new relay log reader for single file approach
    pub async fn create_reader(
        &self,
        _file_name: String, // Ignore file name since we only use relay-main.log
        start_sequence: u64,
    ) -> Result<RelayLogReader> {
        // If start_sequence is 0, use the latest sequence number from the manager
        let actual_start_sequence = if start_sequence == 0 {
            self.current_sequence.load(Ordering::Relaxed)
        } else {
            start_sequence
        };

        RelayLogReader::new(
            self.config.clone(),
            self.reader_state.clone(),
            actual_start_sequence,
            1, // Always use file index 1 for single file
            self.new_data_notify.clone(),
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

/// Writer for relay log files with notification support
pub struct RelayLogWriter {
    config: RelayLogConfig,
    writer_state: Arc<Mutex<WriterState>>,
    sequence_number: AtomicU64,
    new_data_notify: Arc<Notify>,
}

impl RelayLogWriter {
    /// Create a new relay log writer
    async fn new(
        config: RelayLogConfig, 
        writer_state: Arc<Mutex<WriterState>>,
        new_data_notify: Arc<Notify>,
        starting_sequence: u64,
    ) -> Result<Self> {
        Ok(Self {
            config,
            writer_state,
            sequence_number: AtomicU64::new(starting_sequence),
            new_data_notify,
        })
    }

    /// Write an event to the relay log and notify waiting readers
    pub async fn write_event(&self, event: ChangeEvent, source_lsn: Option<String>) -> Result<u64> {
        let sequence_number = self.sequence_number.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = RelayLogEntry {
            sequence_id: sequence_number,
            source_lsn,
            timestamp: chrono::Utc::now(),
            event,
        };

        let mut state = self.writer_state.lock().await;

        // Ensure the relay log file is open (no size limits for single file approach)
        self.ensure_file_open(&mut state).await?;

        // Write the entry
        if let Some(ref mut writer) = state.current_file {
            let json_line = serde_json::to_string(&entry)?;
            let line_size = json_line.len() as u64 + 1;

            writer.write_all(json_line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;

            state.current_file_size += line_size;
        }

        // Notify waiting SQL threads that new data is available
        self.new_data_notify.notify_waiters();

        debug!(
            "Wrote relay log entry: sequence={}, lsn={:?}",
            sequence_number, entry.source_lsn
        );
        Ok(sequence_number)
    }

    /// Ensure the relay log file is open (no rotation for single file approach)
    async fn ensure_file_open(&self, state: &mut WriterState) -> Result<()> {
        // Close current file if exists and reopen to ensure it's valid
        if state.current_file.is_none() {
            let file_path = RelayLogManager::get_log_file_path(&self.config.log_directory);

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await?;

            let writer = BufWriter::with_capacity(self.config.write_buffer_size, file);
            state.current_file = Some(writer);
            state.current_file_size = 0; // Reset size tracking

            info!("Opened relay log file: {}", file_path.display());
        }
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

/// Reader for relay log files using async I/O with wait capability
pub struct RelayLogReader {
    config: RelayLogConfig,
    reader_state: Arc<RwLock<ReaderState>>,
    log_directory: PathBuf,
    current_file_index: u64,
    start_sequence: u64,
    new_data_notify: Arc<Notify>,
}

impl RelayLogReader {
    /// Create a new relay log reader with enhanced functionality
    async fn new(
        config: RelayLogConfig,
        reader_state: Arc<RwLock<ReaderState>>,
        start_sequence: u64,
        file_index: u64,
        new_data_notify: Arc<Notify>,
    ) -> Result<Self> {
        let reader = Self {
            log_directory: config.log_directory.clone(),
            config,
            reader_state,
            current_file_index: file_index,
            start_sequence,
            new_data_notify,
        };

        // Initialize reader state
        {
            let mut state = reader.reader_state.write().await;
            state.current_file_index = file_index;
            state.current_sequence = start_sequence;
        }

        Ok(reader)
    }

    /// Read the next event from relay logs with sequence filtering
    pub async fn read_event(&self) -> Result<Option<RelayLogEntry>> {
        loop {
            let mut state = self.reader_state.write().await;

            // Open current file if needed
            if state.current_file.is_none() {
                let file_path = RelayLogManager::get_log_file_path(&self.log_directory);
                if file_path.exists() {
                    let file = File::open(&file_path).await?;
                    let reader = BufReader::with_capacity(self.config.read_buffer_size, file);
                    state.current_file = Some(reader);
                } else {
                    // No file exists yet for single file approach
                    return Ok(None);
                }
            }

            // Read from current file
            if let Some(ref mut reader) = state.current_file {
                let mut line = String::new();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF - for single file, just return None and wait for more data
                        return Ok(None);
                    }
                    Ok(_) => {
                        if line.trim().is_empty() {
                            continue; // Continue the loop instead of recursive call
                        }
                        
          
                        // Try to parse as a single JSON object first
                        match serde_json::from_str::<RelayLogEntry>(&line) {
                            Ok(entry) => {
                                // Only return entries with sequence >= our start sequence
                                if entry.sequence_id >= state.current_sequence {
                                    state.current_sequence = entry.sequence_id;
                                    debug!("Read relay log entry: sequence={}", entry.sequence_id);
                                    return Ok(Some(entry));
                                } else {
                                    // todo
                                    // Skip this entry and continue reading
                                    continue;
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse relay log entry: {},{:?}",e,line);
                                continue; // Continue the loop instead of recursive call
                            }
                        }
                    }
                    Err(e) => return Err(CdcError::io(format!("Failed to read relay log: {}", e))),
                }
            } else {
                return Ok(None);
            }
        }
    }

    /// Wait for new events with smart waiting - SQL thread will be awakened by I/O thread
    pub async fn wait_for_events(&self) -> Result<Vec<RelayLogEntry>> {
        let mut events = Vec::new();

        loop {
            match self.read_event().await? {
                Some(event) => {
                    events.push(event);
                    
                    // Try to read more events that might be immediately available
                    while events.len() < 100 { // Reasonable batch size
                        match self.read_event().await? {
                            Some(event) => events.push(event),
                            None => break,
                        }
                    }
                    
                    if !events.is_empty() {
                        break;
                    }
                }
                None => {
                    let _ = self.new_data_notify.notified();
                }
            }
        }

        debug!("Returning batch of {} events", events.len());
        Ok(events)
    }

    /// Get current reading position information for position tracking
    pub async fn get_current_position(&self) -> (u64, u64) {
        let state = self.reader_state.read().await;
        (state.current_file_index, state.current_sequence)
    }

    /// Update the current reading position
    pub async fn update_position(&self, file_index: u64, sequence: u64) {
        let mut state = self.reader_state.write().await;
        if file_index != state.current_file_index {
            // File changed, close current file
            state.current_file = None;
            state.current_file_index = file_index;
        }
        state.current_sequence = sequence;
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
        let reader = manager.create_reader("relay-main.log".to_string(), 0).await.unwrap();

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
    async fn test_relay_log_single_file_approach() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig {
            log_directory: temp_dir.path().to_path_buf(),
            max_file_size: 100 * 1024 * 1024, // Large size since we don't rotate
            max_files: 1, // Single file approach
            write_buffer_size: 1024,
            read_buffer_size: 1024,
        };

        let manager = RelayLogManager::new(config).await.unwrap();
        let writer = manager.create_writer().await.unwrap();

        // Write multiple events to single file
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

        // Check that only one file was created (relay-main.log)
        let mut file_count = 0;
        let mut relay_main_exists = false;
        let mut entries = tokio::fs::read_dir(&temp_dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name == "relay-main.log" {
                    relay_main_exists = true;
                }
                if file_name.starts_with("relay-") && file_name.ends_with(".log") {
                    file_count += 1;
                }
            }
        }

        assert_eq!(file_count, 1, "Expected exactly one relay log file");
        assert!(relay_main_exists, "Expected relay-main.log to exist");
    }

    #[tokio::test]
    async fn test_relay_log_writer_starts_from_sql_thread_position() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig::new(temp_dir.path().to_string_lossy().to_string());

        // Create a sql_thread_position.json file with sequence 100
        let position_file = temp_dir.path().join("sql_thread_position.json");
        let position_data = r#"{
            "file_name": "relay-main.log",
            "sequence_number": 100,
            "source_lsn": "0/12345",
            "last_updated": "2025-08-19T12:00:00Z"
        }"#;
        tokio::fs::write(&position_file, position_data).await.unwrap();

        // Create a relay log manager - it should read the position
        let manager = RelayLogManager::new(config).await.unwrap();
        
        // Verify the manager loaded the correct sequence
        assert_eq!(manager.current_sequence.load(Ordering::Relaxed), 100);

        // Create a writer - it should start from sequence 100
        let writer = manager.create_writer().await.unwrap();
        
        // Verify the writer starts from the correct sequence
        assert_eq!(writer.sequence_number.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn test_relay_log_writer_uses_max_of_relay_and_sql_position() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig::new(temp_dir.path().to_string_lossy().to_string());

        // Create a relay log file with sequence 150
        let relay_log_file = temp_dir.path().join("relay-main.log");
        let relay_log_data = r#"{"sequence_id":149,"source_lsn":"0/AAA","timestamp":"2025-08-19T12:00:00Z","event":{"event_type":"Test"}}
{"sequence_id":150,"source_lsn":"0/BBB","timestamp":"2025-08-19T12:00:01Z","event":{"event_type":"Test"}}"#;
        tokio::fs::write(&relay_log_file, relay_log_data).await.unwrap();

        // Create a sql_thread_position.json file with sequence 120 (lower than relay log)
        let position_file = temp_dir.path().join("sql_thread_position.json");
        let position_data = r#"{
            "file_name": "relay-main.log",
            "sequence_number": 120,
            "source_lsn": "0/12345",
            "last_updated": "2025-08-19T12:00:00Z"
        }"#;
        tokio::fs::write(&position_file, position_data).await.unwrap();

        // Create a relay log manager - it should use the higher sequence from relay log
        let manager = RelayLogManager::new(config).await.unwrap();
        
        // Verify the manager uses the max sequence (150 from relay log)
        assert_eq!(manager.current_sequence.load(Ordering::Relaxed), 150);
    }

    #[tokio::test]
    async fn test_relay_log_writer_uses_sql_position_when_higher() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig::new(temp_dir.path().to_string_lossy().to_string());

        // Create a relay log file with sequence 120
        let relay_log_file = temp_dir.path().join("relay-main.log");
        let relay_log_data = r#"{"sequence_id":120,"source_lsn":"0/AAA","timestamp":"2025-08-19T12:00:00Z","event":{"event_type":"Test"}}"#;
        tokio::fs::write(&relay_log_file, relay_log_data).await.unwrap();

        // Create a sql_thread_position.json file with sequence 150 (higher than relay log)
        let position_file = temp_dir.path().join("sql_thread_position.json");
        let position_data = r#"{
            "file_name": "relay-main.log",
            "sequence_number": 150,
            "source_lsn": "0/12345",
            "last_updated": "2025-08-19T12:00:00Z"
        }"#;
        tokio::fs::write(&position_file, position_data).await.unwrap();

        // Create a relay log manager - it should use the higher sequence from sql thread position
        let manager = RelayLogManager::new(config).await.unwrap();
        
        // Verify the manager uses the max sequence (150 from sql thread position)
        assert_eq!(manager.current_sequence.load(Ordering::Relaxed), 150);
    }
}
