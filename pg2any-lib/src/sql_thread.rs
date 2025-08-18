//! SQL Thread implementation for PostgreSQL logical replication
//!
//! This module implements the SQL thread pattern similar to MySQL binlog replication.
//! The SQL thread reads events from relay log files using async I/O and applies them
//! to destination databases, providing better performance through decoupled processing.

use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::relay_log::{RelayLogConfig, RelayLogEntry, RelayLogManager, RelayLogReader};
use crate::types::{ChangeEvent, DestinationType};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Position tracking for relay log resumption
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RelayLogPosition {
    /// Current relay log file name (e.g., "relay-000001.log")
    pub file_name: String,
    /// Position within the file (byte offset)
    /// Latest sequence number processed
    pub sequence_number: u64,
    /// Source LSN from PostgreSQL (if available)
    pub source_lsn: Option<String>,
    /// Timestamp when this position was last updated
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl Default for RelayLogPosition {
    fn default() -> Self {
        Self {
            file_name: "relay-000001.log".to_string(),
            sequence_number: 0,
            source_lsn: None,
            last_updated: chrono::Utc::now(),
        }
    }
}

//todo fix
impl RelayLogPosition {
    /// Create a new position from file index and other parameters
    pub fn new(file_index: u64, sequence_number: u64, source_lsn: Option<String>) -> Self {
        Self {
            file_name: format!("relay-{:06}.log", file_index),
            sequence_number,
            source_lsn,
            last_updated: chrono::Utc::now(),
        }
    }

    /// Extract file index from file name
    pub fn get_file_index(&self) -> Result<u64> {
        if self.file_name.starts_with("relay-") && self.file_name.ends_with(".log") {
            let index_str = &self.file_name[6..self.file_name.len() - 4];
            index_str.parse::<u64>().map_err(|e| {
                CdcError::generic(format!("Invalid relay log file name format: {}", e))
            })
        } else {
            Err(CdcError::generic(format!(
                "Invalid relay log file name: {}",
                self.file_name
            )))
        }
    }
}

/// Manager for persisting and loading relay log positions
pub struct RelayLogPositionManager {
    position_file_path: PathBuf,
}

impl RelayLogPositionManager {
    /// Create a new position manager
    pub fn new(relay_log_dir: &PathBuf) -> Self {
        let position_file_path = relay_log_dir.join("sql_thread_position.json");
        Self { position_file_path }
    }

    /// Save current position to disk
    pub async fn save_position(&self, position: &RelayLogPosition) -> Result<()> {
        info!("Saving relay log position: {:?}", position);

        let json_data = serde_json::to_string_pretty(position)
            .map_err(|e| CdcError::generic(format!("Failed to serialize position: {}", e)))?;

        // Write to a temporary file first, then atomically rename
        let temp_file_path = self.position_file_path.with_extension("tmp");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_file_path)
            .await
            .map_err(|e| CdcError::io(format!("Failed to create position file: {}", e)))?;

        file.write_all(json_data.as_bytes())
            .await
            .map_err(|e| CdcError::io(format!("Failed to write position data: {}", e)))?;

        file.sync_all()
            .await
            .map_err(|e| CdcError::io(format!("Failed to sync position file: {}", e)))?;

        drop(file);

        // todo fix
        tokio::fs::rename(&temp_file_path, &self.position_file_path)
            .await
            .map_err(|e| CdcError::io(format!("Failed to rename position file: {}", e)))?;

        debug!(
            "Successfully saved relay log position to {}",
            self.position_file_path.display()
        );
        Ok(())
    }

    /// Load position from disk
    pub async fn load_position(&self) -> Result<Option<RelayLogPosition>> {
        if !self.position_file_path.exists() {
            info!("Position file does not exist, starting from beginning");
            return Ok(None);
        }

        info!(
            "Loading relay log position from {}",
            self.position_file_path.display()
        );

        let mut file = File::open(&self.position_file_path)
            .await
            .map_err(|e| CdcError::io(format!("Failed to open position file: {}", e)))?;

        let mut content = String::new();
        file.read_to_string(&mut content)
            .await
            .map_err(|e| CdcError::io(format!("Failed to read position file: {}", e)))?;

        match serde_json::from_str::<RelayLogPosition>(&content) {
            Ok(position) => {
                info!(
                    "Loaded relay log position: file={}, seq={}",
                    position.file_name, position.sequence_number
                );
                Ok(Some(position))
            }
            Err(e) => {
                warn!(
                    "Failed to parse position file ({}), starting from beginning: {}",
                    self.position_file_path.display(),
                    e
                );
                Ok(None)
            }
        }
    }

    /// Delete position file (for cleanup/reset)
    pub async fn delete_position(&self) -> Result<()> {
        if self.position_file_path.exists() {
            tokio::fs::remove_file(&self.position_file_path)
                .await
                .map_err(|e| CdcError::io(format!("Failed to delete position file: {}", e)))?;
            info!(
                "Deleted position file: {}",
                self.position_file_path.display()
            );
        }
        Ok(())
    }
}

/// Configuration for the SQL thread
#[derive(Debug, Clone)]
pub struct SqlThreadConfig {
    /// Destination database type
    pub destination_type: DestinationType,
    /// Destination connection string
    pub destination_connection_string: String,
    /// Relay log configuration
    pub relay_log_config: RelayLogConfig,
    /// Starting sequence number (None for latest)
    pub start_sequence: Option<u64>,
    /// Starting relay log position (for resumption)
    pub start_position: Option<RelayLogPosition>,
    /// Whether to auto-create tables
    pub auto_create_tables: bool,
    /// Batch size for processing events
    pub batch_size: usize,
    /// Maximum time to wait for a batch
    pub batch_timeout: Duration,
    /// Heartbeat interval for monitoring
    pub heartbeat_interval: Duration,
    /// Retry configuration
    pub retry_config: RetryConfig,
    /// Position save interval (how often to persist position)
    pub position_save_interval: Duration,
}

/// Retry configuration for error handling
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries for failed operations
    pub max_retries: u32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }
}

impl From<&Config> for SqlThreadConfig {
    fn from(config: &Config) -> Self {
        // Use relay log directory from config, or fall back to environment variable/default
        let relay_log_dir = config
            .relay_log_directory
            .clone()
            .or_else(|| std::env::var("PG2ANY_RELAY_LOG_DIR").ok())
            .unwrap_or_else(|| {
                // If neither config nor env var is set, use a temp directory as fallback
                // This is primarily for tests - in production, this should be explicitly set
                std::env::temp_dir()
                    .join("pg2any_relay_logs")
                    .to_string_lossy()
                    .to_string()
            });

        let relay_log_config = RelayLogConfig::new(relay_log_dir);

        Self {
            destination_type: config.destination_type.clone(),
            destination_connection_string: config.destination_connection_string.clone(),
            relay_log_config,
            start_sequence: None,
            start_position: None,
            auto_create_tables: config.auto_create_tables,
            batch_size: 100, // Reasonable batch size for performance
            batch_timeout: Duration::from_millis(500),
            heartbeat_interval: config.heartbeat_interval,
            retry_config: RetryConfig::default(),
            position_save_interval: Duration::from_secs(5), // Save position every 5 seconds
        }
    }
}

/// Statistics for the SQL thread
#[derive(Debug, Clone)]
pub struct SqlThreadStats {
    pub events_processed: u64,
    pub events_applied: u64,
    pub events_failed: u64,
    pub current_sequence: u64,
    // pub last_heartbeat: chrono::DateTime<chrono::Utc>,
    pub is_connected: bool,
    pub processing_lag_ms: u64,
    pub average_batch_size: f64,
    pub total_batches: u64,
}

/// SQL Thread for reading from relay logs and applying to destinations
pub struct SqlThread {
    config: SqlThreadConfig,
    relay_log_manager: Arc<RelayLogManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    stats: Arc<tokio::sync::RwLock<SqlThreadStats>>,
    position_manager: RelayLogPositionManager,
    current_position: Arc<tokio::sync::RwLock<RelayLogPosition>>,
    cancellation_token: CancellationToken,
    task_handle: Option<JoinHandle<Result<()>>>,
}

impl SqlThread {
    /// Create a new SQL thread
    pub async fn new(mut config: SqlThreadConfig) -> Result<Self> {
        info!("Creating SQL thread for destination processing");

        // Create position manager
        let position_manager = RelayLogPositionManager::new(&config.relay_log_config.log_directory);

        // Try to load previous position
        let loaded_position = position_manager.load_position().await?;
        let current_position = if let Some(position) = loaded_position {
            info!(
                "Resuming from saved position: file={}, seq={}",
                position.file_name, position.sequence_number
            );

            // Update config to use the loaded position
            config.start_position = Some(position.clone());
            config.start_sequence = Some(position.sequence_number);

            position
        } else {
            info!("Starting from beginning - no saved position found");
            RelayLogPosition::default()
        };

        // Create relay log manager
        let relay_log_manager =
            Arc::new(RelayLogManager::new(config.relay_log_config.clone()).await?);

        // Create destination handler
        let destination_handler = DestinationFactory::create(config.destination_type.clone())?;

        let stats = Arc::new(tokio::sync::RwLock::new(SqlThreadStats {
            events_processed: 0,
            events_applied: 0,
            events_failed: 0,
            current_sequence: current_position.sequence_number,
            // last_heartbeat: chrono::Utc::now(),
            is_connected: false,
            processing_lag_ms: 0,
            average_batch_size: 0.0,
            total_batches: 0,
        }));

        Ok(Self {
            config,
            relay_log_manager,
            destination_handler: Some(destination_handler),
            stats,
            position_manager,
            current_position: Arc::new(tokio::sync::RwLock::new(current_position)),
            cancellation_token: CancellationToken::new(),
            task_handle: None,
        })
    }

    /// Start the SQL thread
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting SQL thread");

        // Connect to destination database
        if let Some(ref mut handler) = self.destination_handler {
            handler
                .connect(&self.config.destination_connection_string)
                .await?;

            // Update connection status
            {
                let mut stats_guard = self.stats.write().await;
                stats_guard.is_connected = true;
            }
        }

        let config = self.config.clone();
        let relay_log_manager = self.relay_log_manager.clone();
        let destination_handler = self
            .destination_handler
            .take()
            .ok_or_else(|| CdcError::generic("Destination handler not available"))?;
        let stats = self.stats.clone();
        let current_position = self.current_position.clone();
        let cancellation_token = self.cancellation_token.clone();

        let task_handle = tokio::spawn(async move {
            Self::run_sql_loop(
                config,
                relay_log_manager,
                destination_handler,
                stats,
                current_position,
                cancellation_token,
            )
            .await
        });

        self.task_handle = Some(task_handle);
        info!("SQL thread started successfully");
        Ok(())
    }

    /// Main SQL processing loop
    async fn run_sql_loop(
        config: SqlThreadConfig,
        relay_log_manager: Arc<RelayLogManager>,
        destination_handler: Box<dyn DestinationHandler>,
        stats: Arc<tokio::sync::RwLock<SqlThreadStats>>,
        current_position: Arc<tokio::sync::RwLock<RelayLogPosition>>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("SQL thread main loop started");

        // Determine starting sequence from position or config
        // let start_sequence = if let Some(ref start_pos) = config.start_position {
        //     Some(start_pos.sequence_number)
        // } else {
        //     config.start_sequence
        // };
        // Create relay log reader
        let relay_reader = {
            let current_position = current_position.read().await;
            relay_log_manager
                .create_reader(
                    current_position.file_name.clone(),
                    current_position.sequence_number,
                )
                .await?
        };

        // Create event processing pipeline
        let (event_tx, mut event_rx) = mpsc::channel::<RelayLogEntry>(config.batch_size * 2);

        // Start the relay log reader task
        let reader_task = {
            let relay_reader = relay_reader;
            let stats = stats.clone();
            let cancellation_token = cancellation_token.clone();

            tokio::spawn(async move {
                Self::run_reader_loop(relay_reader, event_tx, stats, cancellation_token).await
            })
        };

        // Start the event processor task
        let processor_task = {
            let stats = stats.clone();
            let current_position = current_position.clone();
            let position_manager =
                RelayLogPositionManager::new(&config.relay_log_config.log_directory);
            let cancellation_token = cancellation_token.clone();
            let batch_size = config.batch_size;
            let batch_timeout = config.batch_timeout;
            let auto_create_tables = config.auto_create_tables;
            let retry_config = config.retry_config;
            let heartbeat_interval = config.heartbeat_interval;
            let position_save_interval = config.position_save_interval;

            tokio::spawn(async move {
                Self::run_processor_loop(
                    &mut event_rx,
                    destination_handler,
                    stats,
                    current_position,
                    position_manager,
                    cancellation_token,
                    batch_size,
                    batch_timeout,
                    auto_create_tables,
                    retry_config,
                    heartbeat_interval,
                    position_save_interval,
                )
                .await
            })
        };

        // Wait for either task to complete or cancellation
        tokio::select! {
            result = reader_task => {
                match result {
                    Ok(Ok(())) => info!("Reader task completed successfully"),
                    Ok(Err(e)) => error!("Reader task failed: {}", e),
                    Err(e) => error!("Reader task panicked: {}", e),
                }
            }
            result = processor_task => {
                match result {
                    Ok(Ok(())) => info!("Processor task completed successfully"),
                    Ok(Err(e)) => error!("Processor task failed: {}", e),
                    Err(e) => error!("Processor task panicked: {}", e),
                }
            }
            _ = cancellation_token.cancelled() => {
                info!("SQL thread received cancellation signal");
            }
        }

        info!("SQL thread main loop finished");
        Ok(())
    }

    /// Reader loop that reads events from relay logs using async I/O
    async fn run_reader_loop(
        relay_reader: RelayLogReader,
        event_tx: mpsc::Sender<RelayLogEntry>,
        stats: Arc<tokio::sync::RwLock<SqlThreadStats>>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("Starting relay log reader loop");

        loop {
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    info!("Relay reader received cancellation signal");
                    break;
                }

                result = relay_reader.wait_for_events() => {
                    match result {
                        Ok(events) => {
                            debug!("Read {} events from relay log", events.len());

                            for event in events {
                                // Update current sequence
                                {
                                    let mut stats_guard = stats.write().await;
                                    stats_guard.current_sequence = event.sequence_id;
                                }

                                // Send to processor
                                if let Err(e) = event_tx.send(event).await {
                                    error!("Failed to send relay log entry to processor: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error reading from relay log: {}", e);

                            // Wait before retrying
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        info!("Relay log reader loop finished");
        Ok(())
    }

    /// Processor loop that applies events to destination in batches
    async fn run_processor_loop(
        event_rx: &mut mpsc::Receiver<RelayLogEntry>,
        mut destination_handler: Box<dyn DestinationHandler>,
        stats: Arc<tokio::sync::RwLock<SqlThreadStats>>,
        current_position: Arc<tokio::sync::RwLock<RelayLogPosition>>,
        position_manager: RelayLogPositionManager,
        cancellation_token: CancellationToken,
        batch_size: usize,
        batch_timeout: Duration,
        auto_create_tables: bool,
        retry_config: RetryConfig,
        heartbeat_interval: Duration,
        position_save_interval: Duration,
    ) -> Result<()> {
        info!("Starting event processor loop");

        let mut batch_buffer = Vec::with_capacity(batch_size);
        let mut batch_timer = tokio::time::interval(batch_timeout);
        let mut heartbeat_timer = tokio::time::interval(heartbeat_interval);
        let mut position_save_timer = tokio::time::interval(position_save_interval);

        loop {
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    info!("Processor received cancellation signal");

                    // Process any remaining events in the batch
                    if !batch_buffer.is_empty() {
                        info!("Processing final batch of {} events", batch_buffer.len());
                        if let Err(e) = Self::process_event_batch(
                            &mut batch_buffer,
                            &mut destination_handler,
                            &stats,
                            &current_position,
                            auto_create_tables,
                            &retry_config,
                        ).await {
                            error!("Failed to process final batch: {}", e);
                        }
                    }

                    // Save final position
                    let final_position = current_position.read().await.clone();
                    if let Err(e) = position_manager.save_position(&final_position).await {
                        error!("Failed to save final position: {}", e);
                    } else {
                        info!("Saved final position before shutdown");
                    }

                    break;
                }

                _ = position_save_timer.tick() => {
                    // Periodically save position to disk
                    let position = current_position.read().await.clone();
                    if let Err(e) = position_manager.save_position(&position).await {
                        warn!("Failed to save position: {}", e);
                    } else {
                        debug!("Saved position: file={}, seq={}", position.file_name, position.sequence_number);
                    }
                }

                _ = heartbeat_timer.tick() => {
                    // Update heartbeat timestamp
                    let mut stats_guard = stats.write().await;
                    // stats_guard.last_heartbeat = chrono::Utc::now();

                    // Calculate processing lag (simplified)
                    stats_guard.processing_lag_ms = 0; // Would calculate based on relay log timestamps

                    debug!("SQL thread heartbeat: sequence={}, processed={}",
                           stats_guard.current_sequence, stats_guard.events_processed);
                }

                _ = batch_timer.tick() => {
                    // Process batch on timeout
                    if !batch_buffer.is_empty() {
                        debug!("Processing batch of {} events due to timeout", batch_buffer.len());
                        if let Err(e) = Self::process_event_batch(
                            &mut batch_buffer,
                            &mut destination_handler,
                            &stats,
                            &current_position,
                            auto_create_tables,
                            &retry_config,
                        ).await {
                            error!("Failed to process batch: {}", e);
                        }
                    }
                }

                event = event_rx.recv() => {
                    match event {
                        Some(relay_entry) => {
                            debug!("Processor received event: sequence={}", relay_entry.sequence_id);
                            batch_buffer.push(relay_entry);

                            // Process batch when it's full
                            if batch_buffer.len() >= batch_size {
                                debug!("Processing full batch of {} events", batch_buffer.len());
                                if let Err(e) = Self::process_event_batch(
                                    &mut batch_buffer,
                                    &mut destination_handler,
                                    &stats,
                                    &current_position,
                                    auto_create_tables,
                                    &retry_config,
                                ).await {
                                    error!("Failed to process batch: {}", e);
                                }
                            }
                        }
                        None => {
                            info!("Event channel closed");
                            break;
                        }
                    }
                }
            }
        }

        info!("Event processor loop finished");
        Ok(())
    }

    /// Process a batch of events
    async fn process_event_batch(
        batch: &mut Vec<RelayLogEntry>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        stats: &Arc<tokio::sync::RwLock<SqlThreadStats>>,
        current_position: &Arc<tokio::sync::RwLock<RelayLogPosition>>,
        auto_create_tables: bool,
        retry_config: &RetryConfig,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        let start_time = std::time::Instant::now();

        debug!("Processing batch of {} events", batch_size);

        for entry in batch.drain(..) {
            let mut retry_count = 0;
            let mut delay = retry_config.initial_delay;

            loop {
                match Self::process_single_event(
                    &entry.event,
                    destination_handler,
                    auto_create_tables,
                )
                .await
                {
                    Ok(()) => {
                        // Update success statistics
                        {
                            let mut stats_guard = stats.write().await;
                            stats_guard.events_processed += 1;
                            stats_guard.events_applied += 1;
                            stats_guard.current_sequence = entry.sequence_id;
                        }

                        // Update current position
                        {
                            let mut position_guard = current_position.write().await;
                            position_guard.sequence_number = entry.sequence_id;
                            position_guard.source_lsn = entry.source_lsn.clone();
                            position_guard.last_updated = chrono::Utc::now();

                            // Note: file_name and file_position would need to be calculated from the relay log entry
                            // For now, we'll just update sequence_number and source_lsn
                            // In a future improvement, the RelayLogEntry could include file position info
                        }

                        break;
                    }
                    Err(e) => {
                        retry_count += 1;

                        if retry_count > retry_config.max_retries {
                            error!(
                                "Failed to process event after {} retries: {}",
                                retry_config.max_retries, e
                            );

                            // Update failure statistics
                            let mut stats_guard = stats.write().await;
                            stats_guard.events_processed += 1;
                            stats_guard.events_failed += 1;
                            break;
                        } else {
                            warn!(
                                "Event processing failed (attempt {}): {}, retrying in {:?}",
                                retry_count, e, delay
                            );

                            tokio::time::sleep(delay).await;

                            // Exponential backoff
                            delay = Duration::from_millis(
                                (delay.as_millis() as f64 * retry_config.backoff_multiplier) as u64,
                            )
                            .min(retry_config.max_delay);
                        }
                    }
                }
            }
        }

        // Update batch statistics
        {
            let mut stats_guard = stats.write().await;
            stats_guard.total_batches += 1;

            // Update average batch size (running average)
            if stats_guard.total_batches == 1 {
                stats_guard.average_batch_size = batch_size as f64;
            } else {
                stats_guard.average_batch_size = (stats_guard.average_batch_size
                    * (stats_guard.total_batches - 1) as f64
                    + batch_size as f64)
                    / stats_guard.total_batches as f64;
            }
        }

        let elapsed = start_time.elapsed();
        debug!("Processed batch of {} events in {:?}", batch_size, elapsed);

        Ok(())
    }

    /// Process a single event
    async fn process_single_event(
        event: &ChangeEvent,
        destination_handler: &mut Box<dyn DestinationHandler>,
        auto_create_tables: bool,
    ) -> Result<()> {
        debug!("Processing event: {:?}", event.event_type);

        // Auto-create table if needed and enabled
        if auto_create_tables && crate::destinations::destination_factory::is_dml_event(event) {
            if let Err(e) = destination_handler.create_table_if_not_exists(event).await {
                error!("Failed to auto-create table for event: {}", e);
                // Continue processing even if table creation fails
            }
        }

        // Process the event
        destination_handler.process_event(event).await?;

        debug!("Successfully processed event: {:?}", event.event_type);
        Ok(())
    }

    /// Stop the SQL thread
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping SQL thread");

        self.cancellation_token.cancel();

        if let Some(task_handle) = self.task_handle.take() {
            match task_handle.await {
                Ok(Ok(())) => info!("SQL thread stopped successfully"),
                Ok(Err(e)) => error!("SQL thread stopped with error: {}", e),
                Err(e) => error!("SQL thread task panicked: {}", e),
            }
        }

        // Save final position
        let final_position = self.current_position.read().await.clone();
        if let Err(e) = self.position_manager.save_position(&final_position).await {
            error!("Failed to save final position during stop: {}", e);
        } else {
            info!(
                "Saved final position during graceful shutdown: seq={}",
                final_position.sequence_number
            );
        }

        // Update connection status
        {
            let mut stats_guard = self.stats.write().await;
            stats_guard.is_connected = false;
        }

        Ok(())
    }

    /// Get current position
    pub async fn get_current_position(&self) -> RelayLogPosition {
        self.current_position.read().await.clone()
    }

    /// Delete saved position file (for cleanup)
    pub async fn delete_saved_position(&self) -> Result<()> {
        self.position_manager.delete_position().await
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> SqlThreadStats {
        self.stats.read().await.clone()
    }

    /// Get cancellation token for external coordination
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Check if the SQL thread is running
    pub fn is_running(&self) -> bool {
        self.task_handle.is_some() && !self.task_handle.as_ref().unwrap().is_finished()
    }
}

impl Drop for SqlThread {
    fn drop(&mut self) {
        // Ensure cancellation is signaled
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sql_thread_creation() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .relay_log_directory(Some(temp_dir.path().to_string_lossy().to_string()))
            .build()
            .unwrap();

        let sql_config = SqlThreadConfig::from(&config);
        let sql_thread = SqlThread::new(sql_config).await.unwrap();

        // Check initial state
        let stats = sql_thread.get_stats().await;
        assert_eq!(stats.events_processed, 0);
        assert_eq!(stats.events_applied, 0);
        assert_eq!(stats.events_failed, 0);
        assert!(!stats.is_connected);
    }

    #[tokio::test]
    async fn test_sql_thread_config_from_cdc_config() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::SqlServer)
            .destination_connection_string("mssql://test:test@localhost:1433/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .auto_create_tables(false)
            .heartbeat_interval(Duration::from_secs(15))
            .relay_log_directory(Some(temp_dir.path().to_string_lossy().to_string()))
            .build()
            .unwrap();

        let sql_config = SqlThreadConfig::from(&config);

        assert_eq!(sql_config.destination_type, DestinationType::SqlServer);
        assert_eq!(
            sql_config.destination_connection_string,
            config.destination_connection_string
        );
        assert!(!sql_config.auto_create_tables);
        assert_eq!(sql_config.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(sql_config.batch_size, 100);
    }

    #[test]
    fn test_retry_config_default() {
        let retry_config = RetryConfig::default();

        assert_eq!(retry_config.max_retries, 5);
        assert_eq!(retry_config.initial_delay, Duration::from_millis(100));
        assert_eq!(retry_config.max_delay, Duration::from_secs(30));
        assert_eq!(retry_config.backoff_multiplier, 2.0);
    }

    #[tokio::test]
    async fn test_relay_log_position_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let position_manager = RelayLogPositionManager::new(&temp_dir.path().to_path_buf());

        // Test saving and loading position
        let original_position = RelayLogPosition::new(5, 12345, Some("0/ABCDEF01".to_string()));

        // Save position
        position_manager
            .save_position(&original_position)
            .await
            .unwrap();

        // Load position
        let loaded_position = position_manager.load_position().await.unwrap().unwrap();

        assert_eq!(loaded_position.file_name, "relay-000005.log");
        assert_eq!(loaded_position.sequence_number, 12345);
        assert_eq!(loaded_position.source_lsn, Some("0/ABCDEF01".to_string()));
    }

    #[tokio::test]
    async fn test_relay_log_position_file_index() {
        let position = RelayLogPosition::new(42, 0, None);
        assert_eq!(position.get_file_index().unwrap(), 42);
        assert_eq!(position.file_name, "relay-000042.log");
    }

    #[tokio::test]
    async fn test_sql_thread_position_resumption() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .relay_log_directory(Some(temp_dir.path().to_string_lossy().to_string()))
            .build()
            .unwrap();

        let sql_config = SqlThreadConfig::from(&config);

        // Create and save a position first
        let position_manager =
            RelayLogPositionManager::new(&sql_config.relay_log_config.log_directory);
        let saved_position = RelayLogPosition::new(3, 9999, Some("0/1234ABCD".to_string()));
        position_manager
            .save_position(&saved_position)
            .await
            .unwrap();

        // Create SQL thread - it should load the saved position
        let sql_thread = SqlThread::new(sql_config).await.unwrap();

        let loaded_position = sql_thread.get_current_position().await;
        assert_eq!(loaded_position.sequence_number, 9999);
        assert_eq!(loaded_position.file_name, "relay-000003.log");
        assert_eq!(loaded_position.source_lsn, Some("0/1234ABCD".to_string()));
    }
}
