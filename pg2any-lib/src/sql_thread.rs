//! SQL Thread implementation for PostgreSQL logical replication
//!
//! This module implements the SQL thread pattern similar to MySQL binlog replication.
//! The SQL thread reads events from relay log files using async I/O and applies them
//! to destination databases, providing better performance through decoupled processing.

use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::relay_log::{RelayLogReader, RelayLogConfig, RelayLogManager, RelayLogEntry};
use crate::types::{ChangeEvent, DestinationType};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
        let relay_log_dir = config.relay_log_directory
            .clone()
            .expect("PG2ANY_RELAY_LOG_DIR must be set");
        
        let relay_log_config = RelayLogConfig::new(relay_log_dir);

        Self {
            destination_type: config.destination_type.clone(),
            destination_connection_string: config.destination_connection_string.clone(),
            relay_log_config,
            start_sequence: None,
            auto_create_tables: config.auto_create_tables,
            batch_size: 100, // Reasonable batch size for performance
            batch_timeout: Duration::from_millis(500),
            heartbeat_interval: config.heartbeat_interval,
            retry_config: RetryConfig::default(),
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
    cancellation_token: CancellationToken,
    task_handle: Option<JoinHandle<Result<()>>>,
}

impl SqlThread {
    /// Create a new SQL thread
    pub async fn new(config: SqlThreadConfig) -> Result<Self> {
        info!("Creating SQL thread for destination processing");

        // Create relay log manager
        let relay_log_manager = Arc::new(
            RelayLogManager::new(config.relay_log_config.clone()).await?
        );

        // Create destination handler
        let destination_handler = DestinationFactory::create(config.destination_type.clone())?;

        let stats = Arc::new(tokio::sync::RwLock::new(SqlThreadStats {
            events_processed: 0,
            events_applied: 0,
            events_failed: 0,
            current_sequence: config.start_sequence.unwrap_or(0),
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
            cancellation_token: CancellationToken::new(),
            task_handle: None,
        })
    }

    /// Start the SQL thread
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting SQL thread");

        // Connect to destination database
        if let Some(ref mut handler) = self.destination_handler {
            handler.connect(&self.config.destination_connection_string).await?;
            
            // Update connection status
            {
                let mut stats_guard = self.stats.write().await;
                stats_guard.is_connected = true;
            }
        }

        let config = self.config.clone();
        let relay_log_manager = self.relay_log_manager.clone();
        let destination_handler = self.destination_handler.take()
            .ok_or_else(|| CdcError::generic("Destination handler not available"))?;
        let stats = self.stats.clone();
        let cancellation_token = self.cancellation_token.clone();

        let task_handle = tokio::spawn(async move {
            Self::run_sql_loop(config, relay_log_manager, destination_handler, stats, cancellation_token).await
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
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("SQL thread main loop started");

        // Create relay log reader
        let relay_reader = relay_log_manager.create_reader(config.start_sequence).await?;

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
            let cancellation_token = cancellation_token.clone();
            let batch_size = config.batch_size;
            let batch_timeout = config.batch_timeout;
            let auto_create_tables = config.auto_create_tables;
            let retry_config = config.retry_config;
            let heartbeat_interval = config.heartbeat_interval;

            tokio::spawn(async move {
                Self::run_processor_loop(
                    &mut event_rx,
                    destination_handler,
                    stats,
                    cancellation_token,
                    batch_size,
                    batch_timeout,
                    auto_create_tables,
                    retry_config,
                    heartbeat_interval,
                ).await
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
        cancellation_token: CancellationToken,
        batch_size: usize,
        batch_timeout: Duration,
        auto_create_tables: bool,
        retry_config: RetryConfig,
        heartbeat_interval: Duration,
    ) -> Result<()> {
        info!("Starting event processor loop");

        let mut batch_buffer = Vec::with_capacity(batch_size);
        let mut batch_timer = tokio::time::interval(batch_timeout);
        let mut heartbeat_timer = tokio::time::interval(heartbeat_interval);

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
                            auto_create_tables,
                            &retry_config,
                        ).await {
                            error!("Failed to process final batch: {}", e);
                        }
                    }
                    break;
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
                match Self::process_single_event(&entry.event, destination_handler, auto_create_tables).await {
                    Ok(()) => {
                        // Update success statistics
                        let mut stats_guard = stats.write().await;
                        stats_guard.events_processed += 1;
                        stats_guard.events_applied += 1;
                        stats_guard.current_sequence = entry.sequence_id;
                        break;
                    }
                    Err(e) => {
                        retry_count += 1;
                        
                        if retry_count > retry_config.max_retries {
                            error!("Failed to process event after {} retries: {}", retry_config.max_retries, e);
                            
                            // Update failure statistics
                            let mut stats_guard = stats.write().await;
                            stats_guard.events_processed += 1;
                            stats_guard.events_failed += 1;
                            break;
                        } else {
                            warn!("Event processing failed (attempt {}): {}, retrying in {:?}", 
                                  retry_count, e, delay);
                            
                            tokio::time::sleep(delay).await;
                            
                            // Exponential backoff
                            delay = Duration::from_millis(
                                (delay.as_millis() as f64 * retry_config.backoff_multiplier) as u64
                            ).min(retry_config.max_delay);
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
                stats_guard.average_batch_size = 
                    (stats_guard.average_batch_size * (stats_guard.total_batches - 1) as f64 + batch_size as f64) 
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

        // Update connection status
        {
            let mut stats_guard = self.stats.write().await;
            stats_guard.is_connected = false;
        }

        Ok(())
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
            .source_connection_string("postgresql://test:test@localhost:5432/test?replication=database".to_string())
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        let mut sql_config = SqlThreadConfig::from(&config);
        sql_config.relay_log_config.log_directory = temp_dir.path().to_path_buf();

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
        let config = Config::builder()
            .source_connection_string("postgresql://test:test@localhost:5432/test?replication=database".to_string())
            .destination_type(DestinationType::SqlServer)
            .destination_connection_string("mssql://test:test@localhost:1433/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .auto_create_tables(false)
            .heartbeat_interval(Duration::from_secs(15))
            .build()
            .unwrap();

        let sql_config = SqlThreadConfig::from(&config);

        assert_eq!(sql_config.destination_type, DestinationType::SqlServer);
        assert_eq!(sql_config.destination_connection_string, config.destination_connection_string);
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
}
