//! I/O Thread implementation for PostgreSQL logical replication
//!
//! This module implements the I/O thread pattern similar to MySQL binlog replication.
//! The I/O thread connects to PostgreSQL, reads WAL events through logical replication,
//! and writes them to relay log files for later processing by SQL threads.

use crate::config::Config;
use crate::error::Result;
use crate::logical_stream::{LogicalReplicationStream, ReplicationStreamConfig};
use crate::relay_log::{RelayLogConfig, RelayLogManager, RelayLogWriter};
use crate::types::ChangeEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Configuration for the I/O thread
#[derive(Debug, Clone)]
pub struct IoThreadConfig {
    /// PostgreSQL connection string for replication
    pub connection_string: String,
    /// Replication stream configuration
    pub replication_config: ReplicationStreamConfig,
    /// Relay log configuration
    pub relay_log_config: RelayLogConfig,
    /// Starting LSN position (None for latest)
    pub start_lsn: Option<u64>,
    /// Heartbeat interval for monitoring
    pub heartbeat_interval: Duration,
    /// Buffer size for events before writing to relay log
    pub buffer_size: usize,
    /// Flush interval for relay log writes
    pub flush_interval: Duration,
}

impl From<&Config> for IoThreadConfig {
    fn from(config: &Config) -> Self {
        let replication_config = ReplicationStreamConfig::from(config);

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
            connection_string: config.source_connection_string.clone(),
            replication_config,
            relay_log_config,
            start_lsn: None,
            heartbeat_interval: config.heartbeat_interval,
            buffer_size: config.buffer_size.max(1000), // Ensure reasonable buffer size
            flush_interval: Duration::from_millis(500),
        }
    }
}

/// Statistics for the I/O thread
#[derive(Debug, Clone)]
pub struct IoThreadStats {
    pub events_read: u64,
    pub events_written: u64,
    pub bytes_written: u64,
    pub current_lsn: Option<String>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
    pub is_connected: bool,
    pub relay_log_file_index: u64,
}

/// I/O Thread for reading from PostgreSQL and writing to relay logs
pub struct IoThread {
    config: IoThreadConfig,
    relay_log_manager: Arc<RelayLogManager>,
    stats: Arc<tokio::sync::RwLock<IoThreadStats>>,
    cancellation_token: CancellationToken,
    task_handle: Option<JoinHandle<Result<()>>>,
}

impl IoThread {
    /// Create a new I/O thread
    pub async fn new(config: IoThreadConfig) -> Result<Self> {
        info!("Creating I/O thread for PostgreSQL logical replication");

        // Create relay log manager
        let relay_log_manager =
            Arc::new(RelayLogManager::new(config.relay_log_config.clone()).await?);

        let stats = Arc::new(tokio::sync::RwLock::new(IoThreadStats {
            events_read: 0,
            events_written: 0,
            bytes_written: 0,
            current_lsn: None,
            last_heartbeat: chrono::Utc::now(),
            is_connected: false,
            relay_log_file_index: 0,
        }));

        Ok(Self {
            config,
            relay_log_manager,
            stats,
            cancellation_token: CancellationToken::new(),
            task_handle: None,
        })
    }

    /// Start the I/O thread
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting I/O thread");

        let config = self.config.clone();
        let relay_log_manager = self.relay_log_manager.clone();
        let stats = self.stats.clone();
        let cancellation_token = self.cancellation_token.clone();

        let task_handle = tokio::spawn(async move {
            Self::run_io_loop(config, relay_log_manager, stats, cancellation_token).await
        });

        self.task_handle = Some(task_handle);
        info!("I/O thread started successfully");
        Ok(())
    }

    /// Main I/O loop
    async fn run_io_loop(
        config: IoThreadConfig,
        relay_log_manager: Arc<RelayLogManager>,
        stats: Arc<tokio::sync::RwLock<IoThreadStats>>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("I/O thread main loop started");

        // Create relay log writer
        let relay_writer = relay_log_manager.create_writer().await?;

        // Create event buffer for batching writes
        let (buffer_tx, mut buffer_rx) = mpsc::channel::<ChangeEvent>(config.buffer_size);

        // Start the writer task for batched writing to relay logs
        let writer_task = {
            let relay_writer = relay_writer;
            let stats = stats.clone();
            let cancellation_token = cancellation_token.clone();
            let flush_interval = config.flush_interval;

            tokio::spawn(async move {
                Self::run_writer_loop(
                    relay_writer,
                    &mut buffer_rx,
                    stats,
                    cancellation_token,
                    flush_interval,
                )
                .await
            })
        };

        // Connect to PostgreSQL and start reading events
        let reader_task = {
            let connection_string = config.connection_string.clone();
            let replication_config = config.replication_config.clone();
            let start_lsn = config.start_lsn;
            let heartbeat_interval = config.heartbeat_interval;
            let stats = stats.clone();
            let cancellation_token = cancellation_token.clone();

            tokio::spawn(async move {
                Self::run_reader_loop(
                    connection_string,
                    replication_config,
                    start_lsn,
                    buffer_tx,
                    stats,
                    cancellation_token,
                    heartbeat_interval,
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
            result = writer_task => {
                match result {
                    Ok(Ok(())) => info!("Writer task completed successfully"),
                    Ok(Err(e)) => error!("Writer task failed: {}", e),
                    Err(e) => error!("Writer task panicked: {}", e),
                }
            }
            _ = cancellation_token.cancelled() => {
                info!("I/O thread received cancellation signal");
            }
        }

        info!("I/O thread main loop finished");
        Ok(())
    }

    /// Reader loop that connects to PostgreSQL and reads events
    async fn run_reader_loop(
        connection_string: String,
        replication_config: ReplicationStreamConfig,
        start_lsn: Option<u64>,
        buffer_tx: mpsc::Sender<ChangeEvent>,
        stats: Arc<tokio::sync::RwLock<IoThreadStats>>,
        cancellation_token: CancellationToken,
        heartbeat_interval: Duration,
    ) -> Result<()> {
        info!("Starting PostgreSQL reader loop");

        // Create logical replication stream
        let mut stream =
            LogicalReplicationStream::new(&connection_string, replication_config.clone()).await?;

        // Initialize the stream
        stream.initialize().await?;

        // Start replication from specified LSN
        stream.start(start_lsn).await?;

        // Update connection status
        {
            let mut stats_guard = stats.write().await;
            stats_guard.is_connected = true;
            stats_guard.last_heartbeat = chrono::Utc::now();
        }

        info!("PostgreSQL logical replication stream connected and started");

        // Set up heartbeat timer
        let mut heartbeat_timer = tokio::time::interval(heartbeat_interval);

        loop {
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    info!("Reader received cancellation signal");
                    break;
                }

                _ = heartbeat_timer.tick() => {
                    // Update heartbeat timestamp
                    let mut stats_guard = stats.write().await;
                    stats_guard.last_heartbeat = chrono::Utc::now();
                    stats_guard.current_lsn = Some(crate::pg_replication::format_lsn(stream.current_lsn()));
                    debug!("I/O thread heartbeat: LSN={:?}", stats_guard.current_lsn);
                }

                result = stream.next_event(&cancellation_token) => {
                    match result {
                        Ok(Some(event)) => {
                            debug!("I/O thread read event: {:?}", event.event_type);

                            // Update statistics
                            {
                                let mut stats_guard = stats.write().await;
                                stats_guard.events_read += 1;
                                if let Some(ref lsn) = event.lsn {
                                    stats_guard.current_lsn = Some(lsn.clone());
                                }
                            }

                            // Send to buffer for writing to relay log
                            if let Err(e) = buffer_tx.send(event).await {
                                error!("Failed to send event to relay log writer: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            // No event available, continue polling
                            debug!("No event available from PostgreSQL stream");
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            error!("Error reading from PostgreSQL stream: {}", e);

                            // Update connection status
                            {
                                let mut stats_guard = stats.write().await;
                                stats_guard.is_connected = false;
                            }

                            // Try to reconnect after a delay
                            warn!("Attempting to reconnect in 5 seconds...");
                            tokio::time::sleep(Duration::from_secs(5)).await;

                            // Attempt reconnection
                            match LogicalReplicationStream::new(&connection_string, replication_config.clone()).await {
                                Ok(mut new_stream) => {
                                    if let Err(e) = new_stream.initialize().await {
                                        error!("Failed to reinitialize stream: {}", e);
                                        continue;
                                    }
                                    if let Err(e) = new_stream.start(None).await {
                                        error!("Failed to restart stream: {}", e);
                                        continue;
                                    }
                                    stream = new_stream;

                                    // Update connection status
                                    let mut stats_guard = stats.write().await;
                                    stats_guard.is_connected = true;
                                    info!("Successfully reconnected to PostgreSQL");
                                }
                                Err(e) => {
                                    error!("Failed to reconnect: {}", e);
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Stop the stream gracefully
        if let Err(e) = stream.stop().await {
            warn!("Error stopping PostgreSQL stream: {}", e);
        }

        // Update connection status
        {
            let mut stats_guard = stats.write().await;
            stats_guard.is_connected = false;
        }

        info!("PostgreSQL reader loop finished");
        Ok(())
    }

    /// Writer loop that writes events to relay logs in batches
    async fn run_writer_loop(
        relay_writer: RelayLogWriter,
        buffer_rx: &mut mpsc::Receiver<ChangeEvent>,
        stats: Arc<tokio::sync::RwLock<IoThreadStats>>,
        cancellation_token: CancellationToken,
        flush_interval: Duration,
    ) -> Result<()> {
        info!("Starting relay log writer loop");

        let mut flush_timer = tokio::time::interval(flush_interval);
        let mut event_buffer = Vec::new();

        loop {
            tokio::select! {
                biased;

                _ = cancellation_token.cancelled() => {
                    info!("Writer received cancellation signal");

                    // Flush any remaining events
                    for event in event_buffer.drain(..) {
                        if let Err(e) = relay_writer.write_event(event, None).await {
                            error!("Failed to write final event to relay log: {}", e);
                        }
                    }

                    if let Err(e) = relay_writer.flush().await {
                        error!("Failed to flush relay log on shutdown: {}", e);
                    }
                    break;
                }

                _ = flush_timer.tick() => {
                    // Periodic flush
                    if !event_buffer.is_empty() {
                        debug!("Periodic flush of {} events to relay log", event_buffer.len());

                        for event in event_buffer.drain(..) {
                            match relay_writer.write_event(event.clone(), event.lsn.clone()).await {
                                Ok(_sequence_id) => {
                                    // Update statistics
                                    let mut stats_guard = stats.write().await;
                                    stats_guard.events_written += 1;
                                    // Rough estimate of bytes written
                                    stats_guard.bytes_written += 256; // Average event size estimate
                                }
                                Err(e) => {
                                    error!("Failed to write event to relay log: {}", e);
                                }
                            }
                        }

                        if let Err(e) = relay_writer.flush().await {
                            error!("Failed to flush relay log: {}", e);
                        }
                    }
                }

                event = buffer_rx.recv() => {
                    match event {
                        Some(event) => {
                            debug!("Writer received event for relay log: {:?}", event.event_type);
                            event_buffer.push(event);

                            // If buffer is getting full, write immediately
                            if event_buffer.len() >= 100 {
                                debug!("Event buffer full, writing {} events to relay log", event_buffer.len());

                                for event in event_buffer.drain(..) {
                                    match relay_writer.write_event(event.clone(), event.lsn.clone()).await {
                                        Ok(_sequence_id) => {
                                            // Update statistics
                                            let mut stats_guard = stats.write().await;
                                            stats_guard.events_written += 1;
                                            stats_guard.bytes_written += 256; // Average event size estimate
                                        }
                                        Err(e) => {
                                            error!("Failed to write event to relay log: {}", e);
                                        }
                                    }
                                }

                                if let Err(e) = relay_writer.flush().await {
                                    error!("Failed to flush relay log: {}", e);
                                }
                            }
                        }
                        None => {
                            info!("Event buffer channel closed");
                            break;
                        }
                    }
                }
            }
        }

        info!("Relay log writer loop finished");
        Ok(())
    }

    /// Stop the I/O thread
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping I/O thread");

        self.cancellation_token.cancel();

        if let Some(task_handle) = self.task_handle.take() {
            match task_handle.await {
                Ok(Ok(())) => info!("I/O thread stopped successfully"),
                Ok(Err(e)) => error!("I/O thread stopped with error: {}", e),
                Err(e) => error!("I/O thread task panicked: {}", e),
            }
        }

        Ok(())
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> IoThreadStats {
        self.stats.read().await.clone()
    }

    /// Get cancellation token for external coordination
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Check if the I/O thread is running
    pub fn is_running(&self) -> bool {
        self.task_handle.is_some() && !self.task_handle.as_ref().unwrap().is_finished()
    }
}

impl Drop for IoThread {
    fn drop(&mut self) {
        // Ensure cancellation is signaled
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::types::DestinationType;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_io_thread_creation() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        let mut io_config = IoThreadConfig::from(&config);
        io_config.relay_log_config.log_directory = temp_dir.path().to_path_buf();

        let io_thread = IoThread::new(io_config).await.unwrap();

        // Check initial state
        let stats = io_thread.get_stats().await;
        assert_eq!(stats.events_read, 0);
        assert_eq!(stats.events_written, 0);
        assert!(!stats.is_connected);
    }

    #[tokio::test]
    async fn test_io_thread_config_from_cdc_config() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let relay_log_path = temp_dir.path().to_string_lossy().to_string();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .protocol_version(2)
            .streaming(true)
            .heartbeat_interval(Duration::from_secs(10))
            .buffer_size(5000)
            .relay_log_directory(Some(relay_log_path))
            .build()
            .unwrap();

        let io_config = IoThreadConfig::from(&config);

        assert_eq!(io_config.connection_string, config.source_connection_string);
        assert_eq!(io_config.replication_config.slot_name, "test_slot");
        assert_eq!(io_config.replication_config.publication_name, "test_pub");
        assert_eq!(io_config.replication_config.protocol_version, 2);
        assert!(io_config.replication_config.streaming_enabled);
        assert_eq!(io_config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(io_config.buffer_size, 5000);
    }
}
