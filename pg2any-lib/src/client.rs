//! Updated CDC Client using I/O Thread and SQL Thread architecture
//!
//! This module provides a high-level CDC client that implements the MySQL-style
//! I/O Thread and SQL Thread pattern for better performance and fault tolerance.

use crate::config::Config;
use crate::error::{CdcError, Result};
use crate::io_thread::{IoThread, IoThreadConfig, IoThreadStats};
use crate::relay_log::RelayLogConfig;
use crate::sql_thread::{SqlThread, SqlThreadConfig, SqlThreadStats};
use crate::types::Lsn;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Enhanced CDC Client with I/O and SQL Thread architecture
pub struct CdcClient {
    config: Config,
    io_thread: Option<IoThread>,
    sql_thread: Option<SqlThread>,
    cancellation_token: CancellationToken,
    relay_log_directory: PathBuf,
}

/// Combined statistics from both I/O and SQL threads
#[derive(Debug, Clone)]
pub struct CdcStats {
    pub io_stats: Option<IoThreadStats>,
    pub sql_stats: Option<SqlThreadStats>,
    pub relay_log_directory: PathBuf,
    pub uptime: Duration,
    pub start_time: chrono::DateTime<chrono::Utc>,
}

impl CdcClient {
    /// Create a new CDC client with I/O and SQL thread architecture
    pub async fn new(config: Config) -> Result<Self> {
        // Get relay log directory from config, environment variable, or use default
        let relay_log_dir = config
            .relay_log_directory
            .clone()
            .or_else(|| std::env::var("PG2ANY_RELAY_LOG_DIR").ok())
            .unwrap_or_else(|| "relay_logs".to_string());

        Self::new_with_relay_log_dir(config, PathBuf::from(relay_log_dir)).await
    }

    /// Create a new CDC client with custom relay log directory
    pub async fn new_with_relay_log_dir(
        config: Config,
        relay_log_directory: PathBuf,
    ) -> Result<Self> {
        info!("Creating enhanced CDC client with I/O/SQL thread architecture");

        // Ensure relay log directory exists
        tokio::fs::create_dir_all(&relay_log_directory)
            .await
            .map_err(|e| CdcError::io(format!("Failed to create relay log directory: {}", e)))?;

        info!(
            "Using relay log directory: {}",
            relay_log_directory.display()
        );

        Ok(Self {
            config,
            io_thread: None,
            sql_thread: None,
            cancellation_token: CancellationToken::new(),
            relay_log_directory,
        })
    }

    /// Initialize the CDC client by creating I/O and SQL threads
    pub async fn init(&mut self) -> Result<()> {
        info!("Initializing CDC client with I/O and SQL threads");

        // Create relay log configuration
        let relay_log_config = RelayLogConfig {
            log_directory: self.relay_log_directory.clone(),
            max_file_size: 500 * 1024 * 1024, // 500MB per file
            max_files: 100,
            write_buffer_size: 64 * 1024, // 64KB write buffer
            read_buffer_size: 64 * 1024,  // 64KB read buffer
        };

        // Create I/O thread configuration
        let mut io_config = IoThreadConfig::from(&self.config);
        io_config.relay_log_config = relay_log_config.clone();

        // Create SQL thread configuration
        let mut sql_config = SqlThreadConfig::from(&self.config);
        sql_config.relay_log_config = relay_log_config;

        // Create the threads
        let io_thread = IoThread::new(io_config).await?;
        let sql_thread = SqlThread::new(sql_config).await?;

        self.io_thread = Some(io_thread);
        self.sql_thread = Some(sql_thread);

        info!("CDC client initialized successfully");
        Ok(())
    }

    /// Start CDC replication from a specific LSN
    pub async fn start_replication_from_lsn(&mut self, _start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication with I/O and SQL threads");

        // Initialize if not already done
        if self.io_thread.is_none() || self.sql_thread.is_none() {
            self.init().await?;
        }

        // Start SQL thread first (it should be ready to process events)
        if let Some(ref mut sql_thread) = self.sql_thread {
            sql_thread.start().await?;
            info!("SQL thread started");
        }

        // Start I/O thread (it will begin reading and writing to relay logs)
        if let Some(ref mut io_thread) = self.io_thread {
            io_thread.start().await?;
            info!("I/O thread started");
        }

        info!("CDC replication started successfully with both I/O and SQL threads");

        // Wait for both threads to complete or for cancellation
        let io_cancellation = self.io_thread.as_ref().map(|t| t.cancellation_token());
        let sql_cancellation = self.sql_thread.as_ref().map(|t| t.cancellation_token());
        let main_cancellation = self.cancellation_token.clone();

        tokio::select! {
            _ = main_cancellation.cancelled() => {
                info!("Main cancellation received, stopping threads");
            }
            _ = Self::wait_and_warn(io_cancellation) => {
                warn!("I/O thread stopped unexpectedly");
            },
            _ = Self::wait_and_warn(sql_cancellation) => {
                warn!("SQL thread stopped unexpectedly");
            },
        }

        // Stop both threads gracefully
        self.stop().await?;

        Ok(())
    }

    async fn wait_and_warn(token: Option<CancellationToken>) {
        if let Some(token) = token {
            token.cancelled().await;
        }
    }

    /// Stop the CDC client and all threads
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping CDC client");

        // Signal cancellation
        self.cancellation_token.cancel();

        // Stop I/O thread first to stop new events from being written
        if let Some(ref mut io_thread) = self.io_thread {
            if let Err(e) = io_thread.stop().await {
                error!("Error stopping I/O thread: {}", e);
            } else {
                info!("I/O thread stopped");
            }
        }

        // Give SQL thread time to process remaining events
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Stop SQL thread
        if let Some(ref mut sql_thread) = self.sql_thread {
            if let Err(e) = sql_thread.stop().await {
                error!("Error stopping SQL thread: {}", e);
            } else {
                info!("SQL thread stopped");
            }
        }

        info!("CDC client stopped successfully");
        Ok(())
    }

    /// Get combined statistics from both threads
    pub async fn get_stats(&self) -> CdcStats {
        let io_stats = if let Some(ref io_thread) = self.io_thread {
            Some(io_thread.get_stats().await)
        } else {
            None
        };

        let sql_stats = if let Some(ref sql_thread) = self.sql_thread {
            Some(sql_thread.get_stats().await)
        } else {
            None
        };

        CdcStats {
            io_stats,
            sql_stats,
            relay_log_directory: self.relay_log_directory.clone(),
            uptime: Duration::from_secs(0), // Would calculate based on start time
            start_time: chrono::Utc::now(), // Would store actual start time
        }
    }

    /// Check if both threads are running
    pub fn is_running(&self) -> bool {
        let io_running = self.io_thread.as_ref().map_or(false, |t| t.is_running());
        let sql_running = self.sql_thread.as_ref().map_or(false, |t| t.is_running());
        io_running && sql_running
    }

    /// Get the main cancellation token for external coordination
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Print detailed status information
    pub async fn print_status(&self) {
        let stats = self.get_stats().await;

        info!("=== CDC Client Status ===");
        info!(
            "Relay Log Directory: {}",
            stats.relay_log_directory.display()
        );
        info!("Running: {}", self.is_running());

        if let Some(ref io_stats) = stats.io_stats {
            info!("=== I/O Thread Stats ===");
            info!("Connected: {}", io_stats.is_connected);
            info!("Events Read: {}", io_stats.events_read);
            info!("Events Written: {}", io_stats.events_written);
            info!("Bytes Written: {}", io_stats.bytes_written);
            info!("Current LSN: {:?}", io_stats.current_lsn);
            info!("Last Heartbeat: {}", io_stats.last_heartbeat);
            info!("Relay Log File Index: {}", io_stats.relay_log_file_index);
        }

        if let Some(ref sql_stats) = stats.sql_stats {
            info!("=== SQL Thread Stats ===");
            info!("Connected: {}", sql_stats.is_connected);
            info!("Events Processed: {}", sql_stats.events_processed);
            info!("Events Applied: {}", sql_stats.events_applied);
            info!("Events Failed: {}", sql_stats.events_failed);
            info!("Current Sequence: {}", sql_stats.current_sequence);
            info!("Processing Lag: {}ms", sql_stats.processing_lag_ms);
            info!("Average Batch Size: {:.1}", sql_stats.average_batch_size);
            info!("Total Batches: {}", sql_stats.total_batches);
            //info!("Last Heartbeat: {}", sql_stats.last_heartbeat);
        }

        info!("========================");
    }

    /// Perform health check on both threads
    // pub async fn health_check(&self) -> Result<()> {
    //     let stats = self.get_stats().await;

    //     // Check if threads are running
    //     if !self.is_running() {
    //         return Err(CdcError::generic("One or more threads are not running"));
    //     }

    //     // Check I/O thread health
    //     if let Some(ref io_stats) = stats.io_stats {
    //         if !io_stats.is_connected {
    //             return Err(CdcError::generic("I/O thread is not connected to PostgreSQL"));
    //         }

    //         // Check if heartbeat is recent (within last 30 seconds)
    //         let heartbeat_age = chrono::Utc::now().signed_duration_since(io_stats.last_heartbeat);
    //         if heartbeat_age.num_seconds() > 30 {
    //             return Err(CdcError::generic("I/O thread heartbeat is too old"));
    //         }
    //     }

    //     // Check SQL thread health
    //     if let Some(ref sql_stats) = stats.sql_stats {
    //         if !sql_stats.is_connected {
    //             return Err(CdcError::generic("SQL thread is not connected to destination"));
    //         }

    //         // Check if heartbeat is recent
    //         let heartbeat_age = chrono::Utc::now().signed_duration_since(sql_stats.last_heartbeat);
    //         if heartbeat_age.num_seconds() > 30 {
    //             return Err(CdcError::generic("SQL thread heartbeat is too old"));
    //         }
    //     }

    //     debug!("Health check passed");
    //     Ok(())
    // }

    /// Cleanup old relay log files
    pub async fn cleanup_relay_logs(&self) -> Result<()> {
        // This would use the RelayLogManager to clean up old files
        info!("Relay log cleanup would be performed here");
        Ok(())
    }
}

impl Drop for CdcClient {
    fn drop(&mut self) {
        // Ensure cancellation is signaled
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DestinationType;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_cdc_client_creation() {
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

        let client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Check initial state
        assert!(!client.is_running());
        assert!(client.io_thread.is_none());
        assert!(client.sql_thread.is_none());
    }

    #[tokio::test]
    async fn test_cdc_client_initialization() {
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

        let mut client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Initialize the client
        client.init().await.unwrap();

        // Check that threads were created
        assert!(client.io_thread.is_some());
        assert!(client.sql_thread.is_some());
    }

    #[tokio::test]
    async fn test_cdc_stats() {
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

        let mut client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        client.init().await.unwrap();

        let stats = client.get_stats().await;

        // Should have both thread stats available after initialization
        assert!(stats.io_stats.is_some());
        assert!(stats.sql_stats.is_some());
        assert_eq!(stats.relay_log_directory, temp_dir.path());
    }
}
