//! Connection retry logic with exponential backoff
//!
//! This module provides retry mechanisms for PostgreSQL replication connections,
//! with configurable backoff strategies and error categorization.

use crate::config::Config;
use crate::error::{CdcError, Result};
use crate::pg_replication::PgReplicationConnection;
use backoff::backoff::Backoff;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

/// Configuration for retry logic
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
    pub max_duration: Duration,
    pub jitter: bool,
}

impl From<&Config> for RetryConfig {
    fn from(config: &Config) -> Self {
        Self {
            max_attempts: config.max_retry_attempts,
            initial_delay: config.initial_retry_delay,
            max_delay: config.max_retry_delay,
            multiplier: config.retry_multiplier,
            max_duration: config.max_retry_duration,
            jitter: config.retry_jitter,
        }
    }
}

impl RetryConfig {
    /// Create an exponential backoff policy from retry configuration
    pub fn to_backoff(&self) -> ExponentialBackoff {
        let mut backoff = ExponentialBackoff {
            current_interval: self.initial_delay,
            initial_interval: self.initial_delay,
            randomization_factor: if self.jitter { 0.3 } else { 0.0 },
            multiplier: self.multiplier,
            max_interval: self.max_delay,
            max_elapsed_time: Some(self.max_duration),
            ..Default::default()
        };

        backoff.reset();
        backoff
    }
}

/// Retry wrapper for PostgreSQL replication connection operations
pub struct ReplicationConnectionRetry {
    config: RetryConfig,
    connection_string: String,
}

impl ReplicationConnectionRetry {
    /// Create a new retry wrapper
    pub fn new(config: RetryConfig, connection_string: String) -> Self {
        Self {
            config,
            connection_string,
        }
    }

    /// Retry connection establishment with exponential backoff
    pub async fn connect_with_retry(&self) -> Result<PgReplicationConnection> {
        let start_time = Instant::now();
        info!("Attempting to connect to PostgreSQL with retry logic");

        let operation = || async {
            debug!("Attempting PostgreSQL connection");

            match PgReplicationConnection::connect(&self.connection_string) {
                Ok(conn) => {
                    info!("Successfully connected to PostgreSQL");
                    Ok(conn)
                }
                Err(e) => {
                    let error_msg = e.to_string();

                    error!("Transient connection error: {}", error_msg);
                    Err(BackoffError::Transient {
                        err: CdcError::transient_connection(error_msg),
                        retry_after: None,
                    })
                }
            }
        };

        let backoff = self.config.to_backoff();
        let result = retry(backoff, operation).await;

        let elapsed = start_time.elapsed();
        match &result {
            Ok(_) => info!("Connection established successfully after {:?}", elapsed),
            Err(e) => error!("Connection failed after {:?} with error: {}", elapsed, e),
        }

        result
    }
}
