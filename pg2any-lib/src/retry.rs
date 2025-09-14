//! Connection retry logic with exponential backoff
//!
//! This module provides retry mechanisms for PostgreSQL replication connections,
//! with configurable backoff strategies and error categorization.

use crate::config::Config;
use crate::error::{CdcError, Result};
use crate::pg_replication::PgReplicationConnection;
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

/// Custom exponential backoff implementation
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    jitter: bool,
    current_delay: Duration,
    attempt: u32,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff instance
    pub fn new(config: &RetryConfig) -> Self {
        Self {
            initial_delay: config.initial_delay,
            max_delay: config.max_delay,
            multiplier: config.multiplier,
            jitter: config.jitter,
            current_delay: config.initial_delay,
            attempt: 0,
        }
    }

    /// Get the next delay duration
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;

        // Calculate next delay with exponential backoff
        let next_delay_ms = (self.current_delay.as_millis() as f64 * self.multiplier) as u64;
        self.current_delay = Duration::from_millis(next_delay_ms).min(self.max_delay);
        self.attempt += 1;

        // Add jitter if enabled
        if self.jitter {
            self.add_jitter(delay)
        } else {
            delay
        }
    }

    /// Add jitter to the delay (±30% randomization)
    fn add_jitter(&self, delay: Duration) -> Duration {
        // Simple jitter implementation without external dependencies
        // Use current time as a simple source of randomness
        let now = Instant::now();
        let nanos = now.elapsed().subsec_nanos();
        let jitter_factor = 0.3;

        // Calculate jitter as ±30% of the delay
        let base_millis = delay.as_millis() as f64;
        let jitter_range = base_millis * jitter_factor;
        let jitter = (nanos % 1000) as f64 / 1000.0; // 0.0 to 1.0
        let jitter_adjustment = (jitter - 0.5) * 2.0 * jitter_range; // -jitter_range to +jitter_range

        let final_millis = (base_millis + jitter_adjustment).max(0.0) as u64;
        Duration::from_millis(final_millis)
    }

    /// Reset the backoff to initial state
    pub fn reset(&mut self) {
        self.current_delay = self.initial_delay;
        self.attempt = 0;
    }

    /// Get current attempt number
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

impl RetryConfig {
    /// Create an exponential backoff policy from retry configuration
    pub fn to_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff::new(self)
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
        let mut backoff = self.config.to_backoff();

        info!("Attempting to connect to PostgreSQL with retry logic");

        for attempt in 1..=self.config.max_attempts {
            debug!(
                "Attempting PostgreSQL connection (attempt {}/{})",
                attempt, self.config.max_attempts
            );

            // Check if we've exceeded the maximum duration
            if start_time.elapsed() >= self.config.max_duration {
                error!(
                    "Connection attempts exceeded maximum duration ({:?})",
                    self.config.max_duration
                );
                return Err(CdcError::transient_connection(format!(
                    "Connection attempts exceeded maximum duration of {:?}",
                    self.config.max_duration
                )));
            }

            match PgReplicationConnection::connect(&self.connection_string) {
                Ok(conn) => {
                    let elapsed = start_time.elapsed();
                    info!(
                        "Successfully connected to PostgreSQL on attempt {} after {:?}",
                        attempt, elapsed
                    );
                    return Ok(conn);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    error!("Connection attempt {} failed: {}", attempt, error_msg);

                    // If this is the last attempt, return the error
                    if attempt >= self.config.max_attempts {
                        let elapsed = start_time.elapsed();
                        error!("All connection attempts failed after {:?}", elapsed);
                        return Err(CdcError::transient_connection(format!(
                            "Failed to connect after {} attempts: {}",
                            self.config.max_attempts, error_msg
                        )));
                    }

                    // Wait before the next attempt
                    let delay = backoff.next_delay();
                    debug!("Waiting {:?} before next attempt", delay);
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // This should never be reached due to the loop logic above
        let elapsed = start_time.elapsed();
        error!("Connection failed after all attempts and {:?}", elapsed);
        Err(CdcError::transient_connection(
            "Connection failed after all retry attempts".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_exponential_backoff_basic() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // First delay should be the initial delay
        let first_delay = backoff.next_delay();
        assert_eq!(first_delay, Duration::from_millis(100));
        assert_eq!(backoff.attempt(), 1);

        // Second delay should be doubled
        let second_delay = backoff.next_delay();
        assert_eq!(second_delay, Duration::from_millis(200));
        assert_eq!(backoff.attempt(), 2);

        // Third delay should be doubled again
        let third_delay = backoff.next_delay();
        assert_eq!(third_delay, Duration::from_millis(400));
        assert_eq!(backoff.attempt(), 3);
    }

    #[test]
    fn test_exponential_backoff_max_delay() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500), // Low max delay for testing
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // Skip to a point where we should hit the max delay
        backoff.next_delay(); // 100ms
        backoff.next_delay(); // 200ms
        backoff.next_delay(); // 400ms
        let delay = backoff.next_delay(); // Should be capped at 500ms, not 800ms

        // The current delay should be capped at max_delay
        assert!(delay <= Duration::from_millis(500));
    }

    #[test]
    fn test_exponential_backoff_reset() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // Make a few attempts
        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 2);

        // Reset should go back to initial state
        backoff.reset();
        assert_eq!(backoff.attempt(), 0);

        let delay = backoff.next_delay();
        assert_eq!(delay, Duration::from_millis(100));
        assert_eq!(backoff.attempt(), 1);
    }

    #[test]
    fn test_exponential_backoff_jitter() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: true,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        let delay = backoff.next_delay();
        // With jitter, the delay should be around 100ms but not exactly 100ms
        // Allow for ±30% jitter range
        assert!(delay >= Duration::from_millis(70));
        assert!(delay <= Duration::from_millis(130));
    }

    #[test]
    fn test_retry_config_from_config() {
        // Create a mock Config struct for testing
        let config = Config {
            max_retry_attempts: 10,
            initial_retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(30),
            retry_multiplier: 1.5,
            max_retry_duration: Duration::from_secs(600),
            retry_jitter: true,
            // Add other required fields as needed
            ..Default::default()
        };

        let retry_config = RetryConfig::from(&config);

        assert_eq!(retry_config.max_attempts, 10);
        assert_eq!(retry_config.initial_delay, Duration::from_millis(500));
        assert_eq!(retry_config.max_delay, Duration::from_secs(30));
        assert_eq!(retry_config.multiplier, 1.5);
        assert_eq!(retry_config.max_duration, Duration::from_secs(600));
        assert_eq!(retry_config.jitter, true);
    }
}
