//! High-level PostgreSQL replication wrappers
//!
//! This module provides application-specific wrappers around the low-level
//! pg_walstream connection layer, integrating with pg2any's error
//! handling, configuration, and CDC event types.

use crate::config::Config;
use crate::error::Result;
use crate::types::{ChangeEvent, Lsn};
use pg_walstream::{LogicalReplicationStream, ReplicationStreamConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

// Re-export connection types from pg_walstream
pub use pg_walstream::{
    format_lsn, format_postgres_timestamp, parse_lsn, postgres_timestamp_to_chrono,
    system_time_to_postgres_timestamp, Oid, PgReplicationConnection, PgResult,
    ReplicationConnectionRetry, RetryConfig, TimestampTz, XLogRecPtr, Xid, INVALID_XLOG_REC_PTR,
};

pub struct ReplicationStream {
    logical_stream: LogicalReplicationStream,
}

impl ReplicationStream {
    pub async fn new(config: &Config) -> Result<Self> {
        let stream_config = ReplicationStreamConfig::from(config);
        let logical_stream =
            LogicalReplicationStream::new(&config.source_connection_string, stream_config)
                .await
                .map_err(|e| crate::error::CdcError::Replication(e))?;

        Ok(Self { logical_stream })
    }

    pub async fn start(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting PostgreSQL logical replication stream");

        // Start replication from the specified LSN
        let start_xlog = start_lsn.map(|lsn| lsn.0);
        self.logical_stream
            .start(start_xlog)
            .await
            .map_err(|e| crate::error::CdcError::Replication(e))?;

        info!("Logical replication stream started successfully");
        Ok(())
    }

    /// Get next event with cancellation support
    ///
    /// # Arguments  
    /// * `cancellation_token` - Optional cancellation token to abort the operation
    ///
    /// # Returns
    /// * `Ok(Some(event))` - Successfully received a change event
    /// * `Ok(None)` - No event available currently  
    /// * `Err(CdcError::Cancelled(_))` - Operation was cancelled
    /// * `Err(_)` - Other errors occurred
    pub async fn next_event(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<ChangeEvent>> {
        debug!("Fetching next single change event with retry support and cancellation");

        let event = self
            .logical_stream
            .next_event_with_retry(cancellation_token)
            .await
            .map_err(|e| crate::error::CdcError::Replication(e))?;

        if let Some(ref event) = event {
            debug!("Received single change event: {:?}", event);
            // Update last received LSN
            self.logical_stream.state.update_lsn(event.lsn.value());
        }

        Ok(event)
    }

    pub fn maybe_send_feedback(&mut self) {
        self.logical_stream.maybe_send_feedback();
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.logical_stream
            .send_feedback()
            .map_err(|e| crate::error::CdcError::Replication(e))?;

        // Note: LSN persistence is now handled by the consumer's LsnTracker
        // which tracks the actually committed LSN to the destination.
        // This ensures we don't save an LSN that wasn't successfully committed.
        info!(
            "Stopping logical replication stream (last received LSN: {})",
            self.current_lsn()
        );

        self.logical_stream
            .stop()
            .await
            .map_err(|e| crate::error::CdcError::Replication(e))?;
        Ok(())
    }

    #[inline]
    pub fn current_lsn(&self) -> Lsn {
        Lsn::from(self.logical_stream.state.last_received_lsn)
    }
}

pub struct ReplicationManager {
    config: Config,
}

impl ReplicationManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn create_stream_async(&self) -> Result<ReplicationStream> {
        ReplicationStream::new(&self.config).await
    }
}
