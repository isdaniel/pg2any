//! PostgreSQL logical replication implementation using libpq-sys
//!
//! This module provides the main replication functionality using the low-level
//! libpq-sys bindings for optimal performance and direct protocol access.

use crate::config::Config;
use crate::connection::PostgresConnection;
use crate::error::{CdcError, Result};
use crate::logical_stream::{LogicalReplicationStream, ReplicationStreamConfig};
use crate::parser::CdcParser;
use crate::types::{ChangeEvent, Lsn};
use std::time::SystemTime;
use tracing::{debug, info};

pub struct ReplicationStream {
    logical_stream: LogicalReplicationStream,
    config: Config,
    parser: CdcParser,
    last_received_lsn: Option<Lsn>,
    last_feedback_time: SystemTime,
}

impl ReplicationStream {
    pub async fn new(_connection: PostgresConnection, config: Config) -> Result<Self> {
        let stream_config = ReplicationStreamConfig::from(&config);
        let logical_stream = LogicalReplicationStream::new(
            &config.source_connection_string,
            stream_config,
        ).await?;

        Ok(Self {
            logical_stream,
            config,
            parser: CdcParser::new(),
            last_received_lsn: None,
            last_feedback_time: SystemTime::now(),
        })
    }

    pub async fn start(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting PostgreSQL logical replication stream");
        
        // Initialize the logical stream
        self.logical_stream.initialize().await?;
        
        // Start replication from the specified LSN
        let start_xlog = start_lsn.map(|lsn| lsn.0);
        self.logical_stream.start(start_xlog).await?;
        
        info!("Logical replication stream started successfully");
        Ok(())
    }

    pub async fn next_batch(&mut self) -> Result<Option<Vec<ChangeEvent>>> {
        debug!("Fetching next batch of changes");
        
        let batch_size = self.config.batch_size;
        let events = self.logical_stream.next_batch(batch_size).await?;
        
        if !events.is_empty() {
            debug!("Received {} change events", events.len());
            
            // Update last received LSN
            if let Some(last_event) = events.last() {
                if let Some(lsn_str) = &last_event.lsn {
                    if let Ok(lsn_value) = crate::pg_replication::parse_lsn(lsn_str) {
                        self.last_received_lsn = Some(Lsn(lsn_value));
                    }
                }
            }
            
            Ok(Some(events))
        } else {
            Ok(None)
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping logical replication stream");
        self.logical_stream.stop().await?;
        Ok(())
    }

    pub fn current_lsn(&self) -> Option<Lsn> {
        self.last_received_lsn
    }
}

pub struct ReplicationManager {
    config: Config
}

impl ReplicationManager {
    pub fn new(config: Config) -> Self {
        Self {
            config
        }
    }

    pub async fn create_stream_async(&mut self) -> Result<ReplicationStream> {
        let connection = PostgresConnection::placeholder();
        ReplicationStream::new(connection, self.config.clone()).await
    }
}
