use crate::config::Config;
use crate::destinations::destination_factory::is_dml_event;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::types::{ChangeEvent, Lsn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tracing::{debug, error, info};

/// Main CDC client for coordinating replication and destination writes
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    event_sender: Option<mpsc::Sender<ChangeEvent>>,
    event_receiver: Option<mpsc::Receiver<ChangeEvent>>,
    is_running: Arc<Mutex<bool>>,
}

impl CdcClient {
    /// Create a new CDC client
    pub async fn new(config: Config) -> Result<Self> {
        info!("Creating CDC client");

        // Create destination handler
        let destination_handler = DestinationFactory::create(config.destination_type.clone())?;

        let replication_manager = ReplicationManager::new(config.clone());

        // Create event channel
        let (event_sender, event_receiver) = mpsc::channel(config.buffer_size);

        Ok(Self {
            config,
            replication_manager: Some(replication_manager),
            destination_handler: Some(destination_handler),
            event_sender: Some(event_sender),
            event_receiver: Some(event_receiver),
            is_running: Arc::new(Mutex::new(false)),
        })
    }

    /// Initialize the CDC client
    pub async fn init(&mut self) -> Result<()> {
        info!("Initializing CDC client");

        // Connect to destination database
        if let Some(ref mut handler) = self.destination_handler {
            handler
                .connect(&self.config.destination_connection_string)
                .await?;
        }

        info!("CDC client initialized successfully");
        Ok(())
    }

    /// Start the CDC replication process
    pub async fn start_replication(&mut self) -> Result<()> {
        self.start_replication_from_lsn(None).await
    }

    /// Start CDC replication from a specific LSN
    pub async fn start_replication_from_lsn(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication");

        // Ensure we're initialized
        self.init().await?;

        // todo this could be state machine support more state
        {
            let mut running = self.is_running.lock().await;
            *running = true;
        }

        // Create replication stream using async method
        let mut replication_manager = self
            .replication_manager
            .take()
            .ok_or_else(|| CdcError::generic("Replication manager not available"))?;

        let mut replication_stream = replication_manager.create_stream_async().await?;

        // Start the replication stream
        replication_stream.start(start_lsn).await?;

        // Start the producer task (reads from PostgreSQL)
        let event_sender = self
            .event_sender
            .take()
            .ok_or_else(|| CdcError::generic("Event sender not available"))?;

        let is_running_producer = Arc::clone(&self.is_running);

        let producer_handle = tokio::spawn(async move {
            Self::run_producer(
                replication_stream,
                event_sender,
                is_running_producer
            )
            .await
        });

        // Start the consumer task (writes to destination)
        let event_receiver = self
            .event_receiver
            .take()
            .ok_or_else(|| CdcError::generic("Event receiver not available"))?;

        let destination_handler = self
            .destination_handler
            .take()
            .ok_or_else(|| CdcError::generic("Destination handler not available"))?;

        let is_running_consumer = Arc::clone(&self.is_running);
        let auto_create_tables = self.config.auto_create_tables;

        let consumer_handle = tokio::spawn(async move {
            Self::run_consumer(
                event_receiver,
                destination_handler,
                is_running_consumer,
                auto_create_tables,
            )
            .await
        });

        info!("CDC replication started successfully");

        // Wait for both tasks to complete
        let (producer_result, consumer_result) = tokio::join!(producer_handle, consumer_handle);

        match (producer_result, consumer_result) {
            (Ok(Ok(())), Ok(Ok(()))) => {
                info!("CDC replication completed successfully");
                Ok(())
            }
            (Ok(Err(e)), _) | (_, Ok(Err(e))) => {
                error!("CDC replication failed: {}", e);
                Err(e)
            }
            (Err(e), _) | (_, Err(e)) => {
                error!("CDC task panicked: {}", e);
                Err(CdcError::generic(format!("Task panic: {}", e)))
            }
        }
    }

    /// Producer task: reads events from PostgreSQL replication stream
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        event_sender: mpsc::Sender<ChangeEvent>,
        is_running: Arc<Mutex<bool>>
    ) -> Result<()> {
        info!("Starting replication producer (single event mode)");

        while *is_running.lock().await {
            match replication_stream.next_event().await {
                Ok(Some(event)) => {
                    debug!("Producer received single event: {:?}", event.event_type);
                    if let Err(e) = event_sender.send(event).await {
                        error!("Failed to send event to consumer: {}", e);
                        break;
                    }
                }
                Ok(None) => {
                    // No event available, wait a bit before trying again
                    sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    error!("Error reading from replication stream: {}", e);
                    break;
                    // Try to reconnect after a delay
                    //sleep(Duration::from_secs(5)).await;
                    // In a production system, you'd want more sophisticated retry logic
                }
            }
        }

        info!("Replication producer stopped");
        Ok(())
    }

    /// Consumer task: processes events and writes to destination
    async fn run_consumer(
        mut event_receiver: mpsc::Receiver<ChangeEvent>,
        mut destination_handler: Box<dyn DestinationHandler>,
        is_running: Arc<Mutex<bool>>,
        auto_create_tables: bool,
    ) -> Result<()> {
        info!("Starting replication consumer (single event mode)");

        loop {
            let has_pending = event_receiver.len() > 0;
            if !*is_running.lock().await && !has_pending {
                break;
            }

            // Wait for a single event
            match event_receiver.recv().await {
                Some(event) => {
                    debug!("Consumer processing event: {:?}", event.event_type);

                    // Auto-create table if needed and enabled
                    if auto_create_tables && is_dml_event(&event) {
                        if let Err(e) = destination_handler.create_table_if_not_exists(&event).await {
                            error!("Failed to auto-create table for event: {}", e);
                        }
                    }

                    // Process the single event immediately
                    if let Err(e) = destination_handler.process_event(&event).await {
                        error!("Failed to process single event: {}", e);
                        // Continue processing other events even if one fails
                    }
                }
                None => {
                    // Channel closed, exit the loop
                    break;
                }
            }
        }

        info!("Replication consumer stopped");
        Ok(())
    }

    /// Stop the CDC replication process
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping CDC replication");

        {
            let mut running = self.is_running.lock().await;
            *running = false;
        }

        // Close destination connection
        if let Some(ref mut handler) = self.destination_handler {
            handler.close().await?;
        }

        info!("CDC replication stopped");
        Ok(())
    }

    /// Check if the CDC client is currently running
    pub async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }

    /// Get the current configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Health check for all components
    pub async fn health_check(&mut self) -> Result<bool> {
        // Check destination connection
        if let Some(ref mut handler) = self.destination_handler {
            if !handler.health_check().await? {
                return Ok(false);
            }
        }

        // In a full implementation, you'd also check:
        // - PostgreSQL connection health
        // - Replication slot status
        // - Event processing lag
        // - Memory usage, etc.

        Ok(true)
    }

    /// Get replication statistics
    pub async fn get_stats(&self) -> ReplicationStats {
        ReplicationStats {
            is_running: self.is_running().await,
            events_processed: 0, // In a real implementation, you'd track this
            last_processed_lsn: None,
            lag_seconds: None,
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub is_running: bool,
    pub events_processed: u64,
    pub last_processed_lsn: Option<Lsn>,
    pub lag_seconds: Option<f64>,
}

impl Drop for CdcClient {
    fn drop(&mut self) {
        // Note: This is a synchronous drop, so we can't call async methods here
        // In a production system, you might want to ensure graceful shutdown
        debug!("CDC client dropped");
    }
}
