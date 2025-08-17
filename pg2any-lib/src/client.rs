use crate::config::Config;
use crate::destinations::destination_factory::is_dml_event;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::types::{ChangeEvent, Lsn};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Main CDC client for coordinating replication and destination writes
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    event_sender: Option<mpsc::Sender<ChangeEvent>>,
    event_receiver: Option<mpsc::Receiver<ChangeEvent>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
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
            cancellation_token: CancellationToken::new(),
            producer_handle: None,
            consumer_handle: None,
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
    
    /// Start CDC replication from a specific LSN
    pub async fn start_replication_from_lsn(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting CDC replication");

        // Ensure we're initialized
        self.init().await?;

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

        let producer_token = self.cancellation_token.clone();

        let producer_handle = tokio::spawn(async move {
            Self::run_producer(replication_stream, event_sender, producer_token).await
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

        let consumer_token = self.cancellation_token.clone();
        let auto_create_tables = self.config.auto_create_tables;

        let consumer_handle = tokio::spawn(async move {
            Self::run_consumer(
                event_receiver,
                destination_handler,
                consumer_token,
                auto_create_tables,
            )
            .await
        });

        // Store the task handles for graceful shutdown
        self.producer_handle = Some(producer_handle);
        self.consumer_handle = Some(consumer_handle);
        let tasks = self.wait_for_tasks_completion();
        info!("CDC replication started successfully");
        tasks.await?;
        Ok(())
    }

    /// Producer task: reads events from PostgreSQL replication stream
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        event_sender: mpsc::Sender<ChangeEvent>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        info!("Starting replication producer (single event mode)");

        loop {
            tokio::select! {
                biased;
                _ = cancellation_token.cancelled() => {
                    info!("Producer received cancellation signal");
                    break;
                }
                result = replication_stream.next_event(&cancellation_token) => {
                    match result {
                        Ok(Some(event)) => {
                            debug!("Producer received single event: {:?}", event.event_type);
                            if let Err(e) = event_sender.send(event).await {
                                error!("Failed to send event to consumer: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            // No event available, wait a bit before trying again
                            debug!("No event available, retrying...");
                            sleep(Duration::from_millis(100)).await; // Reduced sleep time for more responsive feedback
                        }
                        Err(e) => {
                            error!("Error reading from replication stream: {}", e);
                            break;
                        }
                    }
                }
            }
        }
        
        // Gracefully stop the replication stream
        if let Err(e) = replication_stream.stop().await {
            warn!("Error stopping replication stream: {}", e);
        }

        info!("Replication producer stopped gracefully");
        Ok(())
    }

    /// Consumer task: processes events and writes to destination
    async fn run_consumer(
        mut event_receiver: mpsc::Receiver<ChangeEvent>,
        mut destination_handler: Box<dyn DestinationHandler>,
        cancellation_token: CancellationToken,
        auto_create_tables: bool,
    ) -> Result<()> {
        info!("Starting replication consumer (single event mode)");

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Consumer received cancellation signal");
                    // Process any remaining events in the channel
                    let mut remaining_events = 0;
                    while let Ok(event) = event_receiver.try_recv() {
                        remaining_events += 1;
                        debug!("Processing remaining event during shutdown: {:?}", event.event_type);

                        // Auto-create table if needed and enabled
                        if auto_create_tables && is_dml_event(&event) {
                            if let Err(e) = destination_handler.create_table_if_not_exists(&event).await {
                                error!("Failed to auto-create table for event: {}", e);
                            }
                        }

                        // Process the single event immediately
                        if let Err(e) = destination_handler.process_event(&event).await {
                            error!("Failed to process single event during shutdown: {}", e);
                        }
                        remaining_events -= 1;
                    }
                    if remaining_events > 0 {
                        info!("Processed {} remaining events during graceful shutdown", remaining_events);
                    }
                    break;
                }
                event = event_receiver.recv() => {
                    match event {
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
            }
        }

        info!("Replication consumer stopped gracefully");
        Ok(())
    }

    /// Stop the CDC replication process gracefully
    pub async fn stop(&mut self) -> Result<()> {
        info!("Initiating graceful shutdown of CDC replication");

        // Signal cancellation to all tasks
        self.cancellation_token.cancel();

        // Wait for both tasks to complete gracefully
        self.wait_for_tasks_completion().await?;

        // Close destination connection
        if let Some(ref mut handler) = self.destination_handler {
            handler.close().await?;
        }

        info!("CDC replication stopped gracefully");
        Ok(())
    }

    async fn wait_handle(handle: Option<JoinHandle<Result<()>>>, name: &str) -> Result<()> {
        if let Some(h) = handle {
            h.await.expect(&format!("{} task panicked", name))?;
            info!("{} task completed successfully", name);
        }
        Ok(())
    }

    /// Wait for producer and consumer tasks to complete gracefully
    pub async fn wait_for_tasks_completion(&mut self) -> Result<()> {
        Self::wait_handle(self.producer_handle.take(), "Producer").await?;
        Self::wait_handle(self.consumer_handle.take(), "Consumer").await?;
        info!("All CDC tasks completed successfully");
        Ok(())
    }

    /// Check if the CDC client is currently running
    pub fn is_running(&self) -> bool {
        !self.cancellation_token.is_cancelled()
    }

    /// Get the cancellation token for external shutdown coordination
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
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
    pub fn get_stats(&self) -> ReplicationStats {
        ReplicationStats {
            is_running: self.is_running(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigBuilder;
    use crate::types::ChangeEvent;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;

    // Mock destination handler for testing
    pub struct MockDestinationHandler {
        pub events_processed: std::sync::Arc<std::sync::Mutex<Vec<ChangeEvent>>>,
        pub should_fail: bool,
        pub processing_delay: Duration,
    }

    #[async_trait::async_trait]
    impl DestinationHandler for MockDestinationHandler {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: &ChangeEvent) -> Result<()> {
            if self.processing_delay > Duration::ZERO {
                sleep(self.processing_delay).await;
            }

            if self.should_fail {
                return Err(CdcError::generic("Mock error"));
            }

            let mut events = self.events_processed.lock().unwrap();
            events.push(event.clone());
            Ok(())
        }

        async fn create_table_if_not_exists(&mut self, _event: &ChangeEvent) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        async fn health_check(&mut self) -> Result<bool> {
            Ok(true)
        }
    }

    fn create_test_config() -> Config {
        ConfigBuilder::default()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(crate::DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .protocol_version(2)
            .binary_format(false)
            .streaming(true)
            .auto_create_tables(true)
            .connection_timeout(Duration::from_secs(10))
            .query_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .buffer_size(1000)
            .build()
            .expect("Failed to build test config")
    }

    fn create_test_event() -> ChangeEvent {
        ChangeEvent::insert(
            "public".to_string(),
            "test_table".to_string(),
            12345,
            std::collections::HashMap::new(),
        )
    }

    #[tokio::test]
    async fn test_client_creation_and_basic_properties() {
        let config = create_test_config();
        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Test that the client is initially not running (not cancelled)
        assert!(client.is_running());

        // Test that we can get a cancellation token
        let token = client.cancellation_token();
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancellation_token_cancellation() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        let token = client.cancellation_token();
        assert!(!token.is_cancelled());

        // Cancel the token
        client.stop().await.expect("Failed to stop client");

        // The token should be cancelled
        assert!(token.is_cancelled());
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_cancellation_token_propagation() {
        let config = create_test_config();
        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        let token1 = client.cancellation_token();
        let token2 = client.cancellation_token();

        // Both tokens should not be cancelled initially
        assert!(!token1.is_cancelled());
        assert!(!token2.is_cancelled());

        // Cancel the first token
        token1.cancel();

        // Both tokens should be cancelled since they're clones
        assert!(token1.is_cancelled());
        assert!(token2.is_cancelled());
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_producer_task_cancellation() {
        let (_event_sender, _event_receiver) = mpsc::channel::<ChangeEvent>(10);
        let cancellation_token = CancellationToken::new();

        let token_clone = cancellation_token.clone();

        let producer_task = tokio::spawn(async move {
            // Simulate the producer loop structure
            loop {
                tokio::select! {
                    biased;
                    _ = token_clone.cancelled() => {
                        info!("Producer received cancellation signal");
                        break;
                    }
                    _ = sleep(Duration::from_millis(10)) => {
                        // Simulate waiting for events
                        continue;
                    }
                }
            }
            Ok::<(), CdcError>(())
        });

        // Let the producer run for a bit
        sleep(Duration::from_millis(50)).await;

        // Cancel the token
        cancellation_token.cancel();

        // The producer should complete quickly after cancellation
        let result = timeout(Duration::from_millis(100), producer_task)
            .await
            .expect("Producer task should complete quickly after cancellation")
            .expect("Producer task should not panic");

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consumer_task_cancellation() {
        let (_event_sender, event_receiver) = mpsc::channel::<ChangeEvent>(10);
        let cancellation_token = CancellationToken::new();

        let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            events_processed: events_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        let token_clone = cancellation_token.clone();

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer(event_receiver, mock_handler, token_clone, true).await
        });

        // Let the consumer run for a bit
        sleep(Duration::from_millis(50)).await;

        // Cancel the token
        cancellation_token.cancel();

        // The consumer should complete quickly after cancellation
        let result = timeout(Duration::from_millis(100), consumer_task)
            .await
            .expect("Consumer task should complete quickly after cancellation")
            .expect("Consumer task should not panic");

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consumer_processes_remaining_events_on_shutdown() {
        let (event_sender, event_receiver) = mpsc::channel::<ChangeEvent>(10);
        let cancellation_token = CancellationToken::new();

        let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            events_processed: events_processed.clone(),
            should_fail: false,
            processing_delay: Duration::from_millis(10), // Small delay to simulate processing
        });

        // Send some test events
        let test_events = vec![
            create_test_event(),
            create_test_event(),
            create_test_event(),
        ];

        for event in &test_events {
            event_sender
                .send(event.clone())
                .await
                .expect("Failed to send event");
        }

        let token_clone = cancellation_token.clone();
        let events_clone = events_processed.clone();

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer(event_receiver, mock_handler, token_clone, true).await
        });

        // Give the consumer time to start processing
        sleep(Duration::from_millis(50)).await;

        // Cancel the token
        cancellation_token.cancel();

        // Wait for the consumer to complete
        let result = timeout(Duration::from_secs(1), consumer_task)
            .await
            .expect("Consumer task should complete within timeout")
            .expect("Consumer task should not panic");

        assert!(result.is_ok());

        // Check that some events were processed
        let processed_events = events_clone.lock().unwrap();
        assert!(
            !processed_events.is_empty(),
            "Consumer should have processed some events before shutdown"
        );
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_task_handles() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Initially no task handles should be set
        assert!(client.producer_handle.is_none());
        assert!(client.consumer_handle.is_none());

        // Test graceful shutdown without starting tasks
        client
            .stop()
            .await
            .expect("Stop should succeed even without tasks");
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_wait_for_tasks_completion_with_no_tasks() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Should not fail when no tasks are running
        client
            .wait_for_tasks_completion()
            .await
            .expect("Should succeed with no tasks");
    }

    #[tokio::test]
    async fn test_multiple_shutdown_calls_are_safe() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // First stop call
        client.stop().await.expect("First stop call should succeed");
        assert!(!client.is_running());

        // Second stop call should also succeed and not panic
        client
            .stop()
            .await
            .expect("Second stop call should succeed");
        assert!(!client.is_running());

        // Third stop call should also succeed
        client.stop().await.expect("Third stop call should succeed");
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_client_stats_reflect_cancellation_state() {
        let config = create_test_config();
        let mut client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Initially running (not cancelled)
        let stats = client.get_stats();
        assert!(stats.is_running);

        // Stop the client
        client.stop().await.expect("Failed to stop client");

        // Stats should reflect stopped state
        let stats = client.get_stats();
        assert!(!stats.is_running);
    }

    #[tokio::test]
    async fn test_cancellation_token_from_external_source() {
        let config = create_test_config();
        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Get the client's token
        let client_token = client.cancellation_token();

        // Create an external cancellation token
        let external_token = CancellationToken::new();

        // Create a task that links external cancellation to client cancellation
        let client_token_clone = client_token.clone();
        let external_token_clone = external_token.clone();
        let linking_task = tokio::spawn(async move {
            external_token_clone.cancelled().await;
            client_token_clone.cancel();
        });

        // Initially, neither should be cancelled
        assert!(!client_token.is_cancelled());
        assert!(!external_token.is_cancelled());
        assert!(client.is_running());

        // Cancel the external token
        external_token.cancel();

        // Wait for the linking to complete
        linking_task.await.expect("Linking task should complete");

        // Client token should now be cancelled
        assert!(client_token.is_cancelled());
        assert!(!client.is_running());
    }
}
