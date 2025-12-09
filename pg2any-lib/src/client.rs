use crate::config::Config;
use crate::destinations::{DestinationFactory, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::monitoring::{
    MetricsCollector, MetricsCollectorTrait, ProcessingTimer, ProcessingTimerTrait,
};
use crate::pg_replication::{ReplicationManager, ReplicationStream};
use crate::types::{ChangeEvent, Lsn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Main CDC client for coordinating replication and destination writes
///
/// # Parallel Processing Architecture
///
/// This client uses a producer-consumer pattern with configurable parallelism:
///
/// ## Single Producer (PostgreSQL Reader)
/// - Reads from PostgreSQL logical replication stream
/// - Sends events to a bounded channel (configurable buffer size)
/// - Handles LSN tracking and heartbeats
///
/// ## Multiple Consumers (Destination Writers)
/// - Configurable number of worker threads (CDC_CONSUMER_WORKERS)
/// - **Each worker has its own destination database connection**
/// - Workers use a work-stealing pattern via shared channel
/// - TRUE parallelism: workers can write to destination simultaneously
/// - No mutex contention on writes (each worker owns its connection)
///
/// ## Performance Characteristics
///
/// ### With 1 Worker (default):
/// - Sequential processing
/// - Single connection overhead
/// - Predictable order of operations
///
/// ### With Multiple Workers (e.g., 8):
/// - Parallel writes to destination database
/// - Linear scaling for write-heavy workloads
/// - Best for destinations that support concurrent connections (MySQL, PostgreSQL)
/// - Each worker independently processes events without blocking others
///
/// ### Configuration Tips
/// - `CDC_CONSUMER_WORKERS=1`: Single-threaded, lowest latency for low-volume
/// - `CDC_CONSUMER_WORKERS=2-4`: Good balance for moderate throughput
/// - `CDC_CONSUMER_WORKERS=8-16`: High throughput, ensure destination can handle connections
/// - `CDC_BUFFER_SIZE=1000-5000`: Typical buffer size for event channel
/// - `CDC_BUFFER_SIZE=10000+`: Large buffer for burst traffic handling
pub struct CdcClient {
    config: Config,
    replication_manager: Option<ReplicationManager>,
    destination_handler: Option<Box<dyn DestinationHandler>>,
    event_sender: Option<mpsc::Sender<ChangeEvent>>,
    event_receiver: Option<mpsc::Receiver<ChangeEvent>>,
    cancellation_token: CancellationToken,
    producer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    consumer_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    metrics_collector: Arc<MetricsCollector>,
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
            consumer_handles: Vec::new(),
            metrics_collector: Arc::new(MetricsCollector::new()),
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

            // Set schema mappings if any are configured
            if !self.config.schema_mappings.is_empty() {
                handler.set_schema_mappings(self.config.schema_mappings.clone());
                info!("Schema mappings applied: {:?}", self.config.schema_mappings);
            }
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
        let replication_manager = self
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

        let producer_handle = {
            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let start_lsn = start_lsn.unwrap_or_else(|| Lsn::new(0));

            tokio::spawn(Self::run_producer(
                replication_stream,
                event_sender,
                token,
                start_lsn,
                metrics,
            ))
        };

        // Start consumer worker tasks with per-worker destination connections
        let num_workers = self.config.consumer_workers;
        let event_receiver = self
            .event_receiver
            .take()
            .ok_or_else(|| CdcError::generic("Event receiver not available"))?;

        // Discard the pre-created destination handler from init() since we'll create per-worker handlers
        let _ = self.destination_handler.take();

        let dest_type = &self.config.destination_type;
        let dest_connection_string = self.config.destination_connection_string.clone();
        let schema_mappings = self.config.schema_mappings.clone();

        info!(
            "Starting {} consumer worker(s) for TRUE parallel processing (per-worker connections)",
            num_workers
        );

        // Share the receiver among all workers for work-stealing pattern
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));

        for worker_id in 0..num_workers {
            // Create a new destination handler for each worker
            let mut worker_destination = DestinationFactory::create(dest_type.clone())?;

            // Connect the worker's destination handler
            worker_destination.connect(&dest_connection_string).await?;

            // Apply schema mappings to the worker's handler
            if !schema_mappings.is_empty() {
                worker_destination.set_schema_mappings(schema_mappings.clone());
            }

            info!("Worker {} destination connection established", worker_id);

            let token = self.cancellation_token.clone();
            let metrics = self.metrics_collector.clone();
            let dest_type_str = dest_type.to_string();
            let shared_recv = shared_receiver.clone();
            let is_primary = worker_id == 0;

            let consumer_handle = tokio::spawn(Self::run_consumer_loop(
                worker_id,
                shared_recv,
                worker_destination,
                token,
                metrics,
                dest_type_str,
                is_primary,
            ));
            self.consumer_handles.push(consumer_handle);
        }

        // Update metrics for active connections
        self.metrics_collector
            .update_active_connections(num_workers, "consumer");

        self.producer_handle = Some(producer_handle);

        self.start_server_uptime();

        info!(
            "CDC replication started successfully with {} consumer worker(s)",
            num_workers
        );
        self.cancellation_token.cancelled().await;
        Ok(())
    }

    // Start metrics update task
    fn start_server_uptime(&mut self) {
        let metrics = self.metrics_collector.clone();
        let token = self.cancellation_token.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = interval.tick() => {
                        metrics.update_uptime();
                        metrics.update_events_rate();
                    }
                }
            }
        });
    }

    /// Producer task: reads events from PostgreSQL replication stream
    async fn run_producer(
        mut replication_stream: ReplicationStream,
        event_sender: mpsc::Sender<ChangeEvent>,
        cancellation_token: CancellationToken,
        start_lsn: Lsn,
        metrics_collector: Arc<MetricsCollector>,
    ) -> Result<()> {
        info!("Starting replication producer (single event mode)");

        // Initialize connection status
        metrics_collector.update_source_connection_status(true);

        while !cancellation_token.is_cancelled() {
            match replication_stream.next_event(&cancellation_token).await {
                Ok(Some(event)) => {
                    if let Some(current_lsn) = event.lsn {
                        if current_lsn <= start_lsn {
                            debug!("Skipping event with LSN {} <= {}", current_lsn, start_lsn);
                            continue;
                        }

                        // Record current LSN
                        metrics_collector.record_received_lsn(current_lsn.0);
                    }

                    debug!("Producer received single event: {:?}", event.event_type);

                    if let Err(e) = event_sender.send(event).await {
                        error!("Failed to send event to consumer: {}", e);
                        metrics_collector.record_error("event_send_failed", "producer");
                        break;
                    }
                }
                Ok(None) => {
                    // No event available
                    if cancellation_token.is_cancelled() {
                        info!("Ok(None) Producer received cancellation signal");
                        break;
                    }
                }
                Err(e) => {
                    error!("Error reading from replication stream: {}", e);
                    metrics_collector.record_error("replication_stream_error", "producer");
                    break;
                }
            }
        }

        info!("Producer received cancellation signal");

        // Update connection status on shutdown
        metrics_collector.update_source_connection_status(false);

        // Gracefully stop the replication stream
        replication_stream.stop().await?;

        info!("Replication producer stopped gracefully");
        Ok(())
    }

    /// Unified consumer loop for all worker modes (work-stealing pattern with per-worker connections)
    ///
    /// # Arguments
    /// * `worker_id` - Identifier for this worker (0 for single worker mode)
    /// * `event_receiver` - Shared receiver for change events (work-stealing)
    /// * `destination_handler` - Per-worker destination handler (no mutex needed!)
    /// * `cancellation_token` - Token for graceful shutdown
    /// * `metrics_collector` - Metrics collector
    /// * `destination_type` - Type of destination for metrics labeling
    /// * `is_primary` - Whether this worker handles connection status and event draining
    async fn run_consumer_loop(
        worker_id: usize,
        event_receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ChangeEvent>>>,
        mut destination_handler: Box<dyn DestinationHandler>,
        cancellation_token: CancellationToken,
        metrics_collector: Arc<MetricsCollector>,
        destination_type: String,
        is_primary: bool,
    ) -> Result<()> {
        info!(
            "Starting consumer worker {}{}",
            worker_id,
            if is_primary { " (primary)" } else { "" }
        );

        // Initialize destination connection status (only primary worker)
        if is_primary {
            metrics_collector.update_destination_connection_status(&destination_type, true);
        }

        let mut queue_size_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            tokio::select! {
                biased;

                // Handle graceful shutdown
                _ = cancellation_token.cancelled() => {
                    info!("Consumer worker {} received cancellation signal", worker_id);
                    // Only primary worker drains remaining events
                    if is_primary {
                        Self::drain_remaining_events_per_worker(
                            &event_receiver,
                            &mut destination_handler,
                            &metrics_collector,
                            &destination_type,
                            worker_id,
                        ).await;
                    }
                    break;
                }

                // Periodic queue size reporting (only primary worker)
                _ = queue_size_interval.tick() => {
                    if is_primary {
                        let receiver = event_receiver.lock().await;
                        let queue_length = receiver.len();
                        debug!("Consumer worker {} queue length: {}", worker_id, queue_length);
                        metrics_collector.update_consumer_queue_length(queue_length);
                    }
                }

                // Efficiently receive and process events (blocking recv, not polling!)
                event = async {
                    let mut receiver = event_receiver.lock().await;
                    receiver.recv().await
                } => {
                    match event {
                        Some(event) => {
                            debug!("Consumer worker {} processing event: {:?}", worker_id, event.event_type);
                            Self::process_event_per_worker(
                                event,
                                &mut destination_handler,
                                &metrics_collector,
                                &destination_type,
                                worker_id,
                            ).await;
                        }
                        None => {
                            // Channel closed, break loop
                            info!("Consumer worker {} detected channel closure", worker_id);
                            break;
                        }
                    }
                }
            }
        }

        // Close the worker's destination connection
        if let Err(e) = destination_handler.close().await {
            error!(
                "Worker {} failed to close destination connection: {}",
                worker_id, e
            );
        }

        // Update destination connection status on shutdown (only primary worker)
        if is_primary {
            metrics_collector.update_destination_connection_status(&destination_type, false);
        }

        info!("Consumer worker {} stopped gracefully", worker_id);
        Ok(())
    }

    /// Drain any remaining events from the channel during shutdown (per-worker version)
    async fn drain_remaining_events_per_worker(
        event_receiver: &Arc<tokio::sync::Mutex<mpsc::Receiver<ChangeEvent>>>,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
        worker_id: usize,
    ) {
        let mut count = 0;
        loop {
            let event = {
                let mut receiver = event_receiver.lock().await;
                receiver.try_recv().ok()
            };

            match event {
                Some(event) => {
                    debug!("Processing remaining event during shutdown: {:?}", event);
                    Self::process_event_per_worker(
                        event,
                        destination_handler,
                        metrics_collector,
                        destination_type,
                        worker_id,
                    )
                    .await;
                    count += 1;
                }
                None => break,
            }
        }

        info!(
            "Worker {} processed {} remaining events during graceful shutdown",
            worker_id, count
        );
    }

    /// Process a single change event with per-worker destination handler (no mutex!)
    async fn process_event_per_worker(
        event: ChangeEvent,
        destination_handler: &mut Box<dyn DestinationHandler>,
        metrics_collector: &Arc<MetricsCollector>,
        destination_type: &str,
        worker_id: usize,
    ) {
        let event_type = event.event_type_str();
        let timer = ProcessingTimer::start(event_type, destination_type);

        // Process event directly without mutex lock - each worker has its own handler!
        match destination_handler.process_event(&event).await {
            Ok(_) => {
                metrics_collector.record_event(&event, destination_type);
                timer.finish(metrics_collector.as_ref());
            }
            Err(e) => {
                error!("Worker {} failed to process event: {}", worker_id, e);
                metrics_collector.record_error("event_processing_failed", "consumer");
            }
        }
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
        let producer_task = Self::wait_handle(self.producer_handle.take(), "Producer");

        // Wait for all consumer workers
        let consumer_handles: Vec<_> = self.consumer_handles.drain(..).collect();
        let num_workers = consumer_handles.len();

        let consumer_tasks = async {
            let mut results = Vec::new();
            for (idx, handle) in consumer_handles.into_iter().enumerate() {
                let result = handle.await;
                match result {
                    Ok(Ok(_)) => {
                        debug!("Consumer worker {} completed successfully", idx);
                        results.push(Ok(()));
                    }
                    Ok(Err(e)) => {
                        error!("Consumer worker {} failed: {}", idx, e);
                        results.push(Err(e));
                    }
                    Err(e) => {
                        error!("Consumer worker {} panicked: {}", idx, e);
                        results.push(Err(CdcError::generic(format!(
                            "Consumer worker {} panicked",
                            idx
                        ))));
                    }
                }
            }
            // Return first error if any
            for result in results {
                result?;
            }
            Ok::<(), CdcError>(())
        };

        match tokio::join!(producer_task, consumer_tasks) {
            (Ok(_), Ok(_)) => {
                info!(
                    "All CDC tasks completed successfully!! ({} consumer workers)",
                    num_workers.max(1)
                );
            }
            (Err(e), _) | (_, Err(e)) => {
                error!("Task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Check if the CDC client is currently running
    #[inline]
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

    /// Get metrics collector for accessing metrics
    pub fn metrics_collector(&self) -> Arc<MetricsCollector> {
        self.metrics_collector.clone()
    }

    /// Get metrics in Prometheus text format
    pub fn get_metrics(&self) -> Result<String> {
        self.metrics_collector.get_metrics()
    }

    /// Initialize build information in metrics
    pub fn init_build_info(&self, version: &str) {
        self.metrics_collector.init_build_info(version);
    }

    /// Health check for all components
    pub async fn health_check(&mut self) -> Result<bool> {
        // Check destination connection
        if let Some(ref mut handler) = self.destination_handler {
            if !handler.health_check().await? {
                self.metrics_collector.update_destination_connection_status(
                    &self.config.destination_type.to_string(),
                    false,
                );
                return Ok(false);
            } else {
                self.metrics_collector.update_destination_connection_status(
                    &self.config.destination_type.to_string(),
                    true,
                );
            }
        }

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

        fn set_schema_mappings(&mut self, _mappings: std::collections::HashMap<String, String>) {
            // Mock implementation - no-op
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
            .connection_timeout(Duration::from_secs(10))
            .query_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .buffer_size(500)
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
        let metrics_collector = Arc::new(MetricsCollector::new());

        // Use new per-worker API - pass Box directly, not Arc<Mutex>
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));
        let boxed_handler: Box<dyn DestinationHandler> = mock_handler;

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                0,
                shared_receiver,
                boxed_handler,
                token_clone,
                metrics_collector,
                "test".to_string(),
                true, // is_primary
            )
            .await
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
        let metrics_collector = Arc::new(MetricsCollector::new());

        // Use new per-worker API - pass Box directly, not Arc<Mutex>
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));
        let boxed_handler: Box<dyn DestinationHandler> = mock_handler;

        let consumer_task = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                0,
                shared_receiver,
                boxed_handler,
                token_clone,
                metrics_collector,
                "test".to_string(),
                true, // is_primary
            )
            .await
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
        assert!(client.consumer_handles.is_empty());

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

    #[tokio::test]
    async fn test_per_worker_connection_architecture() {
        // Test that multiple workers each get their own destination handler
        let (event_sender, event_receiver) = mpsc::channel::<ChangeEvent>(100);
        let cancellation_token = CancellationToken::new();
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));

        // Create multiple workers with their own handlers
        let num_workers = 3;
        let mut worker_handles = Vec::new();
        let mut events_trackers = Vec::new();

        for worker_id in 0..num_workers {
            let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            events_trackers.push(events_processed.clone());

            let mock_handler = Box::new(MockDestinationHandler {
                events_processed,
                should_fail: false,
                processing_delay: Duration::from_millis(5),
            });

            let token = cancellation_token.clone();
            let metrics = Arc::new(MetricsCollector::new());
            let receiver = shared_receiver.clone();

            let handle = tokio::spawn(async move {
                CdcClient::run_consumer_loop(
                    worker_id,
                    receiver,
                    mock_handler,
                    token,
                    metrics,
                    "test".to_string(),
                    worker_id == 0,
                )
                .await
            });
            worker_handles.push(handle);
        }

        // Send test events
        let num_events = 30;
        for i in 0..num_events {
            let mut event = create_test_event();
            event.lsn = Some(Lsn::new(i as u64)); // Use LSN to track event ID
            event_sender
                .send(event)
                .await
                .expect("Failed to send event");
        }

        // Let workers process
        sleep(Duration::from_millis(200)).await;

        // Cancel and wait
        cancellation_token.cancel();
        for handle in worker_handles {
            let result = timeout(Duration::from_secs(1), handle)
                .await
                .expect("Worker should complete")
                .expect("Worker should not panic");
            assert!(result.is_ok());
        }

        // Verify all events were processed
        let mut total_processed = 0;
        for tracker in &events_trackers {
            let count = tracker.lock().unwrap().len();
            total_processed += count;
            println!("Worker processed {} events", count);
        }

        assert_eq!(
            total_processed, num_events,
            "All events should be processed by workers"
        );

        // Verify work was distributed (at least 2 workers should have processed events)
        let workers_with_events = events_trackers
            .iter()
            .filter(|tracker| !tracker.lock().unwrap().is_empty())
            .count();
        assert!(
            workers_with_events >= 2,
            "Work should be distributed across multiple workers"
        );
    }

    #[tokio::test]
    async fn test_worker_independence_no_mutex_contention() {
        // Test that workers can process events independently without blocking each other
        let (event_sender, event_receiver) = mpsc::channel::<ChangeEvent>(100);
        let cancellation_token = CancellationToken::new();
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));

        let num_workers = 4;
        let mut worker_handles = Vec::new();
        let processing_times = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        for worker_id in 0..num_workers {
            let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let times = processing_times.clone();

            let mock_handler = Box::new(MockDestinationHandler {
                events_processed,
                should_fail: false,
                processing_delay: Duration::from_millis(20), // Simulate slow writes
            });

            let token = cancellation_token.clone();
            let metrics = Arc::new(MetricsCollector::new());
            let receiver = shared_receiver.clone();

            let handle = tokio::spawn(async move {
                let start = std::time::Instant::now();
                let result = CdcClient::run_consumer_loop(
                    worker_id,
                    receiver,
                    mock_handler,
                    token,
                    metrics,
                    "test".to_string(),
                    worker_id == 0,
                )
                .await;
                let elapsed = start.elapsed();
                times.lock().unwrap().push(elapsed);
                result
            });
            worker_handles.push(handle);
        }

        // Send events
        let num_events = 20;
        for _ in 0..num_events {
            event_sender
                .send(create_test_event())
                .await
                .expect("Failed to send");
        }

        // Let workers process
        sleep(Duration::from_millis(150)).await;

        cancellation_token.cancel();
        for handle in worker_handles {
            timeout(Duration::from_secs(1), handle)
                .await
                .expect("Worker should complete")
                .expect("Worker should not panic")
                .expect("Worker should succeed");
        }

        // With 4 workers processing 20 events (each taking 20ms),
        // if they were sequential (mutex bottleneck), it would take 400ms+
        // With parallel processing, it should take much less (around 100-150ms)
        let times = processing_times.lock().unwrap();
        let max_time = times.iter().max().unwrap();
        println!("Max worker time: {:?}", max_time);

        // The key test: parallel processing should complete faster than sequential
        // Sequential would be: 20 events * 20ms = 400ms minimum
        // Parallel with 4 workers: ~100-150ms expected
        assert!(
            max_time.as_millis() < 300,
            "Parallel processing should be faster than sequential (got {}ms)",
            max_time.as_millis()
        );
    }

    #[tokio::test]
    async fn test_process_event_per_worker_direct_no_mutex() {
        // Test that process_event_per_worker doesn't use mutex (direct mutable access)
        let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler: Box<dyn DestinationHandler> = Box::new(MockDestinationHandler {
            events_processed: events_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        // Need to use a mutable variable for the trait object
        let mut handler = mock_handler;
        let metrics = Arc::new(MetricsCollector::new());
        let mut event = create_test_event();
        event.lsn = Some(Lsn::new(12345)); // Set identifiable LSN

        // This should work with direct mutable reference (no mutex)
        CdcClient::process_event_per_worker(event.clone(), &mut handler, &metrics, "test", 0).await;

        // Verify event was processed
        let processed = events_processed.lock().unwrap();
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].lsn, event.lsn);
    }

    #[tokio::test]
    async fn test_drain_remaining_events_per_worker() {
        // Test that primary worker drains remaining events on shutdown
        let (event_sender, event_receiver) = mpsc::channel::<ChangeEvent>(10);
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));

        let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler: Box<dyn DestinationHandler> = Box::new(MockDestinationHandler {
            events_processed: events_processed.clone(),
            should_fail: false,
            processing_delay: Duration::ZERO,
        });

        // Need mutable variable for trait object
        let mut handler = mock_handler;

        // Send events before draining
        let num_events = 5;
        for _ in 0..num_events {
            event_sender
                .send(create_test_event())
                .await
                .expect("Send failed");
        }
        drop(event_sender); // Close sender

        let metrics = Arc::new(MetricsCollector::new());

        // Drain events
        CdcClient::drain_remaining_events_per_worker(
            &shared_receiver,
            &mut handler,
            &metrics,
            "test",
            0,
        )
        .await;

        // Verify all events were drained and processed
        let processed = events_processed.lock().unwrap();
        assert_eq!(
            processed.len(),
            num_events,
            "All remaining events should be drained"
        );
    }

    #[tokio::test]
    async fn test_worker_error_handling() {
        // Test that worker continues processing even if some events fail
        let (event_sender, event_receiver) = mpsc::channel::<ChangeEvent>(10);
        let cancellation_token = CancellationToken::new();
        let shared_receiver = Arc::new(tokio::sync::Mutex::new(event_receiver));

        let events_processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mock_handler = Box::new(MockDestinationHandler {
            events_processed: events_processed.clone(),
            should_fail: true, // Simulate errors
            processing_delay: Duration::ZERO,
        });

        let token = cancellation_token.clone();
        let metrics = Arc::new(MetricsCollector::new());

        let worker_handle = tokio::spawn(async move {
            CdcClient::run_consumer_loop(
                0,
                shared_receiver,
                mock_handler,
                token,
                metrics,
                "test".to_string(),
                true,
            )
            .await
        });

        // Send events (they will fail but worker should continue)
        for _ in 0..5 {
            event_sender
                .send(create_test_event())
                .await
                .expect("Send failed");
        }

        sleep(Duration::from_millis(50)).await;
        cancellation_token.cancel();

        let result = timeout(Duration::from_secs(1), worker_handle)
            .await
            .expect("Worker should complete")
            .expect("Worker should not panic");

        assert!(result.is_ok(), "Worker should handle errors gracefully");
    }

    #[tokio::test]
    async fn test_configurable_buffer_size() {
        // Test that buffer_size configuration is respected
        let custom_buffer_size = 2000;
        let config = ConfigBuilder::default()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(crate::DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .buffer_size(custom_buffer_size)
            .build()
            .expect("Failed to build config");

        assert_eq!(config.buffer_size, custom_buffer_size);

        let client = CdcClient::new(config)
            .await
            .expect("Failed to create client");

        // Verify the client was created successfully with custom buffer
        assert_eq!(client.config().buffer_size, custom_buffer_size);
    }
}
