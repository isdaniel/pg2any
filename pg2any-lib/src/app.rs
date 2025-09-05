//! CDC Application Runner
//!
//! This module provides a high-level application runner that encapsulates
//! the complete CDC workflow including client initialization, shutdown handling,
//! and replication management.

use tokio_util::sync::CancellationToken;

use crate::{client::CdcClient, config::Config, types::Lsn, CdcResult};

/// Configuration for the CDC application
#[derive(Debug, Clone)]
pub struct CdcAppConfig {
    /// CDC configuration
    pub cdc_config: Config,
    /// Metrics server port (if metrics feature is enabled)
    pub metrics_port: Option<u16>,
    /// Application version for metrics
    pub version: String,
}

impl CdcAppConfig {
    /// Create a new CDC application configuration
    pub fn new(cdc_config: Config) -> Self {
        Self {
            cdc_config,
            metrics_port: None,
            version: "unknown".to_string(),
        }
    }

    /// Set the metrics server port
    pub fn with_metrics_port(&mut self, port: u16) {
        self.metrics_port = Some(port);
    }

    /// Set the application version
    pub fn with_version(&mut self, version: &str) {
        self.version = version.to_string();
    }
}

/// High-level CDC application runner
///
/// This struct encapsulates the complete CDC application workflow,
/// providing a clean interface for running CDC replication with
/// proper initialization, shutdown handling, and error management.
pub struct CdcApp {
    client: CdcClient,
    config: CdcAppConfig,
}

impl CdcApp {
    /// Create a new CDC application instance
    ///
    /// # Arguments
    ///
    /// * `config` - The CDC application configuration to use
    ///
    /// # Returns
    ///
    /// Returns a `CdcResult<CdcApp>` with the initialized application instance.
    ///
    /// # Errors
    ///
    /// Returns `CdcError` if the CDC client cannot be created or initialized.
    pub async fn new(config: CdcAppConfig) -> CdcResult<Self> {
        tracing::info!("🔧 Initializing CDC client");
        let mut client = CdcClient::new(config.cdc_config.clone()).await?;

        tracing::info!("⚙️  Performing CDC client initialization");
        client.init().await?;

        tracing::info!("✅ CDC client initialized successfully");

        Ok(Self { client, config })
    }

    /// Create a new CDC application instance with just the CDC config (backwards compatible)
    ///
    /// # Arguments
    ///
    /// * `cdc_config` - The CDC configuration to use
    ///
    /// # Returns
    ///
    /// Returns a `CdcResult<CdcApp>` with the initialized application instance.
    ///
    /// # Errors
    ///
    /// Returns `CdcError` if the CDC client cannot be created or initialized.
    pub async fn from_config(cdc_config: Config) -> CdcResult<Self> {
        let app_config = CdcAppConfig::new(cdc_config);
        Self::new(app_config).await
    }

    /// Run the CDC application with graceful shutdown handling
    ///
    /// This method starts the CDC replication process and handles graceful shutdown
    /// when shutdown signals are received. It automatically loads the last LSN
    /// from persistence to resume replication from where it left off.
    ///
    /// This method now automatically handles metrics server initialization
    /// when the metrics feature is enabled, removing the need for feature checking
    /// in the main application.
    ///
    /// # Arguments
    ///
    /// * `lsn_file_path` - Optional path to the LSN persistence file. If None,
    ///   uses the default from environment variables or "./pg2any_last_lsn"
    ///
    /// # Returns
    ///
    /// Returns `CdcResult<()>` when the application completes successfully
    /// or is gracefully shut down.
    ///
    /// # Errors
    ///
    /// Returns `CdcError` if:
    /// - Replication fails to start or encounters an error
    /// - Shutdown handling fails
    /// - Client stop operation fails
    /// - Metrics server fails to start (when metrics feature is enabled)
    pub async fn run(&mut self, lsn_file_path: Option<&str>) -> CdcResult<()> {
        // Initialize build info for metrics
        self.client.init_build_info(&self.config.version);

        // Start metrics server if metrics feature is enabled and port is configured
        #[cfg(feature = "metrics")]
        let metrics_server = if let Some(port) = self.config.metrics_port {
            tracing::info!("Starting metrics server on port {}", port);
            let server = crate::create_metrics_server(port);
            Some(tokio::spawn(async move { server.start().await }))
        } else {
            None
        };

        // Set up graceful shutdown handling with the client's cancellation token
        let shutdown_handler = tokio::spawn(setup_shutdown_handler(self.client.cancellation_token()));

        // Start the CDC replication process
        tracing::info!("Starting CDC replication stream");
        tracing::info!("This will continuously monitor PostgreSQL changes");

        // Try to load persisted last LSN from file so we can continue where we left off
        let start_lsn = load_last_lsn(lsn_file_path);

        // Run CDC replication with graceful shutdown
        // Handle metrics server conditionally
        #[cfg(feature = "metrics")]
        if let Some(metrics_server) = metrics_server {
            tokio::select! {
                result = self.client.start_replication_from_lsn(start_lsn) => {
                    match result {
                        Ok(()) => {
                            tracing::info!("CDC replication completed successfully");
                            Ok(())
                        }
                        Err(e) => {
                            tracing::error!("CDC replication failed: {}", e);
                            Err(e)
                        }
                    }
                }
                result = metrics_server => {
                    match result {
                        Ok(server_result) => {
                            tracing::error!("Metrics server stopped unexpectedly: {:?}", server_result);
                            // Continue with CDC cleanup
                            self.client.stop().await?;
                            Ok(())
                        }
                        Err(join_error) => {
                            tracing::error!("Metrics server task failed: {:?}", join_error);
                            // Continue with CDC cleanup
                            self.client.stop().await?;
                            Ok(())
                        }
                    }
                }
                _ = shutdown_handler => {
                    tracing::info!("Shutdown signal received, stopping CDC replication gracefully");
                    self.client.stop().await?;
                    tracing::info!("CDC replication stopped successfully");
                    Ok(())
                }
            }
        } else {
            // No metrics server, just run CDC with shutdown handling
            self.run_without_metrics_server(start_lsn, shutdown_handler).await
        }

        #[cfg(not(feature = "metrics"))]
        {
            tracing::info!("Starting CDC replication (metrics disabled)");
            self.run_without_metrics_server(start_lsn, shutdown_handler).await
        }
    }

    /// Run CDC without metrics server (internal helper method)
    async fn run_without_metrics_server(
        &mut self, 
        start_lsn: Option<Lsn>, 
        shutdown_handler: tokio::task::JoinHandle<()>
    ) -> CdcResult<()> {
        tokio::select! {
            result = self.client.start_replication_from_lsn(start_lsn) => {
                match result {
                    Ok(()) => {
                        tracing::info!("CDC replication completed successfully");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("CDC replication failed: {}", e);
                        Err(e)
                    }
                }
            }
            _ = shutdown_handler => {
                tracing::info!("Shutdown signal received, stopping CDC replication gracefully");
                self.client.stop().await?;
                tracing::info!("CDC replication stopped successfully");
                Ok(())
            }
        }
    }

    /// Get metrics in Prometheus text format
    pub fn get_metrics(&self) -> CdcResult<String> {
        self.client.get_metrics()
    }

    /// Initialize build information in metrics  
    pub fn init_build_info(&self, version: &str) {
        self.client.init_build_info(version);
    }

    /// Get access to the underlying client (for advanced use cases)
    pub fn client(&self) -> &CdcClient {
        &self.client
    }

    /// Get mutable access to the underlying client (for advanced use cases)
    pub fn client_mut(&mut self) -> &mut CdcClient {
        &mut self.client
    }
}

/// High-level convenience function to run CDC application with configuration
///
/// This is a convenience function that combines application creation and execution
/// in a single call. It's the simplest way to run a CDC application.
///
/// # Arguments
///
/// * `config` - The CDC configuration to use
/// * `lsn_file_path` - Optional path to the LSN persistence file
///
/// # Returns
///
/// Returns `CdcResult<()>` when the application completes successfully
/// or is gracefully shut down.
///
/// # Errors
///
/// Returns `CdcError` if application creation or execution fails.
///
/// # Example
///
/// ```rust,no_run
/// use pg2any_lib::{app::run_cdc_app, load_config_from_env};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = load_config_from_env()?;
///     run_cdc_app(config, None).await?;
///     Ok(())
/// }
/// ```
pub async fn run_cdc_app(config: Config, lsn_file_path: Option<&str>) -> CdcResult<()> {
    let app_config = CdcAppConfig {
        cdc_config: config,
        metrics_port: None,
        version: "unknown".to_string(),
    };
    let mut app = CdcApp::new(app_config).await?;
    app.run(lsn_file_path).await
}

/// Load and parse the last LSN from a file
///
/// This function attempts to read a persisted LSN (Log Sequence Number) from a file,
/// which allows the CDC process to resume from where it left off instead of starting
/// from the beginning.
///
/// # Arguments
///
/// * `file_path` - Optional path to the LSN file. If None, uses the default from
///   `CDC_LAST_LSN_FILE` environment variable or "./pg2any_last_lsn"
///
/// # Returns
///
/// Returns `Some(Lsn)` if a valid LSN was found and parsed, `None` otherwise.
/// Logs warnings for parsing errors but doesn't fail the application.
fn load_last_lsn(file_path: Option<&str>) -> Option<Lsn> {
    let last_lsn_file = file_path
        .map(String::from)
        .or_else(|| std::env::var("CDC_LAST_LSN_FILE").ok())
        .unwrap_or_else(|| "./pg2any_last_lsn".to_string());

    match std::fs::read_to_string(&last_lsn_file) {
        Ok(contents) => {
            let s = contents.trim();
            if s.is_empty() {
                tracing::info!("LSN file {} is empty, starting from latest", last_lsn_file);
                None
            } else {
                match crate::pg_replication::parse_lsn(s) {
                    Ok(v) => {
                        tracing::info!("Loaded LSN {} from {}", s, last_lsn_file);
                        Some(crate::types::Lsn(v))
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to parse persisted LSN from {}: {}",
                            last_lsn_file,
                            e
                        );
                        None
                    }
                }
            }
        }
        Err(_) => {
            tracing::info!(
                "No persisted LSN file found at {}, starting from latest",
                last_lsn_file
            );
            None
        }
    }
}

/// Set up graceful shutdown signal handling
///
/// This function creates an async task that listens for shutdown signals (SIGTERM, SIGINT)
/// and triggers the provided cancellation token when a signal is received.
///
/// # Arguments
///
/// * `shutdown_token` - CancellationToken to trigger when shutdown is requested
///
/// # Platform Support
///
/// - Unix: Handles SIGTERM and SIGINT signals
/// - Windows: Handles Ctrl+C signal
async fn setup_shutdown_handler(shutdown_token: CancellationToken) {
    use tokio::signal;

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler");

        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Failed to install SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, initiating graceful shutdown");
                shutdown_token.cancel();
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                shutdown_token.cancel();
            }
        }
    }

    #[cfg(windows)]
    {
        signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
        tracing::info!("Received Ctrl+C, initiating graceful shutdown");
        shutdown_token.cancel();
    }
}
