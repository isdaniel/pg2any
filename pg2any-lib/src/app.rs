//! CDC Application Runner
//!
//! This module provides a high-level application runner that encapsulates
//! the complete CDC workflow including client initialization, shutdown handling,
//! and replication management.

use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "metrics")]
use crate::MetricsServer;
use crate::{
    client::CdcClient, config::Config, lsn_tracker::create_lsn_tracker_with_load, types::Lsn,
    CdcResult,
};

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
        info!("Initializing CDC client");
        let client = CdcClient::new(config.cdc_config.clone()).await?;

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
        self.client.init_build_info(&self.config.version);

        // Create LSN tracker and load last known LSN
        let (lsn_tracker, start_lsn) = create_lsn_tracker_with_load(lsn_file_path).await;

        // Set the LSN tracker on the client for tracking committed LSN
        self.client.set_lsn_tracker(lsn_tracker);

        info!("Starting CDC replication stream");

        #[cfg(feature = "metrics")]
        {
            if let Some(port) = self.config.metrics_port {
                info!("Starting metrics server on port {}", port);
                let server = crate::create_metrics_server(port);
                return self.run_with_optional_server(start_lsn, Some(server)).await;
            }
        }

        self.run_with_optional_server(start_lsn, None).await
    }

    /// Internal method to run the CDC application with optional metrics server
    /// This method abstracts away the conditional compilation details and provides a unified interface for running the application.
    /// Metrics will not guarantee always upload successfully, some telemetry losing is acceptable, for better performance.
    /// # Arguments
    /// * `start_lsn` - Optional starting LSN to resume replication from
    /// * `server` - Optional metrics server instance (if metrics feature is enabled)
    /// # Returns
    /// Returns `CdcResult<()>` when the application completes successfully or is gracefully shut down.
    async fn run_with_optional_server(
        &mut self,
        start_lsn: Option<Lsn>,
        #[cfg(feature = "metrics")] server: Option<MetricsServer>,
        #[cfg(not(feature = "metrics"))] _: Option<()>,
    ) -> CdcResult<()> {
        #[cfg(feature = "metrics")]
        if let Some(server) = server {
            let _ = tokio::spawn(async move { server.start().await });
        } else {
            tracing::warn!(
                "Metrics server not started (metrics feature enabled but no port configured)"
            );
        }

        let shutdown_handler = setup_shutdown_handler(self.client.cancellation_token());

        tokio::select! {
            result = self.client.start_replication_from_lsn(start_lsn) => {
                match result {
                    Ok(()) => {
                        info!("CDC replication completed successfully");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("CDC replication failed: {}", e);
                        Err(e)
                    }
                }
            }
            _ = shutdown_handler => {
                info!("Shutdown signal received, stopping CDC replication gracefully");
                self.client.stop().await?;
                info!("CDC replication stopped successfully");
                Ok(())
            }
        }
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
                info!("Received SIGTERM, initiating graceful shutdown");
                shutdown_token.cancel();
            }
            _ = sigint.recv() => {
                info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                shutdown_token.cancel();
            }
        }
    }

    #[cfg(windows)]
    {
        signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
        info!("Received Ctrl+C, initiating graceful shutdown");
        shutdown_token.cancel();
    }
}
