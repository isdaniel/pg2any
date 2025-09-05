use pg2any_lib::{load_config_from_env, CdcApp, CdcAppConfig};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Main entry point for the CDC application
/// This function sets up a complete CDC pipeline from PostgreSQL to MySQL/SqlServer
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize comprehensive logging
    init_logging();

    tracing::info!("Starting PostgreSQL CDC Application");

    // Load configuration from environment variables
    let cdc_config = load_config_from_env()?;

    // Initialize global metrics registry
    pg2any_lib::init_metrics()?;

    // Get metrics port from environment if available
    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok());

    // Get application version
    let version = env!("CARGO_PKG_VERSION");

    // Create CDC application configuration
    let mut app_config = CdcAppConfig::new(cdc_config);
    app_config.with_version(version);

    if let Some(port) = metrics_port {
        app_config.with_metrics_port(port);
    }

    // Create and run CDC application - all feature handling is encapsulated in the library
    let mut cdc_app = CdcApp::new(app_config).await?;
    let result = cdc_app.run(None).await;

    tracing::info!("CDC application stopped");
    result.map_err(|e| e.into())
}

/// Initialize logging with structured output and proper filtering
fn init_logging() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_level(true)
                .with_file(true)
                .with_line_number(true)
                .compact(),
        )
        .with(env_filter)
        .init();
}
