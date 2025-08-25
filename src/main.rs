use pg2any_lib::{load_config_from_env, run_cdc_app};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Main entry point for the CDC application
/// This function sets up a complete CDC pipeline from PostgreSQL to MySQL/SqlServer
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize comprehensive logging
    init_logging();

    tracing::info!("Starting PostgreSQL CDC Application");

    // Load configuration from environment variables
    let config = load_config_from_env()?;

    // Run the CDC application with graceful shutdown handling
    run_cdc_app(config, None).await?;

    tracing::info!("CDC application stopped");
    Ok(())
}

/// Initialize comprehensive logging configuration
///
/// Sets up structured logging with filtering, thread IDs, and ANSI colors.
/// The log level can be controlled via the `RUST_LOG` environment variable.
///
/// # Default Log Level
///
/// If `RUST_LOG` is not set, defaults to:
/// - `pg2any=debug` - Debug level for our application
/// - `tokio_postgres=info` - Info level for PostgreSQL client
/// - `sqlx=info` - Info level for SQL execution
pub fn init_logging() {
    // Create a sophisticated logging setup
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("pg2any=debug,tokio_postgres=info,sqlx=info"));

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(true)
        .compact();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    tracing::info!("Logging initialized with level filtering");
}
