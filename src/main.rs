use pg2any_lib::{CdcClient, Config, DestinationType};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Main entry point for the enhanced CDC application using I/O and SQL threads
/// This function sets up a complete CDC pipeline from PostgreSQL to destination databases
/// using MySQL-style I/O Thread and SQL Thread pattern for better performance
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize comprehensive logging
    init_logging();

    tracing::info!("Starting Enhanced PostgreSQL CDC Application with I/O/SQL Thread Architecture");
    tracing::info!("Loading configuration from environment variables");

    // Load configuration from environment variables
    let config = load_config_from_env()?;

    tracing::info!("Configuration loaded successfully");

    // Create and initialize enhanced CDC client
    tracing::info!("Initializing enhanced CDC client with I/O and SQL threads");
    let mut client = CdcClient::new(config).await?;

    tracing::info!("Performing CDC client initialization");
    client.init().await?;

    tracing::info!("✅ Enhanced CDC client initialized successfully");

    // Set up graceful shutdown handling with the client's cancellation token
    let shutdown_handler = setup_shutdown_handler(client.cancellation_token());

    // Start the CDC replication process with I/O and SQL threads
    tracing::info!("🔄 Starting enhanced CDC replication with I/O and SQL threads");
    tracing::info!("I/O Thread: Reads from PostgreSQL WAL and writes to relay logs");
    tracing::info!("SQL Thread: Reads from relay logs and applies to destination database");

    // Create a separate monitoring task with periodic status reporting
    let monitoring_task = start_monitoring_task();

    // Run CDC replication with graceful shutdown
    tokio::select! {
        result = client.start_replication_from_lsn(None) => {
            match result {
                Ok(()) => {
                    tracing::info!("✅ Enhanced CDC replication completed successfully");
                }
                Err(e) => {
                    tracing::error!("❌ Enhanced CDC replication failed: {}", e);
                    return Err(e.into());
                }
            }
        }
        _ = shutdown_handler => {
            tracing::info!("Shutdown signal received, stopping enhanced CDC replication gracefully");
            client.stop().await?;
            tracing::info!("Enhanced CDC replication stopped successfully");
        }
        // _ = monitoring_task => {
        //     tracing::info!("Monitoring task completed");
        // }
    }

    tracing::info!("👋 Enhanced CDC application stopped");
    Ok(())
}

/// Start a background task to monitor and log CDC statistics
async fn start_monitoring_task() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            tracing::info!("CDC monitoring heartbeat - system is running");

            // Simple monitoring without accessing client state
            // More sophisticated monitoring could be added here
        }
    })
}

/// Initialize comprehensive logging configuration
fn init_logging() {
    // Create a more sophisticated logging setup
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

/// Load configuration from environment variables
fn load_config_from_env() -> Result<Config, Box<dyn std::error::Error>> {
    // Source PostgreSQL configuration
    let source_host = std::env::var("CDC_SOURCE_HOST").unwrap_or_else(|_| "localhost".to_string());
    let source_port = std::env::var("CDC_SOURCE_PORT").unwrap_or_else(|_| "5432".to_string());
    let source_db = std::env::var("CDC_SOURCE_DB").unwrap_or_else(|_| "postgres".to_string());
    let source_user = std::env::var("CDC_SOURCE_USER").unwrap_or_else(|_| "postgres".to_string());
    let source_password =
        std::env::var("CDC_SOURCE_PASSWORD").unwrap_or_else(|_| "postgres".to_string());

    let source_connection_string = format!(
        "postgresql://{}:{}@{}:{}/{}?replication=database",
        source_user, source_password, source_host, source_port, source_db
    );

    tracing::info!(
        "Source PostgreSQL connection string: {}",
        source_connection_string.replace(&source_password, "***")
    );

    // Destination configuration
    let dest_type_str = std::env::var("CDC_DEST_TYPE").unwrap_or_else(|_| "MySQL".to_string());
    let dest_type = match dest_type_str.as_str() {
        "MySQL" | "mysql" => DestinationType::MySQL,
        "SqlServer" | "sqlserver" => DestinationType::SqlServer,
        _ => DestinationType::MySQL,
    };

    let dest_host = std::env::var("CDC_DEST_HOST").unwrap_or_else(|_| "localhost".to_string());
    let dest_port = std::env::var("CDC_DEST_PORT").unwrap_or_else(|_| "3306".to_string());
    let dest_db = std::env::var("CDC_DEST_DB").unwrap_or_else(|_| "cdc_target".to_string());
    let dest_user = std::env::var("CDC_DEST_USER").unwrap_or_else(|_| "cdc_user".to_string());
    let dest_password =
        std::env::var("CDC_DEST_PASSWORD").unwrap_or_else(|_| "cdc_password".to_string());

    let destination_connection_string = match dest_type {
        DestinationType::MySQL => {
            format!(
                "mysql://{}:{}@{}:{}/{}",
                dest_user, dest_password, dest_host, dest_port, dest_db
            )
        }
        DestinationType::SqlServer => {
            format!(
                "mssql://{}:{}@{}:{}/{}",
                dest_user, dest_password, dest_host, dest_port, dest_db
            )
        }
        _ => {
            panic!("Not support type {:?}", dest_type);
        }
    };

    tracing::info!(
        "Destination: {:?} at {}@{}:{}/{}",
        dest_type,
        dest_user,
        dest_host,
        dest_port,
        dest_db
    );

    // CDC-specific configuration
    let replication_slot =
        std::env::var("CDC_REPLICATION_SLOT").unwrap_or_else(|_| "cdc_slot".to_string());
    let publication = std::env::var("CDC_PUBLICATION").unwrap_or_else(|_| "cdc_pub".to_string());
    let auto_create_tables = std::env::var("CDC_AUTO_CREATE_TABLES")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

    let protocol_version = std::env::var("CDC_PROTOCOL_VERSION")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<u32>()
        .unwrap_or(1);

    let binary_format = std::env::var("CDC_BINARY_FORMAT")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    let streaming = std::env::var("CDC_STREAMING")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

    // Timeout configurations
    let connection_timeout_secs = std::env::var("CDC_CONNECTION_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse::<u64>()
        .unwrap_or(30);

    let query_timeout_secs = std::env::var("CDC_QUERY_TIMEOUT")
        .unwrap_or_else(|_| "10".to_string())
        .parse::<u64>()
        .unwrap_or(10);

    let heartbeat_interval_secs = std::env::var("CDC_HEARTBEAT_INTERVAL")
        .unwrap_or_else(|_| "10".to_string())
        .parse::<u64>()
        .unwrap_or(10);

    // Enhanced buffer size for I/O thread performance
    let buffer_size = std::env::var("CDC_BUFFER_SIZE")
        .unwrap_or_else(|_| "10000".to_string())
        .parse::<usize>()
        .unwrap_or(10000);

    // Relay log directory for I/O thread
    let relay_log_directory = std::env::var("PG2ANY_RELAY_LOG_DIR").ok();

    tracing::info!(
        "CDC Config - Slot: {}, Publication: {}, Protocol: {}, Buffer Size: {}",
        replication_slot,
        publication,
        protocol_version,
        buffer_size
    );

    if let Some(ref log_dir) = relay_log_directory {
        tracing::info!("Relay Log Directory: {}", log_dir);
    } else {
        tracing::info!("Relay Log Directory: relay_logs (default)");
    }

    // Build the configuration
    let config = Config::builder()
        .source_connection_string(source_connection_string)
        .destination_type(dest_type)
        .destination_connection_string(destination_connection_string)
        .replication_slot_name(replication_slot)
        .publication_name(publication)
        .protocol_version(protocol_version)
        .binary_format(binary_format)
        .streaming(streaming)
        .auto_create_tables(auto_create_tables)
        .connection_timeout(Duration::from_secs(connection_timeout_secs))
        .query_timeout(Duration::from_secs(query_timeout_secs))
        .heartbeat_interval(Duration::from_secs(heartbeat_interval_secs))
        .buffer_size(buffer_size)
        .relay_log_directory(relay_log_directory)
        .build()?;

    Ok(config)
}

/// Set up graceful shutdown signal handling with CancellationToken
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
