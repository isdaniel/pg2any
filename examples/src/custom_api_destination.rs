//! Example: Custom Destination Handler with API Calls
//!
//! This example demonstrates how to use pg2any with a custom destination handler
//! that sends CDC events to an external REST API instead of a database.
//!
//! Use case: You want to process CDC events and send them to a webhook,
//! external API, message queue, or any custom processing pipeline.
//!
//! To run this example:
//! ```bash
//! # Set up environment variables
//! export CDC_SOURCE_CONNECTION_STRING="postgresql://replicator:password@localhost:5432/source_db?replication=database"
//! export CDC_DEST_TYPE="custom:my-api"
//! export CDC_DEST_URI="https://api.example.com/cdc-events"  # Your API endpoint
//! export CDC_REPLICATION_SLOT="custom_api_slot"
//! export CDC_PUBLICATION="my_publication"
//!
//! # Run the example
//! cargo run --example custom_api_destination
//! ```

use pg2any_lib::{
    destinations::{DestinationFactory, FunctionDestination},
    load_config_from_env, run_cdc_app,
    types::Transaction,
    CdcError,
};
use serde_json::json;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    tracing::info!("Starting CDC with custom API destination");

    // Register a custom destination handler that sends events to an API
    DestinationFactory::register_custom("my-api", || {
        Box::new(FunctionDestination::new(|transaction: Transaction| async move {
            // Process the transaction and send to API
            process_transaction_to_api(transaction).await
        }))
    });

    // Load configuration from environment
    let config = load_config_from_env()?;

    // Verify we're using a custom destination
    tracing::info!("Using destination type: {}", config.destination_type);

    // Run the CDC application
    run_cdc_app(config, None).await?;

    Ok(())
}

/// Process a transaction and send it to an external API
async fn process_transaction_to_api(transaction: Transaction) -> Result<(), CdcError> {
    tracing::info!(
        "Processing transaction {} with {} events",
        transaction.transaction_id,
        transaction.events.len()
    );

    // Build the API payload
    let payload = json!({
        "transaction_id": transaction.transaction_id,
        "commit_lsn": transaction.commit_lsn.map(|lsn| lsn.to_string()),
        "is_streaming": transaction.is_streaming,
        "is_final_batch": transaction.is_final_batch,
        "events": transaction.events.iter().map(|event| {
            json!({
                "event_type": format!("{:?}", event.event_type),
                "lsn": event.lsn.map(|lsn| lsn.to_string()),
                "metadata": event.metadata,
            })
        }).collect::<Vec<_>>(),
    });

    // Send to API (example with reqwest)
    let api_url = std::env::var("CDC_DEST_URI")
        .unwrap_or_else(|_| "https://api.example.com/cdc-events".to_string());

    tracing::debug!("Sending transaction to API: {}", api_url);

    // Use reqwest to send the data
    let client = reqwest::Client::new();
    let response = client
        .post(&api_url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| CdcError::generic(format!("Failed to send to API: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "Unable to read response body".to_string());
        return Err(CdcError::generic(format!(
            "API returned error status {}: {}",
            status, body
        )));
    }

    tracing::info!(
        "Successfully sent transaction {} to API",
        transaction.transaction_id
    );

    Ok(())
}
