//! HTTP metrics server for Prometheus-compatible metrics exposition
//!
//! This module provides an HTTP server that exposes metrics in Prometheus text format
//! and health check endpoints. The server is only available when the "metrics"
//! feature flag is enabled.
//!
//! # Available Endpoints
//!
//! - `GET /metrics` - Prometheus-formatted metrics
//! - `GET /health` - Health check endpoint

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{body::Incoming, service::service_fn, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Configuration for the metrics HTTP server
#[derive(Debug, Clone)]
pub struct MetricsServerConfig {
    pub port: u16,
    pub bind_address: [u8; 4], // IPv4 address as array
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            bind_address: [0, 0, 0, 0], // Bind to all interfaces
        }
    }
}

/// HTTP server for metrics endpoint
///
/// This struct provides an HTTP server that serves Prometheus-compatible metrics
/// on the `/metrics` endpoint and a health check on the `/health` endpoint.

#[derive(Debug)]
pub struct MetricsServer {
    config: MetricsServerConfig,
}

impl MetricsServer {
    /// Create a new metrics server with the given configuration
    pub fn new(config: MetricsServerConfig) -> Self {
        Self { config }
    }

    /// Start the HTTP server for metrics endpoint
    ///
    /// This method starts an HTTP server that listens on the configured address and port.
    /// The server runs indefinitely until an error occurs or the task is cancelled.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the server shuts down gracefully, or an error if binding fails
    /// or another server error occurs.
    pub async fn start(&self) -> Result<(), String> {
        let addr = SocketAddr::from((self.config.bind_address, self.config.port));
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|_| "Failed to bind listener")?;

        info!("Metrics server listening on http://{}", addr);

        loop {
            let (stream, _) = listener
                .accept()
                .await
                .map_err(|_| "Failed to accept connection")?;
            let io = TokioIo::new(stream);

            tokio::task::spawn(async move {
                if let Err(err) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service_fn(metrics_handler))
                    .await
                {
                    if !err.is_incomplete_message() {
                        error!("Error serving connection: {:?}", err);
                    }
                }
            });
        }
    }

    /// Get the server configuration
    pub fn config(&self) -> &MetricsServerConfig {
        &self.config
    }
}

/// Handle HTTP requests for metrics and health endpoints
async fn metrics_handler(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/metrics") => match crate::monitoring::metrics::gather_metrics() {
            Ok(metrics) => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(metrics)))
                .unwrap()),
            Err(err) => {
                error!("Failed to collect metrics: {}", err);
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::new(Bytes::from("Failed to collect metrics")))
                    .unwrap())
            }
        },
        (&hyper::Method::GET, "/health") => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"status":"healthy"}"#)))
            .unwrap()),

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

/// Create a metrics server with custom port
///
/// # Arguments
///
/// * `port` - The port number to bind to
///
/// # Returns
///
/// Returns a configured `MetricsServer` instance ready to be started.
pub fn create_metrics_server(port: u16) -> MetricsServer {
    let config = MetricsServerConfig {
        port,
        ..Default::default()
    };
    MetricsServer::new(config)
}

/// Create a metrics server with custom configuration
///
/// # Arguments
///
/// * `config` - The server configuration
///
/// # Returns
///
/// Returns a configured `MetricsServer` instance ready to be started.
pub fn create_metrics_server_with_config(config: MetricsServerConfig) -> MetricsServer {
    MetricsServer::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_server_config_default() {
        let config = MetricsServerConfig::default();
        assert_eq!(config.port, 8080);
        assert_eq!(config.bind_address, [0, 0, 0, 0]);
    }

    #[test]
    fn test_metrics_server_config_custom() {
        let config = MetricsServerConfig {
            port: 9090,
            bind_address: [127, 0, 0, 1],
        };
        assert_eq!(config.port, 9090);
        assert_eq!(config.bind_address, [127, 0, 0, 1]);
    }

    #[test]
    fn test_create_metrics_server() {
        let server = create_metrics_server(9090);
        assert_eq!(server.config().port, 9090);
        assert_eq!(server.config().bind_address, [0, 0, 0, 0]);
    }

    #[test]
    fn test_create_metrics_server_with_config() {
        let config = MetricsServerConfig {
            port: 8081,
            bind_address: [127, 0, 0, 1],
        };
        let server = create_metrics_server_with_config(config.clone());
        assert_eq!(server.config().port, config.port);
        assert_eq!(server.config().bind_address, config.bind_address);
    }
}
