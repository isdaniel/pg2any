//! Comprehensive tests for the I/O Thread and SQL Thread architecture
//!
//! This module contains integration tests that verify the complete CDC pipeline
//! using the enhanced I/O and SQL thread architecture.

#[cfg(test)]
mod tests {
    use pg2any_lib::{CdcClient, Config, DestinationType};
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_io_sql_thread_integration() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .protocol_version(1)
            .streaming(false)
            .auto_create_tables(true)
            .connection_timeout(Duration::from_secs(5))
            .query_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(1))
            .buffer_size(1000)
            .build()
            .unwrap();

        let mut client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Test initialization
        client.init().await.unwrap();

        // Verify threads are created but not running yet
        assert!(!client.is_running());

        // Get initial stats
        let initial_stats = client.get_stats().await;
        assert!(initial_stats.io_stats.is_some());
        assert!(initial_stats.sql_stats.is_some());
        assert_eq!(initial_stats.relay_log_directory, temp_dir.path());

        // Clean shutdown
        client.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_relay_log_directory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let relay_log_dir = temp_dir.path().join("custom_relay_logs");

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        // Directory should not exist initially
        assert!(!relay_log_dir.exists());

        let client = CdcClient::new_with_relay_log_dir(config, relay_log_dir.clone())
            .await
            .unwrap();

        // Directory should be created
        assert!(relay_log_dir.exists());

        let stats = client.get_stats().await;
        assert_eq!(stats.relay_log_directory, relay_log_dir);
    }

    #[tokio::test]
    async fn test_config_conversion_to_thread_configs() {
        use tempfile::TempDir;

        // Create temporary directory for relay logs
        let temp_dir = TempDir::new().unwrap();
        let relay_log_path = temp_dir.path().to_string_lossy().to_string();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://user:pass@host:5432/db?replication=database".to_string(),
            )
            .destination_type(DestinationType::SqlServer)
            .destination_connection_string("mssql://user:pass@host:1433/db".to_string())
            .replication_slot_name("my_slot".to_string())
            .publication_name("my_pub".to_string())
            .protocol_version(2)
            .streaming(true)
            .auto_create_tables(false)
            .connection_timeout(Duration::from_secs(15))
            .query_timeout(Duration::from_secs(8))
            .heartbeat_interval(Duration::from_secs(20))
            .buffer_size(5000)
            .relay_log_directory(Some(relay_log_path))
            .build()
            .unwrap();

        // Test IoThreadConfig conversion
        let io_config = pg2any_lib::IoThreadConfig::from(&config);
        assert_eq!(io_config.connection_string, config.source_connection_string);
        assert_eq!(io_config.replication_config.slot_name, "my_slot");
        assert_eq!(io_config.replication_config.publication_name, "my_pub");
        assert_eq!(io_config.replication_config.protocol_version, 2);
        assert!(io_config.replication_config.streaming_enabled);
        assert_eq!(io_config.heartbeat_interval, Duration::from_secs(20));
        assert_eq!(io_config.buffer_size, 5000);

        // Test SqlThreadConfig conversion
        let sql_config = pg2any_lib::SqlThreadConfig::from(&config);
        assert_eq!(sql_config.destination_type, DestinationType::SqlServer);
        assert_eq!(
            sql_config.destination_connection_string,
            config.destination_connection_string
        );
        assert!(!sql_config.auto_create_tables);
        assert_eq!(sql_config.heartbeat_interval, Duration::from_secs(20));
        assert_eq!(sql_config.batch_size, 100); // Default batch size
    }

    #[tokio::test]
    async fn test_cdc_client_lifecycle() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        let mut client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Test lifecycle: created -> initialized -> stopped
        assert!(!client.is_running());

        client.init().await.unwrap();
        assert!(!client.is_running()); // Still not running until start() is called

        client.stop().await.unwrap();
        assert!(!client.is_running());
    }

    #[tokio::test]
    async fn test_cancellation_token_propagation() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        let client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Test cancellation token access
        let token = client.cancellation_token();
        assert!(!token.is_cancelled());

        // Dropping client should cancel the token
        drop(client);

        // Give a moment for the cancellation to propagate
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_stats_initialization() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        let mut client = CdcClient::new_with_relay_log_dir(config, temp_dir.path().to_path_buf())
            .await
            .unwrap();
        client.init().await.unwrap();

        let stats = client.get_stats().await;

        // Check I/O thread stats
        let io_stats = stats.io_stats.unwrap();
        assert_eq!(io_stats.events_read, 0);
        assert_eq!(io_stats.events_written, 0);
        assert_eq!(io_stats.bytes_written, 0);
        assert!(!io_stats.is_connected);

        // Check SQL thread stats
        let sql_stats = stats.sql_stats.unwrap();
        assert_eq!(sql_stats.events_processed, 0);
        assert_eq!(sql_stats.events_applied, 0);
        assert_eq!(sql_stats.events_failed, 0);
        assert!(!sql_stats.is_connected);
        assert_eq!(sql_stats.processing_lag_ms, 0);
        assert_eq!(sql_stats.average_batch_size, 0.0);
        assert_eq!(sql_stats.total_batches, 0);
    }

    #[tokio::test]
    async fn test_relay_log_configuration() {
        use pg2any_lib::RelayLogConfig;

        let temp_dir = TempDir::new().unwrap();

        let config = RelayLogConfig {
            log_directory: temp_dir.path().to_path_buf(),
            max_file_size: 1024 * 1024, // 1MB
            max_files: 50,
            write_buffer_size: 32 * 1024, // 32KB
            read_buffer_size: 32 * 1024,  // 32KB
        };

        let manager = pg2any_lib::RelayLogManager::new(config.clone())
            .await
            .unwrap();
        let stats = manager.get_stats();

        assert_eq!(stats.log_directory, temp_dir.path());
        assert_eq!(stats.current_file_index, 1); // Single file approach uses index 1
        assert_eq!(stats.current_sequence, 0);
    }

    #[tokio::test]
    async fn test_error_conditions() {
        // Test with invalid relay log directory (read-only path)
        let invalid_dir = std::path::Path::new("/proc/invalid_relay_logs");

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        let result = CdcClient::new_with_relay_log_dir(config, invalid_dir.to_path_buf()).await;
        assert!(result.is_err(), "Should fail with invalid directory");
    }

    #[tokio::test]
    async fn test_multiple_client_instances() {
        let temp_dir1 = TempDir::new().unwrap();
        let temp_dir2 = TempDir::new().unwrap();

        let config = Config::builder()
            .source_connection_string(
                "postgresql://test:test@localhost:5432/test?replication=database".to_string(),
            )
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .build()
            .unwrap();

        // Create multiple client instances with different relay log directories
        let mut client1 =
            CdcClient::new_with_relay_log_dir(config.clone(), temp_dir1.path().to_path_buf())
                .await
                .unwrap();
        let mut client2 = CdcClient::new_with_relay_log_dir(config, temp_dir2.path().to_path_buf())
            .await
            .unwrap();

        client1.init().await.unwrap();
        client2.init().await.unwrap();

        let stats1 = client1.get_stats().await;
        let stats2 = client2.get_stats().await;

        // Verify they use different relay log directories
        assert_ne!(stats1.relay_log_directory, stats2.relay_log_directory);
        assert_eq!(stats1.relay_log_directory, temp_dir1.path());
        assert_eq!(stats2.relay_log_directory, temp_dir2.path());

        // Clean shutdown
        client1.stop().await.unwrap();
        client2.stop().await.unwrap();
    }
}

/// Performance and stress tests (marked with ignore to run separately)
#[cfg(test)]
mod performance_tests {
    use pg2any_lib::{types::*, RelayLogConfig, RelayLogManager};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_relay_log_high_throughput() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig {
            log_directory: temp_dir.path().to_path_buf(),
            max_file_size: 10 * 1024 * 1024, // 10MB for more realistic file sizes
            max_files: 10,
            write_buffer_size: 256 * 1024, // 256KB for better performance
            read_buffer_size: 256 * 1024,
        };

        let manager = RelayLogManager::new(config).await.unwrap();
        let writer = manager.create_writer().await.unwrap();
        let reader = manager
            .create_reader("relay-000001.log".to_string(), 0)
            .await
            .unwrap();

        let num_events = 10000;
        let barrier = Arc::new(Barrier::new(2));

        // Writer task
        let writer_barrier = barrier.clone();
        let writer_task = tokio::spawn(async move {
            let start = std::time::Instant::now();

            for i in 0..num_events {
                let mut data = HashMap::new();
                data.insert(
                    "id".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(i)),
                );
                data.insert(
                    "data".to_string(),
                    serde_json::Value::String(format!("test_data_{}", i)),
                );

                let event = ChangeEvent {
                    event_type: EventType::Insert {
                        schema: "test".to_string(),
                        table: "performance_test".to_string(),
                        relation_oid: 12345,
                        data,
                    },
                    lsn: Some(format!("0/{:08X}", i)),
                    metadata: None,
                };

                writer.write_event(event, None).await.unwrap();

                if i % 1000 == 0 {
                    writer.flush().await.unwrap();
                }
            }

            writer.flush().await.unwrap();

            let duration = start.elapsed();
            println!(
                "Wrote {} events in {:?} ({:.2} events/sec)",
                num_events,
                duration,
                num_events as f64 / duration.as_secs_f64()
            );

            writer_barrier.wait().await;
        });

        // Reader task
        let reader_barrier = barrier.clone();
        let reader_task = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let mut events_read = 0;

            reader_barrier.wait().await; // Wait for writer to finish

            while events_read < num_events {
                if let Ok(Some(_entry)) = reader.read_event().await {
                    events_read += 1;
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }

            let duration = start.elapsed();
            println!(
                "Read {} events in {:?} ({:.2} events/sec)",
                events_read,
                duration,
                events_read as f64 / duration.as_secs_f64()
            );
        });

        // Wait for both tasks to complete
        writer_task.await.unwrap();
        reader_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_writers_readers() {
        let temp_dir = TempDir::new().unwrap();
        let config = RelayLogConfig {
            log_directory: temp_dir.path().to_path_buf(),
            max_file_size: 5 * 1024 * 1024, // 5MB
            max_files: 20,
            write_buffer_size: 128 * 1024, // 128KB
            read_buffer_size: 128 * 1024,
        };

        let manager = Arc::new(RelayLogManager::new(config).await.unwrap());
        let num_writers = 3;
        let num_readers = 2;
        let events_per_writer = 1000;

        let mut tasks = Vec::new();

        // Spawn writer tasks
        for writer_id in 0..num_writers {
            let manager = manager.clone();
            let task = tokio::spawn(async move {
                let writer = manager.create_writer().await.unwrap();

                for i in 0..events_per_writer {
                    let mut data = HashMap::new();
                    data.insert(
                        "writer_id".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(writer_id)),
                    );
                    data.insert(
                        "event_id".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(i)),
                    );

                    let event = ChangeEvent {
                        event_type: EventType::Insert {
                            schema: "test".to_string(),
                            table: "concurrent_test".to_string(),
                            relation_oid: 12345,
                            data,
                        },
                        lsn: Some(format!("writer_{}/event_{}", writer_id, i)),
                        metadata: None,
                    };

                    writer.write_event(event, None).await.unwrap();
                }

                writer.flush().await.unwrap();
                println!("Writer {} completed", writer_id);
            });
            tasks.push(task);
        }

        // Spawn reader tasks
        for reader_id in 0..num_readers {
            let manager = manager.clone();
            let task = tokio::spawn(async move {
                let reader = manager
                    .create_reader("relay-000001.log".to_string(), 0)
                    .await
                    .unwrap();
                let mut events_read = 0;

                // Read for a limited time
                let timeout_duration = Duration::from_secs(10);
                let deadline = tokio::time::Instant::now() + timeout_duration;

                while tokio::time::Instant::now() < deadline {
                    if let Ok(Some(_entry)) = reader.read_event().await {
                        events_read += 1;
                    } else {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }

                println!("Reader {} read {} events", reader_id, events_read);
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for task in tasks {
            task.await.unwrap();
        }

        println!("Concurrent test completed successfully");
    }
}
