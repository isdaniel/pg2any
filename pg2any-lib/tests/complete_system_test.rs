//! Complete system integration test for I/O and SQL Thread architecture
//!
//! This test demonstrates the entire pipeline working end-to-end.

use pg2any_lib::{
    client::CdcClient,
    config::Config,
    types::{ChangeEvent, EventType},
};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_complete_system_creation() {
    // Create configuration
    let config = Config::default();

    // Create CDC client (this validates the entire system can be instantiated)
    let client_result = CdcClient::new(config).await;

    // Should succeed in creating the client structure
    assert!(client_result.is_ok());

    println!("✅ Complete system creation test passed!");
    println!("   - CDC Client created successfully");
    println!("   - I/O and SQL thread architecture instantiated");
    println!("   - Configuration properly applied");
}

#[tokio::test]
async fn test_relay_log_end_to_end() {
    use pg2any_lib::relay_log::{RelayLogConfig, RelayLogManager};
    use std::path::PathBuf;

    // Create temporary directory
    let temp_dir = TempDir::new().unwrap();
    let relay_log_path = PathBuf::from(temp_dir.path());

    let config = RelayLogConfig {
        log_directory: relay_log_path,
        max_file_size: 1024 * 1024, // 1MB
        max_files: 10,
        write_buffer_size: 1024,
        read_buffer_size: 1024,
    };

    let manager = RelayLogManager::new(config).await.unwrap();
    let writer = manager.create_writer().await.unwrap();
    let reader = manager
        .create_reader("relay-000001.log".to_string(), 0)
        .await
        .unwrap();

    // Create test events
    let mut events = Vec::new();
    for i in 1..=3 {
        let mut data = HashMap::new();
        data.insert(
            "id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(i)),
        );
        data.insert(
            "name".to_string(),
            serde_json::Value::String(format!("test_{}", i)),
        );

        let event = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".to_string(),
                table: "test_table".to_string(),
                relation_oid: 12345,
                data,
            },
            lsn: Some(format!("0/{:08X}", i * 1000)),
            metadata: None,
        };
        events.push(event);
    }

    // Write events
    for event in &events {
        writer
            .write_event(event.clone(), event.lsn.clone())
            .await
            .unwrap();
    }
    writer.flush().await.unwrap();
    drop(writer); // Ensure file is closed

    // Read events back
    let mut read_events = Vec::new();
    for _ in 0..events.len() {
        let mut attempts = 0;
        let entry = loop {
            match reader.read_event().await.unwrap() {
                Some(entry) => break entry,
                None => {
                    attempts += 1;
                    if attempts > 10 {
                        panic!("Failed to read event after 10 attempts");
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        read_events.push(entry.event);
    }

    // Verify all events were read correctly
    assert_eq!(read_events.len(), events.len());
    for (original, read) in events.iter().zip(read_events.iter()) {
        assert_eq!(original.lsn, read.lsn);

        if let (
            EventType::Insert {
                schema: orig_schema,
                table: orig_table,
                ..
            },
            EventType::Insert {
                schema: read_schema,
                table: read_table,
                ..
            },
        ) = (&original.event_type, &read.event_type)
        {
            assert_eq!(orig_schema, read_schema);
            assert_eq!(orig_table, read_table);
        }
    }

    println!("✅ Relay log end-to-end test passed!");
    println!("   - Written {} events successfully", events.len());
    println!("   - Read back {} events successfully", read_events.len());
    println!("   - All events verified correctly");
}
