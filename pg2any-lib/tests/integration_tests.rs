//! Integration tests for PostgreSQL logical replication with libpq-sys
//!
//! These tests verify the basic functionality of the libpq-sys integration

use pg2any_lib::buffer::{BufferReader, BufferWriter};
use pg2any_lib::pg_replication::{format_lsn, parse_lsn};
use pg2any_lib::replication_messages::{ColumnData, ColumnInfo, TupleData};

#[test]
fn test_buffer_operations() {
    // Test basic buffer operations
    let mut buffer = [0u8; 16];
    let bytes_written;
    
    {
        let mut writer = BufferWriter::new(&mut buffer);
        
        // Write test data
        writer.write_u8(0x42).unwrap();
        writer.write_u16(0x1234).unwrap();
        writer.write_u32(0x12345678).unwrap();
        writer.write_u64(0x123456789ABCDEF0).unwrap();
        
        bytes_written = writer.bytes_written();
    }
    
    // Read it back
    let mut reader = BufferReader::new(&buffer[..bytes_written]);
    
    assert_eq!(reader.read_u8().unwrap(), 0x42);
    assert_eq!(reader.read_u16().unwrap(), 0x1234);
    assert_eq!(reader.read_u32().unwrap(), 0x12345678);
    assert_eq!(reader.read_u64().unwrap(), 0x123456789ABCDEF0);
}

#[test]
fn test_lsn_operations() {
    // Test LSN parsing and formatting
    let lsn_str = "1/23456789";
    let lsn = parse_lsn(lsn_str).unwrap();
    assert_eq!(lsn, 0x100000000 + 0x23456789);
    
    let formatted = format_lsn(lsn);
    assert_eq!(formatted, "1/23456789");
}

#[test]
fn test_replication_data_structures() {
    // Test column data creation
    let null_col = ColumnData::null();
    assert!(null_col.is_null());
    assert_eq!(null_col.as_string(), None);
    
    let text_col = ColumnData::text(b"Hello World".to_vec());
    assert!(!text_col.is_null());
    assert_eq!(text_col.as_string(), Some("Hello World".to_string()));
    
    let unchanged_col = ColumnData::unchanged();
    assert!(unchanged_col.is_unchanged());
    
    // Test column info
    let col_info = ColumnInfo::new(1, "id".to_string(), 23, -1);
    assert!(col_info.is_key_column());
    assert_eq!(col_info.name, "id");
    
    // Test tuple data
    let tuple = TupleData::new(vec![
        ColumnData::text(b"1".to_vec()),
        ColumnData::text(b"John Doe".to_vec()),
        ColumnData::null(),
    ]);
    
    assert_eq!(tuple.column_count(), 3);
    assert_eq!(tuple.get_column(0).unwrap().as_string(), Some("1".to_string()));
    assert!(tuple.get_column(2).unwrap().is_null());
}

#[test]
fn test_message_parsing() {
    use pg2any_lib::logical_parser::LogicalReplicationParser;
    use pg2any_lib::replication_messages::message_types;
    
    let mut parser = LogicalReplicationParser::new();
    
    // Create a simple BEGIN message for testing
    let mut begin_msg = vec![message_types::BEGIN];
    begin_msg.extend_from_slice(&0x123456789ABCDEF0u64.to_be_bytes()); // final_lsn
    begin_msg.extend_from_slice(&0x1234567890ABCDEFi64.to_be_bytes()); // timestamp
    begin_msg.extend_from_slice(&0x12345678u32.to_be_bytes());         // xid
    
    let result = parser.parse_wal_message(&begin_msg);
    assert!(result.is_ok());
    
    let streaming_msg = result.unwrap();
    match streaming_msg.message {
        pg2any_lib::replication_messages::LogicalReplicationMessage::Begin { 
            final_lsn, 
            timestamp, 
            xid 
        } => {
            assert_eq!(final_lsn, 0x123456789ABCDEF0);
            assert_eq!(timestamp, 0x1234567890ABCDEF);
            assert_eq!(xid, 0x12345678);
        }
        _ => panic!("Expected Begin message"),
    }
}

#[test]
fn test_config_integration() {
    use pg2any_lib::{Config, DestinationType};
    use std::time::Duration;
    
    // Test that the config can be created and used with libpq-sys features
    let config = Config::builder()
        .source_connection_string("postgresql://test:test@localhost:5432/test".to_string())
        .destination_type(DestinationType::MySQL)
        .destination_connection_string("mysql://test:test@localhost:3306/test".to_string())
        .replication_slot_name("test_slot".to_string())
        .publication_name("test_pub".to_string())
        .protocol_version(2)
        .streaming(true)
        .connection_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    
    assert_eq!(config.protocol_version, 2);
    assert!(config.streaming);
    assert_eq!(config.replication_slot_name, "test_slot");
}
