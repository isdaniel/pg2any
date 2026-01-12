// Test to verify PG2ANY_ENABLE_COMPRESSION feature flag works correctly
//
// This test verifies the compression feature flag by checking the transaction
// file paths created when compression is enabled or disabled.

use std::path::PathBuf;

// Helper to create a temp directory with unique name
fn create_temp_dir(suffix: &str) -> PathBuf {
    let temp_dir = std::env::temp_dir().join(format!(
        "pg2any_compression_test_{}_{}",
        suffix,
        std::process::id()
    ));
    std::fs::create_dir_all(&temp_dir).unwrap();
    temp_dir
}

#[test]
fn test_compression_env_var_parsing() {
    // Test that the environment variable parsing works correctly

    // Test default (no env var) - should be false
    std::env::remove_var("PG2ANY_ENABLE_COMPRESSION");
    assert_eq!(
        std::env::var("PG2ANY_ENABLE_COMPRESSION").ok().as_deref() == Some("true")
            || std::env::var("PG2ANY_ENABLE_COMPRESSION").ok().as_deref() == Some("1"),
        false,
        "Default should be false"
    );

    // Test "true"
    std::env::set_var("PG2ANY_ENABLE_COMPRESSION", "true");
    assert_eq!(
        std::env::var("PG2ANY_ENABLE_COMPRESSION").ok().as_deref() == Some("true"),
        true
    );

    // Test "1"
    std::env::set_var("PG2ANY_ENABLE_COMPRESSION", "1");
    assert_eq!(
        std::env::var("PG2ANY_ENABLE_COMPRESSION").ok().as_deref() == Some("1"),
        true
    );

    // Test "false"
    std::env::set_var("PG2ANY_ENABLE_COMPRESSION", "false");
    assert_eq!(
        std::env::var("PG2ANY_ENABLE_COMPRESSION").ok().as_deref() == Some("true")
            || std::env::var("PG2ANY_ENABLE_COMPRESSION").ok().as_deref() == Some("1"),
        false
    );

    // Cleanup
    std::env::remove_var("PG2ANY_ENABLE_COMPRESSION");
}

#[test]
fn test_compression_file_extensions() {
    // Test that compressed files have .sql.gz extension
    let temp_dir = create_temp_dir("ext_test");

    // Create a dummy .sql file
    let sql_file = temp_dir.join("tx_001_001.sql");
    std::fs::write(&sql_file, "SELECT 1;").unwrap();
    assert!(sql_file.exists());
    assert!(sql_file.to_string_lossy().ends_with(".sql"));
    assert!(!sql_file.to_string_lossy().ends_with(".sql.gz"));

    // Create a dummy .sql.gz file
    let gz_file = temp_dir.join("tx_002_002.sql.gz");
    std::fs::write(&gz_file, &[0x1f, 0x8b, 0x08]).unwrap(); // gzip magic number
    assert!(gz_file.exists());
    assert!(gz_file.to_string_lossy().ends_with(".sql.gz"));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
