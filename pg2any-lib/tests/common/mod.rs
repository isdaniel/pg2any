/// Shared test utilities for pg2any-lib integration tests.
///
/// This module consolidates helper functions used across multiple test files
/// to avoid code duplication and improve maintainability.
use chrono::Utc;
use pg2any_lib::{types::ChangeEvent, Transaction};
use pg_walstream::ColumnValue;

/// Helper function to wrap a single event in a transaction for testing
pub fn wrap_in_transaction(event: ChangeEvent) -> Transaction {
    let mut tx = Transaction::new(1, Utc::now());
    tx.add_event(event);
    tx
}

/// Format a `ColumnValue` as a SQL literal with heuristic type detection.
///
/// This mirrors the production `format_value` logic for SQLite-targeted tests:
/// - Numeric strings are emitted unquoted (e.g., `42`, `3.14`)
/// - Boolean strings are converted to `1`/`0`
/// - Other strings are single-quote escaped
/// - NULL and binary values are handled appropriately
pub fn format_column_value(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Null => "NULL".to_string(),
        ColumnValue::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => {
                if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                    s.to_string()
                } else if s == "true" {
                    "1".to_string()
                } else if s == "false" {
                    "0".to_string()
                } else {
                    format!("'{}'", s.replace('\'', "''"))
                }
            }
            Err(_) => "NULL".to_string(),
        },
        ColumnValue::Binary(b) => {
            format!(
                "X'{}'",
                b.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<String>()
            )
        }
    }
}
