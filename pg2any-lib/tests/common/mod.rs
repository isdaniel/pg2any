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

/// Format a `ColumnValue` as a SQL literal for SQLite-targeted tests.
///
/// This mirrors the production `format_value` logic:
/// - PostgreSQL pgoutput booleans (`"t"` / `"f"`) are converted to `1`/`0`
/// - All other text values are always single-quote escaped (no numeric heuristics)
/// - NULL and binary values are handled appropriately
pub fn format_column_value(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Null => "NULL".to_string(),
        ColumnValue::Text(_) => match value.as_str() {
            Some("t") => "1".to_string(),
            Some("f") => "0".to_string(),
            Some(s) => {
                format!("'{}'", s.replace('\'', "''"))
            }
            None => "NULL".to_string(),
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
