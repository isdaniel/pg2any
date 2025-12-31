/// Common utilities and traits for destination implementations
///
/// This module provides shared functionality for all destination implementations:
///
/// ## Batch Management
/// - `InsertBatch`, `UpdateBatch`, `DeleteBatch`: Data structures for batching DML operations
/// - Dynamic batch size calculation based on actual data size and database constraints
/// - Database-specific limits handling (MySQL max_allowed_packet, SQLite variable limits)
///
/// ## Batch Size Calculation
/// The refactored batch size system uses actual data sampling instead of hardcoded constants:
/// - `estimate_value_size()`: Estimates bytes for JSON values
/// - `estimate_*_batch_row_size()`: Samples batch data to estimate average row size
/// - `calculate_batch_size()`: Generic calculator respecting packet limits and constraints
/// - `analyze_transaction_batch_requirements()`: Analyzes transaction characteristics
///
/// This approach provides:
/// - **Adaptive sizing**: Batch sizes adjust based on actual data size
/// - **Safety**: Respects database-specific limits (MySQL packet size, SQLite variables)
/// - **Performance**: Optimizes throughput while preventing errors
/// - **Consistency**: All destinations use the same calculation logic
///
/// ## WHERE Clause Building
/// - Replica identity-aware WHERE clause generation
/// - Supports FULL, DEFAULT, INDEX, and NOTHING replica identities
/// - Handles key column resolution and old_data availability
///
/// ## Schema Mapping
/// - Maps source schemas to destination databases/schemas
/// - Essential for cross-database replication
use crate::{
    error::{CdcError, Result},
    types::ReplicaIdentity,
};
use std::collections::HashMap;

/// Helper struct for building WHERE clauses with proper parameter binding
/// Uses references to avoid unnecessary cloning of JSON values
#[derive(Debug)]
pub struct WhereClause<'a> {
    pub sql: String,
    pub bind_values: Vec<&'a serde_json::Value>,
}

/// Represents a group of INSERT events for the same table that can be batched
pub struct InsertBatch<'a> {
    pub schema: Option<String>,
    pub table: String,
    /// Column names in consistent order
    pub columns: Vec<String>,
    /// Values for each row, in the same column order
    pub rows: Vec<Vec<&'a serde_json::Value>>,
}

impl<'a> InsertBatch<'a> {
    /// Create a new batch with schema (for MySQL/SQL Server)
    pub fn new_with_schema(schema: String, table: String, columns: Vec<String>) -> Self {
        Self {
            schema: Some(schema),
            table,
            columns,
            rows: Vec::new(),
        }
    }

    /// Create a new batch without schema (for SQLite)
    pub fn new(table: String, columns: Vec<String>) -> Self {
        Self {
            schema: None,
            table,
            columns,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, data: &'a HashMap<String, serde_json::Value>) {
        let values: Vec<&serde_json::Value> = self
            .columns
            .iter()
            .map(|col| data.get(col).unwrap_or(&serde_json::Value::Null))
            .collect();
        self.rows.push(values);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if a new row can be added to this batch (same table and columns)
    pub fn can_add(&self, schema: Option<&str>, table: &str, columns: &[String]) -> bool {
        self.schema.as_deref() == schema && self.table == table && self.columns == columns
    }
}

/// Represents a group of UPDATE events for the same table that can be batched
/// Uses CASE-style batch updates for efficient processing
pub struct UpdateBatch<'a> {
    pub schema: Option<String>,
    pub table: String,
    /// Columns to be updated (SET columns)
    pub update_columns: Vec<String>,
    /// Key columns used in WHERE clause
    pub key_columns: Vec<String>,
    /// Replica identity mode
    pub replica_identity: ReplicaIdentity,
    /// Each row contains: (key_values, update_values)
    pub rows: Vec<(Vec<&'a serde_json::Value>, Vec<&'a serde_json::Value>)>,
}

impl<'a> UpdateBatch<'a> {
    /// Create a new UPDATE batch with schema
    pub fn new_with_schema(
        schema: String,
        table: String,
        update_columns: Vec<String>,
        key_columns: Vec<String>,
        replica_identity: ReplicaIdentity,
    ) -> Self {
        Self {
            schema: Some(schema),
            table,
            update_columns,
            key_columns,
            replica_identity,
            rows: Vec::new(),
        }
    }

    /// Create a new UPDATE batch without schema (for SQLite)
    pub fn new(
        table: String,
        update_columns: Vec<String>,
        key_columns: Vec<String>,
        replica_identity: ReplicaIdentity,
    ) -> Self {
        Self {
            schema: None,
            table,
            update_columns,
            key_columns,
            replica_identity,
            rows: Vec::new(),
        }
    }

    /// Add a row to the UPDATE batch
    pub fn add_row(
        &mut self,
        new_data: &'a HashMap<String, serde_json::Value>,
        key_values: Vec<&'a serde_json::Value>,
    ) {
        let update_values: Vec<&serde_json::Value> = self
            .update_columns
            .iter()
            .map(|col| new_data.get(col).unwrap_or(&serde_json::Value::Null))
            .collect();
        self.rows.push((key_values, update_values));
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if a new row can be added to this batch
    pub fn can_add(
        &self,
        schema: Option<&str>,
        table: &str,
        update_columns: &[String],
        key_columns: &[String],
        replica_identity: &ReplicaIdentity,
    ) -> bool {
        self.schema.as_deref() == schema
            && self.table == table
            && self.update_columns == update_columns
            && self.key_columns == key_columns
            && &self.replica_identity == replica_identity
    }
}

/// Represents a group of DELETE events for the same table that can be batched
/// Uses WHERE IN clause for efficient batch deletion
pub struct DeleteBatch<'a> {
    pub schema: Option<String>,
    pub table: String,
    /// Key columns used in WHERE clause
    pub key_columns: Vec<String>,
    /// Replica identity mode
    pub replica_identity: ReplicaIdentity,
    /// Key values for each row to delete
    pub rows: Vec<Vec<&'a serde_json::Value>>,
}

impl<'a> DeleteBatch<'a> {
    /// Create a new DELETE batch with schema
    pub fn new_with_schema(
        schema: String,
        table: String,
        key_columns: Vec<String>,
        replica_identity: ReplicaIdentity,
    ) -> Self {
        Self {
            schema: Some(schema),
            table,
            key_columns,
            replica_identity,
            rows: Vec::new(),
        }
    }

    /// Create a new DELETE batch without schema (for SQLite)
    pub fn new(table: String, key_columns: Vec<String>, replica_identity: ReplicaIdentity) -> Self {
        Self {
            schema: None,
            table,
            key_columns,
            replica_identity,
            rows: Vec::new(),
        }
    }

    /// Add a row to the DELETE batch
    pub fn add_row(&mut self, key_values: Vec<&'a serde_json::Value>) {
        self.rows.push(key_values);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if a new row can be added to this batch
    pub fn can_add(
        &self,
        schema: Option<&str>,
        table: &str,
        key_columns: &[String],
        replica_identity: &ReplicaIdentity,
    ) -> bool {
        self.schema.as_deref() == schema
            && self.table == table
            && self.key_columns == key_columns
            && &self.replica_identity == replica_identity
    }
}

/// Map a source schema to destination schema using provided mappings
pub fn map_schema(schema_mappings: &HashMap<String, String>, source_schema: &str) -> String {
    schema_mappings
        .get(source_schema)
        .cloned()
        .unwrap_or_else(|| source_schema.to_string())
}

/// Estimate the size of a JSON value in bytes when serialized to SQL
///
/// This provides a reasonable approximation for calculating batch sizes
/// based on actual data size rather than arbitrary constants.
pub fn estimate_value_size(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,    // "NULL"
        serde_json::Value::Bool(_) => 5, // "true" or "false"
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                20 // Max digits for 64-bit integer
            } else {
                30 // Float with decimals
            }
        }
        serde_json::Value::String(s) => s.len() + 2, // String content + quotes
        serde_json::Value::Array(arr) => {
            arr.iter().map(estimate_value_size).sum::<usize>() + arr.len() * 2
        }
        serde_json::Value::Object(obj) => {
            obj.values().map(estimate_value_size).sum::<usize>() + obj.len() * 4
        }
    }
}

/// Generic batch size calculator
///
/// Calculates the maximum safe batch size based on:
/// - Estimated bytes per row (data values)
/// - Statement-specific overhead (SQL keywords, syntax)
/// - Maximum packet/statement size limit with safety margin
/// - Min/max bounds for different operation types
///
/// # Arguments
/// * `estimated_bytes_per_row` - Estimated size of data per row
/// * `statement_overhead` - Per-statement SQL syntax overhead
/// * `fixed_overhead` - Fixed SQL statement overhead (INSERT INTO, etc.)
/// * `max_packet_size` - Maximum packet size (0 = unlimited)
/// * `safety_margin` - Safety margin percentage (e.g., 0.75 = 75%)
/// * `min_rows` - Minimum batch size to return
/// * `max_rows` - Maximum batch size to return
pub fn calculate_batch_size(
    estimated_bytes_per_row: usize,
    statement_overhead: usize,
    fixed_overhead: usize,
    max_packet_size: u64,
    safety_margin: f64,
    min_rows: usize,
    max_rows: usize,
) -> usize {
    let total_bytes_per_row = estimated_bytes_per_row + statement_overhead;

    if max_packet_size == 0 {
        // No packet size limit
        return max_rows;
    }

    let safe_packet_size = (max_packet_size as f64 * safety_margin) as u64;
    let calculated_max =
        (safe_packet_size as usize).saturating_sub(fixed_overhead) / total_bytes_per_row.max(1);
    calculated_max.clamp(min_rows, max_rows)
}

/// Estimate average row size from InsertBatch data
///
/// Samples the first N rows to estimate average data size per row
pub fn estimate_insert_batch_row_size(batch: &InsertBatch<'_>, sample_size: usize) -> usize {
    if batch.rows.is_empty() {
        return batch.columns.len() * 100; // Fallback estimate
    }

    let sample_count = sample_size.min(batch.rows.len());
    let total_estimated: usize = batch
        .rows
        .iter()
        .take(sample_count)
        .map(|row| {
            row.iter()
                .map(|val| estimate_value_size(val))
                .sum::<usize>()
        })
        .sum();

    (total_estimated / sample_count).max(50)
}

/// Estimate average row size from UpdateBatch data
///
/// Samples the first N rows to estimate average data size per row
pub fn estimate_update_batch_row_size(batch: &UpdateBatch<'_>, sample_size: usize) -> usize {
    if batch.rows.is_empty() {
        return (batch.key_columns.len() + batch.update_columns.len()) * 100; // Fallback
    }

    let sample_count = sample_size.min(batch.rows.len());
    let total_estimated: usize = batch
        .rows
        .iter()
        .take(sample_count)
        .map(|(key_vals, update_vals)| {
            let key_size: usize = key_vals.iter().map(|v| estimate_value_size(v)).sum();
            let update_size: usize = update_vals.iter().map(|v| estimate_value_size(v)).sum();
            key_size + update_size
        })
        .sum();

    (total_estimated / sample_count).max(100)
}

/// Estimate average row size from DeleteBatch data
///
/// Samples the first N rows to estimate average key size per row
pub fn estimate_delete_batch_row_size(batch: &DeleteBatch<'_>, sample_size: usize) -> usize {
    if batch.rows.is_empty() {
        return batch.key_columns.len() * 100; // Fallback
    }

    let sample_count = sample_size.min(batch.rows.len());
    let total_estimated: usize = batch
        .rows
        .iter()
        .take(sample_count)
        .map(|key_vals| {
            key_vals
                .iter()
                .map(|v| estimate_value_size(v))
                .sum::<usize>()
        })
        .sum();

    (total_estimated / sample_count).max(50)
}

/// Analyze a Transaction and estimate batch sizes for its events
///
/// This function provides insights into the transaction's data characteristics
/// to help determine optimal batch sizes for processing.
///
/// Returns a tuple: (insert_count, update_count, delete_count, avg_data_size)
pub fn analyze_transaction_batch_requirements(
    transaction: &crate::types::Transaction,
) -> (usize, usize, usize, usize) {
    use crate::types::EventType;

    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;
    let mut total_data_size = 0;
    let mut data_events = 0;

    for event in &transaction.events {
        match &event.event_type {
            EventType::Insert { data, .. } => {
                insert_count += 1;
                let size: usize = data.values().map(estimate_value_size).sum();
                total_data_size += size;
                data_events += 1;
            }
            EventType::Update {
                new_data, old_data, ..
            } => {
                update_count += 1;
                let new_size: usize = new_data.values().map(estimate_value_size).sum();
                let old_size: usize = old_data
                    .as_ref()
                    .map(|d| d.values().map(estimate_value_size).sum())
                    .unwrap_or(0);
                total_data_size += new_size + old_size;
                data_events += 1;
            }
            EventType::Delete { old_data, .. } => {
                delete_count += 1;
                let size: usize = old_data.values().map(estimate_value_size).sum();
                total_data_size += size;
                data_events += 1;
            }
            _ => {}
        }
    }

    let avg_data_size = if data_events > 0 {
        total_data_size / data_events
    } else {
        0
    };

    (insert_count, update_count, delete_count, avg_data_size)
}

/// Build WHERE clause conditions for UPDATE operations based on replica identity
/// Returns the SQL conditions and the bind values
pub fn build_where_conditions_for_update<'a>(
    old_data: &'a Option<HashMap<String, serde_json::Value>>,
    new_data: &'a HashMap<String, serde_json::Value>,
    replica_identity: &ReplicaIdentity,
    key_columns: &'a [String],
    schema: &str,
    table: &str,
) -> Result<Vec<(&'a str, &'a serde_json::Value)>> {
    match replica_identity {
        ReplicaIdentity::Full => {
            // Always require old_data for FULL replica identity
            if let Some(old) = old_data {
                let mut conditions = Vec::new();
                for (key, value) in old.iter() {
                    conditions.push((key.as_str(), value));
                }
                Ok(conditions)
            } else {
                Err(CdcError::generic(format!(
                    "UPDATE with FULL replica identity requires old_data but none provided for table {schema}.{table}"
                )))
            }
        }

        ReplicaIdentity::Default | ReplicaIdentity::Index => {
            if key_columns.is_empty() {
                return Err(CdcError::generic(format!(
                    "UPDATE requires key columns for table {schema}.{table} with DEFAULT/INDEX replica identity"
                )));
            }

            // Use old_data if available, otherwise fall back to new_data
            let data_source = match old_data {
                Some(old) => old,
                None => new_data,
            };

            let mut conditions = Vec::new();
            for key_column in key_columns {
                if let Some(value) = data_source.get(key_column) {
                    conditions.push((key_column.as_str(), value));
                } else {
                    return Err(CdcError::generic(format!(
                        "Key column '{key_column}' not found in data for UPDATE on table {schema}.{table}"
                    )));
                }
            }
            Ok(conditions)
        }

        ReplicaIdentity::Nothing => {
            // For UPDATE with NOTHING, try to use key columns if available
            if key_columns.is_empty() {
                return Err(CdcError::generic(format!(
                    "Cannot UPDATE with NOTHING replica identity and no key columns for table {schema}.{table}"
                )));
            }

            let mut conditions = Vec::new();
            for key_column in key_columns {
                if let Some(value) = new_data.get(key_column) {
                    conditions.push((key_column.as_str(), value));
                } else {
                    return Err(CdcError::generic(format!(
                        "Key column '{key_column}' not found in new_data for UPDATE with NOTHING replica identity on table {schema}.{table}"
                    )));
                }
            }
            Ok(conditions)
        }
    }
}

/// Build WHERE clause conditions for DELETE operations based on replica identity
/// Returns the SQL conditions and the bind values
pub fn build_where_conditions_for_delete<'a>(
    old_data: &'a HashMap<String, serde_json::Value>,
    replica_identity: &ReplicaIdentity,
    key_columns: &'a [String],
    schema: &str,
    table: &str,
) -> Result<Vec<(&'a str, &'a serde_json::Value)>> {
    match replica_identity {
        ReplicaIdentity::Full => {
            let mut conditions = Vec::new();
            for (key, value) in old_data.iter() {
                conditions.push((key.as_str(), value));
            }
            Ok(conditions)
        }

        ReplicaIdentity::Default | ReplicaIdentity::Index => {
            if key_columns.is_empty() {
                return Err(CdcError::generic(format!(
                    "DELETE requires key columns for table {schema}.{table} with DEFAULT/INDEX replica identity"
                )));
            }

            let mut conditions = Vec::new();
            for key_column in key_columns {
                if let Some(value) = old_data.get(key_column) {
                    conditions.push((key_column.as_str(), value));
                } else {
                    return Err(CdcError::generic(format!(
                        "Key column '{key_column}' not found in old_data for DELETE on table {schema}.{table}"
                    )));
                }
            }
            Ok(conditions)
        }

        ReplicaIdentity::Nothing => Err(CdcError::generic(format!(
            "Cannot DELETE with NOTHING replica identity for table {schema}.{table}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_batch_with_schema() {
        let mut batch = InsertBatch::new_with_schema(
            "test_schema".to_string(),
            "test_table".to_string(),
            vec!["col1".to_string(), "col2".to_string()],
        );

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        let mut data = HashMap::new();
        data.insert("col1".to_string(), serde_json::json!("value1"));
        data.insert("col2".to_string(), serde_json::json!(42));

        batch.add_row(&data);
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        assert!(batch.can_add(
            Some("test_schema"),
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(
            Some("other_schema"),
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
    }

    #[test]
    fn test_insert_batch_without_schema() {
        let batch = InsertBatch::new(
            "test_table".to_string(),
            vec!["col1".to_string(), "col2".to_string()],
        );

        assert!(batch.schema.is_none());
        assert!(batch.can_add(
            None,
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(
            Some("schema"),
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
    }

    #[test]
    fn test_map_schema() {
        let mut mappings = HashMap::new();
        mappings.insert("public".to_string(), "cdc_db".to_string());

        assert_eq!(map_schema(&mappings, "public"), "cdc_db");
        assert_eq!(map_schema(&mappings, "other"), "other");
    }

    #[test]
    fn test_where_conditions_for_update_full_identity() {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        old_data.insert("name".to_string(), serde_json::json!("test"));

        let new_data = HashMap::new();
        let old_data_opt = Some(old_data);

        let result = build_where_conditions_for_update(
            &old_data_opt,
            &new_data,
            &ReplicaIdentity::Full,
            &[],
            "public",
            "test_table",
        );

        assert!(result.is_ok());
        let conditions = result.unwrap();
        assert_eq!(conditions.len(), 2);
    }

    #[test]
    fn test_where_conditions_for_delete_nothing_identity() {
        let old_data = HashMap::new();

        let result = build_where_conditions_for_delete(
            &old_data,
            &ReplicaIdentity::Nothing,
            &[],
            "public",
            "test_table",
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot DELETE with NOTHING replica identity"));
    }
}
