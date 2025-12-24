/// Common utilities and traits for destination implementations
use crate::{
    error::{CdcError, Result},
    types::ReplicaIdentity,
};
use std::collections::HashMap;

/// Generic open transaction wrapper that holds database-specific transaction state
/// The generic parameter T allows each destination to use its own connection/transaction type
pub struct OpenTransaction<T> {
    /// PostgreSQL transaction ID and event count tracking
    pub state: TransactionState,
    /// Database-specific transaction handle (connection, transaction object, or flag)
    pub handle: T,
}

impl<T> OpenTransaction<T> {
    /// Create a new open transaction with the given transaction ID and handle
    pub fn new(transaction_id: u32, handle: T) -> Self {
        Self {
            state: TransactionState::new(transaction_id),
            handle,
        }
    }
}

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

/// Transaction state tracking for streaming transactions
pub struct TransactionState {
    /// PostgreSQL transaction ID
    pub transaction_id: u32,
    /// Number of events processed so far
    pub events_processed: usize,
}

impl TransactionState {
    pub fn new(transaction_id: u32) -> Self {
        Self {
            transaction_id,
            events_processed: 0,
        }
    }
}

/// Map a source schema to destination schema using provided mappings
pub fn map_schema(schema_mappings: &HashMap<String, String>, source_schema: &str) -> String {
    schema_mappings
        .get(source_schema)
        .cloned()
        .unwrap_or_else(|| source_schema.to_string())
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
                    "UPDATE with FULL replica identity requires old_data but none provided for table {}.{}",
                    schema, table
                )))
            }
        }

        ReplicaIdentity::Default | ReplicaIdentity::Index => {
            if key_columns.is_empty() {
                return Err(CdcError::generic(format!(
                    "UPDATE requires key columns for table {}.{} with DEFAULT/INDEX replica identity",
                    schema, table
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
                        "Key column '{}' not found in data for UPDATE on table {}.{}",
                        key_column, schema, table
                    )));
                }
            }
            Ok(conditions)
        }

        ReplicaIdentity::Nothing => {
            // For UPDATE with NOTHING, try to use key columns if available
            if key_columns.is_empty() {
                return Err(CdcError::generic(format!(
                    "Cannot UPDATE with NOTHING replica identity and no key columns for table {}.{}",
                    schema, table
                )));
            }

            let mut conditions = Vec::new();
            for key_column in key_columns {
                if let Some(value) = new_data.get(key_column) {
                    conditions.push((key_column.as_str(), value));
                } else {
                    return Err(CdcError::generic(format!(
                        "Key column '{}' not found in new_data for UPDATE with NOTHING replica identity on table {}.{}",
                        key_column, schema, table
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
                    "DELETE requires key columns for table {}.{} with DEFAULT/INDEX replica identity",
                    schema, table
                )));
            }

            let mut conditions = Vec::new();
            for key_column in key_columns {
                if let Some(value) = old_data.get(key_column) {
                    conditions.push((key_column.as_str(), value));
                } else {
                    return Err(CdcError::generic(format!(
                        "Key column '{}' not found in old_data for DELETE on table {}.{}",
                        key_column, schema, table
                    )));
                }
            }
            Ok(conditions)
        }

        ReplicaIdentity::Nothing => Err(CdcError::generic(format!(
            "Cannot DELETE with NOTHING replica identity for table {}.{}",
            schema, table
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
