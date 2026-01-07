//! SQL Generator for Change Data Capture
//!
//! This module implements SQL generation for change events from PostgreSQL logical replication.
//! It handles schema mappings and generates SQL statements for INSERT, UPDATE, DELETE, and TRUNCATE operations.
//!
//! ## Protocol-Compliant Architecture
//!
//! In the new architecture, transactions are buffered in-memory (ReplicationState) and applied
//! directly from the commit queue. This SqlGenerator is used solely for SQL generation,
//! not for file-based persistence.

use crate::error::{CdcError, Result};
use crate::types::{ChangeEvent, DestinationType, EventType, ReplicaIdentity};
use std::collections::HashMap;
use tracing::info;

/// SQL Generator for converting PostgreSQL change events to destination SQL statements
pub struct SqlGenerator {
    destination_type: DestinationType,
    schema_mappings: HashMap<String, String>,
}

impl SqlGenerator {
    /// Create a new SQL generator for the specified destination type
    pub async fn new(destination_type: DestinationType) -> Result<Self> {
        info!(
            "SQL generator initialized for destination: {:?}",
            destination_type
        );

        Ok(Self {
            destination_type,
            schema_mappings: HashMap::new(),
        })
    }

    /// Set schema mappings for SQL generation
    pub fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    /// Generate SQL statement for a change event
    /// Returns empty string for metadata events
    pub fn generate_sql_for_event(&self, event: &ChangeEvent) -> Result<String> {
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => self.generate_insert_sql(schema, table, data),
            EventType::Update {
                schema,
                table,
                old_data,
                new_data,
                replica_identity,
                key_columns,
                ..
            } => self.generate_update_sql(
                schema,
                table,
                new_data,
                old_data,
                replica_identity,
                key_columns,
            ),
            EventType::Delete {
                schema,
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => self.generate_delete_sql(schema, table, old_data, replica_identity, key_columns),
            EventType::Truncate(tables) => self.generate_truncate_sql(tables),
            _ => {
                // Skip non-DML events
                Ok(String::new())
            }
        }
    }

    /// Generate INSERT SQL command
    fn generate_insert_sql(
        &self,
        schema: &str,
        table: &str,
        new_data: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let columns: Vec<String> = new_data.keys().cloned().collect();
        let values: Vec<String> = columns
            .iter()
            .map(|col| self.format_value(&new_data[col]))
            .collect();

        let sql = match self.destination_type {
            DestinationType::MySQL => {
                format!(
                    "INSERT INTO `{}`.`{}` ({}) VALUES ({});",
                    schema,
                    table,
                    columns
                        .iter()
                        .map(|c| format!("`{c}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                )
            }
            DestinationType::SqlServer => {
                format!(
                    "INSERT INTO [{}].[{}] ({}) VALUES ({});",
                    schema,
                    table,
                    columns
                        .iter()
                        .map(|c| format!("[{c}]"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                )
            }
            DestinationType::SQLite => {
                format!(
                    "INSERT INTO \"{}\" ({}) VALUES ({});",
                    table,
                    columns
                        .iter()
                        .map(|c| format!("\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", "),
                    values.join(", ")
                )
            }
        };

        Ok(sql)
    }

    /// Generate UPDATE SQL command
    fn generate_update_sql(
        &self,
        schema: &str,
        table: &str,
        new_data: &HashMap<String, serde_json::Value>,
        old_data: &Option<HashMap<String, serde_json::Value>>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let set_clause: Vec<String> = new_data
            .iter()
            .map(|(col, val)| {
                let formatted_col = match self.destination_type {
                    DestinationType::MySQL => format!("`{col}`"),
                    DestinationType::SqlServer => format!("[{col}]"),
                    DestinationType::SQLite => format!("\"{col}\""),
                };
                format!("{} = {}", formatted_col, self.format_value(val))
            })
            .collect();

        // Build WHERE clause based on replica identity
        let where_clause =
            self.build_where_clause(replica_identity, key_columns, old_data, new_data)?;

        let sql = match self.destination_type {
            DestinationType::MySQL => {
                format!(
                    "UPDATE `{}`.`{}` SET {} WHERE {};",
                    schema,
                    table,
                    set_clause.join(", "),
                    where_clause
                )
            }
            DestinationType::SqlServer => {
                format!(
                    "UPDATE [{}].[{}] SET {} WHERE {};",
                    schema,
                    table,
                    set_clause.join(", "),
                    where_clause
                )
            }
            DestinationType::SQLite => {
                format!(
                    "UPDATE \"{}\" SET {} WHERE {};",
                    table,
                    set_clause.join(", "),
                    where_clause
                )
            }
        };

        Ok(sql)
    }

    /// Generate DELETE SQL command
    fn generate_delete_sql(
        &self,
        schema: &str,
        table: &str,
        old_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
    ) -> Result<String> {
        let schema = self.map_schema(Some(schema));

        let where_clause = self.build_where_clause(
            replica_identity,
            key_columns,
            &Some(old_data.clone()),
            old_data,
        )?;

        let sql = match self.destination_type {
            DestinationType::MySQL => {
                format!("DELETE FROM `{schema}`.`{table}` WHERE {where_clause};")
            }
            DestinationType::SqlServer => {
                format!("DELETE FROM [{schema}].[{table}] WHERE {where_clause};")
            }
            DestinationType::SQLite => {
                format!("DELETE FROM \"{table}\" WHERE {where_clause};")
            }
        };

        Ok(sql)
    }

    /// Generate TRUNCATE SQL command
    fn generate_truncate_sql(&self, tables: &[String]) -> Result<String> {
        // For simplicity, generate a TRUNCATE statement for each table
        let mut sqls = Vec::new();

        for table_spec in tables {
            // table_spec might include schema, parse it
            let parts: Vec<&str> = table_spec.split('.').collect();
            let (schema, table) = if parts.len() == 2 {
                (self.map_schema(Some(parts[0])), parts[1])
            } else {
                (self.map_schema(Some("public")), table_spec.as_str())
            };

            let sql = match self.destination_type {
                DestinationType::MySQL => {
                    format!("TRUNCATE TABLE `{schema}`.`{table}`;")
                }
                DestinationType::SqlServer => {
                    format!("TRUNCATE TABLE [{schema}].[{table}];")
                }
                DestinationType::SQLite => {
                    // SQLite doesn't have TRUNCATE, use DELETE
                    format!("DELETE FROM \"{table}\";")
                }
            };
            sqls.push(sql);
        }

        Ok(sqls.join("\n"))
    }

    /// Build WHERE clause for UPDATE/DELETE based on replica identity
    fn build_where_clause(
        &self,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let conditions: Vec<String> = match replica_identity {
            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                // Use key columns
                let data = old_data.as_ref().unwrap_or(new_data);
                key_columns
                    .iter()
                    .map(|col| {
                        let val = data.get(col).ok_or_else(|| {
                            CdcError::Generic(format!("Key column {col} not found"))
                        })?;
                        let formatted_col = match self.destination_type {
                            DestinationType::MySQL => format!("`{col}`"),
                            DestinationType::SqlServer => format!("[{col}]"),
                            DestinationType::SQLite => format!("\"{col}\""),
                        };
                        Ok(format!("{} = {}", formatted_col, self.format_value(val)))
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            ReplicaIdentity::Full => {
                // Use all columns from old data
                let data = old_data.as_ref().ok_or_else(|| {
                    CdcError::Generic("FULL replica identity requires old data".to_string())
                })?;
                data.iter()
                    .map(|(col, val)| {
                        let formatted_col = match self.destination_type {
                            DestinationType::MySQL => format!("`{col}`"),
                            DestinationType::SqlServer => format!("[{col}]"),
                            DestinationType::SQLite => format!("\"{col}\""),
                        };
                        if val.is_null() {
                            format!("{formatted_col} IS NULL")
                        } else {
                            format!("{} = {}", formatted_col, self.format_value(val))
                        }
                    })
                    .collect()
            }
            ReplicaIdentity::Nothing => {
                return Err(CdcError::Generic(
                    "Cannot generate WHERE clause with NOTHING replica identity".to_string(),
                ));
            }
        };

        Ok(conditions.join(" AND "))
    }

    /// Format a JSON value as SQL literal
    fn format_value(&self, value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => {
                if *b {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => {
                // Escape single quotes by doubling them (SQL standard)
                let escaped = s.replace('\'', "''");
                format!("'{escaped}'")
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                // For complex types, serialize as JSON string
                let json_str = value.to_string().replace('\'', "''");
                format!("'{json_str}'")
            }
        }
    }

    /// Map source schema to destination schema
    fn map_schema(&self, source_schema: Option<&str>) -> String {
        if let Some(schema) = source_schema {
            self.schema_mappings
                .get(schema)
                .cloned()
                .unwrap_or_else(|| schema.to_string())
        } else {
            "public".to_string()
        }
    }
}
