use super::coalescing::{coalesce_commands, QuoteStyle};
use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use std::borrow::Cow;
use std::collections::HashMap;
use tiberius::{Client, ColumnData, Config, TokenRow};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

/// SQL Server destination implementation
pub struct SqlServerDestination {
    client: Option<Client<Compat<TcpStream>>>,
    /// Schema mappings: maps source schema to destination schema
    schema_mappings: HashMap<String, String>,
    /// Maximum rows per INSERT VALUES statement (SQL Server hard limit: 1000)
    max_rows_per_insert: usize,
}

impl SqlServerDestination {
    /// Create a new SQL Server destination instance
    pub fn new() -> Self {
        Self {
            client: None,
            schema_mappings: HashMap::new(),
            max_rows_per_insert: 1000,
        }
    }
}

#[async_trait]
impl DestinationHandler for SqlServerDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let config = Config::from_ado_string(connection_string)
            .map_err(|e| CdcError::generic(format!("Invalid SQL Server connection string: {e}")))?;

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| CdcError::generic(format!("Failed to connect to SQL Server: {e}")))?;
        tcp.set_nodelay(true)
            .map_err(|e| CdcError::generic(format!("Failed to set TCP_NODELAY: {e}")))?;

        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to establish SQL Server connection: {e}"))
            })?;

        self.client = Some(client);
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
        if !self.schema_mappings.is_empty() {
            debug!(
                "SQL Server destination schema mappings set: {:?}",
                self.schema_mappings
            );
        }
    }

    fn set_max_rows_per_insert(&mut self, max_rows: usize) {
        if max_rows > 0 {
            self.max_rows_per_insert = max_rows;
        }
    }

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server client not initialized"))?;

        // Coalesce consecutive DML statements before executing:
        // - INSERT → multi-value INSERT
        // - UPDATE → CASE-WHEN batch UPDATE
        // - DELETE → OR-combined WHERE clause
        let coalesced = coalesce_commands(
            commands,
            u64::MAX,
            QuoteStyle::Bracket,
            self.max_rows_per_insert,
        );

        if coalesced.len() < commands.len() {
            debug!(
                "Coalesced {} commands into {} statements (reduction: {:.1}%)",
                commands.len(),
                coalesced.len(),
                (1.0 - coalesced.len() as f64 / commands.len() as f64) * 100.0
            );
        }

        // Begin a transaction
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("SQL Server BEGIN TRANSACTION failed: {e}")))?;

        // Execute all coalesced commands in the transaction
        let mut execution_result = Ok(());
        for (idx, sql) in coalesced.iter().enumerate() {
            if let Err(e) = client.simple_query(sql.as_ref()).await {
                execution_result = Err(CdcError::generic(format!(
                    "SQL Server execute_sql_batch failed at command {}/{}: {}",
                    idx + 1,
                    coalesced.len(),
                    e
                )));
                break;
            }
        }

        // If any command failed, rollback
        if execution_result.is_err() {
            if let Err(rollback_err) = client.simple_query("ROLLBACK TRANSACTION").await {
                tracing::error!(
                    "SQL Server ROLLBACK failed after execution error: {}",
                    rollback_err
                );
            }
            return execution_result;
        }

        // Execute pre-commit hook BEFORE transaction COMMIT
        if let Some(hook) = pre_commit_hook {
            if let Err(e) = hook().await {
                // Rollback transaction if hook fails
                if let Err(rollback_err) = client.simple_query("ROLLBACK TRANSACTION").await {
                    tracing::error!(
                        "SQL Server ROLLBACK failed after pre-commit hook error: {}",
                        rollback_err
                    );
                }
                return Err(CdcError::generic(format!(
                    "SQL Server pre-commit hook failed, transaction rolled back: {}",
                    e
                )));
            }
        }

        // Commit the transaction
        client
            .simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("SQL Server COMMIT TRANSACTION failed: {e}")))?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            let _ = client.close().await;
        }
        self.client = None;
        info!("SQL Server connection closed successfully");
        Ok(())
    }

    fn supports_bulk_insert(&self) -> bool {
        true
    }

    async fn execute_bulk_insert_with_hook(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<String>],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let row_count = rows.len();
        let bulk_table = table.trim();

        debug!(
            "Attempting TDS Bulk Load: {} rows into {}",
            row_count, bulk_table
        );

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server client not initialized"))?;

        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "SQL Server BEGIN TRANSACTION failed for bulk load: {e}"
                ))
            })?;

        match client.bulk_insert(bulk_table).await {
            Ok(mut req) => {
                for row_values in rows {
                    let mut token_row = TokenRow::new();
                    for value in row_values {
                        token_row.push(parse_sql_value(value));
                    }
                    if let Err(e) = req.send(token_row).await {
                        warn!(
                            "TDS Bulk Load send failed, falling back to multi-value INSERT: {}",
                            e
                        );
                        let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                        return self
                            .fallback_multi_value_insert(table, columns, rows, pre_commit_hook)
                            .await;
                    }
                }

                match req.finalize().await {
                    Ok(result) => {
                        info!(
                            "TDS Bulk Load complete: {} rows loaded into {}",
                            result.total(),
                            bulk_table
                        );

                        if let Some(hook) = pre_commit_hook {
                            if let Err(e) = hook().await {
                                let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                                return Err(CdcError::generic(format!(
                                    "SQL Server bulk insert pre-commit hook failed, rolled back: {}",
                                    e
                                )));
                            }
                        }

                        client
                            .simple_query("COMMIT TRANSACTION")
                            .await
                            .map_err(|e| {
                                CdcError::generic(format!(
                                    "SQL Server COMMIT TRANSACTION failed after bulk load: {e}"
                                ))
                            })?;

                        Ok(())
                    }
                    Err(e) => {
                        warn!(
                            "TDS Bulk Load finalize failed, falling back to multi-value INSERT: {}",
                            e
                        );
                        let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                        self.fallback_multi_value_insert(table, columns, rows, pre_commit_hook)
                            .await
                    }
                }
            }
            Err(e) => {
                warn!(
                    "TDS Bulk Load init failed, falling back to multi-value INSERT: {}",
                    e
                );
                let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                self.fallback_multi_value_insert(table, columns, rows, pre_commit_hook)
                    .await
            }
        }
    }
}

impl Default for SqlServerDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlServerDestination {
    async fn fallback_multi_value_insert(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<String>],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        let sqls = super::bulk_insert::build_chunked_multi_value_inserts(
            table,
            columns,
            rows,
            None,
            Some(self.max_rows_per_insert),
        );
        self.execute_sql_batch_with_hook(&sqls, pre_commit_hook)
            .await
    }
}

fn parse_sql_value(value: &str) -> ColumnData<'static> {
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("NULL") {
        return ColumnData::String(None);
    }

    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let unescaped = inner.replace("''", "'");
        return ColumnData::String(Some(Cow::Owned(unescaped)));
    }

    if let Some(bytes) = decode_hex_0x(trimmed) {
        return ColumnData::Binary(Some(Cow::Owned(bytes)));
    }

    // Send remaining values as strings — SQL Server handles implicit type conversion.
    // Avoids f64 precision loss for large decimals in CDC data.
    ColumnData::String(Some(Cow::Owned(trimmed.to_string())))
}

/// Decode SQL Server hex literal (0xDEADBEEF) into raw bytes.
fn decode_hex_0x(s: &str) -> Option<Vec<u8>> {
    if s.len() < 4 || !s.starts_with("0x") && !s.starts_with("0X") {
        return None;
    }
    let hex_str = &s[2..];
    if hex_str.len() % 2 != 0 || !hex_str.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes: Vec<u8> = (0..hex_str.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).unwrap())
        .collect();
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlserver_destination_creation() {
        let destination = SqlServerDestination::new();
        assert!(destination.client.is_none());
    }

    #[test]
    fn test_parse_sql_value_null() {
        let result = parse_sql_value("NULL");
        assert!(matches!(result, ColumnData::String(None)));
    }

    #[test]
    fn test_parse_sql_value_integer() {
        let result = parse_sql_value("42");
        assert_eq!(
            result,
            ColumnData::String(Some(Cow::Owned("42".to_string())))
        );
    }

    #[test]
    fn test_parse_sql_value_negative_integer() {
        let result = parse_sql_value("-123");
        assert_eq!(
            result,
            ColumnData::String(Some(Cow::Owned("-123".to_string())))
        );
    }

    #[test]
    fn test_parse_sql_value_float() {
        let result = parse_sql_value("3.14");
        assert_eq!(
            result,
            ColumnData::String(Some(Cow::Owned("3.14".to_string())))
        );
    }

    #[test]
    fn test_parse_sql_value_string() {
        let result = parse_sql_value("'hello world'");
        assert_eq!(
            result,
            ColumnData::String(Some(Cow::Owned("hello world".to_string())))
        );
    }

    #[test]
    fn test_parse_sql_value_escaped_string() {
        let result = parse_sql_value("'it''s escaped'");
        assert_eq!(
            result,
            ColumnData::String(Some(Cow::Owned("it's escaped".to_string())))
        );
    }

    #[test]
    fn test_parse_sql_value_unquoted_string() {
        let result = parse_sql_value("some_value");
        assert_eq!(
            result,
            ColumnData::String(Some(Cow::Owned("some_value".to_string())))
        );
    }

    #[test]
    fn test_parse_sql_value_hex_binary() {
        let result = parse_sql_value("0xDEADBEEF");
        assert_eq!(
            result,
            ColumnData::Binary(Some(Cow::Owned(vec![0xDE, 0xAD, 0xBE, 0xEF])))
        );
    }

    #[test]
    fn test_parse_sql_value_hex_binary_lowercase() {
        let result = parse_sql_value("0xcafe");
        assert_eq!(
            result,
            ColumnData::Binary(Some(Cow::Owned(vec![0xCA, 0xFE])))
        );
    }

    #[test]
    fn test_decode_hex_0x_invalid() {
        assert!(decode_hex_0x("hello").is_none());
        assert!(decode_hex_0x("0x").is_none());
        assert!(decode_hex_0x("0xZZ").is_none());
        assert!(decode_hex_0x("0xABC").is_none()); // odd length
    }
}
