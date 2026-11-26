use super::coalescing::{coalesce_commands, QuoteStyle};
use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use tiberius::{
    Client, ColumnData, Config, FixedLenType, SqlBulkCopyOption, TokenRow, TypeInfo, VarLenType,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};

/// SQL Server rejects an INSERT ... VALUES with more rows than this.
const MAX_ROWS_PER_INSERT: usize = 1000;

/// SQL Server destination implementation
pub struct SqlServerDestination {
    client: Option<Client<Compat<TcpStream>>>,
    /// Schema mappings: maps source schema to destination schema
    schema_mappings: HashMap<String, String>,
    /// Maximum rows per INSERT VALUES statement (at most `MAX_ROWS_PER_INSERT`)
    max_rows_per_insert: usize,
    /// Tables where TDS Bulk Load has failed (bypass on subsequent attempts)
    failed_bulk_tables: HashSet<String>,
    /// Set by `begin_snapshot_writes`: the snapshot's rows bulk-load with `KEEP_NULLS` only, as a
    /// bulk copy of existing rows should. The live path also fires the table's triggers and checks
    /// its constraints, as the INSERT it replaces would.
    snapshot_writer: bool,
    /// Types of each bulk-load target's writable columns by (table, column list), read once
    bulk_column_types: HashMap<(String, Vec<String>), Vec<TypeInfo>>,
}

impl SqlServerDestination {
    /// Create a new SQL Server destination instance
    pub fn new() -> Self {
        Self {
            client: None,
            schema_mappings: HashMap::new(),
            max_rows_per_insert: MAX_ROWS_PER_INSERT,
            failed_bulk_tables: HashSet::new(),
            snapshot_writer: false,
            bulk_column_types: HashMap::new(),
        }
    }

    fn snapshot_probe_sql(&self, schema: &str, table: &str) -> String {
        let mut qualified = String::new();
        self.dialect().qualify_table(schema, table, &mut qualified);
        format!("SELECT TOP 1 1 FROM {qualified}")
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
            self.max_rows_per_insert = max_rows.min(MAX_ROWS_PER_INSERT);
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

    async fn snapshot_table_is_empty(&mut self, schema: &str, table: &str) -> Result<bool> {
        let sql = self.snapshot_probe_sql(schema, table);
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server client not initialized"))?;
        let row = client
            .simple_query(sql)
            .await
            .map_err(|e| {
                CdcError::generic(format!("SQL Server snapshot emptiness probe failed: {e}"))
            })?
            .into_row()
            .await
            .map_err(|e| {
                CdcError::generic(format!("SQL Server snapshot emptiness probe failed: {e}"))
            })?;
        Ok(row.is_none())
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

    fn dialect(&self) -> &'static dyn SqlDialect {
        use crate::destinations::dialects::SqlServerDialect;
        static D: SqlServerDialect = SqlServerDialect;
        &D
    }

    fn begin_snapshot_writes(&mut self) {
        self.snapshot_writer = true;
    }

    /// TDS Bulk Load into the named `columns` with `KEEP_NULLS` (a NULL stays NULL instead of
    /// becoming the column's DEFAULT). The renderer quotes every value, so integers arrive as
    /// strings and are converted to the column's integer type (`coerce_to_column`). On the live
    /// path `FIRE_TRIGGERS` and `CHECK_CONSTRAINTS` are requested too, so triggers fire and
    /// constraints apply as they do for the INSERT the bulk load replaces; the snapshot's writers
    /// ask for `KEEP_NULLS` only. A column the call does not name gets its DEFAULT. A bulk
    /// failure falls back to multi-value INSERT for that batch, and for that table from then on
    /// (a table with a type other than text, integer or binary, for example a timestamp, decimal,
    /// uuid or bit, takes that fallback after its first failed load), except when the server
    /// rejected the rows themselves (`is_row_error`).
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

        let bulk_table = table.trim();

        if self.failed_bulk_tables.contains(bulk_table) {
            return self
                .fallback_multi_value_insert(table, columns, rows, pre_commit_hook)
                .await;
        }

        let row_count = rows.len();

        debug!(
            "Attempting TDS Bulk Load: {} rows into {}",
            row_count, bulk_table
        );

        let options = self.bulk_copy_options();
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server client not initialized"))?;

        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "SQL Server BEGIN TRANSACTION failed for bulk load: {e}"
                ))
            })?;

        // On failure `bulk_insert_with_options` below runs the same query and reports it.
        let key = (bulk_table.to_string(), columns.to_vec());
        if !self.bulk_column_types.contains_key(&key) {
            if let Ok(meta) = client.column_metadata(bulk_table, &names).await {
                let types = meta.into_iter().filter(|c| c.base.is_updateable());
                self.bulk_column_types
                    .insert(key.clone(), types.map(|c| c.base.ty).collect());
            }
        }
        let column_types = self
            .bulk_column_types
            .get(&key)
            .cloned()
            .unwrap_or_default();

        match client
            .bulk_insert_with_options(bulk_table, &names, options, &[])
            .await
        {
            Ok(mut req) => {
                for row_values in rows {
                    let mut token_row = TokenRow::new();
                    for (i, value) in row_values.iter().enumerate() {
                        token_row.push(coerce_to_column(
                            parse_sql_value(value),
                            column_types.get(i),
                        ));
                    }
                    if let Err(e) = req.send(token_row).await {
                        warn_bulk_fallback("send", bulk_table, &e);
                        drop(req);
                        let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                        if !is_row_error(&e) {
                            self.failed_bulk_tables.insert(bulk_table.to_string());
                        }
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
                        warn_bulk_fallback("finalize", bulk_table, &e);
                        let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                        if !is_row_error(&e) {
                            self.failed_bulk_tables.insert(bulk_table.to_string());
                        }
                        self.fallback_multi_value_insert(table, columns, rows, pre_commit_hook)
                            .await
                    }
                }
            }
            Err(e) => {
                warn_bulk_fallback("init", bulk_table, &e);
                let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                self.failed_bulk_tables.insert(bulk_table.to_string());
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
    fn bulk_copy_options(&self) -> tiberius::SqlBulkCopyOptions {
        if self.snapshot_writer {
            SqlBulkCopyOption::KeepNulls.into()
        } else {
            SqlBulkCopyOption::KeepNulls
                | SqlBulkCopyOption::FireTriggers
                | SqlBulkCopyOption::CheckConstraints
        }
    }

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

fn warn_bulk_fallback(stage: &str, table: &str, error: &dyn std::fmt::Display) {
    warn!(
        "TDS Bulk Load {stage} failed for {table}: {error}; falling back to multi-value INSERT for this batch (for good, unless the error names a row), where non-ASCII text may be replaced by '?' in NVARCHAR columns"
    );
}

/// The server rejected the rows (duplicate key 2601/2627, CHECK or FOREIGN KEY 547), not the bulk
/// load: the table's types are fine and the next batch bulk-loads again.
fn is_row_error(error: &tiberius::error::Error) -> bool {
    matches!(error.code(), Some(2601 | 2627 | 547))
}

fn parse_sql_value(value: &str) -> ColumnData<'static> {
    let trimmed = value.trim();

    if trimmed.eq_ignore_ascii_case("NULL") {
        return ColumnData::String(None);
    }

    if let Some(inner) = trimmed
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
    {
        let unescaped = inner.replace("''", "'");
        return ColumnData::String(Some(Cow::Owned(unescaped)));
    }

    if let Some(bytes) = decode_hex_0x(trimmed) {
        return ColumnData::Binary(Some(Cow::Owned(bytes)));
    }

    if trimmed.eq_ignore_ascii_case("true") {
        return ColumnData::Bit(Some(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return ColumnData::Bit(Some(false));
    }

    if let Ok(val) = trimmed.parse::<i64>() {
        return ColumnData::I64(Some(val));
    }

    if let Ok(val) = trimmed.parse::<f64>() {
        return ColumnData::F64(Some(val));
    }

    ColumnData::String(Some(Cow::Owned(trimmed.to_string())))
}

/// The bulk-load encoder wants the exact `ColumnData` variant of each destination column, but the
/// renderer quotes every value, so an integer arrives as the string `'42'` and a NULL as a string
/// NULL. Integer columns get their integer; any other column keeps the string (a mismatch makes
/// tiberius reject the batch, which then falls back to multi-value INSERT).
fn coerce_to_column(value: ColumnData<'static>, ty: Option<&TypeInfo>) -> ColumnData<'static> {
    let ColumnData::String(text) = &value else {
        return value;
    };
    let width = match ty {
        Some(TypeInfo::FixedLen(FixedLenType::Int1)) => 1,
        Some(TypeInfo::FixedLen(FixedLenType::Int2)) => 2,
        Some(TypeInfo::FixedLen(FixedLenType::Int4)) => 4,
        Some(TypeInfo::FixedLen(FixedLenType::Int8)) => 8,
        Some(TypeInfo::VarLenSized(vlc)) if vlc.r#type() == VarLenType::Intn => vlc.len(),
        _ => return value,
    };
    let text = text.as_deref().map(str::trim);
    let converted = match width {
        1 => parse_int(text).map(ColumnData::U8),
        2 => parse_int(text).map(ColumnData::I16),
        4 => parse_int(text).map(ColumnData::I32),
        8 => parse_int(text).map(ColumnData::I64),
        _ => None,
    };
    converted.unwrap_or(value)
}

/// `Some(None)` for NULL, `None` when `text` is not an integer of this width.
fn parse_int<T: std::str::FromStr>(text: Option<&str>) -> Option<Option<T>> {
    match text {
        None => Some(None),
        Some(t) => t.parse().ok().map(Some),
    }
}

/// Decode SQL Server hex literal (0xDEADBEEF) into raw bytes.
fn decode_hex_0x(s: &str) -> Option<Vec<u8>> {
    if s.len() < 4 || !(s.starts_with("0x") || s.starts_with("0X")) {
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
    fn snapshot_probe_uses_top_and_brackets() {
        let destination = SqlServerDestination::new();
        assert_eq!(
            destination.snapshot_probe_sql("dbo", "Snap]Mixed"),
            "SELECT TOP 1 1 FROM [dbo].[Snap]]Mixed]"
        );
        assert_eq!(
            destination.snapshot_probe_sql("dbo", "x]; DROP TABLE users; --"),
            "SELECT TOP 1 1 FROM [dbo].[x]]; DROP TABLE users; --]"
        );
    }

    #[tokio::test]
    async fn snapshot_probe_without_a_client_is_an_error() {
        let mut destination = SqlServerDestination::new();
        assert!(destination
            .snapshot_table_is_empty("dbo", "t")
            .await
            .is_err());
    }

    #[test]
    fn test_parse_sql_value_null() {
        let result = parse_sql_value("NULL");
        assert!(matches!(result, ColumnData::String(None)));
    }

    #[test]
    fn test_parse_sql_value_integer() {
        let result = parse_sql_value("42");
        assert_eq!(result, ColumnData::I64(Some(42)));
    }

    #[test]
    fn test_parse_sql_value_negative_integer() {
        let result = parse_sql_value("-123");
        assert_eq!(result, ColumnData::I64(Some(-123)));
    }

    #[test]
    fn test_parse_sql_value_float() {
        let result = parse_sql_value("3.14");
        assert_eq!(result, ColumnData::F64(Some(3.14)));
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
    fn test_parse_sql_value_boolean_true() {
        let result = parse_sql_value("true");
        assert_eq!(result, ColumnData::Bit(Some(true)));
    }

    #[test]
    fn test_parse_sql_value_boolean_false() {
        let result = parse_sql_value("false");
        assert_eq!(result, ColumnData::Bit(Some(false)));
    }

    fn quoted(s: &str) -> ColumnData<'static> {
        parse_sql_value(&format!("'{s}'"))
    }

    #[test]
    fn coerce_to_column_turns_quoted_integers_into_the_columns_integer_type() {
        let big = TypeInfo::FixedLen(FixedLenType::Int8);
        let int = TypeInfo::FixedLen(FixedLenType::Int4);
        assert_eq!(
            coerce_to_column(quoted("42"), Some(&big)),
            ColumnData::I64(Some(42))
        );
        assert_eq!(
            coerce_to_column(quoted(" -7 "), Some(&int)),
            ColumnData::I32(Some(-7))
        );
        assert_eq!(
            coerce_to_column(parse_sql_value("NULL"), Some(&big)),
            ColumnData::I64(None)
        );
    }

    #[test]
    fn coerce_to_column_leaves_everything_else_alone() {
        let big = TypeInfo::FixedLen(FixedLenType::Int8);
        let text = TypeInfo::VarLenSized(tiberius::VarLenContext::new(
            VarLenType::NVarchar,
            8000,
            None,
        ));
        // not an integer of that width, a text column, no metadata, a value that is not a string
        assert_eq!(coerce_to_column(quoted("4x"), Some(&big)), quoted("4x"));
        assert_eq!(
            coerce_to_column(quoted("300"), Some(&TypeInfo::FixedLen(FixedLenType::Int1))),
            quoted("300")
        );
        assert_eq!(coerce_to_column(quoted("42"), Some(&text)), quoted("42"));
        assert_eq!(coerce_to_column(quoted("42"), None), quoted("42"));
        assert_eq!(
            coerce_to_column(ColumnData::I64(Some(1)), Some(&big)),
            ColumnData::I64(Some(1))
        );
    }

    fn intn(len: usize) -> TypeInfo {
        TypeInfo::VarLenSized(tiberius::VarLenContext::new(VarLenType::Intn, len, None))
    }

    #[test]
    fn coerce_to_column_handles_every_integer_width_and_its_nulls() {
        let null = parse_sql_value("NULL");
        let cases: [(TypeInfo, &str, ColumnData<'static>, ColumnData<'static>); 7] = [
            (
                TypeInfo::FixedLen(FixedLenType::Int1),
                "255",
                ColumnData::U8(Some(255)),
                ColumnData::U8(None),
            ),
            (
                TypeInfo::FixedLen(FixedLenType::Int2),
                "-32768",
                ColumnData::I16(Some(-32768)),
                ColumnData::I16(None),
            ),
            (intn(1), "7", ColumnData::U8(Some(7)), ColumnData::U8(None)),
            (
                intn(2),
                "32767",
                ColumnData::I16(Some(32767)),
                ColumnData::I16(None),
            ),
            (
                intn(4),
                "-2147483648",
                ColumnData::I32(Some(i32::MIN)),
                ColumnData::I32(None),
            ),
            (
                intn(8),
                "9223372036854775807",
                ColumnData::I64(Some(i64::MAX)),
                ColumnData::I64(None),
            ),
            (
                TypeInfo::FixedLen(FixedLenType::Int4),
                "0",
                ColumnData::I32(Some(0)),
                ColumnData::I32(None),
            ),
        ];
        for (ty, text, value, typed_null) in cases {
            assert_eq!(coerce_to_column(quoted(text), Some(&ty)), value, "{ty:?}");
            assert_eq!(
                coerce_to_column(null.clone(), Some(&ty)),
                typed_null,
                "NULL for {ty:?}"
            );
        }
    }

    #[test]
    fn coerce_to_column_trims_whitespace_and_accepts_leading_zeros() {
        assert_eq!(
            coerce_to_column(quoted(" 042 "), Some(&intn(4))),
            ColumnData::I32(Some(42))
        );
        assert_eq!(
            coerce_to_column(quoted("007"), Some(&intn(1))),
            ColumnData::U8(Some(7))
        );
        assert_eq!(
            coerce_to_column(
                quoted("-0005"),
                Some(&TypeInfo::FixedLen(FixedLenType::Int2))
            ),
            ColumnData::I16(Some(-5))
        );
    }

    #[test]
    fn coerce_to_column_keeps_values_out_of_tinyint_and_smallint_range() {
        for ty in [TypeInfo::FixedLen(FixedLenType::Int1), intn(1)] {
            for text in ["-1", "256", "300"] {
                assert_eq!(coerce_to_column(quoted(text), Some(&ty)), quoted(text));
            }
        }
        for ty in [TypeInfo::FixedLen(FixedLenType::Int2), intn(2)] {
            assert_eq!(
                coerce_to_column(quoted("32768"), Some(&ty)),
                quoted("32768")
            );
        }
    }

    #[test]
    fn live_bulk_load_fires_triggers_and_checks_constraints() {
        let options = SqlServerDestination::new().bulk_copy_options();
        assert!(options.contains(SqlBulkCopyOption::KeepNulls));
        assert!(options.contains(SqlBulkCopyOption::FireTriggers));
        assert!(options.contains(SqlBulkCopyOption::CheckConstraints));
    }

    #[test]
    fn snapshot_bulk_load_only_keeps_nulls() {
        let mut destination = SqlServerDestination::new();
        destination.begin_snapshot_writes();
        assert_eq!(
            destination.bulk_copy_options(),
            tiberius::SqlBulkCopyOptions::from(SqlBulkCopyOption::KeepNulls)
        );
    }

    #[test]
    fn max_rows_per_insert_is_capped_at_the_sql_server_limit() {
        let mut destination = SqlServerDestination::new();
        destination.set_max_rows_per_insert(5000);
        assert_eq!(destination.max_rows_per_insert, 1000);
        destination.set_max_rows_per_insert(10);
        assert_eq!(destination.max_rows_per_insert, 10);
        destination.set_max_rows_per_insert(0);
        assert_eq!(destination.max_rows_per_insert, 10);
    }

    #[test]
    fn test_decode_hex_0x_invalid() {
        assert!(decode_hex_0x("hello").is_none());
        assert!(decode_hex_0x("0x").is_none());
        assert!(decode_hex_0x("0xZZ").is_none());
        assert!(decode_hex_0x("0xABC").is_none()); // odd length
    }
}
