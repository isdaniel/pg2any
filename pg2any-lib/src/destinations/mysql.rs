use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use mysql_async::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use super::coalescing::{coalesce_commands, QuoteStyle};

/// Look up a column's value in a row BY NAME.
///
/// The bulk-insert trait contract takes `columns` and `rows` separately; a custom caller may pass rows whose `(name, value)` order differs from `columns`. Aligning by name (rather than positionally) prevents silently  writing a value into the wrong column. Returns `None` if the column is absent from the row (callers default to `ColumnValue::Null`).
pub(crate) fn lookup_value_by_name<'a>(
    row: &'a pg_walstream::RowData,
    col: &str,
) -> Option<&'a pg_walstream::ColumnValue> {
    row.iter()
        .find(|(name, _)| name.as_ref() == col)
        .map(|(_, value)| value)
}

/// Render `rows` to per-row SQL value-literal vectors, aligned to `columns` by name (missing column -> NULL). Shared by the no-LOAD-DATA and fallback paths.
pub(crate) fn render_rows_to_sql(
    dialect: &dyn crate::destinations::dialect::SqlDialect,
    columns: &[Arc<str>],
    rows: &[&pg_walstream::RowData],
) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| {
                    let mut s = String::new();
                    match lookup_value_by_name(row, col) {
                        Some(value) => dialect.render_value(value, &mut s),
                        None => dialect.render_value(&pg_walstream::ColumnValue::Null, &mut s),
                    }
                    s
                })
                .collect()
        })
        .collect()
}

struct InfileDataGuard(Arc<Mutex<Option<bytes::Bytes>>>);
impl Drop for InfileDataGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = self.0.lock() {
            *lock = None;
        }
    }
}

pub struct MySQLDestination {
    pool: Option<mysql_async::Pool>,
    infile_data: Arc<Mutex<Option<bytes::Bytes>>>,
    schema_mappings: HashMap<String, String>,
    max_allowed_packet: u64,
    load_data_available: bool,
}

impl MySQLDestination {
    pub fn new() -> Self {
        Self {
            pool: None,
            infile_data: Arc::new(Mutex::new(None)),
            schema_mappings: HashMap::new(),
            max_allowed_packet: 67108864,
            load_data_available: false,
        }
    }
}

impl Default for MySQLDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DestinationHandler for MySQLDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let opts = mysql_async::Opts::from_url(connection_string)
            .map_err(|e| CdcError::generic(format!("Failed to parse MySQL URL: {e}")))?;

        let infile_data = self.infile_data.clone();
        let opts = mysql_async::OptsBuilder::from_opts(opts).local_infile_handler(Some(
            move |_file_name: &[u8]| {
                let data = infile_data.clone();
                Box::pin(async move {
                    let bytes = data
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap_or_else(|| bytes::Bytes::new());
                    let stream =
                        futures_util::stream::once(async move { Ok::<_, std::io::Error>(bytes) });
                    Ok(Box::pin(stream) as mysql_async::InfileData)
                })
                    as futures_util::future::BoxFuture<
                        'static,
                        std::result::Result<mysql_async::InfileData, mysql_async::LocalInfileError>,
                    >
            },
        ));

        let pool = mysql_async::Pool::new(opts);

        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to connect to MySQL: {e}")))?;

        let result: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'max_allowed_packet'")
            .await
            .map_err(|e| CdcError::generic(format!("Failed to query max_allowed_packet: {e}")))?;

        if let Some((_, value)) = result {
            self.max_allowed_packet = value.parse::<u64>().unwrap_or(67108864);
        }

        info!(
            "MySQL max_allowed_packet: {} bytes ({:.2} MB)",
            self.max_allowed_packet,
            self.max_allowed_packet as f64 / 1_048_576.0
        );

        drop(conn);
        self.pool = Some(pool);

        match self.check_load_data_available().await {
            Ok(available) => {
                self.load_data_available = available;
                if available {
                    info!("MySQL LOAD DATA LOCAL INFILE: available");
                } else {
                    warn!("MySQL LOAD DATA LOCAL INFILE: not available, falling back to multi-value INSERT");
                }
            }
            Err(e) => {
                warn!(
                    "Failed to check LOAD DATA availability: {}, falling back to multi-value INSERT",
                    e
                );
                self.load_data_available = false;
            }
        }

        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
        if !self.schema_mappings.is_empty() {
            debug!(
                "MySQL destination schema mappings set: {:?}",
                self.schema_mappings
            );
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

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL pool not initialized"))?;

        let coalesced = coalesce_commands(
            commands,
            self.max_allowed_packet,
            QuoteStyle::Backtick,
            usize::MAX,
        );

        if coalesced.len() < commands.len() {
            debug!(
                "Coalesced {} commands into {} statements (reduction: {:.1}%)",
                commands.len(),
                coalesced.len(),
                (1.0 - coalesced.len() as f64 / commands.len() as f64) * 100.0
            );
        }

        let mut tx = pool
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(|e| CdcError::generic(format!("MySQL BEGIN transaction failed: {e}")))?;

        for (idx, sql) in coalesced.iter().enumerate() {
            if let Err(e) = tx.query_drop(sql.as_ref()).await {
                let _ = tx.rollback().await;
                return Err(CdcError::generic(format!(
                    "MySQL execute_sql_batch failed at command {}/{}: {}",
                    idx + 1,
                    coalesced.len(),
                    e
                )));
            }
        }

        if let Some(hook) = pre_commit_hook {
            if let Err(e) = hook().await {
                let _ = tx.rollback().await;
                return Err(CdcError::generic(format!(
                    "MySQL pre-commit hook failed, transaction rolled back: {}",
                    e
                )));
            }
        }

        tx.commit()
            .await
            .map_err(|e| CdcError::generic(format!("MySQL COMMIT transaction failed: {e}")))?;

        Ok(())
    }

    fn supports_bulk_insert(&self) -> bool {
        self.load_data_available
    }

    fn dialect(&self) -> &'static dyn SqlDialect {
        use crate::destinations::dialects::MySqlDialect;
        static D: MySqlDialect = MySqlDialect;
        &D
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

        if !self.load_data_available {
            let sqls = super::bulk_insert::build_chunked_multi_value_inserts(
                table,
                columns,
                rows,
                Some(self.max_allowed_packet as usize),
                None,
            );
            return self
                .execute_sql_batch_with_hook(&sqls, pre_commit_hook)
                .await;
        }

        self.execute_load_data(table, columns, rows, pre_commit_hook)
            .await
    }

    async fn execute_bulk_insert_rows_with_hook(
        &mut self,
        table: &str,
        columns: &[Arc<str>],
        rows: &[&pg_walstream::RowData],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Quote columns with the MySQL dialect (matches the SQL/LOAD DATA path).
        let dialect = self.dialect();
        let quoted_columns: Vec<String> = columns
            .iter()
            .map(|c| {
                let mut s = String::with_capacity(c.len() + 2);
                dialect.quote_identifier(c, &mut s);
                s
            })
            .collect();

        // No LOAD DATA available: render the structured rows to SQL value
        // strings and go through the multi-value INSERT path.
        if !self.load_data_available {
            let rendered_rows = render_rows_to_sql(dialect, columns, rows);
            let sqls = super::bulk_insert::build_chunked_multi_value_inserts(
                table,
                &quoted_columns,
                &rendered_rows,
                Some(self.max_allowed_packet as usize),
                None,
            );
            return self
                .execute_sql_batch_with_hook(&sqls, pre_commit_hook)
                .await;
        }

        // Fast path: build the TSV buffer DIRECTLY from RowData (no SQL render,
        // no reparse). The fallback multi-value INSERT statements are rendered
        // lazily (the closure below) only if LOAD DATA fails at runtime.
        let tsv_data = generate_tsv_buffer_from_rows(columns, rows);
        let row_count = rows.len();
        let max_packet = self.max_allowed_packet as usize;

        self.load_data_with_tsv(
            table,
            &quoted_columns,
            tsv_data,
            row_count,
            || {
                let rendered_rows = render_rows_to_sql(dialect, columns, rows);
                super::bulk_insert::build_chunked_multi_value_inserts(
                    table,
                    &quoted_columns,
                    &rendered_rows,
                    Some(max_packet),
                    None,
                )
            },
            pre_commit_hook,
        )
        .await
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.disconnect()
                .await
                .map_err(|e| CdcError::generic(format!("Failed to disconnect MySQL pool: {e}")))?;
        }

        info!("MySQL connection closed successfully");
        Ok(())
    }
}

impl MySQLDestination {
    async fn check_load_data_available(&self) -> Result<bool> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("Pool not initialized"))?;

        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to get connection: {e}")))?;

        let result: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'local_infile'")
            .await
            .map_err(|e| CdcError::generic(format!("Failed to check local_infile: {e}")))?;

        drop(conn);

        match result {
            Some((_, value)) => Ok(value.eq_ignore_ascii_case("ON")),
            None => Ok(false),
        }
    }

    async fn execute_load_data(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<String>],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        let tsv_data = generate_tsv_buffer(rows);
        let max_packet = self.max_allowed_packet as usize;
        self.load_data_with_tsv(
            table,
            columns,
            tsv_data,
            rows.len(),
            // Fallback SQL rendered lazily, only if LOAD DATA fails at runtime.
            || {
                super::bulk_insert::build_chunked_multi_value_inserts(
                    table,
                    columns,
                    rows,
                    Some(max_packet),
                    None,
                )
            },
            pre_commit_hook,
        )
        .await
    }

    /// Shared LOAD DATA LOCAL INFILE execution: stream `tsv_data` into `table`,
    /// run the optional pre-commit hook inside the transaction, then commit.
    /// On LOAD DATA failure, roll back and replay `fallback_sqls` (multi-value
    /// INSERT) through the normal SQL batch path.
    async fn load_data_with_tsv(
        &mut self,
        table: &str,
        columns: &[String],
        tsv_data: Vec<u8>,
        row_count: usize,
        fallback_fn: impl FnOnce() -> Vec<String>,
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("Pool not initialized"))?;

        debug!(
            "Executing LOAD DATA LOCAL INFILE for {} rows ({} bytes TSV) into {}",
            row_count,
            tsv_data.len(),
            table
        );

        let mut tx = pool
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(|e| CdcError::generic(format!("MySQL START TRANSACTION failed: {e}")))?;

        let col_spec = columns.join(", ");
        let load_sql = format!(
            "LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE {} \
             FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' ({})",
            table, col_spec
        );

        *self.infile_data.lock().unwrap() = Some(bytes::Bytes::from(tsv_data));

        let result = {
            let _guard = InfileDataGuard(self.infile_data.clone());
            tx.query_drop(&load_sql).await
        };

        if let Err(e) = result {
            debug!("LOAD DATA LOCAL INFILE failed, falling back to multi-value INSERT: {e}");
            let _ = tx.rollback().await;

            // Render the fallback SQL only now, on actual failure — this keeps
            // the LOAD DATA fast path free of any SQL rendering.
            let fallback_sqls = fallback_fn();
            return self
                .execute_sql_batch_with_hook(&fallback_sqls, pre_commit_hook)
                .await;
        }

        if let Some(hook) = pre_commit_hook {
            if let Err(e) = hook().await {
                let _ = tx.rollback().await;
                return Err(CdcError::generic(format!(
                    "MySQL bulk insert pre-commit hook failed, rolled back: {e}"
                )));
            }
        }

        tx.commit()
            .await
            .map_err(|e| CdcError::generic(format!("MySQL COMMIT failed after LOAD DATA: {e}")))?;

        info!(
            "LOAD DATA complete: {} rows loaded into {}",
            row_count, table
        );

        Ok(())
    }
}

fn generate_tsv_buffer(rows: &[Vec<String>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(rows.len() * 128);

    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx > 0 {
                buf.push(b'\t');
            }
            let trimmed = value.trim();
            if trimmed.eq_ignore_ascii_case("NULL") {
                buf.extend_from_slice(b"\\N");
            } else if trimmed.eq_ignore_ascii_case("true") {
                buf.extend_from_slice(b"1");
            } else if trimmed.eq_ignore_ascii_case("false") {
                buf.extend_from_slice(b"0");
            } else if let Some(unquoted) = trimmed
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
            {
                tsv_escape_string(unquoted, &mut buf);
            } else if decode_hex_into(trimmed, &mut buf) {
                // decoded + escaped directly into buf
            } else {
                tsv_escape_raw(trimmed.as_bytes(), &mut buf);
            }
        }
        buf.push(b'\n');
    }

    buf
}

/// Build a MySQL LOAD DATA TSV buffer DIRECTLY from structured `RowData`,
/// skipping the render-SQL-then-reparse-SQL round trip.
///
/// The produced bytes are byte-for-byte identical to running the rows through
/// the legacy path (`MySqlDialect::render_value` → `detect_bulk_insert_batch`
/// → `generate_tsv_buffer`). The byte-equality invariant is proven by
/// `test_tsv_from_rows_matches_sql_roundtrip`.
///
/// Per `ColumnValue`:
///   * `Null`        → `\N`
///   * `Text("t")`   → `1`   (boolean rendering, matches `render_value`)
///   * `Text("f")`   → `0`
///   * `Text(other)` → raw bytes, TSV-escaped
///   * `Binary`      → raw bytes, TSV-escaped (no hex round-trip)
pub(crate) fn generate_tsv_buffer_from_rows(
    columns: &[Arc<str>],
    rows: &[&pg_walstream::RowData],
) -> Vec<u8> {
    use pg_walstream::ColumnValue;

    // Encode a single ColumnValue into the TSV buffer (no field separator).
    fn encode_value(value: &ColumnValue, buf: &mut Vec<u8>) {
        match value {
            ColumnValue::Null => buf.extend_from_slice(b"\\N"),
            ColumnValue::Text(_) => match value.as_str() {
                // Boolean rendering mirrors `MySqlDialect::render_value`:
                // 't'/'f' become bare 1/0 in the legacy SQL path.
                Some("t") => buf.push(b'1'),
                Some("f") => buf.push(b'0'),
                // Valid UTF-8 text: escape the raw bytes. This matches the
                // legacy path where text is quoted ('...') then unquoted and
                // run through `tsv_escape_string`, which is byte-equivalent
                // to `tsv_escape_raw` for all inputs.
                Some(_) => tsv_escape_raw(value.as_bytes(), buf),
                // Invalid UTF-8 text: the legacy path renders it as an
                // X'..' hex literal, which `generate_tsv_buffer` decodes and
                // escapes byte-by-byte — identical to escaping raw bytes.
                None => tsv_escape_raw(value.as_bytes(), buf),
            },
            ColumnValue::Binary(_) => tsv_escape_raw(value.as_bytes(), buf),
        }
    }

    let mut buf = Vec::with_capacity(rows.len() * 128);

    for row in rows {
        for (col_idx, col) in columns.iter().enumerate() {
            if col_idx > 0 {
                buf.push(b'\t');
            }
            // Align to `columns` BY NAME (absent column → Null), so a row whose
            // field order differs from `columns` cannot corrupt other columns.
            match lookup_value_by_name(row, col) {
                Some(value) => encode_value(value, &mut buf),
                None => encode_value(&ColumnValue::Null, &mut buf),
            }
        }
        buf.push(b'\n');
    }

    buf
}

fn decode_hex_into(s: &str, out: &mut Vec<u8>) -> bool {
    if s.len() < 3 {
        return false;
    }
    if !(s.starts_with("X'") || s.starts_with("x'")) || !s.ends_with('\'') {
        return false;
    }
    let hex = &s.as_bytes()[2..s.len() - 1];
    if hex.len() % 2 != 0 || !hex.iter().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    let nibble = |b: u8| -> u8 {
        match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => unreachable!("validated as ascii hexdigit above"),
        }
    };
    let mut i = 0;
    while i < hex.len() {
        tsv_escape_byte((nibble(hex[i]) << 4) | nibble(hex[i + 1]), out);
        i += 2;
    }
    true
}

fn tsv_escape_string(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 1;
                }
                buf.push(b'\'');
            }
            b'\\' => {
                if i + 1 < bytes.len() {
                    let next = bytes[i + 1];
                    match next {
                        b'\\' => {
                            buf.extend_from_slice(b"\\\\");
                            i += 1;
                        }
                        b'n' => {
                            buf.extend_from_slice(b"\\n");
                            i += 1;
                        }
                        b't' => {
                            buf.extend_from_slice(b"\\t");
                            i += 1;
                        }
                        b'r' => {
                            buf.extend_from_slice(b"\\r");
                            i += 1;
                        }
                        b'0' => {
                            buf.extend_from_slice(b"\\0");
                            i += 1;
                        }
                        b'b' => {
                            buf.extend_from_slice(b"\\b");
                            i += 1;
                        }
                        b'Z' => {
                            buf.extend_from_slice(b"\\Z");
                            i += 1;
                        }
                        _ => {
                            buf.push(next);
                            i += 1;
                        }
                    }
                } else {
                    buf.extend_from_slice(b"\\\\");
                }
            }
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            0 => buf.extend_from_slice(b"\\0"),
            _ => buf.push(b),
        }
        i += 1;
    }
}

fn tsv_escape_byte(b: u8, buf: &mut Vec<u8>) {
    match b {
        b'\\' => buf.extend_from_slice(b"\\\\"),
        b'\t' => buf.extend_from_slice(b"\\t"),
        b'\n' => buf.extend_from_slice(b"\\n"),
        b'\r' => buf.extend_from_slice(b"\\r"),
        0 => buf.extend_from_slice(b"\\0"),
        _ => buf.push(b),
    }
}

fn tsv_escape_raw(data: &[u8], buf: &mut Vec<u8>) {
    for &b in data {
        tsv_escape_byte(b, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
        assert!(destination.infile_data.lock().unwrap().is_none());
        assert!(!destination.load_data_available);
    }

    #[test]
    fn test_tsv_generation_basic() {
        let rows = vec![
            vec!["1".to_string(), "'hello'".to_string(), "NULL".to_string()],
            vec!["2".to_string(), "'world'".to_string(), "42".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert_eq!(output, "1\thello\t\\N\n2\tworld\t42\n");
    }

    #[test]
    fn test_tsv_generation_escaping() {
        let rows = vec![vec!["3".to_string(), "'it''s escaped'".to_string()]];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert!(output.contains("it's escaped"));
    }

    #[test]
    fn test_tsv_null_handling() {
        let rows = vec![
            vec!["1".to_string(), "NULL".to_string(), "'text'".to_string()],
            vec!["2".to_string(), "'value'".to_string(), "NULL".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert!(output.contains("\\N"));
        assert!(output.contains("text"));
        assert!(output.contains("value"));
    }

    #[test]
    fn test_tsv_special_characters() {
        let rows = vec![
            vec!["1".to_string(), "'hello\\tworld'".to_string()],
            vec!["2".to_string(), "'line1\\nline2'".to_string()],
            vec!["3".to_string(), "'back\\\\slash'".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines[0], "1\thello\\tworld");
        assert_eq!(lines[1], "2\tline1\\nline2");
        assert_eq!(lines[2], "3\tback\\\\slash");
    }

    #[test]
    fn test_tsv_hex_literal_decoding() {
        let rows = vec![vec![
            "1".to_string(),
            "X'deadbeef'".to_string(),
            "'text'".to_string(),
        ]];
        let tsv = generate_tsv_buffer(&rows);
        assert_eq!(tsv, b"1\t\xde\xad\xbe\xef\ttext\n");
    }

    #[test]
    fn test_tsv_hex_literal_lowercase() {
        let rows = vec![vec!["1".to_string(), "x'cafe'".to_string()]];
        let tsv = generate_tsv_buffer(&rows);
        assert_eq!(tsv, b"1\t\xca\xfe\n");
    }

    #[test]
    fn test_tsv_hex_literal_with_special_bytes() {
        let rows = vec![vec!["1".to_string(), "X'090a5c00'".to_string()]];
        let tsv = generate_tsv_buffer(&rows);
        assert_eq!(tsv, b"1\t\\t\\n\\\\\\0\n");
    }

    #[test]
    fn test_decode_hex_literal_invalid() {
        let mut out = Vec::new();
        assert!(!decode_hex_into("hello", &mut out));
        assert!(!decode_hex_into("X'zz'", &mut out));
        assert!(!decode_hex_into("X'abc'", &mut out));
        assert!(!decode_hex_into("0xdead", &mut out));
        assert!(out.is_empty());
    }

    #[test]
    fn test_tsv_hex_decode_into_matches_previous() {
        // A row with a bytea hex literal, a NULL, a quoted string, and a bare value.
        let rows = vec![vec![
            "X'48656C6C6F'".to_string(), // "Hello"
            "NULL".to_string(),
            "'a\tb'".to_string(), // tab must be escaped
            "42".to_string(),
        ]];
        let buf = generate_tsv_buffer(&rows);
        // Expected bytes: Hello \t \N \t a\\tb \t 42 \n
        let expected = b"Hello\t\\N\ta\\tb\t42\n";
        assert_eq!(buf, expected);
    }

    /// BRIDGE TEST (Phase 3 Stage 5): the TSV produced DIRECTLY from `RowData`
    /// MUST be byte-for-byte identical to the legacy round-trip
    /// (`render_value` → `detect_bulk_insert_batch` → `generate_tsv_buffer`).
    #[test]
    fn test_tsv_from_rows_matches_sql_roundtrip() {
        use crate::destinations::bulk_insert::detect_bulk_insert_batch;
        use crate::destinations::dialect::SqlDialect;
        use crate::destinations::dialects::MySqlDialect;
        use bytes::Bytes;
        use pg_walstream::{ColumnValue, RowData};
        use std::collections::HashMap;

        // A set of INSERT rows covering Text, Null, Binary, boolean text, and
        // values that need TSV escaping (tab, newline, backslash, quote).
        let rows: Vec<RowData> = vec![
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("1")),
                ("name", ColumnValue::text("hello")),
                ("flag", ColumnValue::text("t")),
                ("note", ColumnValue::Null),
                (
                    "blob",
                    ColumnValue::Binary(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
                ),
            ]),
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("2")),
                ("name", ColumnValue::text("o'reilly")),
                ("flag", ColumnValue::text("f")),
                ("note", ColumnValue::text("tab\there")),
                (
                    "blob",
                    ColumnValue::Binary(Bytes::from_static(&[0x00, 0x09, 0x0a, 0x5c])),
                ),
            ]),
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("3")),
                ("name", ColumnValue::text("back\\slash\nnewline")),
                ("flag", ColumnValue::text("maybe")),
                ("note", ColumnValue::text("plain")),
                ("blob", ColumnValue::Binary(Bytes::from_static(&[0xff]))),
            ]),
        ];

        // Path A: legacy round-trip — render each row to INSERT SQL via the
        // MySQL dialect, reparse with detect_bulk_insert_batch, then build TSV.
        let mappings: HashMap<String, String> = HashMap::new();
        let ctx = crate::sql_renderer::RenderContext {
            dialect: &MySqlDialect,
            schema_mappings: &mappings,
        };
        let sql_stmts: Vec<String> = rows
            .iter()
            .map(|row| crate::sql_renderer::generate_insert_sql(&ctx, "public", "t1", row).unwrap())
            .collect();
        let parsed = detect_bulk_insert_batch(&sql_stmts)
            .expect("homogeneous INSERT batch should be detected");
        let tsv_legacy = generate_tsv_buffer(&parsed.rows);

        // Sanity: the legacy column list is the quoted identifiers.
        let mut expected_cols = String::new();
        MySqlDialect.quote_identifier("id", &mut expected_cols);
        assert_eq!(parsed.columns[0], expected_cols);

        // Path B: structured — build TSV directly from RowData. The columns
        // slice mirrors the row field order (already aligned), so output must
        // match the legacy round-trip byte-for-byte.
        let columns: Vec<Arc<str>> = ["id", "name", "flag", "note", "blob"]
            .iter()
            .map(|s| Arc::from(*s))
            .collect();
        let row_refs: Vec<&RowData> = rows.iter().collect();
        let tsv_structured = generate_tsv_buffer_from_rows(&columns, &row_refs);

        assert_eq!(
            tsv_structured, tsv_legacy,
            "structured TSV must equal legacy round-trip TSV byte-for-byte"
        );
    }

    /// Regression guard for PR #113: rows whose `(name, value)` field order
    /// differs from `columns` MUST be aligned BY NAME, not positionally. Under
    /// the old positional code, row2's fields (ordered name,id,flag) would land
    /// in the wrong TSV columns (name's value in the id slot, etc.). This test
    /// FAILS with positional rendering and PASSES with by-name lookup.
    #[test]
    fn test_tsv_from_rows_aligns_by_column_name() {
        use pg_walstream::{ColumnValue, RowData};

        let columns: Vec<Arc<str>> = ["id", "name", "flag"]
            .iter()
            .map(|s| Arc::from(*s))
            .collect();

        let rows: Vec<RowData> = vec![
            // Row 1: fields already in `columns` order.
            RowData::from_pairs(vec![
                ("id", ColumnValue::text("1")),
                ("name", ColumnValue::text("alice")),
                ("flag", ColumnValue::text("x")),
            ]),
            // Row 2: fields in a DIFFERENT order (name, id, flag).
            RowData::from_pairs(vec![
                ("name", ColumnValue::text("bob")),
                ("id", ColumnValue::text("2")),
                ("flag", ColumnValue::text("y")),
            ]),
        ];

        let row_refs: Vec<&RowData> = rows.iter().collect();
        let tsv = generate_tsv_buffer_from_rows(&columns, &row_refs);

        // Both rows must be aligned to columns [id, name, flag]. If rendered
        // positionally, row 2 would be "bob\t2\ty" (wrong) instead of "2\tbob\ty".
        let expected = b"1\talice\tx\n2\tbob\ty\n";
        assert_eq!(
            tsv, expected,
            "rows must be aligned to `columns` by name, not positionally"
        );
    }
}
