use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use mysql_async::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use super::coalescing::{coalesce_commands, QuoteStyle};

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
    /// Set for snapshot writers: LOAD DATA LOCAL skips duplicate-key rows and truncates or changes
    /// misfit values like IGNORE, and a snapshot must fail instead. The live path only warns.
    strict_load_data: bool,
    load_data_skip_warned: bool,
    load_data_fallback_warned: bool,
}

impl MySQLDestination {
    pub fn new() -> Self {
        Self {
            pool: None,
            infile_data: Arc::new(Mutex::new(None)),
            schema_mappings: HashMap::new(),
            max_allowed_packet: 67108864,
            load_data_available: false,
            strict_load_data: false,
            load_data_skip_warned: false,
            load_data_fallback_warned: false,
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

    fn begin_snapshot_writes(&mut self) {
        self.strict_load_data = true;
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

    async fn snapshot_table_is_empty(&mut self, schema: &str, table: &str) -> Result<bool> {
        let sql = self.snapshot_probe_sql(schema, table);
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL pool not initialized"))?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to get MySQL connection: {e}")))?;
        let row: Option<u8> = conn.query_first(sql).await.map_err(|e| {
            CdcError::generic(format!("MySQL snapshot emptiness probe failed: {e}"))
        })?;
        Ok(row.is_none())
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
    fn snapshot_probe_sql(&self, schema: &str, table: &str) -> String {
        let mut qualified = String::new();
        self.dialect().qualify_table(schema, table, &mut qualified);
        format!("SELECT 1 FROM {qualified} LIMIT 1")
    }

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
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("Pool not initialized"))?;

        let tsv_data = generate_tsv_buffer(rows);
        let row_count = rows.len();

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

        let load_sql = load_data_sql(table, columns);

        *self.infile_data.lock().unwrap() = Some(bytes::Bytes::from(tsv_data));

        let result = {
            let _guard = InfileDataGuard(self.infile_data.clone());
            tx.query_drop(&load_sql).await
        };

        if let Err(e) = result {
            if self.load_data_fallback_warned {
                debug!("LOAD DATA LOCAL INFILE into {table} failed, falling back to multi-value INSERT: {e}");
            } else {
                self.load_data_fallback_warned = true;
                warn!(
                    "LOAD DATA LOCAL INFILE into {table} failed, falling back to multi-value \
                     INSERT (later failures on this handler are logged at debug level): {e}"
                );
            }
            let _ = tx.rollback().await;

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

        let affected = tx.affected_rows();
        let warnings = tx.get_warnings();
        let info = tx.info().into_owned();
        if let Some(problem) = load_data_mismatch(table, row_count, affected, warnings, &info) {
            if self.strict_load_data {
                let _ = tx.rollback().await;
                return Err(CdcError::generic(problem));
            }
            if !self.load_data_skip_warned {
                self.load_data_skip_warned = true;
                warn!("{problem} (further occurrences on this handler are logged at debug level)");
            } else {
                debug!("{problem}");
            }
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
            "LOAD DATA complete: {} of {} rows loaded into {}",
            affected, row_count, table
        );

        Ok(())
    }
}

/// LOCAL makes LOAD DATA behave like IGNORE: rows that violate a unique or primary key are
/// skipped, and values that do not fit their column (too long, out of range, invalid date) are
/// stored truncated or changed, all without an error. Only the affected-row count and the
/// warning count reveal it.
fn load_data_mismatch(
    table: &str,
    sent: usize,
    affected: u64,
    warnings: u16,
    info: &str,
) -> Option<String> {
    if affected == sent as u64 {
        return (warnings > 0).then(|| {
            format!(
                "LOAD DATA LOCAL INFILE into {table} loaded {sent} row(s) with {warnings} \
                 warning(s) ({info}): values that do not fit their column (too long, out of \
                 range, invalid date) are stored truncated or changed without an error"
            )
        });
    }
    Some(format!(
        "LOAD DATA LOCAL INFILE into {table} sent {sent} row(s) but the server inserted \
         {affected} ({info}): rows that violate a unique or primary key (including text keys \
         that collide under a case-insensitive collation) are skipped without an error; use \
         CDC_SNAPSHOT_TARGET_POLICY=append only into tables that are empty or hold no \
         colliding keys"
    ))
}

/// The TSV is always UTF-8. Without `CHARACTER SET`, LOAD DATA reads it in the session's
/// `character_set_database`: the database default on a fresh connection but the server default on
/// a pooled one, so a latin1 server default turns `é` into `Ã©` with no warning.
fn load_data_sql(table: &str, columns: &[String]) -> String {
    format!(
        "LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE {table} CHARACTER SET utf8mb4 \
         FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' ({})",
        columns.join(", ")
    )
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

fn decode_hex_into(s: &str, out: &mut Vec<u8>) -> bool {
    if s.len() < 3 {
        return false;
    }
    if !(s.starts_with("X'") || s.starts_with("x'")) || !s.ends_with('\'') {
        return false;
    }
    let hex_str = &s[2..s.len() - 1];
    if hex_str.len() % 2 != 0 || !hex_str.bytes().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    // All chars validated as ASCII hex digits above, so decode with a fast bitwise
    // map instead of from_str_radix + UTF-8 slicing.
    let hex_val = |b: u8| -> u8 {
        match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => 0,
        }
    };
    let bytes = hex_str.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let byte = (hex_val(bytes[i]) << 4) | hex_val(bytes[i + 1]);
        tsv_escape_byte(byte, out);
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
    fn load_data_mismatch_reports_skipped_rows_only() {
        assert_eq!(load_data_mismatch("`d`.`t`", 3, 3, 0, "Records: 3"), None);

        let msg = load_data_mismatch(
            "`d`.`t`",
            3,
            2,
            1,
            "Records: 3  Deleted: 0  Skipped: 1  Warnings: 1",
        )
        .unwrap();
        assert!(msg.contains("`d`.`t`"), "{msg}");
        assert!(msg.contains("sent 3 row(s)"), "{msg}");
        assert!(msg.contains("inserted 2"), "{msg}");
        assert!(msg.contains("Skipped: 1"), "{msg}");
        assert!(msg.contains("append"), "{msg}");
        assert!(
            load_data_mismatch("t", 0, 1, 0, "").is_some(),
            "more is also wrong"
        );
    }

    #[test]
    fn load_data_mismatch_reports_warnings_when_every_row_was_inserted() {
        let msg = load_data_mismatch(
            "`d`.`t`",
            3,
            3,
            2,
            "Records: 3  Deleted: 0  Skipped: 0  Warnings: 2",
        )
        .unwrap();
        assert!(msg.contains("`d`.`t`"), "{msg}");
        assert!(msg.contains("2 warning(s)"), "{msg}");
        assert!(msg.contains("truncated"), "{msg}");
    }

    #[test]
    fn begin_snapshot_writes_makes_load_data_strict() {
        let mut destination = MySQLDestination::new();
        assert!(!destination.strict_load_data);
        destination.begin_snapshot_writes();
        assert!(destination.strict_load_data);
    }

    #[test]
    fn load_data_declares_the_utf8_file_charset() {
        assert_eq!(
            load_data_sql("`d`.`t`", &["`a`".to_string(), "`b`".to_string()]),
            "LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `d`.`t` CHARACTER SET utf8mb4 \
             FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' (`a`, `b`)"
        );
    }

    #[test]
    fn snapshot_probe_uses_limit_and_backticks() {
        let destination = MySQLDestination::new();
        assert_eq!(
            destination.snapshot_probe_sql("cdc_db", "Snap`Mixed"),
            "SELECT 1 FROM `cdc_db`.`Snap``Mixed` LIMIT 1"
        );
        assert_eq!(
            destination.snapshot_probe_sql("cdc_db", "x`; DROP TABLE users; --"),
            "SELECT 1 FROM `cdc_db`.`x``; DROP TABLE users; --` LIMIT 1"
        );
    }

    #[tokio::test]
    async fn snapshot_probe_without_a_pool_is_an_error() {
        let mut destination = MySQLDestination::new();
        assert!(destination
            .snapshot_table_is_empty("cdc_db", "t")
            .await
            .is_err());
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
}
