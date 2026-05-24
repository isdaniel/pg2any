use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use sqlx::MySqlPool;
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
    pool: Option<MySqlPool>,
    bulk_pool: Option<mysql_async::Pool>,
    infile_data: Arc<Mutex<Option<bytes::Bytes>>>,
    schema_mappings: HashMap<String, String>,
    max_allowed_packet: u64,
    load_data_available: bool,
}

impl MySQLDestination {
    pub fn new() -> Self {
        Self {
            pool: None,
            bulk_pool: None,
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
        let pool = MySqlPool::connect(connection_string).await?;

        let row: (String, String) = sqlx::query_as("SHOW VARIABLES LIKE 'max_allowed_packet'")
            .fetch_one(&pool)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to query max_allowed_packet: {e}")))?;

        self.max_allowed_packet = row.1.parse::<u64>().unwrap_or(67108864);

        info!(
            "MySQL max_allowed_packet: {} bytes ({:.2} MB)",
            self.max_allowed_packet,
            self.max_allowed_packet as f64 / 1_048_576.0
        );

        self.pool = Some(pool);

        let opts = mysql_async::Opts::from_url(connection_string).map_err(|e| {
            CdcError::generic(format!("Failed to parse MySQL URL for bulk pool: {e}"))
        })?;

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

        let bulk_pool = mysql_async::Pool::new(opts);
        self.bulk_pool = Some(bulk_pool);

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

        super::common::execute_sqlx_batch_with_hook(pool, &coalesced, pre_commit_hook, "MySQL")
            .await
    }

    fn supports_bulk_insert(&self) -> bool {
        self.load_data_available
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

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;

        if let Some(pool) = self.bulk_pool.take() {
            pool.disconnect().await.map_err(|e| {
                CdcError::generic(format!("Failed to disconnect mysql_async pool: {e}"))
            })?;
        }

        info!("MySQL connection closed successfully");
        Ok(())
    }
}

impl MySQLDestination {
    async fn check_load_data_available(&self) -> Result<bool> {
        use mysql_async::prelude::*;

        let pool = self
            .bulk_pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("Bulk pool not initialized"))?;

        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to get bulk connection: {e}")))?;

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
        use mysql_async::prelude::*;

        let pool = self
            .bulk_pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("Bulk pool not initialized"))?;

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
            } else if let Some(decoded) = decode_hex_literal(trimmed) {
                tsv_escape_raw(&decoded, &mut buf);
            } else {
                tsv_escape_raw(trimmed.as_bytes(), &mut buf);
            }
        }
        buf.push(b'\n');
    }

    buf
}

/// Decode MySQL hex literal (X'aabb' or x'aabb') into raw bytes.
fn decode_hex_literal(s: &str) -> Option<Vec<u8>> {
    if s.len() < 3 {
        return None;
    }
    if !(s.starts_with("X'") || s.starts_with("x'")) || !s.ends_with('\'') {
        return None;
    }
    let hex_str = &s[2..s.len() - 1];
    if hex_str.len() % 2 != 0 || !hex_str.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes: Vec<u8> = (0..hex_str.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).unwrap())
        .collect();
    Some(bytes)
}

/// Unescape MySQL SQL literal content and re-encode for TSV format.
/// Input is the content between single quotes from a MySQL SQL literal.
/// MySQL SQL literals use backslash escaping: \\ → \, \n → newline, \t → tab, etc.
/// TSV format uses: \\ → \, \n → newline, \t → tab, \N → NULL.
fn tsv_escape_string(s: &str, buf: &mut Vec<u8>) {
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                }
                buf.push(b'\'');
            }
            '\\' => {
                match chars.next() {
                    Some('\\') => buf.extend_from_slice(b"\\\\"), // SQL \\ → literal \ → TSV \\
                    Some('n') => buf.extend_from_slice(b"\\n"),   // SQL \n → newline → TSV \n
                    Some('t') => buf.extend_from_slice(b"\\t"),   // SQL \t → tab → TSV \t
                    Some('r') => buf.extend_from_slice(b"\\r"),   // SQL \r → CR → TSV \r
                    Some('0') => buf.extend_from_slice(b"\\0"),   // SQL \0 → null → TSV \0
                    Some('b') => buf.extend_from_slice(b"\\b"),   // SQL \b → backspace → TSV \b
                    Some('Z') => buf.extend_from_slice(b"\\Z"),   // SQL \Z → Control-Z → TSV \Z
                    Some(other) => {
                        // MySQL: \x → x for any unrecognized escape
                        let mut bytes = [0u8; 4];
                        buf.extend_from_slice(other.encode_utf8(&mut bytes).as_bytes());
                    }
                    None => buf.extend_from_slice(b"\\\\"), // trailing backslash
                }
            }
            '\t' => buf.extend_from_slice(b"\\t"),
            '\n' => buf.extend_from_slice(b"\\n"),
            '\r' => buf.extend_from_slice(b"\\r"),
            '\0' => buf.extend_from_slice(b"\\0"),
            _ => {
                let mut bytes = [0u8; 4];
                buf.extend_from_slice(ch.encode_utf8(&mut bytes).as_bytes());
            }
        }
    }
}

fn tsv_escape_raw(data: &[u8], buf: &mut Vec<u8>) {
    for &b in data {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            0 => buf.extend_from_slice(b"\\0"),
            _ => buf.push(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
        assert!(destination.bulk_pool.is_none());
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
            // SQL literal '\\' means one backslash; TSV should produce \\
            vec!["3".to_string(), "'back\\\\slash'".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        // SQL \t → literal tab → TSV \t
        assert_eq!(lines[0], "1\thello\\tworld");
        // SQL \n → literal newline → TSV \n
        assert_eq!(lines[1], "2\tline1\\nline2");
        // SQL \\ → literal backslash → TSV \\
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
        // Hex containing tab, newline, backslash bytes should be TSV-escaped
        let rows = vec![vec!["1".to_string(), "X'090a5c00'".to_string()]];
        let tsv = generate_tsv_buffer(&rows);
        assert_eq!(tsv, b"1\t\\t\\n\\\\\\0\n");
    }

    #[test]
    fn test_decode_hex_literal_invalid() {
        assert!(decode_hex_literal("hello").is_none());
        assert!(decode_hex_literal("X'zz'").is_none());
        assert!(decode_hex_literal("X'abc'").is_none()); // odd length
        assert!(decode_hex_literal("0xdead").is_none()); // 0x prefix is SQL Server style
    }
}
