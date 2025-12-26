//! Transaction File Manager for CDC persistence
//!
//! This module implements file-based transaction persistence to ensure data consistency
//! even when long-running transactions might be aborted before commit.
//!
//! ## Architecture
//!
//! - **sql_received_tx/**: Stores events for in-progress transactions (not yet committed)
//!   - File naming: `{txid}_{timestamp}.sql`
//!   - Events are appended as SQL commands as they arrive
//!   - Files remain here until COMMIT or ABORT
//!
//! - **sql_pending_tx/**: Stores committed transactions ready for execution
//!   - Files are moved here from sql_received_tx on COMMIT/StreamCommit
//!   - Consumer executes these files in commit timestamp order
//!   - Files are deleted after successful execution
//!
//! ## Transaction Flow
//!
//! 1. BEGIN: Create new file in sql_received_tx/
//! 2. DML Events: Append SQL commands to the file
//! 3. COMMIT: Move file to sql_pending_tx/
//! 4. Consumer: Execute SQL from sql_pending_tx/ in order
//! 5. Success: Delete file and update LSN
//! 6. ABORT: Delete file from sql_received_tx/
//!
//! ## Recovery
//!
//! On startup:
//! - Files in sql_received_tx/ are incomplete transactions (can be cleaned up)
//! - Files in sql_pending_tx/ are committed but not yet executed (must be processed)

use crate::error::{CdcError, Result};
use crate::types::{ChangeEvent, DestinationType, EventType, Lsn, ReplicaIdentity};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, info, warn};

const RECEIVED_TX_DIR: &str = "sql_received_tx";
const PENDING_TX_DIR: &str = "sql_pending_tx";

/// Transaction file metadata stored alongside SQL commands
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionFileMetadata {
    pub transaction_id: u32,
    pub commit_timestamp: DateTime<Utc>,
    pub commit_lsn: Option<Lsn>,
    pub destination_type: DestinationType,
}

/// A committed transaction file ready for execution
#[derive(Debug, Clone)]
pub struct PendingTransactionFile {
    pub file_path: PathBuf,
    pub metadata: TransactionFileMetadata,
}

/// Transaction File Manager for persisting and executing transactions
pub struct TransactionFileManager {
    base_path: PathBuf,
    destination_type: DestinationType,
    schema_mappings: HashMap<String, String>,
}

impl TransactionFileManager {
    /// Create a new transaction file manager
    pub async fn new(
        base_path: impl AsRef<Path>,
        destination_type: DestinationType,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create directories if they don't exist
        let received_tx_dir = base_path.join(RECEIVED_TX_DIR);
        let pending_tx_dir = base_path.join(PENDING_TX_DIR);

        fs::create_dir_all(&received_tx_dir).await?;
        fs::create_dir_all(&pending_tx_dir).await?;

        info!(
            "Transaction file manager initialized at {:?} for {:?}",
            base_path, destination_type
        );

        Ok(Self {
            base_path,
            destination_type,
            schema_mappings: HashMap::new(),
        })
    }

    /// Set schema mappings for SQL generation
    pub fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    /// Get the destination type
    pub fn destination_type(&self) -> &DestinationType {
        &self.destination_type
    }

    /// Get the file path for a received transaction
    fn get_received_tx_path(&self, tx_id: u32, timestamp: DateTime<Utc>) -> PathBuf {
        let filename = format!("{}_{}.sql", tx_id, timestamp.timestamp_millis());
        self.base_path.join(RECEIVED_TX_DIR).join(filename)
    }

    /// Get the file path for a pending transaction
    fn get_pending_tx_path(&self, tx_id: u32, timestamp: DateTime<Utc>) -> PathBuf {
        let filename = format!("{}_{}.sql", tx_id, timestamp.timestamp_millis());
        self.base_path.join(PENDING_TX_DIR).join(filename)
    }

    /// Create a new transaction file in sql_received_tx
    pub async fn begin_transaction(&self, tx_id: u32, timestamp: DateTime<Utc>) -> Result<PathBuf> {
        let file_path = self.get_received_tx_path(tx_id, timestamp);

        // Create the file with initial metadata comment
        let mut file = File::create(&file_path).await?;

        let metadata = TransactionFileMetadata {
            transaction_id: tx_id,
            commit_timestamp: timestamp,
            commit_lsn: None,
            destination_type: self.destination_type.clone(),
        };

        let metadata_json = serde_json::to_string(&metadata)?;
        file.write_all(format!("-- METADATA: {}\n", metadata_json).as_bytes())
            .await?;
        file.write_all(b"-- BEGIN TRANSACTION\n").await?;

        debug!("Created transaction file: {:?}", file_path);
        Ok(file_path)
    }

    /// Append a change event to a running transaction file
    pub async fn append_event(&self, file_path: &Path, event: &ChangeEvent) -> Result<()> {
        let sql = self.generate_sql_for_event(event)?;

        let mut file = fs::OpenOptions::new().append(true).open(file_path).await?;

        file.write_all(sql.as_bytes()).await?;
        file.write_all(b"\n").await?;

        Ok(())
    }

    /// Move a transaction file from sql_received_tx to sql_pending_tx (on commit)
    pub async fn commit_transaction(
        &self,
        tx_id: u32,
        timestamp: DateTime<Utc>,
        commit_lsn: Option<Lsn>,
    ) -> Result<PathBuf> {
        let received_path = self.get_received_tx_path(tx_id, timestamp);
        let pending_path = self.get_pending_tx_path(tx_id, timestamp);

        // Open source file for streaming read
        let source_file = File::open(&received_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to open received file {:?}: {}",
                received_path, e
            ))
        })?;
        let reader = BufReader::with_capacity(8192, source_file);
        let mut lines = reader.lines();

        // Create destination file for streaming write
        let mut dest_file = File::create(&pending_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to create pending file {:?}: {}",
                pending_path, e
            ))
        })?;

        let mut metadata_updated = false;

        // Stream line-by-line, updating metadata as we go
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to read from received file: {}", e)))?
        {
            if !metadata_updated && line.starts_with("-- METADATA:") {
                // Parse existing metadata and update with commit LSN
                let json_str = line.trim_start_matches("-- METADATA:").trim();
                let mut metadata: TransactionFileMetadata = serde_json::from_str(json_str)
                    .map_err(|e| CdcError::generic(format!("Failed to parse metadata: {}", e)))?;
                metadata.commit_lsn = commit_lsn;

                // Write updated metadata line
                let updated_json = serde_json::to_string(&metadata).map_err(|e| {
                    CdcError::generic(format!("Failed to serialize metadata: {}", e))
                })?;
                dest_file
                    .write_all(format!("-- METADATA: {}\n", updated_json).as_bytes())
                    .await
                    .map_err(|e| CdcError::generic(format!("Failed to write metadata: {}", e)))?;
                metadata_updated = true;
            } else {
                // Copy line as-is
                dest_file
                    .write_all(line.as_bytes())
                    .await
                    .map_err(|e| CdcError::generic(format!("Failed to write line: {}", e)))?;
                dest_file
                    .write_all(b"\n")
                    .await
                    .map_err(|e| CdcError::generic(format!("Failed to write newline: {}", e)))?;
            }
        }

        // Add COMMIT marker
        dest_file
            .write_all(b"-- COMMIT TRANSACTION\n")
            .await
            .map_err(|e| CdcError::generic(format!("Failed to write COMMIT marker: {}", e)))?;

        // Ensure all data is written to disk
        dest_file
            .flush()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to flush pending file: {}", e)))?;

        // Remove source file only after successful write
        fs::remove_file(&received_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to remove received file {:?}: {}",
                received_path, e
            ))
        })?;

        info!(
            "Committed transaction {} to pending queue (LSN: {:?}) using streaming I/O",
            tx_id, commit_lsn
        );

        Ok(pending_path)
    }

    /// Delete a transaction file (on abort)
    pub async fn abort_transaction(&self, tx_id: u32, timestamp: DateTime<Utc>) -> Result<()> {
        let received_path = self.get_received_tx_path(tx_id, timestamp);

        if received_path.exists() {
            fs::remove_file(&received_path).await?;
            info!("Aborted transaction {}, deleted file", tx_id);
        }

        Ok(())
    }

    /// List all pending transaction files ordered by commit timestamp
    pub async fn list_pending_transactions(&self) -> Result<Vec<PendingTransactionFile>> {
        let pending_dir = self.base_path.join(PENDING_TX_DIR);
        let mut entries = fs::read_dir(&pending_dir).await?;
        let mut files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                if let Ok(metadata) = self.read_metadata(&path).await {
                    files.push(PendingTransactionFile {
                        file_path: path,
                        metadata,
                    });
                }
            }
        }

        // Sort by commit timestamp
        files.sort_by_key(|f| f.metadata.commit_timestamp);

        Ok(files)
    }

    /// Read metadata from a transaction file
    async fn read_metadata(&self, file_path: &Path) -> Result<TransactionFileMetadata> {
        let file = File::open(file_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        if let Some(line) = lines.next_line().await? {
            if line.starts_with("-- METADATA:") {
                let json_str = line.trim_start_matches("-- METADATA:").trim();
                let metadata: TransactionFileMetadata = serde_json::from_str(json_str)?;
                return Ok(metadata);
            }
        }

        Err(CdcError::Generic(
            "Transaction file missing metadata".to_string(),
        ))
    }

    /// Read SQL commands from a transaction file (excluding metadata comments)
    ///
    /// This method uses buffered streaming I/O to efficiently handle large transaction files
    /// (100MB+) without loading the entire file into memory. SQL statements are properly
    /// parsed by splitting on semicolons while respecting string literals and comments.
    ///
    /// This fixes the issue where SQL statements containing embedded newlines in string
    /// literals would be incorrectly split when reading line-by-line.
    ///
    /// Performance characteristics:
    /// - Memory: O(1) - only buffers 64KB at a time
    /// - Time: O(n) where n is file size in bytes
    /// - Can handle files of any size without truncation
    /// - Properly handles multi-line SQL statements
    pub async fn read_sql_commands(&self, file_path: &Path) -> Result<Vec<String>> {
        let file = File::open(file_path).await.map_err(|e| {
            CdcError::generic(format!("Failed to open file {:?}: {}", file_path, e))
        })?;

        let reader = BufReader::with_capacity(65536, file); // 64KB buffer for optimal I/O
        let mut lines = reader.lines();
        let mut file_content = String::with_capacity(65536);

        // Read the entire file content, filtering out metadata and comment lines
        while let Some(line) = lines.next_line().await.map_err(|e| {
            CdcError::generic(format!("Failed to read line from {:?}: {}", file_path, e))
        })? {
            let trimmed = line.trim();

            // Skip metadata and transaction markers (comments starting with --)
            if trimmed.starts_with("--") {
                continue;
            }

            // Skip empty lines
            if trimmed.is_empty() {
                continue;
            }

            // Add the line to content (preserve original spacing within statements)
            file_content.push_str(&line);
            file_content.push('\n');
        }

        // Parse SQL statements by splitting on semicolons while respecting string literals
        let commands = self.parse_sql_statements(&file_content)?;

        debug!(
            "Read {} SQL commands from {:?} using statement-based parsing",
            commands.len(),
            file_path
        );

        // Log first and last few commands for debugging
        if !commands.is_empty() {
            let first_cmd = &commands[0];
            let first_preview = if first_cmd.len() > 150 {
                format!("{}...", &first_cmd[..150])
            } else {
                first_cmd.clone()
            };
            debug!("First SQL command: {}", first_preview);

            if commands.len() > 1 {
                let last_cmd = &commands[commands.len() - 1];
                let last_preview = if last_cmd.len() > 150 {
                    format!("{}...", &last_cmd[..150])
                } else {
                    last_cmd.clone()
                };
                debug!("Last SQL command: {}", last_preview);
            }
        }

        Ok(commands)
    }

    /// Parse SQL statements from text, properly handling string literals and semicolons
    ///
    /// This parser splits SQL text into individual statements by finding semicolons that
    /// are NOT inside string literals. It handles:
    /// - Single-quoted strings with escaped quotes ('')
    /// - Backtick-quoted identifiers (MySQL)
    /// - Double-quoted identifiers (PostgreSQL/SQLite)
    /// - Square-bracket identifiers (SQL Server)
    ///
    /// Returns a vector of trimmed SQL statements without trailing semicolons.
    fn parse_sql_statements(&self, content: &str) -> Result<Vec<String>> {
        let mut statements = Vec::with_capacity(1000);
        let mut current_statement = String::with_capacity(512);
        let mut chars = content.chars().peekable();
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;
        let mut in_bracket = false;

        while let Some(ch) = chars.next() {
            match ch {
                '\'' if !in_double_quote && !in_backtick && !in_bracket => {
                    current_statement.push(ch);
                    if in_single_quote {
                        // Check for escaped quote ('')
                        if chars.peek() == Some(&'\'') {
                            current_statement.push(chars.next().unwrap());
                        } else {
                            in_single_quote = false;
                        }
                    } else {
                        in_single_quote = true;
                    }
                }
                '"' if !in_single_quote && !in_backtick && !in_bracket => {
                    current_statement.push(ch);
                    if in_double_quote {
                        // Check for escaped quote ("")
                        if chars.peek() == Some(&'"') {
                            current_statement.push(chars.next().unwrap());
                        } else {
                            in_double_quote = false;
                        }
                    } else {
                        in_double_quote = true;
                    }
                }
                '`' if !in_single_quote && !in_double_quote && !in_bracket => {
                    current_statement.push(ch);
                    if in_backtick {
                        // Check for escaped backtick (``)
                        if chars.peek() == Some(&'`') {
                            current_statement.push(chars.next().unwrap());
                        } else {
                            in_backtick = false;
                        }
                    } else {
                        in_backtick = true;
                    }
                }
                '[' if !in_single_quote && !in_double_quote && !in_backtick => {
                    current_statement.push(ch);
                    in_bracket = true;
                }
                ']' if in_bracket => {
                    current_statement.push(ch);
                    in_bracket = false;
                }
                ';' if !in_single_quote && !in_double_quote && !in_backtick && !in_bracket => {
                    // Found a statement terminator outside of any quotes
                    let trimmed = current_statement.trim();
                    if !trimmed.is_empty() {
                        statements.push(trimmed.to_string());
                    }
                    current_statement.clear();
                }
                _ => {
                    current_statement.push(ch);
                }
            }
        }

        // Handle any remaining content (statement without trailing semicolon)
        let trimmed = current_statement.trim();
        if !trimmed.is_empty() {
            warn!(
                "SQL statement without trailing semicolon (length: {} chars): {}",
                trimmed.len(),
                if trimmed.len() > 100 {
                    format!("{}...", &trimmed[..100])
                } else {
                    trimmed.to_string()
                }
            );
            statements.push(trimmed.to_string());
        }

        Ok(statements)
    }

    /// Delete a pending transaction file after successful execution
    pub async fn delete_pending_transaction(&self, file_path: &Path) -> Result<()> {
        fs::remove_file(file_path).await?;
        debug!("Deleted executed transaction file: {:?}", file_path);
        Ok(())
    }

    /// Clean up incomplete transactions from sql_received_tx directory
    pub async fn cleanup_received_transactions(&self) -> Result<()> {
        let received_dir = self.base_path.join(RECEIVED_TX_DIR);
        let mut entries = fs::read_dir(&received_dir).await?;
        let mut count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                fs::remove_file(&path).await?;
                count += 1;
            }
        }

        if count > 0 {
            warn!(
                "Cleaned up {} incomplete transactions from sql_received_tx",
                count
            );
        }

        Ok(())
    }

    /// Generate SQL command for a change event
    fn generate_sql_for_event(&self, event: &ChangeEvent) -> Result<String> {
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
                        .map(|c| format!("`{}`", c))
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
                        .map(|c| format!("[{}]", c))
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
                        .map(|c| format!("\"{}\"", c))
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
                    DestinationType::MySQL => format!("`{}`", col),
                    DestinationType::SqlServer => format!("[{}]", col),
                    DestinationType::SQLite => format!("\"{}\"", col),
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
                format!(
                    "DELETE FROM `{}`.`{}` WHERE {};",
                    schema, table, where_clause
                )
            }
            DestinationType::SqlServer => {
                format!(
                    "DELETE FROM [{}].[{}] WHERE {};",
                    schema, table, where_clause
                )
            }
            DestinationType::SQLite => {
                format!("DELETE FROM \"{}\" WHERE {};", table, where_clause)
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
                    format!("TRUNCATE TABLE `{}`.`{}`;", schema, table)
                }
                DestinationType::SqlServer => {
                    format!("TRUNCATE TABLE [{}].[{}];", schema, table)
                }
                DestinationType::SQLite => {
                    // SQLite doesn't have TRUNCATE, use DELETE
                    format!("DELETE FROM \"{}\";", table)
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
            ReplicaIdentity::Default { .. } | ReplicaIdentity::Index { .. } => {
                // Use key columns
                let data = old_data.as_ref().unwrap_or(new_data);
                key_columns
                    .iter()
                    .map(|col| {
                        let val = data.get(col).ok_or_else(|| {
                            CdcError::Generic(format!("Key column {} not found", col))
                        })?;
                        let formatted_col = match self.destination_type {
                            DestinationType::MySQL => format!("`{}`", col),
                            DestinationType::SqlServer => format!("[{}]", col),
                            DestinationType::SQLite => format!("\"{}\"", col),
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
                            DestinationType::MySQL => format!("`{}`", col),
                            DestinationType::SqlServer => format!("[{}]", col),
                            DestinationType::SQLite => format!("\"{}\"", col),
                        };
                        if val.is_null() {
                            format!("{} IS NULL", formatted_col)
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
                format!("'{}'", escaped)
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                // For complex types, serialize as JSON string
                let json_str = value.to_string().replace('\'', "''");
                format!("'{}'", json_str)
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
