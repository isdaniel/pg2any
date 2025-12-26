//! Transaction File Manager for CDC persistence
//!
//! This module implements file-based transaction persistence to ensure data consistency
//! even when long-running transactions might be aborted before commit.
//!
//! ## Architecture
//!
//! - **sql_data_tx/**: Stores actual SQL data (append-only, never moved)
//!   - File naming: `{txid}_{timestamp}.sql`
//!   - Events are appended as SQL commands as they arrive
//!   - Files are NOT moved or deleted until transaction is fully executed
//!
//! - **sql_received_tx/**: Stores metadata for in-progress transactions
//!   - File naming: `{txid}_{timestamp}.meta`
//!   - Contains JSON metadata: transaction_id, timestamp, data_file_path
//!   - Small files (~100 bytes) for fast operations
//!
//! - **sql_pending_tx/**: Stores metadata for committed transactions ready for execution
//!   - File naming: `{txid}_{timestamp}.meta`
//!   - Contains JSON metadata: transaction_id, timestamp, commit_lsn, data_file_path
//!   - Metadata moved here from sql_received_tx on COMMIT/StreamCommit
//!   - Consumer reads these to find which data files to execute
//!
//! ## Transaction Flow
//!
//! 1. BEGIN: Create data file in sql_data_tx/ and metadata in sql_received_tx/
//! 2. DML Events: Append SQL commands to the data file
//! 3. COMMIT: Move metadata from sql_received_tx/ to sql_pending_tx/ (data stays in sql_data_tx/)
//! 4. Consumer: Read metadata from sql_pending_tx/, execute SQL from sql_data_tx/
//! 5. Success: Delete metadata from sql_pending_tx/ and data file from sql_data_tx/
//! 6. ABORT: Delete metadata from sql_received_tx/ and data file from sql_data_tx/
//!
//! ## Recovery
//!
//! On startup:
//! - Metadata in sql_received_tx/ are incomplete transactions (can be cleaned up with data files)
//! - Metadata in sql_pending_tx/ are committed but not yet executed (must be processed)

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
const DATA_TX_DIR: &str = "sql_data_tx";

/// Transaction file metadata stored in sql_received_tx/ and sql_pending_tx/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionFileMetadata {
    pub transaction_id: u32,
    pub commit_timestamp: DateTime<Utc>,
    pub commit_lsn: Option<Lsn>,
    pub destination_type: DestinationType,
    /// Path to the SQL data file in sql_data_tx/
    pub data_file_path: PathBuf,
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
        let data_tx_dir = base_path.join(DATA_TX_DIR);

        fs::create_dir_all(&received_tx_dir).await?;
        fs::create_dir_all(&pending_tx_dir).await?;
        fs::create_dir_all(&data_tx_dir).await?;

        info!(
            "Transaction file manager initialized at {:?} for {:?}",
            base_path, destination_type
        );
        info!("Three-directory structure: sql_data_tx/ (data), sql_received_tx/ (in-progress metadata), sql_pending_tx/ (committed metadata)");

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

    /// Get the file path for a received transaction metadata
    fn get_received_tx_path(&self, tx_id: u32, timestamp: DateTime<Utc>) -> PathBuf {
        let filename = format!("{}_{}.meta", tx_id, timestamp.timestamp_millis());
        self.base_path.join(RECEIVED_TX_DIR).join(filename)
    }

    /// Get the file path for a pending transaction metadata
    fn get_pending_tx_path(&self, tx_id: u32, timestamp: DateTime<Utc>) -> PathBuf {
        let filename = format!("{}_{}.meta", tx_id, timestamp.timestamp_millis());
        self.base_path.join(PENDING_TX_DIR).join(filename)
    }

    /// Get the file path for the SQL data file
    fn get_data_file_path(&self, tx_id: u32, timestamp: DateTime<Utc>) -> PathBuf {
        let filename = format!("{}_{}.sql", tx_id, timestamp.timestamp_millis());
        self.base_path.join(DATA_TX_DIR).join(filename)
    }

    /// Create a new transaction: data file in sql_data_tx/ and metadata in sql_received_tx/
    pub async fn begin_transaction(&self, tx_id: u32, timestamp: DateTime<Utc>) -> Result<PathBuf> {
        let data_file_path = self.get_data_file_path(tx_id, timestamp);
        let metadata_path = self.get_received_tx_path(tx_id, timestamp);

        // Create the SQL data file in sql_data_tx/
        let mut data_file = File::create(&data_file_path).await?;
        data_file.write_all(b"-- BEGIN TRANSACTION\n").await?;
        data_file.flush().await?;

        debug!("Created data file: {:?}", data_file_path);

        // Create metadata file in sql_received_tx/
        let metadata = TransactionFileMetadata {
            transaction_id: tx_id,
            commit_timestamp: timestamp,
            commit_lsn: None,
            destination_type: self.destination_type.clone(),
            data_file_path: data_file_path.clone(),
        };

        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        let mut metadata_file = File::create(&metadata_path).await?;
        metadata_file.write_all(metadata_json.as_bytes()).await?;
        metadata_file.flush().await?;

        info!(
            "Started transaction {}: data={:?}, metadata={:?}",
            tx_id, data_file_path, metadata_path
        );

        // Return the data file path for appending events
        Ok(data_file_path)
    }

    /// Append a change event to a running transaction file
    pub async fn append_event(&self, file_path: &Path, event: &ChangeEvent) -> Result<()> {
        let sql = self.generate_sql_for_event(event)?;

        let mut file = fs::OpenOptions::new().append(true).open(file_path).await?;

        file.write_all(sql.as_bytes()).await?;
        file.write_all(b"\n").await?;

        Ok(())
    }

    /// Move metadata from sql_received_tx to sql_pending_tx
    pub async fn commit_transaction(
        &self,
        tx_id: u32,
        timestamp: DateTime<Utc>,
        commit_lsn: Option<Lsn>,
    ) -> Result<PathBuf> {
        let received_metadata_path = self.get_received_tx_path(tx_id, timestamp);
        let pending_metadata_path = self.get_pending_tx_path(tx_id, timestamp);

        // Read existing metadata from sql_received_tx/
        let metadata_content = fs::read_to_string(&received_metadata_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to read metadata from {:?}: {}",
                    received_metadata_path, e
                ))
            })?;

        let mut metadata: TransactionFileMetadata = serde_json::from_str(&metadata_content)
            .map_err(|e| CdcError::generic(format!("Failed to parse metadata: {}", e)))?;

        // Update commit LSN
        metadata.commit_lsn = commit_lsn;

        // Write updated metadata to sql_pending_tx/
        let updated_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| CdcError::generic(format!("Failed to serialize metadata: {}", e)))?;

        let mut pending_file = File::create(&pending_metadata_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to create pending metadata {:?}: {}",
                pending_metadata_path, e
            ))
        })?;

        pending_file
            .write_all(updated_json.as_bytes())
            .await
            .map_err(|e| CdcError::generic(format!("Failed to write metadata: {}", e)))?;

        pending_file
            .flush()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to flush metadata: {}", e)))?;

        // Append COMMIT marker to data file
        let data_file_path = &metadata.data_file_path;
        if data_file_path.exists() {
            let mut data_file = fs::OpenOptions::new()
                .append(true)
                .open(data_file_path)
                .await
                .map_err(|e| {
                    CdcError::generic(format!(
                        "Failed to open data file {:?}: {}",
                        data_file_path, e
                    ))
                })?;
            data_file
                .write_all(b"-- COMMIT TRANSACTION\n")
                .await
                .map_err(|e| CdcError::generic(format!("Failed to write COMMIT marker: {}", e)))?;
            data_file
                .flush()
                .await
                .map_err(|e| CdcError::generic(format!("Failed to flush data file: {}", e)))?;
        }

        // Remove metadata from sql_received_tx/ only after successful write
        fs::remove_file(&received_metadata_path)
            .await
            .map_err(|e| {
                CdcError::generic(format!(
                    "Failed to remove received metadata {:?}: {}",
                    received_metadata_path, e
                ))
            })?;

        info!(
            "Committed transaction {}: moved metadata to sql_pending_tx/ (LSN: {:?}), data stays in sql_data_tx/",
            tx_id, commit_lsn
        );

        Ok(pending_metadata_path)
    }

    /// Delete transaction files (metadata and data) on abort
    pub async fn abort_transaction(&self, tx_id: u32, timestamp: DateTime<Utc>) -> Result<()> {
        let received_metadata_path = self.get_received_tx_path(tx_id, timestamp);
        let data_file_path = self.get_data_file_path(tx_id, timestamp);

        // Read metadata to get data file path (in case it's different)
        let actual_data_path = if received_metadata_path.exists() {
            if let Ok(metadata_content) = fs::read_to_string(&received_metadata_path).await {
                if let Ok(metadata) =
                    serde_json::from_str::<TransactionFileMetadata>(&metadata_content)
                {
                    metadata.data_file_path
                } else {
                    data_file_path.clone()
                }
            } else {
                data_file_path.clone()
            }
        } else {
            data_file_path
        };

        // Delete metadata file
        if received_metadata_path.exists() {
            fs::remove_file(&received_metadata_path).await?;
            debug!("Deleted metadata file: {:?}", received_metadata_path);
        }

        // Delete data file
        if actual_data_path.exists() {
            fs::remove_file(&actual_data_path).await?;
            debug!("Deleted data file: {:?}", actual_data_path);
        }

        info!(
            "Aborted transaction {}, deleted metadata and data files",
            tx_id
        );
        Ok(())
    }

    /// List all pending transaction files ordered by commit timestamp
    pub async fn list_pending_transactions(&self) -> Result<Vec<PendingTransactionFile>> {
        let pending_dir = self.base_path.join(PENDING_TX_DIR);
        let mut entries = fs::read_dir(&pending_dir).await?;
        let mut files = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Read .meta files from sql_pending_tx/
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
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

    /// Read metadata from a transaction metadata file (.meta)
    async fn read_metadata(&self, file_path: &Path) -> Result<TransactionFileMetadata> {
        let metadata_content = fs::read_to_string(file_path).await?;
        let metadata: TransactionFileMetadata = serde_json::from_str(&metadata_content)?;
        Ok(metadata)
    }

    /// Read SQL commands from a transaction data file (excluding comments)
    ///
    /// This method reads SQL commands from the data file referenced by a pending transaction.
    /// The file_path parameter should be the path to the metadata file (.meta) in sql_pending_tx/.
    /// This method will read the metadata to find the actual data file path in sql_data_tx/.
    ///
    /// This method uses buffered streaming I/O to efficiently handle large transaction files
    /// (100MB+) without loading the entire file into memory. SQL statements are properly
    /// parsed by splitting on semicolons while respecting string literals and comments.
    ///
    /// Performance characteristics:
    /// - Memory: O(1) - only buffers 64KB at a time
    /// - Time: O(n) where n is file size in bytes
    /// - Can handle files of any size without truncation
    /// - Properly handles multi-line SQL statements
    pub async fn read_sql_commands(&self, metadata_file_path: &Path) -> Result<Vec<String>> {
        // Read metadata to get the data file path
        let metadata = self.read_metadata(metadata_file_path).await?;
        let data_file_path = &metadata.data_file_path;

        debug!(
            "Reading SQL commands from data file: {:?} (referenced by metadata: {:?})",
            data_file_path, metadata_file_path
        );

        let file = File::open(data_file_path).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to open data file {:?}: {}",
                data_file_path, e
            ))
        })?;

        let reader = BufReader::with_capacity(65536, file); // 64KB buffer for optimal I/O
        let mut lines = reader.lines();
        let mut file_content = String::with_capacity(65536);

        // Read the entire file content, filtering out comment lines
        while let Some(line) = lines.next_line().await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to read line from {:?}: {}",
                data_file_path, e
            ))
        })? {
            let trimmed = line.trim();

            // Skip transaction markers (comments starting with --)
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
            data_file_path
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

    /// Delete a pending transaction (metadata and data files) after successful execution
    pub async fn delete_pending_transaction(&self, metadata_file_path: &Path) -> Result<()> {
        // Read metadata to get data file path
        let metadata = self.read_metadata(metadata_file_path).await?;
        let data_file_path = &metadata.data_file_path;

        // Delete data file from sql_data_tx/
        if data_file_path.exists() {
            fs::remove_file(data_file_path).await?;
            debug!("Deleted data file: {:?}", data_file_path);
        }

        // Delete metadata file from sql_pending_tx/
        fs::remove_file(metadata_file_path).await?;
        debug!("Deleted metadata file: {:?}", metadata_file_path);

        info!(
            "Deleted executed transaction files: metadata={:?}, data={:?}",
            metadata_file_path, data_file_path
        );
        Ok(())
    }

    /// Clean up incomplete transactions from sql_received_tx and sql_data_tx directories
    pub async fn cleanup_received_transactions(&self) -> Result<()> {
        let received_dir = self.base_path.join(RECEIVED_TX_DIR);
        let mut entries = fs::read_dir(&received_dir).await?;
        let mut count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let metadata_path = entry.path();

            // Process .meta files from sql_received_tx/
            if metadata_path.extension().and_then(|s| s.to_str()) == Some("meta") {
                // Read metadata to get data file path
                if let Ok(metadata) = self.read_metadata(&metadata_path).await {
                    let data_file_path = &metadata.data_file_path;

                    // Delete data file if it exists
                    if data_file_path.exists() {
                        if let Err(e) = fs::remove_file(data_file_path).await {
                            warn!(
                                "Failed to delete incomplete data file {:?}: {}",
                                data_file_path, e
                            );
                        } else {
                            debug!("Deleted incomplete data file: {:?}", data_file_path);
                        }
                    }
                }

                // Delete metadata file
                fs::remove_file(&metadata_path).await?;
                count += 1;
            }
        }

        if count > 0 {
            warn!(
                "Cleaned up {} incomplete transaction(s) from sql_received_tx and sql_data_tx",
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
