//! Low-level PostgreSQL connection using libpq-sys
//!
//! This module provides safe wrappers around libpq functions for logical replication.

use crate::buffer::BufferWriter;
use crate::config::Config;
use crate::connection::PostgresConnection;
use crate::error::{CdcError, Result};
use crate::logical_stream::{LogicalReplicationStream, ReplicationStreamConfig};
use crate::types::{ChangeEvent, Lsn};
use libpq_sys::*;
use std::ffi::{CStr, CString};
use std::os::unix::io::RawFd;
use std::ptr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

// PostgreSQL constants
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800; // Seconds from 1970-01-01 to 2000-01-01
pub const INVALID_XLOG_REC_PTR: u64 = 0;

// Type aliases matching PostgreSQL types
pub type XLogRecPtr = u64;
pub type Xid = u32;
pub type Oid = u32;
pub type TimestampTz = i64;

/// Safe wrapper around PostgreSQL connection for replication
pub struct PgReplicationConnection {
    conn: *mut PGconn,
    is_replication_conn: bool,
    async_fd: Option<AsyncFd<RawFd>>,
}

impl PgReplicationConnection {
    /// Create a new PostgreSQL connection for logical replication
    pub fn connect(conninfo: &str) -> Result<Self> {
        // Ensure libpq is properly initialized
        unsafe {
            let library_version = PQlibVersion();
            debug!("Using libpq version: {}", library_version);
        }

        let c_conninfo = CString::new(conninfo)
            .map_err(|e| CdcError::connection(format!("Invalid connection string: {}", e)))?;

        let conn = unsafe { PQconnectdb(c_conninfo.as_ptr()) };

        if conn.is_null() {
            return Err(CdcError::connection(
                "Failed to allocate PostgreSQL connection object".to_string(),
            ));
        }

        let status = unsafe { PQstatus(conn) };
        if status != ConnStatusType::CONNECTION_OK {
            let error_msg = unsafe {
                let error_ptr = PQerrorMessage(conn);
                if error_ptr.is_null() {
                    "Unknown connection error".to_string()
                } else {
                    CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
                }
            };
            unsafe { PQfinish(conn) };
            return Err(CdcError::connection(format!(
                "PostgreSQL connection failed: {}",
                error_msg
            )));
        }

        // Check server version - logical replication requires PostgreSQL 14+
        let server_version = unsafe { PQserverVersion(conn) };
        if server_version < 140000 {
            unsafe { PQfinish(conn) };
            return Err(CdcError::connection(format!(
                "PostgreSQL version {} is not supported. Logical replication requires PostgreSQL 14+",
                server_version
            )));
        }

        debug!("Connected to PostgreSQL server version: {}", server_version);

        Ok(Self {
            conn,
            is_replication_conn: false,
            async_fd: None,
        })
    }

    /// Execute a replication command (like IDENTIFY_SYSTEM)
    pub fn exec(&self, query: &str) -> Result<PgResult> {
        let c_query = CString::new(query)
            .map_err(|e| CdcError::protocol(format!("Invalid query string: {}", e)))?;

        let result = unsafe { PQexec(self.conn, c_query.as_ptr()) };

        if result.is_null() {
            return Err(CdcError::protocol(
                "Query execution failed - null result".to_string(),
            ));
        }

        let pg_result = PgResult::new(result);
        // Check for errors
        let status = pg_result.status();
        info!(
            "query : {}, pg_result.status() : {:?}",
            query,
            pg_result.status()
        );
        if !matches!(
            status,
            ExecStatusType::PGRES_TUPLES_OK
                | ExecStatusType::PGRES_COMMAND_OK
                | ExecStatusType::PGRES_COPY_BOTH
        ) {
            let error_msg = pg_result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(CdcError::protocol(format!(
                "Query execution failed: {}",
                error_msg
            )));
        }

        Ok(pg_result)
    }

    /// Send IDENTIFY_SYSTEM command
    pub fn identify_system(&self) -> Result<PgResult> {
        debug!("Sending IDENTIFY_SYSTEM command");
        let result = self.exec("IDENTIFY_SYSTEM")?;

        if result.ntuples() > 0 {
            if let (Some(systemid), Some(timeline), Some(xlogpos)) = (
                result.get_value(0, 0),
                result.get_value(0, 1),
                result.get_value(0, 2),
            ) {
                debug!(
                    "System identification: systemid={}, timeline={}, xlogpos={}",
                    systemid, timeline, xlogpos
                );
            }
        }

        Ok(result)
    }

    /// Create a replication slot
    pub fn create_replication_slot(
        &self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<PgResult> {
        let create_slot_sql = format!(
            "CREATE_REPLICATION_SLOT \"{}\" LOGICAL {} NOEXPORT_SNAPSHOT;",
            slot_name, output_plugin
        );

        debug!("Creating replication slot: {}", slot_name);
        let result = self.exec(&create_slot_sql)?;

        if result.ntuples() > 0 {
            if let Some(slot_name_result) = result.get_value(0, 0) {
                debug!("Replication slot created: {}", slot_name_result);
            }
        }

        Ok(result)
    }

    /// Start logical replication
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let mut options_str = String::new();
        for (i, (key, value)) in options.iter().enumerate() {
            if i > 0 {
                options_str.push_str(", ");
            }
            options_str.push_str(&format!("\"{}\" '{}'", key, value));
        }

        let start_replication_sql = if start_lsn == INVALID_XLOG_REC_PTR {
            format!(
                "START_REPLICATION SLOT \"{}\" LOGICAL 0/0 ({})",
                slot_name, options_str
            )
        } else {
            format!(
                "START_REPLICATION SLOT \"{}\" LOGICAL {} ({})",
                slot_name,
                format_lsn(start_lsn),
                options_str
            )
        };

        debug!("Starting replication: {}", start_replication_sql);
        let _result = self.exec(&start_replication_sql)?;

        self.is_replication_conn = true;

        // Initialize async socket for non-blocking operations
        self.initialize_async_socket()?;

        debug!("Replication started successfully");
        Ok(())
    }

    /// Get copy data from replication stream
    pub fn get_copy_data(&self, async_mode: i32) -> Result<Option<Vec<u8>>> {
        if !self.is_replication_conn {
            return Err(CdcError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }

        if async_mode != 0 {
            let _ = unsafe { PQconsumeInput(self.conn) };
        }

        let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe { PQgetCopyData(self.conn, &mut buffer, async_mode) };

        match result {
            -2 => {
                let error_msg = self.last_error_message();
                Err(CdcError::protocol(format!(
                    "Copy operation failed: {}",
                    error_msg
                )))
            }
            -1 => Ok(None), // No more data available
            0 => Ok(None),  // Timeout or no data available
            len if len > 0 => {
                if buffer.is_null() {
                    return Err(CdcError::buffer(
                        "Received null buffer from PQgetCopyData".to_string(),
                    ));
                }

                let data = unsafe {
                    std::slice::from_raw_parts(buffer as *const u8, len as usize).to_vec()
                };

                unsafe { PQfreemem(buffer as *mut std::os::raw::c_void) };
                Ok(Some(data))
            }
            _ => Err(CdcError::protocol(format!(
                "Unexpected result from PQgetCopyData: {}",
                result
            ))),
        }
    }

    /// Send feedback to the server (standby status update)
    pub fn send_standby_status_update(
        &self,
        received_lsn: XLogRecPtr,
        flushed_lsn: XLogRecPtr,
        applied_lsn: XLogRecPtr,
        reply_requested: bool,
    ) -> Result<()> {
        if !self.is_replication_conn {
            return Err(CdcError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }

        let timestamp = system_time_to_postgres_timestamp(SystemTime::now());

        // Build the standby status update message using BufferWriter
        let mut buffer = BufferWriter::with_capacity(34); // 1 + 8 + 8 + 8 + 8 + 1

        buffer.write_u8(b'r')?; // Message type
        buffer.write_u64(received_lsn)?;
        buffer.write_u64(flushed_lsn)?;
        buffer.write_u64(applied_lsn)?;
        buffer.write_i64(timestamp)?;
        buffer.write_u8(if reply_requested { 1 } else { 0 })?;

        let reply_data = buffer.freeze();

        let result = unsafe {
            PQputCopyData(
                self.conn,
                reply_data.as_ptr() as *const std::os::raw::c_char,
                reply_data.len() as i32,
            )
        };

        if result != 1 {
            let error_msg = self.last_error_message();
            return Err(CdcError::protocol(format!(
                "Failed to send standby status update: {}",
                error_msg
            )));
        }

        // Flush the connection
        let flush_result = unsafe { PQflush(self.conn) };
        if flush_result != 0 {
            let error_msg = self.last_error_message();
            return Err(CdcError::protocol(format!(
                "Failed to flush connection: {}",
                error_msg
            )));
        }

        debug!(
            "Sent standby status update: received={}, flushed={}, applied={}, reply_requested={}",
            format_lsn(received_lsn),
            format_lsn(flushed_lsn),
            format_lsn(applied_lsn),
            reply_requested
        );

        Ok(())
    }

    /// Initialize async socket for non-blocking operations
    fn initialize_async_socket(&mut self) -> Result<()> {
        let sock: RawFd = unsafe { PQsocket(self.conn) };
        if sock < 0 {
            return Err(CdcError::protocol("Invalid PostgreSQL socket".to_string()));
        }

        let async_fd = AsyncFd::new(sock)
            .map_err(|e| CdcError::protocol(format!("Failed to create AsyncFd: {}", e)))?;

        self.async_fd = Some(async_fd);
        Ok(())
    }

    /// Get copy data from replication stream (async non-blocking version)
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<Vec<u8>>> {
        if !self.is_replication_conn {
            return Err(CdcError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }

        let async_fd = self
            .async_fd
            .as_ref()
            .ok_or_else(|| CdcError::protocol("AsyncFd not initialized".to_string()))?;

        loop {
            if cancellation_token.is_cancelled() {
                return Ok(None);
            }

            // Wait for socket to be readable
            let mut guard = async_fd.readable().await.map_err(|e| {
                CdcError::protocol(format!("Failed to wait for socket readability: {}", e))
            })?;

            if let Some(data) = self.try_read_copy_data()? {
                return Ok(Some(data));
            }

            guard.clear_ready();
        }
    }

    fn try_read_copy_data(&self) -> Result<Option<Vec<u8>>> {
        // Read libpq socket buffer
        let consumed = unsafe { PQconsumeInput(self.conn) };
        if consumed == 0 {
            return Err(CdcError::protocol(self.last_error_message()));
        }

        if unsafe { PQisBusy(self.conn) } != 0 {
            return Ok(None); // Buffer not complete, wait for next socket readable event
        }

        let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe { PQgetCopyData(self.conn, &mut buffer, 1) };

        match result {
            len if len > 0 => {
                if buffer.is_null() {
                    return Err(CdcError::buffer(
                        "Received null buffer from PQgetCopyData".to_string(),
                    ));
                }

                let data = unsafe {
                    std::slice::from_raw_parts(buffer as *const u8, len as usize).to_vec()
                };

                unsafe { PQfreemem(buffer as *mut std::os::raw::c_void) };
                Ok(Some(data))
            }
            0 | -2 => Ok(None), // No complete data available, continue waiting
            -1 => {
                // COPY finished or channel closed
                debug!("COPY finished or channel closed");
                Ok(None)
            }
            _ => Err(CdcError::protocol(format!(
                "Unexpected PQgetCopyData result: {}",
                result
            ))),
        }
    }

    /// Get the last error message from the connection
    fn last_error_message(&self) -> String {
        unsafe {
            let error_ptr = PQerrorMessage(self.conn);
            if error_ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(error_ptr).to_string_lossy().into_owned()
            }
        }
    }

    /// Check if the connection is still alive
    pub fn is_alive(&self) -> bool {
        if self.conn.is_null() {
            return false;
        }

        unsafe { PQstatus(self.conn) == ConnStatusType::CONNECTION_OK }
    }

    /// Get the server version
    pub fn server_version(&self) -> i32 {
        unsafe { PQserverVersion(self.conn) }
    }
}

impl Drop for PgReplicationConnection {
    fn drop(&mut self) {
        if !self.conn.is_null() {
            debug!("Closing PostgreSQL replication connection");
            unsafe {
                PQfinish(self.conn);
            }
        }
    }
}

// Make the connection Send by ensuring exclusive access
unsafe impl Send for PgReplicationConnection {}

/// Safe wrapper for PostgreSQL result
pub struct PgResult {
    result: *mut PGresult,
}

impl PgResult {
    fn new(result: *mut PGresult) -> Self {
        Self { result }
    }

    /// Get the execution status
    pub fn status(&self) -> ExecStatusType {
        unsafe { PQresultStatus(self.result) }
    }

    /// Check if the result is OK
    pub fn is_ok(&self) -> bool {
        matches!(
            self.status(),
            ExecStatusType::PGRES_TUPLES_OK | ExecStatusType::PGRES_COMMAND_OK
        )
    }

    /// Get number of tuples (rows)
    pub fn ntuples(&self) -> i32 {
        unsafe { PQntuples(self.result) }
    }

    /// Get number of fields (columns)
    pub fn nfields(&self) -> i32 {
        unsafe { PQnfields(self.result) }
    }

    /// Get a field value as string
    pub fn get_value(&self, row: i32, col: i32) -> Option<String> {
        if row >= self.ntuples() || col >= self.nfields() {
            return None;
        }

        let value_ptr = unsafe { PQgetvalue(self.result, row, col) };
        if value_ptr.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(value_ptr).to_string_lossy().into_owned()) }
        }
    }

    /// Get error message if any
    pub fn error_message(&self) -> Option<String> {
        let error_ptr = unsafe { PQresultErrorMessage(self.result) };
        if error_ptr.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(error_ptr).to_string_lossy().into_owned()) }
        }
    }
}

impl Drop for PgResult {
    fn drop(&mut self) {
        if !self.result.is_null() {
            unsafe {
                PQclear(self.result);
            }
        }
    }
}

pub struct ReplicationStream {
    logical_stream: LogicalReplicationStream,
    config: Config,
    last_received_lsn: Option<Lsn>,
    last_feedback_time: SystemTime,
}

impl ReplicationStream {
    pub async fn new(_connection: PostgresConnection, config: Config) -> Result<Self> {
        let stream_config = ReplicationStreamConfig::from(&config);
        let logical_stream =
            LogicalReplicationStream::new(&config.source_connection_string, stream_config).await?;

        Ok(Self {
            logical_stream,
            config,
            last_received_lsn: None,
            last_feedback_time: SystemTime::now(),
        })
    }

    pub async fn start(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        info!("Starting PostgreSQL logical replication stream");

        // Initialize the logical stream
        self.logical_stream.initialize().await?;

        // Start replication from the specified LSN
        let start_xlog = start_lsn.map(|lsn| lsn.0);
        self.logical_stream.start(start_xlog).await?;

        info!("Logical replication stream started successfully");
        Ok(())
    }

    pub async fn next_event(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<ChangeEvent>> {
        debug!("Fetching next single change event");

        let event = self.logical_stream.next_event(&cancellation_token).await?;

        if let Some(ref event) = event {
            debug!("Received single change event: {:?}", event.event_type);

            // Update last received LSN
            self.last_received_lsn = event
                .lsn
                .as_ref()
                .and_then(|lsn_str| crate::pg_replication::parse_lsn(lsn_str).ok())
                .map(Lsn);
        }

        Ok(event)
    }

    pub fn send_feedback(&mut self) {
        self.logical_stream.maybe_send_feedback();
    }

    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping logical replication stream");
        self.logical_stream.stop().await?;
        Ok(())
    }

    pub fn current_lsn(&self) -> Option<Lsn> {
        self.last_received_lsn
    }
}

pub struct ReplicationManager {
    config: Config,
}

impl ReplicationManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn create_stream_async(&mut self) -> Result<ReplicationStream> {
        let connection = PostgresConnection::placeholder();
        ReplicationStream::new(connection, self.config.clone()).await
    }
}

/// Convert SystemTime to PostgreSQL timestamp format (microseconds since 2000-01-01)
pub fn system_time_to_postgres_timestamp(time: SystemTime) -> TimestampTz {
    let duration_since_unix = time
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime is before Unix epoch");

    let unix_secs = duration_since_unix.as_secs() as i64;
    let unix_micros = unix_secs * 1_000_000 + (duration_since_unix.subsec_micros() as i64);

    // Convert from Unix epoch to PostgreSQL epoch
    unix_micros - PG_EPOCH_OFFSET_SECS * 1_000_000
}

/// Convert PostgreSQL timestamp to formatted string
pub fn format_postgres_timestamp(timestamp: TimestampTz) -> String {
    let unix_micros = timestamp + PG_EPOCH_OFFSET_SECS * 1_000_000;
    let unix_secs = unix_micros / 1_000_000;

    match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(unix_secs as u64)) {
        Some(_) => {
            // Simple formatting without external dependencies
            format!("timestamp={}", unix_secs)
        }
        None => "invalid timestamp".to_string(),
    }
}

/// Parse LSN from string format (e.g., "0/12345678")
pub fn parse_lsn(lsn_str: &str) -> Result<XLogRecPtr> {
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        return Err(CdcError::protocol(format!(
            "Invalid LSN format: {}. Expected format: high/low",
            lsn_str
        )));
    }

    let high = u64::from_str_radix(parts[0], 16)
        .map_err(|e| CdcError::protocol(format!("Invalid LSN high part: {}", e)))?;
    let low = u64::from_str_radix(parts[1], 16)
        .map_err(|e| CdcError::protocol(format!("Invalid LSN low part: {}", e)))?;

    Ok((high << 32) | low)
}

/// Format LSN as string (e.g., "0/12345678")
pub fn format_lsn(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_parsing() {
        assert_eq!(parse_lsn("0/12345678").unwrap(), 0x12345678);
        assert_eq!(parse_lsn("1/12345678").unwrap(), 0x100000000 + 0x12345678);
        assert!(parse_lsn("invalid").is_err());
    }

    #[test]
    fn test_lsn_formatting() {
        assert_eq!(format_lsn(0x12345678), "0/12345678");
        assert_eq!(format_lsn(0x100000000 + 0x12345678), "1/12345678");
    }
}
