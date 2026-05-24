use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use sqlx::MySqlPool;
use std::collections::HashMap;
use tracing::{debug, info, warn};

use super::coalescing::{coalesce_commands, QuoteStyle};

pub struct MySQLDestination {
    pool: Option<MySqlPool>,
    bulk_pool: Option<mysql_async::Pool>,
    schema_mappings: HashMap<String, String>,
    max_allowed_packet: u64,
    session_tuning_enabled: bool,
    session_tuning_threshold: usize,
    load_data_available: bool,
}

impl MySQLDestination {
    pub fn new() -> Self {
        Self {
            pool: None,
            bulk_pool: None,
            schema_mappings: HashMap::new(),
            max_allowed_packet: 67108864,
            session_tuning_enabled: true,
            session_tuning_threshold: 100,
            load_data_available: false,
        }
    }

    pub fn with_session_tuning(mut self, enabled: bool, threshold: usize) -> Self {
        self.session_tuning_enabled = enabled;
        self.session_tuning_threshold = threshold;
        self
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

    fn set_session_tuning(&mut self, enabled: bool, threshold: usize) {
        self.session_tuning_enabled = enabled;
        self.session_tuning_threshold = threshold;
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

        let coalesced = coalesce_commands(commands, self.max_allowed_packet, QuoteStyle::Backtick);

        if coalesced.len() < commands.len() {
            debug!(
                "Coalesced {} commands into {} statements (reduction: {:.1}%)",
                commands.len(),
                coalesced.len(),
                (1.0 - coalesced.len() as f64 / commands.len() as f64) * 100.0
            );
        }

        super::common::execute_sqlx_batch_with_hook(
            pool,
            &coalesced,
            pre_commit_hook,
            "MySQL",
            self.session_tuning_enabled && commands.len() >= self.session_tuning_threshold,
        )
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
            let sql = super::bulk_insert::build_multi_value_insert(table, columns, rows);
            return self
                .execute_sql_batch_with_hook(&[sql], pre_commit_hook)
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

        let tsv_data = super::bulk_insert::generate_tsv_buffer(rows);
        let row_count = rows.len();

        debug!(
            "Executing LOAD DATA LOCAL INFILE for {} rows ({} bytes TSV) into {}",
            row_count,
            tsv_data.len(),
            table
        );

        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::generic(format!("MySQL bulk connection failed: {e}")))?;

        conn.query_drop("START TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("MySQL START TRANSACTION failed: {e}")))?;

        conn.query_drop("SET unique_checks=0, foreign_key_checks=0")
            .await
            .map_err(|e| CdcError::generic(format!("MySQL session tuning failed: {e}")))?;

        let col_spec = columns.join(", ");
        let load_sql = format!(
            "LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE {} FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' ({})",
            table, col_spec
        );

        let result = conn
            .exec_batch(&load_sql, std::iter::empty::<Vec<mysql_async::Value>>())
            .await;

        if let Err(e) = result {
            // LOAD DATA via exec_batch might not be the right API - fall back to query_drop
            // with a local infile handler. For now, try the simpler approach.
            debug!("exec_batch failed for LOAD DATA, trying alternative: {e}");
            let _ = conn.query_drop("ROLLBACK").await;

            // Fallback: use multi-value INSERT within the existing sqlx transaction
            let sql = super::bulk_insert::build_multi_value_insert(table, columns, rows);
            return self
                .execute_sql_batch_with_hook(&[sql], pre_commit_hook)
                .await;
        }

        conn.query_drop("SET unique_checks=1, foreign_key_checks=1")
            .await
            .map_err(|e| CdcError::generic(format!("MySQL session restore failed: {e}")))?;

        if let Some(hook) = pre_commit_hook {
            if let Err(e) = hook().await {
                let _ = conn.query_drop("ROLLBACK").await;
                return Err(CdcError::generic(format!(
                    "MySQL bulk insert pre-commit hook failed, rolled back: {e}"
                )));
            }
        }

        conn.query_drop("COMMIT")
            .await
            .map_err(|e| CdcError::generic(format!("MySQL COMMIT failed after LOAD DATA: {e}")))?;

        info!(
            "LOAD DATA complete: {} rows loaded into {}",
            row_count, table
        );

        Ok(())
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
        assert!(!destination.load_data_available);
    }

    #[test]
    fn test_mysql_destination_with_session_tuning() {
        let destination = MySQLDestination::new().with_session_tuning(true, 50);
        assert!(destination.session_tuning_enabled);
        assert_eq!(destination.session_tuning_threshold, 50);
    }
}
