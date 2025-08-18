use crate::error::{CdcError, Result};
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};

/// PostgreSQL connection manager for replication
pub struct PostgresConnection {
    client: Option<Client>,
    connection_string: String,
}

impl PostgresConnection {
    /// Create a new PostgreSQL connection manager
    pub fn new(connection_string: String) -> Self {
        Self {
            client: None,
            connection_string,
        }
    }

    /// Create a placeholder connection (for compatibility with libpq-sys integration)
    pub fn placeholder() -> Self {
        Self {
            client: None,
            connection_string: String::new(),
        }
    }

    /// Establish connection to PostgreSQL
    pub async fn connect(&mut self) -> Result<()> {
        info!(
            "Connecting to PostgreSQL: {}",
            self.mask_password(&self.connection_string)
        );
        //todo support tls connection
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("PostgreSQL connection error: {}", e);
            }
        });

        self.client = Some(client);
        info!("Successfully connected to PostgreSQL");
        Ok(())
    }

    /// Get a reference to the client
    pub fn client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| CdcError::generic("PostgreSQL connection not established"))
    }

    /// Get a mutable reference to the client
    pub fn client_mut(&mut self) -> Result<&mut Client> {
        self.client
            .as_mut()
            .ok_or_else(|| CdcError::generic("PostgreSQL connection not established"))
    }

    /// Check if the connection is active
    pub async fn is_connected(&self) -> bool {
        if let Some(client) = &self.client {
            match client.simple_query("SELECT 1").await {
                Ok(_) => true,
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Reconnect to PostgreSQL
    pub async fn reconnect(&mut self) -> Result<()> {
        info!("Reconnecting to PostgreSQL");
        self.client = None;
        self.connect().await
    }

    /// Execute a simple query
    pub async fn execute(&self, query: &str) -> Result<Vec<tokio_postgres::SimpleQueryMessage>> {
        let client = self.client()?;
        let result = client.simple_query(query).await?;
        Ok(result)
    }

    /// Prepare and execute a query
    pub async fn query(
        &self,
        query: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>> {
        let client = self.client()?;
        let stmt = client.prepare(query).await?;
        let rows = client.query(&stmt, params).await?;
        Ok(rows)
    }

    /// Check if a replication slot exists
    pub async fn replication_slot_exists(&self, slot_name: &str) -> Result<bool> {
        let rows = self
            .query(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    /// Create a logical replication slot
    pub async fn create_replication_slot(
        &self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<String> {
        info!("Creating replication slot: {}", slot_name);

        let query = format!(
            "SELECT pg_create_logical_replication_slot('{}', '{}')",
            slot_name, output_plugin
        );

        let rows = self.execute(&query).await?;

        // Extract LSN from the result
        if let Some(tokio_postgres::SimpleQueryMessage::Row(row)) = rows.first() {
            if let Some(lsn) = row.get("lsn") {
                info!("Created replication slot '{}' at LSN: {}", slot_name, lsn);
                return Ok(lsn.to_string());
            }
        }

        Err(CdcError::replication_slot(
            "Failed to create replication slot",
        ))
    }

    /// Drop a replication slot
    pub async fn drop_replication_slot(&self, slot_name: &str) -> Result<()> {
        info!("Dropping replication slot: {}", slot_name);

        let query = format!("SELECT pg_drop_replication_slot('{}')", slot_name);
        self.execute(&query).await?;

        info!("Dropped replication slot: {}", slot_name);
        Ok(())
    }

    /// Check if a publication exists
    pub async fn publication_exists(&self, pub_name: &str) -> Result<bool> {
        let rows = self
            .query(
                "SELECT pubname FROM pg_publication WHERE pubname = $1",
                &[&pub_name],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    /// Create a publication
    pub async fn create_publication(
        &self,
        pub_name: &str,
        tables: Option<&[String]>,
    ) -> Result<()> {
        info!("Creating publication: {}", pub_name);

        let query = match tables {
            Some(table_list) if !table_list.is_empty() => {
                let tables_str = table_list
                    .iter()
                    .map(|t| format!("\"{}\"", t))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "CREATE PUBLICATION \"{}\" FOR TABLE {}",
                    pub_name, tables_str
                )
            }
            _ => {
                format!("CREATE PUBLICATION \"{}\" FOR ALL TABLES", pub_name)
            }
        };

        self.execute(&query).await?;
        info!("Created publication: {}", pub_name);
        Ok(())
    }

    /// Drop a publication
    pub async fn drop_publication(&self, pub_name: &str) -> Result<()> {
        info!("Dropping publication: {}", pub_name);

        let query = format!("DROP PUBLICATION \"{}\"", pub_name);
        self.execute(&query).await?;

        info!("Dropped publication: {}", pub_name);
        Ok(())
    }

    /// Get the current WAL position
    pub async fn get_current_wal_position(&self) -> Result<String> {
        let rows = self.execute("SELECT pg_current_wal_lsn()").await?;

        if let Some(tokio_postgres::SimpleQueryMessage::Row(row)) = rows.first() {
            if let Some(lsn) = row.get("pg_current_wal_lsn") {
                return Ok(lsn.to_string());
            }
        }

        Err(CdcError::generic("Failed to get current WAL position"))
    }

    /// Get replication slot status
    pub async fn get_replication_slot_status(
        &self,
        slot_name: &str,
    ) -> Result<ReplicationSlotStatus> {
        let rows = self.query(
            "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        ).await?;

        if let Some(row) = rows.first() {
            let status = ReplicationSlotStatus {
                slot_name: row.get("slot_name"),
                active: row.get("active"),
                restart_lsn: row.get("restart_lsn"),
                confirmed_flush_lsn: row.get("confirmed_flush_lsn"),
            };
            return Ok(status);
        }

        Err(CdcError::replication_slot(format!(
            "Replication slot '{}' not found",
            slot_name
        )))
    }

    /// Mask password in connection string for logging
    fn mask_password(&self, connection_string: &str) -> String {
        // Simple password masking - in production you might want more sophisticated masking
        if let Some(password_start) = connection_string.find("password=") {
            let mut masked = connection_string.to_string();
            let password_end = masked[password_start..]
                .find(' ')
                .unwrap_or(masked.len() - password_start);
            masked.replace_range(password_start + 9..password_start + password_end, "***");
            masked
        } else {
            connection_string.to_string()
        }
    }

    /// Close the connection
    pub async fn close(&mut self) {
        if self.client.is_some() {
            info!("Closing PostgreSQL connection");
            self.client = None;
        }
    }
}

/// Replication slot status information
#[derive(Debug, Clone)]
pub struct ReplicationSlotStatus {
    pub slot_name: String,
    pub active: bool,
    pub restart_lsn: Option<String>,
    pub confirmed_flush_lsn: Option<String>,
}
