use crate::{error::{CdcError, Result}, types::{ChangeEvent, DestinationType}};
use async_trait::async_trait;

// Import destination implementations
#[cfg(feature = "mysql")]
use super::mysql::MySQLDestination;

#[cfg(feature = "sqlserver")]
use super::sqlserver::SqlServerDestination;


/// Trait for database destination handlers
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    /// Initialize the destination connection
    async fn connect(&mut self, connection_string: &str) -> Result<()>;
    
    /// Process a single change event
    async fn process_event(&mut self, event: &ChangeEvent) -> Result<()>;
    
    /// Process a batch of change events
    async fn process_batch(&mut self, events: &[ChangeEvent]) -> Result<()> {
        for event in events {
            self.process_event(event).await?;
        }
        Ok(())
    }
    
    /// Create a table if it doesn't exist
    async fn create_table_if_not_exists(&mut self, event: &ChangeEvent) -> Result<()>;
    
    /// Check if the destination is healthy
    async fn health_check(&mut self) -> Result<bool>;
    
    /// Close the connection
    async fn close(&mut self) -> Result<()>;
}

/// Factory for creating destination handlers
pub struct DestinationFactory;

impl DestinationFactory {
    /// Create a new destination handler for the specified type
    pub fn create(destination_type: DestinationType) -> Result<Box<dyn DestinationHandler>> {
        match destination_type {
            #[cfg(feature = "mysql")]
            DestinationType::MySQL => Ok(Box::new(MySQLDestination::new())),
            
            #[cfg(feature = "sqlserver")]
            DestinationType::SqlServer => Ok(Box::new(SqlServerDestination::new())),
            
            _ => Err(CdcError::unsupported(format!(
                "Destination type {:?} is not supported or not enabled",
                destination_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destination_factory_create() {
        // Test factory creation for different destination types
        #[cfg(feature = "mysql")]
        {
            let result = DestinationFactory::create(DestinationType::MySQL);
            assert!(result.is_ok());
        }
        
        #[cfg(feature = "sqlserver")]
        {
            let result = DestinationFactory::create(DestinationType::SqlServer);
            assert!(result.is_ok());
        }
        
        // Test unsupported destination type
        let result = DestinationFactory::create(DestinationType::PostgreSQL);
        assert!(result.is_err());
    }

    #[test]
    fn test_destination_types_serialization() {
        use serde_json;
        
        let mysql_type = DestinationType::MySQL;
        let json = serde_json::to_string(&mysql_type).unwrap();
        assert_eq!(json, "\"MySQL\"");
        
        let deserialized: DestinationType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, mysql_type);
    }
}
