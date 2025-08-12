use thiserror::Error;

/// Comprehensive error types for CDC operations
#[derive(Error, Debug)]
pub enum CdcError {
    /// Database connection errors
    #[error("Database connection error: {0}")]
    Connection(#[from] tokio_postgres::Error),

    /// SQL Server connection errors  
    #[cfg(feature = "sqlserver")]
    #[error("SQL Server connection error: {0}")]
    SqlServer(#[from] tiberius::error::Error),

    /// MySQL connection errors
    #[cfg(feature = "mysql")]
    #[error("MySQL connection error: {0}")]
    MySQL(#[from] sqlx::Error),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Replication slot errors
    #[error("Replication slot error: {0}")]
    ReplicationSlot(String),

    /// Publication errors
    #[error("Publication error: {0}")]
    Publication(String),

    /// Protocol parsing errors
    #[error("Protocol parsing error: {0}")]
    Protocol(String),

    /// Buffer operation errors
    #[error("Buffer error: {0}")]
    Buffer(String),

    /// Message processing errors
    #[error("Message processing error: {0}")]
    MessageProcessing(String),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// String conversion errors (from CString operations)
    #[error("String conversion error: {0}")]
    StringConversion(#[from] std::ffi::NulError),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic errors
    #[error("CDC error: {0}")]
    Generic(String),

    /// Timeout errors
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Authentication errors
    #[error("Authentication failed: {0}")]
    Authentication(String),

    /// Unsupported operation errors
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

impl CdcError {
    /// Create a new configuration error
    pub fn config<S: Into<String>>(msg: S) -> Self {
        CdcError::Config(msg.into())
    }

    /// Create a new replication slot error
    pub fn replication_slot<S: Into<String>>(msg: S) -> Self {
        CdcError::ReplicationSlot(msg.into())
    }

    /// Create a new publication error
    pub fn publication<S: Into<String>>(msg: S) -> Self {
        CdcError::Publication(msg.into())
    }

    /// Create a new protocol error
    pub fn protocol<S: Into<String>>(msg: S) -> Self {
        CdcError::Protocol(msg.into())
    }

    /// Create a new buffer error
    pub fn buffer<S: Into<String>>(msg: S) -> Self {
        CdcError::Buffer(msg.into())
    }

    /// Create a new connection error
    pub fn connection<S: Into<String>>(msg: S) -> Self {
        CdcError::Generic(format!("Connection error: {}", msg.into()))
    }

    /// Create a new message processing error
    pub fn message_processing<S: Into<String>>(msg: S) -> Self {
        CdcError::MessageProcessing(msg.into())
    }

    /// Create a new generic error
    pub fn generic<S: Into<String>>(msg: S) -> Self {
        CdcError::Generic(msg.into())
    }

    /// Create a new timeout error
    pub fn timeout<S: Into<String>>(msg: S) -> Self {
        CdcError::Timeout(msg.into())
    }

    /// Create a new authentication error
    pub fn authentication<S: Into<String>>(msg: S) -> Self {
        CdcError::Authentication(msg.into())
    }

    /// Create a new unsupported operation error
    pub fn unsupported<S: Into<String>>(msg: S) -> Self {
        CdcError::Unsupported(msg.into())
    }
}

/// Result type for CDC operations
pub type Result<T> = std::result::Result<T, CdcError>;
