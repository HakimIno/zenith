//! Error types for Zenith CDC

use thiserror::Error;

/// Main error type for Zenith
#[derive(Error, Debug)]
pub enum Error {
    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] zenith_storage::StorageError),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Invalid pgoutput message: {0}")]
    InvalidMessage(String),

    #[error("Unknown relation: {0}")]
    UnknownRelation(u32),

    #[error("Transaction not found: {0}")]
    TransactionNotFound(u64),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn parse(msg: impl Into<String>) -> Self {
        Error::Parse(msg.into())
    }

    pub fn config(msg: impl Into<String>) -> Self {
        Error::Config(msg.into())
    }

    pub fn clickhouse(msg: impl Into<String>) -> Self {
        Error::ClickHouse(msg.into())
    }

    pub fn invalid_message(msg: impl Into<String>) -> Self {
        Error::InvalidMessage(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }

    pub fn connection(msg: impl Into<String>) -> Self {
        Error::Connection(msg.into())
    }

    pub fn auth(msg: impl Into<String>) -> Self {
        Error::Auth(msg.into())
    }
}

/// Result type alias for Zenith operations
pub type Result<T> = std::result::Result<T, Error>;

/// Extension trait for adding context to errors
pub trait ResultExt<T> {
    fn context(self, msg: &str) -> Result<T>;
}

impl<T, E: Into<Error>> ResultExt<T> for std::result::Result<T, E> {
    fn context(self, msg: &str) -> Result<T> {
        self.map_err(|e| {
            let inner = e.into();
            Error::Other(format!("{}: {}", msg, inner))
        })
    }
}

