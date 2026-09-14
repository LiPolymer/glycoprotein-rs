use std::io;

use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub type Result<T, E = GlycoError> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq)]
pub struct RemoteError {
    pub message: String,
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RemoteError {}

#[derive(Debug, Clone, PartialEq)]
pub struct HandlerError {
    pub message: String,
}

impl HandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for HandlerError {}

impl From<String> for HandlerError {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for HandlerError {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Error)]
pub enum GlycoError {
    #[error("I/O failure: {0}")]
    Io(#[from] io::Error),

    #[error("JSON failure: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid node id '{0}'")]
    InvalidNodeId(String),

    #[error("message is too large: {size} bytes (limit: {limit})")]
    FrameTooLarge { size: usize, limit: usize },

    #[error("the connexon is not started")]
    NotStarted,

    #[error("peer '{0}' is not connected")]
    PeerNotConnected(String),

    #[error("node '{0}' is already running")]
    NodeAlreadyRunning(String),

    #[error("field '{field}' on '{node}' does not exist")]
    FieldNotFound { node: String, field: String },

    #[error("field '{field}' on '{node}' is not a method")]
    NotAMethod { node: String, field: String },

    #[error("field '{0}' is already registered")]
    DuplicateField(String),

    #[error("JSON schema generation failed: {0}")]
    SchemaGeneration(String),

    #[error("JSON schema validation failed: {0}")]
    Validation(String),

    #[error("query {qid} to '{node}/{field}' timed out")]
    Timeout {
        qid: Uuid,
        node: String,
        field: String,
    },

    #[error("query {0} was cancelled")]
    Cancelled(Uuid),

    #[error("remote handler failed: {0}")]
    Remote(#[from] RemoteError),

    #[error("handler failed: {0}")]
    Handler(#[from] HandlerError),

    #[error("background task failed: {0}")]
    Task(String),

    #[error("typed JSON conversion failed for {context}: {source}")]
    TypedJson {
        context: &'static str,
        #[source]
        source: serde_json::Error,
    },

    #[error("invalid protocol payload: {0}")]
    Protocol(String),

    #[error("unexpected reply payload: {0}")]
    UnexpectedPayload(Value),
}
