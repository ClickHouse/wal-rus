//! Storage abstraction
//!
//! Mirrors wal-g object key layout so both tools can operate on same buckets

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use thiserror::Error;
use tokio::io::AsyncRead;

pub mod fs;
pub mod gcs;
pub mod retrying;
pub mod s3;

pub type AsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;
pub type ObjectStream =
    Pin<Box<dyn Stream<Item = std::result::Result<ObjectMeta, StorageError>> + Send + 'static>>;
pub type ByteStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Bytes, StorageError>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http {status}: {body}")]
    Http { status: u16, body: String },
    #[error("transport error: {0}")]
    Transport(String),
    #[error("auth error: {0}")]
    Auth(String),
    #[error("config error: {0}")]
    Config(String),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("unimplemented: {0}")]
    Unimplemented(&'static str),
}

impl StorageError {
    /// True for errors that may succeed on retry (network blips, throttling, 5xx)
    pub fn is_transient(&self) -> bool {
        match self {
            StorageError::Transport(_) => true,
            StorageError::Http { status, .. } => {
                matches!(status, 408 | 425 | 429 | 500..=599)
            }
            StorageError::Io(e) => {
                use std::io::ErrorKind::*;
                matches!(
                    e.kind(),
                    TimedOut
                        | ConnectionReset
                        | ConnectionAborted
                        | ConnectionRefused
                        | Interrupted
                        | BrokenPipe
                        | UnexpectedEof
                        | WouldBlock
                )
            }
            _ => false,
        }
    }
}

impl From<reqwest::Error> for StorageError {
    fn from(e: reqwest::Error) -> Self {
        if let Some(st) = e.status() {
            StorageError::Http {
                status: st.as_u16(),
                body: e.to_string(),
            }
        } else {
            StorageError::Transport(e.to_string())
        }
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Object storage backend
///
/// Implementations stream uploads & downloads, no full-segment buffering
#[async_trait]
pub trait Storage: Send + Sync {
    /// Identifier for logs, eg "s3://bucket/prefix"
    fn describe(&self) -> String;

    /// Upload object. `size_hint` lets s3 backend pick single-PUT vs multipart
    async fn put(&self, key: &str, body: AsyncReader, size_hint: Option<u64>) -> Result<()>;

    /// Download object as streaming reader
    async fn get(&self, key: &str) -> Result<AsyncReader>;

    async fn exists(&self, key: &str) -> Result<bool>;

    /// List objects under prefix, recursively
    async fn list(&self, prefix: &str) -> Result<ObjectStream>;

    /// Delete a single object (idempotent: ok if missing)
    async fn delete(&self, key: &str) -> Result<()>;
}

pub type DynStorage = Arc<dyn Storage>;
