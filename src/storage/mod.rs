//! Storage abstraction over Apache OpenDAL
//!
//! Mirrors wal-g object key layout so both tools can operate on same buckets.
//! Backends (s3/gcs/fs) are OpenDAL `Operator`s; the configured prefix becomes
//! the Operator `root`, so callers pass prefix-relative keys and listings come
//! back prefix-relative, matching wal-g.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use opendal::layers::{HttpClientLayer, RetryLayer};
use opendal::raw::HttpClient;
use opendal::services::{Fs, Gcs, S3};
use opendal::ErrorKind;
pub use opendal::Operator;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::config::StorageSettings;
use crate::retry::RetryPolicy;

pub mod check;
pub mod tools;

pub type AsyncReader = Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>;
pub type ObjStream = Pin<Box<dyn Stream<Item = opendal::Result<ObjMeta>> + Send + 'static>>;

/// Multipart part size; bodies at or under one chunk go out as a single PUT
pub const PART_SIZE: usize = 8 * 1024 * 1024;
/// In-flight parts across ALL concurrent uploads (no-overcommit ceiling)
pub const MAX_INFLIGHT_PARTS: usize = 8;

/// Object listing entry
#[derive(Debug, Clone)]
pub struct ObjMeta {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Resolved S3 settings. Credential fields are flattened: when both key fields
/// are set they're handed to OpenDAL verbatim, otherwise reqsign's chain (env,
/// IMDS, assume-role, web-identity, ECS) resolves them
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub disable_ec2_metadata: bool,
}

#[derive(Debug, Clone, Default)]
pub struct GcsConfig {
    pub bucket: String,
    pub prefix: String,
    pub credentials_path: Option<String>,
    /// emulator override (fake-gcs-server); when set, signing is skipped
    pub endpoint: Option<String>,
}

/// Process-wide part budget. One permit per resident PART_SIZE buffer, so
/// aggregate in-flight bytes stay near MAX_INFLIGHT_PARTS × PART_SIZE across
/// every concurrent upload, independent of writer count
fn part_permits() -> &'static Semaphore {
    static P: OnceLock<Semaphore> = OnceLock::new();
    P.get_or_init(|| Semaphore::new(MAX_INFLIGHT_PARTS))
}

/// reqwest client carrying the project rustls+aws-lc-rs stack, injected into
/// OpenDAL via HttpClientLayer so no second TLS/crypto stack is pulled
fn http_client() -> Result<HttpClient> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .context("build http client")?;
    Ok(HttpClient::with(client))
}

fn retry_layer(p: RetryPolicy) -> RetryLayer {
    // RetryPolicy.max_attempts counts the first try; RetryLayer counts retries
    let mut l = RetryLayer::new()
        .with_max_times(p.max_attempts.saturating_sub(1) as usize)
        .with_min_delay(p.base_delay)
        .with_max_delay(p.max_delay);
    if p.jitter {
        l = l.with_jitter();
    }
    l
}

/// OpenDAL `root` for a wal-g prefix: leading+trailing slash, "/" when empty
fn root_path(prefix: &str) -> String {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        "/".into()
    } else {
        format!("/{p}/")
    }
}

fn abs_path(path: &str) -> Result<String> {
    let p = Path::new(path);
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()?.join(p)
    };
    Ok(abs.to_string_lossy().into_owned())
}

/// Build an `Operator` for the backend. S3/GCS carry the injected HTTP client +
/// retry layer; fs is unwrapped (no transient failures worth retrying)
pub fn build_operator(s: &StorageSettings, policy: RetryPolicy) -> Result<Operator> {
    match s {
        StorageSettings::Fs { path } => {
            let root = abs_path(path)?;
            std::fs::create_dir_all(&root).with_context(|| format!("create {root}"))?;
            let op = Operator::new(Fs::default().root(&root).atomic_write_dir(&root))
                .context("init fs operator")?
                .finish();
            Ok(op)
        }
        StorageSettings::S3(c) => {
            let mut b = S3::default()
                .bucket(&c.bucket)
                .region(&c.region)
                .root(&root_path(&c.prefix));
            if let Some(ep) = &c.endpoint {
                b = b.endpoint(ep);
            }
            // OpenDAL defaults to path-style; virtual-host is opt-in
            if !c.force_path_style {
                b = b.enable_virtual_host_style();
            }
            if let (Some(ak), Some(sk)) = (&c.access_key_id, &c.secret_access_key) {
                b = b.access_key_id(ak).secret_access_key(sk);
                if let Some(t) = &c.session_token {
                    b = b.session_token(t);
                }
            }
            if c.disable_ec2_metadata {
                b = b.disable_ec2_metadata();
            }
            let op = Operator::new(b)
                .context("init s3 operator")?
                .layer(HttpClientLayer::new(http_client()?))
                .layer(retry_layer(policy))
                .finish();
            Ok(op)
        }
        StorageSettings::Gcs(c) => {
            let mut b = Gcs::default().bucket(&c.bucket).root(&root_path(&c.prefix));
            if let Some(ep) = &c.endpoint {
                // emulator: no auth
                b = b.endpoint(ep).skip_signature();
            } else if let Some(cp) = &c.credentials_path {
                b = b.credential_path(cp);
            }
            let op = Operator::new(b)
                .context("init gcs operator")?
                .layer(HttpClientLayer::new(http_client()?))
                .layer(retry_layer(policy))
                .finish();
            Ok(op)
        }
    }
}

/// Upload `body` to `key`. Reads the stream into contiguous PART_SIZE buffers
/// and hands each to OpenDAL's writer as one `Buffer`, so every part is a single
/// mmap-backed allocation freed whole on upload — not a fan of small slices the
/// allocator retains (`into_futures_async_write` coalesces through a 256 KiB
/// buffer, fragmenting each part). `concurrent(1)` keeps one part in flight per
/// writer; one permit per resident buffer bounds aggregate in-flight memory.
/// `size_hint` is unused (call-site stability): a lone part closes as a single
/// PUT, multiple parts as a multipart upload
pub async fn put_reader(
    op: &Operator,
    key: &str,
    mut body: AsyncReader,
    _size_hint: Option<u64>,
) -> Result<()> {
    let mut w = op
        .writer_with(key)
        .concurrent(1)
        .await
        .with_context(|| format!("open writer {key}"))?;
    loop {
        // Permit gates allocation of each part buffer; held over fill + handoff
        let permit = part_permits()
            .acquire()
            .await
            .map_err(|e| anyhow!("part permit pool closed: {e}"))?;
        let mut buf = BytesMut::with_capacity(PART_SIZE);
        while buf.len() < PART_SIZE {
            let n = body
                .read_buf(&mut buf)
                .await
                .with_context(|| format!("read body {key}"))?;
            if n == 0 {
                break;
            }
        }
        if buf.is_empty() {
            break;
        }
        // Short read => source EOF; full read => possibly more parts
        let full = buf.len() == PART_SIZE;
        w.write(buf.freeze())
            .await
            .with_context(|| format!("stream part {key}"))?;
        drop(permit);
        if !full {
            break;
        }
    }
    w.close()
        .await
        .with_context(|| format!("finalize {key}"))?;
    Ok(())
}

/// Download `key` as a streaming reader. Returns `opendal::Error` so callers can
/// match `ErrorKind::NotFound`
pub async fn get_reader(op: &Operator, key: &str) -> opendal::Result<AsyncReader> {
    let r = op.reader_with(key).await?;
    let ar = r.into_futures_async_read(..).await?;
    Ok(Box::pin(ar.compat()))
}

/// List objects under `prefix`, recursively. Directory entries are filtered out
pub async fn list_objs(op: &Operator, prefix: &str) -> opendal::Result<ObjStream> {
    let lister = op.lister_with(prefix).recursive(true).await?;
    let s = lister.filter_map(|res| async move {
        match res {
            Ok(e) if e.metadata().is_dir() => None,
            Ok(e) => Some(Ok(ObjMeta {
                key: e.path().to_string(),
                size: e.metadata().content_length(),
                last_modified: e
                    .metadata()
                    .last_modified()
                    .map(|ts| DateTime::<Utc>::from(SystemTime::from(ts))),
            })),
            Err(e) => Some(Err(e)),
        }
    });
    Ok(Box::pin(s))
}

/// True for OpenDAL not-found errors (drives extension-probe / archive-miss flow)
pub fn is_not_found(e: &opendal::Error) -> bool {
    e.kind() == ErrorKind::NotFound
}

/// Human-readable backend identifier for logs
pub fn describe(op: &Operator) -> String {
    let info = op.info();
    format!("{}://{}{}", info.scheme(), info.name(), info.root())
}

/// fs-backed Operator over `dir`, for tests
#[doc(hidden)]
pub fn fs_operator(dir: impl AsRef<Path>) -> Operator {
    let root = dir.as_ref().to_string_lossy();
    Operator::new(Fs::default().root(&root).atomic_write_dir(&root))
        .expect("fs operator")
        .finish()
}

/// Ergonomic I/O surface over a concrete `Operator`. Thin shim around the free
/// helpers so call sites read `op.put(..)` etc; `exists`/`delete` already exist
/// inherently on `Operator`. Zero dynamic dispatch (RPITIT, `Send` futures)
pub trait ObjExt {
    fn put(
        &self,
        key: &str,
        body: AsyncReader,
        size_hint: Option<u64>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn get(&self, key: &str) -> impl Future<Output = opendal::Result<AsyncReader>> + Send;
    fn list_objs(&self, prefix: &str) -> impl Future<Output = opendal::Result<ObjStream>> + Send;
    fn describe(&self) -> String;
}

impl ObjExt for Operator {
    fn put(
        &self,
        key: &str,
        body: AsyncReader,
        size_hint: Option<u64>,
    ) -> impl Future<Output = Result<()>> + Send {
        put_reader(self, key, body, size_hint)
    }
    fn get(&self, key: &str) -> impl Future<Output = opendal::Result<AsyncReader>> + Send {
        get_reader(self, key)
    }
    fn list_objs(&self, prefix: &str) -> impl Future<Output = opendal::Result<ObjStream>> + Send {
        list_objs(self, prefix)
    }
    fn describe(&self) -> String {
        describe(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    async fn roundtrip(len: usize) {
        let dir = tempfile::tempdir().unwrap();
        let op = fs_operator(dir.path());
        let data = pattern(len);
        let body: AsyncReader = Box::pin(std::io::Cursor::new(data.clone()));
        put_reader(&op, "obj", body, Some(len as u64))
            .await
            .unwrap();
        let mut r = get_reader(&op, "obj").await.unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).await.unwrap();
        assert_eq!(got.len(), len, "length mismatch for len={len}");
        assert_eq!(got, data, "content mismatch for len={len}");
    }

    #[tokio::test]
    async fn put_reader_roundtrips_across_part_boundaries() {
        // empty (close-only), small (single part), exact one part, and bodies
        // spanning the multi-part loop including an exact-multiple boundary
        for len in [
            0,
            1024,
            PART_SIZE,
            PART_SIZE + 4096,
            PART_SIZE * 2,
            PART_SIZE * 2 + 7,
        ] {
            roundtrip(len).await;
        }
    }
}
