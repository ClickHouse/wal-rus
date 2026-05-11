//! wal-fetch: download WAL segment from storage, decompress, write to dst path
//!
//! Try configured compression first, fall back to other extensions to support
//! buckets written by mixed-config wal-g/wal-rs invocations

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::compression;
use crate::config::Settings;
use crate::pg;
use crate::storage::{DynStorage, StorageError};

use super::segment::is_history_filename;

// Fallback order covers wal-g's compressed formats plus uncompressed, so a
// bucket written by any of (zstd, brotli, lz4, lzma, none) is readable.
const CANDIDATE_EXTS: &[&str] = &["zst", "br", "lz4", "lzma", ""];

pub async fn handle(
    settings: &Settings,
    storage: DynStorage,
    name: &str,
    dst: &Path,
) -> Result<()> {
    let history = is_history_filename(name);
    if !history && try_promote_prefetched(name, dst).await? {
        return Ok(());
    }
    let preferred = if history {
        compression::Method::None
    } else {
        settings.compression
    };

    let (key, method) = match find_object(storage.as_ref(), name, preferred).await? {
        Some(p) => p,
        None => return Err(anyhow!("WAL {name} not found in storage")),
    };

    let body = storage
        .get(&key)
        .await
        .with_context(|| format!("get {key}"))?;
    let throttled = settings.throttle_network(body);
    let decrypted = settings.decrypt(throttled);
    let mut decoded = compression::decode(method, decrypted);

    let tmp = tmp_path(dst);
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut out = fs::File::create(&tmp)
        .await
        .with_context(|| format!("create {}", tmp.display()))?;
    tokio::io::copy(&mut decoded, &mut out).await?;
    out.flush().await?;
    out.sync_all().await?;
    drop(out);
    fs::rename(&tmp, dst)
        .await
        .with_context(|| format!("rename {} -> {}", tmp.display(), dst.display()))?;
    tracing::info!(target = "wal_fetch", "fetched {key} -> {}", dst.display());
    Ok(())
}

async fn find_object(
    storage: &dyn crate::storage::Storage,
    name: &str,
    preferred: compression::Method,
) -> Result<Option<(String, compression::Method)>> {
    let preferred_ext = preferred.extension();
    let mut order: Vec<&str> = vec![preferred_ext];
    for e in CANDIDATE_EXTS {
        if !order.contains(e) {
            order.push(e);
        }
    }

    for ext in order {
        let key = if ext.is_empty() {
            format!("{}/{}", pg::WAL_FOLDER, name)
        } else {
            format!("{}/{}.{}", pg::WAL_FOLDER, name, ext)
        };
        match storage.exists(&key).await {
            Ok(true) => {
                let m =
                    compression::Method::from_extension(ext).unwrap_or(compression::Method::None);
                return Ok(Some((key, m)));
            }
            Ok(false) => continue,
            Err(StorageError::NotFound(_)) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(None)
}

fn tmp_path(dst: &Path) -> PathBuf {
    let mut s = dst.as_os_str().to_owned();
    s.push(format!(".tmp.{}", std::process::id()));
    PathBuf::from(s)
}

/// When dst's parent looks like a pg_wal directory, check the wal-g prefetch
/// area for a ready segment & promote via rename. Best-effort — any failure
/// (or missing prefetch dir) falls through to the storage path
async fn try_promote_prefetched(name: &str, dst: &Path) -> Result<bool> {
    let Some(parent) = dst.parent() else {
        return Ok(false);
    };
    let staged = super::prefetch::prefetched_path(parent, name);
    match fs::metadata(&staged).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    }
    if let Some(p) = dst.parent() {
        fs::create_dir_all(p).await.ok();
    }
    fs::rename(&staged, dst).await.with_context(|| {
        format!(
            "promote prefetched {} -> {}",
            staged.display(),
            dst.display()
        )
    })?;
    tracing::info!(
        target = "wal_fetch",
        "promoted prefetched {name} -> {}",
        dst.display()
    );
    Ok(true)
}
