//! wal-fetch: download WAL segment from storage, decompress, write to dst path
//!
//! Try configured compression first, fall back to other extensions to support
//! buckets written by mixed-config wal-g/walross invocations

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
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

/// Whether & how to prefetch subsequent segments once the requested one is
/// served, mirroring wal-g's injected WalPrefetcher (Regular / Daemon / Nop)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prefetch {
    /// No prefetch: internal fetches (wal-restore, the prefetch worker itself)
    Off,
    /// Fork a detached `walross wal-prefetch` child so prefetch outlives this
    /// short-lived process (CLI restore_command path; wal-g RegularPrefetcher)
    Fork,
    /// Run prefetch as a background task in this long-lived process
    /// (daemon path; wal-g DaemonPrefetcher)
    InProcess,
}

/// WAL segment absent from storage; daemon maps to ArchiveNonExistence ('N')
/// response, mirroring wal-g's ArchiveNonExistenceError
#[derive(Debug, thiserror::Error)]
#[error("WAL {0} not found in storage")]
pub struct ArchiveNotFound(pub String);

pub async fn handle(
    settings: &Settings,
    storage: DynStorage,
    name: &str,
    dst: &Path,
    prefetch: Prefetch,
) -> Result<()> {
    let history = is_history_filename(name);
    if !history && try_promote_prefetched(name, dst).await? {
        trigger_prefetch(settings, &storage, name, dst, prefetch);
        return Ok(());
    }
    let preferred = if history {
        compression::Method::None
    } else {
        settings.compression
    };

    let (key, method) = match find_object(storage.as_ref(), name, preferred).await? {
        Some(p) => p,
        None => return Err(ArchiveNotFound(name.to_string()).into()),
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
    trigger_prefetch(settings, &storage, name, dst, prefetch);
    Ok(())
}

/// Kick off prefetch of the next `download_concurrency` segments, mirroring
/// wal-g's deferred prefetch in HandleWALFetch. Best-effort, never blocks the
/// fetch return. wal-g checkPrefetchPossible: skip history/.partial files & a
/// concurrency of 1 (nothing to prefetch ahead)
fn trigger_prefetch(
    settings: &Settings,
    storage: &DynStorage,
    name: &str,
    dst: &Path,
    mode: Prefetch,
) {
    if mode == Prefetch::Off {
        return;
    }
    let count = settings.download_concurrency;
    if count <= 1 || name.contains("history") || name.contains("partial") {
        return;
    }
    let Some(pg_wal) = dst.parent() else {
        return;
    };
    match mode {
        Prefetch::Off => unreachable!(),
        // Long-lived daemon: in-process so we keep the warm storage client
        Prefetch::InProcess => {
            let settings = settings.clone();
            let storage = storage.clone();
            let name = name.to_owned();
            let pg_wal = pg_wal.to_path_buf();
            tokio::spawn(async move {
                if let Err(e) =
                    super::prefetch::handle(&settings, storage, &name, &pg_wal, count as u32).await
                {
                    tracing::warn!(target = "wal_fetch", "prefetch after {name}: {e:#}");
                }
            });
        }
        Prefetch::Fork => fork_prefetch(name, pg_wal),
    }
}

/// Spawn a detached `walross wal-prefetch <name> <pg_wal>` child. It inherits
/// env (storage creds, WALG_*) and is never waited on; once the parent exits,
/// init reaps it (wal-g RegularPrefetcher via exec.Command + cmd.Start)
fn fork_prefetch(name: &str, pg_wal: &Path) {
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target = "wal_fetch", "prefetch fork: current_exe: {e}");
            return;
        }
    };
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("wal-prefetch").arg(name).arg(pg_wal);
    if let Err(e) = cmd.spawn() {
        tracing::warn!(target = "wal_fetch", "prefetch fork: {e}");
    }
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
