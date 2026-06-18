//! wal-fetch: download WAL segment from storage, decompress, write to dst path
//!
//! Try configured compression first, fall back to other extensions to support
//! buckets written by mixed-config wal-g/wal-rs invocations

use std::path::{Path, PathBuf};
use std::time::Duration;

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

/// Poll cadence while waiting on an in-flight prefetch (`running/<seg>`); wal-g
/// HandleWALFetch uses 2 ms
const PREFETCH_POLL_INTERVAL: Duration = Duration::from_millis(2);
/// Abandon the wait after this many polls without the running file growing
/// (~200 ms), presuming a dead/too-slow prefetcher (wal-g maxSizeStallTerations)
const PREFETCH_MAX_STALLS: u32 = 100;

/// Whether & how to prefetch subsequent segments once the requested one is
/// served, mirroring wal-g's injected WalPrefetcher (Regular / Daemon / Nop)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prefetch {
    /// No prefetch: internal fetches (wal-restore, the prefetch worker itself)
    Off,
    /// Fork a detached `wal-rs wal-prefetch` child so prefetch outlives this
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

    // tmp + atomic rename so PG never observes a partial segment at `dst`
    let tmp = tmp_path(dst);
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut out = fs::File::create(&tmp)
        .await
        .with_context(|| format!("create {}", tmp.display()))?;
    stream_object_into(settings, &storage, &key, method, &mut out).await?;
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

/// Spawn a detached `wal-rs wal-prefetch <name> <pg_wal>` child. It inherits
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

/// Stream a located object into `out`: network throttle, decrypt, decompress.
/// Shared by the direct fetch (writes a tmp then renames) and the prefetch
/// worker (writes `running/<seg>` in place so wal-fetch can observe its growth)
async fn stream_object_into(
    settings: &Settings,
    storage: &DynStorage,
    key: &str,
    method: compression::Method,
    out: &mut fs::File,
) -> Result<()> {
    let body = storage
        .get(key)
        .await
        .with_context(|| format!("get {key}"))?;
    let throttled = settings.throttle_network(body);
    let decrypted = settings.decrypt(throttled);
    let mut decoded = compression::decode(method, decrypted);
    tokio::io::copy(&mut decoded, out).await?;
    out.flush().await?;
    out.sync_all().await?;
    Ok(())
}

/// Download `name` straight into `out_path` for the prefetch worker — no tmp
/// indirection, so the partial is visible at `running/<seg>` while it downloads
/// and a concurrent wal-fetch can watch it grow (wal-g prefetchFile). Created
/// exclusively: `Ok(false)` means another prefetcher already owns it, so leave
/// it untouched (wal-g O_EXCL parity). Prefetch never targets history files, so
/// the configured compression is always the preferred extension
pub(super) async fn download_to_running(
    settings: &Settings,
    storage: &DynStorage,
    name: &str,
    out_path: &Path,
) -> Result<bool> {
    let (key, method) = match find_object(storage.as_ref(), name, settings.compression).await? {
        Some(p) => p,
        None => return Err(ArchiveNotFound(name.to_string()).into()),
    };
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut out = match fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(out_path)
        .await
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(e) => return Err(e).with_context(|| format!("create {}", out_path.display())),
    };
    stream_object_into(settings, storage, &key, method, &mut out).await?;
    Ok(true)
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

/// Reuse an in-flight or completed prefetch instead of racing it with a fresh
/// download, mirroring wal-g HandleWALFetch. When dst's parent looks like a
/// pg_wal directory: promote a ready segment by rename; else, while a prefetch
/// is mid-flight at `running/<seg>`, poll until it completes (promote) or stalls
/// (~200 ms of no growth → presume dead, reclaim, fall back to a direct
/// download). Returns true when `dst` was satisfied from prefetch, false to
/// download. Any unexpected error or missing prefetch dir falls through
async fn try_promote_prefetched(name: &str, dst: &Path) -> Result<bool> {
    let Some(parent) = dst.parent() else {
        return Ok(false);
    };
    let ready = super::prefetch::prefetched_path(parent, name);
    let running = super::prefetch::running_dir(parent).join(name);

    let mut seen_size: i64 = -1;
    let mut stalls = 0u32;
    loop {
        match fs::metadata(&ready).await {
            Ok(_) => {
                if let Some(p) = dst.parent() {
                    fs::create_dir_all(p).await.ok();
                }
                fs::rename(&ready, dst).await.with_context(|| {
                    format!(
                        "promote prefetched {} -> {}",
                        ready.display(),
                        dst.display()
                    )
                })?;
                tracing::info!(
                    target = "wal_fetch",
                    "promoted prefetched {name} -> {}",
                    dst.display()
                );
                return Ok(true);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context("stat prefetched segment"),
        }
        // No ready file yet — is a prefetch downloading it right now?
        match fs::metadata(&running).await {
            Ok(m) => {
                let size = m.len() as i64;
                if size > seen_size {
                    seen_size = size;
                    stalls = 0;
                } else {
                    stalls += 1;
                    if stalls >= PREFETCH_MAX_STALLS {
                        // dead/too-slow prefetcher: reclaim and download ourselves
                        let _ = fs::remove_file(&running).await;
                        let _ = fs::remove_file(&ready).await;
                        return Ok(false);
                    }
                }
            }
            // running absent (normal cold fetch) or unreadable: just download
            Err(_) => return Ok(false),
        }
        tokio::time::sleep(PREFETCH_POLL_INTERVAL).await;
    }
}
