//! wal-fetch: download WAL segment from storage, decompress, write to dst path
//!
//! Try configured compression first, fall back to other extensions to support
//! buckets written by mixed-config wal-g/walrus invocations

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::compression;
use crate::config::Settings;
use crate::pg;
use crate::storage::{ObjExt, Operator};

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
    /// Fork a detached `walrus wal-prefetch` child so prefetch outlives this
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
    storage: Operator,
    name: &str,
    dst: &Path,
    prefetch: Prefetch,
) -> Result<()> {
    let history = is_history_filename(name);
    if !history && try_promote_prefetched(name, dst, settings.prefetch_dir.as_deref()).await? {
        trigger_prefetch(settings, &storage, name, dst, prefetch);
        return Ok(());
    }
    let preferred = if history {
        compression::Method::None
    } else {
        settings.compression
    };

    let (key, method) = match find_object(&storage, name, preferred).await? {
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
    storage: &Operator,
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
        Prefetch::Fork => fork_prefetch(name, pg_wal, settings.config_path.as_deref()),
    }
}

/// Spawn a detached `walrus wal-prefetch <name> <pg_wal>` child. It inherits the
/// process env (storage creds, WALG_*) and, since file-only settings are no
/// longer pushed into env, re-passes `--config` so the child re-resolves the
/// same file. Never waited on; once the parent exits, init reaps it (wal-g
/// RegularPrefetcher via exec.Command + cmd.Start)
fn fork_prefetch(name: &str, pg_wal: &Path, config: Option<&Path>) {
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target = "wal_fetch", "prefetch fork: current_exe: {e}");
            return;
        }
    };
    let mut cmd = std::process::Command::new(exe);
    if let Some(config) = config {
        cmd.arg("--config").arg(config);
    }
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
    storage: &Operator,
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
    storage: &Operator,
    name: &str,
    out_path: &Path,
) -> Result<bool> {
    let (key, method) = match find_object(storage, name, settings.compression).await? {
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
    storage: &Operator,
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
            Err(ref e) if crate::storage::is_not_found(e) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(None)
}

/// wal-g `checkWALFileMagic`: the first four bytes of a WAL segment, read as a
/// little-endian u32, carry the XLOG page magic (`xlp_magic` in the low half).
/// Every released magic is >= 0xD061, so a smaller value flags a corrupt or
/// truncated cached segment. A lower-bound heuristic, not a version-exact check
async fn check_wal_file_magic(path: &Path) -> Result<()> {
    let mut f = fs::File::open(path)
        .await
        .with_context(|| format!("open {} for magic check", path.display()))?;
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic)
        .await
        .with_context(|| format!("read magic from {}", path.display()))?;
    if u32::from_le_bytes(magic) < 0xD061 {
        anyhow::bail!(
            "WAL file magic 0x{:X} is invalid",
            u32::from_le_bytes(magic)
        );
    }
    Ok(())
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
async fn try_promote_prefetched(name: &str, dst: &Path, over: Option<&Path>) -> Result<bool> {
    let Some(parent) = dst.parent() else {
        return Ok(false);
    };
    let ready = super::prefetch::prefetched_path(parent, name, over);
    let running = super::prefetch::running_dir(parent, over).join(name);

    let mut seen_size: i64 = -1;
    let mut stalls = 0u32;
    loop {
        match fs::metadata(&ready).await {
            Ok(meta) => {
                // wal-g validates a ready cache entry before trusting it: exact
                // segment size, then WAL magic after the rename. Either failure
                // falls back to a fresh storage download rather than promoting a
                // corrupt or truncated segment into pg_wal
                let seg_size = super::segment::wal_segment_size();
                if meta.len() != seg_size {
                    tracing::warn!(
                        target = "wal_fetch",
                        "prefetched {name} has wrong size {} (want {seg_size}); re-fetching",
                        meta.len()
                    );
                    return Ok(false);
                }
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
                if let Err(e) = check_wal_file_magic(dst).await {
                    tracing::warn!(
                        target = "wal_fetch",
                        "prefetched {name} failed validation: {e:#}; re-fetching"
                    );
                    let _ = fs::remove_file(dst).await;
                    return Ok(false);
                }
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
            // running absent: either a cold fetch (no prefetch in flight) or a
            // prefetcher that just published by renaming running -> ready. We
            // sample ready and running non-atomically, so re-check ready before
            // falling back to a download, else a prefetch completing in this
            // window forces a redundant download
            Err(_) => {
                if fs::metadata(&ready).await.is_ok() {
                    continue;
                }
                return Ok(false);
            }
        }
        tokio::time::sleep(PREFETCH_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // First 4 bytes LE: 0xD10D (PG14 page magic) clears the >= 0xD061 floor
    const VALID_MAGIC: [u8; 4] = [0x0D, 0xD1, 0x00, 0x00];

    #[tokio::test]
    async fn magic_check_accepts_and_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good");
        fs::write(&good, VALID_MAGIC).await.unwrap();
        assert!(check_wal_file_magic(&good).await.is_ok());

        let bad = dir.path().join("bad");
        fs::write(&bad, [0u8; 4]).await.unwrap();
        assert!(check_wal_file_magic(&bad).await.is_err());

        // truncated (< 4 bytes) is rejected, not silently accepted
        let short = dir.path().join("short");
        fs::write(&short, [0x0Du8]).await.unwrap();
        assert!(check_wal_file_magic(&short).await.is_err());
    }

    /// `<pg_wal>/<seg>` plus its prefetch staging path for a ready file
    fn layout(pg_wal: &Path, name: &str) -> (PathBuf, PathBuf) {
        (
            pg_wal.join(name),
            super::super::prefetch::prefetched_path(pg_wal, name, None),
        )
    }

    #[tokio::test]
    async fn wrong_size_ready_falls_back_without_promoting() {
        let dir = tempfile::tempdir().unwrap();
        let name = "000000010000000000000005";
        let (dst, ready) = layout(dir.path(), name);
        fs::create_dir_all(ready.parent().unwrap()).await.unwrap();
        // a single page, not a full segment
        fs::write(&ready, vec![0u8; 8192]).await.unwrap();

        assert!(!try_promote_prefetched(name, &dst, None).await.unwrap());
        assert!(!fs::try_exists(&dst).await.unwrap(), "must not promote");
    }

    #[tokio::test]
    async fn full_size_promotes_or_rejects_on_magic() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = super::super::segment::wal_segment_size() as usize;

        // valid magic + correct size -> promoted by rename
        let ok_name = "000000010000000000000007";
        let (ok_dst, ok_ready) = layout(dir.path(), ok_name);
        fs::create_dir_all(ok_ready.parent().unwrap())
            .await
            .unwrap();
        let mut seg = vec![0u8; seg_size];
        seg[..4].copy_from_slice(&VALID_MAGIC);
        fs::write(&ok_ready, &seg).await.unwrap();
        assert!(
            try_promote_prefetched(ok_name, &ok_dst, None)
                .await
                .unwrap()
        );
        assert_eq!(
            fs::metadata(&ok_dst).await.unwrap().len() as usize,
            seg_size
        );
        assert!(!fs::try_exists(&ok_ready).await.unwrap(), "ready consumed");

        // bad magic + correct size -> dst removed, fall back to download
        let bad_name = "000000010000000000000008";
        let (bad_dst, bad_ready) = layout(dir.path(), bad_name);
        fs::write(&bad_ready, vec![0u8; seg_size]).await.unwrap();
        assert!(
            !try_promote_prefetched(bad_name, &bad_dst, None)
                .await
                .unwrap()
        );
        assert!(
            !fs::try_exists(&bad_dst).await.unwrap(),
            "bad segment removed"
        );
    }
}
