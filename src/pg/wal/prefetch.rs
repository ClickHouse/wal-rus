//! wal-prefetch: pre-stage upcoming WAL segments into a sidecar dir so
//! `wal-fetch` can promote them by rename rather than going to storage.
//!
//! Layout matches wal-g (`pg_wal/.wal-g/prefetch/`):
//!
//!   <pg_wal>/.wal-g/prefetch/running/<seg>   — partial download
//!   <pg_wal>/.wal-g/prefetch/<seg>           — ready to promote
//!
//! wal-fetch consults `<pg_wal>/.wal-g/prefetch/<seg>` first (see
//! `super::fetch::handle`); a successful prefetch turns a network round-trip
//! into a local rename. Skipped on .history files (those re-resolve cheaply).
//!
//! Invoked two ways, mirroring wal-g: standalone (`wal-rs wal-prefetch`, also
//! the fork target) and triggered from wal-fetch (see `fetch::trigger_prefetch`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;

use crate::concurrency::BoundedTasks;
use crate::config::Settings;
use crate::storage::DynStorage;

use super::segment::{SegmentName, wal_segment_size};

/// `<pg_wal>/.wal-g/prefetch/`
pub const PREFETCH_SUBDIR: &str = ".wal-g/prefetch";
pub const RUNNING_SUBDIR: &str = "running";

/// Base dir holding `.wal-g/prefetch/`. `WALG_PREFETCH_DIR` overrides the
/// pg_wal-relative default; honored by both the writer here & the consumer in
/// `fetch::try_promote_prefetched` so the two stay in sync (wal-g parity)
fn prefetch_base(pg_wal: &Path) -> PathBuf {
    match std::env::var_os("WALG_PREFETCH_DIR") {
        Some(d) if !d.is_empty() => PathBuf::from(d),
        _ => pg_wal.to_path_buf(),
    }
}

pub fn prefetch_dir(pg_wal: &Path) -> PathBuf {
    prefetch_base(pg_wal).join(PREFETCH_SUBDIR)
}

pub fn running_dir(pg_wal: &Path) -> PathBuf {
    prefetch_dir(pg_wal).join(RUNNING_SUBDIR)
}

pub fn prefetched_path(pg_wal: &Path, seg: &str) -> PathBuf {
    prefetch_dir(pg_wal).join(seg)
}

pub async fn handle(
    settings: &Settings,
    storage: DynStorage,
    seed: &str,
    pg_wal: &Path,
    count: u32,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let seed_seg =
        SegmentName::parse(seed).with_context(|| format!("invalid seed segment name: {seed}"))?;

    let pre = prefetch_dir(pg_wal);
    let run = running_dir(pg_wal);
    fs::create_dir_all(&run)
        .await
        .with_context(|| format!("create {}", run.display()))?;

    // wal-g CleanupPrefetchDirectories: drop already-replayed segments (< seed)
    cleanup_stale(&pre, &run, seed_seg).await;

    let seg_size = wal_segment_size();
    let mut next = seed_seg.next(seg_size);
    // missing/transient errors don't fail the batch
    let mut tasks = BoundedTasks::new(
        settings.download_concurrency,
        "prefetch",
        |(name, res): (String, Result<()>)| {
            match res {
                Ok(()) => tracing::info!(target = "wal_prefetch", "prefetched {name}"),
                Err(e) => tracing::warn!(target = "wal_prefetch", "skip {name}: {e:#}"),
            }
            Ok(())
        },
    );

    for _ in 0..count {
        let name = next.format();
        next = next.next(seg_size);

        let ready = pre.join(&name);
        let running = run.join(&name);
        let local_in_pgwal = pg_wal.join(&name);
        // skip when ready, in-flight (running/), or already restored into pg_wal
        if fs::try_exists(&ready).await.unwrap_or(false)
            || fs::try_exists(&running).await.unwrap_or(false)
            || fs::try_exists(&local_in_pgwal).await.unwrap_or(false)
        {
            tracing::debug!(target = "wal_prefetch", "{name} already staged, skipping");
            continue;
        }
        let st = storage.clone();
        let cfg = settings.clone();
        tasks
            .spawn(async move {
                let r = fetch_one(&cfg, st, &name, &running, &ready).await;
                (name, r)
            })
            .await?;
    }
    tasks.join().await?;
    Ok(())
}

async fn fetch_one(
    settings: &Settings,
    storage: DynStorage,
    name: &str,
    running: &Path,
    ready: &Path,
) -> Result<()> {
    // Download in place at running/<seg> (candidate-ext fallback, decompression
    // and rate limits via the shared helper) so a concurrent wal-fetch can watch
    // it grow and reuse it rather than re-downloading. Promote by rename on
    // completion. Ok(false): another prefetcher already owns running/<seg>
    match super::fetch::download_to_running(settings, &storage, name, running).await {
        Ok(true) => fs::rename(running, ready)
            .await
            .with_context(|| format!("rename {} -> {}", running.display(), ready.display())),
        Ok(false) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(running).await;
            Err(e)
        }
    }
}

/// Remove staged segments older than `current` (already replayed) from both the
/// ready dir & `running/`, matching wal-g CleanupPrefetchDirectories. Entries
/// that don't parse as a segment name (`running/` itself, `.history`) are left
async fn cleanup_stale(pre: &Path, run: &Path, current: SegmentName) {
    for dir in [pre, run] {
        let mut rd = match fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let drop_it = entry
                .file_name()
                .to_str()
                .and_then(|n| SegmentName::parse(n).ok())
                .is_some_and(|seg| seg < current);
            if drop_it {
                let _ = fs::remove_file(entry.path()).await;
            }
        }
    }
}
