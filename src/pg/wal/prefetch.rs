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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::fs;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::Settings;
use crate::storage::DynStorage;

use super::segment::{DEFAULT_WAL_SEG_SIZE, SegmentName};

/// `<pg_wal>/.wal-g/prefetch/`
pub const PREFETCH_SUBDIR: &str = ".wal-g/prefetch";
pub const RUNNING_SUBDIR: &str = "running";

pub fn prefetch_dir(pg_wal: &Path) -> PathBuf {
    pg_wal.join(PREFETCH_SUBDIR)
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

    let mut next = seed_seg.next(DEFAULT_WAL_SEG_SIZE);
    let sem = Arc::new(Semaphore::new(settings.download_concurrency.max(1)));
    let mut tasks: JoinSet<(String, Result<()>)> = JoinSet::new();

    for _ in 0..count {
        let name = next.format();
        next = next.next(DEFAULT_WAL_SEG_SIZE);

        let ready = pre.join(&name);
        let local_in_pgwal = pg_wal.join(&name);
        if fs::try_exists(&ready).await.unwrap_or(false)
            || fs::try_exists(&local_in_pgwal).await.unwrap_or(false)
        {
            tracing::debug!(target = "wal_prefetch", "{name} already staged, skipping");
            continue;
        }
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .context("acquire prefetch permit")?;
        let st = storage.clone();
        let cfg = settings.clone();
        let running = run.join(&name);
        let ready_path = ready.clone();
        let task_name = name.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let r = fetch_one(&cfg, st, &task_name, &running, &ready_path).await;
            (task_name, r)
        });
    }

    while let Some(joined) = tasks.join_next().await {
        let (name, res) = joined.context("prefetch task join")?;
        match res {
            Ok(()) => tracing::info!(target = "wal_prefetch", "prefetched {name}"),
            // missing-segment + transient errors don't fail the whole batch
            Err(e) => tracing::warn!(target = "wal_prefetch", "skip {name}: {e:#}"),
        }
    }
    Ok(())
}

async fn fetch_one(
    settings: &Settings,
    storage: DynStorage,
    name: &str,
    running: &Path,
    ready: &Path,
) -> Result<()> {
    // Reuse the regular wal-fetch download path so candidate-ext fallback,
    // decompression, and rate limits stay consistent
    let res = super::fetch::handle(settings, storage, name, running).await;
    match res {
        Ok(()) => fs::rename(running, ready)
            .await
            .with_context(|| format!("rename {} -> {}", running.display(), ready.display())),
        Err(e) => {
            let _ = fs::remove_file(running).await;
            Err(e)
        }
    }
}
