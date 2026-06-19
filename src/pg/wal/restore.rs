//! wal-restore: inverse of `wal-show` gaps. Downloads every segment listed
//! as missing in a per-timeline gap into `dst` (typically a recovery's
//! `pg_wal/` or a sidecar archive directory), respecting
//! `WALG_DOWNLOAD_CONCURRENCY`
//!
//! Idempotent: segments already present in `dst` are skipped. Failures on a
//! single segment are logged but don't abort the batch — wal-receive may
//! still be filling in the freshest segments

use std::path::Path;

use anyhow::{Context, Result};
use tokio::fs;

use crate::concurrency::BoundedTasks;
use crate::config::Settings;
use crate::pg::wal::segment::{SegmentName, wal_segment_size};
use crate::pg::wal::show;
use crate::storage::DynStorage;

pub async fn handle(
    settings: &Settings,
    storage: DynStorage,
    dst: &Path,
    timeline_filter: Option<u32>,
) -> Result<()> {
    fs::create_dir_all(dst)
        .await
        .with_context(|| format!("create_dir_all {}", dst.display()))?;

    let seg_size = wal_segment_size();
    let timelines = show::collect(storage.clone()).await?;
    let mut missing: Vec<SegmentName> = Vec::new();
    for t in &timelines {
        if let Some(filter) = timeline_filter
            && t.timeline != filter
        {
            continue;
        }
        for g in &t.gaps {
            // Re-expand the gap into the explicit segments between `from` (the
            // last present segment before the gap) and `to` (the next present
            // segment after the gap)
            let Ok(from) = SegmentName::parse(&g.from) else {
                continue;
            };
            let mut probe = from.next(seg_size);
            // Walk while we have not yet caught up to `to`
            let to_seg = SegmentName::parse(&g.to).ok();
            for _ in 0..g.missing {
                if Some(probe) == to_seg {
                    break;
                }
                missing.push(probe);
                probe = probe.next(seg_size);
            }
        }
    }

    if missing.is_empty() {
        tracing::info!(target = "wal_restore", "no missing segments to restore");
        return Ok(());
    }
    tracing::info!(
        target = "wal_restore",
        "restoring {} segment(s) into {}",
        missing.len(),
        dst.display()
    );

    // missing/transient failures don't abort the batch (wal-receive may still
    // be filling the freshest segments)
    let mut tasks = BoundedTasks::new(
        settings.download_concurrency,
        "restore",
        |(name, res): (String, Result<()>)| {
            match res {
                Ok(()) => tracing::info!(target = "wal_restore", "restored {name}"),
                Err(e) => tracing::warn!(target = "wal_restore", "skip {name}: {e:#}"),
            }
            Ok(())
        },
    );
    for seg in missing {
        let name = seg.format();
        let dst_path = dst.join(&name);
        if fs::try_exists(&dst_path).await.unwrap_or(false) {
            continue;
        }
        let st = storage.clone();
        let cfg = settings.clone();
        tasks
            .spawn(async move {
                let r =
                    super::fetch::handle(&cfg, st, &name, &dst_path, super::fetch::Prefetch::Off)
                        .await;
                (name, r)
            })
            .await?;
    }
    tasks.join().await?;
    Ok(())
}
