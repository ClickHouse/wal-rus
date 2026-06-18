//! Cross-prefix backup copy
//!
//! Copies one or all backups to a different prefix under the same storage
//! backend (same credentials, same bucket). Cross-backend / cross-bucket
//! copies fall back to stream-through with the same flow when a future
//! caller supplies a second `DynStorage`
//!
//! The implementation walks source-side object listings (basebackup-relative
//! +, optionally, WAL-relative) and copies each key server-side
//! (`x-amz-copy-source` / GCS `rewriteTo`) when both handles sit on the same
//! backend, falling back to piping `get` into `put` otherwise

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;

use crate::concurrency::BoundedTasks;
use crate::config::Settings;
use crate::pg::backup::delete::{BackupRecord, collect_records};
use crate::pg::backup::fetch::resolve_name;
use crate::storage::DynStorage;

#[derive(Debug, Clone)]
pub struct CopyArgs {
    /// `--backup-name <name>` or LATEST. When absent and `all=true`, copy every backup
    pub backup_name: Option<String>,
    /// `--all`: copy every backup (forbidden together with backup_name)
    pub all: bool,
    /// `--with-history`: copy WAL segments older than `start_lsn`, too
    pub with_history: bool,
}

/// Copy from `src` to `dst`. `dst` is constructed by the caller (typically a
/// second `build_storage` with an overridden prefix). When `dst` points at
/// the same backend that's a same-cred cross-prefix copy; when different,
/// the same stream-through path applies
pub async fn handle(
    settings: &Settings,
    src: DynStorage,
    dst: DynStorage,
    args: CopyArgs,
) -> Result<()> {
    if args.all && args.backup_name.is_some() {
        bail!("--all and --backup-name are mutually exclusive");
    }
    let backups = collect_records(&src).await?;
    let to_copy: Vec<BackupRecord> = if args.all {
        backups.clone()
    } else {
        let name = args
            .backup_name
            .as_deref()
            .ok_or_else(|| anyhow!("--backup-name or --all is required"))?;
        let resolved = resolve_name(&src, name).await?;
        backups
            .iter()
            .find(|b| b.name == resolved)
            .cloned()
            .map(|b| vec![b])
            .ok_or_else(|| anyhow!("backup {resolved} not found"))?
    };

    if to_copy.is_empty() {
        tracing::info!(target = "copy", "no backups to copy");
        return Ok(());
    }

    let mut keys: Vec<String> = Vec::new();
    for b in &to_copy {
        collect_backup_keys(&src, &b.name, &mut keys).await?;
    }
    if args.with_history || !args.all {
        // wal-g semantics: for a specific backup the WAL window is
        // [start_lsn, finish_lsn] by default, or all-older WAL with --with-history.
        // Whole-bucket `--all` without history doesn't sweep WAL; otherwise we copy
        // the windowed range (per resolved record).
        for b in &to_copy {
            collect_wal_keys(&src, b, args.with_history, &mut keys).await?;
        }
    }

    let mut last_err: Option<anyhow::Error> = None;
    // a failure is logged and remembered but doesn't abort the batch
    // (best-effort sweep); the last error returns at the end
    let mut tasks = BoundedTasks::new(
        settings.upload_concurrency,
        "copy",
        |(key, res): (String, Result<()>)| {
            match res {
                Ok(()) => tracing::info!(target = "copy", "copied {key}"),
                Err(e) => {
                    tracing::warn!(target = "copy", "copy {key}: {e:#}");
                    last_err = Some(e);
                }
            }
            Ok(())
        },
    );
    for k in keys {
        let src = src.clone();
        let dst = dst.clone();
        tasks
            .spawn(async move {
                let r = copy_one(&src, &dst, &k).await;
                (k, r)
            })
            .await?;
    }
    tasks.join().await?;
    if let Some(e) = last_err {
        return Err(e);
    }
    Ok(())
}

async fn copy_one(src: &DynStorage, dst: &DynStorage, key: &str) -> Result<()> {
    // server-side first; any failure falls back to stream-through, which can
    // still succeed where one-sided auth can't (src & dst use separate creds)
    if let Some(loc) = src.copy_source(key) {
        match dst.copy_within(&loc, key).await {
            Ok(()) => return Ok(()),
            Err(crate::storage::StorageError::Unimplemented(_)) => {}
            Err(e) => {
                tracing::debug!(target = "copy", "server-side copy {key}: {e}; streaming")
            }
        }
    }
    let body = src.get(key).await.with_context(|| format!("get {key}"))?;
    dst.put(key, body, None)
        .await
        .with_context(|| format!("put {key}"))?;
    Ok(())
}

async fn collect_backup_keys(src: &DynStorage, name: &str, out: &mut Vec<String>) -> Result<()> {
    // Per-backup prefix holds files_metadata.json, metadata.json,
    // tar_partitions/part_NNN.tar.*. The sentinel lives at `<basebackups>/`
    let backup_prefix = format!("{}/{}/", crate::pg::BASEBACKUP_FOLDER, name);
    let mut s = src
        .list(&backup_prefix)
        .await
        .with_context(|| format!("list {backup_prefix}"))?;
    while let Some(item) = s.next().await {
        let obj = item.context("list iteration")?;
        out.push(obj.key);
    }
    out.push(crate::pg::backup::sentinel_key(name));
    Ok(())
}

async fn collect_wal_keys(
    src: &DynStorage,
    backup: &BackupRecord,
    with_history: bool,
    out: &mut Vec<String>,
) -> Result<()> {
    use crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;
    use crate::pg::wal::segment_file::classify_segment_name;
    let wal_prefix = format!("{}/", crate::pg::WAL_FOLDER);
    let mut s = src
        .list(&wal_prefix)
        .await
        .with_context(|| format!("list {wal_prefix}"))?;
    let segs_per_log = 0x1_0000_0000u64 / DEFAULT_WAL_SEG_SIZE;
    let start = backup.start_seg_no;
    let finish = backup.finish_lsn / DEFAULT_WAL_SEG_SIZE;
    while let Some(item) = s.next().await {
        let obj = item.context("list iteration")?;
        let bare = obj.key.rsplit('/').next().unwrap_or(&obj.key);
        let Ok((seg, _)) = classify_segment_name(bare) else {
            // history files always copied (small, useful for downstream)
            if bare.ends_with(".history") {
                out.push(obj.key);
            }
            continue;
        };
        if seg.timeline != backup.timeline {
            continue;
        }
        let global = (seg.log_id as u64) * segs_per_log + seg.seg_no as u64;
        if with_history {
            if global <= finish {
                out.push(obj.key);
            }
        } else if global >= start && global <= finish {
            out.push(obj.key);
        }
    }
    Ok(())
}
