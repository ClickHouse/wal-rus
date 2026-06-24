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
    use crate::pg::wal::segment::wal_segment_size;
    use crate::pg::wal::segment_file::classify_segment_name;
    let seg_size = wal_segment_size();
    let wal_prefix = format!("{}/", crate::pg::WAL_FOLDER);
    let mut s = src
        .list(&wal_prefix)
        .await
        .with_context(|| format!("list {wal_prefix}"))?;
    let segs_per_log = 0x1_0000_0000u64 / seg_size;
    let start = backup.start_seg_no;
    let finish = backup.finish_lsn / seg_size;
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::pg::backup::{
        BackupSentinelDto, BackupSentinelDtoV2, format_backup_name, sentinel_key,
    };
    use crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;
    use crate::storage::fs::FsStorage;
    use crate::storage::{
        AsyncReader, ObjectStream, Result as StorageResult, Storage, StorageError,
    };
    use std::sync::Arc;

    fn fs(dir: &std::path::Path) -> DynStorage {
        Arc::new(FsStorage::new(dir).unwrap())
    }

    async fn put_bytes(store: &DynStorage, key: &str, bytes: Vec<u8>) {
        let len = bytes.len() as u64;
        let r: AsyncReader = Box::pin(std::io::Cursor::new(bytes));
        store.put(key, r, Some(len)).await.unwrap();
    }

    async fn seed_backup(store: &DynStorage, name: &str, start_lsn: u64, finish_lsn: u64) {
        let v2 = BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: NonZeroU64::new(start_lsn),
                backup_finish_lsn: NonZeroU64::new(finish_lsn),
                pg_version: 160003,
                ..Default::default()
            },
            hostname: "h".into(),
            data_dir: "/d".into(),
            ..Default::default()
        };
        put_bytes(store, &sentinel_key(name), serde_json::to_vec(&v2).unwrap()).await;
    }

    async fn list_keys(store: &DynStorage, prefix: &str) -> Vec<String> {
        let mut s = store.list(prefix).await.unwrap();
        let mut out = Vec::new();
        while let Some(item) = s.next().await {
            out.push(item.unwrap().key);
        }
        out.sort();
        out
    }

    fn args(backup_name: Option<&str>, all: bool, with_history: bool) -> CopyArgs {
        CopyArgs {
            backup_name: backup_name.map(str::to_string),
            all,
            with_history,
        }
    }

    #[tokio::test]
    async fn all_and_backup_name_are_mutually_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let err = handle(
            &Settings::default(),
            fs(dir.path()),
            fs(&dir.path().join("dst")),
            args(Some("x"), true, false),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("mutually exclusive"), "{err:#}");
    }

    #[tokio::test]
    async fn empty_source_copies_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let dst = fs(&dir.path().join("dst"));
        handle(
            &Settings::default(),
            fs(dir.path()),
            dst.clone(),
            args(None, true, false),
        )
        .await
        .unwrap();
        assert!(list_keys(&dst, "").await.is_empty());
    }

    #[tokio::test]
    async fn all_clones_every_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let src = fs(dir.path());
        let seg = DEFAULT_WAL_SEG_SIZE;
        let a = format_backup_name(1, seg, seg);
        let b = format_backup_name(1, 3 * seg, seg);
        seed_backup(&src, &a, seg, seg + 0x100).await;
        seed_backup(&src, &b, 3 * seg, 3 * seg + 0x100).await;
        let dst = fs(&dir.path().join("dst"));
        handle(
            &Settings::default(),
            src,
            dst.clone(),
            args(None, true, false),
        )
        .await
        .unwrap();
        assert!(dst.exists(&sentinel_key(&a)).await.unwrap());
        assert!(dst.exists(&sentinel_key(&b)).await.unwrap());
    }

    #[tokio::test]
    async fn specific_backup_windows_wal_and_passes_history() {
        let dir = tempfile::tempdir().unwrap();
        let src = fs(dir.path());
        let seg = DEFAULT_WAL_SEG_SIZE;
        let name = format_backup_name(1, 2 * seg, seg); // start_seg_no = 2
        seed_backup(&src, &name, 2 * seg, 3 * seg + 0x100).await; // finish global = 3
        let seg_name = |g: u32| format!("{:08X}{:08X}{:08X}", 1u32, 0u32, g);
        for g in 1u32..=4 {
            put_bytes(
                &src,
                &format!("{}/{}", crate::pg::WAL_FOLDER, seg_name(g)),
                vec![0u8; 16],
            )
            .await;
        }
        put_bytes(
            &src,
            &format!("{}/00000001.history", crate::pg::WAL_FOLDER),
            b"1\t0/0\tno reason\n".to_vec(),
        )
        .await;

        let dst = fs(&dir.path().join("dst"));
        handle(
            &Settings::default(),
            src,
            dst.clone(),
            args(Some(&name), false, false),
        )
        .await
        .unwrap();

        let wal = list_keys(&dst, &format!("{}/", crate::pg::WAL_FOLDER)).await;
        let has = |g: u32| wal.iter().any(|k| k.ends_with(&seg_name(g)));
        assert!(has(2) && has(3), "in-window segments copied: {wal:?}");
        assert!(
            !has(1) && !has(4),
            "out-of-window segments skipped: {wal:?}"
        );
        assert!(
            wal.iter().any(|k| k.ends_with("00000001.history")),
            "history passthrough: {wal:?}"
        );
    }

    /// Destination whose `put` always fails — drives the best-effort sweep's
    /// failure accumulation + `last_err` return
    struct FailPut;
    #[async_trait::async_trait]
    impl Storage for FailPut {
        fn describe(&self) -> String {
            "failput".into()
        }
        async fn put(&self, key: &str, _b: AsyncReader, _h: Option<u64>) -> StorageResult<()> {
            Err(StorageError::Transport(format!("put {key} denied")))
        }
        async fn get(&self, _k: &str) -> StorageResult<AsyncReader> {
            Err(StorageError::Unimplemented("get"))
        }
        async fn exists(&self, _k: &str) -> StorageResult<bool> {
            Ok(false)
        }
        async fn list(&self, _p: &str) -> StorageResult<ObjectStream> {
            Err(StorageError::Unimplemented("list"))
        }
        async fn delete(&self, _k: &str) -> StorageResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn copy_failures_surface_last_error() {
        let dir = tempfile::tempdir().unwrap();
        let src = fs(dir.path());
        let seg = DEFAULT_WAL_SEG_SIZE;
        let name = format_backup_name(1, seg, seg);
        seed_backup(&src, &name, seg, seg + 0x100).await;
        let err = handle(
            &Settings::default(),
            src,
            Arc::new(FailPut),
            args(Some(&name), false, false),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("denied"), "{err:#}");
    }
}
