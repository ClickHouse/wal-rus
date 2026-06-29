//! backup-fetch: resolve backup name (or LATEST), restore tablespace
//! symlinks from sentinel `Spec`, then download + decompress + untar each
//! tar part under `basebackups_005/<name>/tar_partitions/`
//!
//! pg_control.tar applies last (sorted in `list_tar_parts`) so an
//! interrupted restore can't leave a stale pg_control behind

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use tokio_tar::Archive;
use tokio_util::io::SyncIoBridge;

use crate::compression;
use crate::concurrency::BoundedTasks;
use crate::config::Settings;
use crate::pg::backup::increment::apply_increment_in_place;
use crate::pg::backup::{
    BackupSentinelDtoV2, FilesMetadataDto, LATEST, TablespaceSpec, files_metadata_key,
    name_from_sentinel_key, sentinel_key, tar_partitions_prefix,
};
use crate::storage::{ObjExt, Operator};

#[derive(Debug, Clone, Default)]
pub struct FetchArgs {
    /// `--tablespace-mapping from=to` pairs. When set, applied to each
    /// sentinel Spec location before creating the symlink; supports
    /// relocating a tablespace at restore time
    pub tablespace_mappings: Vec<(String, String)>,
}

pub async fn handle(
    settings: &Settings,
    storage: Operator,
    name: &str,
    dst: &Path,
) -> Result<()> {
    handle_with_args(settings, storage, name, dst, &FetchArgs::default()).await
}

pub async fn handle_with_args(
    settings: &Settings,
    storage: Operator,
    name: &str,
    dst: &Path,
    args: &FetchArgs,
) -> Result<()> {
    let resolved = resolve_name(&storage, name).await?;
    tracing::info!(
        target = "backup_fetch",
        "fetching {resolved} -> {}",
        dst.display()
    );

    // Walk the delta chain leaf → root then reverse. The leaf sentinel
    // carries Spec/tablespace_mappings; intermediate ancestors share the
    // same Spec (Spec is a function of `pgdata`, not of LSN) so applying
    // symlinks from the leaf covers all parts
    let chain = build_chain(&storage, &resolved).await?;
    let leaf_sentinel = chain.last().expect("chain has leaf").1.clone();

    tokio::fs::create_dir_all(dst)
        .await
        .with_context(|| format!("create_dir_all {}", dst.display()))?;

    // PGDATA itself has no tar entry (the push walk emits its contents, not the
    // root), so its mode would otherwise be the umask default and PG refuses to
    // start on anything but 0700/0750. Subdir modes come from their tar entries
    // in unpack_entry
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(dst, std::fs::Permissions::from_mode(0o700))
            .await
            .with_context(|| format!("chmod {}", dst.display()))?;
    }

    // Restore tablespace symlinks BEFORE extracting parts so the tar crate
    // writes through the symlink rather than materializing a real dir at
    // pg_tblspc/<oid>. wal-g does the same in `EnsureSymlinkExist`
    if let Some(spec) = leaf_sentinel.sentinel.tablespace_spec.as_ref() {
        restore_tablespace_symlinks(dst, spec, &args.tablespace_mappings).await?;
    }

    if chain.len() > 1 {
        tracing::info!(
            target = "backup_fetch",
            "delta chain depth {}: {}",
            chain.len(),
            chain
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(" -> ")
        );
    }

    let download_concurrency = settings.download_concurrency.max(1);

    // Chain steps stay sequential: a delta's increments apply in place onto
    // the file the parent step wrote, so leaf-after-root ordering is a hard
    // dependency. Concurrency is confined to one backup's data parts
    for (name, _) in &chain {
        let parts = list_tar_parts(&storage, name).await?;
        if parts.is_empty() {
            bail!(
                "backup {name} has no tar parts under {}/",
                tar_partitions_prefix(name)
            );
        }
        // Increment lookup: paged files that came down wi1/native-encoded
        // need apply_increment_in_place rather than overwrite. Pulled once
        // per backup; small (~hundreds of KB even for big clusters). Arc'd so
        // the per-part tasks share one copy instead of cloning the set
        let incremented = Arc::new(fetch_incremented_set(&storage, name).await?);
        tracing::info!(
            target = "backup_fetch",
            "found {} tar part(s) for {name} ({} incremented file(s), download_concurrency={download_concurrency})",
            parts.len(),
            incremented.len(),
        );

        // pg_control parts (sorted last by list_tar_parts) must apply strictly
        // after every data part. Fan the data parts out under the download
        // bound, barrier-join, then unpack pg_control. The chain stays
        // sequential, so the leaf's pg_control remains the final write of the
        // whole restore (crash mid-restore leaves no stale-but-complete one).
        // Any part failure aborts the restore (a half-applied backup is unusable)
        let (control, data): (Vec<String>, Vec<String>) =
            parts.into_iter().partition(|k| k.contains("pg_control"));

        let mut tasks = BoundedTasks::new(download_concurrency, "download", |r: Result<()>| r);
        for key in data {
            let settings = settings.clone();
            let storage = storage.clone();
            let dst = dst.to_path_buf();
            let incremented = incremented.clone();
            tasks
                .spawn(
                    async move { unpack_part(&settings, &storage, &key, &dst, incremented).await },
                )
                .await?;
        }
        tasks.join().await?;

        for key in control {
            unpack_part(settings, &storage, &key, dst, incremented.clone()).await?;
        }
    }
    Ok(())
}

/// Walk the delta chain via sentinel `increment_from`, root-first.
/// Returns `[(name, sentinel)]` from chain root to the requested leaf.
/// A full backup yields a single-entry vec
async fn build_chain(
    storage: &Operator,
    leaf: &str,
) -> Result<Vec<(String, BackupSentinelDtoV2)>> {
    let mut out: Vec<(String, BackupSentinelDtoV2)> = Vec::new();
    let mut cur = leaf.to_string();
    let mut seen: HashSet<String> = HashSet::new();
    loop {
        if !seen.insert(cur.clone()) {
            bail!("delta chain has a cycle at {cur}");
        }
        let s = fetch_sentinel(storage, &cur).await?;
        let parent = s.sentinel.increment_from.clone();
        out.push((cur, s));
        match parent {
            Some(p) => cur = p,
            None => break,
        }
        if out.len() > 64 {
            bail!("delta chain longer than 64 steps; refusing to walk further");
        }
    }
    out.reverse();
    Ok(out)
}

async fn fetch_incremented_set(storage: &Operator, name: &str) -> Result<HashSet<String>> {
    let key = files_metadata_key(name);
    // Older backups may omit files_metadata.json; treat any load failure as
    // empty rather than propagating (matches wal-g's tolerant behaviour)
    let meta: FilesMetadataDto = match crate::pg::backup::load_json(storage, &key, 4096).await {
        Ok(m) => m,
        Err(_) => return Ok(HashSet::new()),
    };
    Ok(meta
        .files
        .into_iter()
        .filter_map(|(k, v)| {
            // wal-g records files_metadata keys with a leading `/`
            // (`GetFileRelPath`), but extraction keys off the leading-slash-
            // stripped tar path. Normalize identically so the lookup hits;
            // otherwise wal-g increments are written out as their raw wi1
            // bytes, corrupting the file (garbage page header)
            v.is_incremented.then(|| {
                strip_to_relative(Path::new(&k))
                    .to_string_lossy()
                    .into_owned()
            })
        })
        .collect())
}

/// Reduce a tar-entry / metadata path to its safe relative form, dropping
/// absolute-root, drive-prefix, `.` and `..` components. Both the extraction
/// path and the incremented-file lookup must agree on this, since wal-g and
/// walrus disagree on the leading slash
fn strip_to_relative(p: &Path) -> PathBuf {
    use std::path::Component;
    let mut rel = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Prefix(..)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => {}
            Component::Normal(s) => rel.push(s),
        }
    }
    rel
}

pub async fn resolve_name(storage: &Operator, name: &str) -> Result<String> {
    if name != LATEST {
        return Ok(name.to_string());
    }
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage.list_objs(&prefix).await?;
    let mut latest: Option<(chrono::DateTime<chrono::Utc>, String)> = None;
    while let Some(item) = stream.next().await {
        let obj = item?;
        let Some(n) = name_from_sentinel_key(&obj.key) else {
            continue;
        };
        let mtime = obj.last_modified.unwrap_or_else(chrono::Utc::now);
        match &latest {
            Some((t, _)) if *t >= mtime => {}
            _ => latest = Some((mtime, n.to_string())),
        }
    }
    latest
        .map(|(_, n)| n)
        .ok_or_else(|| anyhow!("no backups found"))
}

pub async fn fetch_sentinel(storage: &Operator, name: &str) -> Result<BackupSentinelDtoV2> {
    crate::pg::backup::load_json(storage, &sentinel_key(name), 4096).await
}

async fn restore_tablespace_symlinks(
    dst: &Path,
    spec: &TablespaceSpec,
    mappings: &[(String, String)],
) -> Result<()> {
    let pg_tblspc = dst.join("pg_tblspc");
    tokio::fs::create_dir_all(&pg_tblspc)
        .await
        .with_context(|| format!("create pg_tblspc under {}", dst.display()))?;
    for name in &spec.tablespace_names {
        let Some(loc) = spec.locations.get(name) else {
            continue;
        };
        let target = apply_mapping(&loc.location, mappings);
        // Materialize the destination dir so the tar extraction writes into it
        tokio::fs::create_dir_all(&target)
            .await
            .with_context(|| format!("create tablespace dir {target}"))?;
        let link = pg_tblspc.join(name);
        match tokio::fs::symlink_metadata(&link).await {
            Ok(_) => {
                // existing entry; replace only if it's a symlink, else bail
                let md = tokio::fs::symlink_metadata(&link).await?;
                if md.file_type().is_symlink() {
                    tokio::fs::remove_file(&link).await.ok();
                } else {
                    bail!(
                        "{} exists and is not a symlink; refusing to overwrite",
                        link.display()
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        #[cfg(unix)]
        tokio::fs::symlink(&target, &link)
            .await
            .with_context(|| format!("symlink {} -> {target}", link.display()))?;
        tracing::info!(
            target = "backup_fetch",
            "tablespace symlink {} -> {}",
            link.display(),
            target
        );
    }
    Ok(())
}

fn apply_mapping(location: &str, mappings: &[(String, String)]) -> String {
    for (from, to) in mappings {
        if location == from {
            return to.clone();
        }
    }
    location.to_string()
}

pub async fn list_tar_parts(storage: &Operator, name: &str) -> Result<Vec<String>> {
    let prefix = format!("{}/", tar_partitions_prefix(name));
    let mut stream = storage.list_objs(&prefix).await?;
    let mut keys = Vec::new();
    while let Some(item) = stream.next().await {
        let obj = item?;
        // pick up part_*.tar* and pg_control.tar* (order: data parts first, then pg_control)
        keys.push(obj.key);
    }
    keys.sort_by(|a, b| {
        let ac = a.contains("pg_control");
        let bc = b.contains("pg_control");
        ac.cmp(&bc).then_with(|| a.cmp(b))
    });
    Ok(keys)
}

async fn unpack_part(
    settings: &Settings,
    storage: &Operator,
    key: &str,
    dst: &Path,
    incremented: Arc<HashSet<String>>,
) -> Result<()> {
    let method = method_from_key(key);
    let body = storage
        .get(key)
        .await
        .with_context(|| format!("get {key}"))?;
    let throttled = settings.throttle_network(body);
    let decrypted = settings.decrypt(throttled);
    let decoded = compression::decode(method, decrypted);

    let mut archive = Archive::new(decoded);
    let mut entries = archive.entries().context("open tar entries")?;
    while let Some(entry) = entries.next().await {
        let entry = entry.context("read tar entry")?;
        unpack_entry(entry, dst, &incremented)
            .await
            .with_context(|| format!("unpack {key}"))?;
    }
    tracing::info!(target = "backup_fetch", "unpacked {key}");
    Ok(())
}

/// Restore one tar entry. PG restores legitimately follow `pg_tblspc/<oid>`
/// symlinks pointing outside `dst`, so we skip the tar crate's "stays inside
/// dst" canonicalization. File bodies bridge to a `spawn_blocking` apply path
/// because `apply_increment_in_place` needs `Seek`
async fn unpack_entry<R>(
    entry: tokio_tar::Entry<R>,
    dst: &Path,
    incremented: &HashSet<String>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let path = entry.path().context("entry path")?.into_owned();
    // Skip absolute / parent-dir traversals
    let rel = strip_to_relative(&path);
    if rel.as_os_str().is_empty() {
        return Ok(());
    }
    let target = dst.join(&rel);
    let header = entry.header().clone();
    let etype = header.entry_type();
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if etype.is_dir() {
        match tokio::fs::create_dir(&target).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        // Apply the archived directory mode (wal-g chmods restored dirs too).
        // Without this dirs land at the umask default (0775) and PG rejects the
        // restored data dir; files already get their mode applied below
        #[cfg(unix)]
        if let Ok(mode) = header.mode() {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode))
                .await
                .with_context(|| format!("chmod dir {}", target.display()))?;
        }
        return Ok(());
    }
    if etype.is_symlink() {
        // pg_tblspc/<oid> links are restored up-front from the sentinel
        // TablespaceSpec (mapping-aware) before any part unpacks. Recreating
        // them from a part entry would race the concurrent data fan-out — its
        // remove+recreate window vs another part materializing the link's
        // pg_tblspc/<oid>/... contents — and would clobber a
        // --tablespace-mapping relocation with the archived (backup-time)
        // target. PG basebackup emits symlinks only under pg_tblspc, so the
        // sentinel link is authoritative; skip the entry
        if rel.parent() == Some(Path::new("pg_tblspc")) {
            return Ok(());
        }
        #[cfg(unix)]
        {
            let link = header
                .link_name()
                .context("symlink target")?
                .ok_or_else(|| anyhow!("symlink without target"))?;
            // best-effort overwrite
            let _ = tokio::fs::remove_file(&target).await;
            tokio::fs::symlink(link.as_ref(), &target).await?;
        }
        return Ok(());
    }
    // ignore fifo, char/block devices — none appear in a PG basebackup. Hard
    // links are treated like regular files (basebackup emits none)
    if !(etype.is_file() || etype.is_hard_link()) {
        return Ok(());
    }

    let path_key = rel.to_string_lossy().into_owned();
    let is_increment = incremented.contains(&path_key);
    let target = target.clone();
    let mode = header.mode().ok();
    let bridge = SyncIoBridge::new(entry);
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::Write;
        let mut bridge = bridge;
        if is_increment {
            // Increment path: apply onto whatever the earlier chain step left
            // in place. The target must already exist (chain root wrote the
            // full file). open() in r+w (not truncate)
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&target)
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("apply increment {path_key}: open target: {e}"),
                    )
                })?;
            let (final_size, _, _) =
                apply_increment_in_place(&mut bridge, &mut f).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("apply increment {path_key}: {e}"),
                    )
                })?;
            f.set_len(final_size)?;
            f.flush()?;
        } else {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&target)?;
            std::io::copy(&mut bridge, &mut f)?;
            f.flush()?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(mode) = mode {
                let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode));
            }
        }
        Ok(())
    })
    .await
    .context("unpack file join")??;
    Ok(())
}

fn method_from_key(key: &str) -> compression::Method {
    let ext = key.rsplit('.').next().unwrap_or("");
    compression::Method::from_extension(ext).unwrap_or(compression::Method::None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tar part whose only file is flagged incremented, restored into an
    /// empty dir: `apply_increment_in_place` needs the base file the parent
    /// chain step wrote, so the missing-target open must surface a wrapped,
    /// path-tagged error rather than a bare io error
    #[tokio::test]
    async fn increment_apply_wraps_missing_target_error() {
        let dir = tempfile::tempdir().unwrap();
        let storage: Operator = crate::storage::fs_operator(dir.path());

        let mut builder = tar::Builder::new(Vec::new());
        let data = crate::pg::backup::increment::INCREMENT_MAGIC.to_vec();
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        builder
            .append_data(&mut header, "base/16384/16385", &data[..])
            .unwrap();
        let tar_bytes = builder.into_inner().unwrap();

        let key = "basebackups_005/base_test/tar_partitions/part_001.tar";
        let len = tar_bytes.len() as u64;
        let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(tar_bytes));
        storage.put(key, r, Some(len)).await.unwrap();

        let restore = dir.path().join("restore");
        let settings = Settings::default();
        let incremented = Arc::new(HashSet::from(["base/16384/16385".to_string()]));
        let err = unpack_part(&settings, &storage, key, &restore, incremented)
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("apply increment"), "{msg}");
        assert!(msg.contains("open target"), "{msg}");
    }

    /// A tar dir entry carries its mode; restore must apply it. PG refuses to
    /// start on a data dir that isn't 0700/0750, and dirs previously landed at
    /// the umask default (0775) because only files got their mode set
    #[cfg(unix)]
    #[tokio::test]
    async fn restores_directory_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let storage: Operator = crate::storage::fs_operator(dir.path());

        let mut builder = tar::Builder::new(Vec::new());
        let mut dh = tar::Header::new_gnu();
        dh.set_size(0);
        dh.set_mode(0o700);
        dh.set_entry_type(tar::EntryType::Directory);
        builder
            .append_data(&mut dh, "global", std::io::empty())
            .unwrap();
        let data = b"x".to_vec();
        let mut fh = tar::Header::new_gnu();
        fh.set_size(data.len() as u64);
        fh.set_mode(0o600);
        fh.set_entry_type(tar::EntryType::Regular);
        builder
            .append_data(&mut fh, "global/pg_filenode.map", &data[..])
            .unwrap();
        let tar_bytes = builder.into_inner().unwrap();

        let key = "basebackups_005/base_test/tar_partitions/part_001.tar";
        let len = tar_bytes.len() as u64;
        let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(tar_bytes));
        storage.put(key, r, Some(len)).await.unwrap();

        let restore = dir.path().join("restore");
        let settings = Settings::default();
        unpack_part(&settings, &storage, key, &restore, Arc::new(HashSet::new()))
            .await
            .unwrap();

        let dir_mode = std::fs::metadata(restore.join("global"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "restored dir mode");
        let file_mode = std::fs::metadata(restore.join("global/pg_filenode.map"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "restored file mode");
    }
}
