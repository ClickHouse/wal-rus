//! backup-fetch: resolve backup name (or LATEST), restore tablespace
//! symlinks from sentinel `Spec`, then download + decompress + untar each
//! tar part under `basebackups_005/<name>/tar_partitions/`
//!
//! pg_control.tar applies last (sorted in `list_tar_parts`) so an
//! interrupted restore can't leave a stale pg_control behind

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use tokio_util::io::SyncIoBridge;

use crate::compression;
use crate::config::Settings;
use crate::pg::backup::increment::apply_increment_in_place;
use crate::pg::backup::{
    BackupSentinelDtoV2, FilesMetadataDto, LATEST, TablespaceSpec, files_metadata_key,
    name_from_sentinel_key, sentinel_key, tar_partitions_prefix,
};
use crate::storage::DynStorage;

#[derive(Debug, Clone, Default)]
pub struct FetchArgs {
    /// `--tablespace-mapping from=to` pairs. When set, applied to each
    /// sentinel Spec location before creating the symlink; supports
    /// relocating a tablespace at restore time
    pub tablespace_mappings: Vec<(String, String)>,
}

pub async fn handle(
    settings: &Settings,
    storage: DynStorage,
    name: &str,
    dst: &Path,
) -> Result<()> {
    handle_with_args(settings, storage, name, dst, &FetchArgs::default()).await
}

pub async fn handle_with_args(
    settings: &Settings,
    storage: DynStorage,
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
        // per backup; small (~hundreds of KB even for big clusters)
        let incremented = fetch_incremented_set(&storage, name).await?;
        tracing::info!(
            target = "backup_fetch",
            "found {} tar part(s) for {name} ({} incremented file(s))",
            parts.len(),
            incremented.len(),
        );
        for key in &parts {
            unpack_part(settings, &storage, key, dst, &incremented).await?;
        }
    }
    Ok(())
}

/// Walk the delta chain via sentinel `increment_from`, root-first.
/// Returns `[(name, sentinel)]` from chain root to the requested leaf.
/// A full backup yields a single-entry vec
async fn build_chain(
    storage: &DynStorage,
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

async fn fetch_incremented_set(storage: &DynStorage, name: &str) -> Result<HashSet<String>> {
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
        .filter_map(|(k, v)| if v.is_incremented { Some(k) } else { None })
        .collect())
}

pub async fn resolve_name(storage: &DynStorage, name: &str) -> Result<String> {
    if name != LATEST {
        return Ok(name.to_string());
    }
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage.list(&prefix).await?;
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

pub async fn fetch_sentinel(storage: &DynStorage, name: &str) -> Result<BackupSentinelDtoV2> {
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

pub async fn list_tar_parts(storage: &DynStorage, name: &str) -> Result<Vec<String>> {
    let prefix = format!("{}/", tar_partitions_prefix(name));
    let mut stream = storage.list(&prefix).await?;
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
    storage: &DynStorage,
    key: &str,
    dst: &Path,
    incremented: &HashSet<String>,
) -> Result<()> {
    let method = method_from_key(key);
    let body = storage
        .get(key)
        .await
        .with_context(|| format!("get {key}"))?;
    let throttled = settings.throttle_network(body);
    let decrypted = settings.decrypt(throttled);
    let decoded = compression::decode(method, decrypted);
    let dst: PathBuf = dst.to_path_buf();
    let incremented = incremented.clone();

    let res: std::io::Result<()> = tokio::task::spawn_blocking(move || {
        let sync_r = SyncIoBridge::new(decoded);
        let mut archive = tar::Archive::new(sync_r);
        unpack_manual(&mut archive, &dst, &incremented)
    })
    .await
    .context("tar unpack join")?;
    res.with_context(|| format!("unpack {key}"))?;
    tracing::info!(target = "backup_fetch", "unpacked {key}");
    Ok(())
}

/// Manual tar extraction without the `tar` crate's "stays inside dst"
/// canonicalization check. PG restores legitimately need to follow
/// `pg_tblspc/<oid>` symlinks that point outside `dst` — the safe-extract
/// behavior in `tar::Archive::unpack` refuses that
fn unpack_manual<R: std::io::Read>(
    archive: &mut tar::Archive<R>,
    dst: &Path,
    incremented: &HashSet<String>,
) -> std::io::Result<()> {
    use std::io::Write;
    use std::path::Component;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        // Skip absolute / parent-dir traversals
        let mut rel = PathBuf::new();
        for c in path.components() {
            match c {
                Component::Prefix(..) | Component::RootDir | Component::CurDir => continue,
                Component::ParentDir => continue,
                Component::Normal(p) => rel.push(p),
            }
        }
        if rel.as_os_str().is_empty() {
            continue;
        }
        let target = dst.join(&rel);
        let header = entry.header().clone();
        let etype = header.entry_type();
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if etype.is_dir() {
            match std::fs::create_dir(&target) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
        } else if etype.is_symlink() {
            #[cfg(unix)]
            {
                let link = header.link_name()?.ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "symlink without target")
                })?;
                // best-effort overwrite
                let _ = std::fs::remove_file(&target);
                std::os::unix::fs::symlink(link.as_ref(), &target)?;
            }
        } else if etype.is_file() || etype.is_hard_link() {
            // ignore hard links to keep this simple; PG basebackup doesn't emit any
            let path_key = rel.to_string_lossy().into_owned();
            if incremented.contains(&path_key) {
                // Increment path: apply onto whatever the earlier chain step
                // left in place. The target must already exist (chain root
                // wrote the full file). open() in r+w (not truncate)
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
                    apply_increment_in_place(&mut entry, &mut f).map_err(|e| {
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
                std::io::copy(&mut entry, &mut f)?;
                f.flush()?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(mode) = header.mode() {
                    let _ =
                        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode));
                }
            }
        }
        // entry types we don't restore: hard links, fifo, char/block devices —
        // none appear in a PG basebackup
    }
    Ok(())
}

fn method_from_key(key: &str) -> compression::Method {
    let ext = key.rsplit('.').next().unwrap_or("");
    compression::Method::from_extension(ext).unwrap_or(compression::Method::None)
}
