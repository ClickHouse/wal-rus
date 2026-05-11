//! backup-fetch: resolve backup name (or LATEST), restore tablespace
//! symlinks from sentinel `Spec`, then download + decompress + untar each
//! tar part under `basebackups_005/<name>/tar_partitions/`
//!
//! pg_control.tar applies last (sorted in `list_tar_parts`) so an
//! interrupted restore can't leave a stale pg_control behind

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tokio_util::io::SyncIoBridge;

use crate::compression;
use crate::config::Settings;
use crate::pg::backup::{
    BackupSentinelDtoV2, LATEST, TablespaceSpec, name_from_sentinel_key, sentinel_key,
    tar_partitions_prefix,
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

    let sentinel = fetch_sentinel(&storage, &resolved).await?;

    tokio::fs::create_dir_all(dst)
        .await
        .with_context(|| format!("create_dir_all {}", dst.display()))?;

    // Restore tablespace symlinks BEFORE extracting parts so the tar crate
    // writes through the symlink rather than materializing a real dir at
    // pg_tblspc/<oid>. wal-g does the same in `EnsureSymlinkExist`
    if let Some(spec) = sentinel.sentinel.tablespace_spec.as_ref() {
        restore_tablespace_symlinks(dst, spec, &args.tablespace_mappings).await?;
    }

    let parts = list_tar_parts(&storage, &resolved).await?;
    if parts.is_empty() {
        bail!(
            "backup {resolved} has no tar parts under {}/",
            tar_partitions_prefix(&resolved)
        );
    }
    tracing::info!(target = "backup_fetch", "found {} tar part(s)", parts.len());

    for key in parts {
        unpack_part(settings, &storage, &key, dst).await?;
    }
    Ok(())
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

async fn fetch_sentinel(storage: &DynStorage, name: &str) -> Result<BackupSentinelDtoV2> {
    let key = sentinel_key(name);
    let mut r = storage
        .get(&key)
        .await
        .with_context(|| format!("get {key}"))?;
    let mut buf = Vec::with_capacity(4096);
    r.read_to_end(&mut buf).await?;
    let v2: BackupSentinelDtoV2 =
        serde_json::from_slice(&buf).with_context(|| format!("parse {key}"))?;
    Ok(v2)
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

async fn list_tar_parts(storage: &DynStorage, name: &str) -> Result<Vec<String>> {
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

    let res: std::io::Result<()> = tokio::task::spawn_blocking(move || {
        let sync_r = SyncIoBridge::new(decoded);
        let mut archive = tar::Archive::new(sync_r);
        unpack_manual(&mut archive, &dst)
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
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&target)?;
            std::io::copy(&mut entry, &mut f)?;
            f.flush()?;
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
