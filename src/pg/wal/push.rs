//! wal-push: read local WAL segment, compress, upload to wal_005/<name>.<ext>

use std::path::Path;

use anyhow::{Context, Result, bail};
use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::compression;
use crate::config::Settings;
use crate::pg;
use crate::storage::DynStorage;

use super::segment::{is_history_filename, is_wal_filename};

pub async fn handle(settings: &Settings, storage: DynStorage, src_path: &Path) -> Result<()> {
    let name = src_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("bad wal path: {}", src_path.display()))?
        .to_string();

    // history files always uncompressed; wal segments use configured method
    let history = is_history_filename(&name);
    if !history && !is_wal_filename(&name) {
        tracing::warn!("uploading non-wal-named file {name}");
    }

    let method = if history {
        compression::Method::None
    } else {
        settings.compression
    };
    let ext = method.extension();
    let key = if ext.is_empty() {
        format!("{}/{}", pg::WAL_FOLDER, name)
    } else {
        format!("{}/{}.{}", pg::WAL_FOLDER, name, ext)
    };

    // history files: always idempotent — PG can rewrite the same .history after
    // a promotion. wal segments: gated by WALG_PREVENT_WAL_OVERWRITE.
    let must_compare = history || settings.prevent_wal_overwrite;
    if must_compare && storage.exists(&key).await.context("check existence")? {
        let matches = compare_existing(&storage, &key, method, src_path)
            .await
            .with_context(|| format!("content-compare {key}"))?;
        if matches {
            tracing::info!(
                target = "wal_push",
                "{key} already archived with identical bytes; skipping upload"
            );
            promote_ready_to_done(src_path, &name).await;
            return Ok(());
        }
        bail!("WAL {key} already present with different bytes (prevent-wal-overwrite)");
    }

    let meta = fs::metadata(src_path)
        .await
        .with_context(|| format!("stat {}", src_path.display()))?;
    let size = meta.len();
    let file = fs::File::open(src_path)
        .await
        .with_context(|| format!("open {}", src_path.display()))?;
    let reader: compression::AsyncReader = settings.throttle_disk(Box::pin(file));

    let compressed = compression::encode(method, reader, settings.compression_level);
    let body = settings.throttle_network(compressed);

    let size_hint = if matches!(method, compression::Method::None) {
        Some(size)
    } else {
        None
    };

    storage
        .put(&key, body, size_hint)
        .await
        .with_context(|| format!("put {key}"))?;
    tracing::info!(
        target = "wal_push",
        "uploaded {key} ({} bytes source)",
        size
    );

    promote_ready_to_done(src_path, &name).await;
    Ok(())
}

/// Compare existing object's decoded bytes against a local file. Returns true
/// when identical, false when length or any byte differs. Streams both sides
/// so a 16 MB segment doesn't materialize in memory
async fn compare_existing(
    storage: &DynStorage,
    key: &str,
    method: compression::Method,
    src_path: &Path,
) -> Result<bool> {
    let remote = storage.get(key).await.context("get for compare")?;
    let mut decoded = compression::decode(method, remote);
    let mut local = fs::File::open(src_path)
        .await
        .with_context(|| format!("open {} for compare", src_path.display()))?;

    let mut a = vec![0u8; 64 * 1024];
    let mut b = vec![0u8; 64 * 1024];
    loop {
        let mut na = 0;
        while na < a.len() {
            let n = decoded.read(&mut a[na..]).await?;
            if n == 0 {
                break;
            }
            na += n;
        }
        let mut nb = 0;
        while nb < b.len() {
            let n = local.read(&mut b[nb..]).await?;
            if n == 0 {
                break;
            }
            nb += n;
        }
        if na != nb || a[..na] != b[..nb] {
            return Ok(false);
        }
        if na == 0 {
            return Ok(true);
        }
    }
}

/// Rename `<pgdata>/pg_wal/archive_status/<seg>.ready` → `.done` after a
/// successful archive. Matches PG's archiver bookkeeping, which wal-g also
/// performs so subsequent `archive_command` invocations stay quiet
///
/// Errors are non-fatal: if the marker is missing or the directory isn't
/// reachable (eg backup-sidecar deployment), the rename is silently skipped
async fn promote_ready_to_done(src_path: &Path, name: &str) {
    let Some(parent) = src_path.parent() else {
        return;
    };
    let ready = parent.join("archive_status").join(format!("{name}.ready"));
    let done = parent.join("archive_status").join(format!("{name}.done"));
    match fs::rename(&ready, &done).await {
        Ok(()) => tracing::debug!(?ready, ?done, "promoted .ready to .done"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(?ready, "archive_status .ready not present; skipping rename");
        }
        Err(e) => {
            tracing::warn!(?ready, error = %e, "failed to rename .ready to .done");
        }
    }
}
