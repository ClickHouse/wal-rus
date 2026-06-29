//! `st check` storage access probes

use anyhow::{Result, anyhow, bail};
use futures::StreamExt;

use super::{AsyncReader, ObjExt, Operator};

/// `st check read [names...]`: drive a root LIST (proves read access), then
/// assert each named object exists. An empty bucket still passes. Mirrors
/// wal-g `HandleCheckRead`
pub async fn read(storage: &Operator, names: &[String]) -> Result<()> {
    let mut stream = storage
        .list_objs("")
        .await
        .map_err(|e| anyhow!("failed to list the storage: {e}"))?;
    // Poll once to force the request and surface auth/permission errors. An
    // empty listing yields None without error, which is a passing read.
    if let Some(first) = stream.next().await {
        first.map_err(|e| anyhow!("failed to list the storage: {e}"))?;
    }
    let mut missing = Vec::new();
    for name in names {
        if !matches!(storage.exists(name).await, Ok(true)) {
            missing.push(name.clone());
        }
    }
    if !missing.is_empty() {
        bail!("files are missing: {}", missing.join(", "));
    }
    tracing::info!("Read check OK");
    Ok(())
}

/// `st check write`: write then delete a probe object. Mirrors wal-g
/// `HandleCheckWrite`; cleanup is best-effort, leaving the probe on delete
/// failure as wal-g does
pub async fn write(storage: &Operator) -> Result<()> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let name = format!("walrus_check_{nanos:032x}");
    let body = b"test";
    let reader: AsyncReader = Box::pin(std::io::Cursor::new(body));
    let put = storage.put(&name, reader, Some(body.len() as u64)).await;
    if storage.delete(&name).await.is_err() {
        tracing::warn!("failed to clean temp file, {name} left in storage");
    }
    put.map_err(|e| anyhow!("failed to write to the storage: {e}"))?;
    tracing::info!("Write check OK");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{read, write};
    use crate::storage::{AsyncReader, ObjExt, fs_operator};
    use futures::StreamExt;

    fn reader(bytes: &[u8]) -> AsyncReader {
        Box::pin(std::io::Cursor::new(bytes.to_vec()))
    }

    #[tokio::test]
    async fn read_passes_when_named_objects_exist() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_operator(dir.path());
        s.put("base/a", reader(b"x"), None).await.unwrap();
        s.put("base/b", reader(b"y"), None).await.unwrap();
        read(&s, &["base/a".into(), "base/b".into()]).await.unwrap();
    }

    #[tokio::test]
    async fn read_passes_on_empty_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_operator(dir.path());
        read(&s, &[]).await.unwrap();
    }

    #[tokio::test]
    async fn read_bails_on_missing_objects() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_operator(dir.path());
        s.put("present", reader(b"x"), None).await.unwrap();
        let err = read(&s, &["present".into(), "absent".into()])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("files are missing"), "{msg}");
        assert!(msg.contains("absent") && !msg.contains("present"), "{msg}");
    }

    #[tokio::test]
    async fn write_probes_then_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_operator(dir.path());
        write(&s).await.unwrap();
        // probe deleted: bucket left empty
        let mut listed = s.list_objs("").await.unwrap();
        assert!(listed.next().await.is_none(), "probe object left behind");
    }
}
