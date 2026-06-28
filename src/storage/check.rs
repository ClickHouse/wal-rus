//! `st check` storage access probes

use anyhow::{Result, anyhow, bail};
use futures::StreamExt;

use super::{AsyncReader, DynStorage};

/// `st check read [names...]`: drive a root LIST (proves read access), then
/// assert each named object exists. An empty bucket still passes. Mirrors
/// wal-g `HandleCheckRead`
pub async fn read(storage: &DynStorage, names: &[String]) -> Result<()> {
    let mut stream = storage
        .list("")
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
pub async fn write(storage: &DynStorage) -> Result<()> {
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
    use crate::storage::fs::FsStorage;
    use crate::storage::{
        AsyncReader, DynStorage, ObjectMeta, ObjectStream, Result as StorageResult, Storage,
        StorageError,
    };
    use async_trait::async_trait;
    use futures::{StreamExt, stream};
    use std::collections::HashSet;
    use std::sync::Arc;

    fn reader(bytes: &[u8]) -> AsyncReader {
        Box::pin(std::io::Cursor::new(bytes.to_vec()))
    }

    fn fs_storage() -> (tempfile::TempDir, DynStorage) {
        let dir = tempfile::tempdir().unwrap();
        let s: DynStorage = Arc::new(FsStorage::new(dir.path()).unwrap());
        (dir, s)
    }

    /// Storage stub driving check.rs error branches; only the methods check.rs
    /// calls return anything meaningful
    #[derive(Default)]
    struct Mock {
        existing: HashSet<String>,
        list: ListMode,
        put_err: bool,
        delete_err: bool,
    }

    #[derive(Default)]
    enum ListMode {
        #[default]
        Empty,
        ItemErr,
        ListErr,
    }

    #[async_trait]
    impl Storage for Mock {
        fn describe(&self) -> String {
            "mock".into()
        }
        async fn put(
            &self,
            _key: &str,
            _body: AsyncReader,
            _hint: Option<u64>,
        ) -> StorageResult<()> {
            if self.put_err {
                Err(StorageError::Transport("put boom".into()))
            } else {
                Ok(())
            }
        }
        async fn get(&self, key: &str) -> StorageResult<AsyncReader> {
            Err(StorageError::NotFound(key.into()))
        }
        async fn exists(&self, key: &str) -> StorageResult<bool> {
            Ok(self.existing.contains(key))
        }
        async fn list(&self, _prefix: &str) -> StorageResult<ObjectStream> {
            match self.list {
                ListMode::ListErr => Err(StorageError::Auth("list boom".into())),
                ListMode::Empty => Ok(Box::pin(stream::iter(Vec::new()))),
                ListMode::ItemErr => Ok(Box::pin(stream::iter(vec![Err::<ObjectMeta, _>(
                    StorageError::Auth("item boom".into()),
                )]))),
            }
        }
        async fn delete(&self, _key: &str) -> StorageResult<()> {
            if self.delete_err {
                Err(StorageError::Transport("delete boom".into()))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn read_passes_when_named_objects_exist() {
        let (_dir, s) = fs_storage();
        s.put("base/a", reader(b"x"), None).await.unwrap();
        s.put("base/b", reader(b"y"), None).await.unwrap();
        read(&s, &["base/a".into(), "base/b".into()]).await.unwrap();
    }

    #[tokio::test]
    async fn read_passes_on_empty_bucket() {
        let (_dir, s) = fs_storage();
        read(&s, &[]).await.unwrap();
    }

    #[tokio::test]
    async fn read_bails_on_missing_objects() {
        let (_dir, s) = fs_storage();
        s.put("present", reader(b"x"), None).await.unwrap();
        let err = read(&s, &["present".into(), "absent".into()])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("files are missing"), "{msg}");
        assert!(msg.contains("absent") && !msg.contains("present"), "{msg}");
    }

    #[tokio::test]
    async fn read_propagates_list_error() {
        let s: DynStorage = Arc::new(Mock {
            list: ListMode::ListErr,
            ..Default::default()
        });
        let err = read(&s, &[]).await.unwrap_err();
        assert!(err.to_string().contains("failed to list"), "{err}");
    }

    #[tokio::test]
    async fn read_surfaces_first_item_error() {
        let s: DynStorage = Arc::new(Mock {
            list: ListMode::ItemErr,
            ..Default::default()
        });
        let err = read(&s, &[]).await.unwrap_err();
        assert!(err.to_string().contains("failed to list"), "{err}");
    }

    #[tokio::test]
    async fn write_probes_then_cleans_up() {
        let (_dir, s) = fs_storage();
        write(&s).await.unwrap();
        // probe deleted: bucket left empty
        let mut listed = s.list("").await.unwrap();
        assert!(listed.next().await.is_none(), "probe object left behind");
    }

    #[tokio::test]
    async fn write_propagates_put_error() {
        let s: DynStorage = Arc::new(Mock {
            put_err: true,
            ..Default::default()
        });
        let err = write(&s).await.unwrap_err();
        assert!(err.to_string().contains("failed to write"), "{err}");
    }

    #[tokio::test]
    async fn write_tolerates_delete_failure() {
        // put ok, delete errors → warn logged, check still passes
        let s: DynStorage = Arc::new(Mock {
            delete_err: true,
            ..Default::default()
        });
        write(&s).await.unwrap();
    }
}
