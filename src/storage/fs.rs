//! Local filesystem backend
//!
//! Atomic puts via temp + rename so partial writes never appear at final key

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{AsyncReader, CopySource, ObjectMeta, ObjectStream, Result, Storage, StorageError};

pub struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn full(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

/// tmp lives next to final so rename stays on same fs
fn tmp_sibling(final_path: &Path) -> PathBuf {
    final_path.with_extension(format!(
        "{}.tmp.{}",
        final_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or(""),
        std::process::id(),
    ))
}

#[async_trait]
impl Storage for FsStorage {
    fn describe(&self) -> String {
        format!("file://{}", self.root.display())
    }

    async fn put(&self, key: &str, mut body: AsyncReader, _size_hint: Option<u64>) -> Result<()> {
        let final_path = self.full(key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp_path = tmp_sibling(&final_path);
        let mut tmp = fs::File::create(&tmp_path).await?;
        match tokio::io::copy(&mut body, &mut tmp).await {
            Ok(_) => {
                tmp.flush().await?;
                tmp.sync_all().await?;
                drop(tmp);
                fs::rename(&tmp_path, &final_path).await?;
                Ok(())
            }
            Err(e) => {
                drop(tmp);
                let _ = fs::remove_file(&tmp_path).await;
                Err(e.into())
            }
        }
    }

    async fn get(&self, key: &str) -> Result<AsyncReader> {
        let path = self.full(key);
        let file = match fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound(key.to_string()));
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Box::pin(file))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match fs::metadata(self.full(key)).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<ObjectStream> {
        let base = self.full(prefix);
        let mut out = Vec::new();
        if base.exists() {
            walk(&base, &self.root, &mut out).await?;
        }
        Ok(Box::pin(stream::iter(out).map(Ok)))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match fs::remove_file(self.full(key)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn copy_source(&self, key: &str) -> Option<CopySource> {
        Some(CopySource {
            backend: "fs".into(),
            bucket: String::new(),
            key: self.full(key).to_string_lossy().into_owned(),
        })
    }

    async fn copy_within(&self, src: &CopySource, dst_key: &str) -> Result<()> {
        if src.backend != "fs" {
            return Err(StorageError::Unimplemented("copy_within backend mismatch"));
        }
        let final_path = self.full(dst_key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp_path = tmp_sibling(&final_path);
        // fs::copy uses copy_file_range / reflink where kernel supports it
        match fs::copy(&src.key, &tmp_path).await {
            Ok(_) => {
                fs::rename(&tmp_path, &final_path).await?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(src.key.clone()))
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp_path).await;
                Err(e.into())
            }
        }
    }
}

async fn walk(dir: &Path, root: &Path, out: &mut Vec<ObjectMeta>) -> Result<()> {
    // recursive async walk via explicit stack, no Box::pin recursion
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let mut rd = match fs::read_dir(&d).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            let path = entry.path();
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let meta = entry.metadata().await?;
                let key = path
                    .strip_prefix(root)
                    .map_err(|e| StorageError::InvalidResponse(e.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                out.push(ObjectMeta {
                    key,
                    size: meta.len(),
                    last_modified: meta
                        .modified()
                        .ok()
                        .map(chrono::DateTime::<chrono::Utc>::from),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    fn reader(bytes: &[u8]) -> AsyncReader {
        Box::pin(Cursor::new(bytes.to_vec()))
    }

    #[tokio::test]
    async fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path()).unwrap();
        s.put("wal_005/foo.zst", reader(b"hello"), Some(5))
            .await
            .unwrap();
        assert!(s.exists("wal_005/foo.zst").await.unwrap());
        let mut r = s.get("wal_005/foo.zst").await.unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello");
    }

    #[tokio::test]
    async fn missing_get() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path()).unwrap();
        match s.get("nope").await {
            Err(StorageError::NotFound(_)) => {}
            other => panic!("expected NotFound, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn list_recursive() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path()).unwrap();
        s.put("a/b.txt", reader(b"x"), None).await.unwrap();
        s.put("a/c/d.txt", reader(b"yy"), None).await.unwrap();
        let mut keys: Vec<String> = s
            .list("a")
            .await
            .unwrap()
            .map(|r| r.unwrap().key)
            .collect()
            .await;
        keys.sort();
        assert_eq!(keys, vec!["a/b.txt", "a/c/d.txt"]);
    }

    #[tokio::test]
    async fn copy_within_across_roots() {
        let dir = tempfile::tempdir().unwrap();
        let src = FsStorage::new(dir.path().join("src")).unwrap();
        let dst = FsStorage::new(dir.path().join("dst")).unwrap();
        src.put("a/b.zst", reader(b"payload"), None).await.unwrap();

        let loc = src.copy_source("a/b.zst").unwrap();
        dst.copy_within(&loc, "a/b.zst").await.unwrap();
        let mut r = dst.get("a/b.zst").await.unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"payload");

        let missing = src.copy_source("nope").unwrap();
        match dst.copy_within(&missing, "x").await {
            Err(StorageError::NotFound(_)) => {}
            other => panic!("expected NotFound, got {:?}", other.err()),
        }

        let mismatched = CopySource {
            backend: "s3:other".into(),
            bucket: "b".into(),
            key: "k".into(),
        };
        match dst.copy_within(&mismatched, "x").await {
            Err(StorageError::Unimplemented(_)) => {}
            other => panic!("expected Unimplemented, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn delete_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let s = FsStorage::new(dir.path()).unwrap();
        s.delete("missing").await.unwrap();
        s.put("k", reader(b"v"), None).await.unwrap();
        s.delete("k").await.unwrap();
        assert!(!s.exists("k").await.unwrap());
    }
}
