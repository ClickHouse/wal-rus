//! Retry wrapper over a Storage backend
//!
//! Retries get/exists/list/delete unconditionally on transient errors;
//! retries put only when the body fits the configured buffer threshold
//! (callers passing size_hint <= threshold get retry, otherwise pass-through)

use async_trait::async_trait;
use bytes::Bytes;
use std::io::Cursor;

use crate::retry::{RetryPolicy, with_retry};

use super::{AsyncReader, CopySource, ObjectStream, Result, Storage, StorageError};

/// Buffer-then-retry threshold for put bodies (matches wal-g's small-object
/// path: sentinels, history files, manifest fragments)
const PUT_RETRY_BUFFER_THRESHOLD: u64 = 8 * 1024 * 1024;

pub struct RetryingStorage<S: Storage + 'static> {
    inner: S,
    policy: RetryPolicy,
}

impl<S: Storage + 'static> RetryingStorage<S> {
    pub fn new(inner: S, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }
}

#[async_trait]
impl<S: Storage + 'static> Storage for RetryingStorage<S> {
    fn describe(&self) -> String {
        self.inner.describe()
    }

    async fn put(&self, key: &str, mut body: AsyncReader, size_hint: Option<u64>) -> Result<()> {
        // Only retry small known-size bodies; large streaming bodies cannot
        // be replayed without buffering the whole thing
        let bufferable = matches!(size_hint, Some(s) if s <= PUT_RETRY_BUFFER_THRESHOLD);
        if !bufferable {
            return self.inner.put(key, body, size_hint).await;
        }
        let mut buf = Vec::with_capacity(size_hint.unwrap_or(0) as usize);
        tokio::io::copy(&mut body, &mut buf).await?;
        let bytes = Bytes::from(buf);
        let len = bytes.len() as u64;
        with_retry(&self.policy, StorageError::is_transient, || {
            let bytes = bytes.clone();
            async move {
                let reader: AsyncReader = Box::pin(Cursor::new(bytes));
                self.inner.put(key, reader, Some(len)).await
            }
        })
        .await
    }

    async fn get(&self, key: &str) -> Result<AsyncReader> {
        with_retry(&self.policy, StorageError::is_transient, || async {
            self.inner.get(key).await
        })
        .await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        with_retry(&self.policy, StorageError::is_transient, || async {
            self.inner.exists(key).await
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<ObjectStream> {
        with_retry(&self.policy, StorageError::is_transient, || async {
            self.inner.list(prefix).await
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        with_retry(&self.policy, StorageError::is_transient, || async {
            self.inner.delete(key).await
        })
        .await
    }

    fn copy_source(&self, key: &str) -> Option<CopySource> {
        self.inner.copy_source(key)
    }

    async fn copy_within(&self, src: &CopySource, dst_key: &str) -> Result<()> {
        // idempotent, safe to replay
        with_retry(&self.policy, StorageError::is_transient, || async {
            self.inner.copy_within(src, dst_key).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ObjectMeta;
    use futures::stream;
    use std::io::Cursor;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    /// Stub backend that returns scripted responses; counts call attempts
    struct StubStorage {
        get_calls: AtomicU32,
        put_calls: AtomicU32,
        put_bodies: Mutex<Vec<Vec<u8>>>,
        get_script: Mutex<Vec<StubResult>>,
        put_script: Mutex<Vec<StubResult>>,
    }

    enum StubResult {
        TransientHttp,
        PermanentNotFound,
        Ok,
    }

    impl StubStorage {
        fn new() -> Self {
            Self {
                get_calls: AtomicU32::new(0),
                put_calls: AtomicU32::new(0),
                put_bodies: Mutex::new(Vec::new()),
                get_script: Mutex::new(Vec::new()),
                put_script: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Storage for StubStorage {
        fn describe(&self) -> String {
            "stub://".into()
        }
        async fn put(
            &self,
            _key: &str,
            mut body: AsyncReader,
            _size_hint: Option<u64>,
        ) -> Result<()> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            let mut buf = Vec::new();
            body.read_to_end(&mut buf).await?;
            self.put_bodies.lock().unwrap().push(buf);
            let next = self.put_script.lock().unwrap().remove(0);
            match next {
                StubResult::TransientHttp => Err(StorageError::Http {
                    status: 503,
                    body: "stub down".into(),
                }),
                StubResult::PermanentNotFound => Err(StorageError::NotFound("nope".into())),
                StubResult::Ok => Ok(()),
            }
        }
        async fn get(&self, _key: &str) -> Result<AsyncReader> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            let next = self.get_script.lock().unwrap().remove(0);
            match next {
                StubResult::TransientHttp => Err(StorageError::Http {
                    status: 500,
                    body: "boom".into(),
                }),
                StubResult::PermanentNotFound => Err(StorageError::NotFound("nope".into())),
                StubResult::Ok => Ok(Box::pin(Cursor::new(b"ok".to_vec()))),
            }
        }
        async fn exists(&self, _key: &str) -> Result<bool> {
            Ok(true)
        }
        async fn list(&self, _prefix: &str) -> Result<ObjectStream> {
            Ok(Box::pin(stream::iter(std::iter::empty::<
                std::result::Result<ObjectMeta, StorageError>,
            >())))
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }
    }

    fn fast_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: false,
        }
    }

    #[tokio::test]
    async fn put_retries_transient_then_succeeds() {
        let stub = StubStorage::new();
        stub.put_script.lock().unwrap().extend([
            StubResult::TransientHttp,
            StubResult::TransientHttp,
            StubResult::Ok,
        ]);
        let retry = RetryingStorage::new(stub, fast_policy());
        let body: AsyncReader = Box::pin(Cursor::new(b"payload".to_vec()));
        retry.put("k", body, Some(7)).await.unwrap();
        assert_eq!(retry.inner.put_calls.load(Ordering::SeqCst), 3);
        // body must be byte-identical across retries
        let bodies = retry.inner.put_bodies.lock().unwrap();
        assert_eq!(bodies.len(), 3);
        assert!(bodies.iter().all(|b| b == b"payload"));
    }

    #[tokio::test]
    async fn put_stops_on_permanent() {
        let stub = StubStorage::new();
        stub.put_script
            .lock()
            .unwrap()
            .push(StubResult::PermanentNotFound);
        let retry = RetryingStorage::new(stub, fast_policy());
        let body: AsyncReader = Box::pin(Cursor::new(b"x".to_vec()));
        let r = retry.put("k", body, Some(1)).await;
        assert!(matches!(r, Err(StorageError::NotFound(_))));
        assert_eq!(retry.inner.put_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_retries_transient() {
        let stub = StubStorage::new();
        stub.get_script
            .lock()
            .unwrap()
            .extend([StubResult::TransientHttp, StubResult::Ok]);
        let retry = RetryingStorage::new(stub, fast_policy());
        let mut r = retry.get("k").await.unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"ok");
        assert_eq!(retry.inner.get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn delegators_pass_through_to_inner() {
        use futures::StreamExt;

        let retry = RetryingStorage::new(StubStorage::new(), fast_policy());
        assert_eq!(retry.describe(), "stub://");
        assert!(retry.exists("k").await.unwrap());
        let mut st = retry.list("").await.unwrap();
        assert!(st.next().await.is_none());
        retry.delete("k").await.unwrap();
        // stub has no server-side copy support: default impls flow through
        assert!(retry.copy_source("k").is_none());
        let src = CopySource {
            backend: "x".into(),
            bucket: "b".into(),
            key: "k".into(),
        };
        assert!(matches!(
            retry.copy_within(&src, "d").await,
            Err(StorageError::Unimplemented(_))
        ));
    }

    #[tokio::test]
    async fn put_bypasses_retry_when_size_unknown() {
        let stub = StubStorage::new();
        stub.put_script
            .lock()
            .unwrap()
            .push(StubResult::TransientHttp);
        let retry = RetryingStorage::new(stub, fast_policy());
        let body: AsyncReader = Box::pin(Cursor::new(b"x".to_vec()));
        let r = retry.put("k", body, None).await;
        assert!(matches!(r, Err(StorageError::Http { status: 503, .. })));
        assert_eq!(retry.inner.put_calls.load(Ordering::SeqCst), 1);
    }
}
