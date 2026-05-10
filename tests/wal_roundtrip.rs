//! End-to-end wal-push -> wal-fetch with fs backend; bytes must match

use std::path::PathBuf;
use std::sync::Arc;

use wal_rs::compression::Method;
use wal_rs::config::{Settings, StorageSettings};
use wal_rs::pg::wal;
use wal_rs::storage::fs::FsStorage;

fn pseudo_wal_segment(seed: u8) -> Vec<u8> {
    // 16MB to match default wal_segsize
    let mut buf = vec![0u8; 16 * 1024 * 1024];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(seed).wrapping_add(seed);
    }
    buf
}

fn settings_for(path: &str, method: Method) -> Settings {
    Settings {
        storage: StorageSettings::Fs { path: path.into() },
        compression: method,
        compression_level: 3,
        upload_concurrency: 1,
        download_concurrency: 1,
        prevent_wal_overwrite: false,
        retry: wal_rs::retry::RetryPolicy::default(),
        network_rate_limit: 0,
        disk_rate_limit: 0,
        delta: Default::default(),
    }
}

#[tokio::test]
async fn push_fetch_zstd_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&stage).unwrap();
    std::fs::create_dir_all(&restore).unwrap();

    let segment_name = "000000010000000000000001";
    let src = stage.join(segment_name);
    std::fs::write(&src, pseudo_wal_segment(7)).unwrap();

    let s = settings_for(storage_dir.to_str().unwrap(), Method::Zstd);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    wal::push::handle(&s, store.clone(), &src).await.unwrap();

    // verify object key shape
    let obj_path = storage_dir.join(format!("wal_005/{segment_name}.zst"));
    assert!(obj_path.exists(), "expected {obj_path:?} to exist");
    let stored_size = std::fs::metadata(&obj_path).unwrap().len();
    let original_size = std::fs::metadata(&src).unwrap().len();
    assert!(
        stored_size < original_size,
        "expected zstd to shrink predictable data: {stored_size} >= {original_size}",
    );

    let dst: PathBuf = restore.join(segment_name);
    wal::fetch::handle(&s, store, segment_name, &dst)
        .await
        .unwrap();

    let original = std::fs::read(&src).unwrap();
    let recovered = std::fs::read(&dst).unwrap();
    assert_eq!(original.len(), recovered.len());
    assert_eq!(original, recovered, "byte-identical recovery");
}

#[tokio::test]
async fn push_fetch_uncompressed() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let src = stage.join("000000010000000000000002");
    std::fs::write(&src, b"raw payload, not 16MB").unwrap();

    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store.clone(), &src).await.unwrap();

    assert!(
        storage_dir
            .join("wal_005/000000010000000000000002")
            .exists()
    );

    let dst = dir.path().join("000000010000000000000002");
    wal::fetch::handle(&s, store, "000000010000000000000002", &dst)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), b"raw payload, not 16MB");
}

#[tokio::test]
async fn ready_marker_is_renamed_to_done_after_push() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let pg_wal = dir.path().join("pg_wal");
    let archive_status = pg_wal.join("archive_status");
    std::fs::create_dir_all(&archive_status).unwrap();

    let segment_name = "000000010000000000000004";
    let src = pg_wal.join(segment_name);
    std::fs::write(&src, b"segment bytes").unwrap();
    let ready = archive_status.join(format!("{segment_name}.ready"));
    let done = archive_status.join(format!("{segment_name}.done"));
    std::fs::write(&ready, b"").unwrap();

    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store, &src).await.unwrap();

    assert!(!ready.exists(), "{ready:?} should be gone");
    assert!(done.exists(), "{done:?} should exist");
}

#[tokio::test]
async fn missing_ready_marker_is_not_an_error() {
    // daemon-mode / sidecar deployment: archive_status not adjacent to file
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let src = stage.join("000000010000000000000005");
    std::fs::write(&src, b"x").unwrap();

    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store, &src).await.unwrap();
}

#[tokio::test]
async fn prevent_overwrite_passes_when_existing_bytes_match() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let src = stage.join("000000010000000000000010");
    std::fs::write(&src, b"identical payload").unwrap();

    let mut s = settings_for(storage_dir.to_str().unwrap(), Method::Zstd);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    wal::push::handle(&s, store.clone(), &src).await.unwrap();
    s.prevent_wal_overwrite = true;
    // second push with identical bytes must succeed (PG re-runs archive_command)
    wal::push::handle(&s, store, &src).await.unwrap();
}

#[tokio::test]
async fn prevent_overwrite_rejects_when_existing_bytes_differ() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let src = stage.join("000000010000000000000011");
    std::fs::write(&src, b"first payload").unwrap();

    let mut s = settings_for(storage_dir.to_str().unwrap(), Method::Zstd);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store.clone(), &src).await.unwrap();

    std::fs::write(&src, b"different bytes").unwrap();
    s.prevent_wal_overwrite = true;
    let err = wal::push::handle(&s, store, &src).await.err().unwrap();
    let msg = format!("{err:#}");
    assert!(msg.contains("different bytes"), "{msg}");
}

#[tokio::test]
async fn history_file_idempotent_overwrite_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let src = stage.join("00000002.history");
    std::fs::write(&src, b"timeline history line\n").unwrap();

    let s = settings_for(storage_dir.to_str().unwrap(), Method::Zstd);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store.clone(), &src).await.unwrap();
    // .history must not bail even without prevent_wal_overwrite when bytes match
    wal::push::handle(&s, store, &src).await.unwrap();
}

#[tokio::test]
async fn fetch_falls_back_to_uncompressed_when_zstd_missing() {
    // simulates bucket written by `WALG_COMPRESSION_METHOD=none` while client requests zstd
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let src = stage.join("000000010000000000000003");
    std::fs::write(&src, b"hello world").unwrap();

    let upload_settings = settings_for(storage_dir.to_str().unwrap(), Method::None);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&upload_settings, store.clone(), &src)
        .await
        .unwrap();

    let fetch_settings = settings_for(storage_dir.to_str().unwrap(), Method::Zstd);
    let dst = dir.path().join("restored");
    wal::fetch::handle(&fetch_settings, store, "000000010000000000000003", &dst)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), b"hello world");
}
