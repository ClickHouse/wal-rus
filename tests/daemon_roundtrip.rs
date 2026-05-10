//! Daemon socket protocol smoke test: spin up server, drive via client

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use wal_rs::cli::DaemonOp;
use wal_rs::compression::Method;
use wal_rs::config::{Settings, StorageSettings};
use wal_rs::storage::fs::FsStorage;

#[tokio::test]
async fn daemon_check_and_wal_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&stage).unwrap();
    std::fs::create_dir_all(&restore).unwrap();
    let socket = dir.path().join("wal-rs.sock");

    let segment = "000000010000000000000001";
    let src = stage.join(segment);
    std::fs::write(&src, b"abcdefg test segment").unwrap();

    let s = Settings {
        storage: StorageSettings::Fs {
            path: storage_dir.to_str().unwrap().into(),
        },
        compression: Method::Zstd,
        compression_level: 3,
        upload_concurrency: 1,
        download_concurrency: 1,
        prevent_wal_overwrite: false,
        retry: wal_rs::retry::RetryPolicy::default(),
        network_rate_limit: 0,
        disk_rate_limit: 0,
    };
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let socket_for_server = socket.clone();
    let s_for_server = s.clone();
    let store_for_server = store.clone();
    let server = tokio::spawn(async move {
        let _ = wal_rs::daemon::serve(&socket_for_server, s_for_server, store_for_server).await;
    });

    // wait for socket to appear
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "socket did not appear");

    wal_rs::daemon::client::run(&socket, DaemonOp::Check)
        .await
        .unwrap();

    wal_rs::daemon::client::run(
        &socket,
        DaemonOp::WalPush {
            wal_filepath: src.clone(),
        },
    )
    .await
    .unwrap();

    let dst: PathBuf = restore.join(segment);
    wal_rs::daemon::client::run(
        &socket,
        DaemonOp::WalFetch {
            name: segment.into(),
            dst: dst.clone(),
        },
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&dst).unwrap(), b"abcdefg test segment");

    server.abort();
}
