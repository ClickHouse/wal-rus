//! Daemon socket protocol smoke test: spin up server, drive via client

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixStream;

use walross::cli::DaemonOp;
use walross::compression::Method;
use walross::config::{Settings, StorageSettings};
use walross::daemon::protocol::{MessageType, read_message, write_message};
use walross::storage::fs::FsStorage;

fn fs_settings(storage_dir: &std::path::Path) -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: storage_dir.to_str().unwrap().into(),
        },
        compression: Method::Zstd,
        compression_level: 3,
        upload_concurrency: 1,
        upload_queue: 1,
        download_concurrency: 1,
        prevent_wal_overwrite: false,
        retry: walross::retry::RetryPolicy::default(),
        network_rate_limit: 0,
        disk_rate_limit: 0,
        delta: Default::default(),
        crypter: None,
    }
}

async fn wait_for_socket(socket: &std::path::Path) {
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(socket.exists(), "socket did not appear");
}

#[tokio::test]
async fn daemon_check_and_wal_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&stage).unwrap();
    std::fs::create_dir_all(&restore).unwrap();
    let socket = dir.path().join("walross.sock");

    let segment = "000000010000000000000001";
    let src = stage.join(segment);
    std::fs::write(&src, b"abcdefg test segment").unwrap();

    let s = fs_settings(&storage_dir);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let socket_for_server = socket.clone();
    let server = tokio::spawn(async move {
        let _ = walross::daemon::serve(&socket_for_server, s, store).await;
    });

    wait_for_socket(&socket).await;

    walross::daemon::client::run(&socket, DaemonOp::Check)
        .await
        .unwrap();

    walross::daemon::client::run(
        &socket,
        DaemonOp::WalPush {
            wal_filepath: src.clone(),
        },
    )
    .await
    .unwrap();

    let dst: PathBuf = restore.join(segment);
    walross::daemon::client::run(
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

/// wal-g's ProcessConnection keeps a connection open across successful Check
/// messages but closes it on any handler error. A known-but-unsupported request
/// type (here a bare Ok) must draw an Error response then EOF on the same conn
#[tokio::test]
async fn daemon_closes_connection_on_handler_error() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let socket = dir.path().join("walross.sock");

    let s = fs_settings(&storage_dir);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let socket_for_server = socket.clone();
    let server = tokio::spawn(async move {
        let _ = walross::daemon::serve(&socket_for_server, s, store).await;
    });
    wait_for_socket(&socket).await;

    let mut stream = UnixStream::connect(&socket).await.unwrap();

    // Check succeeds and the loop stays open for another message
    write_message(&mut stream, MessageType::Check, &[])
        .await
        .unwrap();
    let (resp, _) = read_message(&mut stream).await.unwrap();
    assert_eq!(resp, MessageType::Ok);

    // Unsupported request type: dispatch bails, daemon answers Error and closes
    write_message(&mut stream, MessageType::Ok, &[])
        .await
        .unwrap();
    let (resp, _) = read_message(&mut stream).await.unwrap();
    assert_eq!(resp, MessageType::Error);
    assert!(
        read_message(&mut stream).await.is_err(),
        "connection should be closed after handler error"
    );

    server.abort();
}
