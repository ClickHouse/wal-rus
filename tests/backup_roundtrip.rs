//! Backup-list / backup-fetch end-to-end against fs storage with a synthetic
//! sentinel + tar produced in wal-g format

use std::sync::Arc;

use chrono::Utc;
use wal_rs::compression::Method;
use wal_rs::config::{Settings, StorageSettings};
use wal_rs::pg::backup::fetch as fetch_mod;
use wal_rs::pg::backup::list as list_mod;
use wal_rs::pg::backup::{
    BackupSentinelDto, BackupSentinelDtoV2, METADATA_DATETIME_FORMAT, TablespaceSpec,
    format_backup_name, sentinel_key, tar_part_key,
};
use wal_rs::storage::Storage;
use wal_rs::storage::fs::FsStorage;

fn test_settings() -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: "/tmp".into(),
        },
        compression: Method::Zstd,
        compression_level: 3,
        upload_concurrency: 1,
        download_concurrency: 1,
        prevent_wal_overwrite: false,
        retry: wal_rs::retry::RetryPolicy::default(),
        network_rate_limit: 0,
        disk_rate_limit: 0,
    }
}

fn make_sentinel_v2(name_data_dir: &str) -> BackupSentinelDtoV2 {
    BackupSentinelDtoV2 {
        sentinel: BackupSentinelDto {
            backup_start_lsn: Some(0x0300_0000),
            increment_from_lsn: None,
            increment_from: None,
            increment_full_name: None,
            increment_count: None,
            pg_version: 160003,
            backup_finish_lsn: Some(0x0300_1000),
            system_identifier: Some(7000000000000000000),
            uncompressed_size: 1024,
            compressed_size: 512,
            data_catalog_size: 0,
            user_data: None,
            files_metadata_disabled: true,
            tablespace_spec: None,
            backup_start_chkp_num: Some(0),
            increment_from_chkp_num: None,
        },
        version: 2,
        start_time: Utc::now(),
        finish_time: Utc::now(),
        date_fmt: METADATA_DATETIME_FORMAT.into(),
        hostname: "testhost".into(),
        data_dir: name_data_dir.into(),
        is_permanent: false,
    }
}

fn build_tar(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut b = tar::Builder::new(&mut buf);
        for (name, data) in files {
            let mut h = tar::Header::new_gnu();
            h.set_path(name).unwrap();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append(&h, *data).unwrap();
        }
        b.finish().unwrap();
    }
    buf
}

async fn put_bytes(store: Arc<FsStorage>, key: &str, body: Vec<u8>) {
    let len = body.len() as u64;
    let r: wal_rs::compression::AsyncReader = Box::pin(std::io::Cursor::new(body));
    store.put(key, r, Some(len)).await.unwrap();
}

#[tokio::test]
async fn list_finds_seeded_backup() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
    let sentinel = make_sentinel_v2("/var/lib/postgres/data");
    let sentinel_bytes = serde_json::to_vec(&sentinel).unwrap();
    put_bytes(store.clone(), &sentinel_key(&backup_name), sentinel_bytes).await;

    let summaries = list_mod::collect(store as Arc<dyn Storage>).await.unwrap();
    assert_eq!(summaries.len(), 1);
    let s = &summaries[0];
    assert_eq!(s.name, backup_name);
    assert_eq!(s.start_lsn, Some(0x0300_0000));
    assert_eq!(s.pg_version, 160003);
    assert_eq!(s.hostname.as_deref(), Some("testhost"));
}

#[tokio::test]
async fn fetch_extracts_tar_part() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);

    let sentinel = make_sentinel_v2("/var/lib/postgres/data");
    let sentinel_bytes = serde_json::to_vec(&sentinel).unwrap();
    put_bytes(store.clone(), &sentinel_key(&backup_name), sentinel_bytes).await;

    let payload_a = b"hello from PG_VERSION";
    let payload_b = vec![0xABu8; 4096];
    let tar_bytes = build_tar(&[("PG_VERSION", payload_a), ("global/pg_control", &payload_b)]);
    // Use uncompressed extension; fetch will pick Method::None for unknown ext "tar"
    put_bytes(store.clone(), &tar_part_key(&backup_name, 1, ""), tar_bytes).await;

    fetch_mod::handle(
        &test_settings(),
        store as Arc<dyn Storage>,
        &backup_name,
        &restore,
    )
    .await
    .unwrap();

    assert_eq!(
        std::fs::read(restore.join("PG_VERSION")).unwrap(),
        payload_a
    );
    assert_eq!(
        std::fs::read(restore.join("global/pg_control")).unwrap(),
        payload_b
    );
}

#[tokio::test]
async fn fetch_resolves_latest() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());

    let older = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let newer = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);

    let bytes = serde_json::to_vec(&make_sentinel_v2("/d")).unwrap();
    put_bytes(store.clone(), &sentinel_key(&older), bytes.clone()).await;
    // ensure mtime ordering by sleeping a beat then writing newer
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    put_bytes(store.clone(), &sentinel_key(&newer), bytes).await;

    let resolved = fetch_mod::resolve_name(&(store as Arc<dyn Storage>), "LATEST")
        .await
        .unwrap();
    assert_eq!(resolved, newer);
}

#[tokio::test]
async fn fetch_decompresses_zstd_tar() {
    use async_compression::Level;
    use async_compression::tokio::bufread::ZstdEncoder;
    use tokio::io::AsyncReadExt;
    use tokio::io::BufReader;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
    let sentinel = make_sentinel_v2("/d");
    let sentinel_bytes = serde_json::to_vec(&sentinel).unwrap();
    put_bytes(store.clone(), &sentinel_key(&backup_name), sentinel_bytes).await;

    let tar_bytes = build_tar(&[
        ("file_a.txt", b"alpha"),
        ("dir/file_b.bin", &vec![1u8; 1000]),
    ]);

    // compress with zstd
    let raw = std::io::Cursor::new(tar_bytes);
    let buffered = BufReader::new(raw);
    let mut encoder = ZstdEncoder::with_quality(buffered, Level::Precise(3));
    let mut compressed = Vec::new();
    encoder.read_to_end(&mut compressed).await.unwrap();

    put_bytes(
        store.clone(),
        &tar_part_key(&backup_name, 1, "zst"),
        compressed,
    )
    .await;

    fetch_mod::handle(
        &test_settings(),
        store as Arc<dyn Storage>,
        &backup_name,
        &restore,
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(restore.join("file_a.txt")).unwrap(), b"alpha");
    assert_eq!(
        std::fs::read(restore.join("dir/file_b.bin")).unwrap(),
        vec![1u8; 1000]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn fetch_recreates_tablespace_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let target = dir.path().join("ts_target");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);

    let mut spec = TablespaceSpec::new(restore.to_string_lossy());
    spec.add(16384, target.to_string_lossy());
    let mut sentinel = make_sentinel_v2(restore.to_str().unwrap());
    sentinel.sentinel.tablespace_spec = Some(spec);
    let sentinel_bytes = serde_json::to_vec(&sentinel).unwrap();
    put_bytes(store.clone(), &sentinel_key(&backup_name), sentinel_bytes).await;

    // Re-tarred entry lives under pg_tblspc/16384/
    let tar_bytes = build_tar(&[("pg_tblspc/16384/PG_VERSION", b"16")]);
    put_bytes(store.clone(), &tar_part_key(&backup_name, 1, ""), tar_bytes).await;

    fetch_mod::handle(
        &test_settings(),
        store as Arc<dyn Storage>,
        &backup_name,
        &restore,
    )
    .await
    .unwrap();

    let link = restore.join("pg_tblspc/16384");
    let md = std::fs::symlink_metadata(&link).unwrap();
    assert!(md.file_type().is_symlink(), "expected symlink at {link:?}");
    let pointed_to = std::fs::read_link(&link).unwrap();
    assert_eq!(pointed_to, target);
    // The file should be reachable through the symlink
    assert_eq!(std::fs::read(target.join("PG_VERSION")).unwrap(), b"16");
}

#[tokio::test]
async fn show_round_trip_and_mark_flips_permanent() {
    use wal_rs::pg::backup::show as show_mod;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
    let sentinel = make_sentinel_v2("/var/lib/postgres/data");
    let sentinel_bytes = serde_json::to_vec(&sentinel).unwrap();
    put_bytes(store.clone(), &sentinel_key(&backup_name), sentinel_bytes).await;

    // pure read; just ensure it doesn't error
    show_mod::show(
        store.clone() as Arc<dyn Storage>,
        &backup_name,
        show_mod::Format::Json,
    )
    .await
    .unwrap();

    // flip to permanent
    show_mod::mark(store.clone() as Arc<dyn Storage>, &backup_name, true)
        .await
        .unwrap();

    let raw = std::fs::read(dir.path().join(sentinel_key(&backup_name))).unwrap();
    let after: BackupSentinelDtoV2 = serde_json::from_slice(&raw).unwrap();
    assert!(after.is_permanent);

    // flip off
    show_mod::mark(store as Arc<dyn Storage>, &backup_name, false)
        .await
        .unwrap();
    let raw = std::fs::read(dir.path().join(sentinel_key(&backup_name))).unwrap();
    let after: BackupSentinelDtoV2 = serde_json::from_slice(&raw).unwrap();
    assert!(!after.is_permanent);
}
