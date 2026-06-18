//! Backup-list / backup-fetch end-to-end against fs storage with a synthetic
//! sentinel + tar produced in wal-g format

use std::sync::Arc;

use chrono::Utc;
use pgwalrs::compression::Method;
use pgwalrs::config::{DeltaSettings, Settings, StorageSettings};
use pgwalrs::pg::backup::delta as delta_mod;
use pgwalrs::pg::backup::fetch as fetch_mod;
use pgwalrs::pg::backup::increment::write_increment_header;
use pgwalrs::pg::backup::list as list_mod;
use pgwalrs::pg::backup::{
    BackupSentinelDto, BackupSentinelDtoV2, FileDescription, FilesMetadataDto,
    METADATA_DATETIME_FORMAT, PG_CONTROL_TARNAME, TablespaceSpec, files_metadata_key,
    format_backup_name, sentinel_key, tar_part_key, tar_partitions_prefix,
};
use pgwalrs::storage::Storage;
use pgwalrs::storage::fs::FsStorage;

fn test_settings() -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: "/tmp".into(),
        },
        compression: Method::Zstd,
        compression_level: 3,
        upload_concurrency: 1,
        upload_queue: 1,
        download_concurrency: 1,
        prevent_wal_overwrite: false,
        use_wal_delta: false,
        retry: pgwalrs::retry::RetryPolicy::default(),
        network_rate_limit: 0,
        disk_rate_limit: 0,
        delta: Default::default(),
        crypter: None,
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
            increment_format: Default::default(),
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
    let r: pgwalrs::compression::AsyncReader = Box::pin(std::io::Cursor::new(body));
    store.put(key, r, Some(len)).await.unwrap();
}

/// Seed a sentinel-only backup at `lsn` carrying `user_data`; returns its name
async fn seed_user_data(store: &Arc<FsStorage>, lsn: u64, user_data: serde_json::Value) -> String {
    let name = format_backup_name(1, lsn, 16 * 1024 * 1024);
    let mut sentinel = make_sentinel_v2("/var/lib/postgres/data");
    sentinel.sentinel.user_data = Some(user_data);
    let bytes = serde_json::to_vec(&sentinel).unwrap();
    put_bytes(store.clone(), &sentinel_key(&name), bytes).await;
    name
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
async fn fetch_multipart_concurrent_is_byte_clean() {
    // Many data parts plus a pg_control part, fetched with
    // download_concurrency>1. tar_streamer rotates at file boundaries, so
    // parts are file-disjoint and must land byte-clean regardless of unpack
    // order; pg_control (sorted last) applies after the data barrier
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
    put_bytes(
        store.clone(),
        &sentinel_key(&backup_name),
        serde_json::to_vec(&make_sentinel_v2("/d")).unwrap(),
    )
    .await;

    const PARTS: u32 = 8;
    let mut expected: Vec<(String, Vec<u8>)> = Vec::new();
    for n in 1..=PARTS {
        let fname = format!("base/16384/{}", 16400 + n);
        let body = vec![n as u8; 1024 * n as usize];
        let tar = build_tar(&[(fname.as_str(), &body)]);
        put_bytes(store.clone(), &tar_part_key(&backup_name, n, ""), tar).await;
        expected.push((fname, body));
    }

    // pg_control part keyed under the partitions prefix; list_tar_parts sorts
    // it last and the fetch unpacks it strictly after the data barrier
    let pgc_body = vec![0x5Au8; 8192];
    let pgc_tar = build_tar(&[("global/pg_control", &pgc_body)]);
    let pgc_key = format!(
        "{}/{}",
        tar_partitions_prefix(&backup_name),
        PG_CONTROL_TARNAME
    );
    put_bytes(store.clone(), &pgc_key, pgc_tar).await;

    let mut s = test_settings();
    s.download_concurrency = 4;

    fetch_mod::handle(&s, store as Arc<dyn Storage>, &backup_name, &restore)
        .await
        .unwrap();

    for (fname, body) in &expected {
        assert_eq!(
            &std::fs::read(restore.join(fname)).unwrap(),
            body,
            "{fname}"
        );
    }
    assert_eq!(
        std::fs::read(restore.join("global/pg_control")).unwrap(),
        pgc_body
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
async fn resolve_by_user_data_selects_unique_match() {
    use pgwalrs::pg::backup::show;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());

    let a = seed_user_data(&store, 0x0300_0000, serde_json::json!({"label": "a"})).await;
    seed_user_data(&store, 0x0400_0000, serde_json::json!({"label": "b"})).await;

    let dyn_store = store as Arc<dyn Storage>;
    assert_eq!(
        show::resolve_by_user_data(&dyn_store, r#"{"label":"a"}"#)
            .await
            .unwrap(),
        a
    );
    // no backup carries this value
    assert!(
        show::resolve_by_user_data(&dyn_store, r#"{"label":"z"}"#)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn resolve_by_user_data_errors_on_ambiguous_match() {
    use pgwalrs::pg::backup::show;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());

    let shared = serde_json::json!({"team": "infra"});
    let a = seed_user_data(&store, 0x0300_0000, shared.clone()).await;
    let b = seed_user_data(&store, 0x0400_0000, shared).await;

    let dyn_store = store as Arc<dyn Storage>;
    let err = show::resolve_by_user_data(&dyn_store, r#"{"team":"infra"}"#)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains(&a) && err.contains(&b), "got: {err}");
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
    use pgwalrs::pg::backup::show as show_mod;

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

#[tokio::test]
async fn delta_parent_picks_latest_when_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(FsStorage::new(dir.path()).unwrap());

    // Seed two sentinels; the later one (higher LSN, later StartTime) wins
    let older_name = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let mut older = make_sentinel_v2("/var/lib/postgres/data");
    older.sentinel.backup_start_lsn = Some(0x0100_0000);
    older.start_time = chrono::Utc::now() - chrono::Duration::hours(2);
    older.finish_time = older.start_time + chrono::Duration::minutes(1);
    put_bytes(
        Arc::new(FsStorage::new(dir.path()).unwrap()),
        &sentinel_key(&older_name),
        serde_json::to_vec(&older).unwrap(),
    )
    .await;

    let newer_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
    let mut newer = make_sentinel_v2("/var/lib/postgres/data");
    newer.sentinel.backup_start_lsn = Some(0x0300_0000);
    newer.start_time = chrono::Utc::now();
    newer.finish_time = newer.start_time + chrono::Duration::minutes(1);
    put_bytes(
        Arc::new(FsStorage::new(dir.path()).unwrap()),
        &sentinel_key(&newer_name),
        serde_json::to_vec(&newer).unwrap(),
    )
    .await;

    // Bring up the fs storage's list_mtime via touch-ordering so the
    // newer entry sorts last
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(
        dir.path().join(sentinel_key(&newer_name)),
        serde_json::to_vec(&newer).unwrap(),
    )
    .unwrap();

    let delta = DeltaSettings {
        max_steps: 3,
        from_full: false,
        from_name: None,
        from_user_data: None,
    };
    let info = delta_mod::configure_delta_parent(&store, &delta, false)
        .await
        .unwrap()
        .expect("delta parent should be picked");
    assert_eq!(info.name, newer_name);
    assert_eq!(info.start_lsn, 0x0300_0000);
    assert_eq!(info.timeline, 1);
    assert_eq!(info.increment_count, 1);
}

#[tokio::test]
async fn delta_parent_falls_back_to_full_when_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(FsStorage::new(dir.path()).unwrap());

    let name = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let sentinel = make_sentinel_v2("/var/lib/postgres/data");
    put_bytes(
        Arc::new(FsStorage::new(dir.path()).unwrap()),
        &sentinel_key(&name),
        serde_json::to_vec(&sentinel).unwrap(),
    )
    .await;

    let delta = DeltaSettings::default();
    let info = delta_mod::configure_delta_parent(&store, &delta, false)
        .await
        .unwrap();
    assert!(info.is_none(), "max_steps=0 → must fall back to full");
}

#[tokio::test]
async fn delta_parent_falls_back_when_max_steps_reached() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Storage> = Arc::new(FsStorage::new(dir.path()).unwrap());

    let name = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let mut sentinel = make_sentinel_v2("/var/lib/postgres/data");
    sentinel.sentinel.increment_count = Some(3); // chain already 3 deep
    put_bytes(
        Arc::new(FsStorage::new(dir.path()).unwrap()),
        &sentinel_key(&name),
        serde_json::to_vec(&sentinel).unwrap(),
    )
    .await;

    let delta = DeltaSettings {
        max_steps: 3,
        from_full: false,
        from_name: None,
        from_user_data: None,
    };
    let info = delta_mod::configure_delta_parent(&store, &delta, false)
        .await
        .unwrap();
    assert!(info.is_none(), "next would be increment 4 > max 3");
}

#[tokio::test]
async fn fetch_decrypts_libsodium_tar_part() {
    use async_compression::Level;
    use async_compression::tokio::bufread::ZstdEncoder;
    use pgwalrs::crypto::Crypter as _;
    use pgwalrs::crypto::libsodium::LibsodiumCrypter;
    use tokio::io::AsyncReadExt;
    use tokio::io::BufReader;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    // Build encrypted settings (libsodium + zstd) shared between seeding and fetch
    let mut k = [0u8; 32];
    for (i, b) in k.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(17).wrapping_add(3);
    }
    let crypter = std::sync::Arc::new(LibsodiumCrypter::new(k));
    let mut s = test_settings();
    s.crypter = Some(crypter.clone());

    let backup_name = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
    let sentinel = make_sentinel_v2("/d");
    // sentinel JSON is unencrypted (matches wal-g UploadDto path)
    put_bytes(
        store.clone(),
        &sentinel_key(&backup_name),
        serde_json::to_vec(&sentinel).unwrap(),
    )
    .await;

    let tar_bytes = build_tar(&[
        ("file_a.txt", b"alpha"),
        ("dir/file_b.bin", &vec![7u8; 2048]),
    ]);

    // Compress with zstd, then encrypt with libsodium — same order as
    // backup_push (compress → encrypt → storage)
    let raw = std::io::Cursor::new(tar_bytes);
    let buffered = BufReader::new(raw);
    let mut encoder = ZstdEncoder::with_quality(buffered, Level::Precise(3));
    let mut compressed = Vec::new();
    encoder.read_to_end(&mut compressed).await.unwrap();
    let plain: pgwalrs::compression::AsyncReader = Box::pin(std::io::Cursor::new(compressed));
    let mut encrypted_reader = crypter.encrypt_reader(plain);
    let mut encrypted = Vec::new();
    encrypted_reader.read_to_end(&mut encrypted).await.unwrap();

    put_bytes(
        store.clone(),
        &tar_part_key(&backup_name, 1, "zst"),
        encrypted,
    )
    .await;

    fetch_mod::handle(&s, store as Arc<dyn Storage>, &backup_name, &restore)
        .await
        .unwrap();

    assert_eq!(std::fs::read(restore.join("file_a.txt")).unwrap(), b"alpha");
    assert_eq!(
        std::fs::read(restore.join("dir/file_b.bin")).unwrap(),
        vec![7u8; 2048]
    );
}

#[tokio::test]
async fn fetch_applies_delta_chain_wi1() {
    // Full → wi1-delta chain on a 4-block paged file under base/16384/16400.
    // After fetch, blocks 1 and 3 should reflect the delta's content, while
    // blocks 0 and 2 should still carry the full backup's contents
    const BLCKSZ: usize = 8192;
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let mut s = test_settings();
    s.compression = Method::None;

    // Full backup: 4 blocks, each filled with marker 0xAA, first 4 bytes
    // = block number
    let full_name = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let mut full_body = vec![0xAAu8; 4 * BLCKSZ];
    for b in 0u32..4 {
        let off = (b as usize) * BLCKSZ;
        full_body[off..off + 4].copy_from_slice(&b.to_le_bytes());
    }
    let full_tar = build_tar(&[("base/16384/16400", &full_body)]);

    let full_sentinel = make_sentinel_v2("/d");
    put_bytes(
        store.clone(),
        &sentinel_key(&full_name),
        serde_json::to_vec(&full_sentinel).unwrap(),
    )
    .await;
    put_bytes(store.clone(), &tar_part_key(&full_name, 1, ""), full_tar).await;
    // Empty files_metadata.json: nothing incremented in a full backup
    let full_meta = FilesMetadataDto::default();
    put_bytes(
        store.clone(),
        &files_metadata_key(&full_name),
        serde_json::to_vec(&full_meta).unwrap(),
    )
    .await;

    // Delta: rewrite block 1 with 0xBB-marker and block 3 with 0xCC-marker
    let mut block1 = vec![0xBBu8; BLCKSZ];
    block1[0..4].copy_from_slice(&1u32.to_le_bytes());
    let mut block3 = vec![0xCCu8; BLCKSZ];
    block3[0..4].copy_from_slice(&3u32.to_le_bytes());
    let mut increment = Vec::new();
    write_increment_header(&mut increment, (4 * BLCKSZ) as u64, &[1, 3]).unwrap();
    increment.extend_from_slice(&block1);
    increment.extend_from_slice(&block3);

    let delta_name = format!(
        "{}_D_{}",
        format_backup_name(1, 0x0200_0000, 16 * 1024 * 1024),
        full_name.strip_prefix("base_").unwrap(),
    );
    let mut delta_sentinel = make_sentinel_v2("/d");
    delta_sentinel.sentinel.increment_from = Some(full_name.clone());
    delta_sentinel.sentinel.increment_from_lsn = Some(0x0100_0000);
    delta_sentinel.sentinel.increment_full_name = Some(full_name.clone());
    delta_sentinel.sentinel.increment_count = Some(1);
    delta_sentinel.sentinel.backup_start_lsn = Some(0x0200_0000);
    put_bytes(
        store.clone(),
        &sentinel_key(&delta_name),
        serde_json::to_vec(&delta_sentinel).unwrap(),
    )
    .await;

    let delta_tar = build_tar(&[("base/16384/16400", &increment)]);
    put_bytes(store.clone(), &tar_part_key(&delta_name, 1, ""), delta_tar).await;
    // Delta's files_metadata.json claims the file is incremented
    let mut delta_meta = FilesMetadataDto::default();
    delta_meta.files.insert(
        "base/16384/16400".into(),
        FileDescription {
            is_incremented: true,
            is_skipped: false,
            mtime: Utc::now(),
            updates_count: 0,
        },
    );
    put_bytes(
        store.clone(),
        &files_metadata_key(&delta_name),
        serde_json::to_vec(&delta_meta).unwrap(),
    )
    .await;

    fetch_mod::handle(&s, store as Arc<dyn Storage>, &delta_name, &restore)
        .await
        .unwrap();

    let restored = std::fs::read(restore.join("base/16384/16400")).unwrap();
    assert_eq!(restored.len(), 4 * BLCKSZ);
    // block 0: full backup's 0xAA
    assert!(
        restored[4..BLCKSZ].iter().all(|&b| b == 0xAA),
        "block 0 should carry the full backup contents"
    );
    // block 1: delta's 0xBB
    assert_eq!(&restored[BLCKSZ..BLCKSZ + 4], &1u32.to_le_bytes());
    assert!(
        restored[BLCKSZ + 4..2 * BLCKSZ].iter().all(|&b| b == 0xBB),
        "block 1 should carry the delta contents"
    );
    // block 2: full backup's 0xAA (untouched by delta)
    assert!(
        restored[2 * BLCKSZ + 4..3 * BLCKSZ]
            .iter()
            .all(|&b| b == 0xAA),
        "block 2 should remain from the full backup"
    );
    // block 3: delta's 0xCC
    assert_eq!(&restored[3 * BLCKSZ..3 * BLCKSZ + 4], &3u32.to_le_bytes());
    assert!(
        restored[3 * BLCKSZ + 4..].iter().all(|&b| b == 0xCC),
        "block 3 should carry the delta contents"
    );
}

#[tokio::test]
async fn fetch_applies_delta_chain_walg_leading_slash() {
    // wal-g records tar names & files_metadata keys with a leading `/`
    // (GetFileRelPath: "/" + relpath). The incremented-file lookup must
    // normalize both sides identically, else the wi1 increment is written
    // out verbatim (raw "wi1" bytes) corrupting page 0. Regression for
    // cross_tool_delta reverse interop
    const BLCKSZ: usize = 8192;
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let mut s = test_settings();
    s.compression = Method::None;

    let full_name = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let mut full_body = vec![0xAAu8; 2 * BLCKSZ];
    for b in 0u32..2 {
        let off = (b as usize) * BLCKSZ;
        full_body[off..off + 4].copy_from_slice(&b.to_le_bytes());
    }
    // Tar entry names get the leading slash stripped by extraction either
    // way; the bug is the files_metadata key below keeping its leading slash
    let full_tar = build_tar(&[("base/16384/16400", &full_body)]);
    let full_sentinel = make_sentinel_v2("/d");
    put_bytes(
        store.clone(),
        &sentinel_key(&full_name),
        serde_json::to_vec(&full_sentinel).unwrap(),
    )
    .await;
    put_bytes(store.clone(), &tar_part_key(&full_name, 1, ""), full_tar).await;
    put_bytes(
        store.clone(),
        &files_metadata_key(&full_name),
        serde_json::to_vec(&FilesMetadataDto::default()).unwrap(),
    )
    .await;

    // Delta rewrites block 1 with a 0xBB marker
    let mut block1 = vec![0xBBu8; BLCKSZ];
    block1[0..4].copy_from_slice(&1u32.to_le_bytes());
    let mut increment = Vec::new();
    write_increment_header(&mut increment, (2 * BLCKSZ) as u64, &[1]).unwrap();
    increment.extend_from_slice(&block1);

    let delta_name = format!(
        "{}_D_{}",
        format_backup_name(1, 0x0200_0000, 16 * 1024 * 1024),
        full_name.strip_prefix("base_").unwrap(),
    );
    let mut delta_sentinel = make_sentinel_v2("/d");
    delta_sentinel.sentinel.increment_from = Some(full_name.clone());
    delta_sentinel.sentinel.increment_from_lsn = Some(0x0100_0000);
    delta_sentinel.sentinel.increment_full_name = Some(full_name.clone());
    delta_sentinel.sentinel.increment_count = Some(1);
    delta_sentinel.sentinel.backup_start_lsn = Some(0x0200_0000);
    put_bytes(
        store.clone(),
        &sentinel_key(&delta_name),
        serde_json::to_vec(&delta_sentinel).unwrap(),
    )
    .await;
    let delta_tar = build_tar(&[("base/16384/16400", &increment)]);
    put_bytes(store.clone(), &tar_part_key(&delta_name, 1, ""), delta_tar).await;
    // files_metadata key carries the leading slash, as wal-g writes it
    let mut delta_meta = FilesMetadataDto::default();
    delta_meta.files.insert(
        "/base/16384/16400".into(),
        FileDescription {
            is_incremented: true,
            is_skipped: false,
            mtime: Utc::now(),
            updates_count: 0,
        },
    );
    put_bytes(
        store.clone(),
        &files_metadata_key(&delta_name),
        serde_json::to_vec(&delta_meta).unwrap(),
    )
    .await;

    fetch_mod::handle(&s, store as Arc<dyn Storage>, &delta_name, &restore)
        .await
        .unwrap();

    let restored = std::fs::read(restore.join("base/16384/16400")).unwrap();
    assert_eq!(
        restored.len(),
        2 * BLCKSZ,
        "increment must apply, not write raw wi1 bytes"
    );
    // block 0 untouched: full backup's 0xAA, NOT the "wi1" magic
    assert_eq!(&restored[0..4], &0u32.to_le_bytes());
    assert!(restored[4..BLCKSZ].iter().all(|&b| b == 0xAA));
    // block 1: delta's 0xBB
    assert_eq!(&restored[BLCKSZ..BLCKSZ + 4], &1u32.to_le_bytes());
    assert!(restored[BLCKSZ + 4..].iter().all(|&b| b == 0xBB));
}

#[tokio::test]
async fn fetch_walks_three_step_chain() {
    // full → delta1 → delta2. Verify chain walk visits all three and last
    // writer wins per-block. delta1 changes block 1; delta2 also changes
    // block 1 (must overwrite delta1's bytes) plus block 2 (untouched in
    // delta1, so must reach from delta2 directly)
    const BLCKSZ: usize = 8192;
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());

    let mut s = test_settings();
    s.compression = Method::None;

    // Full: 3 blocks marker 0xAA + block number stamp
    let full_name = format_backup_name(1, 0x0100_0000, 16 * 1024 * 1024);
    let mut full_body = vec![0xAAu8; 3 * BLCKSZ];
    for b in 0u32..3 {
        let off = b as usize * BLCKSZ;
        full_body[off..off + 4].copy_from_slice(&b.to_le_bytes());
    }
    let full_tar = build_tar(&[("base/16384/16400", &full_body)]);
    put_bytes(
        store.clone(),
        &sentinel_key(&full_name),
        serde_json::to_vec(&make_sentinel_v2("/d")).unwrap(),
    )
    .await;
    put_bytes(store.clone(), &tar_part_key(&full_name, 1, ""), full_tar).await;
    put_bytes(
        store.clone(),
        &files_metadata_key(&full_name),
        serde_json::to_vec(&FilesMetadataDto::default()).unwrap(),
    )
    .await;

    // delta1: rewrite block 1 with 0xBB
    let mut delta1_block1 = vec![0xBBu8; BLCKSZ];
    delta1_block1[0..4].copy_from_slice(&1u32.to_le_bytes());
    let mut delta1_inc = Vec::new();
    write_increment_header(&mut delta1_inc, (3 * BLCKSZ) as u64, &[1]).unwrap();
    delta1_inc.extend_from_slice(&delta1_block1);

    let delta1_name = format!(
        "{}_D_{}",
        format_backup_name(1, 0x0200_0000, 16 * 1024 * 1024),
        full_name.strip_prefix("base_").unwrap(),
    );
    let mut s1 = make_sentinel_v2("/d");
    s1.sentinel.increment_from = Some(full_name.clone());
    s1.sentinel.increment_from_lsn = Some(0x0100_0000);
    s1.sentinel.increment_full_name = Some(full_name.clone());
    s1.sentinel.increment_count = Some(1);
    s1.sentinel.backup_start_lsn = Some(0x0200_0000);
    put_bytes(
        store.clone(),
        &sentinel_key(&delta1_name),
        serde_json::to_vec(&s1).unwrap(),
    )
    .await;
    put_bytes(
        store.clone(),
        &tar_part_key(&delta1_name, 1, ""),
        build_tar(&[("base/16384/16400", &delta1_inc)]),
    )
    .await;
    let mut m1 = FilesMetadataDto::default();
    m1.files.insert(
        "base/16384/16400".into(),
        FileDescription {
            is_incremented: true,
            is_skipped: false,
            mtime: Utc::now(),
            updates_count: 0,
        },
    );
    put_bytes(
        store.clone(),
        &files_metadata_key(&delta1_name),
        serde_json::to_vec(&m1).unwrap(),
    )
    .await;

    // delta2: rewrite block 1 with 0xDD (overwrites delta1) AND block 2 0xEE
    let mut delta2_block1 = vec![0xDDu8; BLCKSZ];
    delta2_block1[0..4].copy_from_slice(&1u32.to_le_bytes());
    let mut delta2_block2 = vec![0xEEu8; BLCKSZ];
    delta2_block2[0..4].copy_from_slice(&2u32.to_le_bytes());
    let mut delta2_inc = Vec::new();
    write_increment_header(&mut delta2_inc, (3 * BLCKSZ) as u64, &[1, 2]).unwrap();
    delta2_inc.extend_from_slice(&delta2_block1);
    delta2_inc.extend_from_slice(&delta2_block2);

    let delta2_name = format!(
        "{}_D_{}",
        format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024),
        delta1_name.strip_prefix("base_").unwrap(),
    );
    let mut s2 = make_sentinel_v2("/d");
    s2.sentinel.increment_from = Some(delta1_name.clone());
    s2.sentinel.increment_from_lsn = Some(0x0200_0000);
    s2.sentinel.increment_full_name = Some(full_name.clone());
    s2.sentinel.increment_count = Some(2);
    s2.sentinel.backup_start_lsn = Some(0x0300_0000);
    put_bytes(
        store.clone(),
        &sentinel_key(&delta2_name),
        serde_json::to_vec(&s2).unwrap(),
    )
    .await;
    put_bytes(
        store.clone(),
        &tar_part_key(&delta2_name, 1, ""),
        build_tar(&[("base/16384/16400", &delta2_inc)]),
    )
    .await;
    let mut m2 = FilesMetadataDto::default();
    m2.files.insert(
        "base/16384/16400".into(),
        FileDescription {
            is_incremented: true,
            is_skipped: false,
            mtime: Utc::now(),
            updates_count: 0,
        },
    );
    put_bytes(
        store.clone(),
        &files_metadata_key(&delta2_name),
        serde_json::to_vec(&m2).unwrap(),
    )
    .await;

    fetch_mod::handle(&s, store as Arc<dyn Storage>, &delta2_name, &restore)
        .await
        .unwrap();

    let restored = std::fs::read(restore.join("base/16384/16400")).unwrap();
    assert_eq!(restored.len(), 3 * BLCKSZ);
    // block 0: untouched 0xAA
    assert!(restored[4..BLCKSZ].iter().all(|&b| b == 0xAA));
    // block 1: delta2's 0xDD (delta1's 0xBB must NOT win)
    assert_eq!(&restored[BLCKSZ..BLCKSZ + 4], &1u32.to_le_bytes());
    assert!(
        restored[BLCKSZ + 4..2 * BLCKSZ].iter().all(|&b| b == 0xDD),
        "block 1 final state must be delta2's 0xDD, not delta1's 0xBB"
    );
    // block 2: delta2's 0xEE
    assert_eq!(&restored[2 * BLCKSZ..2 * BLCKSZ + 4], &2u32.to_le_bytes());
    assert!(restored[2 * BLCKSZ + 4..].iter().all(|&b| b == 0xEE));
}
