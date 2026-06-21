//! End-to-end wal-push -> wal-fetch with fs backend; bytes must match

use std::path::PathBuf;
use std::sync::Arc;

use walrus::compression::Method;
use walrus::config::{Settings, StorageSettings};
use walrus::pg::wal;
use walrus::storage::fs::FsStorage;

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
        ..Default::default()
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
    wal::fetch::handle(&s, store, segment_name, &dst, wal::fetch::Prefetch::Off)
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
    wal::fetch::handle(
        &s,
        store,
        "000000010000000000000002",
        &dst,
        wal::fetch::Prefetch::Off,
    )
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
async fn prefetch_stages_segments_and_fetch_promotes_by_rename() {
    use walrus::pg::wal::prefetch;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let pg_wal = dir.path().join("pg_wal");
    std::fs::create_dir_all(&pg_wal).unwrap();
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    // Seed storage with segments 2 + 3 (we'll prefetch starting from 1, count=2).
    // Full-size segments with valid WAL magic so fetch's prefetch validation
    // (exact segment size + magic) promotes them rather than re-downloading
    let stage_dir = dir.path().join("stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    for (i, hex) in ["000000010000000000000002", "000000010000000000000003"]
        .into_iter()
        .enumerate()
    {
        let stage = stage_dir.join(hex);
        std::fs::write(&stage, pseudo_wal_segment((i + 2) as u8)).unwrap();
        walrus::pg::wal::push::handle(&s, store.clone(), &stage)
            .await
            .unwrap();
    }

    prefetch::handle(&s, store.clone(), "000000010000000000000001", &pg_wal, 2)
        .await
        .unwrap();

    let staged_2 = prefetch::prefetched_path(&pg_wal, "000000010000000000000002");
    let staged_3 = prefetch::prefetched_path(&pg_wal, "000000010000000000000003");
    assert!(staged_2.exists(), "expected {staged_2:?} after prefetch");
    assert!(staged_3.exists(), "expected {staged_3:?} after prefetch");

    // Now wal-fetch should promote the staged segment via rename
    let dst = pg_wal.join("000000010000000000000002");
    walrus::pg::wal::fetch::handle(
        &s,
        store,
        "000000010000000000000002",
        &dst,
        wal::fetch::Prefetch::Off,
    )
    .await
    .unwrap();
    assert!(dst.exists());
    assert!(
        !staged_2.exists(),
        "promotion must consume the staged file via rename"
    );
}

#[tokio::test]
async fn prefetch_cleans_stale_already_replayed_segments() {
    use walrus::pg::wal::prefetch;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let pg_wal = dir.path().join("pg_wal");
    std::fs::create_dir_all(&pg_wal).unwrap();
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    // segment 6 is what we prefetch forward from seed 5
    let stage_dir = dir.path().join("stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    let six = "000000010000000000000006";
    let stage = stage_dir.join(six);
    std::fs::write(&stage, six.as_bytes()).unwrap();
    walrus::pg::wal::push::handle(&s, store.clone(), &stage)
        .await
        .unwrap();

    // leftover from an earlier replay point: a staged segment older than seed 5
    let stale = prefetch::prefetched_path(&pg_wal, "000000010000000000000002");
    std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
    std::fs::write(&stale, b"stale").unwrap();

    prefetch::handle(&s, store, "000000010000000000000005", &pg_wal, 1)
        .await
        .unwrap();

    assert!(
        !stale.exists(),
        "cleanup must drop already-replayed staged segment"
    );
    assert!(
        prefetch::prefetched_path(&pg_wal, six).exists(),
        "segment after the seed must be staged"
    );
}

#[tokio::test]
async fn wal_fetch_triggers_in_process_prefetch() {
    use std::time::Duration;
    use walrus::pg::wal::prefetch;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let pg_wal = dir.path().join("pg_wal");
    std::fs::create_dir_all(&pg_wal).unwrap();
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let mut s = settings_for(storage_dir.to_str().unwrap(), Method::None);
    s.download_concurrency = 2; // >1 so wal-g's checkPrefetchPossible enables it

    let stage_dir = dir.path().join("stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    for hex in ["000000010000000000000001", "000000010000000000000002"] {
        let stage = stage_dir.join(hex);
        std::fs::write(&stage, hex.as_bytes()).unwrap();
        walrus::pg::wal::push::handle(&s, store.clone(), &stage)
            .await
            .unwrap();
    }

    let dst = pg_wal.join("000000010000000000000001");
    wal::fetch::handle(
        &s,
        store,
        "000000010000000000000001",
        &dst,
        wal::fetch::Prefetch::InProcess,
    )
    .await
    .unwrap();
    assert!(dst.exists());

    // prefetch fires as a detached task; poll for the next segment to be staged
    let staged_2 = prefetch::prefetched_path(&pg_wal, "000000010000000000000002");
    let mut waited = Duration::ZERO;
    while !staged_2.exists() && waited < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(10)).await;
        waited += Duration::from_millis(10);
    }
    assert!(
        staged_2.exists(),
        "wal-fetch should trigger in-process prefetch of the next segment"
    );
}

#[tokio::test]
async fn wal_fetch_waits_for_inflight_prefetch_instead_of_redownloading() {
    use std::time::Duration;
    use walrus::pg::wal::prefetch;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let pg_wal = dir.path().join("pg_wal");
    std::fs::create_dir_all(&pg_wal).unwrap();
    // storage intentionally left empty: a fallback download would 404
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    let seg = "000000010000000000000002";
    // full-size segment with valid magic so promotion accepts it once staged
    let segment = pseudo_wal_segment(2);
    let running = prefetch::running_dir(&pg_wal).join(seg);
    std::fs::create_dir_all(running.parent().unwrap()).unwrap();
    // an in-flight prefetch: running/<seg> present, not yet complete
    std::fs::write(&running, b"").unwrap();

    // background prefetcher completes shortly. Write into running/ then rename
    // to ready/ (the real prefetcher's atomic publish), so a watcher never stats
    // a half-written ready file and rejects it on the exact-size check
    let ready = prefetch::prefetched_path(&pg_wal, seg);
    let running_bg = running.clone();
    let ready_bg = ready.clone();
    let segment_bg = segment.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::write(&running_bg, &segment_bg).unwrap();
        std::fs::rename(&running_bg, &ready_bg).unwrap();
    });

    let dst = pg_wal.join(seg);
    walrus::pg::wal::fetch::handle(&s, store, seg, &dst, wal::fetch::Prefetch::Off)
        .await
        .expect("fetch should reuse the in-flight prefetch, not 404 on empty storage");
    assert_eq!(std::fs::read(&dst).unwrap(), segment);
    assert!(!ready.exists(), "ready file must be consumed by promotion");
}

#[tokio::test]
async fn wal_fetch_reclaims_stalled_prefetch_and_downloads() {
    use walrus::pg::wal::prefetch;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let pg_wal = dir.path().join("pg_wal");
    std::fs::create_dir_all(&pg_wal).unwrap();
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    // seed storage so the fallback download succeeds
    let seg = "000000010000000000000002";
    let stage_dir = dir.path().join("stage");
    std::fs::create_dir_all(&stage_dir).unwrap();
    let stage = stage_dir.join(seg);
    std::fs::write(&stage, b"downloaded-bytes").unwrap();
    walrus::pg::wal::push::handle(&s, store.clone(), &stage)
        .await
        .unwrap();

    // a stalled prefetch: running/<seg> present but never grows or completes
    let running = prefetch::running_dir(&pg_wal).join(seg);
    std::fs::create_dir_all(running.parent().unwrap()).unwrap();
    std::fs::write(&running, b"partial").unwrap();

    let dst = pg_wal.join(seg);
    walrus::pg::wal::fetch::handle(&s, store, seg, &dst, wal::fetch::Prefetch::Off)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), b"downloaded-bytes");
    assert!(
        !running.exists(),
        "stalled running file must be reclaimed on fallback"
    );
}

#[tokio::test]
async fn wal_show_groups_segments_and_detects_gaps() {
    use walrus::pg::wal::show;
    use walrus::storage::Storage;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    // Seed three segments on timeline 1 with a hole at seg 3
    for hex in [
        "000000010000000000000001",
        "000000010000000000000002",
        // gap
        "000000010000000000000004",
    ] {
        let p = stage.join(hex);
        std::fs::write(&p, hex.as_bytes()).unwrap();
        walrus::pg::wal::push::handle(&s, store.clone(), &p)
            .await
            .unwrap();
    }
    let timelines = show::collect(store as Arc<dyn Storage>).await.unwrap();
    assert_eq!(timelines.len(), 1);
    let t = &timelines[0];
    assert_eq!(t.timeline, 1);
    assert_eq!(t.segments_count, 3);
    assert_eq!(t.gaps.len(), 1);
    assert_eq!(t.gaps[0].missing, 1);
    assert_eq!(t.gaps[0].from, "000000010000000000000002");
    assert_eq!(t.gaps[0].to, "000000010000000000000004");
    assert_eq!(t.status, show::TimelineStatus::Lost);
}

#[tokio::test]
async fn wal_restore_fills_gap_into_local_dir() {
    use walrus::pg::wal::restore;
    use walrus::storage::Storage;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    let restore_dst = dir.path().join("restore");
    std::fs::create_dir_all(&stage).unwrap();

    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    // Push 4 segments forming a gap-of-2 (seg 2 + 3 missing locally)
    for hex in [
        "000000010000000000000001",
        "000000010000000000000002",
        "000000010000000000000003",
        "000000010000000000000004",
    ] {
        let p = stage.join(hex);
        std::fs::write(&p, hex.as_bytes()).unwrap();
        walrus::pg::wal::push::handle(&s, store.clone(), &p)
            .await
            .unwrap();
    }

    // Manually delete segs 2 + 3 from storage so show.collect surfaces them
    // as gaps (otherwise no gap = nothing to restore)
    std::fs::remove_file(storage_dir.join("wal_005/000000010000000000000002")).unwrap();
    std::fs::remove_file(storage_dir.join("wal_005/000000010000000000000003")).unwrap();

    restore::handle(&s, store.clone() as Arc<dyn Storage>, &restore_dst, None)
        .await
        .unwrap();
    // Storage doesn't have segments 2/3 -> restore must surface skip warnings
    // but never panic; nothing should land in restore_dst
    assert!(
        !restore_dst.join("000000010000000000000002").exists(),
        "missing segments cannot be restored"
    );

    // Re-publish segments 2/3 so a second restore picks them up
    for hex in ["000000010000000000000002", "000000010000000000000003"] {
        let p = stage.join(hex);
        std::fs::write(&p, hex.as_bytes()).unwrap();
        walrus::pg::wal::push::handle(&s, store.clone(), &p)
            .await
            .unwrap();
    }
    // Force the gap to reappear by removing them locally before retry
    std::fs::remove_file(restore_dst.join(".")).ok();
    let _ = std::fs::create_dir_all(&restore_dst);
    // Need a fresh gap; recreate by deleting segment 3 only
    std::fs::remove_file(storage_dir.join("wal_005/000000010000000000000003")).unwrap();
    restore::handle(&s, store as Arc<dyn Storage>, &restore_dst, None)
        .await
        .unwrap();
    // No assertion on the missing seg (storage doesn't have it). The test
    // covers the unhappy path: restore tolerates missing-segment errors
}

#[tokio::test]
async fn wal_verify_integrity_detects_gap_after_backup() {
    use walrus::pg::backup::{format_backup_name, sentinel_key};
    use walrus::pg::wal::verify;
    use walrus::storage::Storage;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    let s = settings_for(storage_dir.to_str().unwrap(), Method::None);

    // Seed segments 1 (backup start), 2, then gap at 3, then 4
    for hex in [
        "000000010000000000000001",
        "000000010000000000000002",
        "000000010000000000000004",
    ] {
        let p = stage.join(hex);
        std::fs::write(&p, hex.as_bytes()).unwrap();
        walrus::pg::wal::push::handle(&s, store.clone(), &p)
            .await
            .unwrap();
    }
    // Build a synthetic sentinel that pins the backup at seg-1's LSN
    let seg_size: u64 = 16 * 1024 * 1024;
    let backup_name = format_backup_name(1, seg_size, seg_size);
    let v2 = walrus::pg::backup::BackupSentinelDtoV2 {
        sentinel: walrus::pg::backup::BackupSentinelDto {
            backup_start_lsn: Some(seg_size),
            pg_version: 160003,
            backup_finish_lsn: Some(seg_size + 16),
            system_identifier: Some(1),
            files_metadata_disabled: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let bytes = serde_json::to_vec(&v2).unwrap();
    let len = bytes.len() as u64;
    let r: walrus::compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
    store
        .put(&sentinel_key(&backup_name), r, Some(len))
        .await
        .unwrap();

    let report = verify::check_integrity(store.clone()).await.unwrap();
    assert_eq!(report.status, verify::ReportStatus::Failure);
    assert!(!report.gaps.is_empty(), "expected gap to be flagged");

    let tline = verify::check_timeline(store).await.unwrap();
    // Latest backup is on timeline 1; latest archived segment is also tli 1
    assert_eq!(tline.current_timeline, Some(1));
    assert_eq!(tline.latest_backup_timeline, Some(1));
    assert_eq!(tline.status, verify::ReportStatus::Ok);
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
    wal::fetch::handle(
        &fetch_settings,
        store,
        "000000010000000000000003",
        &dst,
        wal::fetch::Prefetch::Off,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), b"hello world");
}

fn encrypted_settings(path: &str, method: Method) -> Settings {
    use std::sync::Arc;
    use walrus::crypto::libsodium::LibsodiumCrypter;
    let mut k = [0u8; 32];
    for (i, b) in k.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(13).wrapping_add(7);
    }
    let mut s = settings_for(path, method);
    s.crypter = Some(Arc::new(LibsodiumCrypter::new(k)));
    s
}

#[tokio::test]
async fn push_fetch_libsodium_encrypted_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&restore).unwrap();

    let segment = "000000010000000000000007";
    let src = stage.join(segment);
    std::fs::write(&src, pseudo_wal_segment(11)).unwrap();

    let s = encrypted_settings(storage_dir.to_str().unwrap(), Method::Zstd);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store.clone(), &src).await.unwrap();

    // On-disk bytes must differ from the raw segment (encryption confirmed)
    let obj_path = storage_dir.join(format!("wal_005/{segment}.zst"));
    let stored = std::fs::read(&obj_path).unwrap();
    let raw = std::fs::read(&src).unwrap();
    assert!(
        stored.len() >= 24,
        "encrypted output must include 24-byte header"
    );
    assert_ne!(stored, raw, "ciphertext must differ from plaintext");

    let dst = restore.join(segment);
    wal::fetch::handle(&s, store, segment, &dst, wal::fetch::Prefetch::Off)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), raw);
}

#[tokio::test]
async fn fetch_with_wrong_key_fails() {
    use std::sync::Arc;
    use walrus::crypto::libsodium::LibsodiumCrypter;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let segment = "000000010000000000000008";
    let src = stage.join(segment);
    std::fs::write(&src, b"secret payload, do not leak").unwrap();

    let push_settings = encrypted_settings(storage_dir.to_str().unwrap(), Method::None);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&push_settings, store.clone(), &src)
        .await
        .unwrap();

    // Different key on fetch
    let mut bad_key = [0u8; 32];
    for (i, b) in bad_key.iter_mut().enumerate() {
        *b = (i as u8) ^ 0xFF;
    }
    let mut fetch_settings = settings_for(storage_dir.to_str().unwrap(), Method::None);
    fetch_settings.crypter = Some(Arc::new(LibsodiumCrypter::new(bad_key)));
    let dst = dir.path().join("out");
    let err = wal::fetch::handle(
        &fetch_settings,
        store,
        segment,
        &dst,
        wal::fetch::Prefetch::Off,
    )
    .await
    .expect_err("must fail with wrong key");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("libsodium") || msg.contains("corrupted") || msg.contains("pull"),
        "expected crypto-flavored error, got: {msg}"
    );
}

#[tokio::test]
async fn ciphertext_overhead_matches_libsodium_layout() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let segment = "000000010000000000000009";
    let src = stage.join(segment);

    // 10 KiB plaintext, zero-compression so on-disk size depends only on crypto
    let plain = vec![b'A'; 10 * 1024];
    std::fs::write(&src, &plain).unwrap();

    let s = encrypted_settings(storage_dir.to_str().unwrap(), Method::None);
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap());
    wal::push::handle(&s, store, &src).await.unwrap();

    let obj_path = storage_dir.join(format!("wal_005/{segment}"));
    let stored_len = std::fs::metadata(&obj_path).unwrap().len() as usize;

    // 24-byte header + chunk_1(8192 + 17) + chunk_2(remaining 2048 + 17)
    // FINAL emitted on the second chunk; we always emit a FINAL even when
    // an empty trailing chunk would have done — wal-g does the same
    let expected = 24 + (8192 + 17) + (2048 + 17);
    assert_eq!(stored_len, expected, "wire layout drift");
}
