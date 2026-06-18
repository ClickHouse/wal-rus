//! Retention (delete family) + copy integration tests against fs storage
//!
//! Seeds the bucket with a known mix of FULL & DELTA sentinels, permanent and
//! impermanent backups, and WAL segments. Exercises each `delete` mode and
//! the `copy` command end-to-end. Mirrors wal-g's `delete_test.go` shape

use std::sync::Arc;

use chrono::Utc;
use walross::compression::Method;
use walross::config::{Settings, StorageSettings};
use walross::pg::backup::copy as copy_mod;
use walross::pg::backup::delete::{
    self, DeleteModifier, DeleteOp, GarbageScope, try_extract_timeline_seg_no,
};
use walross::pg::backup::{
    BackupSentinelDto, BackupSentinelDtoV2, METADATA_DATETIME_FORMAT, sentinel_key, tar_part_key,
};
use walross::storage::Storage;
use walross::storage::fs::FsStorage;

fn test_settings() -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: "/tmp".into(),
        },
        compression: Method::Zstd,
        compression_level: 3,
        upload_concurrency: 2,
        upload_queue: 1,
        download_concurrency: 2,
        prevent_wal_overwrite: false,
        retry: walross::retry::RetryPolicy::default(),
        network_rate_limit: 0,
        disk_rate_limit: 0,
        delta: Default::default(),
        crypter: None,
    }
}

fn seg_size() -> u64 {
    16 * 1024 * 1024
}

fn make_sentinel(start_lsn: u64, is_permanent: bool) -> BackupSentinelDtoV2 {
    BackupSentinelDtoV2 {
        sentinel: BackupSentinelDto {
            backup_start_lsn: Some(start_lsn),
            increment_from_lsn: None,
            increment_from: None,
            increment_full_name: None,
            increment_count: None,
            increment_format: Default::default(),
            pg_version: 160003,
            backup_finish_lsn: Some(start_lsn + seg_size()),
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
        data_dir: "/var/lib/postgres/data".into(),
        is_permanent,
    }
}

async fn put_bytes(store: &Arc<FsStorage>, key: &str, body: Vec<u8>) {
    let len = body.len() as u64;
    let r: walross::compression::AsyncReader = Box::pin(std::io::Cursor::new(body));
    store.put(key, r, Some(len)).await.unwrap();
}

fn backup_name(timeline: u32, start_lsn: u64) -> String {
    walross::pg::backup::format_backup_name(timeline, start_lsn, seg_size())
}

/// Seed N backups with start LSNs `[1, 2, ..., N] * seg_size` and a few WAL
/// segments per backup. Returns `(store_dir, store, backup_names_in_order)`
async fn seed_bucket(n: u32) -> (tempfile::TempDir, Arc<FsStorage>, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    let mut names = Vec::new();
    for i in 0..n {
        let lsn = (i as u64 + 1) * seg_size();
        let name = backup_name(1, lsn);
        let sentinel = make_sentinel(lsn, false);
        put_bytes(
            &store,
            &sentinel_key(&name),
            serde_json::to_vec(&sentinel).unwrap(),
        )
        .await;
        // tar part
        put_bytes(&store, &tar_part_key(&name, 1, "zst"), b"x".to_vec()).await;
        // one WAL segment per backup
        let seg_no = i + 1;
        let wal_name = format!("00000001000000000000000{seg_no:X}.zst");
        put_bytes(
            &store,
            &format!("{}/{}", walross::pg::WAL_FOLDER, wal_name),
            b"wal".to_vec(),
        )
        .await;
        names.push(name);
    }
    (dir, store, names)
}

#[tokio::test]
async fn delete_retain_keeps_n_newest() {
    let (_dir, store, names) = seed_bucket(4).await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    let plan = delete::handle(
        dyn_store.clone(),
        DeleteOp::Retain {
            count: 2,
            modifier: DeleteModifier::None,
            after: None,
        },
        true,
    )
    .await
    .unwrap();

    // Target is the 2nd-newest (seg=3); everything strictly older than it must be deleted
    assert_eq!(plan.target.as_deref(), Some(names[2].as_str()));
    // Surviving backups: names[2] and names[3]
    for surviving in &names[2..] {
        assert!(
            dyn_store.exists(&sentinel_key(surviving)).await.unwrap(),
            "{surviving} must survive"
        );
    }
    for dead in &names[..2] {
        assert!(
            !dyn_store.exists(&sentinel_key(dead)).await.unwrap(),
            "{dead} should be deleted"
        );
    }
}

#[tokio::test]
async fn delete_before_name_drops_older_only() {
    let (_dir, store, names) = seed_bucket(4).await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    let plan = delete::handle(
        dyn_store.clone(),
        DeleteOp::Before {
            target: names[2].clone(),
            modifier: DeleteModifier::None,
        },
        true,
    )
    .await
    .unwrap();
    assert_eq!(plan.target.as_deref(), Some(names[2].as_str()));
    // The named target survives, anything older is gone
    assert!(dyn_store.exists(&sentinel_key(&names[2])).await.unwrap());
    assert!(!dyn_store.exists(&sentinel_key(&names[1])).await.unwrap());
    assert!(!dyn_store.exists(&sentinel_key(&names[0])).await.unwrap());
}

#[tokio::test]
async fn delete_everything_refuses_with_permanent_unless_force() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    let name = backup_name(1, seg_size());
    let sentinel = make_sentinel(seg_size(), /*is_permanent*/ true);
    put_bytes(
        &store,
        &sentinel_key(&name),
        serde_json::to_vec(&sentinel).unwrap(),
    )
    .await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;

    // without FORCE: refuses
    let err = delete::handle(
        dyn_store.clone(),
        DeleteOp::Everything { force: false },
        true,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("permanent"));

    // with FORCE: deletes everything
    delete::handle(
        dyn_store.clone(),
        DeleteOp::Everything { force: true },
        true,
    )
    .await
    .unwrap();
    assert!(!dyn_store.exists(&sentinel_key(&name)).await.unwrap());
}

#[tokio::test]
async fn delete_permanent_wal_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());

    // Two backups: older permanent (LSN seg 3), newer impermanent (LSN seg 6).
    // Then a few WAL segments scattered across [1..8]
    let perm_name = backup_name(1, 3 * seg_size());
    let perm_sentinel = make_sentinel(3 * seg_size(), true);
    put_bytes(
        &store,
        &sentinel_key(&perm_name),
        serde_json::to_vec(&perm_sentinel).unwrap(),
    )
    .await;
    let newer_name = backup_name(1, 6 * seg_size());
    let newer_sentinel = make_sentinel(6 * seg_size(), false);
    put_bytes(
        &store,
        &sentinel_key(&newer_name),
        serde_json::to_vec(&newer_sentinel).unwrap(),
    )
    .await;
    // tar parts under each
    put_bytes(&store, &tar_part_key(&perm_name, 1, "zst"), b"p".to_vec()).await;
    put_bytes(&store, &tar_part_key(&newer_name, 1, "zst"), b"n".to_vec()).await;
    // WAL segments seg 1..8 on timeline 1
    for seg in 1..=8u32 {
        let wal_name = format!("00000001000000000000000{seg:X}.zst");
        put_bytes(
            &store,
            &format!("{}/{}", walross::pg::WAL_FOLDER, wal_name),
            b"wal".to_vec(),
        )
        .await;
    }
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;

    delete::handle(
        dyn_store.clone(),
        DeleteOp::Before {
            target: newer_name.clone(),
            modifier: DeleteModifier::None,
        },
        true,
    )
    .await
    .unwrap();

    // The permanent backup's sentinel must survive even though it lives in the
    // "older than newer_name" partition
    assert!(dyn_store.exists(&sentinel_key(&perm_name)).await.unwrap());
    // The permanent backup reserves WAL segments [start_lsn-1 / seg_size,
    // finish_lsn-1 / seg_size]. For start=3*seg, finish=4*seg → start_lsn-1
    // lands in seg 2 and finish_lsn-1 lands in seg 3. Those must survive
    assert!(
        dyn_store
            .exists("wal_005/000000010000000000000002.zst")
            .await
            .unwrap(),
        "permanent backup reserves its start segment"
    );
    assert!(
        dyn_store
            .exists("wal_005/000000010000000000000003.zst")
            .await
            .unwrap(),
        "permanent backup reserves its finish segment"
    );
    // Seg 1 is below the permanent's range and gets deleted
    assert!(
        !dyn_store
            .exists("wal_005/000000010000000000000001.zst")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn delete_dry_run_does_not_delete() {
    let (_dir, store, names) = seed_bucket(3).await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    let plan = delete::handle(
        dyn_store.clone(),
        DeleteOp::Retain {
            count: 1,
            modifier: DeleteModifier::None,
            after: None,
        },
        false, // dry run
    )
    .await
    .unwrap();
    assert!(!plan.objects.is_empty());
    // No deletions actually happened
    for n in &names {
        assert!(dyn_store.exists(&sentinel_key(n)).await.unwrap());
    }
}

#[tokio::test]
async fn delete_garbage_scopes_to_wal_archives_only() {
    let (_dir, store, names) = seed_bucket(3).await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    // The oldest non-permanent backup is the first one; "garbage ARCHIVES"
    // deletes only WAL older than it (nothing here, since seg 1 == oldest)
    delete::handle(
        dyn_store.clone(),
        DeleteOp::Garbage {
            scope: GarbageScope::Archives,
        },
        true,
    )
    .await
    .unwrap();
    for n in &names {
        assert!(
            dyn_store.exists(&sentinel_key(n)).await.unwrap(),
            "{n} basebackup must survive an ARCHIVES-only garbage sweep"
        );
    }
}

#[tokio::test]
async fn delete_target_drops_delta_dependants() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    let full = backup_name(1, seg_size());
    let mut full_s = make_sentinel(seg_size(), false);
    full_s.sentinel.backup_start_lsn = Some(seg_size());
    put_bytes(
        &store,
        &sentinel_key(&full),
        serde_json::to_vec(&full_s).unwrap(),
    )
    .await;
    // Two deltas chained off the full
    let d1 = backup_name(1, 2 * seg_size());
    let mut d1_s = make_sentinel(2 * seg_size(), false);
    d1_s.sentinel.increment_from = Some(full.clone());
    d1_s.sentinel.increment_full_name = Some(full.clone());
    d1_s.sentinel.increment_count = Some(1);
    put_bytes(
        &store,
        &sentinel_key(&d1),
        serde_json::to_vec(&d1_s).unwrap(),
    )
    .await;
    let d2 = backup_name(1, 3 * seg_size());
    let mut d2_s = make_sentinel(3 * seg_size(), false);
    d2_s.sentinel.increment_from = Some(d1.clone());
    d2_s.sentinel.increment_full_name = Some(full.clone());
    d2_s.sentinel.increment_count = Some(2);
    put_bytes(
        &store,
        &sentinel_key(&d2),
        serde_json::to_vec(&d2_s).unwrap(),
    )
    .await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;

    // Default modifier deletes target + dependants. Targeting d1 should remove d1 and d2 but keep full
    delete::handle(
        dyn_store.clone(),
        DeleteOp::Target {
            name: d1.clone(),
            modifier: DeleteModifier::None,
        },
        true,
    )
    .await
    .unwrap();
    assert!(dyn_store.exists(&sentinel_key(&full)).await.unwrap());
    assert!(!dyn_store.exists(&sentinel_key(&d1)).await.unwrap());
    assert!(!dyn_store.exists(&sentinel_key(&d2)).await.unwrap());
}

#[tokio::test]
async fn delete_target_find_full_drops_chain_root() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    let full = backup_name(1, seg_size());
    let full_s = make_sentinel(seg_size(), false);
    put_bytes(
        &store,
        &sentinel_key(&full),
        serde_json::to_vec(&full_s).unwrap(),
    )
    .await;
    let d1 = backup_name(1, 2 * seg_size());
    let mut d1_s = make_sentinel(2 * seg_size(), false);
    d1_s.sentinel.increment_from = Some(full.clone());
    d1_s.sentinel.increment_full_name = Some(full.clone());
    put_bytes(
        &store,
        &sentinel_key(&d1),
        serde_json::to_vec(&d1_s).unwrap(),
    )
    .await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;

    // FIND_FULL on the delta should drop the whole chain (full + delta)
    delete::handle(
        dyn_store.clone(),
        DeleteOp::Target {
            name: d1.clone(),
            modifier: DeleteModifier::FindFull,
        },
        true,
    )
    .await
    .unwrap();
    assert!(!dyn_store.exists(&sentinel_key(&full)).await.unwrap());
    assert!(!dyn_store.exists(&sentinel_key(&d1)).await.unwrap());
}

#[tokio::test]
async fn copy_single_backup_to_other_fs_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let src_root = dir.path().join("src");
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&src_root).unwrap();
    let src = Arc::new(FsStorage::new(&src_root).unwrap());
    let dst = Arc::new(FsStorage::new(&dst_root).unwrap());

    let name = backup_name(1, seg_size());
    let sentinel = make_sentinel(seg_size(), false);
    put_bytes(
        &src,
        &sentinel_key(&name),
        serde_json::to_vec(&sentinel).unwrap(),
    )
    .await;
    put_bytes(&src, &tar_part_key(&name, 1, "zst"), b"abc".to_vec()).await;
    // One WAL segment inside the backup's LSN window
    put_bytes(
        &src,
        "wal_005/000000010000000000000001.zst",
        b"wal".to_vec(),
    )
    .await;

    let s = test_settings();
    let src_dyn: walross::storage::DynStorage = src as Arc<dyn Storage>;
    let dst_dyn: walross::storage::DynStorage = dst as Arc<dyn Storage>;
    copy_mod::handle(
        &s,
        src_dyn,
        dst_dyn.clone(),
        copy_mod::CopyArgs {
            backup_name: Some(name.clone()),
            all: false,
            with_history: false,
        },
    )
    .await
    .unwrap();

    // Destination has the sentinel, the tar part, and the in-window WAL
    assert!(dst_dyn.exists(&sentinel_key(&name)).await.unwrap());
    assert!(
        dst_dyn
            .exists(&tar_part_key(&name, 1, "zst"))
            .await
            .unwrap()
    );
    assert!(
        dst_dyn
            .exists("wal_005/000000010000000000000001.zst")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn delete_retain_after_keeps_newer_than_boundary() {
    // 4 backups, start_time ascending with seg_no. `retain 1 --after <ts>` where
    // the timestamp lands between #2 and #3 should keep base_3 + base_4 (count=1
    // newest + everything after the boundary), deleting base_1 + base_2
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    let mut names = Vec::new();
    let mut boundary = None;
    let t0 = Utc::now() - chrono::Duration::hours(4);
    for i in 0..4u32 {
        let lsn = (i as u64 + 1) * seg_size();
        let name = backup_name(1, lsn);
        let mut sentinel = make_sentinel(lsn, false);
        sentinel.start_time = t0 + chrono::Duration::hours(i as i64);
        if i == 2 {
            boundary = Some(sentinel.start_time - chrono::Duration::minutes(1));
        }
        put_bytes(
            &store,
            &sentinel_key(&name),
            serde_json::to_vec(&sentinel).unwrap(),
        )
        .await;
        names.push(name);
    }
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    let after = boundary.unwrap().to_rfc3339();
    let plan = delete::handle(
        dyn_store.clone(),
        DeleteOp::Retain {
            count: 1,
            modifier: DeleteModifier::None,
            after: Some(after),
        },
        true,
    )
    .await
    .unwrap();
    // anchor falls on base_3 (older of: Nth-newest=base_4, after-anchor=base_3)
    assert_eq!(plan.target.as_deref(), Some(names[2].as_str()));
    assert!(!dyn_store.exists(&sentinel_key(&names[0])).await.unwrap());
    assert!(!dyn_store.exists(&sentinel_key(&names[1])).await.unwrap());
    assert!(dyn_store.exists(&sentinel_key(&names[2])).await.unwrap());
    assert!(dyn_store.exists(&sentinel_key(&names[3])).await.unwrap());
}

#[tokio::test]
async fn backup_mark_target_user_data_flips_sentinel() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    // Two backups, one carrying UserData={"id":"tagged"}
    let n1 = backup_name(1, seg_size());
    let s1 = make_sentinel(seg_size(), false);
    put_bytes(&store, &sentinel_key(&n1), serde_json::to_vec(&s1).unwrap()).await;
    let n2 = backup_name(1, 2 * seg_size());
    let mut s2 = make_sentinel(2 * seg_size(), false);
    s2.sentinel.user_data = Some(serde_json::json!({"id": "tagged"}));
    put_bytes(&store, &sentinel_key(&n2), serde_json::to_vec(&s2).unwrap()).await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;

    let resolved =
        walross::pg::backup::show::resolve_by_user_data(&dyn_store, r#"{"id":"tagged"}"#)
            .await
            .unwrap();
    assert_eq!(resolved, n2);

    walross::pg::backup::show::mark(dyn_store.clone(), &resolved, true)
        .await
        .unwrap();
    // Reload sentinel & verify IsPermanent flipped on n2 but not n1
    let read = |key: String| {
        let s = dyn_store.clone();
        async move {
            let mut r = s.get(&key).await.unwrap();
            let mut buf = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut r, &mut buf)
                .await
                .unwrap();
            serde_json::from_slice::<BackupSentinelDtoV2>(&buf).unwrap()
        }
    };
    assert!(read(sentinel_key(&n2)).await.is_permanent);
    assert!(!read(sentinel_key(&n1)).await.is_permanent);
}

#[tokio::test]
async fn backup_mark_target_user_data_rejects_no_match() {
    let (_dir, store, _names) = seed_bucket(2).await;
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    let err = walross::pg::backup::show::resolve_by_user_data(&dyn_store, r#"{"id":"x"}"#)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no backup"));
}

#[tokio::test]
async fn backup_mark_target_user_data_rejects_ambiguous() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStorage::new(dir.path()).unwrap());
    // Two backups sharing the same UserData value
    for i in 0..2u32 {
        let lsn = (i as u64 + 1) * seg_size();
        let name = backup_name(1, lsn);
        let mut s = make_sentinel(lsn, false);
        s.sentinel.user_data = Some(serde_json::json!({"id": "dup"}));
        put_bytes(
            &store,
            &sentinel_key(&name),
            serde_json::to_vec(&s).unwrap(),
        )
        .await;
    }
    let dyn_store: walross::storage::DynStorage = store as Arc<dyn Storage>;
    let err = walross::pg::backup::show::resolve_by_user_data(&dyn_store, r#"{"id":"dup"}"#)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("backups match"));
}

#[tokio::test]
async fn try_extract_segno_handles_typical_keys() {
    // sentinel
    let r = try_extract_timeline_seg_no(
        "basebackups_005/base_000000010000000000000007_backup_stop_sentinel.json",
    )
    .unwrap();
    assert_eq!(r, (1, 7));
    // WAL with compression suffix
    let r = try_extract_timeline_seg_no("wal_005/00000002000000010000000C.lz4").unwrap();
    assert_eq!(r, (2, 256 + 12));
}
