//! Live-PG integration tests, gated on the `vm-test` cargo feature.
//!
//! Reads PGHOST/PGPORT/PGUSER/PGDATABASE from the environment so the VM
//! launcher script can target a specific cluster (PG 13–18). For trust-auth
//! local Debian clusters, PGUSER=postgres and PGPASSWORD unset.
//!
//! # Phase C/C.2 delta-apply live exercise — intentionally absent
//!
//! Delta backups depend on streamer-side emission of per-file wi1 / native
//! INCREMENTAL payloads during BASE_BACKUP. That rewrite is the load-bearing
//! open piece called out at the foot of PHASEC.md / PHASEC2.md / PHASEF.md;
//! today `WALG_DELTA_MAX_STEPS>0` and `--delta-from-wal-summaries` both run
//! the pre-flight eagerly then warn-and-fall-back-to-full, so the bucket
//! never claims a delta it can't deliver. Until that lands there is no way
//! to produce a delta-chain from a live PG that exercises a path beyond
//! what `pg::backup::increment::tests` already covers with crafted fixtures.
//! A live `delta_chain_against_live_pg` test belongs in the same pass that
//! lands the streamer rewrite.

#![cfg(feature = "vm-test")]

use std::sync::Arc;

use walross::compression::Method;
use walross::config::{Settings, StorageSettings};
use walross::pg::backup;
use walross::pg::wal;
use walross::storage::Storage;
use walross::storage::fs::FsStorage;

fn settings_for(path: &str) -> Settings {
    Settings {
        storage: StorageSettings::Fs { path: path.into() },
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

#[tokio::test]
async fn wal_push_fetch_byte_identity() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let stage = dir.path().join("stage");
    std::fs::create_dir_all(&stage).unwrap();

    // pseudo-WAL: 16 MB pattern. Not a real PG segment, but byte-roundtrip
    // is what we're verifying — the segment-parser layer treats it as opaque.
    let segment = "000000010000000000000001";
    let mut payload = vec![0u8; 16 * 1024 * 1024];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(0x37).wrapping_add(0x91);
    }
    let src = stage.join(segment);
    std::fs::write(&src, &payload).unwrap();

    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;
    wal::push::handle(&s, store.clone(), &src).await.unwrap();

    let dst = dir.path().join("restored");
    wal::fetch::handle(&s, store, segment, &dst).await.unwrap();
    let restored = std::fs::read(&dst).unwrap();
    assert_eq!(restored.len(), payload.len());
    assert_eq!(restored, payload);
}

#[tokio::test]
async fn backup_push_fetch_against_live_pg() {
    // Requires PGHOST=127.0.0.1, PGPORT=<cluster>, PGUSER=postgres, trust auth
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;

    let args = backup::push::PushArgs {
        pgdata: None,
        is_permanent: false,
        user_data: None,
        fast_checkpoint: true,
        no_verify_checksums: false,
        tar_size_threshold: 0,
        delta_from_wal_summaries: false,
        full: false,
    };
    backup::push::handle(&s, store.clone(), args)
        .await
        .expect("backup-push against live PG");

    // Verify the basebackup directory has a sentinel + at least one tar part
    let mut found_sentinel = false;
    let mut found_part = false;
    for entry in walkdir(&storage_dir.join("basebackups_005")) {
        if entry.ends_with("_backup_stop_sentinel.json") {
            found_sentinel = true;
        }
        if entry.contains("tar_partitions/part_") {
            found_part = true;
        }
    }
    assert!(found_sentinel, "no sentinel under {:?}", storage_dir);
    assert!(found_part, "no tar parts under {:?}", storage_dir);

    // If the cluster has user tablespaces, redirect them into the temp dir so
    // we don't try to write to /var/lib/postgresql which is postgres-owned
    let resolved = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    let sentinel_key = walross::pg::backup::sentinel_key(&resolved);
    let sentinel_bytes = std::fs::read(storage_dir.join(&sentinel_key)).unwrap();
    let v2_pre: walross::pg::backup::BackupSentinelDtoV2 =
        serde_json::from_slice(&sentinel_bytes).unwrap();
    let mut fetch_args = backup::fetch::FetchArgs::default();
    if let Some(spec) = v2_pre.sentinel.tablespace_spec.as_ref() {
        for name in &spec.tablespace_names {
            if let Some(loc) = spec.locations.get(name) {
                let target = restore.join(format!("tblspc_{name}"));
                fetch_args
                    .tablespace_mappings
                    .push((loc.location.clone(), target.to_string_lossy().into_owned()));
            }
        }
    }

    backup::fetch::handle_with_args(&s, store, "LATEST", &restore, &fetch_args)
        .await
        .expect("backup-fetch from LATEST");

    // PG_VERSION should be in every basebackup
    let pg_version_path = restore.join("PG_VERSION");
    assert!(
        pg_version_path.exists(),
        "PG_VERSION missing under {:?}",
        restore
    );
    let v = std::fs::read_to_string(&pg_version_path).unwrap();
    let major: u32 = v.trim().parse().expect("PG_VERSION integer");
    assert!((13..=30).contains(&major), "unexpected PG_VERSION={v}");

    // files_metadata.json should exist and parse, with at least PG_VERSION listed
    let mut fm_path = None;
    for entry in walkdir(&dir.path().join("storage").join("basebackups_005")) {
        if entry.ends_with("files_metadata.json") {
            fm_path = Some(entry);
            break;
        }
    }
    let fm_path = fm_path.expect("files_metadata.json missing from backup");
    let fm: walross::pg::backup::FilesMetadataDto =
        serde_json::from_slice(&std::fs::read(&fm_path).unwrap()).expect("parse files_metadata");
    assert!(
        fm.files.contains_key("PG_VERSION") || fm.files.values().count() > 0,
        "files_metadata.Files looks empty"
    );
    assert!(
        !fm.tar_file_sets.is_empty(),
        "TarFileSets should not be empty"
    );

    // Sentinel should now carry a positive compressed_size (B.12)
    let mut sentinel_bytes = None;
    for entry in walkdir(&dir.path().join("storage").join("basebackups_005")) {
        if entry.ends_with("_backup_stop_sentinel.json") {
            sentinel_bytes = Some(std::fs::read(&entry).unwrap());
            break;
        }
    }
    let v2: walross::pg::backup::BackupSentinelDtoV2 =
        serde_json::from_slice(&sentinel_bytes.unwrap()).unwrap();
    assert!(
        v2.sentinel.compressed_size > 0,
        "CompressedSize should reflect zstd-encoded bytes"
    );
    assert!(
        v2.sentinel.compressed_size <= v2.sentinel.uncompressed_size,
        "CompressedSize ({}) must not exceed UncompressedSize ({})",
        v2.sentinel.compressed_size,
        v2.sentinel.uncompressed_size
    );
    assert!(!v2.sentinel.files_metadata_disabled);
}

/// PG 14 (5434) and PG 15 (5435) on the VM carry pre-existing user
/// tablespaces. Pre-Phase-B behavior was to bail before any upload;
/// Phase B should now push, fetch, and recreate the tablespace symlinks
#[tokio::test]
async fn backup_with_user_tablespace_against_live_pg() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;

    let args = backup::push::PushArgs {
        pgdata: None,
        is_permanent: false,
        user_data: None,
        fast_checkpoint: true,
        no_verify_checksums: false,
        tar_size_threshold: 0,
        delta_from_wal_summaries: false,
        full: false,
    };
    let push_res = backup::push::handle(&s, store.clone(), args).await;
    // Some VM clusters have no user tablespace — that case is exercised by
    // the previous test. Here we tolerate either outcome but require that
    // _if_ we have a user tablespace, the sentinel carries Spec and the
    // restore reproduces the symlink
    push_res.expect("backup-push with user tablespace");

    // Find sentinel; if Spec is empty, skip the rest as not applicable
    let mut sentinel_path = None;
    for entry in walkdir(&storage_dir.join("basebackups_005")) {
        if entry.ends_with("_backup_stop_sentinel.json") {
            sentinel_path = Some(entry);
            break;
        }
    }
    let sentinel_path = sentinel_path.expect("sentinel missing");
    let v2: walross::pg::backup::BackupSentinelDtoV2 =
        serde_json::from_slice(&std::fs::read(&sentinel_path).unwrap()).unwrap();
    let spec = match v2.sentinel.tablespace_spec.clone() {
        Some(s) if !s.is_empty() => s,
        _ => {
            eprintln!("note: this cluster has no user tablespaces, skipping");
            return;
        }
    };
    eprintln!(
        "cluster has {} user tablespace(s): {:?}",
        spec.tablespace_names.len(),
        spec.tablespace_names
    );

    // For restore, remap each location into the temp dir so we don't write
    // outside the test sandbox (the real tablespace path on the VM is
    // owned by postgres user)
    let mut args = backup::fetch::FetchArgs::default();
    for name in &spec.tablespace_names {
        let loc = &spec.locations[name].location;
        let new = restore.join(format!("tblspc_{name}"));
        args.tablespace_mappings
            .push((loc.clone(), new.to_string_lossy().into_owned()));
    }
    backup::fetch::handle_with_args(&s, store, "LATEST", &restore, &args)
        .await
        .expect("backup-fetch with --tablespace-mapping");

    // Every named tablespace should now exist as a symlink under pg_tblspc/
    for name in &spec.tablespace_names {
        let link = restore.join("pg_tblspc").join(name);
        let md = std::fs::symlink_metadata(&link)
            .unwrap_or_else(|e| panic!("symlink_metadata({link:?}): {e}"));
        assert!(md.file_type().is_symlink(), "{link:?} should be a symlink");
    }
}

// ── Phase F carry: encrypted live-PG backup roundtrip ─────────────────────

fn encrypted_settings_for(path: &str) -> Settings {
    use walross::crypto::libsodium::LibsodiumCrypter;
    let mut k = [0u8; 32];
    for (i, b) in k.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(7).wrapping_add(11);
    }
    let mut s = settings_for(path);
    s.crypter = Some(Arc::new(LibsodiumCrypter::new(k)));
    s
}

fn default_push_args() -> backup::push::PushArgs {
    backup::push::PushArgs {
        pgdata: None,
        is_permanent: false,
        user_data: None,
        fast_checkpoint: true,
        no_verify_checksums: false,
        tar_size_threshold: 0,
        delta_from_wal_summaries: false,
        full: false,
    }
}

fn apply_tablespace_remap(
    spec: &walross::pg::backup::TablespaceSpec,
    restore: &std::path::Path,
    args: &mut backup::fetch::FetchArgs,
) {
    for name in &spec.tablespace_names {
        if let Some(loc) = spec.locations.get(name) {
            let target = restore.join(format!("tblspc_{name}"));
            args.tablespace_mappings
                .push((loc.location.clone(), target.to_string_lossy().into_owned()));
        }
    }
}

#[tokio::test]
async fn encrypted_backup_push_fetch_against_live_pg() {
    use walross::crypto::libsodium::LibsodiumCrypter;

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let restore = dir.path().join("restore");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let s = encrypted_settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;

    backup::push::handle(&s, store.clone(), default_push_args())
        .await
        .expect("encrypted backup-push against live PG");

    // Tar parts must not contain plaintext "PG_VERSION" — sanity gate that
    // encryption is actually engaged. (Sentinels & files_metadata.json
    // intentionally bypass the crypter, matching wal-g's UploadDto path)
    let mut tar_part = None;
    for entry in walkdir(&storage_dir.join("basebackups_005")) {
        if entry.contains("tar_partitions/part_") {
            tar_part = Some(entry);
            break;
        }
    }
    let tar_part = tar_part.expect("tar part missing under basebackups_005");
    let raw = std::fs::read(&tar_part).unwrap();
    let needle = b"PG_VERSION";
    assert!(
        !raw.windows(needle.len()).any(|w| w == needle),
        "encrypted tar part still contains plaintext PG_VERSION at {tar_part:?}"
    );

    // Right-key fetch decrypts cleanly + PG_VERSION readable
    let resolved = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    let sentinel_bytes =
        std::fs::read(storage_dir.join(walross::pg::backup::sentinel_key(&resolved))).unwrap();
    let v2: walross::pg::backup::BackupSentinelDtoV2 =
        serde_json::from_slice(&sentinel_bytes).unwrap();
    let mut fetch_args = backup::fetch::FetchArgs::default();
    if let Some(spec) = v2.sentinel.tablespace_spec.as_ref() {
        apply_tablespace_remap(spec, &restore, &mut fetch_args);
    }
    backup::fetch::handle_with_args(&s, store.clone(), "LATEST", &restore, &fetch_args)
        .await
        .expect("encrypted backup-fetch with correct key");
    let v = std::fs::read_to_string(restore.join("PG_VERSION")).unwrap();
    let major: u32 = v.trim().parse().expect("PG_VERSION integer");
    assert!((13..=30).contains(&major), "unexpected PG_VERSION={v}");

    // Wrong-key fetch must error with a crypto-flavored message
    let wrong = dir.path().join("restore-wrong");
    std::fs::create_dir_all(&wrong).unwrap();
    let mut s_wrong = settings_for(storage_dir.to_str().unwrap());
    s_wrong.crypter = Some(Arc::new(LibsodiumCrypter::new([0u8; 32])));
    let err = backup::fetch::handle_with_args(&s_wrong, store, "LATEST", &wrong, &fetch_args)
        .await
        .expect_err("fetch with wrong key must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("libsodium") || msg.contains("corrupted") || msg.contains("pull"),
        "expected crypto-flavored error, got: {msg}"
    );
}

// ── Phase E carry: live-PG retention exercise ─────────────────────────────

#[tokio::test]
async fn retain_full_one_against_live_pg() {
    use walross::pg::backup::delete::{DeleteModifier, DeleteOp};

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;

    // Two full backups, serialized. Each backup-push's pg_stop_backup advances
    // the LSN, so the second backup's name sorts strictly after the first
    for _ in 0..2 {
        backup::push::handle(&s, store.clone(), default_push_args())
            .await
            .expect("backup-push");
    }

    let pre = backup::list::collect(store.clone()).await.unwrap();
    assert_eq!(pre.len(), 2, "expected 2 backups, got {pre:?}");
    let kept_name = pre.last().expect("non-empty").name.clone();

    let op = DeleteOp::Retain {
        count: 1,
        modifier: DeleteModifier::Full,
        after: None,
    };
    walross::pg::backup::delete::handle(store.clone(), op, true)
        .await
        .expect("delete retain FULL 1");

    let post = backup::list::collect(store).await.unwrap();
    assert_eq!(
        post.len(),
        1,
        "retain FULL 1 should leave exactly one backup"
    );
    assert_eq!(post[0].name, kept_name, "expected newest backup to survive");
}

// ── Phase D carry: live-PG wal-receive exercise ───────────────────────────

#[tokio::test]
async fn wal_receive_archives_segment_against_live_pg() {
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    let archive_dir = dir.path().join("archive");
    std::fs::create_dir_all(&storage_dir).unwrap();
    std::fs::create_dir_all(&archive_dir).unwrap();

    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;

    let s_recv = s.clone();
    let store_recv = store.clone();
    let archive_path = archive_dir.clone();
    let mut receive_task =
        tokio::spawn(async move { wal::receive::handle(&s_recv, store_recv, &archive_path).await });

    // Let START_REPLICATION establish before forcing segment rotations
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Drive segment rotations via psql. pg_switch_wal is the only thing
    // guaranteed to FULLY fill the current segment (backup-push's
    // pg_backup_stop only writes a switch record on some PG configs, which
    // doesn't put zero-pad bytes on the wire before the new segment starts).
    // Fire 3 in quick succession so wal-receive accumulates >1 full segment
    // even if the first switch lands mid-frame.
    let pghost = std::env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into());
    let pgport = std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let pguser = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let pgdb = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into());
    for _ in 0..3 {
        let out = std::process::Command::new("psql")
            .args([
                "-h",
                &pghost,
                "-p",
                &pgport,
                "-U",
                &pguser,
                "-d",
                &pgdb,
                "-Atqc",
                "SELECT pg_switch_wal()",
            ])
            .output()
            .expect("invoke psql; install postgresql-client if missing");
        assert!(
            out.status.success(),
            "psql pg_switch_wal failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut entries = Vec::new();
    while Instant::now() < deadline {
        // Surface a fast receive-task crash rather than waiting out the full
        // deadline for a connection that died on auth
        if receive_task.is_finished() {
            let r = (&mut receive_task).await;
            panic!("wal-receive task exited early: {r:?}");
        }
        let wal_root = storage_dir.join("wal_005");
        if wal_root.exists() {
            entries = walkdir(&wal_root);
            if !entries.is_empty() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let archive_state = walkdir(&archive_dir);
    receive_task.abort();

    assert!(
        !entries.is_empty(),
        "wal-receive did not archive any segment within 45s; \
         archive_dir contents={archive_state:?}, storage_dir={storage_dir:?}"
    );
    let has_seg = entries.iter().any(|e| {
        std::path::Path::new(e)
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.split('.').next())
            .map(|stem| stem.len() == 24 && stem.chars().all(|c| c.is_ascii_hexdigit()))
            .unwrap_or(false)
    });
    assert!(
        has_seg,
        "no 24-hex-char WAL segment in archive entries: {entries:?}"
    );
}

fn walkdir(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Some(s) = path.to_str() {
                out.push(s.to_string());
            }
        }
    }
    out
}
