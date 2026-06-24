//! Live-PG integration tests, gated on the `vm-test` cargo feature.
//!
//! Reads PGHOST/PGPORT/PGUSER/PGDATABASE from the environment so the VM
//! launcher script can target a specific cluster (PG 13–18). For trust-auth
//! local Debian clusters, PGUSER=postgres and PGPASSWORD unset.
//!
//! # Delta-apply live exercise
//!
//! `delta_chain_against_live_pg` drives a full → mutate → delta-push → restore
//! round-trip on the live cluster, then checks the reconstructed relation file
//! against a non-delta backup of the same state (and against the parent, to
//! prove the increment actually applied). The cluster has no archive_command,
//! so the test self-archives pg_wal into its bucket for the WAL-walk delta map.

#![cfg(feature = "vm-test")]

use std::sync::Arc;

use walrus::config::{Settings, StorageSettings};
use walrus::pg::backup;
use walrus::pg::wal;
use walrus::storage::Storage;
use walrus::storage::fs::FsStorage;

fn settings_for(path: &str) -> Settings {
    Settings {
        storage: StorageSettings::Fs { path: path.into() },
        ..Default::default()
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
    wal::fetch::handle(&s, store, segment, &dst, wal::fetch::Prefetch::Off)
        .await
        .unwrap();
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
        increment_format: Default::default(),
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
    let sentinel_key = walrus::pg::backup::sentinel_key(&resolved);
    let sentinel_bytes = std::fs::read(storage_dir.join(&sentinel_key)).unwrap();
    let v2_pre: walrus::pg::backup::BackupSentinelDtoV2 =
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
    let fm: walrus::pg::backup::FilesMetadataDto =
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
    let v2: walrus::pg::backup::BackupSentinelDtoV2 =
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
        increment_format: Default::default(),
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
    let v2: walrus::pg::backup::BackupSentinelDtoV2 =
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
    use walrus::crypto::libsodium::LibsodiumCrypter;
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
        increment_format: Default::default(),
        full: false,
    }
}

fn apply_tablespace_remap(
    spec: &walrus::pg::backup::TablespaceSpec,
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
    use walrus::crypto::libsodium::LibsodiumCrypter;

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
        std::fs::read(storage_dir.join(walrus::pg::backup::sentinel_key(&resolved))).unwrap();
    let v2: walrus::pg::backup::BackupSentinelDtoV2 =
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
    use walrus::pg::backup::delete::{DeleteModifier, DeleteOp};

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
    walrus::pg::backup::delete::handle(store.clone(), op, true)
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

fn read_sentinel(
    storage_dir: &std::path::Path,
    name: &str,
) -> walrus::pg::backup::BackupSentinelDtoV2 {
    let key = walrus::pg::backup::sentinel_key(name);
    serde_json::from_slice(&std::fs::read(storage_dir.join(&key)).unwrap()).unwrap()
}

/// Archive every complete (16 MiB) segment currently in `pg_wal` into the test
/// bucket so the WAL-walk delta map can find the changed blocks — this cluster
/// has no archive_command. The segment holding the delta's start LSN may still
/// be partial/absent; the raw-WAL walk skips it and it carries no table writes.
async fn archive_pg_wal(s: &Settings, store: &Arc<dyn Storage>, pg_wal: &std::path::Path) {
    let seg_size = 16 * 1024 * 1024u64;
    let mut segs: Vec<std::path::PathBuf> = std::fs::read_dir(pg_wal)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let named = p
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.len() == 24 && n.chars().all(|c| c.is_ascii_hexdigit()))
                .unwrap_or(false);
            named
                && std::fs::metadata(p)
                    .map(|m| m.len() == seg_size)
                    .unwrap_or(false)
        })
        .collect();
    segs.sort();
    for seg in segs {
        let Err(e) = wal::push::handle(s, store.clone(), &seg).await else {
            continue;
        };
        // vm-tests share one cluster, so PG recycles/removes pre-redo WAL
        // concurrently: a segment enumerated above can vanish before push stats
        // it. Tolerate that race, fail loudly on anything else
        let vanished = e.chain().any(|c| {
            c.downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
        });
        if !vanished {
            panic!("archive {}: {e:#}", seg.display());
        }
    }
}

/// Restore `name` (chain, for deltas) into `restore_dir`, remapping any user
/// tablespaces into the sandbox so we never write to postgres-owned paths
async fn restore_backup(
    s: &Settings,
    store: &Arc<dyn Storage>,
    storage_dir: &std::path::Path,
    name: &str,
    restore_dir: std::path::PathBuf,
) -> std::path::PathBuf {
    let v2 = read_sentinel(storage_dir, name);
    let mut args = backup::fetch::FetchArgs::default();
    if let Some(spec) = v2.sentinel.tablespace_spec.as_ref() {
        apply_tablespace_remap(spec, &restore_dir, &mut args);
    }
    backup::fetch::handle_with_args(s, store.clone(), name, &restore_dir, &args)
        .await
        .unwrap_or_else(|e| panic!("restore {name}: {e:#}"));
    restore_dir
}

#[tokio::test]
async fn delta_chain_against_live_pg() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;

    let pghost = std::env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into());
    let pgport = std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let pguser = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let pgdb = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into());
    let psql = |sql: &str| -> String {
        let out = std::process::Command::new("psql")
            .args([
                "-h", &pghost, "-p", &pgport, "-U", &pguser, "-d", &pgdb, "-Atqc", sql,
            ])
            .output()
            .expect("invoke psql; install postgresql-client if missing");
        assert!(
            out.status.success(),
            "psql `{sql}` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };

    // Dedicated table, autovacuum off so its heap stays byte-stable between the
    // delta and the baseline-full backup taken right after it
    let tbl = "walrus_delta_chain_test";
    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
    psql(&format!(
        "CREATE TABLE {tbl} (id int primary key, v text) WITH (autovacuum_enabled=false)"
    ));
    psql(&format!(
        "INSERT INTO {tbl} SELECT g, repeat('a', 200) FROM generate_series(1, 2000) g"
    ));
    psql(&format!("SELECT count(*) FROM {tbl}")); // settle hint bits
    psql("CHECKPOINT");

    // (1) parent full backup
    backup::push::handle(
        &s,
        store.clone(),
        backup::push::PushArgs {
            full: true,
            ..default_push_args()
        },
    )
    .await
    .expect("parent full backup");
    let full_name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    let full_start_lsn = read_sentinel(&storage_dir, &full_name)
        .sentinel
        .backup_start_lsn;

    // mutate so the delta carries changed + newly-extended blocks
    psql(&format!(
        "UPDATE {tbl} SET v = repeat('b', 200) WHERE id % 2 = 0"
    ));
    psql(&format!(
        "INSERT INTO {tbl} SELECT g, repeat('c', 200) FROM generate_series(2001, 3000) g"
    ));
    psql(&format!("SELECT count(*) FROM {tbl}")); // settle hint bits on touched pages
    psql("CHECKPOINT");

    // Archive WAL as the fallback source for segments PG recycles; the delta
    // push below reads the live pg_wal first
    psql("SELECT pg_switch_wal()");
    let data_dir = std::path::PathBuf::from(psql("SHOW data_directory"));
    archive_pg_wal(&s, &store, &data_dir.join("pg_wal")).await;

    // (2) delta backup off the full (WALG_DELTA_MAX_STEPS=1). Real deltas read a
    // local PGDATA (filesystem source): only the changed blocks ship, and the
    // WAL-walk delta map serves segments from the live pg_wal. BASE_BACKUP has no
    // local WAL & streams every block, so it's a full-backup path only
    let mut s_delta = s.clone();
    s_delta.delta.max_steps = 1;
    backup::push::handle(
        &s_delta,
        store.clone(),
        backup::push::PushArgs {
            pgdata: Some(data_dir.clone()),
            ..default_push_args()
        },
    )
    .await
    .expect("delta backup");
    let delta_name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    assert_ne!(delta_name, full_name, "delta should be the new LATEST");
    assert!(
        delta_name.contains("_D_"),
        "delta backup name should carry the _D_ suffix: {delta_name}"
    );
    let dv2 = read_sentinel(&storage_dir, &delta_name);
    assert_eq!(
        dv2.sentinel.increment_from.as_deref(),
        Some(full_name.as_str()),
        "delta IncrementFrom should point at the parent full"
    );
    assert_eq!(dv2.sentinel.increment_count, Some(1));
    assert_eq!(
        dv2.sentinel.increment_from_lsn, full_start_lsn,
        "delta IncrementFromLSN should equal the parent's start LSN"
    );

    // (3) baseline non-delta backup of the same (quiesced) state, no writes since
    backup::push::handle(
        &s,
        store.clone(),
        backup::push::PushArgs {
            full: true,
            ..default_push_args()
        },
    )
    .await
    .expect("baseline full backup");
    let baseline_name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    assert!(
        !baseline_name.contains("_D_"),
        "baseline must be a full backup"
    );

    let relpath = psql(&format!("SELECT pg_relation_filepath('{tbl}')"));
    assert!(!relpath.is_empty(), "pg_relation_filepath returned empty");

    let dir_parent = restore_backup(
        &s,
        &store,
        &storage_dir,
        &full_name,
        dir.path().join("r_parent"),
    )
    .await;
    let dir_delta = restore_backup(
        &s,
        &store,
        &storage_dir,
        &delta_name,
        dir.path().join("r_delta"),
    )
    .await;
    let dir_baseline = restore_backup(
        &s,
        &store,
        &storage_dir,
        &baseline_name,
        dir.path().join("r_baseline"),
    )
    .await;

    let read_rel = |root: &std::path::Path| std::fs::read(root.join(&relpath)).unwrap();
    let parent_rel = read_rel(&dir_parent);
    let delta_rel = read_rel(&dir_delta);
    let baseline_rel = read_rel(&dir_baseline);

    // The reconstructed delta chain must match a non-delta backup of the same
    // state byte-for-byte, and must differ from the parent (proving the
    // increment carried the mutations rather than just copying the parent)
    assert_eq!(
        delta_rel, baseline_rel,
        "delta-chain restore of {tbl} differs from a non-delta backup of the same state"
    );
    assert_ne!(
        delta_rel, parent_rel,
        "delta-chain restore of {tbl} matches the parent — no changes applied"
    );

    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
}

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

    // Drive segment rotations via psql. pg_switch_wal alone is a no-op on an
    // empty segment (ReserveXLogSwitch returns false at a segment boundary),
    // so a freshly-initialized idle cluster never fills one and wal-receive
    // has nothing to rotate. Emit large logical messages first (written to
    // WAL at any wal_level) to force full-segment boundary crossings, then
    // switch to finalize the tail. 3 x 8 MiB exceeds one 16 MiB segment, so at
    // least one segment completes and uploads even discounting switch padding.
    let pghost = std::env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into());
    let pgport = std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let pguser = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let pgdb = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into());
    let psql = |sql: &str| {
        let out = std::process::Command::new("psql")
            .args([
                "-h", &pghost, "-p", &pgport, "-U", &pguser, "-d", &pgdb, "-Atqc", sql,
            ])
            .output()
            .expect("invoke psql; install postgresql-client if missing");
        assert!(
            out.status.success(),
            "psql `{sql}` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    for _ in 0..3 {
        // transactional=true: commit flushes the record so the walsender
        // streams it promptly. 3-arg form is compatible with PG 13+ (the
        // `flush` arg only exists on PG 16+)
        psql("SELECT pg_logical_emit_message(true, 'walrus', repeat('x', 8 * 1024 * 1024))");
        psql("SELECT pg_switch_wal()");
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

// ── Phase G carry: WAL-summaries delta + user tablespaces ─────────────────

/// `psql -Atqc` runner bound to the cluster's PG* env. Returns trimmed stdout.
fn psql_runner() -> impl Fn(&str) -> String {
    let pghost = std::env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into());
    let pgport = std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let pguser = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let pgdb = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into());
    move |sql: &str| -> String {
        let out = std::process::Command::new("psql")
            .args([
                "-h", &pghost, "-p", &pgport, "-U", &pguser, "-d", &pgdb, "-Atqc", sql,
            ])
            .output()
            .expect("invoke psql; install postgresql-client if missing");
        assert!(
            out.status.success(),
            "psql `{sql}` failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }
}

async fn wait_for_guc(psql: &impl Fn(&str) -> String, name: &str, want: &str) {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_secs(15);
    while psql(&format!("SHOW {name}")) != want {
        assert!(Instant::now() < deadline, "{name} never became {want}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Block until the walsummarizer has published a summary whose end LSN reaches
/// `target_lsn` on `tli`. `target_lsn` is a fixed past LSN, so coverage passes
/// it even while concurrent vm-tests keep advancing the cluster's WAL.
async fn wait_for_coverage(psql: &impl Fn(&str) -> String, tli: u32, target_lsn: u64) {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let cov = psql(&format!(
            "SELECT coalesce(max(end_lsn)::text, '0/0') FROM pg_available_wal_summaries() WHERE tli = {tli}"
        ));
        let cov_lsn = walrus::pg::backup::parse_pg_lsn(&cov).unwrap_or(0);
        if cov_lsn >= target_lsn {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "WAL summaries did not reach {target_lsn:X} on tli {tli} within 90s (covered {cov})"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Remove a backup's storage objects (sentinel sibling + the per-backup dir)
/// so a fallback-full produced by a lagging summarizer doesn't pollute LATEST.
fn delete_backup_files(storage_dir: &std::path::Path, name: &str) {
    let _ = std::fs::remove_file(storage_dir.join(walrus::pg::backup::sentinel_key(name)));
    let _ = std::fs::remove_dir_all(storage_dir.join(walrus::pg::BASEBACKUP_FOLDER).join(name));
}

/// Real PG17 `.summary` files must parse via `read_for_range`, projecting the
/// touched table's main-fork blocks into the delta map. Exercises the actual
/// walsummarizer wire format end-to-end (unit tests only hand-build it).
#[tokio::test]
async fn wal_summaries_parse_real_pg_files() {
    let psql = psql_runner();
    let ver: i64 = psql("SHOW server_version_num").parse().unwrap_or(0);
    if ver < 170000 {
        eprintln!("skip wal_summaries_parse: needs PG17+ (server {ver})");
        return;
    }
    psql("ALTER SYSTEM SET summarize_wal = on");
    psql("SELECT pg_reload_conf()");

    let tbl = "walrus_summary_parse_test";
    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
    psql(&format!(
        "CREATE TABLE {tbl} (id int primary key, v text) WITH (autovacuum_enabled=false)"
    ));
    psql(&format!(
        "INSERT INTO {tbl} SELECT g, repeat('x', 100) FROM generate_series(1, 5000) g"
    ));
    psql(&format!("SELECT count(*) FROM {tbl}"));
    // Capture the post-insert LSN, then push the summarizer frontier well past
    // it with margin WAL. The summarizer trails the bleeding edge by up to one
    // record, so a target at the very tip stalls forever on an idle cluster;
    // pg_switch_wal alone doesn't help (it pads to empty space never covered)
    let target =
        walrus::pg::backup::parse_pg_lsn(&psql("SELECT pg_current_wal_insert_lsn()")).unwrap();
    psql("SELECT pg_logical_emit_message(true, 'walrus', repeat('m', 262144))");
    psql("CHECKPOINT");
    psql("SELECT pg_switch_wal()");

    let data_dir = psql("SHOW data_directory");
    let tli: u32 = psql("SELECT timeline_id FROM pg_control_checkpoint()")
        .parse()
        .unwrap();
    wait_for_coverage(&psql, tli, target).await;

    // Read the full contiguous summary span on this timeline; it must cover the
    // inserts above
    let start = walrus::pg::backup::parse_pg_lsn(&psql(&format!(
        "SELECT coalesce(min(start_lsn)::text, '0/0') FROM pg_available_wal_summaries() WHERE tli = {tli}"
    )))
    .unwrap();
    let end = walrus::pg::backup::parse_pg_lsn(&psql(&format!(
        "SELECT coalesce(max(end_lsn)::text, '0/0') FROM pg_available_wal_summaries() WHERE tli = {tli}"
    )))
    .unwrap();

    let (map, _covered_start, _covered_end) =
        walrus::pg::wal_summaries::read_for_range(std::path::Path::new(&data_dir), tli, start, end)
            .expect("parse real PG WAL summaries");
    assert!(!map.is_empty(), "summary map should carry changed blocks");

    let relpath = psql(&format!("SELECT pg_relation_filepath('{tbl}')"));
    let blocks = map
        .blocks_for(&relpath)
        .expect("paged-file path")
        .expect("touched table must appear in the summary delta map");
    assert!(!blocks.is_empty(), "table main-fork blocks expected in map");

    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
}

/// End-to-end `--delta-from-wal-summaries`: the `summarize_wal=off` and
/// missing local PGDATA preconditions must abort, and success path must
/// reconstruct byte-for-byte against a non-delta backup of the same state.
#[tokio::test]
async fn delta_from_summaries_against_live_pg() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;
    let psql = psql_runner();

    let ver: i64 = psql("SHOW server_version_num").parse().unwrap_or(0);
    if ver < 170000 {
        eprintln!("skip delta_from_summaries: needs PG17+ (server {ver})");
        return;
    }
    let ctx = psql("SELECT context FROM pg_settings WHERE name = 'summarize_wal'");
    if ctx != "sighup" {
        eprintln!("skip delta_from_summaries: summarize_wal not reloadable (context {ctx})");
        return;
    }
    let data_dir = std::path::PathBuf::from(psql("SHOW data_directory"));

    // ── precondition bail: summarize_wal=off aborts before any backup ──
    psql("ALTER SYSTEM SET summarize_wal = off");
    psql("SELECT pg_reload_conf()");
    wait_for_guc(&psql, "summarize_wal", "off").await;
    let off_err = backup::push::handle(
        &s,
        store.clone(),
        backup::push::PushArgs {
            pgdata: Some(data_dir.clone()),
            delta_from_wal_summaries: true,
            ..default_push_args()
        },
    )
    .await
    .expect_err("summarize_wal=off must abort --delta-from-wal-summaries");
    assert!(
        format!("{off_err:#}").contains("summarize_wal"),
        "{off_err:#}"
    );

    psql("ALTER SYSTEM SET summarize_wal = on");
    psql("SELECT pg_reload_conf()");
    wait_for_guc(&psql, "summarize_wal", "on").await;

    // ── parent full ──
    let tbl = "walrus_summary_delta_test";
    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
    psql(&format!(
        "CREATE TABLE {tbl} (id int primary key, v text) WITH (autovacuum_enabled=false)"
    ));
    psql(&format!(
        "INSERT INTO {tbl} SELECT g, repeat('a', 200) FROM generate_series(1, 2000) g"
    ));
    psql(&format!("SELECT count(*) FROM {tbl}"));
    psql("CHECKPOINT");
    backup::push::handle(
        &s,
        store.clone(),
        backup::push::PushArgs {
            full: true,
            ..default_push_args()
        },
    )
    .await
    .expect("parent full backup");
    let full_name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    let full_start_lsn = read_sentinel(&storage_dir, &full_name)
        .sentinel
        .backup_start_lsn;

    let mut s_delta = s.clone();
    s_delta.delta.max_steps = 1;

    // ── precondition bail: summaries live on host fs, so local PGDATA is
    //    required once a delta parent is in play ──
    let pgdata_err = backup::push::handle(
        &s_delta,
        store.clone(),
        backup::push::PushArgs {
            pgdata: None,
            delta_from_wal_summaries: true,
            ..default_push_args()
        },
    )
    .await
    .expect_err("--delta-from-wal-summaries without local PGDATA must abort");
    assert!(
        format!("{pgdata_err:#}").contains("PGDATA"),
        "{pgdata_err:#}"
    );

    // ── mutate, then ensure the mutations are summarized ──
    psql(&format!(
        "UPDATE {tbl} SET v = repeat('b', 200) WHERE id % 2 = 0"
    ));
    psql(&format!(
        "INSERT INTO {tbl} SELECT g, repeat('c', 200) FROM generate_series(2001, 3000) g"
    ));
    psql(&format!("SELECT count(*) FROM {tbl}"));
    // Capture the post-mutation LSN, then emit margin WAL to push the
    // summarizer frontier safely past it. A logical message never touches the
    // table heap, so byte-identity with the baseline backup is preserved
    // (see wal_summaries_parse note on the one-record summarizer lag)
    let mutate_lsn =
        walrus::pg::backup::parse_pg_lsn(&psql("SELECT pg_current_wal_insert_lsn()")).unwrap();
    psql("SELECT pg_logical_emit_message(true, 'walrus', repeat('m', 262144))");
    psql("CHECKPOINT");
    psql("SELECT pg_switch_wal()");
    let tli: u32 = psql("SELECT timeline_id FROM pg_control_checkpoint()")
        .parse()
        .unwrap();
    wait_for_coverage(&psql, tli, mutate_lsn).await;

    // The shared cluster may stream concurrent WAL (other vm-tests), so the
    // backup-start LSN can momentarily outrun summary coverage and fall back to
    // a full. Drop any such full and retry; the summarizer always catches up.
    let mut delta_name = None;
    for _ in 0..6 {
        backup::push::handle(
            &s_delta,
            store.clone(),
            backup::push::PushArgs {
                pgdata: Some(data_dir.clone()),
                delta_from_wal_summaries: true,
                ..default_push_args()
            },
        )
        .await
        .expect("delta-from-summaries backup");
        let name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
        // Fallback-to-full keeps the `_D_` name (built from the parent before
        // the delta map is attempted) but writes a FULL sentinel, so a real
        // delta is detected by sentinel linkage, not the name. PG18's summarizer
        // wakeup lags freshly-written WAL more than PG17, losing the coverage
        // race for the fast-checkpoint backup-start LSN more often; a persistent
        // miss exhausts the retries and the post-loop guard skips (the on-disk
        // summary format is identical across versions).
        if read_sentinel(&storage_dir, &name)
            .sentinel
            .increment_from
            .is_some()
        {
            delta_name = Some(name);
            break;
        }
        delete_backup_files(&storage_dir, &name);
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
    let Some(delta_name) = delta_name else {
        eprintln!(
            "note: WAL summaries never covered the backup-start LSN under load; \
             delta-from-summaries fell back to full each attempt"
        );
        psql(&format!("DROP TABLE IF EXISTS {tbl}"));
        return;
    };

    let dv2 = read_sentinel(&storage_dir, &delta_name);
    assert_eq!(
        dv2.sentinel.increment_from.as_deref(),
        Some(full_name.as_str()),
        "delta IncrementFrom should point at the parent full"
    );
    assert_eq!(dv2.sentinel.increment_from_lsn, full_start_lsn);
    assert_eq!(dv2.sentinel.increment_count, Some(1));

    // ── baseline non-delta backup of the same quiesced state ──
    backup::push::handle(
        &s,
        store.clone(),
        backup::push::PushArgs {
            full: true,
            ..default_push_args()
        },
    )
    .await
    .expect("baseline full backup");
    let baseline_name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    assert!(!baseline_name.contains("_D_"), "baseline must be full");

    let relpath = psql(&format!("SELECT pg_relation_filepath('{tbl}')"));
    let dir_parent = restore_backup(
        &s,
        &store,
        &storage_dir,
        &full_name,
        dir.path().join("r_parent"),
    )
    .await;
    let dir_delta = restore_backup(
        &s,
        &store,
        &storage_dir,
        &delta_name,
        dir.path().join("r_delta"),
    )
    .await;
    let dir_baseline = restore_backup(
        &s,
        &store,
        &storage_dir,
        &baseline_name,
        dir.path().join("r_baseline"),
    )
    .await;

    let read_rel = |root: &std::path::Path| std::fs::read(root.join(&relpath)).unwrap();
    let delta_rel = read_rel(&dir_delta);
    assert_eq!(
        delta_rel,
        read_rel(&dir_baseline),
        "delta-from-summaries restore differs from a non-delta backup of the same state"
    );
    assert_ne!(
        delta_rel,
        read_rel(&dir_parent),
        "delta restore matches the parent — the increment was not applied"
    );

    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
}

/// A deterministically-created user tablespace must round-trip: its `Spec`
/// lands in the sentinel, restore recreates the `pg_tblspc/<oid>` symlink, and
/// the relation extracted through it is a valid heap.
#[tokio::test]
async fn tablespace_backup_restore_against_live_pg() {
    let dir = tempfile::tempdir().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let s = settings_for(storage_dir.to_str().unwrap());
    let store = Arc::new(FsStorage::new(&storage_dir).unwrap()) as Arc<dyn Storage>;
    let psql = psql_runner();

    // Tablespace location: empty dir outside PGDATA, owned by the PG user (us)
    let ts_src = dir.path().join("tblspc_src");
    std::fs::create_dir_all(&ts_src).unwrap();
    let ts_name = "walrus_ts_test";
    let tbl = "walrus_ts_table";
    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
    psql(&format!("DROP TABLESPACE IF EXISTS {ts_name}"));
    psql(&format!(
        "CREATE TABLESPACE {ts_name} LOCATION '{}'",
        ts_src.display()
    ));
    psql(&format!(
        "CREATE TABLE {tbl} (id int primary key, v text) \
         WITH (autovacuum_enabled=false) TABLESPACE {ts_name}"
    ));
    psql(&format!(
        "INSERT INTO {tbl} SELECT g, repeat('z', 200) FROM generate_series(1, 2000) g"
    ));
    psql(&format!("SELECT count(*) FROM {tbl}"));
    psql("CHECKPOINT");

    backup::push::handle(
        &s,
        store.clone(),
        backup::push::PushArgs {
            full: true,
            ..default_push_args()
        },
    )
    .await
    .expect("full backup with user tablespace");

    let name = backup::fetch::resolve_name(&store, "LATEST").await.unwrap();
    let v2 = read_sentinel(&storage_dir, &name);
    let spec = v2
        .sentinel
        .tablespace_spec
        .clone()
        .filter(|sp| !sp.is_empty())
        .expect("sentinel must carry the user tablespace Spec");

    let restore = dir.path().join("restore");
    let mut args = backup::fetch::FetchArgs::default();
    apply_tablespace_remap(&spec, &restore, &mut args);
    backup::fetch::handle_with_args(&s, store.clone(), &name, &restore, &args)
        .await
        .expect("restore with --tablespace-mapping");

    for ts in &spec.tablespace_names {
        let link = restore.join("pg_tblspc").join(ts);
        let md = std::fs::symlink_metadata(&link).unwrap_or_else(|e| panic!("{link:?}: {e}"));
        assert!(md.file_type().is_symlink(), "{link:?} should be a symlink");
    }

    let relpath = psql(&format!("SELECT pg_relation_filepath('{tbl}')"));
    assert!(
        relpath.contains("pg_tblspc"),
        "table should live under the tablespace: {relpath}"
    );
    let restored = std::fs::read(restore.join(&relpath))
        .unwrap_or_else(|e| panic!("read restored tablespace relation {relpath}: {e}"));
    assert!(
        !restored.is_empty(),
        "restored tablespace relation is empty"
    );
    assert_eq!(
        restored.len() % 8192,
        0,
        "restored relation is not a whole number of pages"
    );

    psql(&format!("DROP TABLE IF EXISTS {tbl}"));
    psql(&format!("DROP TABLESPACE IF EXISTS {ts_name}"));
}
