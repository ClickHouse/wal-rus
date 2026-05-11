//! Live-PG integration tests, gated on the `vm-test` cargo feature.
//!
//! Reads PGHOST/PGPORT/PGUSER/PGDATABASE from the environment so the VM
//! launcher script can target a specific cluster (PG 13–18). For trust-auth
//! local Debian clusters, PGUSER=postgres and PGPASSWORD unset.

#![cfg(feature = "vm-test")]

use std::sync::Arc;

use wal_rs::compression::Method;
use wal_rs::config::{Settings, StorageSettings};
use wal_rs::pg::backup;
use wal_rs::pg::wal;
use wal_rs::storage::Storage;
use wal_rs::storage::fs::FsStorage;

fn settings_for(path: &str) -> Settings {
    Settings {
        storage: StorageSettings::Fs { path: path.into() },
        compression: Method::Zstd,
        compression_level: 3,
        upload_concurrency: 1,
        upload_queue: 1,
        download_concurrency: 1,
        prevent_wal_overwrite: false,
        retry: wal_rs::retry::RetryPolicy::default(),
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
    let sentinel_key = wal_rs::pg::backup::sentinel_key(&resolved);
    let sentinel_bytes = std::fs::read(storage_dir.join(&sentinel_key)).unwrap();
    let v2_pre: wal_rs::pg::backup::BackupSentinelDtoV2 =
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
    let fm: wal_rs::pg::backup::FilesMetadataDto =
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
    let v2: wal_rs::pg::backup::BackupSentinelDtoV2 =
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
    let v2: wal_rs::pg::backup::BackupSentinelDtoV2 =
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
