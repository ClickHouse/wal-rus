//! Exercises the `wal-rs` binary end-to-end so `src/main.rs` (runtime
//! construction + ExitCode mapping) and `cli::run`'s `Cmd` dispatch arms are
//! covered. cargo-llvm-cov merges coverage from spawned instrumented children
//! via LLVM_PROFILE_FILE.

use std::path::Path;
use std::process::Command;

use pgwalrs::pg::WAL_FOLDER;
use pgwalrs::pg::backup::{BackupSentinelDto, BackupSentinelDtoV2, sentinel_key};

fn wal_rs() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wal-rs"));
    // Deterministic regardless of the surrounding CI env: strip every
    // storage selector so the test controls which backend (if any) resolves
    for k in ["WALG_FILE_PREFIX", "WALG_S3_PREFIX", "WALG_GS_PREFIX"] {
        cmd.env_remove(k);
    }
    cmd
}

const BACKUP: &str = "base_000000010000000000000002";

/// Seed a `file://` store with one sentinel (tli 1, seg 2) plus two
/// contiguous WAL segments so the inspect/retention subcommands have data
fn seed_store(dir: &Path) {
    let sentinel = BackupSentinelDtoV2 {
        sentinel: BackupSentinelDto {
            backup_start_lsn: Some(0x0200_0000),
            backup_finish_lsn: Some(0x0200_1000),
            pg_version: 160003,
            uncompressed_size: 2048,
            compressed_size: 1024,
            ..Default::default()
        },
        hostname: "h".into(),
        data_dir: "/d".into(),
        ..Default::default()
    };
    let path = dir.join(sentinel_key(BACKUP));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&sentinel).unwrap()).unwrap();

    let wal = dir.join(WAL_FOLDER);
    std::fs::create_dir_all(&wal).unwrap();
    for seg in ["000000010000000000000002", "000000010000000000000003"] {
        std::fs::write(wal.join(seg), b"").unwrap();
    }
}

#[test]
fn success_path_returns_exit_success() {
    let dir = tempfile::tempdir().unwrap();
    // Empty fs prefix: wal-show resolves storage, finds nothing, exits 0
    let status = wal_rs()
        .env("WALG_FILE_PREFIX", dir.path())
        .arg("wal-show")
        .status()
        .unwrap();
    assert!(status.success(), "wal-show on empty storage should exit 0");
}

#[test]
fn error_path_returns_exit_failure() {
    // No storage configured -> build_storage bails -> ExitCode::FAILURE
    let status = wal_rs()
        .args(["wal-fetch", "000000010000000000000001", "/dev/null"])
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "wal-fetch without storage configured should fail"
    );
}

#[test]
fn dispatch_inspect_and_retention_subcommands() {
    let dir = tempfile::tempdir().unwrap();
    seed_store(dir.path());
    let run = |args: &[&str]| {
        wal_rs()
            .env("WALG_FILE_PREFIX", dir.path())
            .args(args)
            .status()
            .unwrap()
    };

    // Each drives a distinct `Cmd` match arm against the seeded file:// store;
    // delete modes default to dry-run (no --confirm) so they only plan
    for args in [
        vec!["wal-show", "--json"],
        vec!["wal-verify", "all"],
        vec!["wal-verify", "integrity", "--json"],
        vec!["wal-verify", "timeline"],
        vec!["backup-list"],
        vec!["backup-list", "--json"],
        vec!["backup-show", BACKUP],
        vec!["backup-show", "LATEST", "--json"],
        vec!["delete", "before", BACKUP],
        vec!["delete", "retain", "1"],
        vec!["delete", "everything"],
        vec!["delete", "garbage"],
        vec!["delete", "target", BACKUP],
    ] {
        assert!(run(&args).success(), "expected exit 0 for {args:?}");
    }
}

#[test]
fn dispatch_backup_mark_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    seed_store(dir.path());
    let run = |args: &[&str]| {
        wal_rs()
            .env("WALG_FILE_PREFIX", dir.path())
            .args(args)
            .status()
            .unwrap()
    };
    // default marks permanent, --impermanent flips it back; both exercise the
    // BackupMark arm + sentinel rewrite
    assert!(run(&["backup-mark", BACKUP]).success());
    assert!(run(&["backup-mark", BACKUP, "--impermanent"]).success());
}

#[test]
fn dispatch_bail_arms_return_failure() {
    let dir = tempfile::tempdir().unwrap();
    seed_store(dir.path());
    let fail = |args: &[&str]| {
        !wal_rs()
            .env("WALG_FILE_PREFIX", dir.path())
            .args(args)
            .status()
            .unwrap()
            .success()
    };

    let dst = dir.path().join("dst");
    let dst = dst.to_str().unwrap();
    // backup-fetch with neither name nor --target-user-data
    assert!(fail(&["backup-fetch", dst]));
    // backup-mark with neither name nor --target-user-data
    assert!(fail(&["backup-mark"]));
    // delete target with neither name nor --target-user-data
    assert!(fail(&["delete", "target"]));
    // `delete before FULL` is explicitly unsupported
    assert!(fail(&["delete", "before", "FULL", "x"]));
    // --user-data invalid JSON bails before any PG connection
    assert!(fail(&["backup-push", "--user-data", "{not json"]));
}
