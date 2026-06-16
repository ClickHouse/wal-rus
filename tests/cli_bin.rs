//! Exercises the `walross` binary end-to-end so `src/main.rs` (runtime
//! construction + ExitCode mapping) is covered. cargo-llvm-cov merges
//! coverage from spawned instrumented children via LLVM_PROFILE_FILE.

use std::process::Command;

fn walross() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_walross"));
    // Deterministic regardless of the surrounding CI env: strip every
    // storage selector so the test controls which backend (if any) resolves
    for k in ["WALG_FILE_PREFIX", "WALG_S3_PREFIX", "WALG_GS_PREFIX"] {
        cmd.env_remove(k);
    }
    cmd
}

#[test]
fn success_path_returns_exit_success() {
    let dir = tempfile::tempdir().unwrap();
    // Empty fs prefix: wal-show resolves storage, finds nothing, exits 0
    let status = walross()
        .env("WALG_FILE_PREFIX", dir.path())
        .arg("wal-show")
        .status()
        .unwrap();
    assert!(status.success(), "wal-show on empty storage should exit 0");
}

#[test]
fn error_path_returns_exit_failure() {
    // No storage configured -> build_storage bails -> ExitCode::FAILURE
    let status = walross()
        .args(["wal-fetch", "000000010000000000000001", "/dev/null"])
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "wal-fetch without storage configured should fail"
    );
}
