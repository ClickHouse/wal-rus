//! Real-PG client validation for the walsender server.
//!
//! Spawns the wal-rs walsender server on a TCP port, then drives it
//! with the local `psql` binary (libpq) using a replication-mode
//! connection. Asserts the libpq client successfully:
//! * negotiates the StartupMessage exchange (`replication=database`)
//! * issues `IDENTIFY_SYSTEM` and parses the row
//! * receives our cached identity unaltered
//!
//! Skipped silently when `psql` is absent. Gated cleanly when run in
//! environments without a libpq install.

use std::process::Stdio;
use std::time::Duration;

use pgwalrs::pg::replication::server::{Identity, handshake_and_await_start};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

async fn psql_available() -> bool {
    tokio::process::Command::new("psql")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Validate that libpq's StartupMessage + simple-query (`IDENTIFY_SYSTEM`)
/// flow against our walsender produces the cached identity bytes back
/// to psql. This is the protocol-level check the user asked for; pg18
/// being on the host is implicit (the test launches psql, not PG).
#[tokio::test]
async fn libpq_psql_handshakes_and_runs_identify_system() {
    if !psql_available().await {
        eprintln!("skip: psql not on PATH");
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let identity = Identity {
        system_id: "7340000000000000000".into(),
        timeline: 7,
        xlogpos: 0x0123_4567_89AB_CDEF,
        dbname: None,
    };

    let server_task = tokio::spawn({
        let identity = identity.clone();
        async move {
            let (sock, _) = listener.accept().await.expect("accept");
            let _ = sock.set_nodelay(true);
            let mut sock = sock;
            // Walreceiver-style libpq client may issue several
            // simple queries before START_REPLICATION; we expect
            // psql -c "IDENTIFY_SYSTEM" to send exactly the one
            // query then exit. The handshake completes on
            // START_REPLICATION; psql never sends that, so the
            // server's handshake_and_await_start will block on
            // socket close. Drive it under a timeout so we close
            // cleanly when psql disconnects.
            let _ = tokio::time::timeout(
                Duration::from_secs(8),
                handshake_and_await_start(&mut sock, &identity),
            )
            .await;
        }
    });

    // Connect with replication=database and ask for IDENTIFY_SYSTEM.
    // libpq's `replication=database` triggers the replication-mode
    // StartupMessage and lets simple queries work the same as on a
    // normal connection.
    let conninfo = format!(
        "host=127.0.0.1 port={port} user=wal-rs dbname=wal-rs replication=database sslmode=disable"
    );
    let psql = tokio::process::Command::new("psql")
        .arg(&conninfo)
        .arg("-tA")
        .arg("-c")
        .arg("IDENTIFY_SYSTEM")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .spawn();
    let mut child = match psql {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skip: psql failed to start: {e}");
            server_task.abort();
            return;
        }
    };
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let mut out_buf = String::new();
    let mut err_buf = String::new();
    let _ = stdout.read_to_string(&mut out_buf).await;
    let _ = stderr.read_to_string(&mut err_buf).await;
    let status = child.wait().await.expect("psql wait");
    if !status.success() {
        panic!("psql exited with {status}: stderr=\n{err_buf}\nstdout=\n{out_buf}");
    }
    server_task.abort();

    // psql -tA returns one line per row; columns are pipe-separated.
    // IDENTIFY_SYSTEM has 4 columns; dbname is empty in physical mode.
    let line = out_buf
        .lines()
        .find(|l| !l.is_empty())
        .unwrap_or_else(|| panic!("no rows from psql; stdout=\n{out_buf}"));
    let parts: Vec<&str> = line.split('|').collect();
    assert!(
        parts.len() >= 3,
        "expected ≥3 columns in IDENTIFY_SYSTEM, got {line:?}",
    );
    assert_eq!(parts[0], "7340000000000000000", "systemid round-trip");
    assert_eq!(parts[1], "7", "timeline round-trip");
    // pg_lsn text form: hex-pair separated by /.
    let xlogpos_expected = format!("{:X}/{:X}", identity.xlogpos >> 32, identity.xlogpos as u32,);
    assert_eq!(parts[2], xlogpos_expected, "xlogpos round-trip");
}
