//! Daemon mode
//!
//! Wire-compatible with wal-g's `internal/daemon/` socket protocol so we can
//! serve as a drop-in archive_command target while sharing PG hosts
//!
//! Protocol per message: 1 byte type, 2 byte big-endian length (includes the
//! 3-byte header), then optional argument body. Arg body for >=2 args is:
//! 1 byte arg-count, then per-arg [u16 BE length, bytes]. With 1 arg the body
//! is the raw arg bytes.

pub mod client;
pub mod protocol;
mod systemd;
mod uploader;

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};

use crate::config::Settings;
use crate::pg::wal;
use crate::storage::DynStorage;

use protocol::{MessageType, parse_args, read_message, write_message};
use uploader::Uploader;

/// pg_wal lives directly under PGDATA; archive_command passes a bare segment
/// name while restore_command passes a `pg_wal/`-relative path
const PG_WAL: &str = "pg_wal";

/// `WALG_DAEMON_WAL_UPLOAD_TIMEOUT` default, matching wal-g
const DEFAULT_PUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-daemon shared state: the standing uploader, the resolved PGDATA used
/// to turn PG's `%f`/`%p` arguments into absolute paths, and the per-push
/// upload deadline
struct Daemon {
    uploader: Arc<Uploader>,
    pgdata: Option<PathBuf>,
    push_timeout: Duration,
}

pub async fn serve(socket: &Path, settings: Settings, storage: DynStorage) -> Result<()> {
    // Read config before binding so a bad value fails fast without leaving a
    // socket behind
    let push_timeout =
        crate::config::duration_env("WALG_DAEMON_WAL_UPLOAD_TIMEOUT", DEFAULT_PUSH_TIMEOUT)?;
    let pgdata = std::env::var_os("PGDATA").map(PathBuf::from);

    if socket.exists() {
        std::fs::remove_file(socket)
            .with_context(|| format!("remove stale socket {}", socket.display()))?;
    }
    let listener =
        UnixListener::bind(socket).with_context(|| format!("bind {}", socket.display()))?;
    tracing::info!("daemon listening on {}", socket.display());

    // One standing uploader shared across connections holds the look-ahead
    // pool + in-process bookkeeping (see uploader.rs)
    let daemon = Arc::new(Daemon {
        uploader: Arc::new(Uploader::new(Arc::new(settings), storage)),
        pgdata,
        push_timeout,
    });

    spawn_sigusr1_listener();
    systemd::notify("READY=1");
    systemd::spawn_watchdog();

    loop {
        let (stream, _) = listener.accept().await?;
        let d = daemon.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, d).await {
                tracing::error!("daemon conn: {e:#}");
            }
        });
    }
}

/// Reload logging config on SIGUSR1, matching wal-g's signal handler
fn spawn_sigusr1_listener() {
    tokio::spawn(async move {
        let Ok(mut sig) = signal(SignalKind::user_defined1()) else {
            tracing::warn!("SIGUSR1 listener unavailable");
            return;
        };
        while sig.recv().await.is_some() {
            crate::log::reconfigure();
            tracing::info!("reloaded logging config (SIGUSR1)");
        }
    });
}

/// Resolve a daemon WAL path the way wal-g's getFullPath does. PG's
/// archive_command passes `%f` (bare segment name) and restore_command `%p`
/// (`pg_wal/RECOVERYXLOG`, relative to the data dir); both resolve under
/// PGDATA. wal-rs's own client sends absolute paths, honored as-is. Without
/// PGDATA, relative args stay cwd-relative (pre-PGDATA behavior)
fn resolve_pgdata_path(arg: &str, pgdata: Option<&Path>, under_pg_wal: bool) -> PathBuf {
    let p = Path::new(arg);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    let Some(base) = pgdata else {
        return p.to_path_buf();
    };
    // wal-push receives a bare name (%f), so prepend pg_wal unless the caller
    // already did; wal-fetch's %p starts with pg_wal and joins directly
    if under_pg_wal && !p.starts_with(PG_WAL) {
        base.join(PG_WAL).join(p)
    } else {
        base.join(p)
    }
}

async fn handle_conn(mut stream: UnixStream, daemon: Arc<Daemon>) -> Result<()> {
    while let Ok((msg_type, body)) = read_message(&mut stream).await {
        let result = dispatch(msg_type, body, &daemon).await;
        let errored = result.is_err();
        let resp = result.unwrap_or_else(|e| {
            tracing::error!("op {msg_type:?} failed: {e:#}");
            MessageType::Error
        });
        write_message(&mut stream, resp, &[]).await?;
        // wal-g's ProcessConnection closes on any handler error and after the
        // terminal WalPush/WalFetch; only a successful Check loops for more
        if errored || matches!(msg_type, MessageType::WalPush | MessageType::WalFetch) {
            break;
        }
    }
    stream.shutdown().await.ok();
    Ok(())
}

async fn dispatch(
    msg_type: MessageType,
    body: Vec<u8>,
    daemon: &Arc<Daemon>,
) -> Result<MessageType> {
    match msg_type {
        MessageType::Check => Ok(MessageType::Ok),
        MessageType::WalPush => {
            let arg = single_arg(&body)?;
            let path = resolve_pgdata_path(&arg, daemon.pgdata.as_deref(), true);
            let push = daemon.uploader.wal_push(&path);
            if daemon.push_timeout.is_zero() {
                push.await?;
            } else {
                tokio::time::timeout(daemon.push_timeout, push)
                    .await
                    .map_err(|_| anyhow!("wal-push timed out after {:?}", daemon.push_timeout))??;
            }
            Ok(MessageType::Ok)
        }
        MessageType::WalFetch => {
            let args = parse_args(&body)?;
            if args.len() != 2 {
                anyhow::bail!("wal-fetch expects 2 args, got {}", args.len());
            }
            let dst = resolve_pgdata_path(&args[1], daemon.pgdata.as_deref(), false);
            match wal::fetch::handle(
                daemon.uploader.settings(),
                daemon.uploader.storage(),
                &args[0],
                &dst,
                wal::fetch::Prefetch::InProcess,
            )
            .await
            {
                Ok(()) => Ok(MessageType::Ok),
                Err(e) if e.downcast_ref::<wal::fetch::ArchiveNotFound>().is_some() => {
                    Ok(MessageType::ArchiveNonExistence)
                }
                Err(e) => Err(e),
            }
        }
        other => anyhow::bail!("unsupported message type {other:?}"),
    }
}

fn single_arg(body: &[u8]) -> Result<String> {
    // wal-g sends a single arg as the raw bytes (no length prefix), per getMessage()
    String::from_utf8(body.to_vec()).context("non-utf8 arg")
}

#[allow(dead_code)]
async fn _silence_unused() {
    // keep AsyncReadExt referenced to prevent over-eager dead-code linting
    let mut e = tokio::io::empty();
    let mut buf = [0u8; 0];
    let _ = e.read(&mut buf).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_bare_name_resolves_under_pg_wal() {
        let base = Path::new("/var/lib/pg/data");
        let p = resolve_pgdata_path("000000010000000000000001", Some(base), true);
        assert_eq!(p, base.join("pg_wal/000000010000000000000001"));
    }

    #[test]
    fn push_history_file_resolves_under_pg_wal() {
        let base = Path::new("/var/lib/pg/data");
        let p = resolve_pgdata_path("00000002.history", Some(base), true);
        assert_eq!(p, base.join("pg_wal/00000002.history"));
    }

    #[test]
    fn fetch_relative_dest_resolves_under_pgdata() {
        // restore_command passes %p, already pg_wal-prefixed
        let base = Path::new("/var/lib/pg/data");
        let p = resolve_pgdata_path("pg_wal/RECOVERYXLOG", Some(base), false);
        assert_eq!(p, base.join("pg_wal/RECOVERYXLOG"));
    }

    #[test]
    fn push_relative_already_under_pg_wal_not_doubled() {
        let base = Path::new("/var/lib/pg/data");
        let p = resolve_pgdata_path("pg_wal/000000010000000000000001", Some(base), true);
        assert_eq!(p, base.join("pg_wal/000000010000000000000001"));
    }

    #[test]
    fn absolute_arg_wins_over_pgdata() {
        // wal-rs's own client sends absolute paths
        let base = Path::new("/var/lib/pg/data");
        let abs = "/mnt/wal/000000010000000000000001";
        assert_eq!(resolve_pgdata_path(abs, Some(base), true), Path::new(abs));
        assert_eq!(resolve_pgdata_path(abs, Some(base), false), Path::new(abs));
    }

    #[test]
    fn no_pgdata_keeps_relative_arg() {
        let p = resolve_pgdata_path("000000010000000000000001", None, true);
        assert_eq!(p, Path::new("000000010000000000000001"));
    }
}
