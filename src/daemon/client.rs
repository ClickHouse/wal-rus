//! Connect to a walross/wal-g daemon and send a single op
//!
//! Timeouts mirror wal-g's `walg-daemon-client`: a dial deadline
//! (`-connection-timeout`) around the socket connect and an operation
//! deadline (`-timeout`) around the request/response exchange. Either is
//! disabled by passing a zero duration

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use tokio::net::UnixStream;

use crate::cli::DaemonOp;

use super::protocol::{MessageType, read_message, write_message};

pub async fn run(
    socket: &Path,
    op: DaemonOp,
    op_timeout: Duration,
    conn_timeout: Duration,
) -> Result<()> {
    let mut stream = with_timeout(conn_timeout, "daemon socket dial", async {
        Ok(UnixStream::connect(socket).await?)
    })
    .await?;

    let exchange = async {
        match op {
            DaemonOp::Check => {
                write_message(&mut stream, MessageType::Check, &[]).await?;
            }
            DaemonOp::WalPush { wal_filepath } => {
                let p = wal_filepath
                    .to_str()
                    .ok_or_else(|| anyhow!("wal path is not utf8"))?;
                write_message(&mut stream, MessageType::WalPush, &[p]).await?;
            }
            DaemonOp::WalFetch { name, dst } => {
                let d = dst
                    .to_str()
                    .ok_or_else(|| anyhow!("dst path is not utf8"))?;
                write_message(&mut stream, MessageType::WalFetch, &[name.as_str(), d]).await?;
            }
        }
        let (resp, _) = read_message(&mut stream).await?;
        anyhow::Ok(resp)
    };

    let resp = with_timeout(op_timeout, "daemon operation", exchange).await?;
    match resp {
        MessageType::Ok => Ok(()),
        MessageType::ArchiveNonExistence => bail!("archive not found"),
        other => bail!("daemon returned {other:?}"),
    }
}

/// Await `fut`, bounded by `dur` unless it is zero (disabled)
async fn with_timeout<T>(
    dur: Duration,
    what: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    if dur.is_zero() {
        return fut.await;
    }
    match tokio::time::timeout(dur, fut).await {
        Ok(r) => r,
        Err(_) => bail!("{what} timed out after {dur:?}"),
    }
}
