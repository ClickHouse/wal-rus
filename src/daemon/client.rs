//! Connect to a wal-rs/wal-g daemon and send a single op

use std::path::Path;

use anyhow::{Result, bail};
use tokio::net::UnixStream;

use crate::cli::DaemonOp;

use super::protocol::{MessageType, read_message, write_message};

pub async fn run(socket: &Path, op: DaemonOp) -> Result<()> {
    let mut stream = UnixStream::connect(socket).await?;
    match op {
        DaemonOp::Check => {
            write_message(&mut stream, MessageType::Check, &[]).await?;
        }
        DaemonOp::WalPush { wal_filepath } => {
            let p = wal_filepath
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("wal path is not utf8"))?;
            write_message(&mut stream, MessageType::WalPush, &[p]).await?;
        }
        DaemonOp::WalFetch { name, dst } => {
            let d = dst
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("dst path is not utf8"))?;
            write_message(&mut stream, MessageType::WalFetch, &[name.as_str(), d]).await?;
        }
    }
    let (resp, _) = read_message(&mut stream).await?;
    match resp {
        MessageType::Ok => Ok(()),
        MessageType::ArchiveNonExistence => bail!("archive not found"),
        other => bail!("daemon returned {other:?}"),
    }
}
