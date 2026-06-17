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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::config::Settings;
use crate::pg::wal;
use crate::storage::DynStorage;

use protocol::{MessageType, parse_args, read_message, write_message};

pub async fn serve(socket: &Path, settings: Settings, storage: DynStorage) -> Result<()> {
    if socket.exists() {
        std::fs::remove_file(socket)
            .with_context(|| format!("remove stale socket {}", socket.display()))?;
    }
    let listener =
        UnixListener::bind(socket).with_context(|| format!("bind {}", socket.display()))?;
    tracing::info!("daemon listening on {}", socket.display());

    let settings = Arc::new(settings);

    loop {
        let (stream, _) = listener.accept().await?;
        let s = settings.clone();
        let st = storage.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, s, st).await {
                tracing::error!("daemon conn: {e:#}");
            }
        });
    }
}

async fn handle_conn(
    mut stream: UnixStream,
    settings: Arc<Settings>,
    storage: DynStorage,
) -> Result<()> {
    while let Ok((msg_type, body)) = read_message(&mut stream).await {
        let result = dispatch(msg_type, body, &settings, &storage).await;
        let resp = match &result {
            Ok(_) => MessageType::Ok,
            Err(e) => {
                tracing::error!("op {msg_type:?} failed: {e:#}");
                MessageType::Error
            }
        };
        write_message(&mut stream, resp, &[]).await?;
        if matches!(msg_type, MessageType::WalPush | MessageType::WalFetch) {
            break;
        }
    }
    stream.shutdown().await.ok();
    Ok(())
}

async fn dispatch(
    msg_type: MessageType,
    body: Vec<u8>,
    settings: &Settings,
    storage: &DynStorage,
) -> Result<()> {
    match msg_type {
        MessageType::Check => Ok(()),
        MessageType::WalPush => {
            let arg = single_arg(&body)?;
            let path = PathBuf::from(arg);
            wal::push::handle(settings, storage.clone(), &path).await
        }
        MessageType::WalFetch => {
            let args = parse_args(&body)?;
            if args.len() != 2 {
                anyhow::bail!("wal-fetch expects 2 args, got {}", args.len());
            }
            wal::fetch::handle(
                settings,
                storage.clone(),
                &args[0],
                Path::new(&args[1]),
                wal::fetch::Prefetch::InProcess,
            )
            .await
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
