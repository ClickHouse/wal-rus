//! The `sync-standby` service entry: independent startup for the SyncReplica
//! (durable synchronous-standby) receiver, decoupled from the Uploader's
//! `wal::receive::handle`. It resolves its own config, launches the controller
//! (poller / janitor / mTLS control API on its own tokio runtime + OS thread),
//! and runs the synchronous receive hot path (`super::receive::run`) on a
//! dedicated OS thread, bridged by `Shared`.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{Shared, SyncReplicaController, receive};
use crate::config::Settings;
use crate::pg::replication::conn::{PgConfig, ReplicationConn, query_wal_segment_size};
use crate::pg::wal::segment;
use crate::storage::DynStorage;

/// Entry point for `walrus sync-standby <dir>`. The CLI resolves `cfg` + the
/// slot name from `Vars` (env + config file) as it does for `wal-receive`; here
/// we query the server's `wal_segment_size` on a non-replication side connection,
/// then spawn the controller and the synchronous receive loop. The sync loop owns
/// no tokio runtime — `spawn_blocking` only parks this caller's future while the
/// dedicated thread runs, so the async worker isn't blocked.
pub(crate) async fn run(
    settings: &Settings,
    storage: DynStorage,
    archive_dir: &Path,
    cfg: PgConfig,
    slot_name: Option<String>,
) -> Result<()> {
    // wal_segment_size from a normal (non-replication) side connection —
    // replication mode forbids the query (wal-g's getCurrentWalInfo does the same).
    // Slot info is queried per-connect in the receiver, since a failover re-target
    // may find a different slot state on the new primary.
    let seg_size = {
        let mut q = ReplicationConn::connect_with(&cfg, false).await?;
        query_wal_segment_size(&mut q).await?
    };
    segment::set_wal_segment_size(seg_size);
    tracing::info!(
        target = "wal_receive",
        "sync-standby: wal_segment_size={seg_size} slot={}",
        slot_name.as_deref().unwrap_or("<none>"),
    );

    // Bridge to the controller. Default ⇒ sole_acker=false, ack_ceiling=MAX.
    let shared = Arc::new(Shared::default());

    // Sync-replica control plane on its OWN runtime/OS thread (tokio), isolated
    // from the hot path. It observes `shared.stop` to exit.
    let dr_storage = build_dr_storage(settings);
    let ctl = SyncReplicaController::new(
        shared.clone(),
        cfg.clone(),
        slot_name.clone(),
        archive_dir.to_path_buf(),
        seg_size,
        settings.clone(),
        dr_storage,
    );
    let controller = std::thread::Builder::new()
        .name("sync_replica_controller".into())
        .spawn(move || {
            if let Err(e) = ctl.run() {
                tracing::error!(target = "wal_receive", "sync-replica controller: {e:#}");
            }
        })
        .context("spawn sync-replica controller")?;

    // Run the synchronous receiver on a dedicated OS thread. `spawn_blocking`
    // moves it off the async worker; the closure is pure-sync (std::net +
    // std::fs), no tokio inside.
    let settings_c = settings.clone();
    let archive = archive_dir.to_path_buf();
    let shared_c = shared.clone();
    let recv = tokio::task::spawn_blocking(move || {
        receive::run(
            &settings_c,
            &storage,
            &archive,
            cfg,
            slot_name,
            seg_size,
            shared_c,
        )
    })
    .await
    .context("join sync receiver thread")?;

    // The sync loop fires `shared.stop` on shutdown; join the controller.
    shared.stop.notify_one();
    let _ = controller.join();
    recv
}

/// `WALG_WAL_RECEIVE_DR_S3`: deliver the retained tail to a DR-tail S3 lane on
/// `POST /v1/dr-catchup`. Off by default.
fn dr_s3_enabled_from_env() -> bool {
    crate::config::parse_env_bool("WALG_WAL_RECEIVE_DR_S3", false).unwrap_or(false)
}

/// Build the DR-tail S3 storage when enabled. A missing `WALG_S3_PREFIX` or an
/// init error logs a warning and yields `None` — dr-catchup then `500`s with an
/// explanatory error rather than the receiver failing to start.
fn build_dr_storage(settings: &Settings) -> Option<DynStorage> {
    if !dr_s3_enabled_from_env() {
        return None;
    }
    match settings.build_dr_s3_storage() {
        Ok(Some(s)) => Some(s),
        Ok(None) => {
            tracing::warn!(
                target = "wal_receive",
                "WALG_WAL_RECEIVE_DR_S3 set but WALG_S3_PREFIX missing; dr-catchup will 500"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                target = "wal_receive",
                "dr-tail S3 storage init failed: {e:#}; dr-catchup will 500"
            );
            None
        }
    }
}
