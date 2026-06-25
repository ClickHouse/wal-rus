//! Sync-replica control plane.
//!
//! Runs on its OWN tokio runtime (a dedicated OS thread), isolated from the
//! receiver hot path, so control work (the primary-poller → sole-acker; later
//! the janitor and the mTLS control API) can't be starved by — and can't starve
//! — the recv loop. The two runtimes communicate only through [`Shared`], whose
//! fields are lock-free atomics (single-writer, so plain load/store, no locks).
//!
//! Design: `sync_pair/docs/sync-replica-controller.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::Notify;

use crate::pg::backup::parse_pg_lsn;
use crate::pg::replication::conn::{PgConfig, ReplicationConn};

/// How often the primary-poller snapshots the primary (sole-acker latency target)
const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// A frozen peer while our frontier advances for at least this ⇒ the peer is dead
const PEER_STALENESS_MS: u64 = 1500;
/// Consecutive advancing polls needed to credit a returned peer back
const PEER_CREDIT_POLLS: u32 = 3;

/// State shared across the runtime boundary between the receiver hot path and
/// the sync-replica controller. Every field is single-writer.
pub(crate) struct Shared {
    /// Durable fsync frontier. WRITER: hot path (`acc.sync` writes it at fsync
    /// time). READERS: controller (poller) + hot path `send_status`
    pub fsyncd_lsn: AtomicU64,
    /// Receiver is the only live sync acker. WRITER: primary-poller (controller).
    /// READER: hot path drain gate (sole acker ⇒ per-frame fsync)
    pub sole_acker: AtomicBool,
    /// Max flush the hot path may advertise (back-pressure). `u64::MAX` = no cap.
    /// WRITER: janitor (controller). READER: hot path `send_status`
    pub ack_ceiling: AtomicU64,
    /// Shutdown. WRITER: hot path on SIGINT/SIGTERM. READER: controller `serve`
    pub stop: Notify,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            fsyncd_lsn: AtomicU64::new(0),
            sole_acker: AtomicBool::new(false),
            ack_ceiling: AtomicU64::new(u64::MAX),
            stop: Notify::new(),
        }
    }
}

/// The sync-replica control plane. Built by the receiver, run on a dedicated OS
/// thread that owns the controller's tokio runtime.
pub(crate) struct SyncReplicaController {
    shared: Arc<Shared>,
    cfg: PgConfig,
    #[allow(dead_code)] // used by the janitor (slot retention) in a later milestone
    slot_name: Option<String>,
}

impl SyncReplicaController {
    pub(crate) fn new(shared: Arc<Shared>, cfg: PgConfig, slot_name: Option<String>) -> Self {
        Self {
            shared,
            cfg,
            slot_name,
        }
    }

    /// Entry point — runs ON the dedicated OS thread; builds and drives the
    /// controller's own multi-thread runtime so the API + poller don't head-of-
    /// line each other and nothing here can starve the receiver runtime.
    pub(crate) fn run(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("sync-replica-ctl")
            .build()
            .context("build sync-replica controller runtime")?;
        rt.block_on(self.serve())
    }

    async fn serve(self) -> Result<()> {
        let this = Arc::new(self);
        let poller = tokio::spawn(this.clone().primary_poller());
        // bridge: the hot path fires `stop` on SIGINT/SIGTERM
        this.shared.stop.notified().await;
        poller.abort();
        Ok(())
    }

    /// The single primary-poller loop: one side SQL connection, one snapshot per
    /// tick, drives `sole_acker`. (The janitor's prune-gate read + back-pressure
    /// fold into this same loop in a later milestone.)
    async fn primary_poller(self: Arc<Self>) {
        let mut liveness = PeerLiveness::new(PEER_STALENESS_MS, PEER_CREDIT_POLLS);
        let started = Instant::now();
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        let mut conn: Option<ReplicationConn> = None;
        loop {
            tick.tick().await;
            let now_ms = started.elapsed().as_millis() as u64;

            if conn.is_none() {
                match ReplicationConn::connect_with(&self.cfg, false).await {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        tracing::warn!(target = "sync_replica", "poller connect failed: {e:#}");
                        self.fail_safe();
                        continue;
                    }
                }
            }
            let peer = match self.query_primary(conn.as_mut().unwrap()).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(target = "sync_replica", "poller query failed: {e:#}");
                    conn = None; // drop the (possibly broken) connection, reconnect next tick
                    self.fail_safe();
                    continue;
                }
            };
            let frontier = self.shared.fsyncd_lsn.load(Ordering::Acquire);
            let sole = liveness.observe(peer, frontier, now_ms);
            self.shared.sole_acker.store(sole, Ordering::Relaxed);
        }
    }

    /// On any poll error, assume sole acker — the safe side (per-frame fsync)
    fn fail_safe(&self) {
        self.shared.sole_acker.store(true, Ordering::Relaxed);
    }

    /// Max acked `flush_lsn` across active peer sync standbys (≠ self), or `None`
    /// when no such peer is streaming.
    async fn query_primary(&self, conn: &mut ReplicationConn) -> Result<Option<u64>> {
        let self_app = self.cfg.application_name.replace('\'', "''");
        let sql = format!(
            "SELECT max(flush_lsn)::text FROM pg_stat_replication \
             WHERE application_name <> '{self_app}' \
               AND state = 'streaming' \
               AND sync_state IN ('sync', 'quorum', 'potential')"
        );
        let rows = conn.query_rows(&sql).await?;
        let cell = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|c| c.as_deref());
        match cell {
            None | Some("") => Ok(None),
            Some(lsn) => Ok(Some(parse_pg_lsn(lsn)?)),
        }
    }
}

/// Tracks whether a peer sync standby is making progress, deciding whether this
/// receiver is the sole acker (⇒ per-frame fsync). Eager-demote to sole on an
/// absent or stale-frozen peer; lazy-credit back after sustained peer progress.
/// Mirrors the fork's `runSoleAckerPoller`. Pure — no I/O.
struct PeerLiveness {
    staleness_ms: u64,
    credit_polls: u32,
    sole: bool,
    last_peer_lsn: Option<u64>,
    /// when the peer's acked LSN last advanced
    peer_moved_at_ms: u64,
    /// our durable frontier at that advance
    frontier_at_peer_move: u64,
    /// consecutive advancing polls since going sole
    good_polls: u32,
}

impl PeerLiveness {
    fn new(staleness_ms: u64, credit_polls: u32) -> Self {
        Self {
            staleness_ms,
            credit_polls,
            sole: false,
            last_peer_lsn: None,
            peer_moved_at_ms: 0,
            frontier_at_peer_move: 0,
            good_polls: 0,
        }
    }

    /// `peer_lsn`: max acked LSN across active peer standbys (`None` if none
    /// stream). `frontier`: our durable fsync frontier. Returns the sole verdict.
    fn observe(&mut self, peer_lsn: Option<u64>, frontier: u64, now_ms: u64) -> bool {
        let Some(lsn) = peer_lsn else {
            // no peer streaming at all ⇒ we're the only acker
            self.sole = true;
            self.good_polls = 0;
            self.last_peer_lsn = None;
            return true;
        };
        let advanced = self.last_peer_lsn.is_none_or(|p| lsn > p);
        if advanced {
            self.last_peer_lsn = Some(lsn);
            self.peer_moved_at_ms = now_ms;
            self.frontier_at_peer_move = frontier;
            if self.sole {
                self.good_polls += 1;
                if self.good_polls >= self.credit_polls {
                    self.sole = false;
                    self.good_polls = 0;
                }
            }
        } else {
            // peer frozen; dead only once OUR frontier has moved past where it was
            // at the peer's last advance AND it's been frozen long enough
            self.good_polls = 0;
            let we_advanced = frontier > self.frontier_at_peer_move;
            let stale = now_ms.saturating_sub(self.peer_moved_at_ms) >= self.staleness_ms;
            if we_advanced && stale {
                self.sole = true;
            }
        }
        self.sole
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn present_peer_advancing_is_not_sole() {
        let mut pl = PeerLiveness::new(1500, 3);
        assert!(!pl.observe(Some(100), 100, 0));
        assert!(!pl.observe(Some(200), 200, 500));
    }

    #[test]
    fn absent_peer_is_eagerly_sole() {
        let mut pl = PeerLiveness::new(1500, 3);
        assert!(pl.observe(None, 100, 0));
    }

    #[test]
    fn frozen_peer_goes_sole_only_once_stale_and_we_advanced() {
        let mut pl = PeerLiveness::new(1500, 3);
        assert!(!pl.observe(Some(100), 100, 0)); // peer at 100
        // frozen + we advanced, but not yet stale
        assert!(!pl.observe(Some(100), 200, 1000));
        // still frozen, now stale (>=1500ms) and we advanced ⇒ sole
        assert!(pl.observe(Some(100), 300, 1600));
    }

    #[test]
    fn frozen_peer_without_our_progress_is_not_sole() {
        let mut pl = PeerLiveness::new(1500, 3);
        assert!(!pl.observe(Some(100), 100, 0));
        // peer frozen and OUR frontier flat ⇒ no evidence the peer is dead
        assert!(!pl.observe(Some(100), 100, 5000));
    }

    #[test]
    fn returned_peer_is_lazily_credited_back() {
        let mut pl = PeerLiveness::new(1500, 3);
        assert!(pl.observe(None, 100, 0)); // sole
        assert!(pl.observe(Some(100), 100, 500)); // good 1, still sole
        assert!(pl.observe(Some(200), 200, 1000)); // good 2, still sole
        assert!(!pl.observe(Some(300), 300, 1500)); // good 3 ⇒ credited
    }
}
