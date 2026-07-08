//! Sync-replica control plane.
//!
//! Runs on its OWN tokio runtime (a dedicated OS thread), isolated from the
//! receiver hot path, so control work (the primary-poller → sole-acker; later
//! the janitor and the mTLS control API) can't be starved by — and can't starve
//! — the recv loop. The two runtimes communicate only through [`Shared`], whose
//! fields are lock-free atomics (single-writer, so plain load/store, no locks).
//!
//! Design: `sync_pair/docs/sync-replica-controller.md`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::{DrTail, Shared};
use crate::config::Settings;
use crate::pg::backup::parse_pg_lsn;
use crate::pg::replication::conn::{PgConfig, ReplicationConn};
use crate::pg::wal::segment::SegmentName;
use crate::storage::DynStorage;

/// How often the primary-poller snapshots the primary (sole-acker latency target)
const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// A frozen peer while our frontier advances for at least this ⇒ the peer is dead
const PEER_STALENESS_MS: u64 = 1500;
/// Consecutive advancing polls needed to credit a returned peer back
const PEER_CREDIT_POLLS: u32 = 3;
/// Run the janitor every N poll ticks (≈30s at the 500ms poll interval)
const JANITOR_INTERVAL_TICKS: u64 = 60;
/// Per-tenant retention budget (back-pressure trigger) when
/// `WALG_WAL_RECEIVE_MAX_RETAINED_BYTES` is unset — wal-g's 10 GiB default.
const DEFAULT_RETAIN_BUDGET_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Resolve `(budget, release-low-water, hard-cap)` retained-segment counts from
/// `WALG_WAL_RECEIVE_MAX_RETAINED_BYTES` (default 10 GiB) and the segment size.
/// Back-pressure freezes the ACK above `budget`; the hard cap (1.4×, a disk
/// guard) drops the oldest; release hysteresis sits at 3/4 of the budget.
fn retain_budgets(seg_size: u64) -> (usize, usize, usize) {
    let bytes = std::env::var("WALG_WAL_RECEIVE_MAX_RETAINED_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|b| *b > 0)
        .unwrap_or(DEFAULT_RETAIN_BUDGET_BYTES);
    retain_budgets_for(bytes, seg_size)
}

/// Pure `(budget, release, hard_cap)` from a byte budget + segment size.
fn retain_budgets_for(bytes: u64, seg_size: u64) -> (usize, usize, usize) {
    let budget = (bytes / seg_size).max(1) as usize;
    let release = (budget * 3 / 4).max(1);
    let hard_cap = budget * 7 / 5;
    (budget, release, hard_cap)
}

/// The sync-replica control plane. Built by the receiver, run on a dedicated OS
/// thread that owns the controller's tokio runtime.
pub(crate) struct SyncReplicaController {
    shared: Arc<Shared>,
    cfg: PgConfig,
    #[allow(dead_code)] // used for slot-retention back-pressure in a later milestone
    slot_name: Option<String>,
    /// Where the receiver retains `<seg>` files (the janitor prunes here)
    archive_dir: PathBuf,
    seg_size: u64,
    /// Compression/crypto settings, shared with the dr-catchup uploader.
    settings: Settings,
    /// The DR-tail S3 lane (`WALG_S3_PREFIX`), `None` when DR-tail S3 is off.
    dr_storage: Option<DynStorage>,
    /// Retention thresholds in segments: (back-pressure budget, release
    /// low-water, hard cap), sized from `WALG_WAL_RECEIVE_MAX_RETAINED_BYTES`.
    retain: (usize, usize, usize),
}

impl SyncReplicaController {
    pub(crate) fn new(
        shared: Arc<Shared>,
        cfg: PgConfig,
        slot_name: Option<String>,
        archive_dir: PathBuf,
        seg_size: u64,
        settings: Settings,
        dr_storage: Option<DynStorage>,
    ) -> Self {
        Self {
            shared,
            cfg,
            slot_name,
            archive_dir,
            seg_size,
            settings,
            dr_storage,
            retain: retain_budgets(seg_size),
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
        let api = this.spawn_control_api().await;
        // bridge: the hot path fires `stop` on SIGINT/SIGTERM
        this.shared.stop.notified().await;
        poller.abort();
        if let Some(api) = api {
            api.abort();
        }
        Ok(())
    }

    /// Bind + serve the control API (the CP's promote gate). Bind failure is
    /// non-fatal — the receiver still acks WAL; the CP just degrades to
    /// promote-at-standby-LSN. `WALG_WAL_RECEIVE_CONTROL_LISTEN` overrides the
    /// default port (TODO: fold into ReceiveSettings).
    async fn spawn_control_api(&self) -> Option<tokio::task::JoinHandle<()>> {
        let listen = std::env::var("WALG_WAL_RECEIVE_CONTROL_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:8444".into());
        // Resolve mTLS before binding: if the env requests it but the certs are
        // bad/partial we refuse to serve at all rather than fall back to the
        // clear — the control API must never be reachable unauthenticated.
        let tls = match super::api::control_tls_from_env() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(target = "sync_replica", "control API TLS config: {e:#}");
                return None;
            }
        };
        let listener = match tokio::net::TcpListener::bind(&listen).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    target = "sync_replica",
                    "control API bind {listen} failed: {e:#}"
                );
                return None;
            }
        };
        let scheme = if tls.is_some() { "mTLS" } else { "plain HTTP" };
        tracing::info!(
            target = "sync_replica",
            "control API listening on {listen} ({scheme})"
        );
        let dr = self
            .dr_storage
            .clone()
            .map(|storage| Arc::new(DrTail::new(storage, self.settings.clone(), self.seg_size)));
        let state = Arc::new(super::api::ApiState {
            shared: self.shared.clone(),
            partial_dir: self.archive_dir.to_string_lossy().into_owned(),
            dr,
        });
        Some(tokio::spawn(async move {
            if let Err(e) = super::api::serve(listener, state, tls).await {
                tracing::warn!(target = "sync_replica", "control API exited: {e:#}");
            }
        }))
    }

    /// The single primary-poller loop: one side SQL connection, one snapshot per
    /// tick. Every tick drives `sole_acker` (fast); every `JANITOR_INTERVAL_TICKS`
    /// it also runs the janitor (prune retained segments + back-pressure).
    async fn primary_poller(self: Arc<Self>) {
        let mut liveness = PeerLiveness::new(PEER_STALENESS_MS, PEER_CREDIT_POLLS);
        let started = Instant::now();
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        let mut conn: Option<ReplicationConn> = None;
        let mut n: u64 = 0;
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
            let c = conn.as_mut().unwrap();
            let peer = match self.query_primary(c).await {
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
            let prev = self.shared.sole_acker.swap(sole, Ordering::Relaxed);
            if prev != sole {
                tracing::info!(
                    target = "sync_replica",
                    "sole_acker {prev} -> {sole} (peer_lsn={peer:?}, frontier={frontier:#x})"
                );
            }

            // janitor: slow cadence, same snapshot connection. A failure here is
            // non-fatal (sole-acker is the more critical signal)
            if n.is_multiple_of(JANITOR_INTERVAL_TICKS)
                && let Err(e) = self.run_janitor(conn.as_mut().unwrap()).await
            {
                tracing::warn!(target = "sync_replica", "janitor sweep failed: {e:#}");
            }
            n = n.wrapping_add(1);
        }
    }

    /// Read the primary's archived-WAL gate — a quick single-row query that
    /// shares the poller connection — then fire the prune + back-pressure update
    /// as a DETACHED task, so the fs sweep never delays the next peer-health poll.
    /// Only the ~ms gate query is on the poller's critical path; the scan/unlink
    /// runs concurrently on the controller runtime's second worker + blocking pool.
    async fn run_janitor(&self, conn: &mut ReplicationConn) -> Result<()> {
        let last_archived = self.last_archived_wal(conn).await?;
        let frontier = self.shared.fsyncd_lsn.load(Ordering::Acquire);
        let current_ceiling = self.shared.ack_ceiling.load(Ordering::Acquire);
        let dir = self.archive_dir.clone();
        let seg_size = self.seg_size;
        let retain = self.retain;
        let shared = self.shared.clone();
        tokio::spawn(async move {
            match tokio::task::spawn_blocking(move || {
                janitor_sweep(
                    &dir,
                    seg_size,
                    last_archived,
                    frontier,
                    current_ceiling,
                    retain,
                )
            })
            .await
            {
                Ok(Ok(ceiling)) => {
                    let prev = shared.ack_ceiling.swap(ceiling, Ordering::Release);
                    if prev == u64::MAX && ceiling != u64::MAX {
                        tracing::warn!(
                            target = "sync_replica",
                            "back-pressure ENGAGED: ACK pinned at {ceiling:#x} (retained over budget)"
                        );
                    } else if prev != u64::MAX && ceiling == u64::MAX {
                        tracing::info!(target = "sync_replica", "back-pressure released");
                    }
                }
                Ok(Err(e)) => tracing::warn!(target = "sync_replica", "janitor sweep: {e:#}"),
                Err(e) => tracing::warn!(target = "sync_replica", "janitor sweep join: {e:#}"),
            }
        });
        Ok(())
    }

    /// The primary's `pg_stat_archiver.last_archived_wal` — the segment up to
    /// which the primary has shipped WAL (our retained copies below it are
    /// redundant). `None` when the archiver hasn't archived anything.
    async fn last_archived_wal(&self, conn: &mut ReplicationConn) -> Result<Option<SegmentName>> {
        let rows = conn
            .query_rows("SELECT last_archived_wal FROM pg_stat_archiver")
            .await?;
        let cell = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|c| c.as_deref());
        Ok(cell
            .filter(|s| !s.is_empty())
            .and_then(|s| SegmentName::parse(s).ok()))
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

/// One janitor pass (runs on the blocking pool): prune retained `<seg>` files the
/// primary has already archived, drop the oldest beyond the hard cap, and return
/// the back-pressure ceiling (`frontier` to freeze the ACK, else `u64::MAX`).
fn janitor_sweep(
    dir: &Path,
    seg_size: u64,
    last_archived: Option<SegmentName>,
    frontier: u64,
    current_ceiling: u64,
    retain: (usize, usize, usize),
) -> Result<u64> {
    let (budget_segs, release_segs, hard_cap_segs) = retain;
    let retained = scan_retained(dir, seg_size)?;

    // 1. archiver-gated prune: the primary has shipped these, our copy is redundant
    let p1 = prunable(&retained, last_archived);
    if !p1.is_empty() {
        let n = remove_segments(dir, &p1);
        tracing::info!(
            target = "sync_replica",
            "janitor pruned {n} archived segment(s)"
        );
    }
    let after_p1 = &retained[p1.len()..]; // `prunable` returns a sorted prefix

    // 2. hard cap: last resort, sacrifice the oldest DR-tail to save the disk
    let p2 = hard_cap_drop(after_p1, hard_cap_segs);
    if !p2.is_empty() {
        let n = remove_segments(dir, &p2);
        tracing::warn!(
            target = "sync_replica",
            "janitor hard-cap dropped {n} retained segment(s) — DR-tail sacrificed"
        );
    }

    // 3. back-pressure from what remains
    let remaining = after_p1.len() - p2.len();
    Ok(backpressure_ceiling(
        remaining,
        frontier,
        current_ceiling,
        budget_segs,
        release_segs,
    ))
}

/// Decide the ACK ceiling. PIN at `frontier` when retained first exceeds
/// `budget_segs`; HOLD the existing pin while still above `release_segs` —
/// crucially NOT re-raising it to the advancing frontier each pass (that tracks
/// the frontier and defeats the freeze); RELEASE to `u64::MAX` once retained
/// drains below the low-water mark. Hysteresis (budget vs release) avoids
/// engage/release flapping at the boundary.
fn backpressure_ceiling(
    remaining: usize,
    frontier: u64,
    current_ceiling: u64,
    budget_segs: usize,
    release_segs: usize,
) -> u64 {
    let engaged = current_ceiling != u64::MAX;
    if engaged {
        if remaining <= release_segs {
            u64::MAX // drained enough — release
        } else {
            current_ceiling // hold the pin (do NOT track the frontier)
        }
    } else if remaining > budget_segs {
        frontier // engage — pin the ACK at the current frontier
    } else {
        u64::MAX // stay released
    }
}

/// Complete retained `<seg>` files (bare name, size == `seg_size`) in `dir`,
/// sorted ascending. Absent dir ⇒ empty.
fn scan_retained(dir: &Path, seg_size: u64) -> Result<Vec<SegmentName>> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).context("scan retained dir"),
    };
    let mut segs = Vec::new();
    for entry in rd {
        let entry = entry?;
        let Some(seg) = entry
            .file_name()
            .to_str()
            .and_then(|n| SegmentName::parse(n).ok())
        else {
            continue; // `.partial`, `.history`, etc. are rejected by parse
        };
        let meta = entry.metadata()?;
        if meta.is_file() && meta.len() == seg_size {
            segs.push(seg);
        }
    }
    segs.sort();
    Ok(segs)
}

/// Retained segments at or below the primary's last archived segment. `None`
/// gate ⇒ nothing prunable. `retained` must be sorted ascending.
fn prunable(retained: &[SegmentName], last_archived: Option<SegmentName>) -> Vec<SegmentName> {
    let Some(gate) = last_archived else {
        return Vec::new();
    };
    retained
        .iter()
        .copied()
        .take_while(|s| *s <= gate)
        .collect()
}

/// The oldest retained segments to drop when over `hard_cap`. `retained` must be
/// sorted ascending (oldest first).
fn hard_cap_drop(retained: &[SegmentName], hard_cap: usize) -> Vec<SegmentName> {
    if retained.len() <= hard_cap {
        return Vec::new();
    }
    retained[..retained.len() - hard_cap].to_vec()
}

/// Remove the named segment files; returns how many were unlinked.
fn remove_segments(dir: &Path, segs: &[SegmentName]) -> usize {
    let mut removed = 0;
    for s in segs {
        let path = dir.join(s.format());
        match std::fs::remove_file(&path) {
            Ok(()) => removed += 1,
            Err(e) => tracing::warn!(target = "sync_replica", "prune {}: {e}", s.format()),
        }
    }
    removed
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

    fn seg(n: u32) -> SegmentName {
        SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: n,
        }
    }

    #[test]
    fn prunable_takes_segments_at_or_below_the_gate() {
        let retained = vec![seg(1), seg(2), seg(3), seg(4)];
        assert_eq!(prunable(&retained, Some(seg(2))), vec![seg(1), seg(2)]);
        assert_eq!(prunable(&retained, Some(seg(9))), retained);
        assert!(prunable(&retained, Some(seg(0))).is_empty());
        assert!(prunable(&retained, None).is_empty()); // archiver hasn't shipped anything
    }

    #[test]
    fn hard_cap_drops_only_the_oldest_over_cap() {
        let retained = vec![seg(1), seg(2), seg(3), seg(4), seg(5)];
        assert_eq!(hard_cap_drop(&retained, 3), vec![seg(1), seg(2)]);
        assert!(hard_cap_drop(&retained, 5).is_empty());
        assert!(hard_cap_drop(&retained, 10).is_empty());
    }

    #[test]
    fn backpressure_ceiling_pins_holds_and_releases() {
        let max = u64::MAX;
        let (budget, release) = (128usize, 96usize); // explicit budget / low-water
        let over = budget + 1;
        let between = release + 1; // above release, below/at budget
        let under = release;

        // not engaged: under budget stays released; over budget pins the frontier
        assert_eq!(
            backpressure_ceiling(under, 0x5000, max, budget, release),
            max
        );
        assert_eq!(
            backpressure_ceiling(over, 0x5000, max, budget, release),
            0x5000
        );

        // engaged: HOLD the pin even as the frontier advances (the bug was
        // tracking it) — still above the release low-water mark
        assert_eq!(
            backpressure_ceiling(over, 0x9000, 0x5000, budget, release),
            0x5000
        );
        assert_eq!(
            backpressure_ceiling(between, 0x9000, 0x5000, budget, release),
            0x5000
        );

        // engaged: release once drained to/below the low-water mark
        assert_eq!(
            backpressure_ceiling(under, 0x9000, 0x5000, budget, release),
            max
        );
    }

    #[test]
    fn retain_budgets_scale_with_seg_size() {
        // 16 MiB segments, 10 GiB budget → 640 segs, 480 release, 896 cap
        assert_eq!(
            retain_budgets_for(10 * 1024 * 1024 * 1024, 16 * 1024 * 1024),
            (640, 480, 896)
        );
        // a small budget never floors below 1 segment
        assert_eq!(retain_budgets_for(1, 16 * 1024 * 1024), (1, 1, 1));
    }

    #[tokio::test]
    async fn scan_retained_finds_complete_segments_only() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        // complete retained segment
        std::fs::write(dir.path().join(seg(3).format()), vec![0u8; 16]).unwrap();
        // in-progress partial (rejected by parse) + wrong-size file (skipped)
        std::fs::write(
            dir.path().join(format!("{}.partial", seg(4).format())),
            vec![0u8; 16],
        )
        .unwrap();
        std::fs::write(dir.path().join(seg(5).format()), vec![0u8; 4]).unwrap();
        let found = scan_retained(dir.path(), seg_size).unwrap();
        assert_eq!(found, vec![seg(3)]);
    }

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
