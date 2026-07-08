//! Synchronous `SyncReplica` WAL receive hot path.
//!
//! The receiver streams Postgres WAL as a synchronous standby: recv a WAL frame
//! → write it to the local segment file → `fdatasync` → ack the flushed LSN. As
//! the sole quorum acker, every commit blocks on that ack, so the round-trip
//! latency is the throughput ceiling.
//!
//! The async path (`crate::pg::wal::receive::run_replication`) runs this loop on a tokio
//! `current_thread` runtime where every file op goes through `tokio::fs` →
//! `spawn_blocking` — two cross-thread blocking-pool round-trips per commit,
//! serialized, the lone worker parking each time. This module runs the same
//! loop fully synchronously on a dedicated OS thread: blocking socket I/O +
//! `std::fs` syscalls, no tokio, no `spawn_blocking`.
//!
//! In `SyncReplica` mode there is no object-storage work on the hot path
//! (dr-tail upload, the janitor, and the mTLS control API all run on the
//! separate `SyncReplicaController` tokio runtime, bridged only by the `Shared`
//! atomics), so the receive loop has no remaining need for async.
//!
//! Durability contract (absolute): `shared.fsyncd_lsn` is advanced — and a
//! flush ack sent — only AFTER `sync_data()` has returned for the bytes that
//! LSN covers. See [`SyncSegmentWriter::sync`] and [`run_sync_replica`].

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;

use super::Shared;
use crate::config::Settings;
use crate::pg::backup::format_pg_lsn;
use crate::pg::replication::conn::{PgConfig, error_message, message_kind};
use crate::pg::replication::stream::{Frame, build_status_update, decode_frame};
use crate::pg::replication::sync_conn::{RecvOutcome, SyncReplicationConn};
use crate::pg::wal::segment::SegmentName;
use crate::storage::DynStorage;

/// Status-update cadence for a quiet stream (wal-g pings every 10s) — also the
/// blocking-read timeout, so the loop wakes at least this often to keepalive and
/// poll shutdown / retarget.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(10);
/// Backoff between reconnect attempts while a primary is unreachable.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);
/// Batch-accumulation window for 2-acker mode: when the standby is the pacing
/// acker (`sole_acker == false`) our flush ack does NOT gate commits, so we
/// accumulate WAL frames for up to this long before a single `fdatasync` + ack,
/// coalescing many frames per fsync (lower IOPS). Skipped entirely in sole-acker
/// mode, where every commit blocks on our ack and per-frame fsync is fastest.
const BATCH_WINDOW: Duration = Duration::from_millis(1);

/// Round an LSN down to its segment boundary.
fn align(lsn: u64, seg_size: u64) -> u64 {
    lsn - (lsn % seg_size)
}

/// Hot-path batch instrumentation: the recent (windowed) frames coalesced per
/// fsync, logged every `BATCH_STATS_EVERY` fsyncs so ops can confirm the 2-acker
/// window is coalescing (≈1.0 in sole-acker, several in 2-acker).
static BATCH_FSYNCS: AtomicU64 = AtomicU64::new(0);
static BATCH_FRAMES: AtomicU64 = AtomicU64::new(0);
static BATCH_LOG_FSYNCS: AtomicU64 = AtomicU64::new(0);
static BATCH_LOG_FRAMES: AtomicU64 = AtomicU64::new(0);
const BATCH_STATS_EVERY: u64 = 32768;

/// Synchronous segment writer — the `SyncReplica` analogue of
/// `crate::pg::wal::receive::SegmentAccumulator`, minus the upload machinery (this mode
/// always retains `<seg>` on disk). Uses `std::fs::File` with positioned
/// `write_all_at` (never seek+append) and `sync_data()` (fdatasync).
struct SyncSegmentWriter {
    seg_size: u64,
    timeline: u32,
    archive_dir: PathBuf,
    current: Option<CurrentSegment>,
    /// Highest WAL LSN received (write position). Reset to the restart LSN on a
    /// timeline switch / reconnect.
    received_lsn: u64,
}

/// In-progress segment, written to `<seg>.partial`; renamed to bare `<seg>`
/// only once full, so only complete segments ever carry the bare name.
struct CurrentSegment {
    name: SegmentName,
    file: std::fs::File,
    /// `<archive_dir>/<seg>.partial`
    path: PathBuf,
    bytes_written: u64,
}

impl SyncSegmentWriter {
    fn new(timeline: u32, archive_dir: PathBuf, seg_size: u64, start_lsn: u64) -> Result<Self> {
        std::fs::create_dir_all(&archive_dir)
            .with_context(|| format!("create_dir_all {}", archive_dir.display()))?;
        Ok(Self {
            seg_size,
            timeline,
            archive_dir,
            current: None,
            received_lsn: start_lsn,
        })
    }

    fn segment_for_lsn(&self, lsn: u64) -> SegmentName {
        let seg_no = lsn / self.seg_size;
        let xlog_segs_per_xlog_id = 0x1_0000_0000u64 / self.seg_size;
        SegmentName {
            timeline: self.timeline,
            log_id: (seg_no / xlog_segs_per_xlog_id) as u32,
            seg_no: (seg_no % xlog_segs_per_xlog_id) as u32,
        }
    }

    /// Write `data` starting at `start_lsn`, splitting across segment boundaries
    /// and rotating (fsync + rename, retain) each completed segment. Writes are
    /// positioned by absolute offset — never seek+append — so a reconnect at a
    /// non-zero offset leaves the zero pad intact.
    fn write(&mut self, start_lsn: u64, data: &[u8]) -> Result<()> {
        let mut lsn = start_lsn;
        let mut data = data;
        while !data.is_empty() {
            self.ensure_current(lsn)?;
            let cur = self.current.as_mut().expect("ensure_current set it");
            let cur_seg_start = cur.name.start_lsn(self.seg_size);
            let cur_seg_end = cur_seg_start + self.seg_size;
            let offset_in_seg = lsn - cur_seg_start;
            let space_left = cur_seg_end - lsn;
            let chunk_len = std::cmp::min(space_left as usize, data.len());
            cur.file
                .write_all_at(&data[..chunk_len], offset_in_seg)
                .context("write WAL chunk")?;
            cur.bytes_written = (offset_in_seg + chunk_len as u64).max(cur.bytes_written);
            lsn += chunk_len as u64;
            data = &data[chunk_len..];
            if offset_in_seg + chunk_len as u64 == self.seg_size {
                self.rotate()?;
            }
        }
        self.received_lsn = self.received_lsn.max(lsn);
        Ok(())
    }

    fn ensure_current(&mut self, lsn: u64) -> Result<()> {
        if let Some(cur) = self.current.as_ref() {
            let seg_start = cur.name.start_lsn(self.seg_size);
            if lsn >= seg_start && lsn < seg_start + self.seg_size {
                return Ok(());
            }
            tracing::warn!(
                target = "wal_receive",
                "rotating partial segment {} ({} bytes written, target {})",
                cur.name.format(),
                cur.bytes_written,
                self.seg_size
            );
            self.rotate()?;
        }
        let seg = self.segment_for_lsn(lsn);
        let path = self.archive_dir.join(format!("{}.partial", seg.format()));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        // Pre-extend to seg_size so partial-tail writes leave a zero pad.
        file.set_len(self.seg_size)?;
        self.current = Some(CurrentSegment {
            name: seg,
            file,
            path,
            bytes_written: 0,
        });
        Ok(())
    }

    /// fsync the full segment and publish `<seg>.partial` -> `<seg>` (retain).
    fn rotate(&mut self) -> Result<()> {
        let Some(cur) = self.current.take() else {
            return Ok(());
        };
        let CurrentSegment {
            name, file, path, ..
        } = cur;
        file.sync_all()?;
        drop(file);
        let dst = self.archive_dir.join(name.format());
        std::fs::rename(&path, &dst)
            .with_context(|| format!("rename {} -> {}", path.display(), dst.display()))?;
        tracing::info!(
            target = "wal_receive",
            "segment {} complete, retained",
            name.format()
        );
        Ok(())
    }

    /// fdatasync the open partial and raise the durable `frontier` to the synced
    /// write cursor. Single writer (the hot path), so load Relaxed + store
    /// Release; the max guards a reconnect re-streaming a partial from a lower
    /// offset. Returns the new durable LSN.
    fn sync(&mut self, frontier: &AtomicU64) -> Result<u64> {
        let Some(cur) = self.current.as_ref() else {
            return Ok(frontier.load(Ordering::Relaxed));
        };
        cur.file.sync_data()?;
        let durable = cur.name.start_lsn(self.seg_size) + cur.bytes_written;
        let raised = frontier.load(Ordering::Relaxed).max(durable);
        frontier.store(raised, Ordering::Release);
        Ok(raised)
    }

    /// Finish the in-progress segment on a timeline switch / reconnect, fsync'd
    /// and retained on disk. Returns its start LSN (the resume point).
    ///
    /// Raises `frontier` to the now-durable end of the partial. This is critical
    /// for the 2-acker batch window: that window DEFERS the per-frame fsync, so a
    /// stream break (primary loss) can leave the last batch written-but-not-yet-
    /// fsync'd. `sync_all` here makes it durable, and advancing the frontier makes
    /// the receiver REPORT it — so the CP's failover dr-catchup gates on it and
    /// the standby-acked tail isn't stranded (total-loss RPO=0). Without this the
    /// frontier would stall at the last in-window `sync()`, dropping those commits.
    fn finalize_partial(&mut self, frontier: &AtomicU64) -> Result<Option<u64>> {
        let Some(cur) = self.current.take() else {
            return Ok(None);
        };
        let CurrentSegment {
            name,
            file,
            path,
            bytes_written,
        } = cur;
        file.sync_all()?;
        drop(file);
        let start = name.start_lsn(self.seg_size);
        if bytes_written == 0 {
            let _ = std::fs::remove_file(&path);
            return Ok(Some(start));
        }
        let durable = start + bytes_written;
        let raised = frontier.load(Ordering::Relaxed).max(durable);
        frontier.store(raised, Ordering::Release);
        tracing::info!(
            target = "wal_receive",
            "retaining partial segment {} ({} bytes of {}), durable frontier {}",
            name.format(),
            bytes_written,
            self.seg_size,
            format_pg_lsn(raised),
        );
        Ok(Some(start))
    }

    /// Re-aim at a new timeline; `finalize_partial` cleared `current`.
    fn reset_for_timeline(&mut self, timeline: u32, restart_lsn: u64) {
        debug_assert!(self.current.is_none());
        self.timeline = timeline;
        self.received_lsn = restart_lsn;
    }

    fn write_position(&self) -> u64 {
        self.received_lsn
    }
}

/// A process-wide shutdown flag set by the SIGINT/SIGTERM handler. The sync loop
/// polls it on each recv timeout / status tick. A plain atomic (not tokio's
/// `Notify`) so the loop stays tokio-free; the handler does nothing but a single
/// atomic store, which is async-signal-safe.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_term_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Install the SIGINT/SIGTERM handler once. Idempotent across calls (later
/// installs just re-point to the same handler). Returns an error only if
/// `sigaction` fails.
fn install_shutdown_handler() -> Result<()> {
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = handle_term_signal as *const () as libc::sighandler_t;
        libc::sigemptyset(&mut action.sa_mask);
        for sig in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(sig, &action, std::ptr::null_mut()) != 0 {
                bail!("sigaction({sig}): {}", std::io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

/// Entry point for `SyncReplica` mode: run the synchronous receive loop on the
/// CALLING (dedicated) OS thread, tokio-free end to end. The caller already
/// spawned the `SyncReplicaController` on its own thread; `shared` is the bridge.
///
/// `seg_size` and `slot_name` are the one-time setup the async `handle` resolved;
/// this fn does its own connect / identify / slot / timeline derivation (mirrors
/// `connect_and_derive`) so the path needs no tokio.
pub(crate) fn run(
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    mut cfg: PgConfig,
    slot_name: Option<String>,
    seg_size: u64,
    shared: Arc<Shared>,
) -> Result<()> {
    install_shutdown_handler()?;

    // Initial connect: derive resume timeline + aligned start from the slot /
    // server position. `None` resume = initial; reconnects pass our frontier.
    let (mut conn, mut timeline, mut next_start) = connect_and_derive(
        &cfg,
        slot_name.as_deref(),
        settings,
        storage,
        archive_dir,
        seg_size,
        None,
    )?;
    // Seed the durable frontier at the resume point.
    shared.fsyncd_lsn.store(next_start, Ordering::Release);

    let mut writer =
        SyncSegmentWriter::new(timeline, archive_dir.to_path_buf(), seg_size, next_start)?;
    let mut last_status = Instant::now();

    // Reconnect loop: a stream break (primary loss) or a `failover-primary`
    // re-target ends a session WITHOUT exiting — the controller keeps the
    // control API up, and we reconnect to the (possibly re-targeted) primary,
    // resuming from our durable frontier.
    'session: loop {
        let ended = run_session(
            &mut conn,
            &mut writer,
            &shared,
            &mut last_status,
            slot_name.as_deref(),
            &mut timeline,
            &mut next_start,
            settings,
            storage,
            archive_dir,
            seg_size,
        );

        match &ended {
            Ok(SessionEnd::Shutdown) => break 'session,
            Ok(SessionEnd::Retarget) => {
                tracing::info!(
                    target = "wal_receive",
                    "failover-primary: re-target requested"
                )
            }
            Err(e) => tracing::warn!(
                target = "wal_receive",
                "stream session ended: {e:#}; reconnecting"
            ),
        }

        // Retain the in-flight partial (fsync'd) so dr-catchup can ship its tail
        // while we (re)connect; raise the frontier to its durable end so the CP
        // gates failover on everything we hold (RPO=0 with the batch window).
        writer.finalize_partial(&shared.fsyncd_lsn)?;

        // Reconnect with retry + backoff, applying any pending re-target.
        let reconnected = loop {
            if shutdown_requested() {
                break None;
            }
            if let Some(rt) = shared.retarget.lock().unwrap().take() {
                tracing::info!(
                    target = "wal_receive",
                    "re-targeting primary to {}:{}",
                    rt.host,
                    rt.port
                );
                cfg.host = rt.host;
                cfg.port = rt.port;
            }
            let frontier = shared.fsyncd_lsn.load(Ordering::Acquire);
            match connect_and_derive(
                &cfg,
                slot_name.as_deref(),
                settings,
                storage,
                archive_dir,
                seg_size,
                Some(frontier),
            ) {
                Ok(v) => break Some(v),
                Err(e) => {
                    tracing::warn!(
                        target = "wal_receive",
                        "reconnect to {}:{} failed: {e:#}; retrying",
                        cfg.host,
                        cfg.port
                    );
                    sleep_interruptible(RECONNECT_BACKOFF, &shared);
                }
            }
        };

        match reconnected {
            Some((c, tl, ns)) => {
                conn = c;
                timeline = tl;
                next_start = ns;
                writer.reset_for_timeline(tl, ns);
                tracing::info!(
                    target = "wal_receive",
                    "reconnected to {}:{}, resuming timeline {tl} at {}",
                    cfg.host,
                    cfg.port,
                    format_pg_lsn(ns)
                );
            }
            None => break 'session,
        }
    }

    // Graceful shutdown: wake the controller, flush the tail.
    shared.stop.notify_one();
    writer.finalize_partial(&shared.fsyncd_lsn)?;
    Ok(())
}

/// Sleep up to `dur`, waking early on shutdown or a retarget signal. The signal
/// is a tokio `Notify` (cross-runtime), so we can't await it here; instead poll
/// the shutdown flag + the retarget slot in short slices.
fn sleep_interruptible(dur: Duration, shared: &Shared) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline {
        if shutdown_requested() || shared.retarget.lock().unwrap().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50).min(deadline - Instant::now()));
    }
}

fn shutdown_requested() -> bool {
    SHUTDOWN.load(Ordering::SeqCst)
}

/// Outcome of one streaming session (one primary connection).
enum SessionEnd {
    Shutdown,
    Retarget,
}

/// Connect, identify, ensure the slot, and resolve the resume timeline + aligned
/// start LSN. `resume_from = None` is the initial connect (start from the slot's
/// restart_lsn / the server xlogpos); `Some(frontier)` is a reconnect.
#[allow(clippy::too_many_arguments)]
fn connect_and_derive(
    cfg: &PgConfig,
    slot_name: Option<&str>,
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    seg_size: u64,
    resume_from: Option<u64>,
) -> Result<(SyncReplicationConn, u32, u64)> {
    // Slot existence is queried per-connect (a failover target may not hold it).
    let slot = match slot_name {
        Some(name) => {
            let mut q = SyncReplicationConn::connect_with(cfg, false, STATUS_UPDATE_INTERVAL)?;
            query_slot_info(&mut q, name)?
        }
        None => SlotInfo {
            exists: false,
            restart_lsn: None,
        },
    };
    tracing::info!(
        target = "wal_receive",
        "connecting to {}:{} as {} (db={})",
        cfg.host,
        cfg.port,
        cfg.user,
        cfg.database
    );
    let mut conn = SyncReplicationConn::connect(cfg, STATUS_UPDATE_INTERVAL)?;
    let (sysid, sys_timeline, xlogpos) = identify_system(&mut conn)?;

    let start_lsn = match resume_from {
        Some(frontier) => {
            if let Some(name) = slot_name.filter(|_| !slot.exists) {
                conn.create_physical_replication_slot(name)?;
                tracing::info!(
                    target = "wal_receive",
                    "created replication slot {name} on reconnect"
                );
            }
            frontier
        }
        None => match slot_name {
            Some(_) if slot.exists => slot.restart_lsn.unwrap_or(xlogpos),
            Some(name) => {
                conn.create_physical_replication_slot(name)?;
                tracing::info!(target = "wal_receive", "created replication slot {name}");
                xlogpos
            }
            None => xlogpos,
        },
    };
    let timeline = get_start_timeline(
        &mut conn,
        settings,
        storage,
        archive_dir,
        sys_timeline,
        start_lsn,
    )?;
    let next_start = align(start_lsn, seg_size);
    tracing::info!(
        target = "wal_receive",
        "system={sysid} sys_timeline={sys_timeline} start_lsn={} timeline={timeline} (aligned={})",
        format_pg_lsn(start_lsn),
        format_pg_lsn(next_start),
    );
    Ok((conn, timeline, next_start))
}

/// Stream one primary connection across timeline switches until the stream
/// breaks (`Err`), a re-target is requested, or shutdown.
#[allow(clippy::too_many_arguments)]
fn run_session(
    conn: &mut SyncReplicationConn,
    writer: &mut SyncSegmentWriter,
    shared: &Arc<Shared>,
    last_status: &mut Instant,
    slot_name: Option<&str>,
    timeline: &mut u32,
    next_start: &mut u64,
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    seg_size: u64,
) -> Result<SessionEnd> {
    'replication: loop {
        conn.start_replication(slot_name, *next_start, *timeline)?;

        loop {
            // Periodic status keeps a quiet client connected and advances the
            // slot's restart_lsn even with no WAL flowing.
            if last_status.elapsed() >= STATUS_UPDATE_INTERVAL {
                send_status(conn, writer, shared)?;
                *last_status = Instant::now();
            }

            match conn.recv_message()? {
                RecvOutcome::Timeout => {
                    // Stream quiet: tick the keepalive, then poll signals.
                    send_status(conn, writer, shared)?;
                    *last_status = Instant::now();
                    if shutdown_requested() {
                        tracing::info!(target = "wal_receive", "shutdown signal received");
                        shared.stop.notify_one();
                        return Ok(SessionEnd::Shutdown);
                    }
                    if shared.retarget.lock().unwrap().is_some() {
                        tracing::info!(target = "wal_receive", "re-target pending; ending session");
                        return Ok(SessionEnd::Retarget);
                    }
                }
                RecvOutcome::Message(first) => {
                    // Batch-window group commit. In 2-acker mode (the standby is
                    // the pacing acker, `sole_acker == false`) our flush ack does
                    // not gate commits, so we accumulate frames — draining the
                    // read buffer and reading more, up to BATCH_WINDOW — then
                    // issue ONE fdatasync + ONE ack for the whole batch, cutting
                    // the fsync/IOPS rate ~N-fold. In sole-acker mode every commit
                    // blocks on our ack, so we skip the window: per-frame fsync,
                    // minimum latency (this is the path that holds RPO=0 at speed).
                    let batch = !shared.sole_acker.load(Ordering::Relaxed);
                    let deadline = batch.then(|| Instant::now() + BATCH_WINDOW);
                    let mut wrote = false;
                    let mut frames = 0u64;
                    let mut reply_requested = false;
                    let mut copy_done = false;
                    let mut windowed = false;
                    let mut next = Some(first);
                    'batch: loop {
                        // Process everything already available (first + buffered).
                        while let Some(msg) = next.take() {
                            match msg {
                                Message::CopyData(d) => {
                                    let payload: Bytes = d.into_bytes();
                                    match decode_frame(&payload)? {
                                        Frame::Wal(w) => {
                                            writer.write(w.start_lsn, w.data)?;
                                            wrote = true;
                                            frames += 1;
                                        }
                                        Frame::Keepalive(k) => {
                                            reply_requested |= k.reply_requested;
                                        }
                                    }
                                }
                                // CopyDone is a timeline switch: flush what we
                                // have, then leave the stream loop to handle it.
                                Message::CopyDone => {
                                    copy_done = true;
                                    break 'batch;
                                }
                                Message::ErrorResponse(e) => {
                                    bail!("wal-receive: {}", error_message(&e))
                                }
                                m => tracing::debug!(
                                    target = "wal_receive",
                                    "ignoring {}",
                                    message_kind(&m)
                                ),
                            }
                            next = conn.recv_buffered()?;
                        }
                        // 2-acker only: wait briefly for more frames, then flush
                        // once. The deadline check bounds the window to ~BATCH_WINDOW.
                        match deadline {
                            Some(dl) if Instant::now() < dl => {
                                if !windowed {
                                    conn.set_read_timeout(BATCH_WINDOW)?;
                                    windowed = true;
                                }
                                match conn.recv_message()? {
                                    RecvOutcome::Message(m) => next = Some(m),
                                    RecvOutcome::Timeout => break 'batch, // stream quiet
                                }
                            }
                            _ => break 'batch,
                        }
                    }
                    if windowed {
                        conn.set_read_timeout(STATUS_UPDATE_INTERVAL)?;
                    }
                    // Durability: fdatasync raises the frontier BEFORE the ack, so
                    // send_status only ever reports fsync'd LSNs (RPO=0 intact).
                    if wrote {
                        writer.sync(&shared.fsyncd_lsn)?;
                        send_status(conn, writer, shared)?;
                        *last_status = Instant::now();
                        let tf = BATCH_FRAMES.fetch_add(frames, Ordering::Relaxed) + frames;
                        let tb = BATCH_FSYNCS.fetch_add(1, Ordering::Relaxed) + 1;
                        if tb.is_multiple_of(BATCH_STATS_EVERY) {
                            // Windowed (recent) rate since the last log — the
                            // cumulative average is dominated by mode history.
                            let lf = BATCH_LOG_FRAMES.swap(tf, Ordering::Relaxed);
                            let lb = BATCH_LOG_FSYNCS.swap(tb, Ordering::Relaxed);
                            tracing::info!(
                                target = "wal_receive",
                                "batch_stats recent_frames_per_fsync={:.2} sole_acker={} windowed={windowed}",
                                (tf - lf) as f64 / (tb - lb).max(1) as f64,
                                shared.sole_acker.load(Ordering::Relaxed),
                            );
                        }
                    } else if reply_requested {
                        send_status(conn, writer, shared)?;
                        *last_status = Instant::now();
                    }
                    if copy_done {
                        break;
                    }
                }
            }
        }

        tracing::info!(
            target = "wal_receive",
            "server closed CopyOut (timeline switch)"
        );
        conn.end_copy()?;
        let restart = writer
            .finalize_partial(&shared.fsyncd_lsn)?
            .unwrap_or(writer.received_lsn);

        *timeline += 1;
        match conn.timeline_history(*timeline)? {
            Some((fname, content)) => {
                upload_history(settings, storage, archive_dir, &fname, &content)?;
            }
            None => tracing::warn!(
                target = "wal_receive",
                "no history file for timeline {}",
                *timeline
            ),
        }
        *next_start = align(restart, seg_size);
        writer.reset_for_timeline(*timeline, *next_start);
        tracing::info!(
            target = "wal_receive",
            "switched to timeline {}, restarting at {}",
            *timeline,
            format_pg_lsn(*next_start),
        );
        continue 'replication;
    }
}

/// Standby status update. Flush reports the fsync'd durable frontier, capped by
/// the janitor's back-pressure ceiling, so the slot's restart_lsn never advances
/// past durable WAL. Apply mirrors flush.
fn send_status(
    conn: &mut SyncReplicationConn,
    writer: &SyncSegmentWriter,
    shared: &Shared,
) -> Result<()> {
    let write = writer.write_position();
    let flush = shared
        .fsyncd_lsn
        .load(Ordering::Acquire)
        .min(shared.ack_ceiling.load(Ordering::Relaxed))
        .min(write);
    let payload = build_status_update(write, flush, flush);
    conn.send_copy_data(&payload)
}

/// Physical replication slot state read from `pg_replication_slots`.
struct SlotInfo {
    exists: bool,
    restart_lsn: Option<u64>,
}

fn query_slot_info(q: &mut SyncReplicationConn, slot_name: &str) -> Result<SlotInfo> {
    let sql = format!(
        "SELECT active, restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot_name}'"
    );
    let rows = q.query_rows(&sql)?;
    match rows.first() {
        None => Ok(SlotInfo {
            exists: false,
            restart_lsn: None,
        }),
        Some(cols) => {
            let restart_lsn = cols
                .get(1)
                .and_then(|c| c.as_deref())
                .map(crate::pg::backup::parse_pg_lsn)
                .transpose()?;
            Ok(SlotInfo {
                exists: true,
                restart_lsn,
            })
        }
    }
}

fn identify_system(conn: &mut SyncReplicationConn) -> Result<(String, u32, u64)> {
    let rows = conn.query_rows("IDENTIFY_SYSTEM")?;
    let cols = rows
        .first()
        .ok_or_else(|| anyhow!("IDENTIFY_SYSTEM returned an empty result"))?;
    let col = |i: usize| cols.get(i).and_then(|c| c.as_deref());
    let sysid = col(0).unwrap_or_default().to_string();
    let tli: u32 = match col(1) {
        Some(v) => v.parse().context("timeline parse")?,
        None => 0,
    };
    let xlogpos = match col(2) {
        Some(v) => crate::pg::backup::parse_pg_lsn(v)?,
        None => 0,
    };
    if sysid.is_empty() || tli == 0 {
        bail!("IDENTIFY_SYSTEM returned an empty result");
    }
    Ok((sysid, tli, xlogpos))
}

/// Resolve which timeline `xlogpos` belongs to, fetching + retaining the history
/// file when needed. Mirrors `crate::pg::wal::receive::get_start_timeline`.
fn get_start_timeline(
    conn: &mut SyncReplicationConn,
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    sys_timeline: u32,
    xlogpos: u64,
) -> Result<u32> {
    if sys_timeline < 2 {
        return Ok(1);
    }
    match conn.timeline_history(sys_timeline)? {
        Some((fname, content)) => {
            upload_history(settings, storage, archive_dir, &fname, &content)?;
            Ok(lsn_to_timeline(&content, xlogpos, sys_timeline))
        }
        None => Ok(sys_timeline),
    }
}

/// Timeline owning `lsn` per a `.history` file (wal-g LSNToTimeLine). Shared
/// logic with the async path, duplicated here to keep the module tokio-free.
fn lsn_to_timeline(content: &[u8], lsn: u64, file_timeline: u32) -> u32 {
    let text = String::from_utf8_lossy(content);
    let mut rows: Vec<(u32, u64)> = Vec::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("");
        let mut cols = line.split_whitespace();
        if let (Some(t), Some(l)) = (cols.next(), cols.next())
            && let (Ok(t), Ok(l)) = (t.parse::<u32>(), crate::pg::backup::parse_pg_lsn(l))
        {
            rows.push((t, l));
        }
    }
    rows.sort_by_key(|(t, _)| *t);
    for (t, switch_lsn) in &rows {
        if lsn < *switch_lsn {
            return *t;
        }
    }
    file_timeline
}

/// Stage `<tli>.history` locally then retain it (SyncReplica never uploads —
/// the controller's dr-catchup ships the tail). We keep the file on disk so it's
/// available alongside the retained segments.
fn upload_history(
    _settings: &Settings,
    _storage: &DynStorage,
    archive_dir: &Path,
    fname: &str,
    content: &[u8],
) -> Result<()> {
    std::fs::create_dir_all(archive_dir)
        .with_context(|| format!("create_dir_all {}", archive_dir.display()))?;
    let path = archive_dir.join(fname);
    std::fs::write(&path, content).with_context(|| format!("stage {}", path.display()))?;
    tracing::info!(target = "wal_receive", "retained history file {fname}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer(dir: &Path, seg_size: u64) -> SyncSegmentWriter {
        SyncSegmentWriter::new(1, dir.to_path_buf(), seg_size, 0).unwrap()
    }

    #[test]
    fn write_and_rotate_retains_complete_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut w = writer(dir.path(), seg_size);
        w.write(0, &[0xAB; 16]).unwrap(); // fills the segment -> rotate
        let name = w.segment_for_lsn(0).format();
        // completed segment is fsync'd + renamed, retained on disk (no upload)
        assert_eq!(
            std::fs::read(dir.path().join(&name)).unwrap(),
            vec![0xAB; 16],
            "completed segment retained locally"
        );
        assert!(
            !dir.path().join(format!("{name}.partial")).exists(),
            "partial renamed away"
        );
    }

    #[test]
    fn sync_advances_fsyncd_frontier_after_fdatasync() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), 16);
        let frontier = AtomicU64::new(0);
        w.write(0, &[0xAB; 4]).unwrap();
        assert_eq!(
            frontier.load(Ordering::Relaxed),
            0,
            "buffered, not yet fsync'd: frontier must not move"
        );
        let durable = w.sync(&frontier).unwrap();
        assert_eq!(durable, 4);
        assert_eq!(
            frontier.load(Ordering::Relaxed),
            4,
            "frontier = fsync'd write cursor only AFTER sync_data returns"
        );
    }

    #[test]
    fn finalize_partial_retains_partial_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut w = writer(dir.path(), seg_size);
        w.write(0, &[0xCD; 4]).unwrap();
        let name = w.current.as_ref().unwrap().name.format();
        let frontier = AtomicU64::new(0);
        let start = w.finalize_partial(&frontier).unwrap();
        assert_eq!(start, Some(0));
        assert_eq!(
            frontier.load(Ordering::Relaxed),
            4,
            "finalize raises the frontier to the durable end of the partial"
        );
        let partial = dir.path().join(format!("{name}.partial"));
        assert!(partial.exists(), "partial retained: {partial:?}");
        assert!(
            !dir.path().join(&name).exists(),
            "incomplete segment must not carry the bare name"
        );
        let meta = std::fs::metadata(&partial).unwrap();
        assert_eq!(meta.len(), seg_size, "partial keeps the zero pad");
    }

    #[test]
    fn finalize_partial_drops_empty_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = writer(dir.path(), 16);
        w.ensure_current(0).unwrap();
        let name = w.current.as_ref().unwrap().name.format();
        let frontier = AtomicU64::new(7);
        assert_eq!(w.finalize_partial(&frontier).unwrap(), Some(0));
        assert_eq!(
            frontier.load(Ordering::Relaxed),
            7,
            "an empty (dropped) partial leaves the frontier untouched"
        );
        assert!(!dir.path().join(&name).exists());
        assert!(!dir.path().join(format!("{name}.partial")).exists());
    }

    #[test]
    fn positioned_write_after_reconnect_preserves_zero_pad() {
        // Simulate a reconnect at a non-zero offset: write at offset 8 into a
        // fresh segment; bytes [0,8) must stay the zero pad (no seek+append).
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut w = writer(dir.path(), seg_size);
        w.write(8, &[0xEF; 4]).unwrap();
        let name = w.current.as_ref().unwrap().name.format();
        w.finalize_partial(&AtomicU64::new(0)).unwrap();
        let bytes = std::fs::read(dir.path().join(format!("{name}.partial"))).unwrap();
        assert_eq!(bytes.len() as u64, seg_size);
        assert!(
            bytes[..8].iter().all(|&b| b == 0),
            "leading zero pad intact"
        );
        assert_eq!(&bytes[8..12], &[0xEF; 4]);
        assert!(
            bytes[12..].iter().all(|&b| b == 0),
            "trailing zero pad intact"
        );
    }

    #[test]
    fn write_splits_across_segment_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut w = writer(dir.path(), seg_size);
        // 4 bytes in seg0, then a write that crosses into seg1
        w.write(0, &[0xCD; 4]).unwrap();
        w.write(seg_size, &[0xEF; 4]).unwrap();
        let first = w.segment_for_lsn(0).format();
        // The boundary crossing rotated seg0 (partial) to bare, zero-padded.
        let bytes = std::fs::read(dir.path().join(&first)).unwrap();
        assert_eq!(bytes.len() as u64, seg_size);
        assert_eq!(&bytes[..4], &[0xCD; 4]);
        assert!(bytes[4..].iter().all(|&b| b == 0));
    }

    #[test]
    fn lsn_to_timeline_walks_history() {
        let content = b"1\t0/3000000\tno recovery target specified\n\
                        2\t0/5000000\tno recovery target specified\n";
        let at = |s: &str| crate::pg::backup::parse_pg_lsn(s).unwrap();
        assert_eq!(lsn_to_timeline(content, at("0/2000000"), 3), 1);
        assert_eq!(lsn_to_timeline(content, at("0/4000000"), 3), 2);
        assert_eq!(lsn_to_timeline(content, at("0/6000000"), 3), 3);
        assert_eq!(lsn_to_timeline(b"", 0x9999, 5), 5);
    }
}
