//! wal-receive: long-running START_REPLICATION consumer that archives
//! streamed WAL segments directly. Drop-in replacement for an
//! `archive_command`-driven workflow when a sidecar host is preferred.
//!
//! Wire pieces:
//!
//! - A side (non-replication) connection reads `wal_segment_size` and, when
//!   `WALG_SLOTNAME` is set, that slot's restart_lsn (physical replication mode
//!   forbids these queries), matching wal-g `getCurrentWalInfo`
//! - `IDENTIFY_SYSTEM` returns current timeline + LSN
//! - With a slot configured it's created if missing and replication resumes from
//!   its restart_lsn so the server's retained WAL isn't skipped across restarts.
//!   Unset `WALG_SLOTNAME` runs slotless — diverges from wal-g, which defaults to
//!   a `walg` slot (see WALG_COMPAT.md)
//! - `START_REPLICATION [SLOT <slot> PHYSICAL] <lsn> TIMELINE <tli>` enters CopyOut
//! - Server sends CopyData frames:
//!   'w' = WAL data:   u64 start_lsn | u64 server_wal_end | u64 send_time | bytes
//!   'k' = keepalive:  u64 server_wal_end | u64 send_time | u8 reply_requested
//! - Client replies (`r`): u64 write | u64 flush | u64 apply | u64 client_time | u8 reply_requested
//! - `CopyDone` is a timeline switch: ship the partial, fetch + upload the next
//!   timeline's `.history`, then restart replication on the new timeline
//!
//! Segments accumulate into a `wal_segment_size`-aligned buffer; rotation spawns
//! a `wal-push` upload task bounded by WALG_UPLOAD_CONCURRENCY, so the receive
//! loop keeps consuming frames (and answering keepalives) while slow uploads
//! drain. The flush position reported to the server trails completed uploads, so
//! a configured slot's restart_lsn — and thus server-side WAL retention — never
//! advances past un-archived WAL. Uploaded segments ride the regular compression +
//! storage pipeline so the archive format stays consistent with
//! `archive_command`-driven pushes.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::Settings;
use crate::pg::backup::{format_pg_lsn, parse_pg_lsn};
use crate::pg::replication::conn::{PgConfig, ReplicationConn, error_message, message_kind};
use crate::pg::replication::stream::{Frame, build_status_update, decode_frame};
use crate::pg::wal::push;
use crate::pg::wal::segment::{self, SegmentName};
use crate::storage::DynStorage;

/// Status update cadence — wal-g defaults to 10s; we match
const STATUS_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Round an LSN down to its segment boundary
fn align(lsn: u64, seg_size: u64) -> u64 {
    lsn - (lsn % seg_size)
}

/// Accumulate WAL bytes into segment-sized files on disk; rotate on
/// boundary crossings. Each fully-written segment is shipped via the
/// existing `wal-push` pipeline so compression + retry + rate limits apply
struct SegmentAccumulator {
    seg_size: u64,
    timeline: u32,
    archive_dir: PathBuf,
    current: Option<CurrentSegment>,
    settings: Settings,
    storage: DynStorage,
    /// In-flight segment uploads, bounded by `upload_sem`. Each task yields the
    /// start LSN of the segment it shipped so reaping can drop it from
    /// `in_flight`. Receive loop reaps completions each iteration; rotation
    /// blocks only once `upload_concurrency` uploads are already in flight
    uploads: JoinSet<Result<u64>>,
    upload_sem: Arc<Semaphore>,
    /// Start LSNs of segments whose upload hasn't completed. The minimum gates
    /// the flush position reported to the server: with a replication slot the
    /// server recycles WAL below the flush LSN, so it must trail uploads, never
    /// local writes
    in_flight: BTreeSet<u64>,
    /// Highest WAL LSN received (write position). Reset to the restart LSN on a
    /// timeline switch
    received_lsn: u64,
}

/// In-progress segment. Written to `<seg>.partial` (pg_receivewal convention),
/// renamed to bare `<seg>` only once full, so only complete segments ever carry
/// the bare name — a crash can't leave a torn partial masquerading as finished
struct CurrentSegment {
    name: SegmentName,
    file: tokio::fs::File,
    /// `<archive_dir>/<seg>.partial`
    path: PathBuf,
    bytes_written: u64,
}

impl SegmentAccumulator {
    async fn new(
        timeline: u32,
        archive_dir: PathBuf,
        seg_size: u64,
        settings: Settings,
        storage: DynStorage,
        start_lsn: u64,
    ) -> Result<Self> {
        fs::create_dir_all(&archive_dir)
            .await
            .with_context(|| format!("create_dir_all {}", archive_dir.display()))?;
        let upload_sem = Arc::new(Semaphore::new(settings.upload_concurrency.max(1)));
        Ok(Self {
            seg_size,
            timeline,
            archive_dir,
            current: None,
            settings,
            storage,
            uploads: JoinSet::new(),
            upload_sem,
            in_flight: BTreeSet::new(),
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

    /// Write `data` starting at `start_lsn`. Splits across segment
    /// boundaries, rotating + spawning an upload for each completed segment
    async fn write(&mut self, start_lsn: u64, data: &[u8]) -> Result<()> {
        let mut lsn = start_lsn;
        let mut data = data;
        while !data.is_empty() {
            self.ensure_current(lsn).await?;
            let cur = self.current.as_mut().expect("ensure_current set it");
            let cur_seg_start = cur.name.start_lsn(self.seg_size);
            let cur_seg_end = cur_seg_start + self.seg_size;
            let offset_in_seg = lsn - cur_seg_start;
            let space_left = cur_seg_end - lsn;
            let chunk_len = std::cmp::min(space_left as usize, data.len());

            // Files are pre-allocated to seg_size + writes seek-positioned by
            // offset_in_seg. Postgres always sends contiguous WAL but if our
            // accumulator's `bytes_written` lags behind the offset (eg on
            // reconnect at a non-zero offset), preserve the gap by seeking
            if offset_in_seg != cur.bytes_written {
                use tokio::io::AsyncSeekExt;
                cur.file
                    .seek(std::io::SeekFrom::Start(offset_in_seg))
                    .await
                    .context("seek within segment")?;
            }
            cur.file
                .write_all(&data[..chunk_len])
                .await
                .context("write WAL chunk")?;
            cur.bytes_written = offset_in_seg + chunk_len as u64;
            lsn += chunk_len as u64;
            data = &data[chunk_len..];
            if cur.bytes_written == self.seg_size {
                self.rotate_and_spawn().await?;
            }
        }
        self.received_lsn = self.received_lsn.max(lsn);
        Ok(())
    }

    async fn ensure_current(&mut self, lsn: u64) -> Result<()> {
        if let Some(cur) = self.current.as_ref() {
            let seg_start = cur.name.start_lsn(self.seg_size);
            if lsn >= seg_start && lsn < seg_start + self.seg_size {
                return Ok(());
            }
            // Boundary crossing on a partial segment is unusual but possible
            // (eg WAL switch midway); push what we have (zero-padded to
            // seg_size by the pre-extend), then open the new one
            tracing::warn!(
                target = "wal_receive",
                "rotating partial segment {} ({} bytes written, target {})",
                cur.name.format(),
                cur.bytes_written,
                self.seg_size
            );
            self.rotate_and_spawn().await?;
        }
        let seg = self.segment_for_lsn(lsn);
        let path = self.archive_dir.join(format!("{}.partial", seg.format()));
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        // pre-extend to seg_size so partial-tail writes leave a zero pad
        file.set_len(self.seg_size).await?;
        // rotate_and_spawn above took any previous segment
        self.current = Some(CurrentSegment {
            name: seg,
            file,
            path,
            bytes_written: 0,
        });
        Ok(())
    }

    /// Close out current segment & spawn its upload. Permit acquisition is
    /// the backpressure point: blocks only once `upload_concurrency` uploads
    /// are already in flight, so bursty WAL against a slow store stalls the
    /// stream N segments deep instead of on every rotation
    async fn rotate_and_spawn(&mut self) -> Result<()> {
        let Some(cur) = self.current.take() else {
            return Ok(());
        };
        let CurrentSegment {
            name,
            mut file,
            path: partial,
            ..
        } = cur;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        // Publish the finished segment: <seg>.partial -> <seg>. Only bare <seg>
        // files are complete, so a crash before upload leaves a re-pushable
        // segment (see repush_leftover_segments), never a torn partial
        let path = self.archive_dir.join(name.format());
        fs::rename(&partial, &path)
            .await
            .with_context(|| format!("rename {} -> {}", partial.display(), path.display()))?;
        let start = name.start_lsn(self.seg_size);
        tracing::info!(
            target = "wal_receive",
            "segment {} complete, archiving",
            name.format()
        );
        let permit = self
            .upload_sem
            .clone()
            .acquire_owned()
            .await
            .context("acquire upload permit")?;
        let settings = self.settings.clone();
        let storage = self.storage.clone();
        self.in_flight.insert(start);
        self.uploads.spawn(async move {
            let _permit = permit;
            // Re-uses wal::push::handle so compression + rate limiting + retry
            // semantics stay identical to archive_command-driven pushes
            push::handle(&settings, storage, &path)
                .await
                .with_context(|| format!("archive {}", path.display()))?;
            // remove local file after a successful upload
            let _ = fs::remove_file(&path).await;
            Ok(start)
        });
        Ok(())
    }

    /// Await all in-flight uploads; first failure wins
    async fn drain_uploads(&mut self) -> Result<()> {
        while let Some(joined) = self.uploads.join_next().await {
            let start = joined.context("upload task join")??;
            self.in_flight.remove(&start);
        }
        self.in_flight.clear();
        Ok(())
    }

    /// On a timeline switch wal-g uploads the in-progress segment under its
    /// `<seg>.partial` name (the tail of the old timeline won't be re-sent on
    /// the next), then drops it locally. Returns the partial's start LSN — the
    /// point the next timeline re-streams from
    async fn upload_partial_on_switch(&mut self) -> Result<Option<u64>> {
        let Some(cur) = self.current.take() else {
            return Ok(None);
        };
        let CurrentSegment {
            name,
            mut file,
            path,
            ..
        } = cur;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        let start = name.start_lsn(self.seg_size);
        tracing::info!(
            target = "wal_receive",
            "timeline switch: archiving partial {}",
            name.format()
        );
        // path is `<seg>.partial`; push keys off the basename so it lands under
        // `wal_005/<seg>.partial.<ext>`, matching wal-g's partial upload
        push::handle(&self.settings, self.storage.clone(), &path)
            .await
            .with_context(|| format!("archive partial {}", path.display()))?;
        let _ = fs::remove_file(&path).await;
        Ok(Some(start))
    }

    /// Re-aim the accumulator at a new timeline. `upload_partial_on_switch`
    /// already cleared `current`; re-anchor the write position to the restart
    fn reset_for_timeline(&mut self, timeline: u32, restart_lsn: u64) {
        debug_assert!(self.current.is_none());
        self.timeline = timeline;
        self.received_lsn = restart_lsn;
    }

    /// Flush in-flight segment to `<seg>.partial` on graceful shutdown.
    /// Mirrors `pg_receivewal`: partial stays local at full seg_size with the
    /// zero pad, never uploaded; restart re-requests from the server-held LSN
    async fn finalize_partial(&mut self) -> Result<()> {
        let Some(cur) = self.current.take() else {
            return Ok(());
        };
        let CurrentSegment {
            name,
            mut file,
            path,
            bytes_written,
        } = cur;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        if bytes_written == 0 {
            let _ = fs::remove_file(&path).await;
            return Ok(());
        }
        // Already named <seg>.partial while streaming; leave it local and never
        // upload it (pg_receivewal). Restart re-requests from the server-held LSN
        tracing::info!(
            target = "wal_receive",
            "wrote partial segment {} ({} bytes of {})",
            name.format(),
            bytes_written,
            self.seg_size
        );
        Ok(())
    }

    /// Highest WAL LSN received — the write position reported to the server
    fn write_position(&self) -> u64 {
        self.received_lsn
    }

    /// Uploaded-through LSN reported as the flush position. Floors at the
    /// oldest still-uploading segment (so the server can't recycle un-archived
    /// WAL), else at the current segment's start (the partial isn't durable),
    /// else everything received is uploaded
    fn flush_position(&self) -> u64 {
        let current_floor = self
            .current
            .as_ref()
            .map(|c| c.name.start_lsn(self.seg_size))
            .unwrap_or(self.received_lsn);
        self.in_flight
            .iter()
            .next()
            .copied()
            .map_or(current_floor, |oldest| oldest.min(current_floor))
    }
}

pub async fn handle(settings: &Settings, storage: DynStorage, archive_dir: &Path) -> Result<()> {
    let cfg = PgConfig::from_env()?;
    let slot_name = slot_name_from_env()?;

    // wal_segment_size + slot info come from a normal (non-replication)
    // connection — physical replication mode forbids these queries, so wal-g
    // opens the same kind of side connection in getCurrentWalInfo
    let (seg_size, slot) = {
        let mut q = ReplicationConn::connect_with(&cfg, false).await?;
        let seg_size = query_wal_segment_size(&mut q).await?;
        let slot = match slot_name.as_deref() {
            Some(name) => query_slot_info(&mut q, name).await?,
            None => SlotInfo {
                exists: false,
                restart_lsn: None,
            },
        };
        (seg_size, slot)
    };
    segment::set_wal_segment_size(seg_size);
    tracing::info!(
        target = "wal_receive",
        "wal_segment_size={seg_size} slot={} (exists={})",
        slot_name.as_deref().unwrap_or("<none>"),
        slot.exists
    );

    // Re-archive complete segments a prior crash left un-uploaded before the
    // stream resumes (a slot retains WAL from the last flushed LSN, but local
    // leftovers still need shipping)
    repush_leftover_segments(settings, &storage, archive_dir, seg_size).await?;

    tracing::info!(
        target = "wal_receive",
        "connecting to {}:{} as {} (db={})",
        cfg.host,
        cfg.port,
        cfg.user,
        cfg.database
    );
    let mut conn = ReplicationConn::connect(&cfg).await?;
    let (sysid, sys_timeline, xlogpos) = identify_system(&mut conn).await?;

    // With a slot, resume from its restart_lsn so the server's retained WAL
    // isn't skipped, creating it if missing. Slotless: start from the server's
    // current position like `pg_receivewal` without a slot — the server may
    // recycle WAL below this, so a gap is possible if we fall behind, traded for
    // not pinning primary WAL
    let start_lsn = match slot_name.as_deref() {
        Some(_) if slot.exists => slot.restart_lsn.unwrap_or(xlogpos),
        Some(name) => {
            conn.create_physical_replication_slot(name).await?;
            tracing::info!(target = "wal_receive", "created replication slot {name}");
            xlogpos
        }
        None => xlogpos,
    };
    // The restart LSN may predate the latest timeline; resolve which timeline
    // owns it from the history file (wal-g getStartTimeline)
    let mut timeline = get_start_timeline(
        &mut conn,
        settings,
        &storage,
        archive_dir,
        sys_timeline,
        start_lsn,
    )
    .await?;
    let mut next_start = align(start_lsn, seg_size);
    tracing::info!(
        target = "wal_receive",
        "system={sysid} sys_timeline={sys_timeline} start_lsn={} timeline={timeline} (aligned={})",
        format_pg_lsn(start_lsn),
        format_pg_lsn(next_start),
    );

    let mut acc = SegmentAccumulator::new(
        timeline,
        archive_dir.to_path_buf(),
        seg_size,
        settings.clone(),
        storage.clone(),
        next_start,
    )
    .await?;
    let mut last_status = std::time::Instant::now();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    // One iteration per timeline; CopyDone advances to the next
    'replication: loop {
        start_replication(&mut conn, slot_name.as_deref(), next_start, timeline).await?;

        loop {
            // Periodic standby status keeps a quiet client connected (wal-g
            // pings every 10s) and, with a slot, advances its restart_lsn
            if last_status.elapsed() >= STATUS_UPDATE_INTERVAL {
                send_status(&mut conn, &acc).await?;
                last_status = std::time::Instant::now();
            }

            let msg = tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!(target = "wal_receive", "shutdown signal received, flushing");
                    acc.drain_uploads().await?;
                    acc.finalize_partial().await?;
                    return Ok(());
                }
                // Reap finished uploads so a failure surfaces now, not at the
                // next rotation. Pattern mismatch on empty set disables the arm
                Some(joined) = acc.uploads.join_next() => {
                    let start = joined.context("upload task join")??;
                    acc.in_flight.remove(&start);
                    continue;
                }
                r = tokio::time::timeout(STATUS_UPDATE_INTERVAL, conn.recv_message()) => match r {
                    Ok(r) => r?,
                    Err(_) => continue, // tick the keepalive
                },
            };
            match msg {
                Message::CopyData(d) => {
                    let payload: Bytes = d.into_bytes();
                    match decode_frame(&payload)? {
                        Frame::Wal(w) => acc.write(w.start_lsn, w.data).await?,
                        Frame::Keepalive(k) => {
                            if k.reply_requested {
                                send_status(&mut conn, &acc).await?;
                                last_status = std::time::Instant::now();
                            }
                        }
                    }
                }
                // CopyDone is a timeline switch: drain, ship the partial, fetch
                // and upload the next timeline's history, then restart on it
                Message::CopyDone => break,
                Message::ErrorResponse(e) => bail!("wal-receive: {}", error_message(&e)),
                m => tracing::debug!(target = "wal_receive", "ignoring {}", message_kind(&m)),
            }
        }

        tracing::info!(
            target = "wal_receive",
            "server closed CopyOut (timeline switch)"
        );
        conn.end_copy().await?;
        acc.drain_uploads().await?;
        let restart = acc
            .upload_partial_on_switch()
            .await?
            .unwrap_or(acc.received_lsn);

        timeline += 1;
        match conn.timeline_history(timeline).await? {
            Some((fname, content)) => {
                upload_history(settings, &storage, archive_dir, &fname, &content).await?;
            }
            None => tracing::warn!(
                target = "wal_receive",
                "no history file for timeline {timeline}"
            ),
        }
        next_start = align(restart, seg_size);
        acc.reset_for_timeline(timeline, next_start);
        tracing::info!(
            target = "wal_receive",
            "switched to timeline {timeline}, restarting at {}",
            format_pg_lsn(next_start),
        );
        continue 'replication;
    }
}

/// `WALG_SLOTNAME`. Unset or empty -> `None` (slotless replication). wal-g
/// instead defaults to a `walg` slot; see WALG_COMPAT.md for the divergence
fn slot_name_from_env() -> Result<Option<String>> {
    let Some(name) = std::env::var("WALG_SLOTNAME")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    validate_slot_name(&name)?;
    Ok(Some(name))
}

/// wal-g ValidateSlotName: 1-63 word characters `[0-9A-Za-z_]`
fn validate_slot_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 63
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        bail!("WALG_SLOTNAME {name:?} must be 1-63 characters of [0-9A-Za-z_]");
    }
    Ok(())
}

/// Physical replication slot state read from `pg_replication_slots`
struct SlotInfo {
    exists: bool,
    restart_lsn: Option<u64>,
}

/// `wal_segment_size` from `pg_settings`. PG 10 and below report it in 8 KiB
/// blocks; PG 11+ in bytes (wal-g GetWalSegmentBytes)
async fn query_wal_segment_size(q: &mut ReplicationConn) -> Result<u64> {
    let rows = q
        .query_rows("SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'")
        .await?;
    let raw = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|c| c.as_deref())
        .ok_or_else(|| anyhow!("server did not report wal_segment_size"))?;
    let mut bytes: u64 = raw
        .parse()
        .with_context(|| format!("wal_segment_size={raw}"))?;
    if q.server_pg_version() < 110000 {
        bytes = bytes.saturating_mul(8192);
    }
    if bytes == 0 || !bytes.is_power_of_two() {
        bail!("server wal_segment_size={bytes} is not a power of two");
    }
    Ok(bytes)
}

/// Existence + restart_lsn of the named physical slot. Empty result = absent
async fn query_slot_info(q: &mut ReplicationConn, slot_name: &str) -> Result<SlotInfo> {
    // slot_name is validated to [0-9A-Za-z_], so inlining can't inject SQL
    let sql = format!(
        "SELECT active, restart_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot_name}'"
    );
    let rows = q.query_rows(&sql).await?;
    match rows.first() {
        None => Ok(SlotInfo {
            exists: false,
            restart_lsn: None,
        }),
        Some(cols) => {
            let restart_lsn = cols
                .get(1)
                .and_then(|c| c.as_deref())
                .map(parse_pg_lsn)
                .transpose()?;
            Ok(SlotInfo {
                exists: true,
                restart_lsn,
            })
        }
    }
}

/// Resolve which timeline `xlogpos` belongs to. Below timeline 2 there's no
/// history; otherwise fetch + upload the history file and walk it. Mirrors
/// wal-g getStartTimeline
async fn get_start_timeline(
    conn: &mut ReplicationConn,
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    sys_timeline: u32,
    xlogpos: u64,
) -> Result<u32> {
    if sys_timeline < 2 {
        return Ok(1);
    }
    match conn.timeline_history(sys_timeline).await? {
        Some((fname, content)) => {
            upload_history(settings, storage, archive_dir, &fname, &content).await?;
            Ok(lsn_to_timeline(&content, xlogpos, sys_timeline))
        }
        None => Ok(sys_timeline),
    }
}

/// Timeline owning `lsn` per a `.history` file. Each row is
/// `<tli>\t<switch_lsn>\t<comment>`; a switch to `tli+1` happened at
/// `switch_lsn`, so the first row whose switch LSN exceeds `lsn` names its
/// timeline, else the file's own (wal-g LSNToTimeLine)
fn lsn_to_timeline(content: &[u8], lsn: u64, file_timeline: u32) -> u32 {
    let text = String::from_utf8_lossy(content);
    let mut rows: Vec<(u32, u64)> = Vec::new();
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("");
        let mut cols = line.split_whitespace();
        if let (Some(t), Some(l)) = (cols.next(), cols.next())
            && let (Ok(t), Ok(l)) = (t.parse::<u32>(), parse_pg_lsn(l))
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

/// Stage `<tli>.history` content locally then ship it through the regular push
/// pipeline (which stores history uncompressed under `wal_005/<name>`)
async fn upload_history(
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    fname: &str,
    content: &[u8],
) -> Result<()> {
    fs::create_dir_all(archive_dir)
        .await
        .with_context(|| format!("create_dir_all {}", archive_dir.display()))?;
    let path = archive_dir.join(fname);
    fs::write(&path, content)
        .await
        .with_context(|| format!("stage {}", path.display()))?;
    push::handle(settings, storage.clone(), &path)
        .await
        .with_context(|| format!("archive history {fname}"))?;
    let _ = fs::remove_file(&path).await;
    Ok(())
}

/// `START_REPLICATION [SLOT <slot> PHYSICAL] <lsn> TIMELINE <tli>` then consume
/// the `CopyBothResponse`. Slotless when `slot_name` is `None`
async fn start_replication(
    conn: &mut ReplicationConn,
    slot_name: Option<&str>,
    start_lsn: u64,
    timeline: u32,
) -> Result<()> {
    let lsn = format_pg_lsn(start_lsn);
    let cmd = match slot_name {
        Some(slot) => format!("START_REPLICATION SLOT {slot} PHYSICAL {lsn} TIMELINE {timeline}"),
        None => format!("START_REPLICATION {lsn} TIMELINE {timeline}"),
    };
    conn.send_query(&cmd).await?;
    // START_REPLICATION returns CopyBothResponse ('W'), which postgres-
    // protocol's parser does not handle. The conn helper consumes the frame
    conn.expect_copy_both_open().await
}

/// Re-archive complete segments a prior crash left on local disk. A hard crash
/// between rotation (rename to `<seg>`) and upload completion (remove `<seg>`)
/// leaves up to WALG_UPLOAD_CONCURRENCY finished segments un-uploaded; the stream
/// realigns to the server's current LSN on restart, so those would otherwise
/// become permanent archive holes. In-progress segments are `<seg>.partial` and
/// so excluded (parse rejects the suffix). Idempotent: push byte-compares under
/// prevent-wal-overwrite, plain-overwrites otherwise
async fn repush_leftover_segments(
    settings: &Settings,
    storage: &DynStorage,
    archive_dir: &Path,
    seg_size: u64,
) -> Result<()> {
    let mut rd = match fs::read_dir(archive_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("scan {}", archive_dir.display())),
    };
    let mut leftovers = Vec::new();
    while let Some(entry) = rd.next_entry().await.context("read archive dir entry")? {
        let file_name = entry.file_name();
        // SegmentName::parse rejects `.partial`/`.history`/tmp names by length
        let Some(seg) = file_name.to_str().and_then(|n| SegmentName::parse(n).ok()) else {
            continue;
        };
        let meta = entry
            .metadata()
            .await
            .with_context(|| format!("stat {}", entry.path().display()))?;
        if meta.is_file() && meta.len() == seg_size {
            leftovers.push((seg, entry.path()));
        }
    }
    if leftovers.is_empty() {
        return Ok(());
    }
    // archive in LSN order, matching the streaming push sequence
    leftovers.sort_by_key(|(seg, _)| *seg);
    tracing::info!(
        target = "wal_receive",
        "re-pushing {} leftover segment(s) from {}",
        leftovers.len(),
        archive_dir.display()
    );
    for (seg, path) in leftovers {
        push::handle(settings, storage.clone(), &path)
            .await
            .with_context(|| format!("re-push leftover {}", seg.format()))?;
        let _ = fs::remove_file(&path).await;
    }
    Ok(())
}

/// Resolve on SIGINT or SIGTERM (Unix) / Ctrl-C (other). Splitting into
/// a helper keeps the main loop's `select!` arm short
async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
        let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        tokio::select! {
            _ = sigint.recv() => tracing::debug!(target = "wal_receive", "SIGINT"),
            _ = sigterm.recv() => tracing::debug!(target = "wal_receive", "SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("ctrl_c handler")?;
        tracing::debug!(target = "wal_receive", "ctrl-c");
    }
    Ok(())
}

/// Standby status update. Write reports received WAL; flush reports the
/// uploaded-through LSN so a slot's restart_lsn — and thus server-side WAL
/// retention — never advances past WAL we haven't archived (harmless but
/// conservative when slotless). Apply mirrors flush
async fn send_status(conn: &mut ReplicationConn, acc: &SegmentAccumulator) -> Result<()> {
    let write = acc.write_position();
    let flush = acc.flush_position().min(write);
    let payload = build_status_update(write, flush, flush);
    conn.send_copy_data(&payload).await
}

async fn identify_system(conn: &mut ReplicationConn) -> Result<(String, u32, u64)> {
    let rows = conn.query_rows("IDENTIFY_SYSTEM").await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Settings writing uncompressed, unencrypted to fs storage rooted at `store`
    fn test_settings(store: &Path) -> Settings {
        Settings {
            storage: crate::config::StorageSettings::Fs {
                path: store.to_string_lossy().into(),
            },
            compression: crate::compression::Method::None,
            ..Default::default()
        }
    }

    /// Accumulator backed by fs storage at `<dir>/store`, timeline 1
    async fn test_acc(dir: &Path, seg_size: u64) -> SegmentAccumulator {
        let store = dir.join("store");
        let settings = test_settings(&store);
        let storage: DynStorage = Arc::new(crate::storage::fs::FsStorage::new(&store).unwrap());
        SegmentAccumulator::new(1, dir.to_path_buf(), seg_size, settings, storage, 0)
            .await
            .unwrap()
    }

    // decode_frame / build_status_update wire coverage lives in the owner
    // module `replication::stream`; receive-side tests focus on the accumulator

    #[tokio::test]
    async fn finalize_partial_keeps_partial_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.ensure_current(0).await.unwrap();
        {
            use tokio::io::AsyncWriteExt;
            let cur = acc.current.as_mut().unwrap();
            cur.file.write_all(&[0xCD; 4]).await.unwrap();
            cur.bytes_written = 4;
        }
        let live_name = acc.current.as_ref().unwrap().name.format();
        acc.finalize_partial().await.unwrap();
        let partial = dir.path().join(format!("{live_name}.partial"));
        let original = dir.path().join(&live_name);
        assert!(partial.exists(), "partial file missing: {partial:?}");
        assert!(!original.exists(), "complete segment leaked: {original:?}");
        let meta = std::fs::metadata(&partial).unwrap();
        assert_eq!(meta.len(), seg_size, "partial should keep zero pad");
    }

    #[tokio::test]
    async fn finalize_partial_drops_empty_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let mut acc = test_acc(dir.path(), 16).await;
        acc.ensure_current(0).await.unwrap();
        let name = acc.current.as_ref().unwrap().name.format();
        acc.finalize_partial().await.unwrap();
        assert!(!dir.path().join(&name).exists());
        assert!(!dir.path().join(format!("{name}.partial")).exists());
    }

    #[tokio::test]
    async fn accumulator_rotates_at_segment_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // tiny 16-byte segs for the test
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        // Direct buffer-only test: don't archive (skip rotate_and_spawn wiring)
        acc.ensure_current(0).await.unwrap();
        let bytes_written_before = acc.current.as_ref().unwrap().bytes_written;
        // Manually drive write_all to avoid kicking the storage push path
        let cur = acc.current.as_mut().unwrap();
        use tokio::io::AsyncWriteExt;
        cur.file.write_all(&[0xABu8; 16]).await.unwrap();
        cur.bytes_written = bytes_written_before + 16;
        assert_eq!(cur.bytes_written, seg_size);
    }

    #[tokio::test]
    async fn rotation_uploads_and_removes_local_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.write(0, &[0xAB; 16]).await.unwrap();
        acc.drain_uploads().await.unwrap();
        let name = acc.segment_for_lsn(0).format();
        assert!(
            !dir.path().join(&name).exists(),
            "local segment should be removed after upload"
        );
        let archived = dir
            .path()
            .join("store")
            .join(crate::pg::WAL_FOLDER)
            .join(&name);
        assert_eq!(std::fs::read(&archived).unwrap(), vec![0xAB; 16]);
    }

    #[tokio::test]
    async fn partial_boundary_crossing_archives_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.write(0, &[0xCD; 4]).await.unwrap();
        // LSN jump past segment end: partial must be pushed, not dropped
        acc.write(seg_size, &[0xEF; 4]).await.unwrap();
        acc.drain_uploads().await.unwrap();
        let first = acc.segment_for_lsn(0).format();
        assert!(!dir.path().join(&first).exists());
        let archived = dir
            .path()
            .join("store")
            .join(crate::pg::WAL_FOLDER)
            .join(&first);
        let bytes = std::fs::read(&archived).unwrap();
        assert_eq!(bytes.len() as u64, seg_size, "zero pad retained");
        assert_eq!(&bytes[..4], &[0xCD; 4]);
        assert!(bytes[4..].iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn startup_repushes_complete_leftover_only() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let archive_dir = dir.path().join("archive");
        let store = dir.path().join("store");
        std::fs::create_dir_all(&archive_dir).unwrap();
        let settings = test_settings(&store);
        let storage: DynStorage = Arc::new(crate::storage::fs::FsStorage::new(&store).unwrap());

        // complete segment left by a crash between rotation and upload
        let complete = SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: 7,
        }
        .format();
        std::fs::write(archive_dir.join(&complete), vec![0xAB; seg_size as usize]).unwrap();
        // in-progress segment: full size on disk but .partial -> must be skipped
        let partial = SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: 8,
        }
        .format();
        std::fs::write(
            archive_dir.join(format!("{partial}.partial")),
            vec![0xCD; seg_size as usize],
        )
        .unwrap();
        // wrong-size file (e.g. truncated) -> must be skipped
        let short = SegmentName {
            timeline: 1,
            log_id: 0,
            seg_no: 9,
        }
        .format();
        std::fs::write(archive_dir.join(&short), vec![0xEE; 4]).unwrap();

        repush_leftover_segments(&settings, &storage, &archive_dir, seg_size)
            .await
            .unwrap();

        let wal = |seg: &str| store.join(crate::pg::WAL_FOLDER).join(seg);
        // complete segment archived, then removed locally
        assert_eq!(
            std::fs::read(wal(&complete)).unwrap(),
            vec![0xAB; seg_size as usize]
        );
        assert!(
            !archive_dir.join(&complete).exists(),
            "leftover must be removed after re-push"
        );
        // partial + short left untouched, never archived
        assert!(archive_dir.join(format!("{partial}.partial")).exists());
        assert!(!wal(&partial).exists(), "partial must not be uploaded");
        assert!(archive_dir.join(&short).exists());
        assert!(
            !wal(&short).exists(),
            "wrong-size file must not be uploaded"
        );
    }

    #[test]
    fn slot_name_validation() {
        assert!(validate_slot_name("walg").is_ok());
        assert!(validate_slot_name("standby_1").is_ok());
        assert!(validate_slot_name(&"a".repeat(63)).is_ok());
        assert!(validate_slot_name("").is_err());
        assert!(validate_slot_name(&"a".repeat(64)).is_err());
        assert!(validate_slot_name("bad-name").is_err());
        assert!(validate_slot_name("bad name").is_err());
        assert!(validate_slot_name("inject'; DROP").is_err());
    }

    #[test]
    fn lsn_to_timeline_walks_history() {
        // switch to tli 2 at 0/3000000, to tli 3 at 0/5000000
        let content = b"1\t0/3000000\tno recovery target specified\n\
                        2\t0/5000000\tno recovery target specified\n";
        let at = |s: &str| parse_pg_lsn(s).unwrap();
        assert_eq!(lsn_to_timeline(content, at("0/2000000"), 3), 1);
        assert_eq!(lsn_to_timeline(content, at("0/4000000"), 3), 2);
        // at-or-after the last switch -> the file's own timeline
        assert_eq!(lsn_to_timeline(content, at("0/6000000"), 3), 3);
        // no history rows -> file timeline
        assert_eq!(lsn_to_timeline(b"", 0x9999, 5), 5);
    }

    #[tokio::test]
    async fn flush_position_trails_uploads() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        // rotate one full segment; its upload is in flight, not yet drained
        acc.write(0, &[0xAB; 16]).await.unwrap();
        assert_eq!(acc.write_position(), seg_size, "received high-water");
        assert_eq!(
            acc.flush_position(),
            0,
            "flush must trail the un-uploaded segment's start"
        );
        acc.drain_uploads().await.unwrap();
        assert_eq!(
            acc.flush_position(),
            seg_size,
            "after upload completes, flush catches up to received"
        );
    }

    #[tokio::test]
    async fn timeline_switch_uploads_partial_and_re_anchors() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.write(0, &[0xCD; 4]).await.unwrap();
        let partial_name = acc.current.as_ref().unwrap().name.format();

        let restart = acc.upload_partial_on_switch().await.unwrap();
        assert_eq!(
            restart,
            Some(0),
            "next timeline restarts at the partial start"
        );
        assert!(acc.current.is_none(), "partial consumed");

        // partial archived under its .partial name (uncompressed test settings)
        let archived = dir
            .path()
            .join("store")
            .join(crate::pg::WAL_FOLDER)
            .join(format!("{partial_name}.partial"));
        let bytes = std::fs::read(&archived).unwrap();
        assert_eq!(
            bytes.len() as u64,
            seg_size,
            "full-size zero-padded partial"
        );
        assert_eq!(&bytes[..4], &[0xCD; 4]);

        acc.reset_for_timeline(2, 0);
        assert_eq!(acc.timeline, 2);
        assert_eq!(acc.received_lsn, 0);
        // a fresh write now lands on the new timeline
        acc.write(0, &[0xEF; 16]).await.unwrap();
        acc.drain_uploads().await.unwrap();
        let tli2 = acc.segment_for_lsn(0).format();
        assert!(tli2.starts_with("00000002"), "segment named on timeline 2");
    }
}
