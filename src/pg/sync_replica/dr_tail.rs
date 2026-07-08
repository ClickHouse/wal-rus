//! DR-tail S3 delivery — the receiver side of `POST /v1/dr-catchup`.
//!
//! On failover the CP asks the receiver to make its retained tail (completed
//! segments + the in-flight partial) durable in object storage so the promotion
//! candidate can fetch the gap with normal `wal-fetch`. We upload under each
//! full segment name into `<WALG_S3_PREFIX>/dr-tail/wal_005/`, compressed exactly
//! like an archive object, last-write-wins. The dr-tail lane is kept strictly
//! separate from the archive prefix so an uploaded partial can never masquerade
//! as a complete archive segment.
//!
//! **Durability invariant (the RPO=0 contract).** The returned gate must reflect
//! ONLY WAL the candidate can fetch+replay CONTIGUOUSLY. The candidate runs
//! `wal-fetch` per segment in order; a single missing/un-fetchable segment stalls
//! replay one short of the gate. So we upload the gate timeline's segments in
//! ascending order and report the end LSN of the LONGEST CONTIGUOUS run of
//! SUCCESSFULLY-uploaded segments — never past a hole or a failed PUT, never
//! above `toLsn`. The in-flight partial is shipped byte-for-byte (received bytes
//! plus the natural zero pad); we do NOT parse WAL to trim the torn tail — the
//! candidate's own recovery stops at the last CRC-valid record and forks there.
//!
//! Ported from wal-g `wal_receive_dr_s3.go`. The contiguity/gate logic is split
//! into pure helpers (`contiguous_run`, `seg_end`, `clamp_gate`) so it is unit
//! tested without S3.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;

use crate::compression;
use crate::config::Settings;
use crate::pg;
use crate::pg::wal::segment::{SegmentName, is_wal_filename};
use crate::storage::DynStorage;

/// The dedicated DR-tail prefix under `WALG_S3_PREFIX`.
const DR_TAIL_SUBDIR: &str = "dr-tail";

/// Everything the dr-catchup handler needs: the S3 lane, the compression/crypto
/// settings, and the segment size. Built only when DR-tail S3 is enabled.
pub(crate) struct DrTail {
    storage: DynStorage,
    settings: Settings,
    seg_size: u64,
}

/// A retained WAL file paired with its parsed name (completed `<seg>` or the
/// in-flight `<seg>.partial`).
#[derive(Clone, Debug)]
struct RetainedSeg {
    path: PathBuf,
    name: SegmentName,
}

impl DrTail {
    pub(crate) fn new(storage: DynStorage, settings: Settings, seg_size: u64) -> Self {
        Self {
            storage,
            settings,
            seg_size,
        }
    }

    /// Upload the retained tail in `(from_lsn, to_lsn]` and return
    /// `(objects_written, durable_contiguous_gate)`. The gate is 0 when nothing
    /// durable can be offered (no retained segment covers `to_lsn`, or a gap at
    /// the candidate's replay start `from_lsn`).
    pub(crate) async fn upload(
        &self,
        partial_dir: &Path,
        from_lsn: u64,
        to_lsn: u64,
    ) -> Result<(usize, u64)> {
        let all = list_retained(partial_dir, self.seg_size).context("list retained segments")?;
        if all.is_empty() {
            return Ok((0, 0));
        }

        // The gate lives on the in-flight segment's timeline. Restrict the walk to
        // THAT timeline: a candidate promoting on the new timeline replays it
        // contiguously, and mixing old-timeline partials (from a prior failover)
        // would fabricate a false gap or let a stale segment masquerade.
        let Some(gate_timeline) = timeline_containing(&all, to_lsn, self.seg_size) else {
            tracing::warn!(
                target = "sync_replica_api",
                "dr-tail: no retained segment contains gate {to_lsn:#x}; reporting empty gate"
            );
            return Ok((0, 0));
        };

        let on_timeline: Vec<RetainedSeg> = all
            .into_iter()
            .filter(|s| s.name.timeline == gate_timeline)
            .collect();

        // The contiguous run to upload, anchored at `from_lsn` (the candidate's
        // replay start — it already has `[0, from)` from S3). Anchoring drops
        // stale low cruft so an irrelevant hole far below the requested range
        // can't truncate the run short of the tail the candidate actually needs.
        let run = deliverable_run(&on_timeline, from_lsn, to_lsn, self.seg_size);
        let mut n = 0usize;
        let mut durable = 0u64;
        for s in &run {
            if let Err(e) = self.put_one(s).await {
                // A failed PUT breaks the durable run — every segment above it is
                // unreachable for the candidate. Stop; the gate stays at the last
                // success. The failed segment is retried on the next flush.
                tracing::warn!(
                    target = "sync_replica_api",
                    "dr-tail: put {} failed: {e:#}",
                    s.name.format()
                );
                break;
            }
            n += 1;
            durable = seg_end(s, to_lsn, self.seg_size);
        }
        let durable = clamp_gate(durable, to_lsn);
        tracing::info!(
            target = "sync_replica_api",
            "dr-tail: PUT {n} object(s) (timeline {gate_timeline:08X}, from {from_lsn:#x}, frontier {to_lsn:#x}, durable gate {durable:#x})"
        );
        Ok((n, durable))
    }

    /// Upload one retained segment byte-for-byte to `dr-tail/wal_005/<name>.<ext>`,
    /// compressed/encrypted exactly like a normal archive object (last-write-wins).
    async fn put_one(&self, s: &RetainedSeg) -> Result<()> {
        let method = self.settings.compression;
        let ext = method.extension();
        let seg = s.name.format();
        let key = if ext.is_empty() {
            format!("{DR_TAIL_SUBDIR}/{}/{seg}", pg::WAL_FOLDER)
        } else {
            format!("{DR_TAIL_SUBDIR}/{}/{seg}.{ext}", pg::WAL_FOLDER)
        };

        let meta = fs::metadata(&s.path)
            .await
            .with_context(|| format!("stat {}", s.path.display()))?;
        let size = meta.len();
        let file = fs::File::open(&s.path)
            .await
            .with_context(|| format!("open {}", s.path.display()))?;
        let reader: compression::AsyncReader = self.settings.throttle_disk(Box::pin(file));
        let compressed = compression::encode(method, reader, self.settings.compression_level);
        let encrypted = self.settings.encrypt(compressed);
        let body = self.settings.throttle_network(encrypted);

        // Size hint only when neither compression nor encryption varies length.
        let size_hint =
            if matches!(method, compression::Method::None) && self.settings.crypter.is_none() {
                Some(size)
            } else {
                None
            };
        self.storage
            .put(&key, body, size_hint)
            .await
            .with_context(|| format!("dr-tail put {key}"))?;
        Ok(())
    }
}

/// List retained WAL files in `dir`: completed bare `<seg>` files AND the
/// in-flight `<seg>.partial`. (wal-g globs only `.partial` because it retains
/// everything as `.partial`; walrus renames completed segments to bare names.)
fn list_retained(dir: &Path, _seg_size: u64) -> Result<Vec<RetainedSeg>> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read_dir {}", dir.display())),
    };
    let mut out = Vec::new();
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let fname = entry.file_name();
        let Some(fname) = fname.to_str() else {
            continue;
        };
        let seg = fname.strip_suffix(".partial").unwrap_or(fname);
        // bare or stripped-of-.partial must be a 24-hex WAL name
        if !is_wal_filename(seg) {
            continue;
        }
        let Ok(name) = SegmentName::parse(seg) else {
            continue;
        };
        out.push(RetainedSeg {
            path: entry.path(),
            name,
        });
    }
    Ok(out)
}

/// The timeline of the retained segment whose LSN range contains `lsn` (the
/// in-flight gate segment). `None` when no retained segment covers it.
fn timeline_containing(segs: &[RetainedSeg], lsn: u64, seg_size: u64) -> Option<u32> {
    segs.iter().find_map(|s| {
        let start = s.name.start_lsn(seg_size);
        (lsn >= start && lsn < start + seg_size).then_some(s.name.timeline)
    })
}

/// The run to upload for a `(from_lsn, to_lsn]` request: drop segments entirely
/// below `from_lsn`'s segment (the candidate already restored those from S3, and
/// stale low cruft from a prior failover must not anchor the run), then take the
/// maximal contiguous run from the lowest remaining segment up to the in-flight
/// segment.
///
/// The drop-below-`from` is what lets total-loss recover: a stale low segment
/// left from a prior failover would otherwise make `contiguous_run` stop at an
/// irrelevant hole far below the tail the candidate actually needs. We do NOT
/// additionally require the run to start exactly at `from`'s segment: when the
/// janitor has pruned `[from, lowest)` (it only prunes ARCHIVED segments), the
/// candidate bridges that gap from the S3 archive, so delivering `[lowest, to]`
/// is correct — and that is the common standby-behind shape (the lagging
/// candidate's `from` sits below `last_archived`, which the janitor pruned).
fn deliverable_run(
    segs: &[RetainedSeg],
    from_lsn: u64,
    to_lsn: u64,
    seg_size: u64,
) -> Vec<RetainedSeg> {
    let from_seg = from_lsn - (from_lsn % seg_size);
    let mut anchored: Vec<RetainedSeg> = segs
        .iter()
        .filter(|s| s.name.start_lsn(seg_size) >= from_seg)
        .cloned()
        .collect();
    anchored.sort_by_key(|s| s.name.start_lsn(seg_size));
    contiguous_run(&anchored, to_lsn, seg_size)
}

/// Maximal contiguous prefix of `segs` (sorted ascending, single timeline) to
/// upload: stops BEFORE the first hole, and ends AT the in-flight segment (the
/// one containing `to_lsn`) since nothing above it matters.
fn contiguous_run(segs: &[RetainedSeg], to_lsn: u64, seg_size: u64) -> Vec<RetainedSeg> {
    let mut out = Vec::new();
    let mut prev_global = 0u64;
    let mut have_prev = false;
    for s in segs {
        let global = s.name.start_lsn(seg_size) / seg_size;
        if have_prev && global != prev_global + 1 {
            break; // hole: every segment above is an orphan the candidate can't reach
        }
        out.push(s.clone());
        let start = s.name.start_lsn(seg_size);
        if to_lsn >= start && to_lsn < start + seg_size {
            break; // the in-flight (gate) segment tops the run
        }
        have_prev = true;
        prev_global = global;
    }
    out
}

/// Durable end LSN of a successfully-uploaded segment: the raw frontier `to_lsn`
/// for the in-flight segment, else the segment's end.
fn seg_end(s: &RetainedSeg, to_lsn: u64, seg_size: u64) -> u64 {
    let start = s.name.start_lsn(seg_size);
    if to_lsn >= start && to_lsn < start + seg_size {
        to_lsn
    } else {
        start + seg_size
    }
}

/// Cap a computed gate at the requested frontier (never report more than asked);
/// 0 (nothing durable) stays 0.
fn clamp_gate(gate: u64, to_lsn: u64) -> u64 {
    gate.min(to_lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEG: u64 = 16 * 1024 * 1024; // 16 MiB

    /// Build a RetainedSeg for timeline 1, global segment `g` (16 MiB segs).
    fn seg(g: u64) -> RetainedSeg {
        let log_id = (g * SEG) >> 32;
        let seg_no = ((g * SEG) & 0xFFFF_FFFF) / SEG;
        let name =
            SegmentName::parse(&format!("{:08X}{:08X}{:08X}", 1u32, log_id, seg_no)).unwrap();
        RetainedSeg {
            path: PathBuf::from(format!("/x/{}", name.format())),
            name,
        }
    }

    fn lsn_in(g: u64, off: u64) -> u64 {
        g * SEG + off
    }

    #[test]
    fn seg_start_lsn_roundtrips() {
        assert_eq!(seg(5).name.start_lsn(SEG), 5 * SEG);
        assert_eq!(seg(5).name.timeline, 1);
    }

    #[test]
    fn contiguous_run_includes_in_flight_and_stops_there() {
        let segs = [seg(3), seg(4), seg(5)];
        // to_lsn lands inside segment 5 → the in-flight segment tops the run
        let run = contiguous_run(&segs, lsn_in(5, 4096), SEG);
        assert_eq!(run.len(), 3);
        assert_eq!(run[2].name.start_lsn(SEG), 5 * SEG);
    }

    #[test]
    fn contiguous_run_breaks_at_a_hole_before_the_gate() {
        // 3,4 then a hole (no 5), gate is inside 6 → run stops at 4; 6 unreachable
        let segs = [seg(3), seg(4), seg(6)];
        let run = contiguous_run(&segs, lsn_in(6, 0), SEG);
        assert_eq!(run.len(), 2);
        assert_eq!(run.last().unwrap().name.start_lsn(SEG), 4 * SEG);
    }

    #[test]
    fn contiguous_run_completed_only_no_in_flight() {
        // gate beyond all retained segments → no in-flight; whole contiguous run
        let segs = [seg(3), seg(4)];
        let run = contiguous_run(&segs, lsn_in(9, 0), SEG);
        assert_eq!(run.len(), 2);
    }

    #[test]
    fn deliverable_run_anchors_at_from_dropping_stale_low_cruft() {
        // The total-loss shape: a stale low segment (3) left from a prior
        // failover, a pruned hole, then the real tail (10,11,12). from is in 10.
        // contiguous_run from the lowest (3) would stop at the hole → gate 4.
        // deliverable_run anchors at `from`, dropping 3, and delivers 10..=12.
        let segs = [seg(3), seg(10), seg(11), seg(12)];
        let run = deliverable_run(&segs, lsn_in(10, 0), lsn_in(12, 4096), SEG);
        let starts: Vec<u64> = run.iter().map(|s| s.name.start_lsn(SEG) / SEG).collect();
        assert_eq!(starts, vec![10, 11, 12]);
    }

    #[test]
    fn deliverable_run_delivers_above_from_when_from_was_pruned() {
        // standby-behind shape: the candidate's `from` (in 10) was archived and
        // pruned, so the lowest retained is 11. The candidate bridges [from, 11)
        // from the S3 archive, so we deliver the retained contiguous tail 11..=12
        // (the old gap-at-from guard wrongly returned empty here → RPO loss).
        let segs = [seg(11), seg(12)];
        let run = deliverable_run(&segs, lsn_in(10, 0), lsn_in(12, 0), SEG);
        let starts: Vec<u64> = run.iter().map(|s| s.name.start_lsn(SEG) / SEG).collect();
        assert_eq!(starts, vec![11, 12]);
    }

    #[test]
    fn deliverable_run_from_zero_is_unanchored() {
        // from = 0 (failover-primary flush / absent) → no anchor, behaves like
        // contiguous_run from the lowest segment.
        let segs = [seg(3), seg(4), seg(5)];
        let run = deliverable_run(&segs, 0, lsn_in(5, 4096), SEG);
        assert_eq!(run.len(), 3);
    }

    #[test]
    fn deliverable_run_stops_at_a_hole_within_the_needed_range() {
        // Anchored at from (in 10), but a hole at 11 → honest stop at 10; the
        // gate stays below `to` (RPO>0 reported truthfully, not over-claimed).
        let segs = [seg(10), seg(12)];
        let run = deliverable_run(&segs, lsn_in(10, 0), lsn_in(12, 4096), SEG);
        let starts: Vec<u64> = run.iter().map(|s| s.name.start_lsn(SEG) / SEG).collect();
        assert_eq!(starts, vec![10]);
    }

    #[test]
    fn seg_end_is_frontier_for_in_flight_else_segment_end() {
        // segment 4 holds the gate → end is the raw frontier
        assert_eq!(seg_end(&seg(4), lsn_in(4, 4096), SEG), lsn_in(4, 4096));
        // segment 3 is fully below the gate → end is its segment end
        assert_eq!(seg_end(&seg(3), lsn_in(4, 4096), SEG), 4 * SEG);
    }

    #[test]
    fn clamp_never_exceeds_frontier() {
        assert_eq!(clamp_gate(5 * SEG, lsn_in(4, 4096)), lsn_in(4, 4096));
        assert_eq!(clamp_gate(0, lsn_in(4, 4096)), 0);
        assert_eq!(clamp_gate(3 * SEG, 4 * SEG), 3 * SEG);
    }

    #[test]
    fn timeline_containing_finds_the_gate_segment() {
        let segs = [seg(3), seg(4), seg(5)];
        assert_eq!(timeline_containing(&segs, lsn_in(4, 10), SEG), Some(1));
        // beyond all retained → None
        assert_eq!(timeline_containing(&segs, lsn_in(99, 0), SEG), None);
    }

    #[test]
    fn list_retained_collects_bare_and_partial_skips_others() {
        let dir = tempfile::tempdir().unwrap();
        let bare = SegmentName::parse("000000010000000000000003")
            .unwrap()
            .format();
        let partial = SegmentName::parse("000000010000000000000004")
            .unwrap()
            .format();
        std::fs::write(dir.path().join(&bare), b"x").unwrap();
        std::fs::write(dir.path().join(format!("{partial}.partial")), b"x").unwrap();
        std::fs::write(dir.path().join("00000001.history"), b"x").unwrap();
        std::fs::write(dir.path().join("garbage.txt"), b"x").unwrap();

        let mut got = list_retained(dir.path(), SEG).unwrap();
        got.sort_by_key(|s| s.name.start_lsn(SEG));
        let names: Vec<String> = got.iter().map(|s| s.name.format()).collect();
        assert_eq!(names, vec![bare, partial]);
    }

    #[test]
    fn list_retained_missing_dir_is_empty() {
        let got = list_retained(Path::new("/no/such/dir/walrus-test"), SEG).unwrap();
        assert!(got.is_empty());
    }
}
