//! WAL delta sidecar recording (`WALG_USE_WAL_DELTA`)
//!
//! As each WAL segment is archived, parse it and accumulate the set of changed
//! `(RelFileNode, block)` into a `<group>_delta` sidecar in `wal_005/`, one per
//! 16-segment group. backup-push's delta-map build then reads those sidecars
//! for whole groups instead of re-parsing every segment — O(touched relations)
//! instead of O(WAL volume). Mirrors wal-g's `delta_file_manager.go` +
//! `wal_delta_recording_reader.go`; the on-bucket `_delta` format is identical
//! (`DeltaFile::save`), so the two tools interoperate.
//!
//! Records straddling segment boundaries are stitched via `<group>_delta_part`
//! scratch files in `<pg_wal>/walg_data`. Part files never leave local disk, so
//! their layout is free to differ from wal-g — here it also carries the per-head
//! page magic so the boundary-record re-parse picks the right FPI bit layout.
//!
//! walrus records in a separate pass over the on-disk segment after upload
//! (wal-g tees the upload stream); the local file is page-cached so the reread
//! is cheap, and it keeps recording off the compress/encrypt/retry upload path.
//! Concurrent archives (daemon `WALG_UPLOAD_CONCURRENCY`) serialize on a
//! process mutex for the read-modify-write; cross-process correctness relies on
//! PG issuing `archive_command` serially, the same assumption wal-g makes.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow, bail};

use crate::compression;
use crate::config::Settings;
use crate::pg::WAL_FOLDER;
use crate::pg::wal::segment::{SegmentName, wal_segment_size};
use crate::pg::walparser::{
    BlockLocation, WAL_PAGE_SIZE, WalParser, XLP_PAGE_MAGIC_PG14, extract_block_locations,
    parse_record_from_bytes, write_location_tuples, write_locations_to,
};
use crate::storage::{ObjExt, Operator};

/// WAL segments per delta group (wal-g `WalFileInDelta`)
pub const WAL_FILES_IN_DELTA: u64 = 16;
const DELTA_SUFFIX: &str = "_delta";
const PART_SUFFIX: &str = "_part";
const DATA_FOLDER_NAME: &str = "walg_data";

// ─── segment-number / group-name helpers ────────────────────────────────────

/// Segments per log id: `2^32 / seg_size` (256 for the 16 MiB default)
fn segments_per_logid(seg_size: u64) -> u64 {
    0x1_0000_0000 / seg_size
}

/// Global segment number (`lsn / seg_size`) for a segment name
fn seg_global_no(name: &SegmentName, seg_size: u64) -> u64 {
    name.log_id as u64 * segments_per_logid(seg_size) + name.seg_no as u64
}

/// Reconstruct a `SegmentName` from a global segment number. Inverse of
/// [`seg_global_no`]; `log_id = no / per`, `seg_no = no % per`
pub(crate) fn seg_name_from_global(timeline: u32, no: u64, seg_size: u64) -> SegmentName {
    let per = segments_per_logid(seg_size);
    SegmentName {
        timeline,
        log_id: (no / per) as u32,
        seg_no: (no % per) as u32,
    }
}

/// Group-aligned segment number for the group a segment belongs to
pub(crate) fn delta_group_no(global: u64) -> u64 {
    global - global % WAL_FILES_IN_DELTA
}

/// `<group-first-segment-name>_delta` — the sidecar base name (no compression
/// extension). wal-g `GetDeltaFilenameFor`
pub(crate) fn delta_group_name(timeline: u32, group_no: u64, seg_size: u64) -> String {
    format!(
        "{}{DELTA_SUFFIX}",
        seg_name_from_global(timeline, group_no, seg_size).format()
    )
}

/// Storage key for a sidecar given the configured compression
pub(crate) fn delta_storage_key(group_name: &str, method: compression::Method) -> String {
    let ext = method.extension();
    if ext.is_empty() {
        format!("{WAL_FOLDER}/{group_name}")
    } else {
        format!("{WAL_FOLDER}/{group_name}.{ext}")
    }
}

// ─── per-segment parse ───────────────────────────────────────────────────────

/// What one segment contributes to its delta group
struct SegmentRecording {
    /// Tail of the record continuing from the previous segment (dropped by a
    /// fresh per-segment parser); becomes the group's `tails[pos]`
    leading_tail: Vec<u8>,
    /// Head of the record continuing into the next segment; becomes
    /// `heads[pos]` (and the next group's `prev_head` when pos is last)
    trailing_head: Vec<u8>,
    /// Page magic observed, for FPI layout when re-parsing the stitched record
    page_magic: u16,
    /// Block locations of records wholly inside this segment
    locations: Vec<BlockLocation>,
}

/// Parse one WAL segment with a fresh parser, matching wal-g's
/// `WalDeltaRecordingReader`: first productive page yields the leading tail,
/// in-segment records yield locations, EOF leaves the trailing head
fn parse_segment(bytes: &[u8]) -> Result<SegmentRecording> {
    let mut parser = WalParser::new();
    let mut leading_tail: Option<Vec<u8>> = None;
    let mut locations = Vec::new();
    let page = WAL_PAGE_SIZE as usize;
    let mut off = 0;
    while off < bytes.len() {
        let end = (off + page).min(bytes.len());
        let (tail, records) = parser
            .parse_records_from_page(&bytes[off..end])
            .context("parse wal page")?;
        if !tail.is_empty() || !records.is_empty() {
            if leading_tail.is_none() {
                leading_tail = Some(tail);
            } else if !tail.is_empty() {
                // wal-g cancels recording here (CantDiscardWalDataError)
                bail!("unexpected discarded record tail mid-segment");
            }
        }
        if !records.is_empty() {
            locations.extend(extract_block_locations(&records));
        }
        off = end;
    }
    Ok(SegmentRecording {
        leading_tail: leading_tail.unwrap_or_default(),
        trailing_head: parser.current_record_data().to_vec(),
        page_magic: parser.page_magic(),
        locations,
    })
}

// ─── part file (local-only boundary-record scratch) ─────────────────────────

const PART_MAGIC: u32 = 0x5741_4C50; // "WALP"
const PART_VERSION: u8 = 1;

/// Per-group stitching state for records crossing segment boundaries.
/// `tails[i]`/`heads[i]` are the leading/trailing fragments of segment i in the
/// group; `prev_head` is the trailing head of the previous group's last segment
#[derive(Debug, Clone)]
struct PartFile {
    tails: Vec<Option<Vec<u8>>>,
    prev_head: Option<Vec<u8>>,
    prev_magic: Option<u16>,
    heads: Vec<Option<Vec<u8>>>,
    head_magics: Vec<Option<u16>>,
}

impl PartFile {
    fn new() -> Self {
        let n = WAL_FILES_IN_DELTA as usize;
        Self {
            tails: vec![None; n],
            prev_head: None,
            prev_magic: None,
            heads: vec![None; n],
            head_magics: vec![None; n],
        }
    }

    /// All 16 tails + heads + the previous group's head recorded (wal-g
    /// `WalPartFile.IsComplete`)
    fn is_complete(&self) -> bool {
        self.prev_head.is_some()
            && self.tails.iter().all(Option::is_some)
            && self.heads.iter().all(Option::is_some)
    }

    /// Stitch each boundary-crossing record (`head[id-1] ++ tail[id]`, with
    /// `head[-1]` = `prev_head`) and extract its block locations. wal-g
    /// `WalPartFile.CombineRecords` + `ExtractBlockLocations`
    fn combine(&self) -> Result<Vec<BlockLocation>> {
        let n = WAL_FILES_IN_DELTA as usize;
        let mut out = Vec::new();
        for id in 0..n {
            let (head, magic) = if id == 0 {
                (self.prev_head.as_deref(), self.prev_magic)
            } else {
                (self.heads[id - 1].as_deref(), self.head_magics[id - 1])
            };
            let head = head.unwrap_or_default();
            let tail = self.tails[id].as_deref().unwrap_or_default();
            if head.is_empty() && tail.is_empty() {
                continue;
            }
            let mut data = Vec::with_capacity(head.len() + tail.len());
            data.extend_from_slice(head);
            data.extend_from_slice(tail);
            let rec = parse_record_from_bytes(&data, magic.unwrap_or(XLP_PAGE_MAGIC_PG14))
                .with_context(|| format!("parse stitched boundary record at position {id}"))?;
            out.extend(extract_block_locations(std::slice::from_ref(&rec)));
        }
        Ok(out)
    }

    /// Serialized byte length, for `Vec::with_capacity`. Must track `save`
    fn serialized_len(&self) -> usize {
        let opt_bytes = |b: &Option<Vec<u8>>| 1 + b.as_ref().map_or(0, |d| 4 + d.len());
        let opt_u16 = |v: &Option<u16>| if v.is_some() { 3 } else { 1 };
        4 + 1
            + self.tails.iter().map(opt_bytes).sum::<usize>()
            + opt_bytes(&self.prev_head)
            + opt_u16(&self.prev_magic)
            + self.heads.iter().map(opt_bytes).sum::<usize>()
            + self.head_magics.iter().map(opt_u16).sum::<usize>()
    }

    fn save<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&PART_MAGIC.to_le_bytes())?;
        w.write_all(&[PART_VERSION])?;
        for t in &self.tails {
            write_opt_bytes(&mut w, t.as_deref())?;
        }
        write_opt_bytes(&mut w, self.prev_head.as_deref())?;
        write_opt_u16(&mut w, self.prev_magic)?;
        for (h, m) in self.heads.iter().zip(&self.head_magics) {
            write_opt_bytes(&mut w, h.as_deref())?;
            write_opt_u16(&mut w, *m)?;
        }
        Ok(())
    }

    fn load<R: Read>(mut r: R) -> io::Result<Self> {
        let mut hdr = [0u8; 4];
        r.read_exact(&mut hdr)?;
        if u32::from_le_bytes(hdr) != PART_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad part magic"));
        }
        let mut ver = [0u8; 1];
        r.read_exact(&mut ver)?;
        if ver[0] != PART_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported part version",
            ));
        }
        let n = WAL_FILES_IN_DELTA as usize;
        let mut pf = PartFile::new();
        for slot in pf.tails.iter_mut() {
            *slot = read_opt_bytes(&mut r)?;
        }
        pf.prev_head = read_opt_bytes(&mut r)?;
        pf.prev_magic = read_opt_u16(&mut r)?;
        for i in 0..n {
            pf.heads[i] = read_opt_bytes(&mut r)?;
            pf.head_magics[i] = read_opt_u16(&mut r)?;
        }
        Ok(pf)
    }
}

fn write_opt_bytes<W: Write>(mut w: W, b: Option<&[u8]>) -> io::Result<()> {
    match b {
        Some(d) => {
            w.write_all(&[1])?;
            w.write_all(&(d.len() as u32).to_le_bytes())?;
            w.write_all(d)
        }
        None => w.write_all(&[0]),
    }
}

fn read_opt_bytes<R: Read>(mut r: R) -> io::Result<Option<Vec<u8>>> {
    let mut present = [0u8; 1];
    r.read_exact(&mut present)?;
    if present[0] == 0 {
        return Ok(None);
    }
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let mut data = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut data)?;
    Ok(Some(data))
}

fn write_opt_u16<W: Write>(mut w: W, v: Option<u16>) -> io::Result<()> {
    match v {
        Some(x) => {
            w.write_all(&[1])?;
            w.write_all(&x.to_le_bytes())
        }
        None => w.write_all(&[0]),
    }
}

fn read_opt_u16<R: Read>(mut r: R) -> io::Result<Option<u16>> {
    let mut present = [0u8; 1];
    r.read_exact(&mut present)?;
    if present[0] == 0 {
        return Ok(None);
    }
    let mut v = [0u8; 2];
    r.read_exact(&mut v)?;
    Ok(Some(u16::from_le_bytes(v)))
}

// ─── data-folder I/O ─────────────────────────────────────────────────────────

fn part_path(folder: &Path, group_name: &str) -> PathBuf {
    folder.join(format!("{group_name}{PART_SUFFIX}"))
}

fn local_delta_path(folder: &Path, group_name: &str) -> PathBuf {
    folder.join(group_name)
}

async fn load_part(folder: &Path, group_name: &str) -> Result<PartFile> {
    match tokio::fs::read(part_path(folder, group_name)).await {
        Ok(buf) => Ok(PartFile::load(buf.as_slice()).context("decode part file")?),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(PartFile::new()),
        Err(e) => Err(e).context("read part file"),
    }
}

async fn save_part(folder: &Path, group_name: &str, part: &PartFile) -> Result<()> {
    let mut buf = Vec::with_capacity(part.serialized_len());
    part.save(&mut buf).context("encode part file")?;
    tokio::fs::write(part_path(folder, group_name), &buf)
        .await
        .context("write part file")
}

/// Append a segment's location tuples to the running `<group>` working file.
/// Append-only: the file accumulates raw tuples across the group, so recording
/// holds at most one segment's locations rather than the whole group's
async fn append_local_delta(folder: &Path, group_name: &str, locs: &[BlockLocation]) -> Result<()> {
    if locs.is_empty() {
        return Ok(());
    }
    use tokio::io::AsyncWriteExt;
    let mut buf = Vec::with_capacity(locs.len() * 16);
    write_location_tuples(&mut buf, locs).context("encode location tuples")?;
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(local_delta_path(folder, group_name))
        .await
        .context("open local delta file")?;
    f.write_all(&buf).await.context("append local delta file")?;
    f.flush().await.context("flush local delta file")?;
    Ok(())
}

async fn remove_group_files(folder: &Path, group_name: &str) {
    for p in [
        part_path(folder, group_name),
        local_delta_path(folder, group_name),
    ] {
        if let Err(e) = tokio::fs::remove_file(&p).await
            && e.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(target = "wal_push", "remove {}: {e}", p.display());
        }
    }
}

/// Finalize the working file (append boundary-record tuples, the terminator,
/// then the parser seed) and stream it to the bucket compressed+encrypted.
/// Reads from the on-disk file rather than a buffer, so the group's locations
/// are never held in memory
async fn finalize_and_upload(
    settings: &Settings,
    storage: &Operator,
    folder: &Path,
    group_name: &str,
    combined: &[BlockLocation],
    parser: &WalParser,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    // tuples + all-zero terminator + parser state, mirroring wal-g's DeltaFile
    let mut tail = Vec::new();
    write_locations_to(&mut tail, combined).context("encode boundary tuples")?;
    parser.save(&mut tail).context("encode parser state")?;
    let path = local_delta_path(folder, group_name);
    {
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .context("open local delta file")?;
        f.write_all(&tail)
            .await
            .context("finalize local delta file")?;
        f.flush().await.context("flush local delta file")?;
    }

    let size = tokio::fs::metadata(&path)
        .await
        .context("stat local delta file")?
        .len();
    let file = tokio::fs::File::open(&path)
        .await
        .context("reopen local delta file")?;
    let method = settings.compression;
    let key = delta_storage_key(group_name, method);
    let reader: compression::AsyncReader = Box::pin(file);
    let compressed = compression::encode(method, reader, settings.compression_level);
    let body = settings.encrypt(compressed);
    storage
        .put(&key, body, None)
        .await
        .with_context(|| format!("put {key}"))?;
    tracing::info!(
        target = "wal_push",
        "uploaded delta sidecar {key} ({size} bytes)"
    );
    Ok(())
}

// ─── orchestration ───────────────────────────────────────────────────────────

/// Process-wide guard for the data-folder read-modify-write. Parsing happens
/// outside it; only the disk update + occasional sidecar upload serialize
fn record_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Record one archived WAL segment into its delta group, uploading the
/// `<group>_delta` sidecar once the group's 16 segments are all present.
/// Best-effort: callers log and swallow errors so a recording failure never
/// fails the WAL push (matching wal-g, where delta errors are warnings)
pub async fn record_segment(
    settings: &Settings,
    storage: &Operator,
    src_path: &Path,
    name: &str,
) -> Result<()> {
    let seg = SegmentName::parse(name).with_context(|| format!("parse segment name {name}"))?;
    let folder = src_path
        .parent()
        .ok_or_else(|| anyhow!("wal path has no parent: {}", src_path.display()))?
        .join(DATA_FOLDER_NAME);
    tokio::fs::create_dir_all(&folder)
        .await
        .with_context(|| format!("create data folder {}", folder.display()))?;

    let bytes = tokio::fs::read(src_path)
        .await
        .with_context(|| format!("read {}", src_path.display()))?;
    let rec = tokio::task::spawn_blocking(move || parse_segment(&bytes))
        .await
        .context("join segment parse")??;

    let seg_size = wal_segment_size();
    let global = seg_global_no(&seg, seg_size);
    let group_no = delta_group_no(global);
    let pos = (global % WAL_FILES_IN_DELTA) as usize;
    let group_name = delta_group_name(seg.timeline, group_no, seg_size);

    let _guard = record_lock().lock().await;

    let mut part = load_part(&folder, &group_name).await?;
    part.tails[pos] = Some(rec.leading_tail);
    part.heads[pos] = Some(rec.trailing_head.clone());
    part.head_magics[pos] = Some(rec.page_magic);

    append_local_delta(&folder, &group_name, &rec.locations).await?;

    // Last segment of the group also seeds the next group's prev_head
    if pos == WAL_FILES_IN_DELTA as usize - 1 {
        let next_name = delta_group_name(seg.timeline, group_no + WAL_FILES_IN_DELTA, seg_size);
        let mut next_part = load_part(&folder, &next_name).await?;
        next_part.prev_head = Some(rec.trailing_head);
        next_part.prev_magic = Some(rec.page_magic);
        save_part(&folder, &next_name, &next_part).await?;
    }

    if part.is_complete() {
        let combined = part.combine()?;
        // Resume point for the consumer's cross-group stitching
        let last_head = part.heads[WAL_FILES_IN_DELTA as usize - 1]
            .clone()
            .unwrap_or_default();
        let parser = WalParser::from_current_record_head(last_head);
        finalize_and_upload(settings, storage, &folder, &group_name, &combined, &parser).await?;
        remove_group_files(&folder, &group_name).await;
        tracing::debug!(target = "wal_push", "delta group {group_name} complete");
    } else {
        save_part(&folder, &group_name, &part).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;

    #[test]
    fn global_segno_round_trips_through_name() {
        let seg_size = DEFAULT_WAL_SEG_SIZE;
        for (timeline, log_id, seg_no) in [(1u32, 0u32, 5u32), (1, 3, 200), (2, 7, 255)] {
            let name = SegmentName {
                timeline,
                log_id,
                seg_no,
            };
            let global = seg_global_no(&name, seg_size);
            assert_eq!(seg_name_from_global(timeline, global, seg_size), name);
        }
    }

    #[test]
    fn global_segno_matches_lsn_over_seg_size() {
        let seg_size = DEFAULT_WAL_SEG_SIZE;
        // log_id=3, seg_no=200 → crosses the 4 GiB log boundary
        let name = SegmentName {
            timeline: 1,
            log_id: 3,
            seg_no: 200,
        };
        assert_eq!(
            seg_global_no(&name, seg_size),
            name.start_lsn(seg_size) / seg_size
        );
    }

    #[test]
    fn delta_group_alignment_and_position() {
        // global 0..15 → group 0; 16..31 → group 16
        assert_eq!(delta_group_no(0), 0);
        assert_eq!(delta_group_no(15), 0);
        assert_eq!(delta_group_no(16), 16);
        assert_eq!(delta_group_no(33), 32);
    }

    #[test]
    fn delta_group_name_is_first_segment_plus_suffix() {
        let seg_size = DEFAULT_WAL_SEG_SIZE;
        // group 16 on timeline 1 → first segment 00000001 00000000 00000010
        assert_eq!(
            delta_group_name(1, 16, seg_size),
            "000000010000000000000010_delta"
        );
        // group 256 → rolls into log_id 1, seg_no 0
        assert_eq!(
            delta_group_name(1, 256, seg_size),
            "000000010000000100000000_delta"
        );
    }

    #[test]
    fn part_file_round_trips() {
        let mut p = PartFile::new();
        p.tails[0] = Some(vec![1, 2, 3]);
        p.tails[1] = Some(vec![]);
        p.prev_head = Some(vec![9, 9]);
        p.prev_magic = Some(0xD10D);
        p.heads[0] = Some(vec![4, 5]);
        p.head_magics[0] = Some(0xD113);
        let mut buf = Vec::new();
        p.save(&mut buf).unwrap();
        let q = PartFile::load(buf.as_slice()).unwrap();
        assert_eq!(q.tails[0], Some(vec![1, 2, 3]));
        assert_eq!(q.tails[1], Some(vec![]));
        assert_eq!(q.tails[2], None);
        assert_eq!(q.prev_head, Some(vec![9, 9]));
        assert_eq!(q.prev_magic, Some(0xD10D));
        assert_eq!(q.heads[0], Some(vec![4, 5]));
        assert_eq!(q.head_magics[0], Some(0xD113));
    }

    #[test]
    fn is_complete_requires_all_slots() {
        let mut p = PartFile::new();
        assert!(!p.is_complete());
        for i in 0..WAL_FILES_IN_DELTA as usize {
            p.tails[i] = Some(vec![]);
            p.heads[i] = Some(vec![]);
        }
        assert!(!p.is_complete(), "prev_head still missing");
        p.prev_head = Some(vec![]);
        assert!(p.is_complete());
    }

    #[test]
    fn combine_skips_empty_and_parses_present() {
        // All-empty fragments → no records, no locations, no parse errors
        let mut p = PartFile::new();
        for i in 0..WAL_FILES_IN_DELTA as usize {
            p.tails[i] = Some(vec![]);
            p.heads[i] = Some(vec![]);
        }
        p.prev_head = Some(vec![]);
        assert!(p.combine().unwrap().is_empty());
    }

    /// 8 KiB WAL page holding one heap record that references a single block
    /// (`base/16384/16385` block `block`), rest zero-padded. No FPI, no
    /// cross-page tail, so a per-segment parser sees exactly one in-segment
    /// record and leaves no trailing head
    fn one_record_segment(block: u32) -> Vec<u8> {
        use crate::pg::walparser::{RmId, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER};
        let mut body = Vec::new();
        body.push(0u8); // block id 0
        body.push(0u8); // fork_flags: no data, no image
        body.extend_from_slice(&0u16.to_le_bytes()); // data_length
        body.extend_from_slice(&1663u32.to_le_bytes()); // spc
        body.extend_from_slice(&16384u32.to_le_bytes()); // db
        body.extend_from_slice(&16385u32.to_le_bytes()); // rel
        body.extend_from_slice(&block.to_le_bytes());

        let total = X_LOG_RECORD_HEADER_SIZE + body.len();
        let mut rec = Vec::new();
        rec.extend_from_slice(&(total as u32).to_le_bytes()); // total_record_length
        rec.extend_from_slice(&0u32.to_le_bytes()); // xact
        rec.extend_from_slice(&0u64.to_le_bytes()); // prev
        rec.push(0u8); // info
        rec.push(RmId::Heap as u8); // rmid
        rec.push(0u8); // pad
        rec.push(0u8); // pad
        rec.extend_from_slice(&0u32.to_le_bytes()); // crc
        rec.extend_from_slice(&body);

        let mut page = Vec::with_capacity(WAL_PAGE_SIZE as usize);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG14.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
        page.extend_from_slice(&[0u8; 4]); // align 36 → 40
        page.extend_from_slice(&rec);
        page.resize(WAL_PAGE_SIZE as usize, 0);
        page
    }

    fn test_settings(bucket: &Path) -> Settings {
        use crate::config::StorageSettings;
        Settings {
            storage: StorageSettings::Fs {
                path: bucket.to_string_lossy().into_owned(),
            },
            compression: compression::Method::None,
            use_wal_delta: true,
            ..Default::default()
        }
    }

    /// Record segments 15..=32: segment 15 seeds group 16's `prev_head`,
    /// segments 16..31 complete group 16 (sidecar uploaded), segment 32 is the
    /// tail in group 32. Only segment 32's raw WAL is uploaded, so a fallback to
    /// the raw-WAL walk could recover *only* block 1032 — recovering the full
    /// 1016..=1032 set proves the consumer read the group-16 sidecar
    #[tokio::test]
    async fn record_then_consume_uses_sidecar() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let pg_wal = tmp.path().join("pg_wal");
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&pg_wal).unwrap();
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        let tail = 2 * n; // 32: first segment of group 32
        for g in (n - 1)..=tail {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(1000 + g as u32);
            let path = pg_wal.join(&name);
            std::fs::write(&path, &bytes).unwrap();
            // Only the tail segment needs to be fetchable as raw WAL
            if g == tail {
                let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
                storage
                    .put(&format!("{WAL_FOLDER}/{name}"), r, None)
                    .await
                    .unwrap();
            }
            record_segment(&settings, &storage, &path, &name)
                .await
                .unwrap();
        }

        // Group 16 sidecar must have been uploaded once segment 31 completed it
        let group16 = delta_group_name(1, n, seg_size);
        assert!(
            storage
                .exists(&delta_storage_key(&group16, compression::Method::None))
                .await
                .unwrap(),
            "group 16 sidecar should exist"
        );

        let start_lsn = n * seg_size; // segment 16
        let end_lsn = tail * seg_size + 100; // inside segment 32
        let map = build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            start_lsn,
            end_lsn,
            compression::Method::None,
            None,
        )
        .await
        .unwrap();
        let got: std::collections::BTreeSet<u32> =
            map.locations().into_iter().map(|l| l.block_no).collect();
        // segs 16..31 via the sidecar + seg 32 via the tail walk; seg 15 is
        // before start_lsn and lives in group 0's (unread) sidecar
        let want: std::collections::BTreeSet<u32> =
            (1000 + n as u32..=1000 + tail as u32).collect();
        assert_eq!(got, want, "sidecar group + tail WAL must cover every block");
    }

    /// Unaligned start_lsn: the parent full begins mid-group (segment 20, in
    /// group 16), so group 16's sidecar would cover pre-start segments the full
    /// never archived and is never finalized. The consumer must raw-walk the
    /// leading partial [20, 31], fold the complete group-32 sidecar (segs
    /// 32..=47), then raw-walk the trailing segment 48 — recovering 1020..=1048.
    /// Before the leading-partial fix this errored on the absent group-16 sidecar
    /// and fell back to a raw walk of the (unfetchable) sidecar-only segments
    #[tokio::test]
    async fn unaligned_start_walks_leading_partial() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let pg_wal = tmp.path().join("pg_wal");
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&pg_wal).unwrap();
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        let start = n + 4; // 20: group 16, position 4
        let last_complete = 2 * n; // group 32
        let tail = 3 * n; // 48: trailing partial group

        // Leading partial [start, 31] + trailing seg 48 fetchable as raw WAL.
        // Group-32 segments are deliberately absent here so a fold of that
        // sidecar (not a raw fetch) is the only way to recover their blocks
        for g in (start..last_complete).chain(std::iter::once(tail)) {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(1000 + g as u32);
            let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
            storage
                .put(&format!("{WAL_FOLDER}/{name}"), r, None)
                .await
                .unwrap();
        }

        // Record 31..=47: seg 31 seeds group 32's prev_head, 32..=47 complete it.
        // Group 16 stays incomplete, so its sidecar is never uploaded
        for g in (last_complete - 1)..(last_complete + n) {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(1000 + g as u32);
            let path = pg_wal.join(&name);
            std::fs::write(&path, &bytes).unwrap();
            record_segment(&settings, &storage, &path, &name)
                .await
                .unwrap();
        }

        let none = compression::Method::None;
        assert!(
            !storage
                .exists(&delta_storage_key(&delta_group_name(1, n, seg_size), none))
                .await
                .unwrap(),
            "leading group 16 sidecar must be absent"
        );
        assert!(
            storage
                .exists(&delta_storage_key(
                    &delta_group_name(1, last_complete, seg_size),
                    none
                ))
                .await
                .unwrap(),
            "complete group 32 sidecar must exist"
        );

        let start_lsn = start * seg_size;
        let end_lsn = tail * seg_size + 100;
        let map = build_delta_map_from_wal(&settings, &storage, 1, start_lsn, end_lsn, none, None)
            .await
            .unwrap();
        let got: std::collections::BTreeSet<u32> =
            map.locations().into_iter().map(|l| l.block_no).collect();
        let want: std::collections::BTreeSet<u32> =
            (1000 + start as u32..=1000 + tail as u32).collect();
        assert_eq!(
            got, want,
            "leading raw walk + group-32 sidecar + trailing seg must cover every block"
        );
    }

    /// Aligned mid-stream start: recording begins exactly on group boundary 32,
    /// so group 32 fills all 16 positions yet never seeds prev_head (segment 31
    /// was never recorded) and its sidecar is never finalized. The consumer must
    /// raw-walk the sidecar-less group 32, fold the present group-48 sidecar, then
    /// raw-walk the trailing segment 64 — recovering 1032..=1064 — instead of
    /// failing the whole range to a full reparse on the absent group-32 sidecar
    #[tokio::test]
    async fn aligned_start_walks_missing_first_group() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let pg_wal = tmp.path().join("pg_wal");
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&pg_wal).unwrap();
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        let start = 2 * n; // 32: group boundary, recording starts here
        let second_group = 3 * n; // 48
        let tail = 4 * n; // 64: trailing partial

        // Record 32..=63 aligned: group 32 fills every position but never seeds
        // prev_head (segment 31 unrecorded) so no group-32 sidecar; segment 47
        // seeds group 48, which completes at segment 63
        for g in start..(start + 2 * n) {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(1000 + g as u32);
            let path = pg_wal.join(&name);
            std::fs::write(&path, &bytes).unwrap();
            record_segment(&settings, &storage, &path, &name)
                .await
                .unwrap();
        }

        // Raw WAL for the sidecar-less group 32 + the tail seg 64 fetchable; group
        // 48's raw segments deliberately absent so only its sidecar covers them
        for g in (start..second_group).chain(std::iter::once(tail)) {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(1000 + g as u32);
            let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
            storage
                .put(&format!("{WAL_FOLDER}/{name}"), r, None)
                .await
                .unwrap();
        }

        let none = compression::Method::None;
        assert!(
            !storage
                .exists(&delta_storage_key(
                    &delta_group_name(1, start, seg_size),
                    none
                ))
                .await
                .unwrap(),
            "aligned first group 32 sidecar must be absent"
        );
        assert!(
            storage
                .exists(&delta_storage_key(
                    &delta_group_name(1, second_group, seg_size),
                    none
                ))
                .await
                .unwrap(),
            "group 48 sidecar must exist"
        );

        let start_lsn = start * seg_size;
        let end_lsn = tail * seg_size + 100;
        let map = build_delta_map_from_wal(&settings, &storage, 1, start_lsn, end_lsn, none, None)
            .await
            .unwrap();
        let got: std::collections::BTreeSet<u32> =
            map.locations().into_iter().map(|l| l.block_no).collect();
        let want: std::collections::BTreeSet<u32> =
            (1000 + start as u32..=1000 + tail as u32).collect();
        assert_eq!(
            got, want,
            "raw-walked group 32 + group-48 sidecar + tail must cover every block"
        );
    }

    /// Multi-segment raw-WAL fallback: no sidecars, three fetchable segments in
    /// group 0 so `build_delta_map_from_wal` bails to the full walk. Exercises
    /// the fetch-vs-parse prefetch pipeline (segment N+1 fetched while N parses)
    /// and proves the changed-block set is the union across all segments
    #[tokio::test]
    async fn full_walk_pipelines_multiple_segments() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        // Segments 1..=3 (group 0), each touching a distinct block, all fetchable
        for g in 1u64..=3 {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(2000 + g as u32);
            let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
            storage
                .put(&format!("{WAL_FOLDER}/{name}"), r, None)
                .await
                .unwrap();
        }

        let start_lsn = seg_size; // segment 1
        let end_lsn = 3 * seg_size + 100; // inside segment 3
        let map = build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            start_lsn,
            end_lsn,
            compression::Method::None,
            None,
        )
        .await
        .unwrap();
        let got: std::collections::BTreeSet<u32> =
            map.locations().into_iter().map(|l| l.block_no).collect();
        let want: std::collections::BTreeSet<u32> = (2001..=2003).collect();
        assert_eq!(
            got, want,
            "full walk must union every segment's changed blocks"
        );
    }

    /// A missing segment in the required range is a hard error, never a silent
    /// skip: dropping segment 2 would omit its changed pages from the increment
    /// and restore stale parent data. Segments 1 and 3 present, 2 missing
    #[tokio::test]
    async fn full_walk_errors_on_missing_segment() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        for g in [1u64, 3] {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(2000 + g as u32);
            let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
            storage
                .put(&format!("{WAL_FOLDER}/{name}"), r, None)
                .await
                .unwrap();
        }

        build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            seg_size,
            3 * seg_size + 100,
            compression::Method::None,
            None,
        )
        .await
        .expect_err("missing required segment must error, not skip");
    }

    /// `wal_dir` makes the walk read raw segments from local `pg_wal` and fall
    /// back to the archive only for what is absent locally. Segment 1 lives only
    /// on disk, segments 2 and 3 only in the bucket: recovering 3001, 3002 and
    /// 3003 proves local-first read with archive fallback for the rest
    #[tokio::test]
    async fn full_walk_prefers_local_pg_wal() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let tmp = tempfile::tempdir().unwrap();
        let pg_wal = tmp.path().join("pg_wal");
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&pg_wal).unwrap();
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        // segment 1: local pg_wal only (uncompressed, raw segment name)
        let name1 = seg_name_from_global(1, 1, seg_size).format();
        std::fs::write(pg_wal.join(&name1), one_record_segment(3001)).unwrap();
        // segments 2,3: archive only — exercise the NotFound → archive fallback
        for g in [2u64, 3] {
            let name = seg_name_from_global(1, g, seg_size).format();
            let r: compression::AsyncReader =
                Box::pin(std::io::Cursor::new(one_record_segment(3000 + g as u32)));
            storage
                .put(&format!("{WAL_FOLDER}/{name}"), r, None)
                .await
                .unwrap();
        }

        let map = build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            seg_size,
            3 * seg_size + 100,
            compression::Method::None,
            Some(&pg_wal),
        )
        .await
        .unwrap();
        let got: std::collections::BTreeSet<u32> =
            map.locations().into_iter().map(|l| l.block_no).collect();
        let want: std::collections::BTreeSet<u32> = [3001, 3002, 3003].into_iter().collect();
        assert_eq!(
            got, want,
            "local segment + archive-fallback segments recovered"
        );
    }

    /// Build a `total`-byte heap record referencing `base/200/300` block 7
    /// (24 header + 20 block-0 header + 5 LONG main-data marker + main data)
    fn boundary_block_record(total: usize) -> Vec<u8> {
        use crate::pg::walparser::{RmId, X_LOG_RECORD_HEADER_SIZE, XLR_BLOCK_ID_DATA_LONG};
        let main_len = total - X_LOG_RECORD_HEADER_SIZE - 20 - 5;
        let mut r = Vec::new();
        r.extend_from_slice(&(total as u32).to_le_bytes());
        r.extend_from_slice(&0u32.to_le_bytes()); // xact
        r.extend_from_slice(&0u64.to_le_bytes()); // prev
        r.push(0u8); // info
        r.push(RmId::Heap as u8);
        r.push(0);
        r.push(0);
        r.extend_from_slice(&0u32.to_le_bytes()); // crc
        r.push(0u8); // block id 0
        r.push(0u8); // fork_flags: no image, no data
        r.extend_from_slice(&0u16.to_le_bytes()); // data_length
        r.extend_from_slice(&1663u32.to_le_bytes()); // spc
        r.extend_from_slice(&200u32.to_le_bytes()); // db
        r.extend_from_slice(&300u32.to_le_bytes()); // rel
        r.extend_from_slice(&7u32.to_le_bytes()); // block_no
        r.push(XLR_BLOCK_ID_DATA_LONG);
        r.extend_from_slice(&(main_len as u32).to_le_bytes());
        r.extend_from_slice(&vec![0x5Au8; main_len]);
        assert_eq!(r.len(), total);
        r
    }

    /// Long-header page (36 B header + 4 B align) holding `body` bytes, zero-padded
    fn long_header_page(body: &[u8]) -> Vec<u8> {
        use crate::pg::walparser::XLP_LONG_HEADER;
        let mut page = Vec::with_capacity(WAL_PAGE_SIZE as usize);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG14.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len (no continuation)
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
        page.extend_from_slice(&[0u8; 4]); // align 36 → 40
        page.extend_from_slice(body);
        page.resize(WAL_PAGE_SIZE as usize, 0);
        page
    }

    /// Short-header continuation page (20 B header + 4 B align) carrying `rem`
    /// bytes of remaining-data length and `body` bytes, zero-padded
    fn cont_header_page(rem: u32, body: &[u8]) -> Vec<u8> {
        use crate::pg::walparser::XLP_FIRST_IS_CONT_RECORD;
        let mut page = Vec::with_capacity(WAL_PAGE_SIZE as usize);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG14.to_le_bytes());
        page.extend_from_slice(&XLP_FIRST_IS_CONT_RECORD.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&(WAL_PAGE_SIZE as u64).to_le_bytes()); // page_address
        page.extend_from_slice(&rem.to_le_bytes());
        page.extend_from_slice(&[0u8; 4]); // align 20 → 24
        page.extend_from_slice(body);
        page.resize(WAL_PAGE_SIZE as usize, 0);
        page
    }

    async fn put_segment(storage: &Operator, seg: u64, bytes: Vec<u8>) {
        let name = seg_name_from_global(1, seg, wal_segment_size()).format();
        let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(bytes));
        storage
            .put(&format!("{WAL_FOLDER}/{name}"), r, None)
            .await
            .unwrap();
    }

    /// End-to-end parallel full walk over a record whose body spans the seg 1 /
    /// seg 2 boundary: seg 1 holds the head, seg 2 the tail. The boundary stitch
    /// must recover block 7, which neither segment's in-segment parse sees alone
    #[tokio::test]
    async fn full_walk_stitches_boundary_record() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        let total = 9049;
        let split = 8152; // bytes on seg 1 after long header + align
        let record = boundary_block_record(total);
        put_segment(&storage, 1, long_header_page(&record[..split])).await;
        put_segment(
            &storage,
            2,
            cont_header_page((total - split) as u32, &record[split..]),
        )
        .await;

        let map = build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            seg_size,
            2 * seg_size + 100,
            compression::Method::None,
            None,
        )
        .await
        .unwrap();
        let got: Vec<u32> = map.locations().into_iter().map(|l| l.block_no).collect();
        assert_eq!(got, vec![7], "boundary record's block recovered via stitch");
    }

    /// A record longer than a segment (head in seg 1, all of seg 2 its middle,
    /// tail in seg 3) can't be reconstructed pairwise; the parallel walk detects
    /// the fully-middle seg 2 and falls back to the serial threaded walk, which
    /// still recovers block 7
    #[tokio::test]
    async fn full_walk_falls_back_on_multi_segment_record() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        let head = 8152; // bytes on seg 1 (long header + align)
        let mid = 8168; // bytes on seg 2 (short header + align), fully inside record
        let tail = 500;
        let total = head + mid + tail;
        let record = boundary_block_record(total);
        put_segment(&storage, 1, long_header_page(&record[..head])).await;
        put_segment(
            &storage,
            2,
            cont_header_page((total - head) as u32, &record[head..head + mid]),
        )
        .await;
        put_segment(
            &storage,
            3,
            cont_header_page((total - head - mid) as u32, &record[head + mid..]),
        )
        .await;

        let map = build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            seg_size,
            3 * seg_size + 100,
            compression::Method::None,
            None,
        )
        .await
        .unwrap();
        let got: Vec<u32> = map.locations().into_iter().map(|l| l.block_no).collect();
        assert_eq!(got, vec![7], "serial fallback recovers the oversize record");
    }

    /// A record straddling the seg 1 / seg 2 boundary leaves seg 1 with a
    /// trailing head only seg 2 can complete. With seg 2 absent the parallel
    /// walk must error, never return a map missing the stitched block — a
    /// partial increment would restore stale parent data for that page
    #[tokio::test]
    async fn parallel_parse_missing_boundary_neighbor_errors() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        // seg 1 holds only the head of a boundary-spanning record (trailing head
        // non-empty); seg 2, carrying the tail, is never uploaded
        let total = 9049;
        let split = 8152; // bytes on seg 1 after long header + align
        let record = boundary_block_record(total);
        put_segment(&storage, 1, long_header_page(&record[..split])).await;

        build_delta_map_from_wal(
            &settings,
            &storage,
            1,
            seg_size,
            2 * seg_size + 100,
            compression::Method::None,
            None,
        )
        .await
        .expect_err("absent boundary neighbor must error, not yield a partial map");
    }

    /// A complete group's sidecar truncated mid-stream (no terminator, eg an
    /// interrupted upload) must never fold as a partial map: the consumer errors
    /// the sidecar path and re-walks the group's raw WAL, recovering exactly the
    /// real blocks. The truncated sidecar's stray tuple must not leak through
    #[tokio::test]
    async fn truncated_sidecar_falls_back_to_raw_walk() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);
        let none = compression::Method::None;

        // group 16's 16 raw segments fetchable for the fallback walk
        for g in n..(2 * n) {
            put_segment(&storage, g, one_record_segment(1000 + g as u32)).await;
        }

        // Truncated group-16 sidecar: a lone tuple (block 9999), no terminator
        // or parser state — mimics a finalize cut short
        let group16 = delta_group_name(1, n, seg_size);
        let mut raw = Vec::new();
        write_location_tuples(&mut raw, &[BlockLocation::new(1663, 16384, 16385, 9999)]).unwrap();
        let key = delta_storage_key(&group16, none);
        let len = raw.len() as u64;
        let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(raw));
        storage.put(&key, r, Some(len)).await.unwrap();

        let start_lsn = n * seg_size; // seg 16, group-aligned
        let end_lsn = 2 * n * seg_size; // seg 32 exclusive → no trailing group
        let map = build_delta_map_from_wal(&settings, &storage, 1, start_lsn, end_lsn, none, None)
            .await
            .unwrap();
        let got: std::collections::BTreeSet<u32> =
            map.locations().into_iter().map(|l| l.block_no).collect();
        // raw walk of segs 16..=31 → blocks 1016..=1031; the truncated sidecar's
        // 9999 is discarded, not folded
        let want: std::collections::BTreeSet<u32> = (1016..=1031).collect();
        assert_eq!(got, want, "fallback raw walk recovers real blocks only");
        assert!(
            !got.contains(&9999),
            "truncated sidecar tuple must not leak"
        );
    }

    /// Companion to [`aligned_start_walks_missing_first_group`]: when the
    /// sidecar-less aligned first group also has no fetchable raw WAL, the build
    /// must error rather than silently drop that group's changed blocks. Records
    /// segs 32..=63 (group 48 finalizes, group 32 never does) but uploads no raw
    /// segments, so group 32 is recoverable by neither sidecar nor raw walk
    #[tokio::test]
    async fn aligned_first_group_missing_sidecar_and_raw_errors() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let pg_wal = tmp.path().join("pg_wal");
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&pg_wal).unwrap();
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);

        let start = 2 * n; // 32: group boundary, recording starts here
        let second_group = 3 * n; // 48

        // Record 32..=63 aligned: group 32 fills every position but never seeds
        // prev_head (segment 31 unrecorded) so no group-32 sidecar; group 48
        // completes. Recording only writes local scratch + the finalized
        // sidecar — no raw segments reach the bucket
        for g in start..(start + 2 * n) {
            let name = seg_name_from_global(1, g, seg_size).format();
            let bytes = one_record_segment(1000 + g as u32);
            let path = pg_wal.join(&name);
            std::fs::write(&path, &bytes).unwrap();
            record_segment(&settings, &storage, &path, &name)
                .await
                .unwrap();
        }

        let none = compression::Method::None;
        assert!(
            !storage
                .exists(&delta_storage_key(
                    &delta_group_name(1, start, seg_size),
                    none
                ))
                .await
                .unwrap(),
            "aligned first group 32 sidecar must be absent"
        );
        assert!(
            storage
                .exists(&delta_storage_key(
                    &delta_group_name(1, second_group, seg_size),
                    none
                ))
                .await
                .unwrap(),
            "group 48 sidecar must exist"
        );

        let start_lsn = start * seg_size;
        let end_lsn = 4 * n * seg_size + 100; // through seg 64
        build_delta_map_from_wal(&settings, &storage, 1, start_lsn, end_lsn, none, None)
            .await
            .expect_err("sidecar-less group with no raw WAL must error, not drop blocks");
    }

    /// Corrupt sidecar with no fallback WAL. A truncated group-16 sidecar forces
    /// the raw-WAL fallback (see [`truncated_sidecar_falls_back_to_raw_walk`] for
    /// the success case), but group 16's raw segments are absent from the archive
    /// too, so the build must error rather than fold the corrupt sidecar's partial
    /// map — a partial increment would restore stale parent data
    #[tokio::test]
    async fn corrupt_sidecar_without_raw_wal_errors() {
        use crate::pg::backup::delta::build_delta_map_from_wal;
        use crate::storage::Operator;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: Operator = crate::storage::fs_operator(&bucket);
        let none = compression::Method::None;

        // Truncated group-16 sidecar: lone tuple, no terminator. No raw segments
        // uploaded, so the fallback walk has nothing to read
        let group16 = delta_group_name(1, n, seg_size);
        let mut raw = Vec::new();
        write_location_tuples(&mut raw, &[BlockLocation::new(1663, 16384, 16385, 9999)]).unwrap();
        let key = delta_storage_key(&group16, none);
        let len = raw.len() as u64;
        let r: compression::AsyncReader = Box::pin(std::io::Cursor::new(raw));
        storage.put(&key, r, Some(len)).await.unwrap();

        let start_lsn = n * seg_size; // seg 16, group-aligned
        let end_lsn = 2 * n * seg_size; // seg 32 exclusive → only group 16
        build_delta_map_from_wal(&settings, &storage, 1, start_lsn, end_lsn, none, None)
            .await
            .expect_err("corrupt sidecar with no raw WAL must error, not fold a partial map");
    }
}
