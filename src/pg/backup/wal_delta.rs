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
//! wal-rs records in a separate pass over the on-disk segment after upload
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
use crate::pg::backup::delta::DeltaFile;
use crate::pg::wal::segment::{SegmentName, wal_segment_size};
use crate::pg::walparser::{
    BlockLocation, WAL_PAGE_SIZE, WalParser, XLP_PAGE_MAGIC_PG14, extract_block_locations,
    parse_record_from_bytes,
};
use crate::storage::DynStorage;

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

async fn load_local_delta(folder: &Path, group_name: &str) -> Result<DeltaFile> {
    match tokio::fs::read(local_delta_path(folder, group_name)).await {
        Ok(buf) => Ok(DeltaFile::load(buf.as_slice()).context("decode local delta file")?),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(DeltaFile::new(WalParser::new())),
        Err(e) => Err(e).context("read local delta file"),
    }
}

async fn save_local_delta(folder: &Path, group_name: &str, delta: &DeltaFile) -> Result<()> {
    let mut buf = Vec::with_capacity(delta.serialized_len());
    delta.save(&mut buf).context("encode local delta file")?;
    tokio::fs::write(local_delta_path(folder, group_name), &buf)
        .await
        .context("write local delta file")
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

async fn upload_delta(
    settings: &Settings,
    storage: &DynStorage,
    group_name: &str,
    delta: &DeltaFile,
) -> Result<()> {
    let mut buf = Vec::with_capacity(delta.serialized_len());
    delta.save(&mut buf).context("encode delta sidecar")?;
    let method = settings.compression;
    let key = delta_storage_key(group_name, method);
    let reader: compression::AsyncReader = Box::pin(std::io::Cursor::new(buf));
    let compressed = compression::encode(method, reader, settings.compression_level);
    let body = settings.encrypt(compressed);
    storage
        .put(&key, body, None)
        .await
        .with_context(|| format!("put {key}"))?;
    tracing::info!(
        target = "wal_push",
        "uploaded delta sidecar {key} ({} location(s))",
        delta.locations.len()
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
    storage: &DynStorage,
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

    let mut delta = load_local_delta(&folder, &group_name).await?;
    delta.locations.extend(rec.locations);

    // Last segment of the group also seeds the next group's prev_head
    if pos == WAL_FILES_IN_DELTA as usize - 1 {
        let next_name = delta_group_name(seg.timeline, group_no + WAL_FILES_IN_DELTA, seg_size);
        let mut next_part = load_part(&folder, &next_name).await?;
        next_part.prev_head = Some(rec.trailing_head);
        next_part.prev_magic = Some(rec.page_magic);
        save_part(&folder, &next_name, &next_part).await?;
    }

    if part.is_complete() {
        delta.locations.extend(part.combine()?);
        // Resume point for the consumer's cross-group stitching
        let last_head = part.heads[WAL_FILES_IN_DELTA as usize - 1]
            .clone()
            .unwrap_or_default();
        delta.wal_parser = WalParser::from_current_record_head(last_head);
        upload_delta(settings, storage, &group_name, &delta).await?;
        remove_group_files(&folder, &group_name).await;
        tracing::debug!(target = "wal_push", "delta group {group_name} complete");
    } else {
        save_part(&folder, &group_name, &part).await?;
        save_local_delta(&folder, &group_name, &delta).await?;
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
        use crate::config::{DeltaSettings, StorageSettings};
        Settings {
            storage: StorageSettings::Fs {
                path: bucket.to_string_lossy().into_owned(),
            },
            compression: compression::Method::None,
            compression_level: 0,
            upload_concurrency: 1,
            upload_queue: 1,
            download_concurrency: 1,
            prevent_wal_overwrite: false,
            use_wal_delta: true,
            retry: crate::retry::RetryPolicy::default(),
            network_rate_limit: 0,
            disk_rate_limit: 0,
            delta: DeltaSettings::default(),
            crypter: None,
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
        use crate::storage::DynStorage;
        use crate::storage::fs::FsStorage;
        use std::sync::Arc;

        let seg_size = DEFAULT_WAL_SEG_SIZE;
        let n = WAL_FILES_IN_DELTA;
        let tmp = tempfile::tempdir().unwrap();
        let pg_wal = tmp.path().join("pg_wal");
        let bucket = tmp.path().join("bucket");
        std::fs::create_dir_all(&pg_wal).unwrap();
        std::fs::create_dir_all(&bucket).unwrap();
        let settings = test_settings(&bucket);
        let storage: DynStorage = Arc::new(FsStorage::new(&bucket).unwrap());

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
}
