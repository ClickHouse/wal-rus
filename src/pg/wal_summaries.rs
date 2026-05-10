//! PG17 WAL summary file reader (`pg_wal/summaries/<...>.summary`)
//!
//! When `summarize_wal=on`, the postgres walsummarizer writes per-WAL-range
//! BlockRefTable files. This module parses them & projects MAIN_FORKNUM
//! entries into a `PagedFileDeltaMap` so backup-push can build a delta map
//! without re-parsing WAL itself
//!
//! Wire format (postgres `src/common/blkreftable.c`):
//! 1. `u32 magic = 0x652b137b`
//! 2. repeated entries:
//!    - 24-byte `BlockRefTableSerializedEntry` (`{spcOid, dbOid, relNumber}`,
//!      forknum i32, limit_block u32, nchunks u32)
//!    - `nchunks * u16` chunk-usage array
//!    - per-chunk payload: array-of-u16 offsets if `used < MAX_ENTRIES_PER_CHUNK`,
//!      otherwise bitmap of `MAX_ENTRIES_PER_CHUNK` u16 words (8 KiB)
//! 3. 24 zero bytes (sentinel)
//! 4. `u32` CRC32C-Castagnoli over everything above
//!
//! Filename: `%08X%08X%08X%08X%08X.summary` where the fields are timeline,
//! start LSN high, start LSN low, end LSN high, end LSN low (per
//! `src/backend/postmaster/walsummarizer.c:1205`). 40 hex chars total

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::pg::backup::delta::PagedFileDeltaMap;
use crate::pg::walparser::RelFileNode;

pub const SUMMARIES_DIR: &str = "pg_wal/summaries";

const BLOCK_REF_TABLE_MAGIC: u32 = 0x652b137b;
const BLOCKS_PER_CHUNK: u32 = 1 << 16;
/// `BITS_PER_BYTE * sizeof(uint16)` in postgres
const BLOCKS_PER_ENTRY: u32 = 16;
const MAX_ENTRIES_PER_CHUNK: u32 = BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY; // 4096
const SERIALIZED_ENTRY_SIZE: usize = 24;
const INVALID_BLOCK_NUMBER: u32 = 0xFFFF_FFFF;
const MAIN_FORK_NUM: i32 = 0;

#[derive(Debug, Error)]
pub enum SummaryError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("wrong magic: expected {expected:#x}, got {got:#x}")]
    BadMagic { expected: u32, got: u32 },
    #[error("CRC mismatch: expected {expected:08X}, got {got:08X}")]
    BadCrc { expected: u32, got: u32 },
    #[error("empty LSN range [{start:X}, {end:X})")]
    EmptyRange { start: u64, end: u64 },
    #[error(
        "no WAL summaries cover [{start:X}, {end:X}) on timeline {timeline} \
        (enable summarize_wal and retain summaries for the full range)"
    )]
    NoSummariesForRange { start: u64, end: u64, timeline: u32 },
    #[error("WAL summary gap at start: first summary begins at {first:X}, need {need:X}")]
    GapAtStart { first: u64, need: u64 },
    #[error("WAL summary gap between {a_end:X} and {b_start:X}")]
    GapInside { a_end: u64, b_start: u64 },
    #[error("WAL summary gap at end: last summary ends at {last:X}, need {need:X}")]
    GapAtEnd { last: u64, need: u64 },
}

/// One on-disk summary file, decoded from its filename.
/// `start_lsn` is inclusive, `end_lsn` is exclusive (matches postgres)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryFile {
    pub path: PathBuf,
    pub timeline: u32,
    pub start_lsn: u64,
    pub end_lsn: u64,
}

/// Top-level: walk `$pgdata/pg_wal/summaries`, pick the files covering
/// `[first_used_lsn, first_not_used_lsn)` on `timeline`, verify contiguous
/// coverage, parse them chronologically, return main-fork blocks aggregated
/// into a `PagedFileDeltaMap`
pub fn read_for_range(
    pg_data_dir: &Path,
    timeline: u32,
    first_used_lsn: u64,
    first_not_used_lsn: u64,
) -> Result<PagedFileDeltaMap, SummaryError> {
    let dir = pg_data_dir.join(SUMMARIES_DIR);
    let files = list_summary_files(&dir)?;
    let selected = select_for_range(&files, timeline, first_used_lsn, first_not_used_lsn)?;
    let mut state: BTreeMap<RelForkKey, RelForkState> = BTreeMap::new();
    for f in &selected {
        tracing::info!(
            target = "backup_push",
            "reading WAL summary {}",
            f.path.display()
        );
        parse_summary_file(&f.path, &mut state)?;
    }
    let mut delta = PagedFileDeltaMap::new();
    for (key, st) in state {
        if key.fork_num != MAIN_FORK_NUM {
            continue;
        }
        for block in st.blocks {
            delta.add_location(crate::pg::walparser::BlockLocation {
                rel: key.rel,
                block_no: block,
            });
        }
    }
    Ok(delta)
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct RelForkKey {
    rel: RelFileNode,
    fork_num: i32,
}

#[derive(Debug, Default)]
struct RelForkState {
    /// Set of changed block numbers in this rel/fork. Ranges past the
    /// observed limit_block get pruned via `prune_above`
    blocks: std::collections::BTreeSet<u32>,
}

impl RelForkState {
    /// wal-g semantics: remove everything `>= limit`. BTreeSet::split_off
    /// returns the upper portion `[limit..)`, discarding it via `_`
    fn prune_above(&mut self, limit: u32) {
        let _ = self.blocks.split_off(&limit);
    }
}

pub fn list_summary_files(dir: &Path) -> Result<Vec<SummaryFile>, SummaryError> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(SummaryError::Io(e)),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(mut f) = parse_summary_filename(&name_str) {
            f.path = entry.path();
            out.push(f);
        }
    }
    Ok(out)
}

/// Parse `<8h><8h><8h><8h><8h>.summary` into `SummaryFile` (path left
/// empty; caller fills). Returns `None` if the name doesn't match
pub fn parse_summary_filename(name: &str) -> Option<SummaryFile> {
    let stem = name.strip_suffix(".summary")?;
    if stem.len() != 40 {
        return None;
    }
    if !stem.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let timeline = u32::from_str_radix(&stem[0..8], 16).ok()?;
    let start_hi = u32::from_str_radix(&stem[8..16], 16).ok()?;
    let start_lo = u32::from_str_radix(&stem[16..24], 16).ok()?;
    let end_hi = u32::from_str_radix(&stem[24..32], 16).ok()?;
    let end_lo = u32::from_str_radix(&stem[32..40], 16).ok()?;
    Some(SummaryFile {
        path: PathBuf::new(),
        timeline,
        start_lsn: ((start_hi as u64) << 32) | (start_lo as u64),
        end_lsn: ((end_hi as u64) << 32) | (end_lo as u64),
    })
}

/// Pick the subset of `files` overlapping `[first_used, first_not_used)`
/// on the given timeline, then assert contiguous coverage. Errors out on
/// any gap (covers wal-g semantics: a delta with missing WAL summaries
/// must NOT silently produce an incorrect delta)
pub fn select_for_range(
    files: &[SummaryFile],
    timeline: u32,
    first_used_lsn: u64,
    first_not_used_lsn: u64,
) -> Result<Vec<SummaryFile>, SummaryError> {
    if first_not_used_lsn <= first_used_lsn {
        return Err(SummaryError::EmptyRange {
            start: first_used_lsn,
            end: first_not_used_lsn,
        });
    }
    let mut kept: Vec<SummaryFile> = files
        .iter()
        .filter(|f| f.timeline == timeline)
        .filter(|f| f.end_lsn > first_used_lsn && f.start_lsn < first_not_used_lsn)
        .cloned()
        .collect();
    kept.sort_by_key(|f| f.start_lsn);
    if kept.is_empty() {
        return Err(SummaryError::NoSummariesForRange {
            start: first_used_lsn,
            end: first_not_used_lsn,
            timeline,
        });
    }
    if kept[0].start_lsn > first_used_lsn {
        return Err(SummaryError::GapAtStart {
            first: kept[0].start_lsn,
            need: first_used_lsn,
        });
    }
    for w in kept.windows(2) {
        if w[1].start_lsn > w[0].end_lsn {
            return Err(SummaryError::GapInside {
                a_end: w[0].end_lsn,
                b_start: w[1].start_lsn,
            });
        }
    }
    let last = kept.last().unwrap();
    if last.end_lsn < first_not_used_lsn {
        return Err(SummaryError::GapAtEnd {
            last: last.end_lsn,
            need: first_not_used_lsn,
        });
    }
    Ok(kept)
}

/// Stream one summary file, fold its entries into `state`. Reads through a
/// `Crc32cHasher` so the trailing 4-byte CRC can be verified against
/// everything-up-to-the-CRC. Order matters: process summaries
/// chronologically so `limit_block` truncations apply to already-accumulated
/// blocks
fn parse_summary_file(
    path: &Path,
    state: &mut BTreeMap<RelForkKey, RelForkState>,
) -> Result<(), SummaryError> {
    let mut f = File::open(path)?;
    let mut hasher = Crc32cHasher::new();

    let mut magic_buf = [0u8; 4];
    read_full(&mut f, &mut magic_buf, &mut hasher)?;
    let magic = u32::from_le_bytes(magic_buf);
    if magic != BLOCK_REF_TABLE_MAGIC {
        return Err(SummaryError::BadMagic {
            expected: BLOCK_REF_TABLE_MAGIC,
            got: magic,
        });
    }

    let mut entry_buf = [0u8; SERIALIZED_ENTRY_SIZE];
    loop {
        read_full(&mut f, &mut entry_buf, &mut hasher)?;
        if entry_buf.iter().all(|&b| b == 0) {
            // sentinel: read & compare 4-byte CRC
            let want = hasher.finalize();
            let mut crc_buf = [0u8; 4];
            // CRC bytes themselves NOT fed into hasher
            f.read_exact(&mut crc_buf)?;
            let got = u32::from_le_bytes(crc_buf);
            if got != want {
                return Err(SummaryError::BadCrc {
                    expected: want,
                    got,
                });
            }
            return Ok(());
        }
        let spc_oid = u32::from_le_bytes(entry_buf[0..4].try_into().unwrap());
        let db_oid = u32::from_le_bytes(entry_buf[4..8].try_into().unwrap());
        let rel_number = u32::from_le_bytes(entry_buf[8..12].try_into().unwrap());
        let fork_num = i32::from_le_bytes(entry_buf[12..16].try_into().unwrap());
        let limit_block = u32::from_le_bytes(entry_buf[16..20].try_into().unwrap());
        let nchunks = u32::from_le_bytes(entry_buf[20..24].try_into().unwrap());
        parse_chunks(
            &mut f,
            &mut hasher,
            nchunks,
            spc_oid,
            db_oid,
            rel_number,
            fork_num,
            limit_block,
            state,
        )?;
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_chunks(
    f: &mut File,
    hasher: &mut Crc32cHasher,
    nchunks: u32,
    spc_oid: u32,
    db_oid: u32,
    rel_number: u32,
    fork_num: i32,
    limit_block: u32,
    state: &mut BTreeMap<RelForkKey, RelForkState>,
) -> Result<(), SummaryError> {
    let key = RelForkKey {
        rel: RelFileNode {
            spc_node: spc_oid,
            db_node: db_oid,
            rel_node: rel_number,
        },
        fork_num,
    };
    let st = state.entry(key).or_default();
    // Truncation: drop accumulated blocks >= limit_block (only when set)
    if limit_block != INVALID_BLOCK_NUMBER {
        st.prune_above(limit_block);
    }
    if nchunks == 0 {
        return Ok(());
    }
    let mut usage_buf = vec![0u8; nchunks as usize * 2];
    read_full(f, &mut usage_buf, hasher)?;
    let usage: Vec<u16> = (0..nchunks as usize)
        .map(|i| u16::from_le_bytes(usage_buf[i * 2..i * 2 + 2].try_into().unwrap()))
        .collect();
    for (chunk_no, &used) in usage.iter().enumerate() {
        if used == 0 {
            continue;
        }
        let base = chunk_no as u32 * BLOCKS_PER_CHUNK;
        if used == MAX_ENTRIES_PER_CHUNK as u16 {
            // 4096 u16 words = 8 KiB bitmap; bit j of word i → block base + i*16 + j
            let mut buf = vec![0u8; MAX_ENTRIES_PER_CHUNK as usize * 2];
            read_full(f, &mut buf, hasher)?;
            for i in 0..MAX_ENTRIES_PER_CHUNK as usize {
                let w = u16::from_le_bytes(buf[i * 2..i * 2 + 2].try_into().unwrap());
                if w == 0 {
                    continue;
                }
                for bit in 0..BLOCKS_PER_ENTRY as usize {
                    if w & (1u16 << bit) != 0 {
                        st.blocks
                            .insert(base + i as u32 * BLOCKS_PER_ENTRY + bit as u32);
                    }
                }
            }
        } else {
            // array of `used` u16 offsets within the chunk
            let mut buf = vec![0u8; used as usize * 2];
            read_full(f, &mut buf, hasher)?;
            for i in 0..used as usize {
                let off = u16::from_le_bytes(buf[i * 2..i * 2 + 2].try_into().unwrap());
                st.blocks.insert(base + off as u32);
            }
        }
    }
    Ok(())
}

/// Read exactly `buf.len()` bytes from `r`, also feeding them into `hasher`
fn read_full<R: Read>(r: &mut R, buf: &mut [u8], hasher: &mut Crc32cHasher) -> io::Result<()> {
    r.read_exact(buf)?;
    hasher.update(buf);
    Ok(())
}

/// Streaming CRC32C-Castagnoli (postgres `INIT_CRC32C` / `COMP_CRC32C` /
/// `FIN_CRC32C`). The `crc32c` crate doesn't expose an incremental API on
/// stable, so accumulate via the `update` free function which seeds with
/// the previous state
pub(crate) struct Crc32cHasher {
    state: u32,
}

impl Crc32cHasher {
    pub(crate) fn new() -> Self {
        // `crc32c::crc32c_append(prev, data)` produces the final (post-FIN)
        // CRC when prev = 0. We mirror INIT/COMP/FIN by seeding 0 here and
        // returning the result directly from `finalize`
        Self { state: 0 }
    }
    pub(crate) fn update(&mut self, data: &[u8]) {
        self.state = crc32c::crc32c_append(self.state, data);
    }
    pub(crate) fn finalize(&self) -> u32 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Hand-build a 24-byte serialized entry
    fn enc_entry(spc: u32, db: u32, rel: u32, fork: i32, limit: u32, nchunks: u32) -> [u8; 24] {
        let mut e = [0u8; 24];
        e[0..4].copy_from_slice(&spc.to_le_bytes());
        e[4..8].copy_from_slice(&db.to_le_bytes());
        e[8..12].copy_from_slice(&rel.to_le_bytes());
        e[12..16].copy_from_slice(&fork.to_le_bytes());
        e[16..20].copy_from_slice(&limit.to_le_bytes());
        e[20..24].copy_from_slice(&nchunks.to_le_bytes());
        e
    }

    /// Build a complete summary byte stream: magic + entries + zero sentinel + CRC32C
    struct SummaryBuilder {
        buf: Vec<u8>,
    }
    impl SummaryBuilder {
        fn new() -> Self {
            let mut s = Self { buf: Vec::new() };
            s.buf
                .extend_from_slice(&BLOCK_REF_TABLE_MAGIC.to_le_bytes());
            s
        }
        fn entry_array(
            mut self,
            spc: u32,
            db: u32,
            rel: u32,
            fork: i32,
            limit: u32,
            offsets: &[u16],
        ) -> Self {
            self.buf
                .extend_from_slice(&enc_entry(spc, db, rel, fork, limit, 1));
            // chunk usage: 1 chunk with `offsets.len()` entries (array form)
            self.buf
                .extend_from_slice(&(offsets.len() as u16).to_le_bytes());
            for o in offsets {
                self.buf.extend_from_slice(&o.to_le_bytes());
            }
            self
        }
        fn entry_bitmap(
            mut self,
            spc: u32,
            db: u32,
            rel: u32,
            fork: i32,
            limit: u32,
            bitmap: &[u16],
        ) -> Self {
            assert_eq!(bitmap.len(), MAX_ENTRIES_PER_CHUNK as usize);
            self.buf
                .extend_from_slice(&enc_entry(spc, db, rel, fork, limit, 1));
            self.buf
                .extend_from_slice(&(MAX_ENTRIES_PER_CHUNK as u16).to_le_bytes());
            for w in bitmap {
                self.buf.extend_from_slice(&w.to_le_bytes());
            }
            self
        }
        fn entry_no_chunks(mut self, spc: u32, db: u32, rel: u32, fork: i32, limit: u32) -> Self {
            self.buf
                .extend_from_slice(&enc_entry(spc, db, rel, fork, limit, 0));
            self
        }
        fn finish(mut self) -> Vec<u8> {
            self.buf.extend_from_slice(&[0u8; SERIALIZED_ENTRY_SIZE]);
            let crc = crc32c::crc32c(&self.buf);
            self.buf.extend_from_slice(&crc.to_le_bytes());
            self.buf
        }
    }

    fn write_tmp_summary(data: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn parse_filename_full() {
        let f = parse_summary_filename("0000000100000001000000000000000200000000.summary").unwrap();
        assert_eq!(f.timeline, 1);
        assert_eq!(f.start_lsn, 0x0000_0001_0000_0000);
        assert_eq!(f.end_lsn, 0x0000_0002_0000_0000);
    }

    #[test]
    fn parse_filename_lowercase_hex() {
        let f = parse_summary_filename("0000000200000000000000ff00000000000001ff.summary").unwrap();
        assert_eq!(f.timeline, 2);
        assert_eq!(f.start_lsn, 0xff);
        assert_eq!(f.end_lsn, 0x1ff);
    }

    #[test]
    fn parse_filename_rejects_non_summary() {
        assert!(parse_summary_filename("not_a_summary.txt").is_none());
        assert!(parse_summary_filename("0000000100000000.summary").is_none());
        assert!(
            parse_summary_filename("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.summary").is_none()
        );
    }

    #[test]
    fn select_for_range_full_coverage() {
        let files = vec![
            SummaryFile {
                path: PathBuf::new(),
                timeline: 1,
                start_lsn: 0x100,
                end_lsn: 0x200,
            },
            SummaryFile {
                path: PathBuf::new(),
                timeline: 1,
                start_lsn: 0x200,
                end_lsn: 0x300,
            },
            SummaryFile {
                path: PathBuf::new(),
                timeline: 1,
                start_lsn: 0x300,
                end_lsn: 0x400,
            },
            // wrong timeline — must be ignored
            SummaryFile {
                path: PathBuf::new(),
                timeline: 2,
                start_lsn: 0x000,
                end_lsn: 0x500,
            },
        ];
        let got = select_for_range(&files, 1, 0x150, 0x350).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].start_lsn, 0x100);
        assert_eq!(got[2].end_lsn, 0x400);
    }

    #[test]
    fn select_for_range_gap_detection() {
        // gap between 0x200 and 0x280, range crosses it
        let files = vec![
            SummaryFile {
                path: PathBuf::new(),
                timeline: 1,
                start_lsn: 0x100,
                end_lsn: 0x200,
            },
            SummaryFile {
                path: PathBuf::new(),
                timeline: 1,
                start_lsn: 0x280,
                end_lsn: 0x300,
            },
        ];
        // Request [0x150, 0x2C0): both files overlap; the gap in the middle
        // must be caught as GapInside
        let err = select_for_range(&files, 1, 0x150, 0x2C0).unwrap_err();
        assert!(matches!(err, SummaryError::GapInside { .. }), "{err:?}");
    }

    #[test]
    fn select_for_range_tail_missing() {
        let files = vec![SummaryFile {
            path: PathBuf::new(),
            timeline: 1,
            start_lsn: 0x100,
            end_lsn: 0x200,
        }];
        let err = select_for_range(&files, 1, 0x150, 0x300).unwrap_err();
        assert!(matches!(err, SummaryError::GapAtEnd { .. }), "{err:?}");
    }

    #[test]
    fn select_for_range_empty_range_rejected() {
        let err = select_for_range(&[], 1, 0x200, 0x200).unwrap_err();
        assert!(matches!(err, SummaryError::EmptyRange { .. }), "{err:?}");
    }

    #[test]
    fn parse_array_chunk() {
        // rel (spc=1663, db=16385, rel=100), main fork, no truncation,
        // one chunk with blocks {10, 20, 30}
        let data = SummaryBuilder::new()
            .entry_array(
                1663,
                16385,
                100,
                MAIN_FORK_NUM,
                INVALID_BLOCK_NUMBER,
                &[10, 20, 30],
            )
            .finish();
        let f = write_tmp_summary(&data);
        let mut state = BTreeMap::new();
        parse_summary_file(f.path(), &mut state).unwrap();
        let key = RelForkKey {
            rel: RelFileNode {
                spc_node: 1663,
                db_node: 16385,
                rel_node: 100,
            },
            fork_num: MAIN_FORK_NUM,
        };
        let st = state.get(&key).unwrap();
        let got: Vec<u32> = st.blocks.iter().copied().collect();
        assert_eq!(got, vec![10, 20, 30]);
    }

    #[test]
    fn parse_bitmap_chunk() {
        // bits for blocks 0, 1, 15, 16, 17. Within a u16, bit 0 = lowest block
        let mut bitmap = vec![0u16; MAX_ENTRIES_PER_CHUNK as usize];
        bitmap[0] = 1u16 | (1 << 1) | (1 << 15); // blocks 0,1,15
        bitmap[1] = 1u16 | (1 << 1); // blocks 16,17
        let data = SummaryBuilder::new()
            .entry_bitmap(
                1663,
                16385,
                42,
                MAIN_FORK_NUM,
                INVALID_BLOCK_NUMBER,
                &bitmap,
            )
            .finish();
        let f = write_tmp_summary(&data);
        let mut state = BTreeMap::new();
        parse_summary_file(f.path(), &mut state).unwrap();
        let key = RelForkKey {
            rel: RelFileNode {
                spc_node: 1663,
                db_node: 16385,
                rel_node: 42,
            },
            fork_num: MAIN_FORK_NUM,
        };
        let st = state.get(&key).unwrap();
        let got: Vec<u32> = st.blocks.iter().copied().collect();
        assert_eq!(got, vec![0, 1, 15, 16, 17]);
    }

    #[test]
    fn limit_block_prunes_earlier_blocks() {
        // First summary records blocks {5, 10, 20}. Second announces
        // truncation to 15. Combined: {5, 10}
        let mut state = BTreeMap::new();

        let data1 = SummaryBuilder::new()
            .entry_array(
                1663,
                16385,
                7,
                MAIN_FORK_NUM,
                INVALID_BLOCK_NUMBER,
                &[5, 10, 20],
            )
            .finish();
        let f1 = write_tmp_summary(&data1);
        parse_summary_file(f1.path(), &mut state).unwrap();

        let data2 = SummaryBuilder::new()
            .entry_no_chunks(1663, 16385, 7, MAIN_FORK_NUM, 15)
            .finish();
        let f2 = write_tmp_summary(&data2);
        parse_summary_file(f2.path(), &mut state).unwrap();

        let key = RelForkKey {
            rel: RelFileNode {
                spc_node: 1663,
                db_node: 16385,
                rel_node: 7,
            },
            fork_num: MAIN_FORK_NUM,
        };
        let st = state.get(&key).unwrap();
        let got: Vec<u32> = st.blocks.iter().copied().collect();
        assert_eq!(got, vec![5, 10]);
    }

    #[test]
    fn bad_magic_rejected() {
        let f = write_tmp_summary(&[0xde, 0xad, 0xbe, 0xef]);
        let mut state = BTreeMap::new();
        let err = parse_summary_file(f.path(), &mut state).unwrap_err();
        assert!(matches!(err, SummaryError::BadMagic { .. }), "{err:?}");
    }

    #[test]
    fn bad_crc_rejected() {
        let mut data = SummaryBuilder::new()
            .entry_array(1663, 16385, 100, MAIN_FORK_NUM, INVALID_BLOCK_NUMBER, &[10])
            .finish();
        // flip last byte of CRC
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        let f = write_tmp_summary(&data);
        let mut state = BTreeMap::new();
        let err = parse_summary_file(f.path(), &mut state).unwrap_err();
        assert!(matches!(err, SummaryError::BadCrc { .. }), "{err:?}");
    }

    #[test]
    fn list_skips_non_summary_files() {
        let dir = tempfile::tempdir().unwrap();
        let summary_name = "0000000100000001000000000000000200000000.summary";
        std::fs::write(dir.path().join(summary_name), b"").unwrap();
        std::fs::write(dir.path().join("README"), b"").unwrap();
        std::fs::write(dir.path().join("temp.summary"), b"").unwrap();
        let files = list_summary_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].timeline, 1);
    }

    #[test]
    fn empty_pgdata_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let m = read_for_range(dir.path(), 1, 0x100, 0x200).unwrap_err();
        // No summaries → NoSummariesForRange (after empty dir lookup)
        assert!(matches!(m, SummaryError::NoSummariesForRange { .. }));
    }

    #[test]
    fn end_to_end_main_fork_only() {
        // Build one summary with two entries: one MAIN fork, one VM fork
        // (forknum=2). The map produced by read_for_range must include the
        // MAIN fork's blocks & exclude the VM fork's.
        let dir = tempfile::tempdir().unwrap();
        let sum_dir = dir.path().join(SUMMARIES_DIR);
        std::fs::create_dir_all(&sum_dir).unwrap();
        let data = SummaryBuilder::new()
            .entry_array(
                1663,
                16385,
                100,
                MAIN_FORK_NUM,
                INVALID_BLOCK_NUMBER,
                &[1, 2],
            )
            .entry_array(1663, 16385, 100, 2, INVALID_BLOCK_NUMBER, &[7, 8])
            .finish();
        // filename must cover [0x100, 0x200) on timeline 1
        let fname = "0000000100000000000001000000000000000200.summary";
        std::fs::write(sum_dir.join(fname), &data).unwrap();

        let m = read_for_range(dir.path(), 1, 0x100, 0x200).unwrap();
        let blocks = m
            .blocks_for("base/16385/100")
            .unwrap()
            .expect("rel should be in map");
        assert_eq!(
            blocks.into_iter().collect::<Vec<_>>(),
            vec![1u32, 2u32],
            "vm fork must be excluded"
        );
    }
}
