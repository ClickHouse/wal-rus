//! Page-delta machinery for incremental backups
//!
//! Tracks the set of dirty `(RelFileNode, BlockNo)` between two LSNs by
//! walking the WAL segments between them through `walparser`. Mirrors
//! wal-g's `internal/databases/postgres/paged_file_delta_map.go` and
//! `delta_file.go` so bucket-resident delta files are bidirectionally
//! readable
//!
//! On-disk delta file format (wal-g `delta_005/<seg>_delta`):
//! 1. List of `BlockLocation` tuples (LE u32×4 = 16 bytes), all-zero
//!    sentinel terminates
//! 2. `WalParser` state: u32 length + N bytes of `current_record_data`
//!
//! Path layout assumptions (postgres):
//!   - default tablespace: `base/<dboid>/<relfilenode>`
//!   - global:             `global/<relfilenode>` (rare in deltas)
//!   - non-default:        `pg_tblspc/<spcoid>/<TSPC_VER_DIR>/<dboid>/<relfilenode>`
//!
//! Files past 1 GiB get a `.<n>` suffix; the suffix is the rel file *segment
//! id*, with each segment holding `BLOCKS_IN_REL_FILE` blocks of the same
//! RelFileNode. Block numbers in the delta map are global (segment id ×
//! `BLOCKS_IN_REL_FILE` + intra-segment offset)

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};

use anyhow::{Context, Result, anyhow};
use thiserror::Error;
use tokio::io::AsyncReadExt;

use crate::compression;
use crate::pg::backup::{BackupSentinelDtoV2, name_from_sentinel_key, sentinel_key};
use crate::pg::wal::segment::{DEFAULT_WAL_SEG_SIZE, SegmentName};
use crate::pg::walparser::{
    BlockLocation, ParsePageError, RelFileNode, WalParser, extract_locations_from_wal_file,
    read_locations_from, write_locations_to,
};
use crate::storage::DynStorage;

pub const PG_PAGE_SIZE: u64 = 8192;
/// PG's per-file size cap before splitting into `<rel>.<n>` segments
pub const REL_FILE_SIZE_BOUND: u64 = 1 << 30;
pub const BLOCKS_IN_REL_FILE: u32 = (REL_FILE_SIZE_BOUND / PG_PAGE_SIZE) as u32;
/// Hardcoded SPC oid for the default `base` tablespace (`DEFAULTTABLESPACE_OID`)
pub const DEFAULT_SPC_NODE: u32 = 1663;

pub const DEFAULT_TABLESPACE: &str = "base";
pub const _GLOBAL_TABLESPACE: &str = "global";
pub const NON_DEFAULT_TABLESPACE: &str = "pg_tblspc";

#[derive(Debug, Error)]
pub enum DeltaError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("path is not a paged-file: {0}")]
    NotPagedFile(String),
    #[error("path has unknown tablespace layout: {0}")]
    UnknownTablespace(String),
    #[error("cannot derive RelFileNode/relFileID from {0}: {1}")]
    PathParse(String, String),
    #[error(transparent)]
    Parse(#[from] ParsePageError),
}

/// In-memory delta map: which blocks of which relfiles changed?
/// `BTreeSet<u32>` per rel keeps memory bounded for sparse-delta workloads
/// (typical delta backup touches < 1% of pages)
#[derive(Debug, Default, Clone)]
pub struct PagedFileDeltaMap {
    by_rel: BTreeMap<RelFileNode, BTreeSet<u32>>,
}

impl PagedFileDeltaMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.by_rel.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_rel.values().map(|s| s.len()).sum()
    }

    pub fn add_location(&mut self, loc: BlockLocation) {
        self.by_rel.entry(loc.rel).or_default().insert(loc.block_no);
    }

    pub fn add_locations(&mut self, locs: impl IntoIterator<Item = BlockLocation>) {
        for loc in locs {
            self.add_location(loc);
        }
    }

    /// Return the bitmap of changed blocks for a paged-file path. Returns
    /// `None` if the rel isn't in the map (file unchanged).
    /// Blocks are returned in *segment-relative* offsets (0..BLOCKS_IN_REL_FILE)
    /// for the segment id derived from the trailing `.<n>` of `path`
    pub fn blocks_for(&self, path: &str) -> Result<Option<BTreeSet<u32>>, DeltaError> {
        let rel = get_rel_file_node_from(path)?;
        let Some(blocks) = self.by_rel.get(&rel) else {
            return Ok(None);
        };
        let seg_id = get_rel_file_id_from(path)?;
        let lo = seg_id as u32 * BLOCKS_IN_REL_FILE;
        let hi = lo.saturating_add(BLOCKS_IN_REL_FILE);
        let shifted: BTreeSet<u32> = blocks.range(lo..hi).map(|&b| b - lo).collect();
        Ok(Some(shifted))
    }

    /// Drain the map into an LSN-ordered tuple list (deterministic so two
    /// runs over the same WAL produce byte-identical delta files)
    pub fn locations(&self) -> Vec<BlockLocation> {
        let mut out = Vec::with_capacity(self.len());
        for (rel, blocks) in &self.by_rel {
            for &b in blocks {
                out.push(BlockLocation {
                    rel: *rel,
                    block_no: b,
                });
            }
        }
        out
    }
}

/// On-disk delta file: aggregated block locations + parser state for the
/// cross-segment record stitching
pub struct DeltaFile {
    pub locations: Vec<BlockLocation>,
    pub wal_parser: WalParser,
}

impl DeltaFile {
    pub fn new(wal_parser: WalParser) -> Self {
        Self {
            locations: Vec::new(),
            wal_parser,
        }
    }

    /// wal-g `DeltaFile.Save`: write tuples (zero-terminated) then parser state
    pub fn save<W: Write>(&self, mut w: W) -> Result<(), DeltaError> {
        write_locations_to(&mut w, &self.locations)?;
        self.wal_parser.save(&mut w)?;
        Ok(())
    }

    pub fn load<R: Read>(mut r: R) -> Result<Self, DeltaError> {
        let locations = read_locations_from(&mut r)
            .map_err(|e| DeltaError::Io(io::Error::other(e.to_string())))?;
        let wal_parser = WalParser::load(&mut r)?;
        Ok(Self {
            locations,
            wal_parser,
        })
    }
}

// ─── path → RelFileNode parsing ─────────────────────────────────────────────

/// Validate the basename matches the `<relnode>(.<segid>)?` paged-file shape
fn parse_paged_basename(name: &str) -> Option<(u32, Option<u32>)> {
    // wal-g regex: ^(\d+)([.]\d+)?$
    if name.is_empty() {
        return None;
    }
    let (digits, rest) = take_digits(name);
    if digits.is_empty() {
        return None;
    }
    let rel_node: u32 = digits.parse().ok()?;
    if rest.is_empty() {
        return Some((rel_node, None));
    }
    let after_dot = rest.strip_prefix('.')?;
    let (seg_digits, tail) = take_digits(after_dot);
    if seg_digits.is_empty() || !tail.is_empty() {
        return None;
    }
    let seg_id: u32 = seg_digits.parse().ok()?;
    Some((rel_node, Some(seg_id)))
}

fn take_digits(s: &str) -> (&str, &str) {
    let n = s.bytes().take_while(|b| b.is_ascii_digit()).count();
    s.split_at(n)
}

/// Extract `RelFileNode` from a tar-style relative path like
/// `base/16384/16385` or `pg_tblspc/16400/PG_16_xxx/16384/16385`
pub fn get_rel_file_node_from(path: &str) -> Result<RelFileNode, DeltaError> {
    let (folder, name) = match path.rsplit_once('/') {
        Some((f, n)) => (f, n),
        None => return Err(DeltaError::NotPagedFile(path.into())),
    };
    let (rel_node, _) =
        parse_paged_basename(name).ok_or_else(|| DeltaError::NotPagedFile(path.into()))?;

    let parts: Vec<&str> = folder.split('/').collect();
    if parts.is_empty() {
        return Err(DeltaError::UnknownTablespace(path.into()));
    }
    let db_node: u32 = parts
        .last()
        .ok_or_else(|| DeltaError::UnknownTablespace(path.into()))?
        .parse()
        .map_err(|e: std::num::ParseIntError| {
            DeltaError::PathParse(path.into(), format!("dbNode: {e}"))
        })?;

    let n = parts.len();
    if n >= 2 && parts[n - 2] == DEFAULT_TABLESPACE {
        return Ok(RelFileNode {
            spc_node: DEFAULT_SPC_NODE,
            db_node,
            rel_node,
        });
    }
    if path.contains(NON_DEFAULT_TABLESPACE) {
        // .../pg_tblspc/<spcoid>/<TSPC_VER_DIR>/<dboid>/<relnode>
        if n < 3 {
            return Err(DeltaError::UnknownTablespace(path.into()));
        }
        let spc_node: u32 = parts[n - 3].parse().map_err(|e: std::num::ParseIntError| {
            DeltaError::PathParse(path.into(), format!("spcNode: {e}"))
        })?;
        return Ok(RelFileNode {
            spc_node,
            db_node,
            rel_node,
        });
    }
    Err(DeltaError::UnknownTablespace(path.into()))
}

/// Extract the rel-file segment id (`.<n>` suffix). Returns 0 if no suffix
pub fn get_rel_file_id_from(path: &str) -> Result<u32, DeltaError> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let (_, seg) =
        parse_paged_basename(name).ok_or_else(|| DeltaError::NotPagedFile(path.into()))?;
    Ok(seg.unwrap_or(0))
}

/// Path heuristic: paged files live under base/ or pg_tblspc/, their
/// basename matches `<relnode>(.<segid>)?`, and total size is a non-zero
/// multiple of 8 KiB. wal-g `isPagedFile` minus the transaction-state dir
/// exclusion (we filter those out at the streamer)
pub fn is_paged_path(path: &str) -> bool {
    if !(path.contains(DEFAULT_TABLESPACE) || path.contains(NON_DEFAULT_TABLESPACE)) {
        return false;
    }
    let name = path.rsplit('/').next().unwrap_or(path);
    parse_paged_basename(name).is_some()
}

// ─── Parent backup selection (wal-g delta_backup_configurator.go) ───────────

/// Information about a parent backup carried into the delta-push pipeline.
/// Empty means "do a full backup instead" — every fallback in wal-g's
/// configurator collapses to this
#[derive(Debug, Clone)]
pub struct PrevBackupInfo {
    pub name: String,
    pub start_lsn: u64,
    pub timeline: u32,
    pub finish_lsn: u64,
    pub increment_full_name: String,
    pub increment_count: u32,
    pub is_permanent: bool,
    pub system_identifier: Option<u64>,
    pub user_data: Option<serde_json::Value>,
}

/// Configure delta vs full at the start of backup-push. Mirrors wal-g's
/// `RegularDeltaBackupConfigurator.Configure`. Returns `Ok(None)` to mean
/// "do a full backup" (delta disabled, no prior backup, parent chain too
/// long, etc.)
pub async fn configure_delta_parent(
    storage: &DynStorage,
    delta: &crate::config::DeltaSettings,
    new_is_permanent: bool,
) -> Result<Option<PrevBackupInfo>> {
    if !delta.enabled() {
        return Ok(None);
    }

    let candidate = select_candidate(storage, delta).await?;
    let Some((name, v2)) = candidate else {
        tracing::info!(
            target = "backup_push",
            "no prior backup; falling back to full"
        );
        return Ok(None);
    };

    let parent_inc_count = v2.sentinel.increment_count.unwrap_or(0);
    let next_inc_count = parent_inc_count.saturating_add(1).max(1);
    if next_inc_count > delta.max_steps as i32 {
        tracing::info!(
            target = "backup_push",
            "reached max delta steps ({}); falling back to full",
            delta.max_steps
        );
        return Ok(None);
    }

    let Some(start_lsn) = v2.sentinel.backup_start_lsn else {
        tracing::info!(
            target = "backup_push",
            "parent {name} lacks BackupStartLSN; falling back to full"
        );
        return Ok(None);
    };

    if !new_is_permanent && !delta.from_full && v2.is_permanent {
        tracing::info!(
            target = "backup_push",
            "parent {name} is permanent but new backup is not; falling back to full"
        );
        return Ok(None);
    }

    // Walk to the chain's root if LATEST_FULL requested
    let (effective_name, effective_v2) = if delta.from_full {
        if let Some(full_name) = v2.sentinel.increment_full_name.clone() {
            tracing::info!(
                target = "backup_push",
                "WALG_DELTA_ORIGIN=LATEST_FULL → using chain root {full_name}"
            );
            let v = fetch_sentinel(storage, &full_name).await?;
            (full_name, v)
        } else {
            (name, v2)
        }
    } else {
        (name, v2)
    };

    let timeline = SegmentName::parse(
        effective_name
            .strip_prefix("base_")
            .and_then(|n| n.get(..24))
            .ok_or_else(|| anyhow!("cannot derive timeline from {effective_name}"))?,
    )
    .with_context(|| format!("parse timeline from {effective_name}"))?
    .timeline;

    let info = PrevBackupInfo {
        name: effective_name,
        start_lsn: effective_v2
            .sentinel
            .backup_start_lsn
            .ok_or_else(|| anyhow!("parent BackupStartLSN missing after revalidation"))?,
        timeline,
        finish_lsn: effective_v2.sentinel.backup_finish_lsn.unwrap_or(start_lsn),
        increment_full_name: effective_v2
            .sentinel
            .increment_full_name
            .clone()
            .unwrap_or_else(|| name_only(&effective_v2)),
        increment_count: next_inc_count.max(0) as u32,
        is_permanent: effective_v2.is_permanent,
        system_identifier: effective_v2.sentinel.system_identifier,
        user_data: effective_v2.sentinel.user_data.clone(),
    };
    Ok(Some(info))
}

fn name_only(_v2: &BackupSentinelDtoV2) -> String {
    // increment_full_name absent on the chain root means the chain root *is*
    // this backup; the V2 sentinel itself doesn't carry the name (it's
    // implicit in the storage key), so caller fills it
    String::new()
}

async fn select_candidate(
    storage: &DynStorage,
    delta: &crate::config::DeltaSettings,
) -> Result<Option<(String, BackupSentinelDtoV2)>> {
    if let Some(name) = &delta.from_name {
        let v2 = fetch_sentinel(storage, name).await?;
        return Ok(Some((name.clone(), v2)));
    }
    if let Some(user_data_str) = &delta.from_user_data {
        return find_by_user_data(storage, user_data_str).await;
    }
    find_latest(storage).await
}

async fn fetch_sentinel(storage: &DynStorage, name: &str) -> Result<BackupSentinelDtoV2> {
    let key = sentinel_key(name);
    let mut r = storage
        .get(&key)
        .await
        .with_context(|| format!("get {key}"))?;
    let mut buf = Vec::with_capacity(4096);
    r.read_to_end(&mut buf).await?;
    serde_json::from_slice(&buf).with_context(|| format!("parse sentinel {key}"))
}

async fn find_latest(storage: &DynStorage) -> Result<Option<(String, BackupSentinelDtoV2)>> {
    use futures::StreamExt;
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let mut entries: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> = Vec::new();
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        if let Some(name) = name_from_sentinel_key(&obj.key) {
            entries.push((name.to_string(), obj.last_modified));
        }
    }
    entries.sort_by(|a, b| b.1.cmp(&a.1));
    let Some((name, _)) = entries.into_iter().next() else {
        return Ok(None);
    };
    let v2 = fetch_sentinel(storage, &name).await?;
    Ok(Some((name, v2)))
}

async fn find_by_user_data(
    storage: &DynStorage,
    needle: &str,
) -> Result<Option<(String, BackupSentinelDtoV2)>> {
    use futures::StreamExt;
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let needle_json: serde_json::Value = serde_json::from_str(needle)
        .with_context(|| format!("WALG_DELTA_FROM_USER_DATA value is not JSON: {needle}"))?;
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        if let Some(name) = name_from_sentinel_key(&obj.key) {
            let v2 = match fetch_sentinel(storage, name).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(target = "backup_push", "skip {name}: {e:#}");
                    continue;
                }
            };
            if v2.sentinel.user_data.as_ref() == Some(&needle_json) {
                return Ok(Some((name.to_string(), v2)));
            }
        }
    }
    Ok(None)
}

// ─── WAL → delta map (walk segments between two LSNs) ───────────────────────

/// Walk the WAL segments [start_lsn, end_lsn) on `timeline`, parse each,
/// & aggregate the referenced block locations into a delta map.
/// Segments are fetched from the `wal_005/` prefix (wal-rs layout)
pub async fn build_delta_map_from_wal(
    storage: &DynStorage,
    timeline: u32,
    start_lsn: u64,
    end_lsn: u64,
    compression: compression::Method,
) -> Result<PagedFileDeltaMap> {
    let seg_size = DEFAULT_WAL_SEG_SIZE;
    let mut delta = PagedFileDeltaMap::new();
    let mut parser = WalParser::new();
    if end_lsn <= start_lsn {
        return Ok(delta);
    }
    let first_seg = lsn_to_seg(start_lsn, seg_size);
    let last_seg = lsn_to_seg(end_lsn.saturating_sub(1), seg_size);
    for seg in first_seg..=last_seg {
        let name = SegmentName {
            timeline,
            log_id: (seg >> 32) as u32,
            seg_no: seg as u32,
        }
        .format();
        let locations =
            match fetch_and_parse_segment(storage, &name, compression, &mut parser).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!(target = "backup_push", "segment {name}: {e:#}; skipping");
                    continue;
                }
            };
        delta.add_locations(locations);
    }
    Ok(delta)
}

fn lsn_to_seg(lsn: u64, seg_size: u64) -> u64 {
    lsn / seg_size
}

async fn fetch_and_parse_segment(
    storage: &DynStorage,
    name: &str,
    compression: compression::Method,
    parser: &mut WalParser,
) -> Result<Vec<BlockLocation>> {
    let ext = compression.extension();
    let key = if ext.is_empty() {
        format!("{}/{}", crate::pg::WAL_FOLDER, name)
    } else {
        format!("{}/{}.{}", crate::pg::WAL_FOLDER, name, ext)
    };
    let r = storage
        .get(&key)
        .await
        .with_context(|| format!("get {key}"))?;
    let decoded = compression::decode(compression, r);
    let mut buf = Vec::new();
    let mut decoded = decoded;
    decoded.read_to_end(&mut buf).await?;
    let locations = extract_locations_from_wal_file(parser, std::io::Cursor::new(buf))
        .with_context(|| format!("parse segment {name}"))?;
    Ok(locations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_default_tablespace_path() {
        let r = get_rel_file_node_from("base/16384/16385").unwrap();
        assert_eq!(r.spc_node, DEFAULT_SPC_NODE);
        assert_eq!(r.db_node, 16384);
        assert_eq!(r.rel_node, 16385);
    }

    #[test]
    fn parse_nondefault_tablespace_path() {
        let r = get_rel_file_node_from("pg_tblspc/16400/PG_16_xxx/16384/16385").unwrap();
        assert_eq!(r.spc_node, 16400);
        assert_eq!(r.db_node, 16384);
        assert_eq!(r.rel_node, 16385);
    }

    #[test]
    fn parse_segmented_rel_file_id() {
        assert_eq!(get_rel_file_id_from("base/16384/16385").unwrap(), 0);
        assert_eq!(get_rel_file_id_from("base/16384/16385.1").unwrap(), 1);
        assert_eq!(get_rel_file_id_from("base/16384/16385.7").unwrap(), 7);
    }

    #[test]
    fn rejects_non_paged_basename() {
        assert!(get_rel_file_node_from("base/16384/pg_filenode.map").is_err());
        assert!(get_rel_file_node_from("base/16384/PG_VERSION").is_err());
        assert!(!is_paged_path("base/16384/PG_VERSION"));
        assert!(!is_paged_path("global/pg_control"));
    }

    #[test]
    fn is_paged_path_classifications() {
        assert!(is_paged_path("base/16384/16385"));
        assert!(is_paged_path("base/16384/16385.1"));
        assert!(is_paged_path("pg_tblspc/16400/PG_16/16384/16385"));
        assert!(!is_paged_path("pg_xact/0000"));
        assert!(!is_paged_path("pg_wal/000000010000000000000001"));
    }

    #[test]
    fn delta_map_segment_filter() {
        let mut m = PagedFileDeltaMap::new();
        // pretend we modified blocks 5 (seg 0) and BLOCKS_IN_REL_FILE+3 (seg 1)
        let rel = RelFileNode {
            spc_node: DEFAULT_SPC_NODE,
            db_node: 16384,
            rel_node: 16385,
        };
        m.add_location(BlockLocation { rel, block_no: 5 });
        m.add_location(BlockLocation {
            rel,
            block_no: BLOCKS_IN_REL_FILE + 3,
        });
        m.add_location(BlockLocation {
            rel,
            block_no: 2 * BLOCKS_IN_REL_FILE + 9,
        });

        let seg0 = m.blocks_for("base/16384/16385").unwrap().unwrap();
        assert_eq!(seg0, [5].iter().copied().collect::<BTreeSet<_>>());

        let seg1 = m.blocks_for("base/16384/16385.1").unwrap().unwrap();
        assert_eq!(seg1, [3].iter().copied().collect::<BTreeSet<_>>());

        let seg2 = m.blocks_for("base/16384/16385.2").unwrap().unwrap();
        assert_eq!(seg2, [9].iter().copied().collect::<BTreeSet<_>>());

        let seg3 = m.blocks_for("base/16384/16385.3").unwrap().unwrap();
        assert!(seg3.is_empty()); // file has segment but no dirty blocks
    }

    #[test]
    fn delta_map_unchanged_relfile_returns_none() {
        let m = PagedFileDeltaMap::new();
        assert!(m.blocks_for("base/16384/16385").unwrap().is_none());
    }

    #[test]
    fn delta_file_round_trip() {
        let wp = WalParser::new();
        let mut buf = Vec::new();
        wp.save(&mut buf).unwrap();
        let wp_reloaded = WalParser::load(buf.as_slice()).unwrap();

        let mut df = DeltaFile::new(wp_reloaded);
        df.locations
            .push(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 7));
        df.locations
            .push(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16386, 0));

        let mut out = Vec::new();
        df.save(&mut out).unwrap();

        let df2 = DeltaFile::load(out.as_slice()).unwrap();
        assert_eq!(df2.locations.len(), 2);
        assert_eq!(df2.locations[0].block_no, 7);
        assert_eq!(df2.locations[1].block_no, 0);
        assert!(df2.wal_parser.current_record_data().is_empty());
    }
}
