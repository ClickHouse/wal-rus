//! Page-delta machinery for incremental backups
//!
//! Tracks the set of dirty `(RelFileNode, BlockNo)` between two LSNs by
//! walking the WAL segments between them through `walparser`. Mirrors
//! wal-g's `internal/databases/postgres/paged_file_delta_map.go` and
//! `delta_file.go` so bucket-resident delta files are bidirectionally
//! readable
//!
//! On-disk delta file format (wal-g `wal_005/<group>_delta`, same folder as
//! WAL segments):
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

use std::collections::{BTreeMap, HashSet};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use roaring::RoaringBitmap;
use thiserror::Error;
use tokio_util::io::SyncIoBridge;

use crate::compression;
use crate::concurrency::BoundedTasks;
use crate::pg::backup::fetch::fetch_sentinel;
use crate::pg::backup::wal_delta::{
    WAL_FILES_IN_DELTA, delta_group_name, delta_group_no, delta_storage_key, seg_name_from_global,
};
use crate::pg::backup::{BackupSentinelDtoV2, format_pg_lsn, increment, name_from_sentinel_key};
use crate::pg::wal::segment::{SegmentName, wal_segment_size};
use crate::pg::walparser::{
    BlockLocation, ParsePageError, RelFileNode, SegmentBoundary, WalParser,
    extract_block_locations, extract_locations_from_wal_file, parse_record_from_bytes,
    walk_segment_locations,
};
use crate::retry::{RetryPolicy, with_retry};
use crate::storage::{ObjExt, Operator};

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
/// `RoaringBitmap` per rel run/bitmap-compresses dense rewrites (VACUUM FULL,
/// CREATE INDEX, bulk load) that balloon a `BTreeSet<u32>` to ~13 B/block;
/// sparse OLTP deltas stay comparable. Matches wal-g's `map[RelFileNode]*roaring.Bitmap`
#[derive(Debug, Default, Clone)]
pub struct PagedFileDeltaMap {
    by_rel: BTreeMap<RelFileNode, RoaringBitmap>,
}

impl PagedFileDeltaMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.by_rel.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_rel.values().map(|s| s.len() as usize).sum()
    }

    pub fn add_location(&mut self, loc: BlockLocation) {
        self.by_rel.entry(loc.rel).or_default().insert(loc.block_no);
    }

    pub fn add_locations(&mut self, locs: impl IntoIterator<Item = BlockLocation>) {
        for loc in locs {
            self.add_location(loc);
        }
    }

    /// Per-rel set union of another map. Disjoint LSN sub-ranges of one delta
    /// (eg summaries + a raw-walked gap) compose by changed-block union
    pub fn merge(&mut self, other: PagedFileDeltaMap) {
        for (rel, blocks) in other.by_rel {
            *self.by_rel.entry(rel).or_default() |= blocks;
        }
    }

    /// Return the changed blocks for a paged-file path, ascending. Returns
    /// `None` if the rel isn't in the map (file unchanged).
    /// Blocks are returned in *segment-relative* offsets (0..BLOCKS_IN_REL_FILE)
    /// for the segment id derived from the trailing `.<n>` of `path`
    pub fn blocks_for(&self, path: &str) -> Result<Option<Vec<u32>>, DeltaError> {
        let rel = get_rel_file_node_from(path)?;
        let Some(blocks) = self.by_rel.get(&rel) else {
            return Ok(None);
        };
        let seg_id = get_rel_file_id_from(path)?;
        let lo = seg_id as u32 * BLOCKS_IN_REL_FILE;
        let hi = lo.saturating_add(BLOCKS_IN_REL_FILE);
        let shifted: Vec<u32> = blocks.range(lo..hi).map(|b| b - lo).collect();
        Ok(Some(shifted))
    }

    /// Drain the map into an LSN-ordered tuple list (deterministic so two
    /// runs over the same WAL produce byte-identical delta files)
    pub fn locations(&self) -> Vec<BlockLocation> {
        let mut out = Vec::with_capacity(self.len());
        for (rel, blocks) in &self.by_rel {
            for b in blocks {
                out.push(BlockLocation {
                    rel: *rel,
                    block_no: b,
                });
            }
        }
        out
    }
}

// On-disk sidecar format (wal-g `DeltaFile`): location tuples, all-zero
// terminator, then `WalParser` state (u32 len + bytes). Never materialized as
// a struct — `wal_delta::record_segment` writes it append-only and
// `fold_sidecar_into_map` streams it tuple-by-tuple, so neither side holds the
// whole group's locations in memory

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
    /// Increment format of the parent when it is itself a delta; `None` for a
    /// full parent. A new delta must match this (chains can't mix wi1 & native)
    pub parent_increment_format: Option<increment::Format>,
    /// Paths present in the increment-base backup (its `files_metadata.json`
    /// `Files` keys, skipped entries included). wal-g only increments files
    /// that `wasInBase`; a relation created after the parent has no base file
    /// to apply onto, so it must ship in full. Empty if metadata is missing
    pub parent_files: Arc<HashSet<String>>,
}

/// Configure delta vs full at the start of backup-push. Mirrors wal-g's
/// `RegularDeltaBackupConfigurator.Configure`. Returns `Ok(None)` to mean
/// "do a full backup" (delta disabled, no prior backup, parent chain too
/// long, etc.)
pub async fn configure_delta_parent(
    storage: &Operator,
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

    let parent_files = Arc::new(load_parent_file_set(storage, &effective_name).await);

    let info = PrevBackupInfo {
        name: effective_name,
        start_lsn: effective_v2
            .sentinel
            .backup_start_lsn
            .ok_or_else(|| anyhow!("parent BackupStartLSN missing after revalidation"))?
            .get(),
        timeline,
        finish_lsn: effective_v2
            .sentinel
            .backup_finish_lsn
            .unwrap_or(start_lsn)
            .get(),
        increment_full_name: effective_v2
            .sentinel
            .increment_full_name
            .clone()
            .unwrap_or_else(|| name_only(&effective_v2)),
        increment_count: next_inc_count.max(0) as u32,
        is_permanent: effective_v2.is_permanent,
        system_identifier: effective_v2.sentinel.system_identifier,
        user_data: effective_v2.sentinel.user_data.clone(),
        // Constrain format only when the parent is itself a delta; a full
        // parent starts a fresh chain that may pick either format
        parent_increment_format: effective_v2
            .sentinel
            .increment_from
            .as_ref()
            .map(|_| effective_v2.sentinel.increment_format),
        parent_files,
    };
    Ok(Some(info))
}

/// Load the `Files` key set from a parent backup's `files_metadata.json`.
/// Tolerant: a missing or unreadable sidecar yields an empty set, which
/// degrades the delta to shipping all paged files in full (safe, matches
/// wal-g's behaviour when the base file list is unavailable)
async fn load_parent_file_set(storage: &Operator, name: &str) -> HashSet<String> {
    use crate::pg::backup::{FilesMetadataDto, files_metadata_key, load_json};
    let key = files_metadata_key(name);
    match load_json::<FilesMetadataDto>(storage, &key, 1 << 16).await {
        Ok(m) => m.files.into_keys().collect(),
        Err(e) => {
            tracing::warn!(
                target = "backup_push",
                "parent {name} files_metadata unavailable ({e:#}); \
                 delta will ship paged files in full",
            );
            HashSet::new()
        }
    }
}

fn name_only(_v2: &BackupSentinelDtoV2) -> String {
    // increment_full_name absent on the chain root means the chain root *is*
    // this backup; the V2 sentinel itself doesn't carry the name (it's
    // implicit in the storage key), so caller fills it
    String::new()
}

async fn select_candidate(
    storage: &Operator,
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

async fn find_latest(storage: &Operator) -> Result<Option<(String, BackupSentinelDtoV2)>> {
    use futures::StreamExt;
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list_objs(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let mut entries: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> = Vec::new();
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        if let Some(name) = name_from_sentinel_key(&obj.key) {
            entries.push((name.to_string(), obj.last_modified));
        }
    }
    let Some((name, _)) = entries.into_iter().max_by_key(|e| e.1) else {
        return Ok(None);
    };
    let v2 = fetch_sentinel(storage, &name).await?;
    Ok(Some((name, v2)))
}

async fn find_by_user_data(
    storage: &Operator,
    needle: &str,
) -> Result<Option<(String, BackupSentinelDtoV2)>> {
    use futures::StreamExt;
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list_objs(&prefix)
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

/// Build the delta map for blocks changed in `[start_lsn, end_lsn)` on
/// `timeline`. Reads `<group>_delta` sidecars for the complete 16-segment
/// groups (O(touched relations)) and parses only the trailing partial group's
/// raw WAL. Mirrors wal-g `getDeltaMap`.
///
/// A missing sidecar for a complete group is raw-walked in place (one group's
/// reparse); only an unreadable sidecar or raw-walk failure falls back to a full
/// raw-WAL walk of the range — wal-g hard errors here, but the fallback keeps
/// buckets archived without `WALG_USE_WAL_DELTA` working unchanged
///
/// `wal_dir` is the local `pg_wal` when the push reads a local data dir; raw
/// segments are served from there (uncompressed, no S3 round-trip), falling
/// back to the archive only for segments PG has already recycled. `None` for a
/// remote replication source, which has no local WAL
pub async fn build_delta_map_from_wal(
    settings: &crate::config::Settings,
    storage: &Operator,
    timeline: u32,
    start_lsn: u64,
    end_lsn: u64,
    compression: compression::Method,
    wal_dir: Option<&Path>,
) -> Result<PagedFileDeltaMap> {
    if end_lsn <= start_lsn {
        return Ok(PagedFileDeltaMap::new());
    }
    match build_delta_map_from_sidecars(
        settings,
        storage,
        timeline,
        start_lsn,
        end_lsn,
        compression,
        wal_dir,
    )
    .await
    {
        Ok(delta) => Ok(delta),
        Err(e) => {
            tracing::warn!(
                target = "backup_push",
                "delta sidecars unusable ({e:#}); re-parsing raw WAL [{}, {})",
                format_pg_lsn(start_lsn),
                format_pg_lsn(end_lsn),
            );
            build_delta_map_from_wal_full(
                settings,
                storage,
                timeline,
                start_lsn,
                end_lsn,
                compression,
                wal_dir,
            )
            .await
        }
    }
}

/// Sidecar-driven build: a raw-WAL walk of the leading partial group, delta
/// files for the whole groups between, and a raw-WAL walk of the trailing
/// partial group. One `WalParser` threads across every group so a sidecar-less
/// group's raw walk stitches its leading boundary record from the prior group's
/// trailing head; a fold adopts the sidecar's own saved parser, authoritative
/// whichever path produced the previous group.
///
/// A complete group whose sidecar is absent is raw-walked rather than erroring
/// the whole range. The archiver can't finalize a group whose preceding segment
/// it never recorded (no prev_head to seed) — the first complete group after a
/// recording start that lands on a group boundary — so that one object is
/// legitimately missing; walking it raw costs one group's reparse instead of a
/// full-range fallback. Other errors (corrupt/undecodable sidecar) still
/// propagate so the caller falls back to the full walk.
///
/// `start_lsn` lands mid-group, so its group's sidecar would cover pre-`start_lsn`
/// segments the parent full never archived (the group never finalized, so no
/// object exists). First usable sidecar is the next group-aligned boundary; the
/// leading partial is walked raw, mirroring wal-g `getDeltaMap` which uses a
/// delta file only for a segment beginning a complete in-range group
async fn build_delta_map_from_sidecars(
    settings: &crate::config::Settings,
    storage: &Operator,
    timeline: u32,
    start_lsn: u64,
    end_lsn: u64,
    compression: compression::Method,
    wal_dir: Option<&Path>,
) -> Result<PagedFileDeltaMap> {
    let seg_size = wal_segment_size();
    let n = WAL_FILES_IN_DELTA;
    let start_seg = lsn_to_seg(start_lsn, seg_size);
    let first_not_used_delta = delta_group_no(lsn_to_seg(end_lsn, seg_size));
    if first_not_used_delta < n {
        anyhow::bail!(
            "range [{}, {}) has no complete delta group ahead of it",
            format_pg_lsn(start_lsn),
            format_pg_lsn(end_lsn)
        );
    }
    let last_complete_group = first_not_used_delta - n;

    // First group-aligned boundary at/after start_seg; leading partial walked raw
    let lead_group = delta_group_no(start_seg);
    let first_complete = if start_seg == lead_group {
        lead_group
    } else {
        lead_group + n
    };
    if first_complete > last_complete_group {
        anyhow::bail!(
            "range [{}, {}) spans no complete delta group",
            format_pg_lsn(start_lsn),
            format_pg_lsn(end_lsn)
        );
    }

    let ctx = WalWalkCtx {
        settings,
        storage,
        timeline,
        seg_size,
        compression,
        wal_dir,
    };
    let mut delta = PagedFileDeltaMap::new();
    // Threaded across every group: a sidecar-less group's raw walk stitches its
    // leading boundary record from the prior group's trailing head; a fold
    // replaces it with the sidecar's self-contained saved parser
    let mut parser = WalParser::new();

    // Leading partial group: raw WAL from start_seg to the first aligned group.
    // Records attribute by start position, so this and the first sidecar partition
    // cleanly
    if start_seg < first_complete {
        walk_segments_pipelined(&ctx, start_seg, first_complete - 1, &mut parser, &mut delta)
            .await?;
    }

    // Every complete group: fold its sidecar when present, else raw-walk the
    // group (the archiver leaves no sidecar for a group it started recording on a
    // boundary, with no preceding segment to seed prev_head)
    let mut g = first_complete;
    while g <= last_complete_group {
        let name = delta_group_name(timeline, g, seg_size);
        let key = delta_storage_key(&name, compression);
        if storage
            .exists(&key)
            .await
            .with_context(|| format!("stat {key}"))?
        {
            let (d, p) = fold_sidecar_into_map(settings, storage, &name, compression, delta)
                .await
                .with_context(|| format!("delta sidecar {name}"))?;
            delta = d;
            parser = p;
        } else {
            tracing::info!(
                target = "backup_push",
                "delta sidecar {name} absent; raw-walking group"
            );
            walk_segments_pipelined(&ctx, g, g + n - 1, &mut parser, &mut delta).await?;
        }
        g += n;
    }

    // Trailing partial group: raw WAL from the group start up to end_lsn, seeded
    // from the last complete group's trailing head
    let tail_first = first_not_used_delta;
    let tail_last = lsn_to_seg(end_lsn.saturating_sub(1), seg_size);
    walk_segments_pipelined(&ctx, tail_first, tail_last, &mut parser, &mut delta).await?;
    Ok(delta)
}

/// Fetch a `<group>_delta` sidecar from `wal_005/` and fold its location tuples
/// straight into `map`, returning the trailing `WalParser` state. Streams 16-byte
/// tuples through a `SyncIoBridge` so the group's locations are never collected
/// into a `Vec` — they land in the roaring map a tuple at a time
async fn fold_sidecar_into_map(
    settings: &crate::config::Settings,
    storage: &Operator,
    group_name: &str,
    compression: compression::Method,
    map: PagedFileDeltaMap,
) -> Result<(PagedFileDeltaMap, WalParser)> {
    let key = delta_storage_key(group_name, compression);
    let r = storage
        .get(&key)
        .await
        .with_context(|| format!("get {key}"))?;
    let decrypted = settings.decrypt(r);
    let decoded = compression::decode(compression, decrypted);
    tokio::task::spawn_blocking(move || fold_sidecar_stream(decoded, map))
        .await
        .context("join sidecar fold")?
        .with_context(|| format!("decode {key}"))
}

/// Sync side of [`fold_sidecar_into_map`]: read tuples until the all-zero
/// terminator, then the parser state. EOF before the terminator means a
/// truncated sidecar (interrupted upload/finalization) — error rather than
/// accept a partial map and a bogus empty parser, so the caller falls back to
/// a complete raw-WAL walk
fn fold_sidecar_stream(
    decoded: compression::AsyncReader,
    mut map: PagedFileDeltaMap,
) -> Result<(PagedFileDeltaMap, WalParser)> {
    let mut r = SyncIoBridge::new(decoded);
    let mut buf = [0u8; 16];
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                anyhow::bail!("sidecar truncated: EOF before terminal tuple")
            }
            Err(e) => return Err(anyhow::Error::from(e).context("read sidecar tuple")),
        }
        let loc = BlockLocation::new(
            u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        );
        if loc.is_terminal() {
            let parser = WalParser::load(&mut r).context("load sidecar parser state")?;
            return Ok((map, parser));
        }
        map.add_location(loc);
    }
}

/// Full raw-WAL walk of `[start_lsn, end_lsn)`: parse every segment. Fallback
/// when sidecars are absent; O(WAL volume). A missing or corrupt segment errors:
/// the whole range is required WAL, so the caller takes a full backup rather than
/// recording a delta that silently omits the skipped segment's pages
///
/// Each segment parses independently with a fresh parser, fanned across cores,
/// so the changed-block walk scales past the single-thread CPU bound the serial
/// walk hit. Records crossing a segment boundary are stitched from the saved
/// head/tail fragments. Degrades to the serial threaded walk when a record
/// spans more than one segment (longer than a segment), which the per-segment
/// model can't represent
async fn build_delta_map_from_wal_full(
    settings: &crate::config::Settings,
    storage: &Operator,
    timeline: u32,
    start_lsn: u64,
    end_lsn: u64,
    compression: compression::Method,
    wal_dir: Option<&Path>,
) -> Result<PagedFileDeltaMap> {
    let seg_size = wal_segment_size();
    if end_lsn <= start_lsn {
        return Ok(PagedFileDeltaMap::new());
    }
    let first_seg = lsn_to_seg(start_lsn, seg_size);
    let last_seg = lsn_to_seg(end_lsn.saturating_sub(1), seg_size);
    let ctx = WalWalkCtx {
        settings,
        storage,
        timeline,
        seg_size,
        compression,
        wal_dir,
    };
    if let Some(delta) = parse_segments_parallel(&ctx, first_seg, last_seg).await? {
        return Ok(delta);
    }
    tracing::warn!(
        target = "backup_push",
        "parallel reparse hit a record spanning >1 segment; serial walk [{first_seg}, {last_seg}]",
    );
    let mut delta = PagedFileDeltaMap::new();
    let mut parser = WalParser::new();
    walk_segments_pipelined(&ctx, first_seg, last_seg, &mut parser, &mut delta).await?;
    Ok(delta)
}

/// Shared, `'static` slice of [`WalWalkCtx`] for the spawned fetch+parse tasks
struct SegFetch {
    settings: crate::config::Settings,
    storage: Operator,
    timeline: u32,
    seg_size: u64,
    compression: compression::Method,
    wal_dir: Option<PathBuf>,
}

/// One segment's parse result, tagged by its offset from `first_seg` so the
/// completion handler can place fragments in segment order for boundary stitch
struct SegOut {
    rel: usize,
    result: Result<(Vec<BlockLocation>, SegmentBoundary)>,
}

/// Parse `[first_seg, last_seg]` raw WAL concurrently: fetch + parse each
/// segment independently on a bounded fan-out, union the per-segment changed
/// blocks, then stitch the record crossing each segment boundary back together
/// from the saved head/tail fragments.
///
/// Returns `Ok(None)` when the range holds a record spanning more than one
/// segment boundary — the per-segment model can't reconstruct it, so the caller
/// re-runs the threaded serial walk. A missing or corrupt segment is a hard
/// error: every segment in the range is required WAL, so skipping one would drop
/// its changed pages from the increment and silently restore stale parent data
async fn parse_segments_parallel(
    ctx: &WalWalkCtx<'_>,
    first_seg: u64,
    last_seg: u64,
) -> Result<Option<PagedFileDeltaMap>> {
    let count = (last_seg - first_seg + 1) as usize;
    let fetch = Arc::new(SegFetch {
        settings: ctx.settings.clone(),
        storage: ctx.storage.clone(),
        timeline: ctx.timeline,
        seg_size: ctx.seg_size,
        compression: ctx.compression,
        wal_dir: ctx.wal_dir.map(Path::to_path_buf),
    });

    let mut delta = PagedFileDeltaMap::new();
    let mut fragments: Vec<Option<SegmentBoundary>> = (0..count).map(|_| None).collect();

    let concurrency = ctx.settings.download_concurrency + 1;
    {
        let delta = &mut delta;
        let fragments = &mut fragments;
        let timeline = ctx.timeline;
        let seg_size = ctx.seg_size;
        let mut tasks = BoundedTasks::new(concurrency, "wal-parse", move |out: SegOut| {
            let rel = out.rel;
            let (locs, boundary) = out.result.with_context(|| {
                let name = seg_name_from_global(timeline, first_seg + rel as u64, seg_size);
                format!("wal segment {}", name.format())
            })?;
            delta.add_locations(locs);
            fragments[rel] = Some(boundary);
            Ok(())
        });
        for seg in first_seg..=last_seg {
            let fetch = fetch.clone();
            let rel = (seg - first_seg) as usize;
            tasks
                .spawn(async move {
                    let result = fetch_and_walk_segment(&fetch, seg).await;
                    SegOut { rel, result }
                })
                .await?;
        }
        tasks.join().await?;
    }

    // A fragment ending mid-record but not at a record start means a record
    // spans more than this segment + the next — pairwise stitching can't
    // recover it. Bail to the threaded serial walk
    if fragments
        .iter()
        .flatten()
        .any(|f| !f.trailing_is_record_start && !f.trailing_head.is_empty())
    {
        return Ok(None);
    }

    // Stitch each boundary record: head of segment i + leading tail of i+1.
    // Every fragment is present: a fetch/parse error aborts join() above, so a
    // None here is an internal invariant break, not a recoverable gap
    for rel in 0..count.saturating_sub(1) {
        let (Some(head), Some(tail)) = (&fragments[rel], &fragments[rel + 1]) else {
            anyhow::bail!("missing parsed WAL fragment at offset {rel} after successful walk");
        };
        if head.trailing_head.is_empty() {
            continue; // record ended exactly at the boundary
        }
        let mut data = Vec::with_capacity(head.trailing_head.len() + tail.leading_tail.len());
        data.extend_from_slice(&head.trailing_head);
        data.extend_from_slice(&tail.leading_tail);
        match parse_record_from_bytes(&data, head.page_magic) {
            Ok(rec) => delta.add_locations(extract_block_locations(std::slice::from_ref(&rec))),
            Err(e) => {
                let name = seg_name_from_global(ctx.timeline, first_seg + rel as u64, ctx.seg_size)
                    .format();
                tracing::warn!(
                    target = "backup_push",
                    "boundary record after segment {name} unparseable ({e}); serial walk",
                );
                return Ok(None);
            }
        }
    }
    Ok(Some(delta))
}

/// Fetch one segment and parse it with a fresh per-segment parser on the
/// blocking pool, returning its in-segment block locations + boundary fragments
async fn fetch_and_walk_segment(
    fetch: &SegFetch,
    seg: u64,
) -> Result<(Vec<BlockLocation>, SegmentBoundary)> {
    let name = seg_name_from_global(fetch.timeline, seg, fetch.seg_size).format();
    let buf = fetch_segment(
        &fetch.settings,
        &fetch.storage,
        fetch.compression,
        fetch.wal_dir.as_deref(),
        &name,
    )
    .await?;
    tokio::task::spawn_blocking(move || {
        let mut locs = Vec::new();
        let boundary = walk_segment_locations(&buf, |l| locs.push(l))
            .with_context(|| format!("parse segment {name}"))?;
        Ok((locs, boundary))
    })
    .await
    .context("join segment parse")?
}

fn lsn_to_seg(lsn: u64, seg_size: u64) -> u64 {
    lsn / seg_size
}

/// Per-walk invariants shared across every segment of a raw-WAL walk
struct WalWalkCtx<'a> {
    settings: &'a crate::config::Settings,
    storage: &'a Operator,
    timeline: u32,
    seg_size: u64,
    compression: compression::Method,
    /// Local `pg_wal` to read raw segments from before falling back to the
    /// archive; `None` when the push has no local data dir
    wal_dir: Option<&'a Path>,
}

/// Walk raw WAL segments `[first_seg, last_seg]` into `delta`, prefetching the
/// next segment's bytes while parsing the current one (download+decode overlaps
/// the CPU-bound parse). Parsing stays serial: WAL records span segment
/// boundaries, so `parser` state threads across iterations. A missing or corrupt
/// segment is a hard error: every segment in the range is required WAL, so
/// skipping one would silently drop its changed pages from the increment
async fn walk_segments_pipelined(
    ctx: &WalWalkCtx<'_>,
    first_seg: u64,
    last_seg: u64,
    parser: &mut WalParser,
    delta: &mut PagedFileDeltaMap,
) -> Result<()> {
    if first_seg > last_seg {
        return Ok(());
    }
    let name = |s: u64| seg_name_from_global(ctx.timeline, s, ctx.seg_size).format();

    // Prime the pipeline, then carry each prefetch into the next iteration; the
    // prefetch yields None past the last segment, ending the loop
    let mut pending = Some(
        fetch_segment(
            ctx.settings,
            ctx.storage,
            ctx.compression,
            ctx.wal_dir,
            &name(first_seg),
        )
        .await,
    );
    let mut seg = first_seg;
    while let Some(fetched) = pending.take() {
        let cur = name(seg);
        let buf = fetched.with_context(|| format!("wal segment {cur}"))?;
        // Parse current segment on the blocking pool while the next prefetches.
        // Parser is moved in and returned via the join handle so cross-segment
        // record-stitching state survives
        let parser_in = std::mem::take(parser);
        let parse_handle = tokio::task::spawn_blocking(move || {
            let mut parser_in = parser_in;
            let res = extract_locations_from_wal_file(&mut parser_in, io::Cursor::new(buf));
            (parser_in, res)
        });

        let (joined, next) = tokio::join!(parse_handle, async {
            if seg < last_seg {
                Some(
                    fetch_segment(
                        ctx.settings,
                        ctx.storage,
                        ctx.compression,
                        ctx.wal_dir,
                        &name(seg + 1),
                    )
                    .await,
                )
            } else {
                None
            }
        });

        let (parser_out, locs) = joined.context("join segment walk")?;
        *parser = parser_out;
        delta.add_locations(locs.with_context(|| format!("parse segment {cur}"))?);

        pending = next;
        seg += 1;
    }
    Ok(())
}

/// Read one WAL segment fully into memory so the next segment can prefetch while
/// this one parses. Bounded at ~2 × seg_size in flight.
/// Bounded wait for the archiver to ship a just-switched-out WAL segment.
/// full-jitter, ~11s worst case (sum of capped backoffs over 10 attempts)
const WAL_ARCHIVE_WAIT: RetryPolicy = RetryPolicy {
    max_attempts: 10,
    base_delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(2),
    jitter: true,
};

async fn fetch_segment(
    settings: &crate::config::Settings,
    storage: &Operator,
    compression: compression::Method,
    wal_dir: Option<&Path>,
    name: &str,
) -> Result<Vec<u8>> {
    if let Some(dir) = wal_dir {
        match tokio::fs::read(dir.join(name)).await {
            Ok(buf) => return Ok(buf),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("read local wal segment {name}")),
        }
    }
    let ext = compression.extension();
    let key = if ext.is_empty() {
        format!("{}/{}", crate::pg::WAL_FOLDER, name)
    } else {
        format!("{}/{}.{}", crate::pg::WAL_FOLDER, name, ext)
    };
    // A delta range's trailing segment is the one BASE_BACKUP forced a switch out
    // of at start (PG do_pg_backup_start -> RequestXLogSwitch), so PG's async
    // archive_command may not have shipped it to the bucket yet. It is switched
    // out, so it will arrive — wait out archiver lag on NotFound before the caller
    // gives up to a full backup. Transient errors are already retried in storage
    let r = with_retry(
        &WAL_ARCHIVE_WAIT,
        |e: &opendal::Error| crate::storage::is_not_found(e),
        || async { storage.get(&key).await },
    )
    .await
    .with_context(|| format!("get {key}"))?;
    let decrypted = settings.decrypt(r);
    let mut decoded = compression::decode(compression, decrypted);
    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut decoded, &mut buf)
        .await
        .with_context(|| format!("read segment {name}"))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;

    #[tokio::test]
    async fn delta_parent_carries_increment_format() {
        use crate::config::DeltaSettings;
        use crate::pg::backup::{BackupSentinelDto, format_backup_name, sentinel_key};

        let seg = DEFAULT_WAL_SEG_SIZE;

        let sentinel = |from: Option<&str>, fmt: increment::Format| BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: NonZeroU64::new(seg),
                increment_from_lsn: from.and_then(|_| NonZeroU64::new(seg / 2)),
                increment_from: from.map(String::from),
                increment_full_name: from.map(String::from),
                increment_count: from.map(|_| 1),
                increment_format: fmt,
                pg_version: 170000,
                backup_finish_lsn: NonZeroU64::new(seg + 1),
                ..Default::default()
            },
            hostname: "h".into(),
            data_dir: "/d".into(),
            ..Default::default()
        };

        // Parent is itself a delta → its format constrains the new push
        let probe = |v2: BackupSentinelDtoV2| async move {
            let delta = DeltaSettings {
                max_steps: 5,
                ..Default::default()
            };
            let dir = tempfile::tempdir().unwrap();
            let storage: Operator = crate::storage::fs_operator(dir.path());
            let name = format_backup_name(1, seg, seg);
            let body = serde_json::to_vec(&v2).unwrap();
            let len = body.len() as u64;
            let r: crate::compression::AsyncReader = Box::pin(std::io::Cursor::new(body));
            storage
                .put(&sentinel_key(&name), r, Some(len))
                .await
                .unwrap();
            configure_delta_parent(&storage, &delta, false)
                .await
                .unwrap()
                .unwrap()
        };

        let from_delta = probe(sentinel(Some("base_root"), increment::Format::Native)).await;
        assert_eq!(
            from_delta.parent_increment_format,
            Some(increment::Format::Native)
        );

        // Full parent starts a fresh chain → no format constraint
        let from_full = probe(sentinel(None, increment::Format::Wi1)).await;
        assert_eq!(from_full.parent_increment_format, None);
    }

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
        assert_eq!(seg0, vec![5u32]);

        let seg1 = m.blocks_for("base/16384/16385.1").unwrap().unwrap();
        assert_eq!(seg1, vec![3u32]);

        let seg2 = m.blocks_for("base/16384/16385.2").unwrap().unwrap();
        assert_eq!(seg2, vec![9u32]);

        let seg3 = m.blocks_for("base/16384/16385.3").unwrap().unwrap();
        assert!(seg3.is_empty()); // file has segment but no dirty blocks
    }

    #[test]
    fn delta_map_unchanged_relfile_returns_none() {
        let m = PagedFileDeltaMap::new();
        assert!(m.blocks_for("base/16384/16385").unwrap().is_none());
    }

    #[test]
    fn sidecar_format_round_trip() {
        // Bytes the streaming writer emits: tuples + terminator + parser state
        use crate::pg::walparser::{read_locations_from, write_locations_to};
        let mut buf = Vec::new();
        write_locations_to(
            &mut buf,
            &[
                BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 7),
                BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16386, 0),
            ],
        )
        .unwrap();
        WalParser::new().save(&mut buf).unwrap();

        let mut cur = buf.as_slice();
        let locs = read_locations_from(&mut cur).unwrap();
        assert_eq!(locs.len(), 2);
        assert_eq!(locs[0].block_no, 7);
        assert_eq!(locs[1].block_no, 0);
        let wp = WalParser::load(&mut cur).unwrap();
        assert!(wp.current_record_data().is_empty());
    }

    #[test]
    fn delta_map_is_empty_tracks_contents() {
        let mut m = PagedFileDeltaMap::new();
        assert!(m.is_empty());
        m.add_location(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 0));
        assert!(!m.is_empty());
    }

    async fn seed_sentinel(storage: &Operator, name: &str, user_data: serde_json::Value) {
        use crate::pg::backup::{BackupSentinelDto, sentinel_key};
        let v2 = BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: NonZeroU64::new(DEFAULT_WAL_SEG_SIZE),
                backup_finish_lsn: NonZeroU64::new(DEFAULT_WAL_SEG_SIZE + 1),
                pg_version: 170000,
                user_data: Some(user_data),
                ..Default::default()
            },
            hostname: "h".into(),
            data_dir: "/d".into(),
            ..Default::default()
        };
        let body = serde_json::to_vec(&v2).unwrap();
        let len = body.len() as u64;
        let r: crate::compression::AsyncReader = Box::pin(std::io::Cursor::new(body));
        storage
            .put(&sentinel_key(name), r, Some(len))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn find_by_user_data_matches_sentinel() {
        use crate::pg::backup::format_backup_name;
        let seg = DEFAULT_WAL_SEG_SIZE;
        let dir = tempfile::tempdir().unwrap();
        let storage: Operator = crate::storage::fs_operator(dir.path());
        let a = format_backup_name(1, seg, seg);
        let b = format_backup_name(1, seg * 3, seg);
        seed_sentinel(&storage, &a, serde_json::json!({"label": "alpha"})).await;
        seed_sentinel(&storage, &b, serde_json::json!({"label": "beta"})).await;

        let (name, v2) = find_by_user_data(&storage, r#"{"label":"beta"}"#)
            .await
            .unwrap()
            .expect("a sentinel matches the needle");
        assert_eq!(name, b);
        assert_eq!(
            v2.sentinel.user_data,
            Some(serde_json::json!({"label": "beta"}))
        );

        // No match -> Ok(None)
        assert!(
            find_by_user_data(&storage, r#"{"label":"absent"}"#)
                .await
                .unwrap()
                .is_none()
        );
        // Non-JSON needle -> Err
        assert!(find_by_user_data(&storage, "not json").await.is_err());
    }

    #[tokio::test]
    async fn fold_sidecar_into_map_reads_blocks() {
        use crate::pg::walparser::write_locations_to;
        let dir = tempfile::tempdir().unwrap();
        let storage: Operator = crate::storage::fs_operator(dir.path());
        let settings = crate::config::Settings::default();
        let method = compression::Method::None;

        // Sidecar bytes: one tuple + terminator + parser state
        let mut raw = Vec::new();
        write_locations_to(
            &mut raw,
            &[BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 7)],
        )
        .unwrap();
        WalParser::new().save(&mut raw).unwrap();

        let group = delta_group_name(1, 0, DEFAULT_WAL_SEG_SIZE);
        let key = delta_storage_key(&group, method);
        let len = raw.len() as u64;
        let r: crate::compression::AsyncReader = Box::pin(std::io::Cursor::new(raw));
        storage.put(&key, r, Some(len)).await.unwrap();

        let (map, parser) = fold_sidecar_into_map(
            &settings,
            &storage,
            &group,
            method,
            PagedFileDeltaMap::new(),
        )
        .await
        .unwrap();
        let blocks = map.blocks_for("base/16384/16385").unwrap().unwrap();
        assert_eq!(blocks.into_iter().collect::<Vec<_>>(), vec![7u32]);
        assert!(parser.current_record_data().is_empty());
    }

    #[tokio::test]
    async fn fold_sidecar_truncated_before_terminator_errors() {
        use crate::pg::walparser::write_location_tuples;
        let dir = tempfile::tempdir().unwrap();
        let storage: Operator = crate::storage::fs_operator(dir.path());
        let settings = crate::config::Settings::default();
        let method = compression::Method::None;

        // Tuples with no terminator + parser state: truncated upload
        let mut raw = Vec::new();
        write_location_tuples(
            &mut raw,
            &[BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 7)],
        )
        .unwrap();

        let group = delta_group_name(1, 0, DEFAULT_WAL_SEG_SIZE);
        let key = delta_storage_key(&group, method);
        let len = raw.len() as u64;
        let r: crate::compression::AsyncReader = Box::pin(std::io::Cursor::new(raw));
        storage.put(&key, r, Some(len)).await.unwrap();

        let err = fold_sidecar_into_map(
            &settings,
            &storage,
            &group,
            method,
            PagedFileDeltaMap::new(),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("truncated"), "{err:#}");
    }

    #[test]
    fn merge_unions_overlapping_rels() {
        // Summaries + a raw-walked gap each touch the same rel; merge must union
        // their blocks, not replace
        let mut a = PagedFileDeltaMap::new();
        a.add_location(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 1));
        a.add_location(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 3));
        let mut b = PagedFileDeltaMap::new();
        b.add_location(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 3));
        b.add_location(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16385, 5));
        b.add_location(BlockLocation::new(DEFAULT_SPC_NODE, 16384, 16386, 0));
        a.merge(b);
        assert_eq!(
            a.blocks_for("base/16384/16385").unwrap().unwrap(),
            vec![1u32, 3, 5]
        );
        assert_eq!(
            a.blocks_for("base/16384/16386").unwrap().unwrap(),
            vec![0u32]
        );
    }
}
