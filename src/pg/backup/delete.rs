//! Retention. Mirrors wal-g's `delete` family across `before`, `retain`,
//! `everything`, `target`, `garbage` modes
//!
//! Comparison key for every object is `(timeline, segment_global_no)` extracted
//! from a 24-hex chunk in the name. Backups order by their start LSN; WAL
//! segments order by their natural archive sequence. wal-g's
//! `timelineAndSegmentNoLess` semantics
//!
//! Permanent backups are skipped; their reserved WAL range
//! (start LSN through finish LSN, inclusive on the segment grid) is skipped too

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Serialize;

use crate::pg::backup::fetch::fetch_sentinel;
use crate::pg::backup::{LATEST, name_from_sentinel_key, strip_leftmost_backup_name};
use crate::pg::wal::segment::{SegmentName, wal_segment_size};
use crate::storage::DynStorage;

/// `TryFetchTimelineAndLogSegNo`: find the first 24-hex chunk in `name`,
/// parse as TTTTTTTTLLLLLLLLSSSSSSSS, return (timeline, global_seg_no)
///
/// global_seg_no = log_id * SEGS_PER_XLOG_ID + seg_no (16 MiB segs => 256/log_id)
pub fn try_extract_timeline_seg_no(name: &str) -> Option<(u32, u64)> {
    let bytes = name.as_bytes();
    let mut i = 0;
    while i + 24 <= bytes.len() {
        if bytes[i..i + 24].iter().all(|b| b.is_ascii_hexdigit()) {
            // Ensure boundary char isn't itself hex (matches `[0-9A-F]{24}` longest)
            let left_ok = i == 0 || !bytes[i - 1].is_ascii_hexdigit();
            let right_ok = i + 24 == bytes.len() || !bytes[i + 24].is_ascii_hexdigit();
            if left_ok && right_ok {
                let s = std::str::from_utf8(&bytes[i..i + 24]).ok()?;
                let seg = SegmentName::parse(s).ok()?;
                let segs_per_log = 0x1_0000_0000u64 / wal_segment_size();
                let global = (seg.log_id as u64) * segs_per_log + seg.seg_no as u64;
                return Some((seg.timeline, global));
            }
        }
        i += 1;
    }
    None
}

#[derive(Debug, Clone)]
pub struct BackupRecord {
    pub name: String,
    pub timeline: u32,
    pub start_seg_no: u64,
    pub start_lsn: u64,
    pub finish_lsn: u64,
    pub start_time: DateTime<Utc>,
    pub is_permanent: bool,
    pub increment_from: Option<String>,
    pub increment_full_name: Option<String>,
}

impl BackupRecord {
    pub fn is_full_backup(&self) -> bool {
        self.increment_full_name.is_none()
    }

    /// For a full backup, that's its own name; for a delta, the chain root
    pub fn base_backup_name(&self) -> &str {
        self.increment_full_name.as_deref().unwrap_or(&self.name)
    }
}

/// Enumerate every sentinel under `basebackups_005/`, build a BackupRecord
/// for each. Backups with unreadable sentinels are skipped with a warning
pub async fn collect_records(storage: &DynStorage) -> Result<Vec<BackupRecord>> {
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let mut names: Vec<String> = Vec::new();
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        if let Some(name) = name_from_sentinel_key(&obj.key) {
            names.push(name.to_string());
        }
    }
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        match fetch_record(storage, &name).await {
            Ok(r) => out.push(r),
            Err(e) => {
                tracing::warn!(target = "delete", "skip {name}: {e:#}");
            }
        }
    }
    out.sort_by(|a, b| {
        (a.timeline, a.start_seg_no, a.start_time).cmp(&(b.timeline, b.start_seg_no, b.start_time))
    });
    Ok(out)
}

async fn fetch_record(storage: &DynStorage, name: &str) -> Result<BackupRecord> {
    let v2 = fetch_sentinel(storage, name).await?;
    let (timeline, start_seg_no) = try_extract_timeline_seg_no(name)
        .ok_or_else(|| anyhow!("cannot derive timeline / segment from backup name {name}"))?;
    Ok(BackupRecord {
        name: name.to_string(),
        timeline,
        start_seg_no,
        start_lsn: v2.sentinel.backup_start_lsn.unwrap_or(0),
        finish_lsn: v2.sentinel.backup_finish_lsn.unwrap_or(0),
        start_time: v2.start_time,
        is_permanent: v2.is_permanent,
        increment_from: v2.sentinel.increment_from.clone(),
        increment_full_name: v2.sentinel.increment_full_name.clone(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteModifier {
    None,
    Full,
    FindFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarbageScope {
    All,
    Archives,
    Backups,
}

#[derive(Debug, Clone)]
pub enum DeleteOp {
    /// Delete every object older than (timeline, seg_no) of the resolved target backup
    Before {
        /// RFC3339 timestamp or backup name prefix (`base_…`)
        target: String,
        modifier: DeleteModifier,
    },
    /// Keep N most-recent backups; delete-before-target the rest.
    /// `after` (timestamp or backup-name prefix) additionally keeps every
    /// backup at or newer than the resolved boundary
    Retain {
        count: usize,
        modifier: DeleteModifier,
        after: Option<String>,
    },
    /// Wipe basebackups + WAL. Refuses when any permanent backup exists unless `force`
    Everything { force: bool },
    /// Delete one backup and its dependants (FindFull deletes the whole chain)
    Target {
        name: String,
        modifier: DeleteModifier,
    },
    /// Find oldest non-permanent backup; delete everything older than it
    Garbage { scope: GarbageScope },
}

#[derive(Debug, Clone, Serialize)]
pub struct DeletePlan {
    pub objects: Vec<String>,
    pub kept_permanent_backups: Vec<String>,
    pub target: Option<String>,
}

pub async fn handle(storage: DynStorage, op: DeleteOp, confirm: bool) -> Result<DeletePlan> {
    let backups = collect_records(&storage).await?;
    let plan = plan_delete(&storage, &backups, &op).await?;
    print_plan(&plan, confirm);
    if confirm && !plan.objects.is_empty() {
        execute_delete(&storage, &plan.objects).await?;
    }
    Ok(plan)
}

async fn plan_delete(
    storage: &DynStorage,
    backups: &[BackupRecord],
    op: &DeleteOp,
) -> Result<DeletePlan> {
    match op {
        DeleteOp::Before { target, modifier } => {
            plan_before(storage, backups, target, *modifier).await
        }
        DeleteOp::Retain {
            count,
            modifier,
            after,
        } => plan_retain(storage, backups, *count, *modifier, after.as_deref()).await,
        DeleteOp::Everything { force } => plan_everything(storage, backups, *force).await,
        DeleteOp::Target { name, modifier } => plan_target(storage, backups, name, *modifier).await,
        DeleteOp::Garbage { scope } => plan_garbage(storage, backups, *scope).await,
    }
}

// ── before & retain (share `delete-before-target` core) ────────────────────

async fn plan_before(
    storage: &DynStorage,
    backups: &[BackupRecord],
    target: &str,
    modifier: DeleteModifier,
) -> Result<DeletePlan> {
    let resolved = resolve_before_target(backups, target, modifier)?;
    let Some(target_record) = resolved else {
        return Ok(DeletePlan {
            objects: Vec::new(),
            kept_permanent_backups: permanent_names(backups),
            target: None,
        });
    };
    delete_before_target_plan(storage, backups, &target_record, None).await
}

async fn plan_retain(
    storage: &DynStorage,
    backups: &[BackupRecord],
    count: usize,
    modifier: DeleteModifier,
    after: Option<&str>,
) -> Result<DeletePlan> {
    if count == 0 {
        bail!("retain count must be >= 1");
    }
    let resolved = match after {
        Some(s) => resolve_retain_after_target(backups, count, s, modifier)?,
        None => resolve_retain_target(backups, count, modifier),
    };
    let Some(target_record) = resolved else {
        return Ok(DeletePlan {
            objects: Vec::new(),
            kept_permanent_backups: permanent_names(backups),
            target: None,
        });
    };
    delete_before_target_plan(storage, backups, &target_record, None).await
}

/// Resolve the target backup for `delete before <name|time> [FIND_FULL]`.
/// Algorithm mirrors wal-g: walk newest -> oldest; first match is target;
/// in FIND_FULL mode skip past matches until we hit a full backup
fn resolve_before_target(
    backups: &[BackupRecord],
    target: &str,
    modifier: DeleteModifier,
) -> Result<Option<BackupRecord>> {
    if matches!(modifier, DeleteModifier::Full) {
        bail!("`delete before FULL` is not a supported modifier");
    }
    let time_target = DateTime::parse_from_rfc3339(target)
        .map(|d| d.with_timezone(&Utc))
        .ok();
    if let Some(t) = time_target
        && t > Utc::now()
    {
        bail!("cannot delete before a future timestamp");
    }

    // newest-first walk
    let mut sorted: Vec<&BackupRecord> = backups.iter().collect();
    sorted.sort_by(|a, b| {
        (b.timeline, b.start_seg_no, b.start_time).cmp(&(a.timeline, a.start_seg_no, a.start_time))
    });

    let mut hit = false;
    for b in &sorted {
        let matches = if let Some(t) = time_target {
            b.start_time <= t
        } else {
            b.name.starts_with(target)
        };
        if !hit && matches {
            hit = true;
        }
        if hit {
            match modifier {
                DeleteModifier::None => return Ok(Some((*b).clone())),
                DeleteModifier::FindFull => {
                    if b.is_full_backup() {
                        return Ok(Some((*b).clone()));
                    }
                }
                DeleteModifier::Full => unreachable!(),
            }
        }
    }
    Ok(None)
}

/// Resolve the target for `retain [FULL|FIND_FULL] N`.
/// Walk newest -> oldest; for `None` modifier the Nth backup is target;
/// for `FULL` the Nth full backup is target; for `FIND_FULL` we count any
/// backup but only stop on a full once N is reached
fn resolve_retain_target(
    backups: &[BackupRecord],
    count: usize,
    modifier: DeleteModifier,
) -> Option<BackupRecord> {
    let mut sorted: Vec<&BackupRecord> = backups.iter().collect();
    sorted.sort_by(|a, b| {
        (b.timeline, b.start_seg_no, b.start_time).cmp(&(a.timeline, a.start_seg_no, a.start_time))
    });
    let mut seen = 0usize;
    for b in &sorted {
        match modifier {
            DeleteModifier::None => {
                seen += 1;
                if seen == count {
                    return Some((*b).clone());
                }
            }
            DeleteModifier::Full => {
                if b.is_full_backup() {
                    seen += 1;
                    if seen == count {
                        return Some((*b).clone());
                    }
                }
            }
            DeleteModifier::FindFull => {
                seen += 1;
                if seen >= count && b.is_full_backup() {
                    return Some((*b).clone());
                }
            }
        }
    }
    None
}

/// Resolve target for `retain N --after <ts|name>`. Picks the older of two
/// anchors so the surviving set is `(N newest) ∪ (every backup at-or-after the
/// boundary)`. Mirrors wal-g's `FindTargetRetainAfter*`
fn resolve_retain_after_target(
    backups: &[BackupRecord],
    count: usize,
    after: &str,
    modifier: DeleteModifier,
) -> Result<Option<BackupRecord>> {
    let time_target = DateTime::parse_from_rfc3339(after)
        .map(|d| d.with_timezone(&Utc))
        .ok();
    if let Some(t) = time_target
        && t > Utc::now()
    {
        bail!("cannot retain after a future timestamp");
    }
    let t1 = resolve_retain_target(backups, count, modifier);
    let t2 = resolve_after_target(backups, after, time_target, modifier);
    Ok(match (t1, t2) {
        (None, None) => None,
        (Some(x), None) | (None, Some(x)) => Some(x),
        (Some(a), Some(b)) => {
            let key = |r: &BackupRecord| (r.timeline, r.start_seg_no, r.start_time);
            if key(&a) <= key(&b) { Some(a) } else { Some(b) }
        }
    })
}

/// Walk oldest -> newest; first backup at-or-after the boundary is the anchor.
/// For non-`None` modifier only full backups can anchor (matches wal-g).
/// For the name form, the boundary "latches" once the named backup is seen
fn resolve_after_target(
    backups: &[BackupRecord],
    after_str: &str,
    time_target: Option<DateTime<Utc>>,
    modifier: DeleteModifier,
) -> Option<BackupRecord> {
    let mut sorted: Vec<&BackupRecord> = backups.iter().collect();
    sorted.sort_by(|a, b| {
        (a.timeline, a.start_seg_no, a.start_time).cmp(&(b.timeline, b.start_seg_no, b.start_time))
    });
    let mut met_name = false;
    for b in &sorted {
        let candidate = if let Some(t) = time_target {
            b.start_time >= t
        } else {
            met_name = met_name || b.name.starts_with(after_str);
            met_name
        };
        if !candidate {
            continue;
        }
        match modifier {
            DeleteModifier::None => return Some((*b).clone()),
            DeleteModifier::Full | DeleteModifier::FindFull => {
                if b.is_full_backup() {
                    return Some((*b).clone());
                }
            }
        }
    }
    None
}

/// Core "delete-before-target": walk every object, drop the ones that order
/// strictly before target. Permanent backups (and their reserved WAL range)
/// are preserved. `prefix_filter` optionally restricts the walk
async fn delete_before_target_plan(
    storage: &DynStorage,
    backups: &[BackupRecord],
    target: &BackupRecord,
    prefix_filter: Option<&str>,
) -> Result<DeletePlan> {
    if !target.is_full_backup() {
        bail!(
            "{} is incremental & its predecessors cannot be deleted; consider FIND_FULL",
            target.name
        );
    }
    let permanent_wal = permanent_wal_set(backups);
    let permanent_backups = permanent_backup_names(backups);

    let mut objects = Vec::new();
    enumerate_and_filter(storage, prefix_filter, &mut objects, |key| {
        less_than_target(key, target)
            && !is_permanent_object(key, &permanent_backups, &permanent_wal)
    })
    .await?;

    Ok(DeletePlan {
        objects,
        kept_permanent_backups: permanent_backups.into_iter().collect(),
        target: Some(target.name.clone()),
    })
}

fn less_than_target(key: &str, target: &BackupRecord) -> bool {
    let Some((tli, seg)) = try_extract_timeline_seg_no(key) else {
        return false;
    };
    (tli, seg) < (target.timeline, target.start_seg_no)
}

// ── everything ─────────────────────────────────────────────────────────────

async fn plan_everything(
    storage: &DynStorage,
    backups: &[BackupRecord],
    force: bool,
) -> Result<DeletePlan> {
    let permanents = permanent_names(backups);
    if !permanents.is_empty() && !force {
        bail!(
            "found permanent backups ({}); pass FORCE to delete anyway",
            permanents.join(",")
        );
    }
    let mut objects = Vec::new();
    enumerate_and_filter(storage, None, &mut objects, |_| true).await?;
    Ok(DeletePlan {
        objects,
        kept_permanent_backups: Vec::new(),
        target: None,
    })
}

// ── target ─────────────────────────────────────────────────────────────────

async fn plan_target(
    storage: &DynStorage,
    backups: &[BackupRecord],
    name: &str,
    modifier: DeleteModifier,
) -> Result<DeletePlan> {
    if matches!(modifier, DeleteModifier::Full) {
        bail!("`delete target FULL` is not a supported modifier");
    }
    let resolved = if name == LATEST {
        backups
            .iter()
            .max_by_key(|b| (b.timeline, b.start_seg_no, b.start_time))
            .ok_or_else(|| anyhow!("no backups found"))?
    } else {
        backups
            .iter()
            .find(|b| b.name.starts_with(name))
            .ok_or_else(|| anyhow!("no backup named {name}"))?
    };
    let to_delete = match modifier {
        DeleteModifier::FindFull => find_related_backups(backups, resolved),
        DeleteModifier::None => find_dependant_backups(backups, resolved),
        DeleteModifier::Full => unreachable!(),
    };
    if let Some(p) = to_delete.iter().find(|b| b.is_permanent) {
        bail!("refusing to delete permanent backup {}", p.name);
    }
    let names: HashSet<String> = to_delete.iter().map(|b| b.name.clone()).collect();
    let mut objects = Vec::new();
    enumerate_and_filter(
        storage,
        Some(crate::pg::BASEBACKUP_FOLDER),
        &mut objects,
        |key| {
            let Some(name) = strip_leftmost_backup_name(key) else {
                return false;
            };
            names.contains(name)
        },
    )
    .await?;
    Ok(DeletePlan {
        objects,
        kept_permanent_backups: permanent_names(backups),
        target: Some(resolved.name.clone()),
    })
}

/// All backups sharing the same chain root as target (the full + every delta
/// off of it). When target is a full, that's target itself + all increments
fn find_related_backups(backups: &[BackupRecord], target: &BackupRecord) -> Vec<BackupRecord> {
    let target_base = target.base_backup_name().to_string();
    backups
        .iter()
        .filter(|b| {
            b.name == target_base || b.base_backup_name() == target_base || b.name == target.name
        })
        .cloned()
        .collect()
}

/// Target backup + every delta that has it (or one of its descendants)
/// somewhere in its chain. BFS over the increment graph
fn find_dependant_backups(backups: &[BackupRecord], target: &BackupRecord) -> Vec<BackupRecord> {
    let mut children: HashMap<String, Vec<BackupRecord>> = HashMap::new();
    for b in backups {
        if let Some(parent) = &b.increment_from {
            children.entry(parent.clone()).or_default().push(b.clone());
        }
    }
    let mut out = Vec::new();
    let mut queue = vec![target.clone()];
    while let Some(b) = queue.pop() {
        if let Some(kids) = children.get(&b.name) {
            queue.extend(kids.iter().cloned());
        }
        out.push(b);
    }
    out
}

// ── garbage ────────────────────────────────────────────────────────────────

async fn plan_garbage(
    storage: &DynStorage,
    backups: &[BackupRecord],
    scope: GarbageScope,
) -> Result<DeletePlan> {
    // Find the oldest non-permanent backup; delete-before-target everything
    // older than it, filtered by `scope` to wal-only / backup-only / both
    let mut non_perm: Vec<&BackupRecord> = backups.iter().filter(|b| !b.is_permanent).collect();
    non_perm.sort_by(|a, b| {
        (a.timeline, a.start_seg_no, a.start_time).cmp(&(b.timeline, b.start_seg_no, b.start_time))
    });
    let Some(oldest) = non_perm.first().cloned().cloned() else {
        return Ok(DeletePlan {
            objects: Vec::new(),
            kept_permanent_backups: permanent_names(backups),
            target: None,
        });
    };
    let prefix = match scope {
        GarbageScope::All => None,
        GarbageScope::Archives => Some(crate::pg::WAL_FOLDER),
        GarbageScope::Backups => Some(crate::pg::BASEBACKUP_FOLDER),
    };
    delete_before_target_plan(storage, backups, &oldest, prefix).await
}

// ── permanent-object tracking ──────────────────────────────────────────────

fn permanent_names(backups: &[BackupRecord]) -> Vec<String> {
    backups
        .iter()
        .filter(|b| b.is_permanent)
        .map(|b| b.name.clone())
        .collect()
}

fn permanent_backup_names(backups: &[BackupRecord]) -> HashSet<String> {
    backups
        .iter()
        .filter(|b| b.is_permanent)
        .map(|b| b.name.clone())
        .collect()
}

/// Every WAL segment that lives within `[start_seg-1, finish_seg-1]` of a
/// permanent backup is itself permanent. wal-g uses the LSN-1 boundary to
/// match `pg_walfile_name_offset` (segment containing the LSN). Returned
/// `(timeline, segment_global_no)` pairs the caller can probe per-object
fn permanent_wal_set(backups: &[BackupRecord]) -> HashSet<(u32, u64)> {
    let seg_size = wal_segment_size();
    let segs_per_log = 0x1_0000_0000u64 / seg_size;
    let mut out = HashSet::new();
    for b in backups.iter().filter(|b| b.is_permanent) {
        // Bail to start_seg_no if backup_start_lsn is missing; ignore segments-1 underflow
        let start_lsn = b.start_lsn.saturating_sub(1);
        let finish_lsn = b.finish_lsn.saturating_sub(1);
        if finish_lsn < start_lsn {
            continue;
        }
        let mut seg = start_lsn / seg_size;
        let last = finish_lsn / seg_size;
        while seg <= last {
            let log_id = seg / segs_per_log;
            let seg_lo = seg % segs_per_log;
            let global = log_id * segs_per_log + seg_lo;
            out.insert((b.timeline, global));
            seg += 1;
        }
    }
    out
}

fn is_permanent_object(
    key: &str,
    permanent_backups: &HashSet<String>,
    permanent_wal: &HashSet<(u32, u64)>,
) -> bool {
    let basebackup_prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let wal_prefix = format!("{}/", crate::pg::WAL_FOLDER);
    if key.starts_with(&basebackup_prefix)
        && let Some(name) = strip_leftmost_backup_name(key)
    {
        return permanent_backups.contains(name);
    }
    if key.starts_with(&wal_prefix)
        && let Some((tli, seg)) = try_extract_timeline_seg_no(key)
    {
        return permanent_wal.contains(&(tli, seg));
    }
    false
}

// ── object enumeration / execution ────────────────────────────────────────

async fn enumerate_and_filter<F>(
    storage: &DynStorage,
    prefix: Option<&str>,
    out: &mut Vec<String>,
    mut keep: F,
) -> Result<()>
where
    F: FnMut(&str) -> bool,
{
    let prefixes: Vec<String> = match prefix {
        Some(p) => vec![format!("{p}/")],
        None => vec![
            format!("{}/", crate::pg::BASEBACKUP_FOLDER),
            format!("{}/", crate::pg::WAL_FOLDER),
        ],
    };
    for p in prefixes {
        let mut s = storage
            .list(&p)
            .await
            .with_context(|| format!("list {p}"))?;
        while let Some(item) = s.next().await {
            let obj = item.context("list iteration")?;
            if keep(&obj.key) {
                out.push(obj.key);
            }
        }
    }
    Ok(())
}

async fn execute_delete(storage: &DynStorage, keys: &[String]) -> Result<()> {
    for k in keys {
        if let Err(e) = storage.delete(k).await {
            tracing::warn!(target = "delete", "delete {k}: {e:#}");
        } else {
            tracing::info!(target = "delete", "deleted {k}");
        }
    }
    Ok(())
}

fn print_plan(plan: &DeletePlan, confirm: bool) {
    if let Some(t) = &plan.target {
        println!("delete-before target: {t}");
    }
    if plan.objects.is_empty() {
        println!("no objects matched");
        return;
    }
    println!(
        "{} object(s) {}",
        plan.objects.len(),
        if confirm {
            "to delete"
        } else {
            "would be deleted (dry run; pass --confirm to execute)"
        }
    );
    for k in &plan.objects {
        println!("  {k}");
    }
    if !plan.kept_permanent_backups.is_empty() {
        println!(
            "{} permanent backup(s) preserved",
            plan.kept_permanent_backups.len()
        );
    }
}

/// Convenience: parse `["FULL", "5"]` / `["FIND_FULL", "5"]` / `["5"]` into
/// `(modifier, value)`. Mirrors wal-g's `ExtractDeleteModifierFromArgs`
pub fn parse_modifier_args(args: &[String]) -> Result<(DeleteModifier, String)> {
    match args {
        [v] => Ok((DeleteModifier::None, v.clone())),
        [m, v] if m == "FULL" => Ok((DeleteModifier::Full, v.clone())),
        [m, v] if m == "FIND_FULL" => Ok((DeleteModifier::FindFull, v.clone())),
        _ => bail!("expected `[FULL|FIND_FULL] <value>`, got {args:?}"),
    }
}

/// Name is optional: when absent the caller resolves it from `--target-user-data`
pub fn parse_target_modifier(args: &[String]) -> Result<(DeleteModifier, Option<String>)> {
    match args {
        [] => Ok((DeleteModifier::None, None)),
        [m] if m == "FIND_FULL" => Ok((DeleteModifier::FindFull, None)),
        [v] => Ok((DeleteModifier::None, Some(v.clone()))),
        [m, v] if m == "FIND_FULL" => Ok((DeleteModifier::FindFull, Some(v.clone()))),
        _ => bail!("expected `[FIND_FULL] [backup]`, got {args:?}"),
    }
}

pub fn parse_everything_force(args: &[String]) -> Result<bool> {
    match args {
        [] => Ok(false),
        [m] if m == "FORCE" => Ok(true),
        _ => bail!("expected nothing or `FORCE`, got {args:?}"),
    }
}

pub fn parse_garbage_scope(args: &[String]) -> Result<GarbageScope> {
    match args {
        [] => Ok(GarbageScope::All),
        [m] if m == "ARCHIVES" => Ok(GarbageScope::Archives),
        [m] if m == "BACKUPS" => Ok(GarbageScope::Backups),
        _ => bail!("expected nothing | ARCHIVES | BACKUPS, got {args:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;

    fn make_record(name: &str, tli: u32, seg: u64, is_full: bool, perm: bool) -> BackupRecord {
        BackupRecord {
            name: name.to_string(),
            timeline: tli,
            start_seg_no: seg,
            start_lsn: seg * DEFAULT_WAL_SEG_SIZE,
            finish_lsn: (seg + 1) * DEFAULT_WAL_SEG_SIZE,
            start_time: Utc::now() - chrono::Duration::seconds(seg as i64),
            is_permanent: perm,
            increment_from: None,
            increment_full_name: if is_full {
                None
            } else {
                Some("base_000000010000000000000001".into())
            },
        }
    }

    #[test]
    fn extract_segno_from_wal_name() {
        let r = try_extract_timeline_seg_no("000000010000000000000003.zst").unwrap();
        assert_eq!(r, (1, 3));
        // 16 MiB segs => 256 per log_id
        let r = try_extract_timeline_seg_no("000000020000000100000000").unwrap();
        assert_eq!(r, (2, 256));
    }

    #[test]
    fn extract_segno_from_backup_name() {
        let r = try_extract_timeline_seg_no(
            "basebackups_005/base_000000010000000000000005_backup_stop_sentinel.json",
        )
        .unwrap();
        assert_eq!(r, (1, 5));
    }

    #[test]
    fn extract_segno_takes_first_match_for_delta() {
        let r =
            try_extract_timeline_seg_no("base_000000010000000000000007_D_000000010000000000000005")
                .unwrap();
        assert_eq!(r, (1, 7));
    }

    #[test]
    fn strip_leftmost_handles_sentinel() {
        assert_eq!(
            strip_leftmost_backup_name(
                "basebackups_005/base_000000010000000000000005_backup_stop_sentinel.json"
            ),
            Some("base_000000010000000000000005")
        );
        assert_eq!(
            strip_leftmost_backup_name(
                "basebackups_005/base_000000010000000000000005/tar_partitions/part_001.tar.zst"
            ),
            Some("base_000000010000000000000005")
        );
        assert_eq!(
            strip_leftmost_backup_name(
                "basebackups_005/base_000000010000000000000007_D_000000010000000000000005/files_metadata.json"
            ),
            Some("base_000000010000000000000007_D_000000010000000000000005")
        );
    }

    #[test]
    fn less_than_target_compares_by_seg_no() {
        let t = make_record("base_000000010000000000000005", 1, 5, true, false);
        assert!(less_than_target("wal_005/000000010000000000000003.zst", &t));
        assert!(!less_than_target(
            "wal_005/000000010000000000000007.zst",
            &t
        ));
        // Equal seg_no is not less
        assert!(!less_than_target(
            "wal_005/000000010000000000000005.zst",
            &t
        ));
    }

    #[test]
    fn target_modifier_parses_optional_name() {
        let p = |a: &[&str]| {
            parse_target_modifier(&a.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        assert!(matches!(p(&[]).unwrap(), (DeleteModifier::None, None)));
        assert!(matches!(
            p(&["FIND_FULL"]).unwrap(),
            (DeleteModifier::FindFull, None)
        ));
        assert_eq!(
            p(&["base_1"]).unwrap(),
            (DeleteModifier::None, Some("base_1".into()))
        );
        assert_eq!(
            p(&["FIND_FULL", "base_1"]).unwrap(),
            (DeleteModifier::FindFull, Some("base_1".into()))
        );
        assert!(p(&["FIND_FULL", "base_1", "extra"]).is_err());
        assert!(p(&["BOGUS", "base_1"]).is_err());
    }

    #[test]
    fn retain_target_walks_backups() {
        let backups = vec![
            make_record("base_000000010000000000000001", 1, 1, true, false),
            make_record("base_000000010000000000000003", 1, 3, true, false),
            make_record("base_000000010000000000000005", 1, 5, true, false),
            make_record("base_000000010000000000000007", 1, 7, true, false),
        ];
        // retain N=2 should pick the 2nd-newest backup (seg 5)
        let t = resolve_retain_target(&backups, 2, DeleteModifier::None).unwrap();
        assert_eq!(t.start_seg_no, 5);
    }

    #[test]
    fn retain_full_modifier_only_counts_fulls() {
        let mut deltas = vec![
            make_record("base_000000010000000000000001", 1, 1, true, false),
            make_record("base_d2", 1, 2, false, false),
            make_record("base_d3", 1, 3, false, false),
            make_record("base_000000010000000000000005", 1, 5, true, false),
            make_record("base_d6", 1, 6, false, false),
            make_record("base_000000010000000000000007", 1, 7, true, false),
        ];
        // sort by seg ascending then make_record handled time order separately
        deltas.sort_by_key(|b| b.start_seg_no);
        let t = resolve_retain_target(&deltas, 2, DeleteModifier::Full).unwrap();
        assert_eq!(t.start_seg_no, 5, "2nd-newest FULL is seg 5");
    }

    #[test]
    fn find_full_walks_chain_root() {
        let mut backups = vec![
            make_record("base_000000010000000000000001", 1, 1, true, false),
            make_record("base_d2", 1, 2, false, false),
            make_record("base_d3", 1, 3, false, false),
        ];
        backups[1].increment_full_name = Some("base_000000010000000000000001".into());
        backups[2].increment_full_name = Some("base_000000010000000000000001".into());
        let t = resolve_before_target(&backups, "base_d3", DeleteModifier::FindFull)
            .unwrap()
            .unwrap();
        assert_eq!(t.name, "base_000000010000000000000001");
    }

    #[test]
    fn permanent_wal_marks_segments_inclusive() {
        let b = BackupRecord {
            name: "base_000000010000000000000003".into(),
            timeline: 1,
            start_seg_no: 3,
            start_lsn: 3 * DEFAULT_WAL_SEG_SIZE + 100,
            finish_lsn: 5 * DEFAULT_WAL_SEG_SIZE + 100,
            start_time: Utc::now(),
            is_permanent: true,
            increment_from: None,
            increment_full_name: None,
        };
        let set = permanent_wal_set(&[b]);
        // segments containing start_lsn-1 (seg 3) through finish_lsn-1 (seg 5) inclusive
        assert!(set.contains(&(1, 3)));
        assert!(set.contains(&(1, 4)));
        assert!(set.contains(&(1, 5)));
        assert!(!set.contains(&(1, 2)));
        assert!(!set.contains(&(1, 6)));
    }

    #[test]
    fn permanent_object_check_routes_by_prefix() {
        let mut perm_backups = HashSet::new();
        perm_backups.insert("base_000000010000000000000003".to_string());
        let mut perm_wal = HashSet::new();
        perm_wal.insert((1, 7));

        assert!(is_permanent_object(
            "basebackups_005/base_000000010000000000000003_backup_stop_sentinel.json",
            &perm_backups,
            &perm_wal,
        ));
        assert!(is_permanent_object(
            "wal_005/000000010000000000000007.zst",
            &perm_backups,
            &perm_wal,
        ));
        assert!(!is_permanent_object(
            "wal_005/000000010000000000000008.zst",
            &perm_backups,
            &perm_wal,
        ));
    }

    fn make_record_at(
        name: &str,
        seg: u64,
        start_time: DateTime<Utc>,
        is_full: bool,
    ) -> BackupRecord {
        let mut r = make_record(name, 1, seg, is_full, false);
        r.start_time = start_time;
        r
    }

    #[test]
    fn retain_after_time_picks_older_of_two_anchors() {
        // 4 backups, ascending in time and seg_no
        let t0 = Utc::now() - chrono::Duration::hours(4);
        let backups = vec![
            make_record_at("base_1", 1, t0, true),
            make_record_at("base_2", 2, t0 + chrono::Duration::hours(1), true),
            make_record_at("base_3", 3, t0 + chrono::Duration::hours(2), true),
            make_record_at("base_4", 4, t0 + chrono::Duration::hours(3), true),
        ];
        // retain N=1 alone would anchor at base_4. After-time falls between base_2 and base_3
        // so after-anchor = base_3; older = base_3 (seg 3 < seg 4)
        let after = (t0 + chrono::Duration::hours(2)).to_rfc3339();
        let t = resolve_retain_after_target(&backups, 1, &after, DeleteModifier::None)
            .unwrap()
            .unwrap();
        assert_eq!(t.start_seg_no, 3);
    }

    #[test]
    fn retain_after_time_falls_back_to_retain_when_no_after_match() {
        let t0 = Utc::now() - chrono::Duration::hours(4);
        let backups = vec![
            make_record_at("base_1", 1, t0, true),
            make_record_at("base_2", 2, t0 + chrono::Duration::hours(1), true),
            make_record_at("base_3", 3, t0 + chrono::Duration::hours(2), true),
        ];
        // After-time is past every backup: t2=None, return t1 (Nth-newest)
        let after = (Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
        let t = resolve_retain_after_target(&backups, 2, &after, DeleteModifier::None)
            .unwrap()
            .unwrap();
        // 2nd-newest is base_2 (seg 2)
        assert_eq!(t.start_seg_no, 2);
    }

    #[test]
    fn retain_after_name_latches_on_match() {
        let backups = vec![
            make_record("base_1", 1, 1, true, false),
            make_record("base_2", 1, 2, true, false),
            make_record("base_3", 1, 3, true, false),
            make_record("base_4", 1, 4, true, false),
        ];
        // After="base_3": once seen (oldest-first walk), every subsequent is candidate
        // first one we hit (and accept under modifier=None) is base_3 itself
        let t = resolve_retain_after_target(&backups, 1, "base_3", DeleteModifier::None)
            .unwrap()
            .unwrap();
        assert_eq!(t.start_seg_no, 3);
    }

    #[test]
    fn retain_after_full_modifier_picks_full_only() {
        let backups = vec![
            make_record("base_1", 1, 1, true, false),
            make_record("base_2", 1, 2, false, false), // delta
            make_record("base_3", 1, 3, false, false), // delta
            make_record("base_4", 1, 4, true, false),  // full
        ];
        // After="base_2" but FIND_FULL: latches at base_2, walks until it sees a full backup
        // First full backup after the latch is base_4 (seg 4)
        let t = resolve_retain_after_target(&backups, 1, "base_2", DeleteModifier::FindFull)
            .unwrap()
            .unwrap();
        assert_eq!(t.start_seg_no, 4);
    }

    #[test]
    fn retain_after_rejects_future_timestamp() {
        let backups = vec![make_record("base_1", 1, 1, true, false)];
        let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let err =
            resolve_retain_after_target(&backups, 1, &future, DeleteModifier::None).unwrap_err();
        assert!(err.to_string().contains("future"));
    }

    #[test]
    fn find_dependants_walks_increment_chain() {
        let mut backups = vec![
            make_record("base_full", 1, 1, true, false),
            make_record("base_d1", 1, 2, false, false),
            make_record("base_d2", 1, 3, false, false),
            make_record("base_d3", 1, 4, false, false),
        ];
        backups[1].increment_from = Some("base_full".into());
        backups[2].increment_from = Some("base_d1".into());
        backups[3].increment_from = Some("base_d2".into());

        let deps = find_dependant_backups(&backups, &backups[1]);
        let names: HashSet<String> = deps.into_iter().map(|b| b.name).collect();
        assert!(names.contains("base_d1"));
        assert!(names.contains("base_d2"));
        assert!(names.contains("base_d3"));
        assert!(!names.contains("base_full"));
    }

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn modifier_args_parse_all_shapes() {
        assert_eq!(
            parse_modifier_args(&args(&["5"])).unwrap(),
            (DeleteModifier::None, "5".into())
        );
        assert_eq!(
            parse_modifier_args(&args(&["FULL", "5"])).unwrap(),
            (DeleteModifier::Full, "5".into())
        );
        assert_eq!(
            parse_modifier_args(&args(&["FIND_FULL", "5"])).unwrap(),
            (DeleteModifier::FindFull, "5".into())
        );
        assert!(parse_modifier_args(&args(&[])).is_err());
        assert!(parse_modifier_args(&args(&["BOGUS", "5"])).is_err());
        assert!(parse_modifier_args(&args(&["FULL", "5", "x"])).is_err());
    }

    #[test]
    fn everything_force_parses() {
        assert!(!parse_everything_force(&args(&[])).unwrap());
        assert!(parse_everything_force(&args(&["FORCE"])).unwrap());
        assert!(parse_everything_force(&args(&["NOPE"])).is_err());
        assert!(parse_everything_force(&args(&["FORCE", "x"])).is_err());
    }

    #[test]
    fn garbage_scope_parses() {
        assert_eq!(parse_garbage_scope(&args(&[])).unwrap(), GarbageScope::All);
        assert_eq!(
            parse_garbage_scope(&args(&["ARCHIVES"])).unwrap(),
            GarbageScope::Archives
        );
        assert_eq!(
            parse_garbage_scope(&args(&["BACKUPS"])).unwrap(),
            GarbageScope::Backups
        );
        assert!(parse_garbage_scope(&args(&["WHAT"])).is_err());
        assert!(parse_garbage_scope(&args(&["ARCHIVES", "x"])).is_err());
    }

    fn empty_store() -> (tempfile::TempDir, DynStorage) {
        let dir = tempfile::tempdir().unwrap();
        let s: DynStorage =
            std::sync::Arc::new(crate::storage::fs::FsStorage::new(dir.path()).unwrap());
        (dir, s)
    }

    #[tokio::test]
    async fn plan_before_no_match_returns_empty() {
        let (_d, storage) = empty_store();
        let backups = vec![make_record(
            "base_000000010000000000000005",
            1,
            5,
            true,
            false,
        )];
        let plan = plan_before(&storage, &backups, "base_zzz", DeleteModifier::None)
            .await
            .unwrap();
        assert!(plan.objects.is_empty());
        assert!(plan.target.is_none());
    }

    #[tokio::test]
    async fn plan_retain_zero_errors_and_overshoot_is_empty() {
        let (_d, storage) = empty_store();
        let backups = vec![make_record("base_1", 1, 1, true, false)];
        assert!(
            plan_retain(&storage, &backups, 0, DeleteModifier::None, None)
                .await
                .is_err()
        );
        // retaining more than exist resolves no target -> empty plan
        let plan = plan_retain(&storage, &backups, 5, DeleteModifier::None, None)
            .await
            .unwrap();
        assert!(plan.objects.is_empty());
        assert!(plan.target.is_none());
    }

    #[tokio::test]
    async fn plan_garbage_all_permanent_returns_empty() {
        let (_d, storage) = empty_store();
        let backups = vec![make_record("base_1", 1, 1, true, true)];
        let plan = plan_garbage(&storage, &backups, GarbageScope::All)
            .await
            .unwrap();
        assert!(plan.objects.is_empty());
        assert!(plan.target.is_none());
        assert_eq!(plan.kept_permanent_backups, vec!["base_1".to_string()]);
    }

    #[tokio::test]
    async fn plan_everything_force_gate() {
        let (_d, storage) = empty_store();
        let backups = vec![make_record("base_1", 1, 1, true, true)];
        assert!(
            plan_everything(&storage, &backups, false).await.is_err(),
            "must refuse permanent without FORCE"
        );
        let plan = plan_everything(&storage, &backups, true).await.unwrap();
        assert!(plan.objects.is_empty());
    }

    #[tokio::test]
    async fn plan_target_latest_selects_newest() {
        let (_d, storage) = empty_store();
        let backups = vec![
            make_record("base_000000010000000000000001", 1, 1, true, false),
            make_record("base_000000010000000000000007", 1, 7, true, false),
        ];
        let plan = plan_target(&storage, &backups, LATEST, DeleteModifier::None)
            .await
            .unwrap();
        assert_eq!(
            plan.target.as_deref(),
            Some("base_000000010000000000000007")
        );
        assert!(plan.objects.is_empty());

        // FULL modifier is rejected for target
        assert!(
            plan_target(&storage, &backups, LATEST, DeleteModifier::Full)
                .await
                .is_err()
        );
        // unknown name errors
        assert!(
            plan_target(&storage, &backups, "base_nope", DeleteModifier::None)
                .await
                .is_err()
        );
    }
}
