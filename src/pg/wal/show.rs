//! wal-show: enumerate timelines, basebackups, WAL segment ranges & gaps
//!
//! Reads `wal_005/` for archived segments + `basebackups_005/*_backup_stop_sentinel.json`
//! for backup boundaries, groups by timeline, computes per-timeline gaps.
//! Pretty / JSON output mirrors wal-g's `wal-show` so existing dashboards
//! parse identically

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use futures::StreamExt;
use serde::Serialize;
use tokio::io::AsyncReadExt;

use crate::pg::WAL_FOLDER;
use crate::pg::backup::list as backup_list;
use crate::pg::backup::{format_pg_lsn, parse_timeline_from_backup_name};
use crate::pg::wal::segment::{SegmentName, wal_segment_size};
use crate::pg::wal::segment_file::classify_segment_name;
use crate::storage::DynStorage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Plain,
    Json,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineInfo {
    pub timeline: u32,
    pub start_segment: Option<String>,
    pub end_segment: Option<String>,
    pub segments_count: usize,
    pub gaps: Vec<GapInfo>,
    pub backups: Vec<BackupRef>,
    pub status: TimelineStatus,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TimelineStatus {
    Ok,
    Lost,
}

#[derive(Debug, Clone, Serialize)]
pub struct GapInfo {
    pub from: String,
    pub to: String,
    pub missing: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupRef {
    pub name: String,
    pub start_lsn: Option<u64>,
    pub finish_lsn: Option<u64>,
}

pub async fn handle(storage: DynStorage, format: Format) -> Result<()> {
    let timelines = collect(storage).await?;
    match format {
        Format::Plain => print_plain(&timelines),
        Format::Json => println!("{}", serde_json::to_string_pretty(&timelines)?),
    }
    Ok(())
}

pub async fn collect(storage: DynStorage) -> Result<Vec<TimelineInfo>> {
    let segs_by_tli = list_segments(&storage).await?;
    let backups = backup_list::collect(storage.clone()).await?;

    let mut backups_by_tli: BTreeMap<u32, Vec<BackupRef>> = BTreeMap::new();
    for b in &backups {
        // backup name carries the timeline as the first 8 hex chars after "base_"
        let Some(tli) = parse_timeline_from_backup_name(&b.name) else {
            continue;
        };
        backups_by_tli.entry(tli).or_default().push(BackupRef {
            name: b.name.clone(),
            start_lsn: b.start_lsn,
            finish_lsn: b.finish_lsn,
        });
    }

    let mut out = Vec::new();
    let mut all_tlis: std::collections::BTreeSet<u32> = segs_by_tli.keys().copied().collect();
    all_tlis.extend(backups_by_tli.keys().copied());
    for tli in all_tlis {
        let segs = segs_by_tli.get(&tli).cloned().unwrap_or_default();
        let info = summarize_timeline(tli, segs, backups_by_tli.remove(&tli).unwrap_or_default());
        out.push(info);
    }
    Ok(out)
}

async fn list_segments(
    storage: &DynStorage,
) -> Result<BTreeMap<u32, std::collections::BTreeSet<SegmentName>>> {
    let prefix = format!("{}/", WAL_FOLDER);
    let mut stream = storage
        .list(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let mut by_tli: BTreeMap<u32, std::collections::BTreeSet<SegmentName>> = BTreeMap::new();
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        let bare = obj.key.rsplit('/').next().unwrap_or(&obj.key);
        if let Ok((seg, _)) = classify_segment_name(bare) {
            by_tli.entry(seg.timeline).or_default().insert(seg);
        }
    }
    Ok(by_tli)
}

fn summarize_timeline(
    tli: u32,
    segs: std::collections::BTreeSet<SegmentName>,
    backups: Vec<BackupRef>,
) -> TimelineInfo {
    if segs.is_empty() {
        return TimelineInfo {
            timeline: tli,
            start_segment: None,
            end_segment: None,
            segments_count: 0,
            gaps: Vec::new(),
            backups,
            status: TimelineStatus::Lost,
        };
    }
    let seg_size = wal_segment_size();
    let first = *segs.iter().next().unwrap();
    let last = *segs.iter().next_back().unwrap();
    let mut gaps = Vec::new();
    let mut cursor = first;
    while cursor != last {
        let nxt = cursor.next(seg_size);
        if !segs.contains(&nxt) {
            // Find next present segment after `cursor`
            let mut probe = nxt;
            let mut missing: u64 = 0;
            while !segs.contains(&probe) {
                missing += 1;
                if probe == last {
                    break;
                }
                probe = probe.next(seg_size);
            }
            gaps.push(GapInfo {
                from: cursor.format(),
                to: probe.format(),
                missing,
            });
            cursor = probe;
        } else {
            cursor = nxt;
        }
    }
    let status = if gaps.is_empty() {
        TimelineStatus::Ok
    } else {
        TimelineStatus::Lost
    };
    TimelineInfo {
        timeline: tli,
        start_segment: Some(first.format()),
        end_segment: Some(last.format()),
        segments_count: segs.len(),
        gaps,
        backups,
        status,
    }
}

fn print_plain(timelines: &[TimelineInfo]) {
    if timelines.is_empty() {
        println!("(no timelines archived)");
        return;
    }
    for t in timelines {
        println!(
            "TLI {}  status={:?}  segments={}",
            t.timeline, t.status, t.segments_count
        );
        if let (Some(s), Some(e)) = (&t.start_segment, &t.end_segment) {
            println!("  range: {s} - {e}");
        }
        for g in &t.gaps {
            println!("  gap: {} -> {} (missing {})", g.from, g.to, g.missing);
        }
        for b in &t.backups {
            let start = b.start_lsn.map(format_pg_lsn).unwrap_or_else(|| "-".into());
            let finish = b
                .finish_lsn
                .map(format_pg_lsn)
                .unwrap_or_else(|| "-".into());
            println!("  backup: {} start={} finish={}", b.name, start, finish);
        }
    }
}

/// Helper exposed for `wal-restore`: enumerate gaps across all timelines
pub async fn gaps_by_timeline(storage: DynStorage) -> Result<BTreeMap<u32, Vec<GapInfo>>> {
    let timelines = collect(storage).await?;
    Ok(timelines
        .into_iter()
        .filter(|t| !t.gaps.is_empty())
        .map(|t| (t.timeline, t.gaps))
        .collect())
}

/// Helper for `wal-verify integrity`: every segment from each backup's
/// start LSN forward through the latest archived segment must be present
pub async fn integrity_for_backup(
    storage: DynStorage,
    backup_start_lsn: u64,
    timeline: u32,
) -> Result<Vec<GapInfo>> {
    let segs_by_tli = list_segments(&storage).await?;
    let Some(segs) = segs_by_tli.get(&timeline) else {
        return Ok(vec![GapInfo {
            from: "n/a".into(),
            to: "n/a".into(),
            missing: 0,
        }]);
    };
    let Some(&end) = segs.iter().next_back() else {
        return Ok(Vec::new());
    };
    let seg_size = wal_segment_size();
    let start_seg_no = backup_start_lsn / seg_size;
    let xlog_segs_per_xlog_id = 0x1_0000_0000u64 / seg_size;
    let start = SegmentName {
        timeline,
        log_id: (start_seg_no / xlog_segs_per_xlog_id) as u32,
        seg_no: (start_seg_no % xlog_segs_per_xlog_id) as u32,
    };
    let mut gaps = Vec::new();
    let mut cursor = start;
    loop {
        if !segs.contains(&cursor) {
            let mut probe = cursor;
            let mut missing: u64 = 0;
            while !segs.contains(&probe) && probe != end {
                missing += 1;
                probe = probe.next(seg_size);
            }
            gaps.push(GapInfo {
                from: cursor.format(),
                to: probe.format(),
                missing,
            });
            if probe == end && !segs.contains(&end) {
                break;
            }
            cursor = probe;
        }
        if cursor == end {
            break;
        }
        cursor = cursor.next(seg_size);
    }
    Ok(gaps)
}

#[allow(dead_code)]
async fn _unused_reader(_r: &mut (dyn tokio::io::AsyncRead + Unpin)) {
    let mut buf = [0u8; 0];
    let _ = AsyncReadExt::read(_r, &mut buf).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AsyncReader, fs::FsStorage};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn seg(tli: u32, seg_no: u32) -> SegmentName {
        SegmentName {
            timeline: tli,
            log_id: 0,
            seg_no,
        }
    }

    fn set(tli: u32, nos: &[u32]) -> BTreeSet<SegmentName> {
        nos.iter().map(|&n| seg(tli, n)).collect()
    }

    #[test]
    fn contiguous_timeline_has_no_gaps() {
        let info = summarize_timeline(1, set(1, &[1, 2, 3, 4]), vec![]);
        assert_eq!(info.status, TimelineStatus::Ok);
        assert!(info.gaps.is_empty());
        assert_eq!(info.segments_count, 4);
        assert_eq!(
            info.start_segment.as_deref(),
            Some("000000010000000000000001")
        );
        assert_eq!(
            info.end_segment.as_deref(),
            Some("000000010000000000000004")
        );
    }

    #[test]
    fn single_missing_segment_is_one_gap() {
        let info = summarize_timeline(1, set(1, &[1, 2, 4, 5]), vec![]);
        assert_eq!(info.status, TimelineStatus::Lost);
        assert_eq!(info.gaps.len(), 1);
        let g = &info.gaps[0];
        assert_eq!(g.from, "000000010000000000000002");
        assert_eq!(g.to, "000000010000000000000004");
        assert_eq!(g.missing, 1);
    }

    #[test]
    fn multi_segment_gap_counts_all_missing() {
        let info = summarize_timeline(1, set(1, &[1, 5]), vec![]);
        assert_eq!(info.gaps.len(), 1);
        assert_eq!(info.gaps[0].from, "000000010000000000000001");
        assert_eq!(info.gaps[0].to, "000000010000000000000005");
        assert_eq!(info.gaps[0].missing, 3);
    }

    #[test]
    fn empty_timeline_is_lost() {
        let info = summarize_timeline(7, BTreeSet::new(), vec![]);
        assert_eq!(info.status, TimelineStatus::Lost);
        assert!(info.start_segment.is_none());
        assert_eq!(info.segments_count, 0);
    }

    #[test]
    fn single_segment_timeline_ok() {
        let info = summarize_timeline(1, set(1, &[3]), vec![]);
        assert_eq!(info.status, TimelineStatus::Ok);
        assert!(info.gaps.is_empty());
        assert_eq!(info.start_segment, info.end_segment);
    }

    fn empty_body() -> AsyncReader {
        Box::pin(std::io::Cursor::new(Vec::new()))
    }

    // collect() over a seeded fs bucket: a timeline switch yields one
    // TimelineInfo per TLI, a .history file is ignored (no phantom timeline),
    // and a trailing .partial counts as a present segment (classify accepts it)
    #[tokio::test]
    async fn collect_splits_timelines_ignores_history_counts_partial() {
        let dir = tempfile::tempdir().unwrap();
        let store: DynStorage = Arc::new(FsStorage::new(dir.path()).unwrap());
        for k in [
            "wal_005/000000010000000000000001",
            "wal_005/000000010000000000000002",
            "wal_005/000000010000000000000003.partial",
            "wal_005/000000020000000000000005",
            "wal_005/000000020000000000000007",
            "wal_005/00000002.history",
        ] {
            store.put(k, empty_body(), None).await.unwrap();
        }

        let tlis = collect(store).await.unwrap();
        assert_eq!(tlis.len(), 2, "history must not create a phantom timeline");

        let t1 = tlis.iter().find(|t| t.timeline == 1).unwrap();
        assert_eq!(t1.segments_count, 3, ".partial segment must be counted");
        assert_eq!(t1.status, TimelineStatus::Ok);

        let t2 = tlis.iter().find(|t| t.timeline == 2).unwrap();
        assert_eq!(t2.gaps.len(), 1);
        assert_eq!(t2.gaps[0].missing, 1);
        assert_eq!(t2.status, TimelineStatus::Lost);
    }
}
