//! wal-verify: two-mode WAL archive sanity check
//!
//! - integrity: every segment from the latest backup's start LSN through the
//!   freshest archived segment on the same timeline must be present
//! - timeline: HEAD timeline (newest archived segment) must match the latest
//!   backup's timeline; mismatch implies a missed promotion or a deleted
//!   backup that bumped the chain
//!
//! Mirrors wal-g's `wal-verify` modes; output is intentionally machine-
//! readable so it can drive an exit-non-zero check

use anyhow::{Context, Result, anyhow};
use serde::Serialize;

use crate::cli::WalVerifyOp;
use crate::pg::backup::list as backup_list;
use crate::pg::backup::{format_pg_lsn, parse_timeline_from_backup_name};
use crate::pg::wal::show::{self, GapInfo};
use crate::storage::DynStorage;

#[derive(Debug, Clone, Serialize)]
pub struct IntegrityReport {
    pub status: ReportStatus,
    pub backup_name: Option<String>,
    pub timeline: u32,
    pub start_lsn: Option<u64>,
    pub gaps: Vec<GapInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineReport {
    pub status: ReportStatus,
    pub current_timeline: Option<u32>,
    pub latest_backup_timeline: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ReportStatus {
    Ok,
    Failure,
    Empty,
}

pub async fn run(storage: DynStorage, op: WalVerifyOp) -> Result<()> {
    let (json, integrity, timeline) = match op {
        WalVerifyOp::Integrity { json } => (json, true, false),
        WalVerifyOp::Timeline { json } => (json, false, true),
        WalVerifyOp::All { json } => (json, true, true),
    };
    let i = if integrity {
        Some(check_integrity(storage.clone()).await?)
    } else {
        None
    };
    let t = if timeline {
        Some(check_timeline(storage).await?)
    } else {
        None
    };

    if json {
        #[derive(Serialize)]
        struct Combined {
            #[serde(skip_serializing_if = "Option::is_none")]
            integrity: Option<IntegrityReport>,
            #[serde(skip_serializing_if = "Option::is_none")]
            timeline: Option<TimelineReport>,
        }
        let c = Combined {
            integrity: i.clone(),
            timeline: t.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&c)?);
    } else {
        if let Some(r) = &i {
            print_integrity(r);
        }
        if let Some(r) = &t {
            print_timeline(r);
        }
    }

    let mut failed = false;
    if let Some(r) = &i
        && r.status == ReportStatus::Failure
    {
        failed = true;
    }
    if let Some(r) = &t
        && r.status == ReportStatus::Failure
    {
        failed = true;
    }
    if failed {
        return Err(anyhow!("wal-verify reported FAILURE"));
    }
    Ok(())
}

pub async fn check_integrity(storage: DynStorage) -> Result<IntegrityReport> {
    let backups = backup_list::collect(storage.clone())
        .await
        .context("list backups")?;
    let Some(latest) = backups.last().cloned() else {
        return Ok(IntegrityReport {
            status: ReportStatus::Empty,
            backup_name: None,
            timeline: 0,
            start_lsn: None,
            gaps: Vec::new(),
        });
    };
    let timeline = parse_timeline_from_backup_name(&latest.name).unwrap_or(0);
    let Some(start) = latest.start_lsn else {
        return Ok(IntegrityReport {
            status: ReportStatus::Failure,
            backup_name: Some(latest.name),
            timeline,
            start_lsn: None,
            gaps: Vec::new(),
        });
    };
    let gaps = show::integrity_for_backup(storage, start, timeline).await?;
    let status = if gaps.is_empty() {
        ReportStatus::Ok
    } else {
        ReportStatus::Failure
    };
    Ok(IntegrityReport {
        status,
        backup_name: Some(latest.name),
        timeline,
        start_lsn: Some(start),
        gaps,
    })
}

pub async fn check_timeline(storage: DynStorage) -> Result<TimelineReport> {
    let timelines = show::collect(storage.clone()).await?;
    let current = timelines.iter().map(|t| t.timeline).max();
    let backups = backup_list::collect(storage)
        .await
        .context("list backups")?;
    let latest_backup_tli = backups
        .last()
        .and_then(|b| parse_timeline_from_backup_name(&b.name));
    let status = match (current, latest_backup_tli) {
        (None, None) => ReportStatus::Empty,
        (Some(a), Some(b)) if a == b => ReportStatus::Ok,
        (_, _) => ReportStatus::Failure,
    };
    Ok(TimelineReport {
        status,
        current_timeline: current,
        latest_backup_timeline: latest_backup_tli,
    })
}

fn print_integrity(r: &IntegrityReport) {
    println!("integrity: {:?}", r.status);
    if let Some(name) = &r.backup_name {
        println!(
            "  backup: {name} timeline={} start_lsn={}",
            r.timeline,
            r.start_lsn.map(format_pg_lsn).unwrap_or_else(|| "-".into())
        );
    }
    for g in &r.gaps {
        println!("  gap: {} -> {} (missing {})", g.from, g.to, g.missing);
    }
}

fn print_timeline(r: &TimelineReport) {
    println!("timeline: {:?}", r.status);
    if let Some(c) = r.current_timeline {
        println!("  current: {c}");
    }
    if let Some(b) = r.latest_backup_timeline {
        println!("  latest_backup: {b}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::backup::test_fixtures::{fs_store, lsn_for_seg, put_sentinel, put_wal_segment};
    use crate::pg::backup::{BackupSentinelDto, BackupSentinelDtoV2};

    fn sentinel(seg_no: u64) -> BackupSentinelDtoV2 {
        BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: Some(lsn_for_seg(seg_no)),
                backup_finish_lsn: Some(lsn_for_seg(seg_no)),
                pg_version: 160003,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn integrity_ok_when_contiguous() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(&s, "base_000000010000000000000002", &sentinel(2)).await;
        put_wal_segment(&s, "000000010000000000000002").await;
        put_wal_segment(&s, "000000010000000000000003").await;

        let r = check_integrity(s).await.unwrap();
        assert_eq!(r.status, ReportStatus::Ok);
        assert_eq!(r.timeline, 1);
        assert_eq!(r.start_lsn, Some(lsn_for_seg(2)));
        assert!(r.gaps.is_empty());
    }

    #[tokio::test]
    async fn integrity_failure_on_gap() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(&s, "base_000000010000000000000002", &sentinel(2)).await;
        put_wal_segment(&s, "000000010000000000000002").await;
        // gap: seg 3 missing, head is seg 4
        put_wal_segment(&s, "000000010000000000000004").await;

        let r = check_integrity(s.clone()).await.unwrap();
        assert_eq!(r.status, ReportStatus::Failure);
        assert!(!r.gaps.is_empty());

        // run() surfaces FAILURE as a non-zero (Err) exit
        let err = run(s, WalVerifyOp::Integrity { json: false })
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("FAILURE"), "{err}");
    }

    #[tokio::test]
    async fn integrity_empty_without_backups() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_wal_segment(&s, "000000010000000000000002").await;
        let r = check_integrity(s).await.unwrap();
        assert_eq!(r.status, ReportStatus::Empty);
        assert!(r.backup_name.is_none());
    }

    #[tokio::test]
    async fn integrity_failure_without_start_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        // sentinel lacking LSN -> can't bound the WAL range -> FAILURE
        let mut sent = sentinel(2);
        sent.sentinel.backup_start_lsn = None;
        put_sentinel(&s, "base_000000010000000000000002", &sent).await;
        let r = check_integrity(s).await.unwrap();
        assert_eq!(r.status, ReportStatus::Failure);
        assert!(r.start_lsn.is_none());
    }

    #[tokio::test]
    async fn timeline_ok_failure_empty() {
        // empty store -> Empty
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        assert_eq!(check_timeline(s).await.unwrap().status, ReportStatus::Empty);

        // matching timelines -> Ok
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(&s, "base_000000010000000000000002", &sentinel(2)).await;
        put_wal_segment(&s, "000000010000000000000002").await;
        let t = check_timeline(s).await.unwrap();
        assert_eq!(t.status, ReportStatus::Ok);
        assert_eq!(t.current_timeline, Some(1));
        assert_eq!(t.latest_backup_timeline, Some(1));

        // backup on tli 1 but archived segment on tli 2 -> Failure
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(&s, "base_000000010000000000000002", &sentinel(2)).await;
        put_wal_segment(&s, "000000020000000000000009").await;
        assert_eq!(
            check_timeline(s).await.unwrap().status,
            ReportStatus::Failure
        );
    }

    #[tokio::test]
    async fn run_all_ok_plain_and_json() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(&s, "base_000000010000000000000002", &sentinel(2)).await;
        put_wal_segment(&s, "000000010000000000000002").await;
        put_wal_segment(&s, "000000010000000000000003").await;
        // plain branch (print_integrity + print_timeline)
        run(s.clone(), WalVerifyOp::All { json: false })
            .await
            .unwrap();
        // json branch
        run(s, WalVerifyOp::All { json: true }).await.unwrap();
    }
}
