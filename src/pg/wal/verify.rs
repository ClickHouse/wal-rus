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
use crate::pg::backup::parse_timeline_from_backup_name;
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
            r.start_lsn
                .map(|l| format!("{:X}/{:X}", l >> 32, l as u32))
                .unwrap_or_else(|| "-".into())
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
