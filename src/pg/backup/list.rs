//! backup-list: enumerate sentinel files under basebackups_005/, fetch each,
//! print backup names with start/finish times and LSNs

use std::num::NonZeroU64;

use anyhow::{Context, Result};
use futures::StreamExt;

use crate::pg::backup::fetch::fetch_sentinel;
use crate::pg::backup::name_from_sentinel_key;
use crate::storage::{ObjExt, Operator};

#[derive(Debug, Clone, serde::Serialize)]
pub struct BackupSummary {
    pub name: String,
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub finish_time: Option<chrono::DateTime<chrono::Utc>>,
    pub start_lsn: Option<NonZeroU64>,
    pub finish_lsn: Option<NonZeroU64>,
    pub pg_version: i32,
    pub hostname: Option<String>,
    pub is_permanent: bool,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Plain,
    Json,
}

pub async fn handle(storage: Operator, format: Format) -> Result<()> {
    let backups = collect(storage).await?;
    match format {
        Format::Plain => print_plain(&backups),
        Format::Json => {
            let s = serde_json::to_string_pretty(&backups)?;
            println!("{s}");
        }
    }
    Ok(())
}

pub async fn collect(storage: Operator) -> Result<Vec<BackupSummary>> {
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list_objs(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let mut sentinel_keys: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> = Vec::new();
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        if name_from_sentinel_key(&obj.key).is_some() {
            sentinel_keys.push((obj.key, obj.last_modified));
        }
    }

    let mut out = Vec::with_capacity(sentinel_keys.len());
    for (key, mtime) in sentinel_keys {
        let name = name_from_sentinel_key(&key).unwrap().to_string();
        let summary = match fetch_summary(&storage, &name).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target = "backup_list", "skip {name}: {e:#}");
                BackupSummary {
                    name,
                    start_time: mtime,
                    finish_time: None,
                    start_lsn: None,
                    finish_lsn: None,
                    pg_version: 0,
                    hostname: None,
                    is_permanent: false,
                    compressed_size: 0,
                    uncompressed_size: 0,
                }
            }
        };
        out.push(summary);
    }
    out.sort_by_key(|a| a.start_time);
    Ok(out)
}

async fn fetch_summary(storage: &Operator, name: &str) -> Result<BackupSummary> {
    let v2 = fetch_sentinel(storage, name).await?;
    Ok(BackupSummary {
        name: name.to_string(),
        start_time: Some(v2.start_time),
        finish_time: Some(v2.finish_time),
        start_lsn: v2.sentinel.backup_start_lsn,
        finish_lsn: v2.sentinel.backup_finish_lsn,
        pg_version: v2.sentinel.pg_version,
        hostname: Some(v2.hostname),
        is_permanent: v2.is_permanent,
        compressed_size: v2.sentinel.compressed_size,
        uncompressed_size: v2.sentinel.uncompressed_size,
    })
}

fn print_plain(backups: &[BackupSummary]) {
    println!(
        "{:<48} {:<26} {:<26} {:>12} {:<10}",
        "name", "start_time", "finish_time", "pg_ver", "hostname"
    );
    for b in backups {
        let start = b
            .start_time
            .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_else(|| "-".into());
        let finish = b
            .finish_time
            .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_else(|| "-".into());
        let host = b.hostname.as_deref().unwrap_or("-");
        println!(
            "{:<48} {:<26} {:<26} {:>12} {:<10}",
            b.name, start, finish, b.pg_version, host
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::backup::test_fixtures::{fs_store, put_bytes, put_sentinel};
    use crate::pg::backup::{BackupSentinelDto, BackupSentinelDtoV2, sentinel_key};
    use chrono::{TimeZone, Utc};

    fn sentinel(host: &str, ts: i64, perm: bool) -> BackupSentinelDtoV2 {
        BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: NonZeroU64::new(0x0200_0000),
                backup_finish_lsn: NonZeroU64::new(0x0200_1000),
                pg_version: 160003,
                uncompressed_size: 2048,
                compressed_size: 1024,
                ..Default::default()
            },
            hostname: host.into(),
            is_permanent: perm,
            start_time: Utc.timestamp_opt(ts, 0).unwrap(),
            finish_time: Utc.timestamp_opt(ts + 60, 0).unwrap(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn collect_reads_sorts_and_skips_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(
            &s,
            "base_000000010000000000000005",
            &sentinel("hostB", 2000, false),
        )
        .await;
        put_sentinel(
            &s,
            "base_000000010000000000000003",
            &sentinel("hostA", 1000, true),
        )
        .await;
        // corrupt sentinel: discovered by suffix, fetched as a fallback summary
        put_bytes(
            &s,
            &sentinel_key("base_000000010000000000000007"),
            b"not json".to_vec(),
        )
        .await;

        let backups = collect(s.clone()).await.unwrap();
        assert_eq!(backups.len(), 3);

        let a = backups
            .iter()
            .find(|b| b.name.ends_with("00000003"))
            .unwrap();
        assert_eq!(a.hostname.as_deref(), Some("hostA"));
        assert!(a.is_permanent);
        assert_eq!(a.pg_version, 160003);
        assert_eq!(a.compressed_size, 1024);

        let corrupt = backups
            .iter()
            .find(|b| b.name.ends_with("00000007"))
            .unwrap();
        assert_eq!(corrupt.pg_version, 0, "corrupt sentinel -> fallback");
        assert!(corrupt.hostname.is_none());

        // both output formats render without error
        handle(s.clone(), Format::Plain).await.unwrap();
        handle(s, Format::Json).await.unwrap();
    }
}
