//! backup-list: enumerate sentinel files under basebackups_005/, fetch each,
//! print backup names with start/finish times and LSNs

use anyhow::{Context, Result};
use futures::StreamExt;

use crate::pg::backup::fetch::fetch_sentinel;
use crate::pg::backup::name_from_sentinel_key;
use crate::storage::DynStorage;

#[derive(Debug, Clone, serde::Serialize)]
pub struct BackupSummary {
    pub name: String,
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub finish_time: Option<chrono::DateTime<chrono::Utc>>,
    pub start_lsn: Option<u64>,
    pub finish_lsn: Option<u64>,
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

pub async fn handle(storage: DynStorage, format: Format) -> Result<()> {
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

pub async fn collect(storage: DynStorage) -> Result<Vec<BackupSummary>> {
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list(&prefix)
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

async fn fetch_summary(storage: &DynStorage, name: &str) -> Result<BackupSummary> {
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
