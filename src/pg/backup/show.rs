//! backup-show & backup-mark
//!
//! Pure sentinel read / mutation, no replication protocol involved

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;

use crate::compression::AsyncReader;
use crate::pg::backup::{
    BackupSentinelDtoV2, FilesMetadataDto,
    fetch::{fetch_sentinel, resolve_name},
    files_metadata_key, format_pg_lsn, load_json, name_from_sentinel_key, sentinel_key,
};
use crate::storage::DynStorage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Plain,
    Json,
}

/// `backup-show <name|LATEST>` -- pretty/JSON dump of one sentinel and a
/// summary line from `files_metadata.json` (file count, tar-part count)
pub async fn show(storage: DynStorage, name: &str, format: Format) -> Result<()> {
    let resolved = resolve_name(&storage, name).await?;
    let sentinel = fetch_sentinel(&storage, &resolved).await?;
    // files_metadata is optional — older backups may not have it
    let files = match fetch_files_metadata(&storage, &resolved).await {
        Ok(f) => Some(f),
        Err(e) => {
            tracing::warn!(
                target = "backup_show",
                "files_metadata missing or unparseable for {resolved}: {e:#}"
            );
            None
        }
    };

    match format {
        Format::Json => {
            #[derive(serde::Serialize)]
            struct Out<'a> {
                name: &'a str,
                sentinel: &'a BackupSentinelDtoV2,
                #[serde(skip_serializing_if = "Option::is_none")]
                files_metadata: Option<&'a FilesMetadataDto>,
            }
            let out = Out {
                name: &resolved,
                sentinel: &sentinel,
                files_metadata: files.as_ref(),
            };
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        Format::Plain => print_plain(&resolved, &sentinel, files.as_ref()),
    }
    Ok(())
}

/// `backup-mark <name> --permanent | --impermanent`
/// Fetches the sentinel, flips `IsPermanent`, re-uploads. wal-g's behavior
pub async fn mark(storage: DynStorage, name: &str, permanent: bool) -> Result<()> {
    let resolved = resolve_name(&storage, name).await?;
    let mut sentinel = fetch_sentinel(&storage, &resolved).await?;
    if sentinel.is_permanent == permanent {
        tracing::info!(
            target = "backup_mark",
            "{resolved} already IsPermanent={permanent}; no-op"
        );
        return Ok(());
    }
    sentinel.is_permanent = permanent;
    let bytes = serde_json::to_vec(&sentinel)?;
    let len = bytes.len() as u64;
    let r: AsyncReader = Box::pin(std::io::Cursor::new(bytes));
    let key = sentinel_key(&resolved);
    storage
        .put(&key, r, Some(len))
        .await
        .with_context(|| format!("put {key}"))?;
    println!("{resolved} IsPermanent={permanent}");
    Ok(())
}

/// Resolve a backup name from a `--target-user-data` JSON value. Walks every
/// sentinel, deeply compares its `UserData` to the parsed target. Errors when
/// nothing matches, or when two distinct backups share the value
pub async fn resolve_by_user_data(storage: &DynStorage, user_data_str: &str) -> Result<String> {
    let target: serde_json::Value = serde_json::from_str(user_data_str)
        .with_context(|| format!("--target-user-data is not valid JSON: {user_data_str}"))?;
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let mut stream = storage
        .list(&prefix)
        .await
        .with_context(|| format!("list {prefix}"))?;
    let mut matches: Vec<String> = Vec::new();
    while let Some(item) = stream.next().await {
        let obj = item.context("list iteration")?;
        let Some(name) = name_from_sentinel_key(&obj.key) else {
            continue;
        };
        let name = name.to_string();
        let sentinel = match fetch_sentinel(storage, &name).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    target = "backup_mark",
                    "skip {name} during user-data search: {e:#}"
                );
                continue;
            }
        };
        let ud = sentinel
            .sentinel
            .user_data
            .unwrap_or(serde_json::Value::Null);
        if ud == target {
            matches.push(name);
        }
    }
    match matches.len() {
        0 => Err(anyhow!("no backup found with user-data: {user_data_str}")),
        1 => Ok(matches.into_iter().next().unwrap()),
        _ => bail!(
            "{} backups match user-data: {}",
            matches.len(),
            matches.join(", ")
        ),
    }
}

async fn fetch_files_metadata(storage: &DynStorage, name: &str) -> Result<FilesMetadataDto> {
    load_json(storage, &files_metadata_key(name), 16 * 1024).await
}

fn print_plain(name: &str, s: &BackupSentinelDtoV2, files: Option<&FilesMetadataDto>) {
    println!("name              {name}");
    println!("start_time        {}", s.start_time);
    println!("finish_time       {}", s.finish_time);
    println!("hostname          {}", s.hostname);
    println!("data_dir          {}", s.data_dir);
    println!("pg_version        {}", s.sentinel.pg_version);
    println!(
        "system_identifier {}",
        s.sentinel
            .system_identifier
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".into())
    );
    println!("is_permanent      {}", s.is_permanent);
    println!(
        "start_lsn         {}",
        s.sentinel
            .backup_start_lsn
            .map(format_pg_lsn)
            .unwrap_or_else(|| "-".into())
    );
    println!(
        "finish_lsn        {}",
        s.sentinel
            .backup_finish_lsn
            .map(format_pg_lsn)
            .unwrap_or_else(|| "-".into())
    );
    println!("uncompressed_size {}", s.sentinel.uncompressed_size);
    println!("compressed_size   {}", s.sentinel.compressed_size);
    println!(
        "files_metadata    {}",
        if s.sentinel.files_metadata_disabled {
            "disabled".into()
        } else if let Some(f) = files {
            format!(
                "{} files across {} part(s)",
                f.files.len(),
                f.tar_file_sets.len()
            )
        } else {
            "missing".into()
        }
    );
    if let Some(spec) = s.sentinel.tablespace_spec.as_ref() {
        println!(
            "tablespaces       {} user-defined",
            spec.tablespace_names.len()
        );
        for name in &spec.tablespace_names {
            if let Some(loc) = spec.locations.get(name) {
                println!("  {name:>10}  loc={}  link={}", loc.location, loc.symlink);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::backup::test_fixtures::{fs_store, put_files_metadata, put_sentinel};
    use crate::pg::backup::{BackupSentinelDto, FileDescription, LATEST, TablespaceSpec};

    const NAME: &str = "base_000000010000000000000002";

    fn sentinel() -> BackupSentinelDtoV2 {
        BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: Some(0x0200_0000),
                backup_finish_lsn: Some(0x0200_1000),
                pg_version: 160003,
                system_identifier: Some(7_000_000_000_000_000_000),
                uncompressed_size: 2048,
                compressed_size: 1024,
                ..Default::default()
            },
            hostname: "host".into(),
            data_dir: "/data".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn show_with_files_metadata_and_tablespaces() {
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        let mut sent = sentinel();
        let mut spec = TablespaceSpec::new("/var/lib/pg/16/main");
        spec.add(16384, "/srv/ts_a");
        sent.sentinel.tablespace_spec = Some(spec);
        put_sentinel(&s, NAME, &sent).await;

        let mut fm = FilesMetadataDto::default();
        fm.files.insert("base/1".into(), FileDescription::default());
        fm.tar_file_sets
            .insert("part_001.tar".into(), vec!["base/1".into()]);
        put_files_metadata(&s, NAME, &fm).await;

        // plain + json branches; plain hits the files + tablespace formatting
        show(s.clone(), NAME, Format::Plain).await.unwrap();
        show(s.clone(), NAME, Format::Json).await.unwrap();
        // LATEST resolves the single backup by mtime
        show(s, LATEST, Format::Plain).await.unwrap();
    }

    #[tokio::test]
    async fn show_files_metadata_disabled_then_missing() {
        // FilesMetadataDisabled -> "disabled", no sidecar fetch attempted
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        let mut sent = sentinel();
        sent.sentinel.files_metadata_disabled = true;
        put_sentinel(&s, NAME, &sent).await;
        show(s, NAME, Format::Plain).await.unwrap();

        // not disabled, no sidecar present -> "missing" (warn path)
        let dir = tempfile::tempdir().unwrap();
        let s = fs_store(dir.path());
        put_sentinel(&s, NAME, &sentinel()).await;
        show(s, NAME, Format::Plain).await.unwrap();
    }
}
