//! backup-push: streaming BASE_BACKUP via the postgres replication protocol
//!
//! Pipeline:
//!   BASE_BACKUP archive (per tablespace) → tar_streamer (path remap +
//!   per-file metadata + part rotation at WALG_TAR_SIZE_THRESHOLD) →
//!   compression → counting reader → Storage::put
//!
//! The data dir's `global/pg_control` is teed into a separate `pg_control.tar`
//! so `backup-fetch` can apply it last (matches wal-g's restore ordering)
//!
//! `--pgdata` is optional; absent it, the sentinel records the PG-reported
//! `data_directory` and we never touch the local filesystem

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use bytes::{Bytes, BytesMut};
use postgres_protocol::message::backend::Message;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;

use crate::compression::{self, AsyncReader};
use crate::config::Settings;
use crate::pg::backup::delta;
use crate::pg::backup::tar_streamer::{self, StreamerOpts, tablespace_prefix};
use crate::pg::backup::{
    BACKUP_NAME_PREFIX, BackupSentinelDto, BackupSentinelDtoV2, ExtendedMetadataDto,
    FileDescription, FilesMetadataDto, METADATA_DATETIME_FORMAT, TablespaceSpec,
    files_metadata_key, format_backup_name, metadata_key, sentinel_key, tar_part_key,
    tar_partitions_prefix,
};
use crate::pg::replication::PgConfig;
use crate::pg::replication::base_backup::{
    BackupEvent, BaseBackupOpts, ChannelReader, Tablespace, run_base_backup,
};
use crate::pg::replication::conn::{ReplicationConn, error_message};
use crate::storage::DynStorage;

/// Entry name (post-remap) that should be teed into a standalone tar so
/// restore can apply it last
const PG_CONTROL_ENTRY: &str = "global/pg_control";
const PG_CONTROL_TAR: &str = "pg_control.tar";

#[derive(Debug, Default, Clone)]
pub struct PushArgs {
    pub pgdata: Option<PathBuf>,
    pub is_permanent: bool,
    pub user_data: Option<serde_json::Value>,
    pub fast_checkpoint: bool,
    pub no_verify_checksums: bool,
    /// `WALG_TAR_SIZE_THRESHOLD` override (bytes). 0 = use the streamer default
    pub tar_size_threshold: u64,
    /// `--delta-from-wal-summaries`: build delta map from PG17 walsummarizer
    /// output, emit native INCREMENTAL files instead of wal-g `wi1` format
    pub delta_from_wal_summaries: bool,
    /// `--full`: explicit override to skip delta detection
    pub full: bool,
}

pub async fn handle(settings: &Settings, storage: DynStorage, args: PushArgs) -> Result<()> {
    let start_time = chrono::Utc::now();

    // Resolve a delta parent if WALG_DELTA_MAX_STEPS > 0 (or --delta-from-
    // wal-summaries). The pre-flight runs eagerly so misconfig surfaces.
    // Streamer-side increment emission is not yet wired (see PHASEC.md /
    // PHASEC2.md): when a parent is found we log it & fall back to full,
    // so the sentinel never claims a delta that the bucket can't deliver.
    //
    // `--full` short-circuits delta detection entirely (matches wal-g's
    // `--full` flag). `--delta-from-wal-summaries` is mutually exclusive
    let parent = if args.full {
        None
    } else {
        delta::configure_delta_parent(&storage, &settings.delta, args.is_permanent).await?
    };
    if let Some(p) = parent.as_ref() {
        let mode = if args.delta_from_wal_summaries {
            "wal-summaries + PG17 native"
        } else {
            "WAL-walk + wi1"
        };
        tracing::warn!(
            target = "backup_push",
            "delta parent {} resolved (count={}, start_lsn={:X}, mode={mode}) but increment \
             generation not yet implemented; producing a full backup",
            p.name,
            p.increment_count,
            p.start_lsn,
        );
        // TODO(phase-c): pass parent + format flag into streamer
    }
    let _drop_parent_for_now: Option<delta::PrevBackupInfo> = None;
    let _ = _drop_parent_for_now;

    let cfg = PgConfig::from_env()?;
    tracing::info!(
        target = "backup_push",
        "connecting to {}:{} as {} (db={})",
        cfg.host,
        cfg.port,
        cfg.user,
        cfg.database
    );
    let mut conn = ReplicationConn::connect(&cfg).await?;
    let pg_version = conn.server_pg_version();
    let system_identifier = identify_system(&mut conn)
        .await
        .context("IDENTIFY_SYSTEM")?;
    let data_directory = match args.pgdata.as_ref() {
        Some(p) => p
            .canonicalize()
            .unwrap_or_else(|_| p.clone())
            .display()
            .to_string(),
        None => fetch_setting(&mut conn, "data_directory")
            .await
            .unwrap_or_default(),
    };

    // `--delta-from-wal-summaries`: server-side preconditions + build the
    // delta map up front so the BASE_BACKUP can run with a hot map. PG17 +
    // summarize_wal=on are hard requirements; abort early on either miss
    if args.delta_from_wal_summaries {
        if pg_version < 170000 {
            bail!(
                "--delta-from-wal-summaries requires PostgreSQL 17 or newer (server reports {pg_version})"
            );
        }
        let on = fetch_setting(&mut conn, "summarize_wal").await?;
        if on.trim() != "on" {
            bail!("--delta-from-wal-summaries requires summarize_wal=on on the server");
        }
        // TODO(phase-c2): build delta map from pg_wal/summaries & feed it
        // into the streamer. The upper bound (this backup's start LSN) only
        // becomes known after BackupEvent::Start fires, so the build either
        // has to move into the event loop, or wait until START_REPLICATION
        // returns the start LSN
        match (parent.as_ref(), args.pgdata.as_ref()) {
            (None, _) => tracing::info!(
                target = "backup_push",
                "no delta parent resolved; --delta-from-wal-summaries will produce a full backup"
            ),
            (Some(p), Some(pgdata)) => tracing::info!(
                target = "backup_push",
                "would build delta map from {}/pg_wal/summaries for [{:X}, ?) on timeline {}",
                pgdata.display(),
                p.start_lsn,
                p.timeline,
            ),
            (Some(_), None) => tracing::warn!(
                target = "backup_push",
                "--delta-from-wal-summaries without --pgdata: WAL summaries live on the \
                 PG host's filesystem, not reachable from a remote pusher; skipping",
            ),
        }
    }

    let label = format!("wal-rs {}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"));
    let opts = BaseBackupOpts {
        label: label.clone(),
        fast_checkpoint: args.fast_checkpoint,
        no_verify_checksums: args.no_verify_checksums,
        max_rate_kib: None,
    };

    let (tx, mut rx) = mpsc::channel::<Result<BackupEvent>>(2);
    let pump = tokio::spawn(run_base_backup(conn, opts, tx));

    let tar_size = if args.tar_size_threshold == 0 {
        tar_streamer::DEFAULT_TAR_SIZE_THRESHOLD
    } else {
        args.tar_size_threshold
    };

    let mut start_lsn = None;
    let mut backup_name: Option<String> = None;
    let mut uncompressed_size: i64 = 0;
    let mut compressed_size: i64 = 0;
    let mut file_no: u32 = 0;
    let mut tablespace_list: Vec<Tablespace> = Vec::new();
    let mut end_lsn: Option<u64> = None;
    let mut all_files: std::collections::HashMap<String, FileDescription> =
        std::collections::HashMap::new();
    let mut tar_file_sets: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut pg_control_tee: Option<Bytes> = None;

    while let Some(event) = rx.recv().await {
        let event = event?;
        match event {
            BackupEvent::Start(info) => {
                start_lsn = Some(info.start_lsn);
                tablespace_list = info.tablespaces.clone();
                let seg_size = crate::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;
                let name = format_backup_name(info.timeline, info.start_lsn, seg_size);
                debug_assert!(name.starts_with(BACKUP_NAME_PREFIX));
                backup_name = Some(name);
                tracing::info!(
                    target = "backup_push",
                    "BASE_BACKUP started: lsn={:X}/{:X} timeline={} tablespaces={}",
                    info.start_lsn >> 32,
                    info.start_lsn as u32,
                    info.timeline,
                    info.tablespaces.len()
                );
            }
            BackupEvent::Archive { meta, body } => {
                let name = backup_name
                    .as_ref()
                    .ok_or_else(|| anyhow!("archive before start info"))?;
                let is_data_dir = meta.is_data_dir();
                let prefix = if is_data_dir {
                    None
                } else {
                    Some(tablespace_prefix(meta.oid))
                };
                let tee_names = if is_data_dir {
                    vec![PG_CONTROL_ENTRY.to_string()]
                } else {
                    Vec::new()
                };
                let archive_reader: AsyncReader = Box::pin(ChannelReader::new(body));
                let (counter_handle, counted) = wrap_with_counter(archive_reader);

                let streamer_opts = StreamerOpts {
                    prefix,
                    tee_names,
                    max_tar_size: tar_size,
                    starting_file_no: file_no,
                };
                let archive_label = if is_data_dir {
                    "base.tar".to_string()
                } else {
                    format!("{}.tar", meta.oid)
                };
                tracing::info!(
                    target = "backup_push",
                    "streaming {archive_label} via tarball streamer (threshold={tar_size} bytes)"
                );
                let (mut parts_rx, streamer_task) = tar_streamer::start(counted, streamer_opts);

                while let Some(part_res) = parts_rx.recv().await {
                    let part =
                        part_res.with_context(|| format!("streamer part: {archive_label}"))?;
                    let key = tar_part_key(name, part.file_no, settings.compression.extension());
                    tracing::info!(target = "backup_push", "uploading {key} <- {archive_label}");
                    let reader: AsyncReader = Box::pin(part.reader);
                    let compressed = compression::encode(
                        settings.compression,
                        reader,
                        settings.compression_level,
                    );
                    let put_counter = Arc::new(AtomicU64::new(0));
                    let counting = wrap_counted_reader(compressed, put_counter.clone());
                    let throttled = settings.throttle_network(counting);
                    storage
                        .put(&key, throttled, None)
                        .await
                        .with_context(|| format!("put {key}"))?;
                    compressed_size += put_counter.load(Ordering::Relaxed) as i64;
                    file_no = file_no.max(part.file_no);
                }

                let result = streamer_task
                    .await
                    .context("streamer task join")?
                    .with_context(|| format!("streamer task: {archive_label}"))?;
                file_no = result.last_file_no;
                uncompressed_size += counter_handle.bytes() as i64;
                for (name, meta) in result.files {
                    all_files.insert(
                        name,
                        FileDescription {
                            is_incremented: meta.is_incremented,
                            is_skipped: meta.is_skipped,
                            mtime: meta.mtime,
                            updates_count: 0,
                        },
                    );
                }
                for (k, v) in result.tar_file_sets {
                    tar_file_sets.entry(k).or_default().extend(v);
                }
                if is_data_dir && let Some(t) = result.tee_bytes {
                    pg_control_tee = Some(t);
                }
            }
            BackupEvent::Finish(info) => {
                end_lsn = Some(info.end_lsn);
                tracing::info!(
                    target = "backup_push",
                    "BASE_BACKUP finished at {:X}/{:X}",
                    info.end_lsn >> 32,
                    info.end_lsn as u32
                );
            }
        }
    }

    if let Err(e) = pump.await {
        bail!("base backup pump panicked: {e}");
    }

    let backup_name = backup_name.ok_or_else(|| anyhow!("no start info received"))?;
    let start_lsn = start_lsn.ok_or_else(|| anyhow!("no start LSN received"))?;
    let end_lsn = end_lsn.ok_or_else(|| anyhow!("no end LSN received"))?;

    // Upload pg_control.tar as a tee so restore can apply it last
    if let Some(bytes) = pg_control_tee {
        let ext = settings.compression.extension();
        let key = if ext.is_empty() {
            format!("{}/{}", tar_partitions_prefix(&backup_name), PG_CONTROL_TAR)
        } else {
            format!(
                "{}/{}.{}",
                tar_partitions_prefix(&backup_name),
                PG_CONTROL_TAR,
                ext
            )
        };
        tracing::info!(target = "backup_push", "uploading {key} (pg_control tee)");
        let raw: AsyncReader = Box::pin(std::io::Cursor::new(bytes.to_vec()));
        let compressed = compression::encode(settings.compression, raw, settings.compression_level);
        let put_counter = Arc::new(AtomicU64::new(0));
        let counting = wrap_counted_reader(compressed, put_counter.clone());
        let throttled = settings.throttle_network(counting);
        storage
            .put(&key, throttled, None)
            .await
            .with_context(|| format!("put {key}"))?;
        compressed_size += put_counter.load(Ordering::Relaxed) as i64;
    }

    // Build TablespaceSpec from non-default tablespaces. Mirrors wal-g
    let user_tablespaces: Vec<&Tablespace> =
        tablespace_list.iter().filter(|t| !t.is_default()).collect();
    let tablespace_spec = if user_tablespaces.is_empty() {
        None
    } else {
        let mut spec = TablespaceSpec::new(&data_directory);
        for t in &user_tablespaces {
            spec.add(t.oid, &t.location);
        }
        Some(spec)
    };

    // Emit files_metadata.json sidecar
    let files_meta = FilesMetadataDto {
        files: all_files,
        tar_file_sets,
        databases_by_names: Default::default(),
    };
    upload_json(&storage, &files_metadata_key(&backup_name), &files_meta).await?;

    let hostname = hostname().unwrap_or_default();
    let finish_time = chrono::Utc::now();

    let sentinel = BackupSentinelDto {
        backup_start_lsn: Some(start_lsn),
        increment_from_lsn: None,
        increment_from: None,
        increment_full_name: None,
        increment_count: None,
        pg_version,
        backup_finish_lsn: Some(end_lsn),
        system_identifier: Some(system_identifier),
        uncompressed_size,
        compressed_size,
        data_catalog_size: 0,
        user_data: args.user_data.clone(),
        files_metadata_disabled: false,
        tablespace_spec: tablespace_spec.clone(),
        backup_start_chkp_num: None,
        increment_from_chkp_num: None,
    };
    let v2 = BackupSentinelDtoV2 {
        sentinel: sentinel.clone(),
        version: 2,
        start_time,
        finish_time,
        date_fmt: METADATA_DATETIME_FORMAT.into(),
        hostname: hostname.clone(),
        data_dir: data_directory.clone(),
        is_permanent: args.is_permanent,
    };
    let meta = ExtendedMetadataDto {
        start_time,
        finish_time,
        date_fmt: METADATA_DATETIME_FORMAT.into(),
        hostname,
        data_dir: data_directory,
        pg_version,
        start_lsn,
        finish_lsn: end_lsn,
        is_permanent: args.is_permanent,
        system_identifier: Some(system_identifier),
        uncompressed_size,
        compressed_size,
        user_data: args.user_data.clone(),
    };

    upload_json(&storage, &metadata_key(&backup_name), &meta).await?;
    upload_json(&storage, &sentinel_key(&backup_name), &v2).await?;

    tracing::info!(
        target = "backup_push",
        "wrote {backup_name} ({} parts, {} tablespace(s), {} bytes uncompressed, {} bytes compressed)",
        file_no,
        tablespace_list.len(),
        uncompressed_size,
        compressed_size,
    );
    println!("{backup_name}");
    Ok(())
}

async fn identify_system(conn: &mut ReplicationConn) -> Result<u64> {
    conn.send_query("IDENTIFY_SYSTEM").await?;
    let mut sysid: Option<u64> = None;
    loop {
        match conn.recv_message().await? {
            Message::RowDescription(_) => {}
            Message::DataRow(row) => {
                let buf = row.buffer_bytes().clone();
                if let Some(Some(range)) = first_data_row_col(&row) {
                    let s = std::str::from_utf8(&buf[range])?;
                    sysid = Some(s.parse().context("parse system_identifier")?);
                }
            }
            Message::CommandComplete(_) => {}
            Message::ReadyForQuery(_) => break,
            Message::ErrorResponse(e) => bail!("IDENTIFY_SYSTEM: {}", error_message(&e)),
            _ => continue,
        }
    }
    sysid.ok_or_else(|| anyhow!("IDENTIFY_SYSTEM did not return system identifier"))
}

async fn fetch_setting(conn: &mut ReplicationConn, name: &str) -> Result<String> {
    let q = format!("SHOW {name}");
    conn.send_query(&q).await?;
    let mut value: Option<String> = None;
    loop {
        match conn.recv_message().await? {
            Message::RowDescription(_) => {}
            Message::DataRow(row) => {
                let buf = row.buffer_bytes().clone();
                if let Some(Some(range)) = first_data_row_col(&row) {
                    value = Some(String::from_utf8(buf[range].to_vec())?);
                }
            }
            Message::CommandComplete(_) => {}
            Message::ReadyForQuery(_) => break,
            Message::ErrorResponse(e) => bail!("SHOW {name}: {}", error_message(&e)),
            _ => continue,
        }
    }
    value.ok_or_else(|| anyhow!("SHOW {name} returned no rows"))
}

fn first_data_row_col(
    row: &postgres_protocol::message::backend::DataRowBody,
) -> Option<Option<std::ops::Range<usize>>> {
    use fallible_iterator::FallibleIterator as _;
    row.ranges().next().ok().flatten()
}

async fn upload_json<T: serde::Serialize>(
    storage: &DynStorage,
    key: &str,
    value: &T,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    let len = bytes.len() as u64;
    let r: AsyncReader = Box::pin(std::io::Cursor::new(bytes));
    storage
        .put(key, r, Some(len))
        .await
        .with_context(|| format!("put {key}"))
}

fn hostname() -> Option<String> {
    let out = std::process::Command::new("hostname").output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Wraps an AsyncReader to count bytes read (for uncompressed_size). The
/// returned `CounterHandle` clones the same atomic so the final value is
/// visible after the reader is consumed
struct CountingReader {
    inner: AsyncReader,
    counter: Arc<AtomicU64>,
}

impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &r {
            let added = buf.filled().len() - before;
            self.counter.fetch_add(added as u64, Ordering::Relaxed);
        }
        r
    }
}

struct CounterHandle(Arc<AtomicU64>);

impl CounterHandle {
    fn bytes(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn wrap_with_counter(input: AsyncReader) -> (CounterHandle, AsyncReader) {
    let counter = Arc::new(AtomicU64::new(0));
    let r = CountingReader {
        inner: input,
        counter: counter.clone(),
    };
    (CounterHandle(counter), Box::pin(r))
}

fn wrap_counted_reader(input: AsyncReader, counter: Arc<AtomicU64>) -> AsyncReader {
    Box::pin(CountingReader {
        inner: input,
        counter,
    })
}

// silence unused-import warnings during partial builds
#[allow(dead_code)]
fn _bytes_marker(_: BytesMut) {}
