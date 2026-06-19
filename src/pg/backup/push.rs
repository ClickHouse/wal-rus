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
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;

use crate::compression::{self, AsyncReader};
use crate::concurrency::BoundedTasks;
use crate::config::Settings;
use crate::pg::backup::delta::{self, PrevBackupInfo};
use crate::pg::backup::increment::Format as IncrementFormat;
use crate::pg::backup::tar_streamer::{self, DeltaContext, StreamerOpts, tablespace_prefix};
use crate::pg::backup::{
    BACKUP_NAME_PREFIX, BackupSentinelDto, BackupSentinelDtoV2, ExtendedMetadataDto,
    FileDescription, FilesMetadataDto, METADATA_DATETIME_FORMAT, TablespaceSpec,
    files_metadata_key, format_backup_name, format_pg_lsn, metadata_key, sentinel_key,
    tar_part_key, tar_partitions_prefix,
};
use crate::pg::replication::PgConfig;
use crate::pg::replication::base_backup::{
    BackupEvent, BaseBackupOpts, ChannelReader, Tablespace, run_base_backup,
};
use crate::pg::replication::conn::ReplicationConn;
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
    /// `--delta-from-wal-summaries`: build the delta map from the PG17
    /// walsummarizer instead of walking archived WAL. Source only; output
    /// encoding is `increment_format`
    pub delta_from_wal_summaries: bool,
    /// `--increment-format`: delta file wire format, `wi1` (default) or PG17
    /// `native`. Independent of the delta-map source
    pub increment_format: IncrementFormat,
    /// `--full`: explicit override to skip delta detection
    pub full: bool,
}

pub async fn handle(settings: &Settings, storage: DynStorage, args: PushArgs) -> Result<()> {
    let start_time = chrono::Utc::now();

    // Resolve a delta parent if WALG_DELTA_MAX_STEPS > 0 (or --delta-from-
    // wal-summaries). When found, build a delta map after BackupEvent::Start
    // (its end-LSN is only known then) & feed it into the streamer
    //
    // `--full` short-circuits delta detection entirely (matches wal-g's
    // `--full` flag). Output encoding is `--increment-format` (wi1 default),
    // independent of whether the map came from WAL walk or wal-summaries
    let parent = if args.full {
        None
    } else {
        delta::configure_delta_parent(&storage, &settings.delta, args.is_permanent).await?
    };
    let increment_format = args.increment_format;
    if let Some(p) = parent.as_ref() {
        // A restore chain reconstructs linearly via increment_from, so it must
        // use one format end-to-end: wal-g can't read native, & each apply
        // assumes the parent state was written by the same scheme. Refuse to
        // extend a delta parent with a different format (full parents start a
        // fresh chain, so they're unconstrained)
        if let Some(parent_fmt) = p.parent_increment_format
            && parent_fmt != increment_format
        {
            bail!(
                "increment format mismatch: delta parent {} uses {parent_fmt:?} but \
                 --increment-format requests {increment_format:?}; a chain must use one \
                 format end-to-end (match the parent, or pass --full for a new chain)",
                p.name,
            );
        }
        tracing::info!(
            target = "backup_push",
            "delta parent {} (count={}, start_lsn={}, format={:?})",
            p.name,
            p.increment_count,
            format_pg_lsn(p.start_lsn),
            increment_format,
        );
    }

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

    // `--delta-from-wal-summaries`: server-side preconditions checked up
    // front. PG17 + summarize_wal=on are hard requirements; abort early on
    // either miss. The map itself is built once BackupEvent::Start delivers
    // the new start LSN, since `[parent.start_lsn, this.start_lsn)` is the
    // input range for both wal-summary & WAL-walk paths
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
        if parent.is_some() && args.pgdata.is_none() {
            bail!(
                "--delta-from-wal-summaries requires --pgdata: WAL summaries live on \
                 the PG host filesystem & cannot be read remotely"
            );
        }
    }

    let label = format!("wal-rs {}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"));
    let opts = BaseBackupOpts {
        label: label.clone(),
        fast_checkpoint: args.fast_checkpoint,
        no_verify_checksums: args.no_verify_checksums,
        max_rate_kib: None,
        // wal-g push uploads tablespaces separately and ships WAL via
        // `wal-push`; inlining the segments would duplicate them
        wal: false,
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
    // Built when BackupEvent::Start arrives, then shared by every tablespace
    // streamer for this push. Stays None for full backups
    let mut delta_context: Option<DeltaContext> = None;

    while let Some(event) = rx.recv().await {
        let event = event?;
        match event {
            BackupEvent::Start(info) => {
                start_lsn = Some(info.start_lsn);
                tablespace_list = info.tablespaces.clone();
                let seg_size = crate::pg::wal::segment::wal_segment_size();
                let base_name = format_backup_name(info.timeline, info.start_lsn, seg_size);
                debug_assert!(base_name.starts_with(BACKUP_NAME_PREFIX));
                // Delta backups get a `_D_<parent-without-base_>` suffix
                // (wal-g convention). delete/list/show all key off this
                let resolved_name = match parent.as_ref() {
                    Some(p) => format!(
                        "{base_name}_D_{}",
                        p.name.strip_prefix(BACKUP_NAME_PREFIX).unwrap_or(&p.name),
                    ),
                    None => base_name.clone(),
                };
                backup_name = Some(resolved_name);
                tracing::info!(
                    target = "backup_push",
                    "BASE_BACKUP started: lsn={} timeline={} tablespaces={}",
                    format_pg_lsn(info.start_lsn),
                    info.timeline,
                    info.tablespaces.len()
                );

                // Build the delta map once we know the upper LSN bound. WAL
                // walk vs wal-summaries decided by --delta-from-wal-summaries.
                // Failures here drop us to a full backup rather than aborting
                // (wal-g semantics: a partial delta is worse than a full)
                if let Some(p) = parent.as_ref() {
                    let span = info.start_lsn.saturating_sub(p.start_lsn);
                    if info.start_lsn <= p.start_lsn {
                        tracing::warn!(
                            target = "backup_push",
                            "new start LSN {:X} <= parent {:X}; producing a full backup",
                            info.start_lsn,
                            p.start_lsn,
                        );
                    } else if args.delta_from_wal_summaries {
                        match build_delta_map_from_summaries(
                            args.pgdata.as_deref(),
                            info.timeline,
                            p.start_lsn,
                            info.start_lsn,
                        ) {
                            Ok(map) => {
                                tracing::info!(
                                    target = "backup_push",
                                    "delta map built from wal-summaries: \
                                     {} dirty page(s) over {} bytes of WAL",
                                    map.len(),
                                    span,
                                );
                                delta_context = Some(DeltaContext {
                                    map: Arc::new(map),
                                    format: increment_format,
                                    parent_files: p.parent_files.clone(),
                                });
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target = "backup_push",
                                    "delta map from wal-summaries failed ({e:#}); \
                                     producing a full backup",
                                );
                            }
                        }
                    } else {
                        match delta::build_delta_map_from_wal(
                            settings,
                            &storage,
                            p.timeline,
                            p.start_lsn,
                            info.start_lsn,
                            settings.compression,
                        )
                        .await
                        {
                            Ok(map) => {
                                tracing::info!(
                                    target = "backup_push",
                                    "delta map built from WAL walk: \
                                     {} dirty page(s) over {} bytes of WAL",
                                    map.len(),
                                    span,
                                );
                                delta_context = Some(DeltaContext {
                                    map: Arc::new(map),
                                    format: increment_format,
                                    parent_files: p.parent_files.clone(),
                                });
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target = "backup_push",
                                    "delta map from WAL walk failed ({e:#}); \
                                     producing a full backup",
                                );
                            }
                        }
                    }
                }
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
                    queue_depth: settings.upload_queue,
                    delta_context: delta_context.clone(),
                };
                let archive_label = if is_data_dir {
                    "base.tar".to_string()
                } else {
                    format!("{}.tar", meta.oid)
                };
                tracing::info!(
                    target = "backup_push",
                    "streaming {archive_label} via tarball streamer (threshold={tar_size} bytes, \
                     upload_concurrency={}, upload_queue={})",
                    settings.upload_concurrency,
                    settings.upload_queue,
                );
                let (mut parts_rx, streamer_task) = tar_streamer::start(counted, streamer_opts);

                let mut uploads =
                    BoundedTasks::new(settings.upload_concurrency, "upload", |r: Result<u64>| {
                        compressed_size += r? as i64;
                        Ok(())
                    });
                while let Some(part_res) = parts_rx.recv().await {
                    let part =
                        part_res.with_context(|| format!("streamer part: {archive_label}"))?;
                    let key = tar_part_key(name, part.file_no, settings.compression.extension());
                    tracing::info!(target = "backup_push", "uploading {key} <- {archive_label}");
                    file_no = file_no.max(part.file_no);

                    let s = storage.clone();
                    let cfg = settings.clone();
                    uploads
                        .spawn(async move {
                            let reader: AsyncReader = Box::pin(part.reader);
                            let compressed =
                                compression::encode(cfg.compression, reader, cfg.compression_level);
                            let encrypted = cfg.encrypt(compressed);
                            let counter = Arc::new(AtomicU64::new(0));
                            let counting = wrap_counted_reader(encrypted, counter.clone());
                            let throttled = cfg.throttle_network(counting);
                            s.put(&key, throttled, None)
                                .await
                                .with_context(|| format!("put {key}"))?;
                            Ok(counter.load(Ordering::Relaxed))
                        })
                        .await?;
                }

                let result = streamer_task
                    .await
                    .context("streamer task join")?
                    .with_context(|| format!("streamer task: {archive_label}"))?;
                uploads.join().await?;
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
                    "BASE_BACKUP finished at {}",
                    format_pg_lsn(info.end_lsn)
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
        let encrypted = settings.encrypt(compressed);
        let put_counter = Arc::new(AtomicU64::new(0));
        let counting = wrap_counted_reader(encrypted, put_counter.clone());
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

    // Wire the parent linkage into the sentinel only when increment
    // generation actually ran (delta_context is set). If the delta map
    // build failed earlier, parent stays informational but the sentinel
    // must claim FULL — otherwise restore would walk a chain whose
    // increments don't exist
    let (incr_from_lsn, incr_from_name, incr_full_name, incr_count, incr_format) =
        match (parent.as_ref(), delta_context.as_ref()) {
            (Some(p), Some(ctx)) => (
                Some(p.start_lsn),
                Some(p.name.clone()),
                Some(resolve_increment_full_name(p)),
                Some(p.increment_count as i32),
                ctx.format,
            ),
            _ => (None, None, None, None, IncrementFormat::default()),
        };
    let sentinel = BackupSentinelDto {
        backup_start_lsn: Some(start_lsn),
        increment_from_lsn: incr_from_lsn,
        increment_from: incr_from_name,
        increment_full_name: incr_full_name,
        increment_count: incr_count,
        increment_format: incr_format,
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
    let rows = conn.query_rows("IDENTIFY_SYSTEM").await?;
    rows.first()
        .and_then(|cols| cols.first())
        .and_then(|c| c.as_deref())
        .ok_or_else(|| anyhow!("IDENTIFY_SYSTEM did not return system identifier"))?
        .parse()
        .context("parse system_identifier")
}

async fn fetch_setting(conn: &mut ReplicationConn, name: &str) -> Result<String> {
    let rows = conn.query_rows(&format!("SHOW {name}")).await?;
    rows.into_iter()
        .next()
        .and_then(|cols| cols.into_iter().next().flatten())
        .ok_or_else(|| anyhow!("SHOW {name} returned no rows"))
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

/// Pick the chain-root name to record under `DeltaFullName`.
/// `PrevBackupInfo.increment_full_name` is empty when the parent IS the
/// chain root (no further indirection in V2 sentinel), in which case the
/// root *is* the parent
fn resolve_increment_full_name(p: &PrevBackupInfo) -> String {
    if p.increment_full_name.is_empty() {
        p.name.clone()
    } else {
        p.increment_full_name.clone()
    }
}

/// PG17 wal-summaries → delta map. Returns an error if --pgdata is absent
/// since the summaries live on the server's filesystem
fn build_delta_map_from_summaries(
    pgdata: Option<&std::path::Path>,
    timeline: u32,
    first_used_lsn: u64,
    first_not_used_lsn: u64,
) -> Result<crate::pg::backup::delta::PagedFileDeltaMap> {
    let pgdata = pgdata.ok_or_else(|| anyhow!("--delta-from-wal-summaries requires --pgdata"))?;
    let map = crate::pg::wal_summaries::read_for_range(
        pgdata,
        timeline,
        first_used_lsn,
        first_not_used_lsn,
    )
    .with_context(|| {
        format!(
            "read WAL summaries [{first_used_lsn:X}, {first_not_used_lsn:X}) timeline {timeline}"
        )
    })?;
    Ok(map)
}
