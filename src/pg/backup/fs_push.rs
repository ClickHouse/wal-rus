//! backup-push from a local data directory (wal-g-style filesystem source)
//!
//! Walks `$PGDATA`, packs files into tar parts across N concurrent workers —
//! each worker streams one part through compression to S3 — and brackets the
//! copy with `pg_backup_start` / `pg_backup_stop` over a non-replication SQL
//! session. Output layout matches the BASE_BACKUP path (`tar_partitions/
//! part_NNN.tar.<ext>`, `pg_control.tar.<ext>`, files_metadata.json, sentinel,
//! metadata) so backup-fetch is identical
//!
//! Concurrency is the throughput win over the single-stream BASE_BACKUP path:
//! `WALG_UPLOAD_CONCURRENCY` parts pack + compress + upload simultaneously, so
//! several S3 connections and CPU cores run at once instead of one

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use chrono::Utc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, mpsc};
use tokio_tar::{Builder, EntryType, Header};

use crate::compression::{self, AsyncBufReader, AsyncReader};
use crate::config::Settings;
use crate::pg::backup::delta;
use crate::pg::backup::increment::Format as IncrementFormat;
use crate::pg::backup::push::{self, Finalize, PushArgs};
use crate::pg::backup::tar_streamer::{
    DeltaClass, DeltaContext, IncrementBodyReader, PartWriter, classify_for_delta,
};
use crate::pg::backup::{
    BACKUP_NAME_PREFIX, FileDescription, TablespaceSpec, format_backup_name, format_pg_lsn,
    parse_pg_lsn, tar_part_key,
};
use crate::pg::replication::PgConfig;
use crate::pg::replication::base_backup::ChannelReader;
use crate::pg::replication::conn::ReplicationConn;
use crate::storage::DynStorage;

const PG_CONTROL_ENTRY: &str = "global/pg_control";

/// Coalesce file-body reads. tokio_tar copies each body through io::copy's 8 KB
/// buffer, and every tokio::fs::File read is a blocking-pool dispatch; reading a
/// multi-GB relation in 8 KB units floods the pool and bounds single-stream
/// throughput. A BufReader turns ~CAP/8KB dispatches into one. 256 KB is the knee
/// (matches CHUNK_BYTES); peak resident is CAP × upload_concurrency (one open
/// file per packer)
const FILE_READ_BUF: usize = 256 * 1024;

/// Filenames dropped from the copy, matched by basename anywhere in the tree.
/// Mirrors wal-g's `ExcludedFilenames` plus `pg_internal.init` / `recovery.signal`
/// (which pgbackrest also drops). Directories appear as empty entries (recreated
/// on restore) but aren't recursed; files are dropped entirely. `pg_control` is
/// handled separately (tee'd into `pg_control.tar`)
const EXCLUDED: &[&str] = &[
    "log",
    "pg_log",
    "pg_xlog",
    "pg_wal",
    "pgsql_tmp",
    "postgresql.auto.conf.tmp",
    "postmaster.pid",
    "postmaster.opts",
    "recovery.conf",
    "pg_dynshmem",
    "pg_notify",
    "pg_replslot",
    "pg_serial",
    "pg_stat_tmp",
    "pg_snapshots",
    "pg_subtrans",
    "pg_internal.init",
    "standby.signal",
    "recovery.signal",
];

/// True when `path` looks like a local PG data directory (so backup-push reads
/// the filesystem rather than streaming BASE_BACKUP)
pub fn is_pgdata_dir(path: &Path) -> bool {
    path.join("PG_VERSION").is_file() || path.join("global/pg_control").is_file()
}

#[derive(Clone)]
enum EntryKind {
    Dir,
    File,
}

#[derive(Clone)]
struct WalkEntry {
    kind: EntryKind,
    /// path inside the tar (relative to the data dir; tablespaces remapped
    /// under `pg_tblspc/<oid>/`)
    tar_path: String,
    /// absolute on-disk path (files only)
    abs: PathBuf,
    /// size recorded at stat time; the body is padded/truncated to match
    size: u64,
    mode: u32,
    mtime: i64,
}

/// Walk results not carried in the entry stream: tablespace list, pg_control
/// path, and the entry count for the post-walk log
struct WalkMeta {
    /// (oid, location) for each non-default tablespace
    tablespaces: Vec<(u32, String)>,
    pg_control: Option<PathBuf>,
    entry_count: usize,
}

/// Accumulates walked entries into `tar_size`-bounded batches and blocking-sends
/// each completed batch downstream. Rotation matches the old consumer-side
/// `next_batch`: split before an entry would overflow a non-empty batch, close a
/// batch once it reaches the threshold, let a lone oversize entry stand alone.
/// Runs inside `spawn_blocking`, so `blocking_send` backpressures the walk when
/// the packers fall behind, capping resident entries instead of materializing
/// the whole tree
struct Batcher {
    tar_size: u64,
    tx: mpsc::Sender<Vec<WalkEntry>>,
    cur: Vec<WalkEntry>,
    cur_size: u64,
    count: usize,
}

impl Batcher {
    fn new(tar_size: u64, tx: mpsc::Sender<Vec<WalkEntry>>) -> Self {
        Self {
            tar_size,
            tx,
            cur: Vec::new(),
            cur_size: 0,
            count: 0,
        }
    }

    fn push(&mut self, e: WalkEntry) -> Result<()> {
        if !self.cur.is_empty() && self.cur_size.saturating_add(e.size) > self.tar_size {
            self.flush()?;
        }
        self.cur_size = self.cur_size.saturating_add(e.size);
        self.count += 1;
        self.cur.push(e);
        if self.cur_size >= self.tar_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.cur.is_empty() {
            return Ok(());
        }
        self.cur_size = 0;
        let batch = std::mem::take(&mut self.cur);
        self.tx
            .blocking_send(batch)
            .map_err(|_| anyhow!("pack workers dropped before walk completed"))
    }
}

/// Sink threaded through the recursive walk: batches entries, records
/// tablespaces and the pg_control path
struct WalkSink {
    batcher: Batcher,
    tablespaces: Vec<(u32, String)>,
    pg_control: Option<PathBuf>,
}

#[derive(Default)]
struct WorkerResult {
    files: HashMap<String, FileDescription>,
    tar_file_sets: HashMap<String, Vec<String>>,
    compressed: i64,
    uncompressed: i64,
    max_file_no: u32,
}

pub async fn handle(settings: &Settings, storage: DynStorage, args: PushArgs) -> Result<()> {
    let start_time = Utc::now();
    let pgdata = args
        .pgdata
        .clone()
        .ok_or_else(|| anyhow!("filesystem backup-push requires local PGDATA"))?;

    // Resolve a delta parent unless --full (matches BASE_BACKUP path)
    let parent = if args.full {
        None
    } else {
        delta::configure_delta_parent(&storage, &settings.delta, args.is_permanent).await?
    };
    let increment_format = args.increment_format;
    if let Some(p) = parent.as_ref()
        && let Some(parent_fmt) = p.parent_increment_format
        && parent_fmt != increment_format
    {
        bail!(
            "increment format mismatch: delta parent {} uses {parent_fmt:?} but \
             --increment-format requests {increment_format:?}; a chain must use one \
             format end-to-end (match the parent, or pass --full for a new chain)",
            p.name,
        );
    }

    let cfg = PgConfig::from_env()?;
    tracing::info!(
        target = "backup_push",
        "filesystem backup-push from {} (connecting to {}:{} as {})",
        pgdata.display(),
        cfg.host,
        cfg.port,
        cfg.user,
    );
    let mut conn = ReplicationConn::connect_with(&cfg, false).await?;
    let pg_version = conn.server_pg_version();
    let system_identifier = query_u64(
        &mut conn,
        "SELECT system_identifier FROM pg_control_system()",
    )
    .await
    .context("read system_identifier")?;
    let timeline =
        query_u64(&mut conn, "SELECT timeline_id FROM pg_control_checkpoint()").await? as u32;
    let data_directory = pgdata
        .canonicalize()
        .unwrap_or_else(|_| pgdata.clone())
        .display()
        .to_string();

    if args.delta_from_wal_summaries {
        if pg_version < 170000 {
            bail!(
                "--delta-from-wal-summaries requires PostgreSQL 17 or newer (server reports {pg_version})"
            );
        }
        let on = show_setting(&mut conn, "summarize_wal").await?;
        if on.trim() != "on" {
            bail!("--delta-from-wal-summaries requires summarize_wal=on on the server");
        }
    }

    // pg_backup_start brackets the copy; the session must stay open until stop
    let label = format!("walrus {}", Utc::now().format("%Y%m%dT%H%M%SZ"));
    let start_lsn = backup_start(&mut conn, pg_version, &label, args.fast_checkpoint).await?;
    tracing::info!(
        target = "backup_push",
        "pg_backup_start: lsn={} timeline={}",
        format_pg_lsn(start_lsn),
        timeline,
    );

    let seg_size = crate::pg::wal::segment::wal_segment_size();
    let base_name = format_backup_name(timeline, start_lsn, seg_size);
    let backup_name = match parent.as_ref() {
        Some(p) => format!(
            "{base_name}_D_{}",
            p.name.strip_prefix(BACKUP_NAME_PREFIX).unwrap_or(&p.name),
        ),
        None => base_name.clone(),
    };

    // Build the delta map now that the upper LSN bound is known. Failure drops
    // to a full backup (wal-g semantics: a partial delta is worse than a full)
    let delta_context = build_delta_context(
        settings,
        &storage,
        parent.as_ref(),
        &args,
        increment_format,
        pgdata.as_path(),
        timeline,
        start_lsn,
    )
    .await;

    let tar_size = if args.tar_size_threshold == 0 {
        crate::pg::backup::tar_streamer::DEFAULT_TAR_SIZE_THRESHOLD
    } else {
        args.tar_size_threshold
    };

    let n_workers = settings.upload_concurrency.max(1);
    tracing::info!(
        target = "backup_push",
        "packing with upload_concurrency={}",
        settings.upload_concurrency,
    );

    // Stream the walk into a bounded batch channel instead of materializing every
    // WalkEntry resident first. The metadata-only walk far outruns packing, so an
    // unbounded handoff would hold the whole entry list in memory; channel depth =
    // worker count, so blocking_send backpressures the walk and packing overlaps it
    let (batch_tx, batch_rx) = mpsc::channel::<Vec<WalkEntry>>(n_workers);
    let walk_pgdata = pgdata.clone();
    let walk_task =
        tokio::task::spawn_blocking(move || walk_data_dir(&walk_pgdata, tar_size, batch_tx));

    // Concurrent packing: N workers steal batches off the shared receiver, each
    // streaming one part through compression to S3. A JoinSet ensures that if
    // one worker fails, dropping the set aborts the rest (and each aborted
    // worker aborts its in-flight upload via AbortOnDrop) — nothing keeps
    // touching PGDATA / S3 after this returns and the backup session closes.
    // Dropping every receiver clone also unblocks the walk's blocking_send,
    // ending the producer
    let batch_rx = Arc::new(Mutex::new(batch_rx));
    let counter = Arc::new(AtomicU32::new(0));
    let mut set: tokio::task::JoinSet<Result<WorkerResult>> = tokio::task::JoinSet::new();
    for _ in 0..n_workers {
        let batch_rx = batch_rx.clone();
        let counter = counter.clone();
        let settings = settings.clone();
        let storage = storage.clone();
        let backup_name = backup_name.clone();
        let delta_context = delta_context.clone();
        set.spawn(async move {
            pack_worker(
                batch_rx,
                counter,
                settings,
                storage,
                backup_name,
                delta_context,
            )
            .await
        });
    }

    let mut all_files: HashMap<String, FileDescription> = HashMap::new();
    let mut tar_file_sets: HashMap<String, Vec<String>> = HashMap::new();
    let mut compressed_size: i64 = 0;
    let mut uncompressed_size: i64 = 0;
    let mut max_file_no: u32 = 0;
    while let Some(joined) = set.join_next().await {
        let r = joined.context("pack worker join")??;
        all_files.extend(r.files);
        for (k, v) in r.tar_file_sets {
            tar_file_sets.entry(k).or_default().extend(v);
        }
        compressed_size += r.compressed;
        uncompressed_size += r.uncompressed;
        max_file_no = max_file_no.max(r.max_file_no);
    }

    // Producer closed the channel once the walk finished, so every worker has
    // drained and exited by here; its tablespace list & pg_control path are final
    let walk = walk_task.await.context("walk join")??;
    let pg_control = walk.pg_control;
    let tablespaces = walk.tablespaces;
    tracing::info!(
        target = "backup_push",
        "walked {} entries, {} tablespace(s)",
        walk.entry_count,
        tablespaces.len(),
    );

    // pg_control tee → pg_control.tar (applied last on restore). BASE_BACKUP
    // counts pg_control inline in its archive stream; here it never enters a
    // data part, so add the tee tar bytes to keep uncompressed_size consistent
    let pg_control_tee = match pg_control {
        Some(abs) => Some(build_pg_control_tar(&abs).await?),
        None => None,
    };
    if let Some(tee) = pg_control_tee.as_ref() {
        uncompressed_size += tee.len() as i64;
    }

    // pg_backup_stop: end LSN + non-exclusive backup_label / tablespace_map
    let (end_lsn, labelfile, spcmapfile) = backup_stop(&mut conn, pg_version).await?;
    tracing::info!(
        target = "backup_push",
        "pg_backup_stop at {}",
        format_pg_lsn(end_lsn)
    );

    // backup_label (+ tablespace_map) ship as a final part so restore writes
    // them into the data dir; they don't exist on disk in non-exclusive backup
    let label_file_no = counter.fetch_add(1, Ordering::SeqCst) + 1;
    max_file_no = max_file_no.max(label_file_no);
    let part_name = format!("part_{label_file_no:03}.tar");
    let mut label_entries: Vec<(&str, &str)> = vec![("backup_label", labelfile.as_str())];
    if !spcmapfile.trim().is_empty() {
        label_entries.push(("tablespace_map", spcmapfile.as_str()));
    }
    let label_tar = build_small_tar(&label_entries).await?;
    let key = tar_part_key(
        &backup_name,
        label_file_no,
        settings.compression.extension(),
    );
    uncompressed_size += label_tar.len() as i64;
    compressed_size += upload_bytes(settings, &storage, &key, label_tar).await? as i64;
    let now = Utc::now();
    for (name, _) in &label_entries {
        all_files.insert(
            (*name).to_string(),
            FileDescription {
                is_incremented: false,
                is_skipped: false,
                mtime: now,
                updates_count: 0,
            },
        );
        tar_file_sets
            .entry(part_name.clone())
            .or_default()
            .push((*name).to_string());
    }

    let tablespace_spec = if tablespaces.is_empty() {
        None
    } else {
        let mut spec = TablespaceSpec::new(&data_directory);
        for (oid, location) in &tablespaces {
            spec.add(*oid, location);
        }
        Some(spec)
    };

    push::finalize_backup(Finalize {
        settings,
        storage: &storage,
        backup_name,
        start_lsn,
        end_lsn,
        pg_version,
        system_identifier,
        uncompressed_size,
        compressed_size,
        data_directory,
        tablespace_spec,
        tablespace_count: tablespaces.len(),
        all_files,
        tar_file_sets,
        pg_control_tee,
        parent: parent.as_ref(),
        delta_context: delta_context.as_ref(),
        args: &args,
        start_time,
        part_count: max_file_no,
    })
    .await
}

/// One packing worker: repeatedly steals a pre-batched part off the shared
/// receiver and packs it into a single part streamed to S3, until the producer
/// closes the channel
async fn pack_worker(
    batch_rx: Arc<Mutex<mpsc::Receiver<Vec<WalkEntry>>>>,
    counter: Arc<AtomicU32>,
    settings: Settings,
    storage: DynStorage,
    backup_name: String,
    delta_context: Option<DeltaContext>,
) -> Result<WorkerResult> {
    let mut res = WorkerResult::default();
    loop {
        // recv() only awaits while the producer is mid-walk with nothing
        // buffered; a closed channel (walk done) yields None and ends the worker
        let batch = {
            let mut rx = batch_rx.lock().await;
            rx.recv().await
        };
        let Some(batch) = batch else { break };
        if batch.is_empty() {
            continue;
        }
        let file_no = counter.fetch_add(1, Ordering::SeqCst) + 1;
        res.max_file_no = res.max_file_no.max(file_no);
        let part_name = format!("part_{file_no:03}.tar");
        let key = tar_part_key(&backup_name, file_no, settings.compression.extension());

        // part bytes stream through the channel to a concurrent upload task
        let (byte_tx, byte_rx) = mpsc::channel::<std::io::Result<Bytes>>(4);
        let reader = ChannelReader::new(byte_rx);
        let upload = tokio::spawn(upload_part(reader, key, settings.clone(), storage.clone()));

        let counter_bytes = Arc::new(AtomicU64::new(0));
        let mut builder = Builder::new(PartWriter::new(byte_tx, counter_bytes.clone()));
        // Abort the upload if this worker errors or is cancelled before the part
        // is fully written, so it can't keep reading PGDATA / uploading after
        // backup-push has returned. Declared after `builder` so on drop it aborts
        // before the part channel closes (no finalize of a partial object)
        let upload = AbortOnDrop::new(upload);
        for e in &batch {
            let written = append_entry(&mut builder, e, &delta_context, &mut res).await?;
            if written {
                res.tar_file_sets
                    .entry(part_name.clone())
                    .or_default()
                    .push(e.tar_path.clone());
            }
        }
        builder.finish().await.context("finish part")?;
        let mut writer = builder.into_inner().await.context("into_inner part")?;
        writer.shutdown().await.context("flush part")?;
        // Drop the writer (and its PollSender) to close the channel so the
        // upload's ChannelReader sees EOF; shutdown only flushes, it doesn't
        // close. Without this the upload never completes and the worker hangs
        drop(writer);

        // Count real tar bytes (headers, padding, dir entries), matching the
        // BASE_BACKUP path which counts its whole input archive stream rather
        // than logical file bodies
        res.uncompressed += counter_bytes.load(Ordering::Relaxed) as i64;
        res.compressed += upload.disarm().await.context("upload join")?? as i64;
    }
    Ok(res)
}

/// Append one walked entry to `builder`, recording per-file metadata. Returns
/// whether anything was written to the tar (delta-skipped files write nothing)
async fn append_entry(
    builder: &mut Builder<PartWriter>,
    e: &WalkEntry,
    delta_context: &Option<DeltaContext>,
    res: &mut WorkerResult,
) -> Result<bool> {
    if matches!(e.kind, EntryKind::Dir) {
        let mut h = header(e, EntryType::Directory, 0);
        builder
            .append_data(&mut h, &e.tar_path, tokio::io::empty())
            .await
            .with_context(|| format!("append dir {}", e.tar_path))?;
        return Ok(true);
    }

    match classify_for_delta(delta_context, &e.tar_path, e.size) {
        DeltaClass::Skip => {
            res.files.insert(
                e.tar_path.clone(),
                FileDescription {
                    is_incremented: false,
                    is_skipped: true,
                    mtime: mtime_dt(e.mtime),
                    updates_count: 0,
                },
            );
            Ok(false)
        }
        DeltaClass::Increment {
            header_bytes,
            blocks,
            total_size,
        } => {
            let Some(mut file) = open_walked(&e.abs).await? else {
                return Ok(false);
            };
            let mut h = header(e, EntryType::Regular, total_size);
            let body = IncrementBodyReader::new(header_bytes, &mut file, blocks, e.size);
            builder
                .append_data(&mut h, &e.tar_path, body)
                .await
                .with_context(|| format!("append increment {}", e.tar_path))?;
            res.files.insert(
                e.tar_path.clone(),
                FileDescription {
                    is_incremented: true,
                    is_skipped: false,
                    mtime: mtime_dt(e.mtime),
                    updates_count: 0,
                },
            );
            Ok(true)
        }
        DeltaClass::Passthrough => {
            let Some(file) = open_walked(&e.abs).await? else {
                return Ok(false);
            };
            let body = FixedSizeReader::new(file, e.size);
            let mut h = header(e, EntryType::Regular, e.size);
            builder
                .append_data(&mut h, &e.tar_path, body)
                .await
                .with_context(|| format!("append {}", e.tar_path))?;
            res.files.insert(
                e.tar_path.clone(),
                FileDescription {
                    is_incremented: false,
                    is_skipped: false,
                    mtime: mtime_dt(e.mtime),
                    updates_count: 0,
                },
            );
            Ok(true)
        }
    }
}

/// Open a walked file, tolerating it vanishing between the walk and the pack:
/// DROP TABLE unlinks a relation, pg_internal.init is recreated, etc. Returns
/// None on ENOENT so the caller omits it — matching wal-g, which skips a file
/// removed mid-backup; the unlink is in the WAL and replays on restore
async fn open_walked(abs: &Path) -> Result<Option<BufReader<tokio::fs::File>>> {
    match tokio::fs::File::open(abs).await {
        Ok(f) => Ok(Some(BufReader::with_capacity(FILE_READ_BUF, f))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                target = "backup_push",
                "{} vanished during backup; skipping",
                abs.display(),
            );
            Ok(None)
        }
        Err(e) => Err(e).with_context(|| format!("open {}", abs.display())),
    }
}

fn header(e: &WalkEntry, kind: EntryType, size: u64) -> Header {
    let mut h = Header::new_gnu();
    h.set_size(size);
    h.set_mode(e.mode);
    h.set_mtime(e.mtime.max(0) as u64);
    h.set_entry_type(kind);
    h
}

fn mtime_dt(secs: i64) -> chrono::DateTime<Utc> {
    chrono::DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(|| chrono::DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

/// Owns a spawned task handle and aborts it on drop unless `disarm`ed. Ensures
/// a per-part upload can't outlive its worker (on error or cancellation), which
/// would otherwise keep uploading after backup-push returned
struct AbortOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    /// Take the handle back; the guard no longer aborts (caller awaits it)
    fn disarm(mut self) -> tokio::task::JoinHandle<T> {
        self.0.take().expect("disarm called once")
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

async fn upload_part(
    reader: ChannelReader,
    key: String,
    settings: Settings,
    storage: DynStorage,
) -> Result<u64> {
    let reader: AsyncBufReader = Box::pin(reader);
    let compressed =
        compression::encode_buffered(settings.compression, reader, settings.compression_level);
    let encrypted = settings.encrypt(compressed);
    let counter = Arc::new(AtomicU64::new(0));
    let counting = push::wrap_counted_reader(encrypted, counter.clone());
    let throttled = settings.throttle_network(counting);
    storage
        .put(&key, throttled, None)
        .await
        .with_context(|| format!("put {key}"))?;
    Ok(counter.load(Ordering::Relaxed))
}

/// Compress+encrypt a small in-memory tar and PUT it; returns compressed bytes
async fn upload_bytes(
    settings: &Settings,
    storage: &DynStorage,
    key: &str,
    bytes: Bytes,
) -> Result<u64> {
    let raw: AsyncReader = Box::pin(std::io::Cursor::new(bytes.to_vec()));
    let compressed = compression::encode(settings.compression, raw, settings.compression_level);
    let encrypted = settings.encrypt(compressed);
    let counter = Arc::new(AtomicU64::new(0));
    let counting = push::wrap_counted_reader(encrypted, counter.clone());
    let throttled = settings.throttle_network(counting);
    storage
        .put(key, throttled, None)
        .await
        .with_context(|| format!("put {key}"))?;
    Ok(counter.load(Ordering::Relaxed))
}

async fn build_pg_control_tar(abs: &Path) -> Result<Bytes> {
    let data = tokio::fs::read(abs)
        .await
        .with_context(|| format!("read {}", abs.display()))?;
    let mut b = Builder::new(Vec::new());
    let mut h = Header::new_gnu();
    h.set_size(data.len() as u64);
    h.set_mode(0o600);
    h.set_mtime(0);
    h.set_entry_type(EntryType::Regular);
    b.append_data(&mut h, PG_CONTROL_ENTRY, &data[..])
        .await
        .context("append pg_control tee")?;
    b.finish().await.context("finish pg_control tar")?;
    let buf = b.into_inner().await.context("into_inner pg_control tar")?;
    Ok(Bytes::from(buf))
}

async fn build_small_tar(entries: &[(&str, &str)]) -> Result<Bytes> {
    let mut b = Builder::new(Vec::new());
    for (name, content) in entries {
        let mut h = Header::new_gnu();
        h.set_size(content.len() as u64);
        h.set_mode(0o600);
        h.set_mtime(0);
        h.set_entry_type(EntryType::Regular);
        b.append_data(&mut h, name, content.as_bytes())
            .await
            .with_context(|| format!("append {name}"))?;
    }
    b.finish().await.context("finish tar")?;
    let buf = b.into_inner().await.context("into_inner tar")?;
    Ok(Bytes::from(buf))
}

// ─── filesystem walk ────────────────────────────────────────────────────────

fn walk_data_dir(
    pgdata: &Path,
    tar_size: u64,
    tx: mpsc::Sender<Vec<WalkEntry>>,
) -> Result<WalkMeta> {
    let mut out = WalkSink {
        batcher: Batcher::new(tar_size, tx),
        tablespaces: Vec::new(),
        pg_control: None,
    };
    walk_dir(pgdata, "", &mut out)?;
    out.batcher.flush()?;
    Ok(WalkMeta {
        tablespaces: out.tablespaces,
        pg_control: out.pg_control,
        entry_count: out.batcher.count,
    })
}

fn walk_dir(dir: &Path, rel_prefix: &str, out: &mut WalkSink) -> Result<()> {
    let read = std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in read {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let ft = entry.file_type()?;
        let rel = if rel_prefix.is_empty() {
            name.clone()
        } else {
            format!("{rel_prefix}/{name}")
        };
        let abs = entry.path();

        if ft.is_symlink() {
            // Only pg_tblspc/<oid> symlinks matter: record the tablespace and
            // walk its target remapped under pg_tblspc/<oid>/
            if rel_prefix == "pg_tblspc"
                && let Ok(oid) = name.parse::<u32>()
            {
                let target = std::fs::read_link(&abs)
                    .with_context(|| format!("readlink {}", abs.display()))?;
                out.tablespaces.push((oid, target.display().to_string()));
                walk_dir(&target, &rel, out)?;
            }
            continue;
        }

        let excluded = EXCLUDED.contains(&name.as_str());

        // Resolve file drops before stat: an excluded file (eg pg_internal.init)
        // can vanish between readdir and stat, so stat'ing it would fail the
        // walk for a file we discard anyway. pg_control rides only in
        // pg_control.tar (applied last on restore), never a regular entry
        if ft.is_file() {
            if excluded {
                continue;
            }
            if rel == PG_CONTROL_ENTRY {
                out.pg_control = Some(abs);
                continue;
            }
        }

        let meta = match entry.metadata() {
            Ok(m) => m,
            // vanished between readdir and stat (eg DROP TABLE); the removal is
            // in the WAL and replays on restore, so dropping it stays consistent
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("stat {}", abs.display())),
        };
        let mode = meta.permissions().mode();
        let mtime = mtime_secs(&meta);

        if ft.is_dir() {
            // Emit the dir entry even when excluded so it exists on restore,
            // but don't recurse into excluded dirs
            out.batcher.push(WalkEntry {
                kind: EntryKind::Dir,
                tar_path: rel.clone(),
                abs: abs.clone(),
                size: 0,
                mode,
                mtime,
            })?;
            if !excluded {
                walk_dir(&abs, &rel, out)?;
            }
        } else if ft.is_file() {
            out.batcher.push(WalkEntry {
                kind: EntryKind::File,
                tar_path: rel,
                abs,
                size: meta.len(),
                mode,
                mtime,
            })?;
        }
    }
    Ok(())
}

fn mtime_secs(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ─── pg_backup_start / pg_backup_stop ───────────────────────────────────────

async fn backup_start(
    conn: &mut ReplicationConn,
    pg_version: i32,
    label: &str,
    fast: bool,
) -> Result<u64> {
    // Non-exclusive backup (session-scoped). PG15+ renamed the functions
    let sql = if pg_version >= 150000 {
        format!("SELECT pg_backup_start('{}', {fast})", sql_lit(label))
    } else {
        format!(
            "SELECT pg_start_backup('{}', {fast}, false)",
            sql_lit(label)
        )
    };
    let rows = conn.query_rows(&sql).await.context("pg_backup_start")?;
    let lsn = first_col(&rows).ok_or_else(|| anyhow!("pg_backup_start returned no LSN"))?;
    parse_pg_lsn(&lsn).context("parse start LSN")
}

/// Returns (end_lsn, backup_label, tablespace_map)
async fn backup_stop(conn: &mut ReplicationConn, pg_version: i32) -> Result<(u64, String, String)> {
    // wait_for_archive=false: walrus ships WAL separately, and waiting can hang
    // when no archiver is running
    let sql = if pg_version >= 150000 {
        "SELECT lsn::text, labelfile, spcmapfile FROM pg_backup_stop(false)"
    } else {
        "SELECT lsn::text, labelfile, spcmapfile FROM pg_stop_backup(false, false)"
    };
    let rows = conn.query_rows(sql).await.context("pg_backup_stop")?;
    let row = rows
        .first()
        .ok_or_else(|| anyhow!("pg_backup_stop returned no row"))?;
    let lsn = row
        .first()
        .and_then(|c| c.clone())
        .ok_or_else(|| anyhow!("pg_backup_stop returned no LSN"))?;
    let labelfile = row.get(1).and_then(|c| c.clone()).unwrap_or_default();
    let spcmapfile = row.get(2).and_then(|c| c.clone()).unwrap_or_default();
    Ok((
        parse_pg_lsn(&lsn).context("parse end LSN")?,
        labelfile,
        spcmapfile,
    ))
}

async fn query_u64(conn: &mut ReplicationConn, sql: &str) -> Result<u64> {
    let rows = conn.query_rows(sql).await?;
    first_col(&rows)
        .ok_or_else(|| anyhow!("`{sql}` returned no value"))?
        .trim()
        .parse()
        .with_context(|| format!("parse u64 from `{sql}`"))
}

async fn show_setting(conn: &mut ReplicationConn, name: &str) -> Result<String> {
    let rows = conn.query_rows(&format!("SHOW {name}")).await?;
    first_col(&rows).ok_or_else(|| anyhow!("SHOW {name} returned no rows"))
}

fn first_col(rows: &[Vec<Option<String>>]) -> Option<String> {
    rows.first().and_then(|r| r.first()).and_then(|c| c.clone())
}

fn sql_lit(s: &str) -> String {
    s.replace('\'', "''")
}

#[allow(clippy::too_many_arguments)]
async fn build_delta_context(
    settings: &Settings,
    storage: &DynStorage,
    parent: Option<&delta::PrevBackupInfo>,
    args: &PushArgs,
    increment_format: IncrementFormat,
    pgdata: &Path,
    timeline: u32,
    start_lsn: u64,
) -> Option<DeltaContext> {
    let p = parent?;
    if start_lsn <= p.start_lsn {
        tracing::warn!(
            target = "backup_push",
            "new start LSN <= parent; producing a full backup",
        );
        return None;
    }
    let map = if args.delta_from_wal_summaries {
        push::build_delta_map_from_summaries(Some(pgdata), timeline, p.start_lsn, start_lsn)
    } else {
        delta::build_delta_map_from_wal(
            settings,
            storage,
            p.timeline,
            p.start_lsn,
            start_lsn,
            settings.compression,
        )
        .await
    };
    match map {
        Ok(map) => {
            tracing::info!(
                target = "backup_push",
                "delta map: {} dirty page(s)",
                map.len(),
            );
            Some(DeltaContext {
                map: Arc::new(map),
                format: increment_format,
                parent_files: p.parent_files.clone(),
            })
        }
        Err(e) => {
            tracing::warn!(
                target = "backup_push",
                "delta map build failed ({e:#}); producing a full backup",
            );
            None
        }
    }
}

// ─── fixed-size body reader ─────────────────────────────────────────────────

/// Emits exactly `remaining` bytes from `inner`: truncates if the file grew,
/// zero-pads if it shrank, since a file can change between stat and read under
/// pg_backup_start. Keeps the tar body length matching the header size
struct FixedSizeReader<R> {
    inner: R,
    remaining: u64,
    inner_eof: bool,
}

impl<R> FixedSizeReader<R> {
    fn new(inner: R, size: u64) -> Self {
        Self {
            inner,
            remaining: size,
            inner_eof: false,
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for FixedSizeReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        let me = self.get_mut();
        if me.remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        let want = (buf.remaining() as u64).min(me.remaining) as usize;
        if want == 0 {
            return Poll::Ready(Ok(()));
        }
        if me.inner_eof {
            // initialize_unfilled_to zeroes the region; emit padding
            buf.initialize_unfilled_to(want);
            buf.advance(want);
            me.remaining -= want as u64;
            return Poll::Ready(Ok(()));
        }
        let n;
        {
            let dst = buf.initialize_unfilled_to(want);
            let mut tmp = tokio::io::ReadBuf::new(dst);
            match std::pin::Pin::new(&mut me.inner).poll_read(cx, &mut tmp) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => n = tmp.filled().len(),
            }
        }
        if n == 0 {
            // file shorter than recorded size: pad the rest with zeros
            me.inner_eof = true;
            buf.initialize_unfilled_to(want);
            buf.advance(want);
            me.remaining -= want as u64;
        } else {
            buf.advance(n);
            me.remaining -= n as u64;
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;
    use crate::compression::Method;
    use crate::storage::fs::FsStorage;
    use tokio::io::AsyncReadExt;

    fn write_file(root: &Path, rel: &str, content: &[u8]) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }

    /// Run the streaming walk to completion and flatten every batch back into one
    /// entry list, for tests that inspect the walk's output rather than pack it
    async fn walk_collect(root: &Path, tar_size: u64) -> (Vec<WalkEntry>, WalkMeta) {
        let (tx, mut rx) = mpsc::channel::<Vec<WalkEntry>>(1024);
        let root = root.to_path_buf();
        let handle = tokio::task::spawn_blocking(move || walk_data_dir(&root, tar_size, tx));
        let mut entries = Vec::new();
        while let Some(batch) = rx.recv().await {
            entries.extend(batch);
        }
        let meta = handle.await.unwrap().unwrap();
        (entries, meta)
    }

    /// Walk into a shared receiver for driving `pack_worker`. Buffers every batch
    /// (test inputs are tiny), then drops the sender so the worker sees EOF
    async fn walk_batches(
        root: &Path,
        tar_size: u64,
    ) -> Arc<Mutex<mpsc::Receiver<Vec<WalkEntry>>>> {
        let (tx, rx) = mpsc::channel::<Vec<WalkEntry>>(1024);
        let root = root.to_path_buf();
        tokio::task::spawn_blocking(move || walk_data_dir(&root, tar_size, tx))
            .await
            .unwrap()
            .unwrap();
        Arc::new(Mutex::new(rx))
    }

    #[test]
    fn is_pgdata_dir_detects_marker() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_pgdata_dir(dir.path()));
        std::fs::write(dir.path().join("PG_VERSION"), b"16").unwrap();
        assert!(is_pgdata_dir(dir.path()));
    }

    #[tokio::test]
    async fn walk_excludes_dirs_files_and_tees_pg_control() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_file(root, "PG_VERSION", b"16");
        write_file(root, "base/1/1234", b"relation");
        write_file(root, "global/pg_control", b"control");
        write_file(root, "global/pg_internal.init", b"relcache");
        write_file(root, "base/1/pg_internal.init", b"relcache");
        write_file(root, "pg_wal/000000010000000000000001", b"walseg");
        write_file(root, "postmaster.pid", b"123");
        write_file(root, "standby.signal", b"");
        write_file(root, "recovery.signal", b"");

        let (entries, meta) = walk_collect(root, u64::MAX).await;
        let paths: std::collections::HashSet<&str> =
            entries.iter().map(|e| e.tar_path.as_str()).collect();

        assert!(paths.contains("PG_VERSION"));
        assert!(paths.contains("base/1/1234"));
        // excluded dir present as an (empty) entry, its contents are not
        assert!(paths.contains("pg_wal"));
        assert!(!paths.iter().any(|p| p.starts_with("pg_wal/")));
        // excluded file dropped entirely
        assert!(!paths.contains("postmaster.pid"));
        // pg_internal.init churns under relcache invalidation; dropped in every
        // directory (global + per-database) so a stat→open can't race a vanish
        assert!(!paths.iter().any(|p| p.ends_with("pg_internal.init")));
        // signal files dropped so a restore controls its own recovery state
        assert!(!paths.contains("standby.signal"));
        assert!(!paths.contains("recovery.signal"));
        // pg_control rides only in the tee, never a regular entry
        assert!(!paths.contains("global/pg_control"));
        assert_eq!(meta.pg_control, Some(root.join("global/pg_control")));

        let pg_wal = entries.iter().find(|e| e.tar_path == "pg_wal").unwrap();
        assert!(matches!(pg_wal.kind, EntryKind::Dir));
    }

    /// pg_tblspc/<oid> symlinks: record (oid, on-disk target) and remap the
    /// target's contents under pg_tblspc/<oid>/ in the tar
    #[tokio::test]
    async fn walk_remaps_tablespace_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("pgdata");
        write_file(&root, "PG_VERSION", b"16");
        write_file(&root, "global/pg_control", b"control");

        // external tablespace location holding a relation file
        let ts = dir.path().join("tblspc_a");
        write_file(&ts, "PG_16_202307071/16400/12345", &[9u8; 100]);
        std::fs::create_dir_all(root.join("pg_tblspc")).unwrap();
        std::os::unix::fs::symlink(&ts, root.join("pg_tblspc/16384")).unwrap();

        let (entries, meta) = walk_collect(&root, u64::MAX).await;
        let paths: std::collections::HashSet<&str> =
            entries.iter().map(|e| e.tar_path.as_str()).collect();

        // tablespace recorded by oid → on-disk target
        assert_eq!(meta.tablespaces, vec![(16384u32, ts.display().to_string())]);
        // pg_tblspc dir emitted; target contents remapped beneath the oid
        assert!(paths.contains("pg_tblspc"));
        assert!(paths.contains("pg_tblspc/16384/PG_16_202307071/16400/12345"));
        // symlinked relation file points back at its real on-disk location
        let rel = entries
            .iter()
            .find(|e| e.tar_path == "pg_tblspc/16384/PG_16_202307071/16400/12345")
            .unwrap();
        assert!(matches!(rel.kind, EntryKind::File));
        assert_eq!(rel.size, 100);
        assert_eq!(rel.abs, ts.join("PG_16_202307071/16400/12345"));
    }

    fn file_entry(path: &str, size: u64) -> WalkEntry {
        WalkEntry {
            kind: EntryKind::File,
            tar_path: path.into(),
            abs: PathBuf::new(),
            size,
            mode: 0o644,
            mtime: 0,
        }
    }

    #[tokio::test]
    async fn batcher_rotation() {
        // threshold 100: [40, 40] fits one part; next 40 alone; oversize 500 alone
        let (tx, mut rx) = mpsc::channel::<Vec<WalkEntry>>(64);
        // blocking_send must run off the runtime; flush on drop is via explicit flush
        tokio::task::spawn_blocking(move || {
            let mut b = Batcher::new(100, tx);
            for e in [
                file_entry("a", 40),
                file_entry("b", 40),
                file_entry("c", 40),
                file_entry("big", 500),
                file_entry("d", 10),
            ] {
                b.push(e).unwrap();
            }
            b.flush().unwrap();
        })
        .await
        .unwrap();

        let mut batches: Vec<Vec<String>> = Vec::new();
        while let Some(batch) = rx.recv().await {
            batches.push(batch.iter().map(|e| e.tar_path.clone()).collect());
        }
        let got: Vec<Vec<&str>> = batches
            .iter()
            .map(|b| b.iter().map(String::as_str).collect())
            .collect();
        assert_eq!(got, vec![vec!["a", "b"], vec!["c"], vec!["big"], vec!["d"]]);
    }

    #[tokio::test]
    async fn fixed_size_reader_truncates_and_pads() {
        // truncate: 6 bytes available, want 4
        let mut r = FixedSizeReader::new(std::io::Cursor::new(b"abcdef".to_vec()), 4);
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"abcd");

        // pad: 3 bytes available, want 6 → zero-filled tail
        let mut r = FixedSizeReader::new(std::io::Cursor::new(b"abc".to_vec()), 6);
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"abc\0\0\0");
    }

    /// walk → concurrent pack → read parts back: every file & dir survives
    /// byte-clean through the async packer (uncompressed for a simple check)
    #[tokio::test]
    async fn pack_roundtrip_to_storage() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("pgdata");
        write_file(&root, "PG_VERSION", b"16");
        write_file(&root, "base/1/1234", &vec![7u8; 5000]);
        write_file(&root, "base/1/5678", b"small");
        write_file(&root, "global/123", &vec![3u8; 9000]);
        write_file(&root, "pg_wal/seg", b"excluded");

        // expected file bodies (pg_wal/seg is excluded by the walk)
        let expect: std::collections::HashMap<String, Vec<u8>> = [
            ("PG_VERSION".to_string(), b"16".to_vec()),
            ("base/1/1234".to_string(), vec![7u8; 5000]),
            ("base/1/5678".to_string(), b"small".to_vec()),
            ("global/123".to_string(), vec![3u8; 9000]),
        ]
        .into_iter()
        .collect();

        let store_dir = tempfile::tempdir().unwrap();
        let storage: DynStorage = Arc::new(FsStorage::new(store_dir.path()).unwrap());
        let settings = Settings {
            compression: Method::None,
            ..Default::default()
        };

        let batch_rx = walk_batches(&root, 4096).await;
        let counter = Arc::new(AtomicU32::new(0));
        let name = "base_test";
        let res = pack_worker(
            batch_rx,
            counter,
            settings,
            storage.clone(),
            name.to_string(),
            None,
        )
        .await
        .unwrap();
        assert!(res.max_file_no >= 1);

        // read every emitted part & collect file bodies
        let mut got: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
        let mut part_bytes_total: u64 = 0;
        for file_no in 1..=res.max_file_no {
            let key = tar_part_key(name, file_no, "");
            let mut body = storage.get(&key).await.unwrap();
            let mut bytes = Vec::new();
            body.read_to_end(&mut bytes).await.unwrap();
            part_bytes_total += bytes.len() as u64;
            let mut ar = tar::Archive::new(&bytes[..]);
            for e in ar.entries().unwrap() {
                let mut e = e.unwrap();
                let p = e.path().unwrap().to_string_lossy().into_owned();
                if e.header().entry_type().is_dir() {
                    continue;
                }
                let mut c = Vec::new();
                e.read_to_end(&mut c).unwrap();
                got.insert(p, c);
            }
        }

        assert_eq!(got.len(), expect.len(), "file count mismatch: {got:?}");
        for (path, content) in &expect {
            assert_eq!(got.get(path), Some(content), "mismatch for {path}");
        }
        // excluded file never made it into a part
        assert!(!got.contains_key("pg_wal/seg"));
        // uncompressed_size counts real tar bytes (headers, padding, dir
        // entries), not just logical file bodies: with Method::None the stored
        // part bytes equal the tar bytes the PartWriter counted
        assert_eq!(
            res.uncompressed as u64, part_bytes_total,
            "uncompressed must equal actual tar part bytes"
        );
    }

    #[tokio::test]
    async fn open_walked_tolerates_missing() {
        let dir = tempfile::tempdir().unwrap();
        let present = dir.path().join("here");
        std::fs::write(&present, b"x").unwrap();
        assert!(open_walked(&present).await.unwrap().is_some());
        assert!(
            open_walked(&dir.path().join("gone"))
                .await
                .unwrap()
                .is_none()
        );
    }

    /// A relation unlinked between walk and pack (DROP TABLE) is dropped from the
    /// backup without failing the part, matching wal-g
    #[tokio::test]
    async fn pack_skips_file_removed_after_walk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("pgdata");
        write_file(&root, "PG_VERSION", b"16");
        write_file(&root, "base/1/1234", b"relation");
        write_file(&root, "base/1/5678", b"dropme");

        // walk records the file, then simulate DROP TABLE before the pack opens it
        let batch_rx = walk_batches(&root, 4096).await;
        std::fs::remove_file(root.join("base/1/5678")).unwrap();

        let store_dir = tempfile::tempdir().unwrap();
        let storage: DynStorage = Arc::new(FsStorage::new(store_dir.path()).unwrap());
        let settings = Settings {
            compression: Method::None,
            ..Default::default()
        };
        let res = pack_worker(
            batch_rx,
            Arc::new(AtomicU32::new(0)),
            settings,
            storage,
            "base_drop".to_string(),
            None,
        )
        .await
        .unwrap();

        assert!(res.files.contains_key("base/1/1234"));
        assert!(!res.files.contains_key("base/1/5678"));
    }
}
