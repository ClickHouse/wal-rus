//! Tarball re-streamer with prefix remap, optional tee, and part rotation
//!
//! Port of wal-g's `TarballStreamer` (Phase B.7/B.8/B.9). Reads tar entries
//! from a single tar input, rewrites each entry's path (so a tablespace tar
//! can land under `pg_tblspc/<oid>/...`), collects per-file metadata for
//! `files_metadata.json`, optionally tees selected entries to a separate
//! in-memory tar (used for `pg_control.tar.<ext>`), and yields a stream of
//! output tar parts. Each output part stays under `max_tar_size` bytes; the
//! one exception is a single entry larger than the threshold which spills
//! into its own part (wal-g matches this behavior; mirrors a real PG tar
//! that occasionally carries multi-GB segment files)
//!
//! The streamer runs as a `tokio::spawn` task over `astral-tokio-tar`'s async
//! `Archive` / `Builder`; per-part output flows over an mpsc of `Bytes` that
//! the caller reads as an `AsyncRead` (see `ChannelReader`)

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tar::{Archive, Builder, Header};
use tokio_util::sync::PollSender;

use crate::pg::backup::delta::{self as delta_mod, PG_PAGE_SIZE, PagedFileDeltaMap};
use crate::pg::backup::increment::{
    self, Format as IncrementFormat, write_increment_header, write_native_increment_header,
};
use crate::pg::replication::base_backup::ChannelReader;

/// wal-g default: `WALG_TAR_SIZE_THRESHOLD` = 1 GiB
pub const DEFAULT_TAR_SIZE_THRESHOLD: u64 = 1 << 30;

/// Channel chunk size between writer thread and async consumer.
/// Small enough to keep memory bounded, large enough to amortize wake cost
const CHUNK_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub struct StreamerOpts {
    /// Prefix prepended to each entry's name (None = pass through)
    pub prefix: Option<String>,
    /// Entry names which should additionally be written to a tee tar in memory.
    /// Compared against the post-remap path
    pub tee_names: Vec<String>,
    /// Approximate maximum part size before rotation
    pub max_tar_size: u64,
    /// Numbering continues from here (1-based; first part = starting_file_no + 1)
    pub starting_file_no: u32,
    /// Buffer depth for the parts channel between this streamer and its
    /// consumer. Equivalent to wal-g's `WALG_UPLOAD_QUEUE` once the
    /// consumer spawns concurrent upload workers
    pub queue_depth: usize,
    /// Delta mode. When set, paged files with a non-empty changed-block list
    /// get rewritten as wi1 or PG17-native increments; paged files with an
    /// empty list (or absent from the map) get skipped entirely
    pub delta_context: Option<DeltaContext>,
}

/// Parent-backup delta state, shared across tablespace streamers
#[derive(Clone, Debug)]
pub struct DeltaContext {
    pub map: Arc<PagedFileDeltaMap>,
    pub format: IncrementFormat,
    /// Paths present in the increment-base backup. Files absent here are new
    /// since the parent and must ship in full, not as increments
    pub parent_files: Arc<HashSet<String>>,
    /// Parent backup's start LSN, for the page-LSN final-state filter. `Some`
    /// only on the filesystem push path, which has random page access to read
    /// each candidate block's on-disk page header; the WAL/summary candidate
    /// set is trimmed to blocks whose page changed at/after this LSN (wal-g's
    /// selectivity). `None` on the BASE_BACKUP stream path (no random access),
    /// leaving the candidate set unfiltered
    pub parent_start_lsn: Option<NonZeroU64>,
}

impl Default for StreamerOpts {
    fn default() -> Self {
        Self {
            prefix: None,
            tee_names: Vec::new(),
            max_tar_size: DEFAULT_TAR_SIZE_THRESHOLD,
            starting_file_no: 0,
            queue_depth: 1,
            delta_context: None,
        }
    }
}

/// One output part. The consumer wraps `reader` in compression and hands
/// it to `Storage::put`
pub struct Part {
    pub file_no: u32,
    pub reader: ChannelReader,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct FileMeta {
    #[serde(rename = "IsIncremented", default)]
    pub is_incremented: bool,
    #[serde(rename = "IsSkipped", default)]
    pub is_skipped: bool,
    #[serde(rename = "MTime")]
    pub mtime: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct StreamerResult {
    pub files: HashMap<String, FileMeta>,
    /// Map of part filename (eg `part_001.tar`) to the entry names it contains
    pub tar_file_sets: HashMap<String, Vec<String>>,
    pub last_file_no: u32,
    /// Concatenated tee tar bytes (terminated with two zero blocks)
    pub tee_bytes: Option<Bytes>,
}

/// Start a streamer task. Returns the parts receiver and a handle to the
/// `spawn_blocking` task. The task completes after all input entries are
/// consumed and the last part's channel is closed
pub fn start<R>(
    input: R,
    opts: StreamerOpts,
) -> (
    mpsc::Receiver<Result<Part>>,
    JoinHandle<Result<StreamerResult>>,
)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let (parts_tx, parts_rx) = mpsc::channel::<Result<Part>>(opts.queue_depth.max(1));
    let handle = tokio::spawn(run_async(input, opts, parts_tx));
    (parts_rx, handle)
}

async fn run_async<R: AsyncRead + Send + Unpin + 'static>(
    input: R,
    opts: StreamerOpts,
    parts_tx: mpsc::Sender<Result<Part>>,
) -> Result<StreamerResult> {
    let mut archive = Archive::new(input);
    let mut entries = archive.entries().context("open tar entries")?;

    let mut result = StreamerResult::default();
    let mut file_no = opts.starting_file_no;
    let mut tee_builder: Option<Builder<Vec<u8>>> = if opts.tee_names.is_empty() {
        None
    } else {
        Some(Builder::new(Vec::new()))
    };

    let mut current: Option<PartCtx> = None;

    while let Some(entry) = entries.next().await {
        let mut entry = entry.context("read tar entry")?;
        let header = entry.header().clone();
        let orig_path = entry
            .path()
            .context("entry path")?
            .to_string_lossy()
            .into_owned();
        let mapped = match &opts.prefix {
            Some(p) => format!("{p}{}", orig_path),
            None => orig_path.clone(),
        };
        let entry_size = header.size().unwrap_or(0);
        let is_dir = header.entry_type().is_dir();
        let canonical_name = strip_dotslash(&mapped).to_string();

        // Delta-mode classification: paged files with a tracked delta map
        // either get rewritten as increments or skipped entirely. Other
        // entries (non-paged or no delta map) go through unchanged
        let delta_class = classify_for_delta(&opts.delta_context, &canonical_name, entry_size);

        if matches!(delta_class, DeltaClass::Skip) {
            // Skip entry: record as IsSkipped, don't write to tar at all.
            // `entries.next()` will advance past the unread bytes.
            result.files.insert(
                canonical_name.clone(),
                FileMeta {
                    is_incremented: false,
                    is_skipped: true,
                    mtime: header_mtime(&header),
                },
            );
            continue;
        }

        // Compute the body size that will land in the output tar. For
        // increments this is the on-disk wi1/native size, not the input
        // entry size — used both for rotation budgeting and the new header
        let out_body_size = match &delta_class {
            DeltaClass::Increment { total_size, .. } => *total_size,
            _ => entry_size,
        };

        // Rotate before writing if it would push us past the threshold,
        // mirrors wal-g's pre-emptive split (avoids straddled entries)
        if let Some(ctx) = current.as_ref()
            && ctx.bytes_written() > 0
            && ctx.bytes_written().saturating_add(out_body_size) > opts.max_tar_size
        {
            finalize_part(current.take().unwrap()).await?;
        }
        if current.is_none() {
            file_no += 1;
            current = Some(start_part(file_no, &parts_tx).await?);
        }
        let ctx = current.as_mut().unwrap();

        // append_data handles path encoding (auto-emits GNU LongLink for
        // > 100 char paths) and cksum, so no set_path here
        let mut new_hdr = header.clone();

        // Decide whether to tee this entry
        let tee_match =
            !is_dir && tee_builder.is_some() && opts.tee_names.iter().any(|n| n == &mapped);

        let (is_incremented, is_skipped) = match delta_class {
            DeltaClass::Increment {
                header_bytes,
                blocks,
                total_size,
            } => {
                // Increment path: pre-encoded header, then stream filtered
                // pages from the input entry through IncrementBodyReader.
                // Tee + increment is impossible (paged files never tee), so
                // no need to mirror to tee_builder
                new_hdr.set_size(total_size);
                new_hdr.set_cksum();
                let body = IncrementBodyReader::new(header_bytes, &mut entry, blocks, entry_size);
                ctx.builder
                    .append_data(&mut new_hdr, &mapped, body)
                    .await
                    .context("append increment to current part")?;
                (true, false)
            }
            DeltaClass::Passthrough | DeltaClass::Skip => {
                if tee_match {
                    // Tee path: buffer in memory (only used for small files like pg_control)
                    let mut buf = Vec::with_capacity(entry_size as usize);
                    entry
                        .read_to_end(&mut buf)
                        .await
                        .context("read tee entry")?;
                    ctx.builder
                        .append_data(&mut new_hdr, &mapped, &buf[..])
                        .await
                        .context("append to current part")?;
                    if let Some(tb) = tee_builder.as_mut() {
                        let mut tee_hdr = header.clone();
                        tb.append_data(&mut tee_hdr, &mapped, &buf[..])
                            .await
                            .context("append to tee tar")?;
                    }
                } else {
                    ctx.builder
                        .append_data(&mut new_hdr, &mapped, &mut entry)
                        .await
                        .context("append to current part")?;
                }
                (false, false)
            }
        };

        if !is_dir {
            result.files.insert(
                canonical_name.clone(),
                FileMeta {
                    is_incremented,
                    is_skipped,
                    mtime: header_mtime(&header),
                },
            );
        }
        let part_name = format!("part_{:03}.tar", ctx.file_no);
        result
            .tar_file_sets
            .entry(part_name)
            .or_default()
            .push(canonical_name);
    }

    if let Some(ctx) = current.take() {
        finalize_part(ctx).await?;
    }
    if let Some(mut tb) = tee_builder.take() {
        tb.finish().await.context("finish tee tar")?;
        let buf = tb.into_inner().await.context("into_inner tee tar")?;
        if !buf.is_empty() {
            result.tee_bytes = Some(Bytes::from(buf));
        }
    }

    result.last_file_no = file_no;
    Ok(result)
}

/// Outcome of the delta-mode lookup for one entry
pub(crate) enum DeltaClass {
    /// Not a paged file (or no delta map): pass body through unchanged
    Passthrough,
    /// Paged file whose changed-block set intersects the file: emit increment
    Increment {
        header_bytes: Vec<u8>,
        blocks: Vec<u32>,
        total_size: u64,
    },
    /// Paged file unchanged since parent: omit from tar (`IsSkipped: true`)
    Skip,
}

pub(crate) fn classify_for_delta(
    ctx: &Option<DeltaContext>,
    path: &str,
    entry_size: u64,
) -> DeltaClass {
    let Some(ctx) = ctx.as_ref() else {
        return DeltaClass::Passthrough;
    };
    if !delta_mod::is_paged_path(path) {
        return DeltaClass::Passthrough;
    }
    // wal-g only increments files that were in the increment-base backup
    // (`wasInBase`). A relation (or rel-file segment) created after the parent
    // has no base file to apply onto, so it ships in full — otherwise restore
    // fails with "incremented file should always exist"
    if !ctx.parent_files.contains(path) {
        return DeltaClass::Passthrough;
    }
    // Files smaller than one block can't be page-incrementally encoded;
    // wal-g passes those through as full content
    if entry_size < PG_PAGE_SIZE {
        return DeltaClass::Passthrough;
    }
    let lookup = match ctx.map.blocks_for(path) {
        Ok(v) => v,
        Err(_) => return DeltaClass::Passthrough,
    };
    // Filter to blocks that actually exist in the current file. Blocks past
    // entry_size/BLCKSZ would underflow the wi1/native reader on apply
    let file_blocks = (entry_size / PG_PAGE_SIZE) as u32;
    let blocks_vec: Vec<u32> = match lookup {
        Some(s) => s.into_iter().take_while(|b| *b < file_blocks).collect(),
        None => return DeltaClass::Skip,
    };
    increment_class_for_blocks(ctx.format, entry_size, blocks_vec)
}

/// Encode the increment class for a final block set (already filtered to the
/// file's range, ascending). Empty → `Skip`; a header-encoding failure degrades
/// to `Passthrough` (ship full), matching `classify_for_delta`. Split out so the
/// fs push path can rebuild the class after the page-LSN filter trims blocks
pub(crate) fn increment_class_for_blocks(
    format: IncrementFormat,
    entry_size: u64,
    blocks: Vec<u32>,
) -> DeltaClass {
    if blocks.is_empty() {
        return DeltaClass::Skip;
    }
    let mut header_bytes = Vec::new();
    match format {
        IncrementFormat::Wi1 => {
            if write_increment_header(&mut header_bytes, entry_size, &blocks).is_err() {
                return DeltaClass::Passthrough;
            }
        }
        IncrementFormat::Native => {
            let trunc = (entry_size / PG_PAGE_SIZE) as u32;
            if write_native_increment_header(&mut header_bytes, trunc, &blocks).is_err() {
                return DeltaClass::Passthrough;
            }
        }
    }
    let total_size = header_bytes.len() as u64 + (blocks.len() as u64) * PG_PAGE_SIZE;
    DeltaClass::Increment {
        header_bytes,
        blocks,
        total_size,
    }
}

// ─── PG page header (page-LSN final-state filter) ───────────────────────────

/// Bytes of the postgres page header consulted by the page-LSN filter. `pd_lsn`
/// occupies the first 8 (`xlogid` high u32, `xrecoff` low u32, native-endian);
/// validity checks reach through `pd_pagesize_version` at offset 18
pub(crate) const PG_PAGE_HEADER_SIZE: usize = 24;

/// wal-g `postgres_page_header.go` constants
const PAGE_VALID_FLAGS: u16 = 7;
const PAGE_LAYOUT_VERSION: u16 = 5;

fn page_lsn(h: &[u8]) -> u64 {
    let hi = u32::from_le_bytes(h[0..4].try_into().unwrap()) as u64;
    let lo = u32::from_le_bytes(h[4..8].try_into().unwrap()) as u64;
    (hi << 32) | lo
}

/// `PageIsNew`: `pd_upper == 0` (offset 14). A vacuumed/never-initialised page
fn page_is_new(h: &[u8]) -> bool {
    u16::from_le_bytes(h[14..16].try_into().unwrap()) == 0
}

/// Mirrors wal-g `PageHeader.isValid`: flag/offset sanity plus a non-zero LSN
/// and a `BLCKSZ`-matching size/version. A page failing this is torn or not a
/// standard heap page, so its LSN can't be trusted for the filter
fn page_is_valid(h: &[u8]) -> bool {
    let pd_flags = u16::from_le_bytes(h[10..12].try_into().unwrap());
    let pd_lower = u16::from_le_bytes(h[12..14].try_into().unwrap());
    let pd_upper = u16::from_le_bytes(h[14..16].try_into().unwrap());
    let pd_special = u16::from_le_bytes(h[16..18].try_into().unwrap());
    let pd_pagesize_version = u16::from_le_bytes(h[18..20].try_into().unwrap());
    (pd_flags & PAGE_VALID_FLAGS) == pd_flags
        && pd_lower >= PG_PAGE_HEADER_SIZE as u16
        && pd_lower <= pd_upper
        && pd_upper <= pd_special
        && pd_special as u64 <= PG_PAGE_SIZE
        && page_lsn(h) != 0
        && (pd_pagesize_version & 0xFF00) as u64 == PG_PAGE_SIZE
        && (pd_pagesize_version & 0x00FF) <= PAGE_LAYOUT_VERSION
}

/// Should a candidate block stay in the increment, given its on-disk page header
/// and the parent backup's start LSN? Mirrors wal-g `SelectNewValidPage`: keep a
/// new/empty page, keep an unparseable/torn page, and keep any page whose LSN is
/// at/after the parent (changed since). Drop only a valid, non-new page settled
/// strictly below the parent — that block is byte-identical to the parent's copy,
/// so the WAL-derived candidate set over-counted it. Never drops a block that
/// might have changed, so the increment stays correct
pub(crate) fn page_changed_since(header: &[u8], parent_start_lsn: u64) -> bool {
    if header.len() < PG_PAGE_HEADER_SIZE {
        return true;
    }
    if page_is_new(header) || !page_is_valid(header) {
        return true;
    }
    page_lsn(header) >= parent_start_lsn
}

/// `AsyncRead` impl that emits a pre-encoded increment header followed by the
/// subset of input pages whose block numbers appear in `blocks`. Reads the
/// input strictly forward — pages before each target are read & discarded
enum IncrementPhase {
    Header,
    /// load the next target page (skipping intervening pages first)
    Load,
    /// emit the page currently buffered in `page_buf`
    Emit,
    Done,
}

pub(crate) struct IncrementBodyReader<'a, R> {
    header: Vec<u8>,
    header_pos: usize,
    input: &'a mut R,
    blocks: Vec<u32>,
    next_idx: usize,
    /// next block index still to be read off the input
    cur_block: u32,
    page_buf: [u8; PG_PAGE_SIZE as usize],
    /// bytes filled into `page_buf` while loading the current page
    fill: usize,
    /// emit cursor into `page_buf`
    emit_pos: usize,
    phase: IncrementPhase,
}

impl<'a, R: AsyncRead + Unpin> IncrementBodyReader<'a, R> {
    pub(crate) fn new(
        header: Vec<u8>,
        input: &'a mut R,
        blocks: Vec<u32>,
        _entry_size: u64,
    ) -> Self {
        Self {
            header,
            header_pos: 0,
            input,
            blocks,
            next_idx: 0,
            cur_block: 0,
            page_buf: [0u8; PG_PAGE_SIZE as usize],
            fill: 0,
            emit_pos: 0,
            phase: IncrementPhase::Header,
        }
    }
}

impl<'a, R: AsyncRead + Unpin> AsyncRead for IncrementBodyReader<'a, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = self.get_mut();
        let page = PG_PAGE_SIZE as usize;
        loop {
            match me.phase {
                IncrementPhase::Header => {
                    if me.header_pos < me.header.len() {
                        let n = (me.header.len() - me.header_pos).min(out.remaining());
                        if n == 0 {
                            return Poll::Ready(Ok(()));
                        }
                        out.put_slice(&me.header[me.header_pos..me.header_pos + n]);
                        me.header_pos += n;
                        return Poll::Ready(Ok(()));
                    }
                    me.phase = IncrementPhase::Load;
                }
                IncrementPhase::Load => {
                    if me.next_idx >= me.blocks.len() {
                        me.phase = IncrementPhase::Done;
                        continue;
                    }
                    let target = me.blocks[me.next_idx];
                    // fill page_buf with one full page from the input
                    while me.fill < page {
                        let mut rb = ReadBuf::new(&mut me.page_buf[me.fill..]);
                        match Pin::new(&mut *me.input).poll_read(cx, &mut rb) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Ready(Ok(())) => {
                                let got = rb.filled().len();
                                if got == 0 {
                                    return Poll::Ready(Err(std::io::Error::new(
                                        std::io::ErrorKind::UnexpectedEof,
                                        "increment input ended mid-page",
                                    )));
                                }
                                me.fill += got;
                            }
                        }
                    }
                    me.fill = 0;
                    if me.cur_block < target {
                        // intervening page: discard & advance
                        me.cur_block += 1;
                        continue;
                    }
                    me.emit_pos = 0;
                    me.phase = IncrementPhase::Emit;
                }
                IncrementPhase::Emit => {
                    let n = (page - me.emit_pos).min(out.remaining());
                    if n == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    out.put_slice(&me.page_buf[me.emit_pos..me.emit_pos + n]);
                    me.emit_pos += n;
                    if me.emit_pos == page {
                        me.next_idx += 1;
                        me.cur_block += 1;
                        me.phase = IncrementPhase::Load;
                    }
                    return Poll::Ready(Ok(()));
                }
                IncrementPhase::Done => return Poll::Ready(Ok(())),
            }
        }
    }
}

// Keep `increment` import non-dead in case future code routes through the
// module-typed Format directly
#[allow(dead_code)]
fn _bind_increment(_: increment::IncrementHeader) {}

struct PartCtx {
    file_no: u32,
    builder: Builder<PartWriter>,
    bytes_counter: Arc<AtomicU64>,
}

impl PartCtx {
    fn bytes_written(&self) -> u64 {
        self.bytes_counter.load(Ordering::Relaxed)
    }
}

async fn start_part(file_no: u32, parts_tx: &mpsc::Sender<Result<Part>>) -> Result<PartCtx> {
    let (byte_tx, byte_rx) = mpsc::channel::<std::io::Result<Bytes>>(4);
    let reader = ChannelReader::new(byte_rx);
    parts_tx
        .send(Ok(Part { file_no, reader }))
        .await
        .map_err(|_| anyhow!("parts consumer dropped"))?;
    let counter = Arc::new(AtomicU64::new(0));
    let writer = PartWriter::new(byte_tx, counter.clone());
    Ok(PartCtx {
        file_no,
        builder: Builder::new(writer),
        bytes_counter: counter,
    })
}

async fn finalize_part(ctx: PartCtx) -> Result<()> {
    // finish writes the two trailing zero blocks; shutdown flushes the tail
    // chunk, then dropping the writer closes the channel → ChannelReader EOF
    let mut builder = ctx.builder;
    builder.finish().await.context("finish tar part")?;
    let mut writer = builder.into_inner().await.context("into_inner tar part")?;
    writer.shutdown().await.context("flush part")?;
    Ok(())
}

fn strip_dotslash(s: &str) -> &str {
    s.strip_prefix("./").unwrap_or(s)
}

fn header_mtime(h: &Header) -> DateTime<Utc> {
    let secs = h.mtime().unwrap_or(0) as i64;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

fn broken_pipe() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "part consumer dropped")
}

/// Async writer that pushes coalesced chunks through a tokio mpsc as `Bytes`.
/// `PollSender::poll_reserve` parks the task when the channel is full — that's
/// the backpressure. `counter` tracks total tar bytes for rotation budgeting
pub(crate) struct PartWriter {
    sink: PollSender<std::io::Result<Bytes>>,
    scratch: Vec<u8>,
    counter: Arc<AtomicU64>,
}

impl PartWriter {
    pub(crate) fn new(tx: mpsc::Sender<std::io::Result<Bytes>>, counter: Arc<AtomicU64>) -> Self {
        Self {
            sink: PollSender::new(tx),
            scratch: Vec::with_capacity(CHUNK_BYTES),
            counter,
        }
    }

    /// Send the buffered scratch as one `Bytes` chunk, swapping in a fresh
    /// buffer. Avoids the per-CHUNK_BYTES memcpy that `Bytes::copy_from_slice`
    /// would do
    fn flush_chunk(&mut self, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        if self.scratch.is_empty() {
            return Poll::Ready(Ok(()));
        }
        match self.sink.poll_reserve(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => return Poll::Ready(Err(broken_pipe())),
            Poll::Ready(Ok(())) => {}
        }
        let chunk = Bytes::from(std::mem::replace(
            &mut self.scratch,
            Vec::with_capacity(CHUNK_BYTES),
        ));
        self.sink.send_item(Ok(chunk)).map_err(|_| broken_pipe())?;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for PartWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let me = self.get_mut();
        // Flush the pending chunk before growing scratch past the threshold.
        // Re-poll re-enters here without re-buffering `buf` (extend happens once,
        // after the flush completes Ready)
        if me.scratch.len() >= CHUNK_BYTES {
            match me.flush_chunk(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
        }
        me.scratch.extend_from_slice(buf);
        me.counter.fetch_add(buf.len() as u64, Ordering::Relaxed);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        self.get_mut().flush_chunk(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        // Flush tail bytes; dropping the writer (and its PollSender) closes the
        // channel so the ChannelReader sees EOF
        self.get_mut().flush_chunk(cx)
    }
}

/// Public helper for callers that want to compute a remap prefix for a
/// non-default tablespace from its OID
pub fn tablespace_prefix(oid: u32) -> String {
    format!("pg_tblspc/{oid}/")
}

#[cfg(test)]
mod tests {
    // Test fixtures build & inspect archives with the sync `tar` crate; the
    // `Read` import drives `read_to_end` on those sync entries
    use std::io::Read as _;

    use super::*;
    use tokio::io::AsyncReadExt;

    fn build_input_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        {
            let mut b = tar::Builder::new(&mut out);
            for (name, data) in entries {
                let mut h = tar::Header::new_gnu();
                h.set_path(name).unwrap();
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
                h.set_mtime(1_700_000_000);
                h.set_cksum();
                b.append(&h, *data).unwrap();
            }
            b.finish().unwrap();
        }
        out
    }

    async fn collect_parts(mut rx: mpsc::Receiver<Result<Part>>) -> Vec<(u32, Vec<u8>)> {
        let mut out = Vec::new();
        while let Some(p) = rx.recv().await {
            let mut p = p.unwrap();
            let mut bytes = Vec::new();
            p.reader.read_to_end(&mut bytes).await.unwrap();
            out.push((p.file_no, bytes));
        }
        out
    }

    fn list_entries(tar_bytes: &[u8]) -> Vec<(String, u64)> {
        let mut a = tar::Archive::new(tar_bytes);
        let mut out = Vec::new();
        for e in a.entries().unwrap() {
            let e = e.unwrap();
            let h = e.header();
            let name = e.path().unwrap().to_string_lossy().into_owned();
            out.push((name, h.size().unwrap()));
        }
        out
    }

    #[tokio::test]
    async fn passthrough_single_part() {
        let input = build_input_tar(&[("PG_VERSION", b"16"), ("global/pg_control", b"X")]);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 10 * 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, 1);
        let listed = list_entries(&parts[0].1);
        assert_eq!(
            listed,
            vec![("PG_VERSION".into(), 2), ("global/pg_control".into(), 1)]
        );
        assert_eq!(res.last_file_no, 1);
        assert!(res.files.contains_key("PG_VERSION"));
        assert!(res.files.contains_key("global/pg_control"));
    }

    #[tokio::test]
    async fn applies_prefix_remap() {
        let input = build_input_tar(&[("PG_VERSION", b"16")]);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                prefix: Some(tablespace_prefix(16384)),
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        let listed = list_entries(&parts[0].1);
        assert_eq!(listed, vec![("pg_tblspc/16384/PG_VERSION".into(), 2)]);
        assert!(res.files.contains_key("pg_tblspc/16384/PG_VERSION"));
    }

    #[tokio::test]
    async fn rotates_parts_at_threshold() {
        // Three 600 KiB entries, threshold 1 MiB → expect 3 parts (each part
        // can fit only one entry without overflow)
        let big = vec![0u8; 600 * 1024];
        let input = build_input_tar(&[("a.bin", &big), ("b.bin", &big), ("c.bin", &big)]);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert_eq!(parts.len(), 3, "expected 3 parts, got {}", parts.len());
        for (n, (file_no, bytes)) in parts.iter().enumerate() {
            assert_eq!(*file_no, (n + 1) as u32);
            let listed = list_entries(bytes);
            assert_eq!(listed.len(), 1);
        }
        assert_eq!(res.last_file_no, 3);
    }

    #[tokio::test]
    async fn oversize_entry_gets_own_part() {
        // 2 MiB single entry, threshold 1 MiB → one part with that entry alone
        let big = vec![0u8; 2 * 1024 * 1024];
        let input = build_input_tar(&[("PG_VERSION", b"16"), ("huge.bin", &big)]);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert_eq!(parts.len(), 2);
        let p0 = list_entries(&parts[0].1);
        assert_eq!(p0, vec![("PG_VERSION".into(), 2)]);
        let p1 = list_entries(&parts[1].1);
        assert_eq!(p1, vec![("huge.bin".into(), big.len() as u64)]);
        assert_eq!(res.last_file_no, 2);
    }

    #[tokio::test]
    async fn tees_named_entry_to_separate_tar() {
        let input = build_input_tar(&[
            ("PG_VERSION", b"16"),
            ("global/pg_control", b"control-bytes"),
            ("base/1/2606", b"data"),
        ]);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                tee_names: vec!["global/pg_control".into()],
                max_tar_size: 10 * 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        // Main part still contains pg_control
        let names: Vec<_> = list_entries(&parts[0].1)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert!(names.iter().any(|n| n == "global/pg_control"));
        // Tee tar exists and contains only pg_control
        let tee = res.tee_bytes.expect("tee tar bytes");
        let tee_names: Vec<_> = list_entries(&tee).into_iter().map(|(n, _)| n).collect();
        assert_eq!(tee_names, vec!["global/pg_control".to_string()]);
    }

    /// PG basebackup tars carry paths > 100 chars (long table/relation names,
    /// nested tablespace dirs) which require GNU LongLink emission. Confirms
    /// the streamer reads them in (Archive auto-resolves LongLink) and writes
    /// them out with prefix prepended, surviving a round-trip read.
    #[tokio::test]
    async fn long_path_roundtrip_with_prefix() {
        // 180-char path in the input — well past ustar's 100-byte name limit
        let long_segment = "a".repeat(120);
        let long_path = format!("base/16384/{long_segment}");
        assert!(long_path.len() > 100);

        let mut input = Vec::new();
        {
            let mut b = tar::Builder::new(&mut input);
            // append_data emits LongLink ('L') automatically for > 100 char paths
            b.append_data(
                &mut {
                    let mut h = tar::Header::new_gnu();
                    h.set_size(4);
                    h.set_mode(0o644);
                    h.set_mtime(1_700_000_000);
                    h.set_entry_type(tar::EntryType::Regular);
                    h
                },
                &long_path,
                &b"DATA"[..],
            )
            .unwrap();
            b.finish().unwrap();
        }

        let prefix = tablespace_prefix(16385);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                prefix: Some(prefix.clone()),
                max_tar_size: 10 * 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert_eq!(parts.len(), 1);

        let listed = list_entries(&parts[0].1);
        let want = format!("{prefix}{long_path}");
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert_eq!(listed[0].0, want, "remapped path lost on long-name path");
        assert_eq!(listed[0].1, 4);
        assert!(res.files.contains_key(&want), "files map: {:?}", res.files);
    }

    /// PG basebackup occasionally embeds pax extended headers (utf-8 names,
    /// large mtime resolutions). Verify the streamer reads through pax and
    /// writes the correct effective path on output.
    #[tokio::test]
    async fn pax_extended_header_roundtrip() {
        // Hand-craft an input tar with a pax 'x' extended-header entry that
        // overrides the 'path' attribute, followed by the actual file entry
        // with a placeholder short name. Mirrors what GNU tar emits when
        // configured with --format=pax.
        let real_path = "base/16384/very/deeply/nested/dir/relation_with_a_truly_overlong_name_of_more_than_one_hundred_and_twenty_chars_xxxxxx";
        assert!(real_path.len() > 100);

        // pax record: "<len> path=<real_path>\n" where <len> includes itself
        let mut len = real_path.len() + " path=\n".len();
        let mut digits = format!("{len}").len();
        loop {
            let cand = len + digits;
            if format!("{cand}").len() == digits {
                len = cand;
                break;
            }
            digits += 1;
        }
        let pax_record = format!("{len} path={real_path}\n");
        assert_eq!(pax_record.len(), len);

        let mut input = Vec::new();
        {
            let mut pax_hdr = tar::Header::new_ustar();
            pax_hdr.set_path("PaxHeader/dummy").unwrap();
            pax_hdr.set_size(pax_record.len() as u64);
            pax_hdr.set_mode(0o644);
            pax_hdr.set_mtime(1_700_000_000);
            pax_hdr.set_entry_type(tar::EntryType::XHeader);
            pax_hdr.set_cksum();

            let mut data_hdr = tar::Header::new_ustar();
            data_hdr.set_path("placeholder.short").unwrap();
            data_hdr.set_size(4);
            data_hdr.set_mode(0o644);
            data_hdr.set_mtime(1_700_000_000);
            data_hdr.set_entry_type(tar::EntryType::Regular);
            data_hdr.set_cksum();

            let mut b = tar::Builder::new(&mut input);
            b.append(&pax_hdr, pax_record.as_bytes()).unwrap();
            b.append(&data_hdr, &b"DATA"[..]).unwrap();
            b.finish().unwrap();
        }

        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 10 * 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert_eq!(parts.len(), 1);

        let listed = list_entries(&parts[0].1);
        // Effective path (resolved from pax) must survive to the output
        let names: Vec<&str> = listed.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&real_path),
            "pax-overridden path missing in output: {names:?}"
        );
        assert!(res.files.contains_key(real_path), "{:?}", res.files);
    }

    // ─── page-LSN final-state filter ────────────────────────────────────────

    /// Valid 24-byte heap page header carrying `lsn` (pd_upper non-zero ⇒ not
    /// new, size/version/offsets all in range so `page_is_valid` holds)
    fn page_header(lsn: u64) -> [u8; PG_PAGE_HEADER_SIZE] {
        let mut h = [0u8; PG_PAGE_HEADER_SIZE];
        h[0..4].copy_from_slice(&((lsn >> 32) as u32).to_le_bytes()); // pd_lsn high
        h[4..8].copy_from_slice(&(lsn as u32).to_le_bytes()); // pd_lsn low
        h[10..12].copy_from_slice(&0u16.to_le_bytes()); // pd_flags
        h[12..14].copy_from_slice(&(PG_PAGE_HEADER_SIZE as u16).to_le_bytes()); // pd_lower
        h[14..16].copy_from_slice(&(PG_PAGE_SIZE as u16).to_le_bytes()); // pd_upper
        h[16..18].copy_from_slice(&(PG_PAGE_SIZE as u16).to_le_bytes()); // pd_special
        h[18..20].copy_from_slice(&(0x2000u16 | 4).to_le_bytes()); // BLCKSZ | layout v4
        h
    }

    #[test]
    fn page_filter_keeps_changed_and_drops_settled() {
        let parent = 200u64;
        // settled strictly below parent → identical to parent's copy → drop
        assert!(!page_changed_since(&page_header(100), parent));
        // changed at/after parent → keep
        assert!(page_changed_since(&page_header(200), parent));
        assert!(page_changed_since(&page_header(300), parent));
    }

    #[test]
    fn page_filter_keeps_new_invalid_and_short() {
        // all-zero (vacuumed/new) page: pd_upper == 0 ⇒ kept despite lsn 0
        assert!(page_changed_since(&[0u8; PG_PAGE_HEADER_SIZE], 200));
        // non-new but invalid (bad size/version) ⇒ lsn untrustworthy ⇒ kept
        let mut bad = page_header(50);
        bad[18..20].copy_from_slice(&0u16.to_le_bytes()); // wipe pd_pagesize_version
        assert!(page_changed_since(&bad, 200));
        // truncated header ⇒ kept
        assert!(page_changed_since(&[0u8; 8], 200));
    }

    // ─── delta mode ─────────────────────────────────────────────────────────

    /// Parent-backup file set for delta tests: the paths the increment base
    /// is pretended to contain, so eligible files clear the `wasInBase` gate
    fn parent_set(paths: &[&str]) -> Arc<HashSet<String>> {
        Arc::new(paths.iter().map(|p| p.to_string()).collect())
    }

    fn build_paged_tar(name: &str, n_blocks: u32, fill: u8) -> Vec<u8> {
        let size = n_blocks as usize * PG_PAGE_SIZE as usize;
        let mut body = vec![fill; size];
        // Mark each block with its block number in the first 4 bytes so
        // post-apply we can assert which page came from where
        for b in 0..n_blocks {
            let off = b as usize * PG_PAGE_SIZE as usize;
            body[off..off + 4].copy_from_slice(&b.to_le_bytes());
        }
        build_input_tar(&[(name, &body)])
    }

    #[tokio::test]
    async fn delta_emits_wi1_increment_with_only_changed_blocks() {
        // 4-block rel file, parent says blocks 1 and 3 changed
        let rel_path = "base/16384/16400";
        let input = build_paged_tar(rel_path, 4, 0xAA);

        let rel = crate::pg::walparser::RelFileNode {
            spc_node: delta_mod::DEFAULT_SPC_NODE,
            db_node: 16384,
            rel_node: 16400,
        };
        let mut map = PagedFileDeltaMap::new();
        map.add_location(crate::pg::walparser::BlockLocation { rel, block_no: 1 });
        map.add_location(crate::pg::walparser::BlockLocation { rel, block_no: 3 });

        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 10 * 1024 * 1024,
                delta_context: Some(DeltaContext {
                    map: Arc::new(map),
                    format: IncrementFormat::Wi1,
                    parent_files: parent_set(&[rel_path]),
                    parent_start_lsn: None,
                }),
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();

        assert_eq!(parts.len(), 1);
        let meta = res.files.get(rel_path).expect("file in metadata");
        assert!(meta.is_incremented, "delta-eligible file should be marked");
        assert!(!meta.is_skipped);

        // Decode the increment from the output tar & confirm the body is
        // exactly the two changed pages (with their leading block numbers)
        let mut a = tar::Archive::new(parts[0].1.as_slice());
        let mut entry = a.entries().unwrap().next().unwrap().unwrap();
        let mut increment_bytes = Vec::new();
        entry.read_to_end(&mut increment_bytes).unwrap();

        let header = increment::read_increment_header(&increment_bytes[..]).unwrap();
        assert_eq!(header.blocks, vec![1, 3]);
        assert_eq!(header.file_size, 4 * PG_PAGE_SIZE);

        // Page bodies start right after the header (magic 4 + size 8 + count 4 + 2*u32 = 24)
        let body_off = 4 + 8 + 4 + header.blocks.len() * 4;
        let blcksz = PG_PAGE_SIZE as usize;
        let page1 = &increment_bytes[body_off..body_off + blcksz];
        let page3 = &increment_bytes[body_off + blcksz..body_off + 2 * blcksz];
        assert_eq!(&page1[..4], &1u32.to_le_bytes());
        assert_eq!(&page3[..4], &3u32.to_le_bytes());
    }

    #[tokio::test]
    async fn delta_emits_native_increment_padded_to_blcksz() {
        // PG17-native dispatch — single dirty block
        let rel_path = "base/16384/16401";
        let input = build_paged_tar(rel_path, 5, 0xCC);

        let rel = crate::pg::walparser::RelFileNode {
            spc_node: delta_mod::DEFAULT_SPC_NODE,
            db_node: 16384,
            rel_node: 16401,
        };
        let mut map = PagedFileDeltaMap::new();
        map.add_location(crate::pg::walparser::BlockLocation { rel, block_no: 2 });

        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                delta_context: Some(DeltaContext {
                    map: Arc::new(map),
                    format: IncrementFormat::Native,
                    parent_files: parent_set(&[rel_path]),
                    parent_start_lsn: None,
                }),
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();

        let meta = res.files.get(rel_path).expect("file metadata");
        assert!(meta.is_incremented);

        let mut a = tar::Archive::new(parts[0].1.as_slice());
        let mut entry = a.entries().unwrap().next().unwrap().unwrap();
        let mut increment_bytes = Vec::new();
        entry.read_to_end(&mut increment_bytes).unwrap();

        // header padded to BLCKSZ + one page body
        assert_eq!(increment_bytes.len() as u64, PG_PAGE_SIZE + PG_PAGE_SIZE);

        // Apply on top of a same-size scratch & verify page 2 carries the
        // marker we stamped during build_paged_tar
        let mut target = std::io::Cursor::new(vec![0u8; 5 * PG_PAGE_SIZE as usize]);
        let (size, n, fmt) = increment::apply_increment_in_place(
            &mut std::io::Cursor::new(increment_bytes),
            &mut target,
        )
        .unwrap();
        assert_eq!(size, 5 * PG_PAGE_SIZE);
        assert_eq!(n, 1);
        assert_eq!(fmt, IncrementFormat::Native);
        let inner = target.into_inner();
        let block2 = &inner[2 * PG_PAGE_SIZE as usize..3 * PG_PAGE_SIZE as usize];
        assert_eq!(&block2[..4], &2u32.to_le_bytes());
    }

    #[tokio::test]
    async fn delta_skips_unchanged_paged_files_entirely() {
        // No entries in the delta map → blocks_for() returns None → skip
        let rel_path = "base/16384/16402";
        let input = build_paged_tar(rel_path, 3, 0x11);
        let map = PagedFileDeltaMap::new();
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                delta_context: Some(DeltaContext {
                    map: Arc::new(map),
                    format: IncrementFormat::Wi1,
                    parent_files: parent_set(&[rel_path]),
                    parent_start_lsn: None,
                }),
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();

        // Sentinel entry in metadata, but no body in tar
        let meta = res.files.get(rel_path).expect("file metadata");
        assert!(meta.is_skipped);
        assert!(!meta.is_incremented);
        if !parts.is_empty() {
            let listed = list_entries(&parts[0].1);
            assert!(
                listed.is_empty(),
                "tar should be empty when only entry got skipped; got {listed:?}"
            );
        }
    }

    #[tokio::test]
    async fn delta_passes_through_non_paged_entries() {
        // Non-paged file: PG_VERSION sits next to the paged file
        let rel_path = "base/16384/16403";
        // First write a paged file, then a non-paged one in the same tar
        let n_blocks = 2u32;
        let size = n_blocks as usize * PG_PAGE_SIZE as usize;
        let mut paged_body = vec![0xAAu8; size];
        for b in 0..n_blocks {
            let off = b as usize * PG_PAGE_SIZE as usize;
            paged_body[off..off + 4].copy_from_slice(&b.to_le_bytes());
        }
        let input = build_input_tar(&[(rel_path, &paged_body), ("PG_VERSION", b"16")]);

        let map = PagedFileDeltaMap::new(); // empty → paged file gets skipped
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                delta_context: Some(DeltaContext {
                    map: Arc::new(map),
                    format: IncrementFormat::Wi1,
                    parent_files: parent_set(&[rel_path]),
                    parent_start_lsn: None,
                }),
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert!(res.files.get(rel_path).expect("paged meta").is_skipped);
        let pg_version_meta = res.files.get("PG_VERSION").expect("non-paged meta");
        assert!(!pg_version_meta.is_skipped);
        assert!(!pg_version_meta.is_incremented);

        let listed = list_entries(&parts[0].1);
        assert_eq!(listed, vec![("PG_VERSION".into(), 2)]);
    }

    #[tokio::test]
    async fn delta_ships_new_file_in_full_not_incremented() {
        // Relation created after the parent: its pages are in the delta map,
        // but it was NOT in the increment base. wal-g would have no base file
        // to apply onto ("incremented file should always exist"), so it must
        // ship in full. Regression for cross_tool_delta forward interop
        let rel_path = "base/16384/16405";
        let input = build_paged_tar(rel_path, 4, 0xAB);

        let rel = crate::pg::walparser::RelFileNode {
            spc_node: delta_mod::DEFAULT_SPC_NODE,
            db_node: 16384,
            rel_node: 16405,
        };
        let mut map = PagedFileDeltaMap::new();
        map.add_location(crate::pg::walparser::BlockLocation { rel, block_no: 0 });
        map.add_location(crate::pg::walparser::BlockLocation { rel, block_no: 2 });

        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 10 * 1024 * 1024,
                delta_context: Some(DeltaContext {
                    map: Arc::new(map),
                    format: IncrementFormat::Wi1,
                    // parent did NOT contain this file
                    parent_files: parent_set(&["base/16384/99999"]),
                    parent_start_lsn: None,
                }),
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();

        let meta = res.files.get(rel_path).expect("file metadata");
        assert!(!meta.is_incremented, "new file must not be incremented");
        assert!(!meta.is_skipped, "new file must not be skipped");

        // Full body present: 4 whole blocks, no wi1 magic
        let listed = list_entries(&parts[0].1);
        assert_eq!(listed, vec![(rel_path.into(), 4 * PG_PAGE_SIZE)]);
        let mut a = tar::Archive::new(parts[0].1.as_slice());
        let mut entry = a.entries().unwrap().next().unwrap().unwrap();
        let mut body = Vec::new();
        entry.read_to_end(&mut body).unwrap();
        assert_eq!(body.len() as u64, 4 * PG_PAGE_SIZE);
        assert_ne!(&body[..3], &increment::INCREMENT_MAGIC[..3]);
    }

    #[tokio::test]
    async fn delta_filters_blocks_past_eof() {
        // Delta map claims block 99 is dirty, but file only has 3 blocks.
        // Streamer must filter & end up with nothing-to-emit → skip
        let rel_path = "base/16384/16404";
        let input = build_paged_tar(rel_path, 3, 0x55);
        let rel = crate::pg::walparser::RelFileNode {
            spc_node: delta_mod::DEFAULT_SPC_NODE,
            db_node: 16384,
            rel_node: 16404,
        };
        let mut map = PagedFileDeltaMap::new();
        map.add_location(crate::pg::walparser::BlockLocation { rel, block_no: 99 });

        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                delta_context: Some(DeltaContext {
                    map: Arc::new(map),
                    format: IncrementFormat::Wi1,
                    parent_files: parent_set(&[rel_path]),
                    parent_start_lsn: None,
                }),
                ..Default::default()
            },
        );
        let _parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        let meta = res.files.get(rel_path).expect("file metadata");
        assert!(meta.is_skipped, "out-of-range blocks must skip the file");
    }

    #[tokio::test]
    async fn continues_file_numbering() {
        let input = build_input_tar(&[("foo", b"hi")]);
        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                starting_file_no: 7,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();
        assert_eq!(parts[0].0, 8);
        assert_eq!(res.last_file_no, 8);
    }

    /// Thousands of tiny entries against a part-size threshold: a single part
    /// must pack many thousands of entries, all entries survive byte-clean
    /// across the parts, and tar_file_sets accounts for each exactly once.
    /// Downscaled from the ~130k-entries-per-part production worry (8.2 KiB
    /// on-wire each vs a 1 GiB threshold) to keep the test fast.
    #[tokio::test]
    async fn many_tiny_entries_pack_across_parts() {
        const N: usize = 5000;
        let bodies: Vec<(String, Vec<u8>)> = (0..N)
            .map(|i| {
                (
                    format!("base/16384/{}", 16384 + i),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        let entries: Vec<(&str, &[u8])> = bodies
            .iter()
            .map(|(n, d)| (n.as_str(), d.as_slice()))
            .collect();
        let input = build_input_tar(&entries);

        let (rx, h) = start(
            std::io::Cursor::new(input),
            StreamerOpts {
                max_tar_size: 2 * 1024 * 1024,
                ..Default::default()
            },
        );
        let parts = collect_parts(rx).await;
        let res = h.await.unwrap().unwrap();

        assert!(
            parts.len() >= 2,
            "expected multiple parts, got {}",
            parts.len()
        );
        let max_entries = res.tar_file_sets.values().map(|v| v.len()).max().unwrap();
        assert!(
            max_entries > 1000,
            "no part packed many entries: {max_entries}"
        );

        // Every entry survives byte-clean across all parts
        let mut seen: HashMap<String, Vec<u8>> = HashMap::new();
        for (_no, bytes) in &parts {
            let mut a = tar::Archive::new(bytes.as_slice());
            for e in a.entries().unwrap() {
                let mut e = e.unwrap();
                let name = e.path().unwrap().to_string_lossy().into_owned();
                let mut body = Vec::new();
                e.read_to_end(&mut body).unwrap();
                seen.insert(name, body);
            }
        }
        assert_eq!(seen.len(), N, "entry count mismatch after repack");
        for (name, want) in &bodies {
            assert_eq!(seen.get(name), Some(want), "entry {name} corrupted/missing");
        }
        let total: usize = res.tar_file_sets.values().map(|v| v.len()).sum();
        assert_eq!(total, N, "tar_file_sets must account for every entry once");
    }
}
