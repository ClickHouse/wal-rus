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
//! The streamer runs as `spawn_blocking` because `tar::Archive` /
//! `tar::Builder` are sync. Async input is bridged via `SyncIoBridge`;
//! per-part output flows over an mpsc of `Bytes` that the caller reads as
//! an `AsyncRead` (see `ChannelReader`)

use std::collections::HashMap;
use std::io::{Read, Write};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::io::SyncIoBridge;

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
}

impl Default for StreamerOpts {
    fn default() -> Self {
        Self {
            prefix: None,
            tee_names: Vec::new(),
            max_tar_size: DEFAULT_TAR_SIZE_THRESHOLD,
            starting_file_no: 0,
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
    let (parts_tx, parts_rx) = mpsc::channel::<Result<Part>>(1);
    let handle = tokio::task::spawn_blocking(move || -> Result<StreamerResult> {
        let sync_input = SyncIoBridge::new(input);
        run_blocking(sync_input, opts, parts_tx)
    });
    (parts_rx, handle)
}

fn run_blocking<R: Read>(
    input: R,
    opts: StreamerOpts,
    parts_tx: mpsc::Sender<Result<Part>>,
) -> Result<StreamerResult> {
    let mut archive = tar::Archive::new(input);
    let mut entries = archive.entries().context("open tar entries")?;

    let mut result = StreamerResult::default();
    let mut file_no = opts.starting_file_no;
    let mut tee_builder: Option<tar::Builder<Vec<u8>>> = if opts.tee_names.is_empty() {
        None
    } else {
        Some(tar::Builder::new(Vec::new()))
    };

    let mut current: Option<PartCtx> = None;

    for entry in entries.by_ref() {
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

        // Rotate before writing if it would push us past the threshold,
        // mirrors wal-g's pre-emptive split (avoids straddled entries)
        if let Some(ctx) = current.as_ref()
            && ctx.bytes_written() > 0
            && ctx.bytes_written().saturating_add(entry_size) > opts.max_tar_size
        {
            finalize_part(current.take().unwrap())?;
        }
        if current.is_none() {
            file_no += 1;
            current = Some(start_part(file_no, &parts_tx)?);
        }
        let ctx = current.as_mut().unwrap();

        // append_data handles path encoding (auto-emits GNU LongLink for
        // > 100 char paths) and cksum, so no set_path here
        let mut new_hdr = header.clone();

        // Decide whether to tee this entry
        let tee_match =
            !is_dir && tee_builder.is_some() && opts.tee_names.iter().any(|n| n == &mapped);

        if tee_match {
            // Tee path: buffer in memory (only used for small files like pg_control)
            let mut buf = Vec::with_capacity(entry_size as usize);
            entry.read_to_end(&mut buf).context("read tee entry")?;
            ctx.builder
                .append_data(&mut new_hdr, &mapped, std::io::Cursor::new(&buf))
                .context("append to current part")?;
            if let Some(tb) = tee_builder.as_mut() {
                let mut tee_hdr = header.clone();
                tb.append_data(&mut tee_hdr, &mapped, std::io::Cursor::new(&buf))
                    .context("append to tee tar")?;
            }
        } else {
            ctx.builder
                .append_data(&mut new_hdr, &mapped, &mut entry)
                .context("append to current part")?;
        }

        if !is_dir {
            result.files.insert(
                strip_dotslash(&mapped).to_string(),
                FileMeta {
                    is_incremented: false,
                    is_skipped: false,
                    mtime: header_mtime(&header),
                },
            );
        }
        let part_name = format!("part_{:03}.tar", ctx.file_no);
        result
            .tar_file_sets
            .entry(part_name)
            .or_default()
            .push(strip_dotslash(&mapped).to_string());
    }

    if let Some(ctx) = current.take() {
        finalize_part(ctx)?;
    }
    if let Some(tb) = tee_builder.take() {
        let buf = tb.into_inner().context("finish tee tar")?;
        if !buf.is_empty() {
            result.tee_bytes = Some(Bytes::from(buf));
        }
    }

    result.last_file_no = file_no;
    Ok(result)
}

struct PartCtx {
    file_no: u32,
    builder: tar::Builder<CountingWriter<BlockingSender>>,
    bytes_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl PartCtx {
    fn bytes_written(&self) -> u64 {
        self.bytes_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn start_part(file_no: u32, parts_tx: &mpsc::Sender<Result<Part>>) -> Result<PartCtx> {
    let (byte_tx, byte_rx) = mpsc::channel::<std::io::Result<Bytes>>(4);
    let reader = ChannelReader::new(byte_rx);
    parts_tx
        .blocking_send(Ok(Part { file_no, reader }))
        .map_err(|_| anyhow!("parts consumer dropped"))?;
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer = CountingWriter {
        inner: BlockingSender {
            tx: byte_tx,
            scratch: Vec::with_capacity(CHUNK_BYTES),
        },
        counter: counter.clone(),
    };
    Ok(PartCtx {
        file_no,
        builder: tar::Builder::new(writer),
        bytes_counter: counter,
    })
}

fn finalize_part(ctx: PartCtx) -> Result<()> {
    // tar::Builder::into_inner writes the two trailing zero blocks then
    // returns the inner writer
    let writer = ctx.builder.into_inner().context("finish tar part")?;
    let CountingWriter { mut inner, .. } = writer;
    inner.flush().context("flush part")?;
    drop(inner); // drop sender → ChannelReader sees EOF
    Ok(())
}

fn strip_dotslash(s: &str) -> &str {
    s.strip_prefix("./").unwrap_or(s)
}

fn header_mtime(h: &tar::Header) -> DateTime<Utc> {
    let secs = h.mtime().unwrap_or(0) as i64;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

/// Sync writer that pushes its writes through a tokio mpsc as `Bytes`.
/// `blocking_send` parks the writer thread when the channel is full —
/// that's the backpressure
struct BlockingSender {
    tx: mpsc::Sender<std::io::Result<Bytes>>,
    scratch: Vec<u8>,
}

impl Write for BlockingSender {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Coalesce small writes into a single channel send to avoid per-512-byte
        // tar block traffic across the channel
        self.scratch.extend_from_slice(buf);
        if self.scratch.len() >= CHUNK_BYTES {
            self.flush_scratch()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_scratch()
    }
}

impl BlockingSender {
    fn flush_scratch(&mut self) -> std::io::Result<()> {
        if self.scratch.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::copy_from_slice(&self.scratch);
        self.scratch.clear();
        self.tx.blocking_send(Ok(chunk)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "part consumer dropped")
        })
    }
}

impl Drop for BlockingSender {
    fn drop(&mut self) {
        // Best-effort flush of any tail bytes before EOF
        let _ = self.flush_scratch();
    }
}

struct CountingWriter<W: Write> {
    inner: W,
    counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.counter
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Public helper for callers that want to compute a remap prefix for a
/// non-default tablespace from its OID
pub fn tablespace_prefix(oid: u32) -> String {
    format!("pg_tblspc/{oid}/")
}

#[cfg(test)]
mod tests {
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
}
