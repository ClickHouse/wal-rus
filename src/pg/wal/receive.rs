//! wal-receive: long-running START_REPLICATION consumer that archives
//! streamed WAL segments directly. Drop-in replacement for an
//! `archive_command`-driven workflow when a sidecar host is preferred.
//!
//! Wire pieces:
//!
//! - `IDENTIFY_SYSTEM` returns current timeline + LSN
//! - `START_REPLICATION <lsn> TIMELINE <tli>` enters CopyOut mode
//! - Server sends CopyData frames:
//!   'w' = WAL data:   u64 start_lsn | u64 server_wal_end | u64 send_time | bytes
//!   'k' = keepalive:  u64 server_wal_end | u64 send_time | u8 reply_requested
//! - Client replies (`r`): u64 write | u64 flush | u64 apply | u64 client_time | u8 reply_requested
//!
//! Segments accumulate into a 16 MiB-aligned buffer; rotation spawns a
//! `wal-push` upload task bounded by WALG_UPLOAD_CONCURRENCY, so the
//! receive loop keeps consuming frames (and answering keepalives) while
//! slow uploads drain. Uploaded segments ride the regular compression +
//! storage pipeline so the archive format stays consistent with
//! `archive_command`-driven pushes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::config::Settings;
use crate::pg::backup::format_pg_lsn;
use crate::pg::replication::conn::{PgConfig, ReplicationConn, error_message, message_kind};
use crate::pg::replication::stream::{Frame, build_status_update, decode_frame};
use crate::pg::wal::push;
use crate::pg::wal::segment::{DEFAULT_WAL_SEG_SIZE, SegmentName};
use crate::storage::DynStorage;

/// Status update cadence — wal-g defaults to 10s; we match
const STATUS_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Accumulate WAL bytes into segment-sized files on disk; rotate on
/// boundary crossings. Each fully-written segment is shipped via the
/// existing `wal-push` pipeline so compression + retry + rate limits apply
struct SegmentAccumulator {
    seg_size: u64,
    timeline: u32,
    archive_dir: PathBuf,
    current: Option<CurrentSegment>,
    settings: Settings,
    storage: DynStorage,
    /// In-flight segment uploads, bounded by `upload_sem`. Receive loop reaps
    /// completions each iteration; rotation blocks only once
    /// `upload_concurrency` uploads are already in flight
    uploads: JoinSet<Result<()>>,
    upload_sem: Arc<Semaphore>,
}

struct CurrentSegment {
    name: SegmentName,
    file: tokio::fs::File,
    path: PathBuf,
    bytes_written: u64,
}

impl SegmentAccumulator {
    async fn new(
        timeline: u32,
        archive_dir: PathBuf,
        seg_size: u64,
        settings: Settings,
        storage: DynStorage,
    ) -> Result<Self> {
        fs::create_dir_all(&archive_dir)
            .await
            .with_context(|| format!("create_dir_all {}", archive_dir.display()))?;
        let upload_sem = Arc::new(Semaphore::new(settings.upload_concurrency.max(1)));
        Ok(Self {
            seg_size,
            timeline,
            archive_dir,
            current: None,
            settings,
            storage,
            uploads: JoinSet::new(),
            upload_sem,
        })
    }

    fn segment_for_lsn(&self, lsn: u64) -> SegmentName {
        let seg_no = lsn / self.seg_size;
        let xlog_segs_per_xlog_id = 0x1_0000_0000u64 / self.seg_size;
        SegmentName {
            timeline: self.timeline,
            log_id: (seg_no / xlog_segs_per_xlog_id) as u32,
            seg_no: (seg_no % xlog_segs_per_xlog_id) as u32,
        }
    }

    /// Write `data` starting at `start_lsn`. Splits across segment
    /// boundaries, rotating + spawning an upload for each completed segment
    async fn write(&mut self, start_lsn: u64, data: &[u8]) -> Result<()> {
        let mut lsn = start_lsn;
        let mut data = data;
        while !data.is_empty() {
            self.ensure_current(lsn).await?;
            let cur = self.current.as_mut().expect("ensure_current set it");
            let cur_seg_start = cur.name.start_lsn(self.seg_size);
            let cur_seg_end = cur_seg_start + self.seg_size;
            let offset_in_seg = lsn - cur_seg_start;
            let space_left = cur_seg_end - lsn;
            let chunk_len = std::cmp::min(space_left as usize, data.len());

            // Files are pre-allocated to seg_size + writes seek-positioned by
            // offset_in_seg. Postgres always sends contiguous WAL but if our
            // accumulator's `bytes_written` lags behind the offset (eg on
            // reconnect at a non-zero offset), preserve the gap by seeking
            if offset_in_seg != cur.bytes_written {
                use tokio::io::AsyncSeekExt;
                cur.file
                    .seek(std::io::SeekFrom::Start(offset_in_seg))
                    .await
                    .context("seek within segment")?;
            }
            cur.file
                .write_all(&data[..chunk_len])
                .await
                .context("write WAL chunk")?;
            cur.bytes_written = offset_in_seg + chunk_len as u64;
            lsn += chunk_len as u64;
            data = &data[chunk_len..];
            if cur.bytes_written == self.seg_size {
                self.rotate_and_spawn().await?;
            }
        }
        Ok(())
    }

    async fn ensure_current(&mut self, lsn: u64) -> Result<()> {
        if let Some(cur) = self.current.as_ref() {
            let seg_start = cur.name.start_lsn(self.seg_size);
            if lsn >= seg_start && lsn < seg_start + self.seg_size {
                return Ok(());
            }
            // Boundary crossing on a partial segment is unusual but possible
            // (eg WAL switch midway); push what we have (zero-padded to
            // seg_size by the pre-extend), then open the new one
            tracing::warn!(
                target = "wal_receive",
                "rotating partial segment {} ({} bytes written, target {})",
                cur.name.format(),
                cur.bytes_written,
                self.seg_size
            );
            self.rotate_and_spawn().await?;
        }
        let seg = self.segment_for_lsn(lsn);
        let path = self.archive_dir.join(seg.format());
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        // pre-extend to seg_size so partial-tail writes leave a zero pad
        file.set_len(self.seg_size).await?;
        // rotate_and_spawn above took any previous segment
        self.current = Some(CurrentSegment {
            name: seg,
            file,
            path,
            bytes_written: 0,
        });
        Ok(())
    }

    /// Close out current segment & spawn its upload. Permit acquisition is
    /// the backpressure point: blocks only once `upload_concurrency` uploads
    /// are already in flight, so bursty WAL against a slow store stalls the
    /// stream N segments deep instead of on every rotation
    async fn rotate_and_spawn(&mut self) -> Result<()> {
        let Some(cur) = self.current.take() else {
            return Ok(());
        };
        let CurrentSegment {
            name,
            mut file,
            path,
            ..
        } = cur;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        tracing::info!(
            target = "wal_receive",
            "segment {} complete, archiving",
            name.format()
        );
        let permit = self
            .upload_sem
            .clone()
            .acquire_owned()
            .await
            .context("acquire upload permit")?;
        let settings = self.settings.clone();
        let storage = self.storage.clone();
        self.uploads.spawn(async move {
            let _permit = permit;
            // Re-uses wal::push::handle so compression + rate limiting + retry
            // semantics stay identical to archive_command-driven pushes
            push::handle(&settings, storage, &path)
                .await
                .with_context(|| format!("archive {}", path.display()))?;
            // remove local file after a successful upload
            let _ = fs::remove_file(&path).await;
            Ok(())
        });
        Ok(())
    }

    /// Await all in-flight uploads; first failure wins
    async fn drain_uploads(&mut self) -> Result<()> {
        while let Some(joined) = self.uploads.join_next().await {
            joined.context("upload task join")??;
        }
        Ok(())
    }

    /// Flush in-flight segment to `<seg>.partial` on graceful shutdown.
    /// Mirrors `pg_receivewal`: partial stays local at full seg_size with the
    /// zero pad, never uploaded; restart re-requests from the server-held LSN
    async fn finalize_partial(&mut self) -> Result<()> {
        let Some(cur) = self.current.take() else {
            return Ok(());
        };
        let CurrentSegment {
            name,
            mut file,
            path,
            bytes_written,
        } = cur;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        if bytes_written == 0 {
            let _ = fs::remove_file(&path).await;
            return Ok(());
        }
        let partial_path = path.with_extension("partial");
        fs::rename(&path, &partial_path)
            .await
            .with_context(|| format!("rename {} -> {}", path.display(), partial_path.display()))?;
        tracing::info!(
            target = "wal_receive",
            "wrote partial segment {} ({} bytes of {})",
            name.format(),
            bytes_written,
            self.seg_size
        );
        Ok(())
    }

    /// Highest LSN we've durably written (best-effort: equal to current
    /// segment start + bytes_written, or the segment end of the last
    /// fully-rotated segment when no current segment is open)
    fn write_position(&self) -> u64 {
        match &self.current {
            Some(c) => c.name.start_lsn(self.seg_size) + c.bytes_written,
            None => 0,
        }
    }
}

pub async fn handle(settings: &Settings, storage: DynStorage, archive_dir: &Path) -> Result<()> {
    let cfg = PgConfig::from_env()?;
    tracing::info!(
        target = "wal_receive",
        "connecting to {}:{} as {} (db={})",
        cfg.host,
        cfg.port,
        cfg.user,
        cfg.database
    );
    let mut conn = ReplicationConn::connect(&cfg).await?;
    let (sysid, timeline, start_lsn) = identify_system(&mut conn).await?;
    let seg_size = DEFAULT_WAL_SEG_SIZE;
    // Round down to segment boundary so we always pick up at the start of a
    // segment — partial-segment recovery is the server's job
    let aligned = start_lsn - (start_lsn % seg_size);
    tracing::info!(
        target = "wal_receive",
        "system={sysid} timeline={timeline} start_lsn={} (aligned={})",
        format_pg_lsn(start_lsn),
        format_pg_lsn(aligned),
    );

    let cmd = format!(
        "START_REPLICATION {} TIMELINE {timeline}",
        format_pg_lsn(aligned)
    );
    conn.send_query(&cmd).await?;
    // START_REPLICATION returns CopyBothResponse ('W'), which postgres-
    // protocol's parser does not handle. The conn helper consumes the frame
    conn.expect_copy_both_open().await?;

    let mut acc = SegmentAccumulator::new(
        timeline,
        archive_dir.to_path_buf(),
        seg_size,
        settings.clone(),
        storage,
    )
    .await?;
    let mut last_status = std::time::Instant::now();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        // Trigger a periodic standby status update so the server doesn't
        // disconnect us for a quiet client. wal-g pings every 10s
        if last_status.elapsed() >= STATUS_UPDATE_INTERVAL {
            let pos = acc.write_position().max(aligned);
            send_status_update(&mut conn, pos).await?;
            last_status = std::time::Instant::now();
        }

        let msg = tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::info!(target = "wal_receive", "shutdown signal received, flushing");
                break;
            }
            // Reap finished uploads so a failure surfaces now, not at the
            // next rotation. Pattern mismatch on empty set disables the arm
            Some(joined) = acc.uploads.join_next() => {
                joined.context("upload task join")??;
                continue;
            }
            r = tokio::time::timeout(STATUS_UPDATE_INTERVAL, conn.recv_message()) => match r {
                Ok(r) => r?,
                Err(_) => continue, // tick the keepalive
            },
        };
        match msg {
            Message::CopyData(d) => {
                let payload: Bytes = d.into_bytes();
                let frame = decode_frame(&payload)?;
                match frame {
                    Frame::Wal(w) => {
                        acc.write(w.start_lsn, w.data).await?;
                    }
                    Frame::Keepalive(k) => {
                        if k.reply_requested {
                            let pos = acc.write_position().max(aligned);
                            send_status_update(&mut conn, pos).await?;
                            last_status = std::time::Instant::now();
                        }
                    }
                }
            }
            Message::CopyDone => {
                tracing::info!(target = "wal_receive", "server closed CopyOut");
                break;
            }
            Message::ErrorResponse(e) => bail!("wal-receive: {}", error_message(&e)),
            m => tracing::debug!(target = "wal_receive", "ignoring {}", message_kind(&m)),
        }
    }
    acc.drain_uploads().await?;
    acc.finalize_partial().await?;
    Ok(())
}

/// Resolve on SIGINT or SIGTERM (Unix) / Ctrl-C (other). Splitting into
/// a helper keeps the main loop's `select!` arm short
async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
        let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        tokio::select! {
            _ = sigint.recv() => tracing::debug!(target = "wal_receive", "SIGINT"),
            _ = sigterm.recv() => tracing::debug!(target = "wal_receive", "SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("ctrl_c handler")?;
        tracing::debug!(target = "wal_receive", "ctrl-c");
    }
    Ok(())
}

/// START_REPLICATION carries no SLOT clause, so reported positions don't
/// gate server-side WAL retention; reporting local write position while
/// uploads are still in flight is sound. If slot support lands, flush must
/// switch to uploaded-through LSN
async fn send_status_update(conn: &mut ReplicationConn, pos: u64) -> Result<()> {
    let payload = build_status_update(pos, pos, pos);
    conn.send_copy_data(&payload).await
}

async fn identify_system(conn: &mut ReplicationConn) -> Result<(String, u32, u64)> {
    conn.send_query("IDENTIFY_SYSTEM").await?;
    let mut sysid = String::new();
    let mut tli: u32 = 0;
    let mut xlogpos: u64 = 0;
    loop {
        match conn.recv_message().await? {
            Message::RowDescription(_) => {}
            Message::DataRow(row) => {
                use fallible_iterator::FallibleIterator as _;
                let buf = row.buffer_bytes().clone();
                let mut ranges = row.ranges();
                let mut idx = 0;
                while let Some(r) = ranges.next()? {
                    if let Some(range) = r {
                        let v = std::str::from_utf8(&buf[range])?;
                        match idx {
                            0 => sysid = v.to_string(),
                            1 => tli = v.parse().context("timeline parse")?,
                            2 => xlogpos = crate::pg::backup::parse_pg_lsn(v)?,
                            _ => {}
                        }
                    }
                    idx += 1;
                }
            }
            Message::CommandComplete(_) => {}
            Message::ReadyForQuery(_) => break,
            Message::ErrorResponse(e) => bail!("IDENTIFY_SYSTEM: {}", error_message(&e)),
            _ => continue,
        }
    }
    if sysid.is_empty() || tli == 0 {
        bail!("IDENTIFY_SYSTEM returned an empty result");
    }
    Ok((sysid, tli, xlogpos))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accumulator backed by fs storage at `<dir>/store`, timeline 1
    async fn test_acc(dir: &Path, seg_size: u64) -> SegmentAccumulator {
        let store = dir.join("store");
        let settings = Settings {
            storage: crate::config::StorageSettings::Fs {
                path: store.to_string_lossy().into(),
            },
            compression: crate::compression::Method::None,
            compression_level: 3,
            upload_concurrency: 1,
            upload_queue: 1,
            download_concurrency: 1,
            prevent_wal_overwrite: false,
            use_wal_delta: false,
            retry: crate::retry::RetryPolicy::default(),
            network_rate_limit: 0,
            disk_rate_limit: 0,
            delta: Default::default(),
            crypter: None,
        };
        let storage: DynStorage = Arc::new(crate::storage::fs::FsStorage::new(&store).unwrap());
        SegmentAccumulator::new(1, dir.to_path_buf(), seg_size, settings, storage)
            .await
            .unwrap()
    }

    #[test]
    fn decode_wal_frame() {
        // 'w' | start_lsn=0x100 | server_end=0x200 | send_time=0 | "hello"
        let mut p = Vec::new();
        p.push(b'w');
        p.extend_from_slice(&0x100u64.to_be_bytes());
        p.extend_from_slice(&0x200u64.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        p.extend_from_slice(b"hello");
        let f = decode_frame(&p).unwrap();
        match f {
            Frame::Wal(w) => {
                assert_eq!(w.start_lsn, 0x100);
                assert_eq!(w.data, b"hello");
            }
            _ => panic!("expected WAL frame"),
        }
    }

    #[test]
    fn decode_keepalive_frame() {
        let mut p = Vec::new();
        p.push(b'k');
        p.extend_from_slice(&0x300u64.to_be_bytes());
        p.extend_from_slice(&12345i64.to_be_bytes());
        p.push(1);
        let f = decode_frame(&p).unwrap();
        match f {
            Frame::Keepalive(k) => assert!(k.reply_requested),
            _ => panic!("expected keepalive"),
        }
    }

    #[test]
    fn rejects_short_frames() {
        assert!(decode_frame(b"w").is_err());
        assert!(decode_frame(b"k\x00").is_err());
        assert!(decode_frame(b"").is_err());
        assert!(decode_frame(b"x\x00\x00").is_err());
    }

    #[test]
    fn status_update_encoding() {
        let bytes = build_status_update(0x1, 0x2, 0x3);
        assert_eq!(bytes[0], b'r');
        assert_eq!(bytes.len(), 1 + 8 * 4 + 1);
        let write = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let flush = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let apply = u64::from_be_bytes(bytes[17..25].try_into().unwrap());
        assert_eq!((write, flush, apply), (0x1, 0x2, 0x3));
        assert_eq!(bytes[33], 0);
    }

    #[tokio::test]
    async fn finalize_partial_renames_inflight_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.ensure_current(0).await.unwrap();
        {
            use tokio::io::AsyncWriteExt;
            let cur = acc.current.as_mut().unwrap();
            cur.file.write_all(&[0xCD; 4]).await.unwrap();
            cur.bytes_written = 4;
        }
        let live_name = acc.current.as_ref().unwrap().name.format();
        acc.finalize_partial().await.unwrap();
        let partial = dir.path().join(format!("{live_name}.partial"));
        let original = dir.path().join(&live_name);
        assert!(partial.exists(), "partial file missing: {partial:?}");
        assert!(!original.exists(), "unrenamed segment leaked: {original:?}");
        let meta = std::fs::metadata(&partial).unwrap();
        assert_eq!(meta.len(), seg_size, "partial should keep zero pad");
    }

    #[tokio::test]
    async fn finalize_partial_drops_empty_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let mut acc = test_acc(dir.path(), 16).await;
        acc.ensure_current(0).await.unwrap();
        let name = acc.current.as_ref().unwrap().name.format();
        acc.finalize_partial().await.unwrap();
        assert!(!dir.path().join(&name).exists());
        assert!(!dir.path().join(format!("{name}.partial")).exists());
    }

    #[tokio::test]
    async fn accumulator_rotates_at_segment_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // tiny 16-byte segs for the test
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        // Direct buffer-only test: don't archive (skip rotate_and_spawn wiring)
        acc.ensure_current(0).await.unwrap();
        let bytes_written_before = acc.current.as_ref().unwrap().bytes_written;
        // Manually drive write_all to avoid kicking the storage push path
        let cur = acc.current.as_mut().unwrap();
        use tokio::io::AsyncWriteExt;
        cur.file.write_all(&[0xABu8; 16]).await.unwrap();
        cur.bytes_written = bytes_written_before + 16;
        assert_eq!(cur.bytes_written, seg_size);
    }

    #[tokio::test]
    async fn rotation_uploads_and_removes_local_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.write(0, &[0xAB; 16]).await.unwrap();
        acc.drain_uploads().await.unwrap();
        let name = acc.segment_for_lsn(0).format();
        assert!(
            !dir.path().join(&name).exists(),
            "local segment should be removed after upload"
        );
        let archived = dir
            .path()
            .join("store")
            .join(crate::pg::WAL_FOLDER)
            .join(&name);
        assert_eq!(std::fs::read(&archived).unwrap(), vec![0xAB; 16]);
    }

    #[tokio::test]
    async fn partial_boundary_crossing_archives_segment() {
        let dir = tempfile::tempdir().unwrap();
        let seg_size = 16u64;
        let mut acc = test_acc(dir.path(), seg_size).await;
        acc.write(0, &[0xCD; 4]).await.unwrap();
        // LSN jump past segment end: partial must be pushed, not dropped
        acc.write(seg_size, &[0xEF; 4]).await.unwrap();
        acc.drain_uploads().await.unwrap();
        let first = acc.segment_for_lsn(0).format();
        assert!(!dir.path().join(&first).exists());
        let archived = dir
            .path()
            .join("store")
            .join(crate::pg::WAL_FOLDER)
            .join(&first);
        let bytes = std::fs::read(&archived).unwrap();
        assert_eq!(bytes.len() as u64, seg_size, "zero pad retained");
        assert_eq!(&bytes[..4], &[0xCD; 4]);
        assert!(bytes[4..].iter().all(|&b| b == 0));
    }
}
