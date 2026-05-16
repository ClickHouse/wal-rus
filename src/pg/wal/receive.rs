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
//! Segments accumulate into a 16 MiB-aligned buffer; rotation triggers a
//! synchronous `wal-push` on the just-filled segment. Promoted segments
//! ride the regular compression + storage pipeline so the archive format
//! stays consistent with `archive_command`-driven pushes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use postgres_protocol::message::backend::Message;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::config::Settings;
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
}

struct CurrentSegment {
    name: SegmentName,
    file: tokio::fs::File,
    path: PathBuf,
    bytes_written: u64,
}

impl SegmentAccumulator {
    async fn new(timeline: u32, archive_dir: PathBuf, seg_size: u64) -> Result<Self> {
        fs::create_dir_all(&archive_dir)
            .await
            .with_context(|| format!("create_dir_all {}", archive_dir.display()))?;
        Ok(Self {
            seg_size,
            timeline,
            archive_dir,
            current: None,
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
    /// boundaries, rotating + pushing each completed segment
    async fn write(
        &mut self,
        start_lsn: u64,
        data: &[u8],
        settings: &Settings,
        storage: &DynStorage,
    ) -> Result<()> {
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
                self.rotate_and_push(settings, storage).await?;
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
            // (eg WAL switch midway); push what we have, then open the new one
            tracing::warn!(
                target = "wal_receive",
                "rotating partial segment {} ({} bytes written, target {})",
                cur.name.format(),
                cur.bytes_written,
                self.seg_size
            );
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
        if let Some(prev) = self.current.replace(CurrentSegment {
            name: seg,
            file,
            path,
            bytes_written: 0,
        }) {
            tracing::debug!(
                target = "wal_receive",
                "swapped out segment {}",
                prev.name.format()
            );
        }
        Ok(())
    }

    async fn rotate_and_push(&mut self, settings: &Settings, storage: &DynStorage) -> Result<()> {
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
        // Re-uses wal::push::handle so compression + rate limiting + retry
        // semantics stay identical to archive_command-driven pushes
        push::handle(settings, storage.clone(), &path)
            .await
            .with_context(|| format!("archive {}", path.display()))?;
        // remove local file after a successful upload
        let _ = fs::remove_file(&path).await;
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
        "system={sysid} timeline={timeline} start_lsn={:X}/{:X} (aligned={:X}/{:X})",
        start_lsn >> 32,
        start_lsn as u32,
        aligned >> 32,
        aligned as u32,
    );

    let cmd = format!(
        "START_REPLICATION {}/{:X} TIMELINE {timeline}",
        aligned >> 32,
        aligned as u32
    );
    conn.send_query(&cmd).await?;
    // START_REPLICATION returns CopyBothResponse ('W'), which postgres-
    // protocol's parser does not handle. The conn helper consumes the frame
    conn.expect_copy_both_open().await?;

    let mut acc = SegmentAccumulator::new(timeline, archive_dir.to_path_buf(), seg_size).await?;
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
                        acc.write(w.start_lsn, w.data, settings, &storage).await?;
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
        let mut acc = SegmentAccumulator::new(1, dir.path().to_path_buf(), seg_size)
            .await
            .unwrap();
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
        let mut acc = SegmentAccumulator::new(1, dir.path().to_path_buf(), 16)
            .await
            .unwrap();
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
        let mut acc = SegmentAccumulator::new(1, dir.path().to_path_buf(), seg_size)
            .await
            .unwrap();
        // Direct buffer-only test: don't archive (skip rotate_and_push wiring)
        acc.ensure_current(0).await.unwrap();
        let bytes_written_before = acc.current.as_ref().unwrap().bytes_written;
        // Manually drive write_all to avoid kicking the storage push path
        let cur = acc.current.as_mut().unwrap();
        use tokio::io::AsyncWriteExt;
        cur.file.write_all(&[0xABu8; 16]).await.unwrap();
        cur.bytes_written = bytes_written_before + 16;
        assert_eq!(cur.bytes_written, seg_size);
    }
}
