//! BASE_BACKUP issuer + archive-iteration pump
//!
//! PG14- per-tablespace CopyOut sessions; PG15+ tagged CopyData ('d'/'p'/'n'/'m')
//! within a singleton CopyOut. Mirrors wal-g PR #2262
//!
//! Design: a tokio task owns the connection and drives the protocol forward,
//! emitting BackupEvents over a tokio mpsc channel. The controller spawns
//! per-archive byte channels so the upload pipeline can wrap a `ChannelReader`
//! as `AsyncReader` for `Storage::put`

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use fallible_iterator::FallibleIterator as _;
use postgres_protocol::message::backend::{DataRowBody, Message};
use std::pin::Pin;
use std::task::{Context as TaskCtx, Poll};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::pg::backup::parse_pg_lsn;
use crate::pg::replication::conn::{ReplicationConn, error_message, message_kind};

#[derive(Debug, Clone, Default)]
pub struct BaseBackupOpts {
    pub label: String,
    pub fast_checkpoint: bool,
    pub no_verify_checksums: bool,
    pub max_rate_kib: Option<i32>,
    /// Include WAL segments covering `[start_lsn, end_lsn]` inside the
    /// data-dir archive. PG15+ paren-form: `WAL true`. PG14- positional
    /// form: bare `WAL` keyword (presence ≡ true). With this on, a
    /// downstream standby reaches consistent recovery from the tar
    /// alone, no `restore_command` needed for the bootstrap window
    pub wal: bool,
}

#[derive(Debug, Clone)]
pub struct ArchiveMeta {
    pub name: String,
    pub oid: u32,
    pub path: String,
}

impl ArchiveMeta {
    pub fn is_data_dir(&self) -> bool {
        self.oid == 0
    }
}

#[derive(Debug, Clone)]
pub struct Tablespace {
    pub oid: u32,
    pub location: String,
    pub size: Option<i64>,
}

impl Tablespace {
    /// First row of the BASE_BACKUP tablespace list represents the data dir
    /// itself (NULL oid, NULL path); user tablespaces have non-zero oids
    pub fn is_default(&self) -> bool {
        self.oid == 0
    }
}

#[derive(Debug, Clone)]
pub struct StartInfo {
    pub start_lsn: u64,
    pub timeline: u32,
    pub tablespaces: Vec<Tablespace>,
}

#[derive(Debug, Clone)]
pub struct EndInfo {
    pub end_lsn: u64,
    pub timeline: u32,
}

pub enum BackupEvent {
    Start(StartInfo),
    Archive {
        meta: ArchiveMeta,
        body: mpsc::Receiver<std::io::Result<Bytes>>,
    },
    Finish(EndInfo),
}

/// Channel-fed AsyncRead. Yields each Bytes as a single read; partial reads
/// just hold remaining tail
pub struct ChannelReader {
    rx: mpsc::Receiver<std::io::Result<Bytes>>,
    leftover: Bytes,
    closed: bool,
}

impl ChannelReader {
    pub fn new(rx: mpsc::Receiver<std::io::Result<Bytes>>) -> Self {
        Self {
            rx,
            leftover: Bytes::new(),
            closed: false,
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskCtx<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.closed && self.leftover.is_empty() {
            return Poll::Ready(Ok(()));
        }
        // Loop so an empty Bytes payload (eg, an empty CopyData frame) doesn't
        // get mistaken for EOF by the downstream reader. AsyncRead semantics:
        // Ready(Ok) with zero filled bytes = EOF; the caller can never receive
        // an empty buf-write unless the producer is truly done
        while self.leftover.is_empty() {
            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.closed = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(Some(Ok(b))) => self.leftover = b,
            }
        }
        let n = std::cmp::min(self.leftover.len(), buf.remaining());
        if n == 0 {
            // buf had no room; do not signal EOF, just yield with no progress
            // and wake when the caller calls again
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let chunk = self.leftover.split_to(n);
        buf.put_slice(&chunk);
        Poll::Ready(Ok(()))
    }
}

/// Drive a BASE_BACKUP session, emitting events on `events`.
/// Returns when the session is fully drained or an error occurs
pub async fn run_base_backup(
    mut conn: ReplicationConn,
    opts: BaseBackupOpts,
    events: mpsc::Sender<Result<BackupEvent>>,
) {
    let res = run_inner(&mut conn, opts, &events).await;
    if let Err(e) = res {
        let _ = events.send(Err(e)).await;
    }
}

async fn run_inner(
    conn: &mut ReplicationConn,
    opts: BaseBackupOpts,
    events: &mpsc::Sender<Result<BackupEvent>>,
) -> Result<()> {
    let pg_version = conn.server_pg_version();
    let cmd = build_base_backup_sql(&opts, pg_version);
    tracing::debug!(target = "base_backup", "issuing: {cmd}");
    conn.send_query(&cmd).await?;

    let (start_lsn, timeline) = read_lsn_row(conn).await.context("read start info")?;
    let tablespaces = read_tablespaces(conn).await.context("read tablespaces")?;
    let start = StartInfo {
        start_lsn,
        timeline,
        tablespaces: tablespaces.clone(),
    };
    if events.send(Ok(BackupEvent::Start(start))).await.is_err() {
        return Ok(());
    }

    if pg_version >= 150000 {
        stream_archives_v15(conn, events, &tablespaces).await?;
    } else {
        stream_archives_compat(conn, events, &tablespaces).await?;
    }

    let (end_lsn, end_timeline) = read_lsn_row(conn).await.context("read end info")?;
    expect_command_complete(conn).await?;
    expect_ready_for_query(conn).await?;

    let _ = events
        .send(Ok(BackupEvent::Finish(EndInfo {
            end_lsn,
            timeline: end_timeline,
        })))
        .await;
    Ok(())
}

fn build_base_backup_sql(opts: &BaseBackupOpts, pg_version: i32) -> String {
    let label = quote_pg_str(&opts.label);
    if pg_version >= 150000 {
        let mut parts: Vec<String> = vec![format!("LABEL {label}")];
        parts.push(format!(
            "CHECKPOINT '{}'",
            if opts.fast_checkpoint {
                "fast"
            } else {
                "spread"
            }
        ));
        parts.push(format!("WAL {}", opts.wal));
        parts.push("TABLESPACE_MAP true".into());
        parts.push("MANIFEST 'no'".into());
        if opts.no_verify_checksums {
            parts.push("VERIFY_CHECKSUMS false".into());
        }
        if let Some(rate) = opts.max_rate_kib {
            parts.push(format!("MAX_RATE {rate}"));
        }
        format!("BASE_BACKUP ({})", parts.join(", "))
    } else {
        let mut parts: Vec<String> = vec![format!("LABEL {label}")];
        if opts.fast_checkpoint {
            parts.push("FAST".into());
        }
        if opts.wal {
            parts.push("WAL".into());
        }
        parts.push("TABLESPACE_MAP".into());
        if opts.no_verify_checksums {
            parts.push("NOVERIFY_CHECKSUMS".into());
        }
        if let Some(rate) = opts.max_rate_kib {
            parts.push(format!("MAX_RATE {rate}"));
        }
        format!("BASE_BACKUP {}", parts.join(" "))
    }
}

fn quote_pg_str(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

async fn read_lsn_row(conn: &mut ReplicationConn) -> Result<(u64, u32)> {
    let mut start_lsn: Option<u64> = None;
    let mut timeline: Option<u32> = None;
    loop {
        match conn.recv_message().await? {
            Message::RowDescription(_) => {}
            Message::DataRow(row) => {
                let cols = data_row_cols(&row)?;
                if cols.len() != 2 {
                    bail!("expected 2 cols for LSN row, got {}", cols.len());
                }
                let lsn_text = utf8_or_err(&cols[0])?;
                start_lsn = Some(parse_pg_lsn(lsn_text)?);
                let tli_text = utf8_or_err(&cols[1])?;
                timeline = Some(tli_text.parse().context("tli is not an int")?);
            }
            Message::CommandComplete(_) => break,
            Message::ErrorResponse(e) => bail!("read_lsn_row: {}", error_message(&e)),
            m => bail!("read_lsn_row: unexpected message {:?}", message_kind(&m)),
        }
    }
    Ok((
        start_lsn.ok_or_else(|| anyhow!("no LSN row received"))?,
        timeline.ok_or_else(|| anyhow!("no timeline received"))?,
    ))
}

async fn read_tablespaces(conn: &mut ReplicationConn) -> Result<Vec<Tablespace>> {
    let mut out = Vec::new();
    loop {
        match conn.recv_message().await? {
            Message::RowDescription(_) => {}
            Message::DataRow(row) => {
                let cols = data_row_cols(&row)?;
                if cols.len() < 3 {
                    bail!("tablespace row needs 3 cols, got {}", cols.len());
                }
                let oid: u32 = match &cols[0] {
                    Some(b) => utf8_or_err(&Some(b.clone()))?
                        .parse()
                        .context("tablespace oid")?,
                    None => 0,
                };
                let location = match &cols[1] {
                    Some(b) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                    None => String::new(),
                };
                let size: Option<i64> = match &cols[2] {
                    Some(b) => Some(
                        std::str::from_utf8(b)
                            .context("tablespace size utf8")?
                            .parse()
                            .context("tablespace size int")?,
                    ),
                    None => None,
                };
                out.push(Tablespace {
                    oid,
                    location,
                    size,
                });
            }
            Message::CommandComplete(_) => break,
            Message::ErrorResponse(e) => bail!("read_tablespaces: {}", error_message(&e)),
            m => bail!(
                "read_tablespaces: unexpected message {:?}",
                message_kind(&m)
            ),
        }
    }
    Ok(out)
}

fn data_row_cols(row: &DataRowBody) -> Result<Vec<Option<Bytes>>> {
    let buf = row.buffer_bytes();
    let mut ranges = row.ranges();
    let mut out: Vec<Option<Bytes>> = Vec::new();
    while let Some(range) = ranges.next()? {
        match range {
            Some(r) => out.push(Some(buf.slice(r))),
            None => out.push(None),
        }
    }
    Ok(out)
}

fn utf8_or_err(b: &Option<Bytes>) -> Result<&str> {
    match b {
        Some(b) => std::str::from_utf8(b).context("non-utf8 column"),
        None => bail!("unexpected null column"),
    }
}

async fn stream_archives_compat(
    conn: &mut ReplicationConn,
    events: &mpsc::Sender<Result<BackupEvent>>,
    tablespaces: &[Tablespace],
) -> Result<()> {
    // PG14-: per-tablespace CopyOut, one per row in the tablespace result set.
    // PG sends the data dir first (NULL oid row -> base.tar), then user
    // tablespaces in result-set order. We mirror that order exactly so the
    // CopyOut framing matches what we expect to read
    for ts in tablespaces {
        let meta = if ts.is_default() {
            ArchiveMeta {
                name: "base.tar".into(),
                oid: 0,
                path: String::new(),
            }
        } else {
            ArchiveMeta {
                name: format!("{}.tar", ts.oid),
                oid: ts.oid,
                path: ts.location.clone(),
            }
        };
        emit_compat_archive(conn, events, meta).await?;
    }
    Ok(())
}

async fn emit_compat_archive(
    conn: &mut ReplicationConn,
    events: &mpsc::Sender<Result<BackupEvent>>,
    meta: ArchiveMeta,
) -> Result<()> {
    // expect CopyOutResponse before any CopyData
    match conn.recv_message().await? {
        Message::CopyOutResponse(_) => {}
        Message::ErrorResponse(e) => bail!("compat copy-out: {}", error_message(&e)),
        m => bail!("compat copy-out: unexpected message {:?}", message_kind(&m)),
    }
    let (tx, rx) = mpsc::channel::<std::io::Result<Bytes>>(8);
    if events
        .send(Ok(BackupEvent::Archive { meta, body: rx }))
        .await
        .is_err()
    {
        return Ok(());
    }
    loop {
        match conn.recv_message().await? {
            Message::CopyData(d) => {
                if tx.send(Ok(d.into_bytes())).await.is_err() {
                    drop(tx);
                    return drain_until_copy_done(conn).await;
                }
            }
            Message::CopyDone => return Ok(()),
            Message::ErrorResponse(e) => {
                let _ = tx.send(Err(std::io::Error::other(error_message(&e)))).await;
                bail!("compat copy: server error");
            }
            m => bail!("compat copy: unexpected message {:?}", message_kind(&m)),
        }
    }
}

async fn drain_until_copy_done(conn: &mut ReplicationConn) -> Result<()> {
    loop {
        match conn.recv_message().await? {
            Message::CopyData(_) => continue,
            Message::CopyDone => return Ok(()),
            Message::ErrorResponse(e) => bail!("drain: {}", error_message(&e)),
            m => bail!("drain: unexpected message {:?}", message_kind(&m)),
        }
    }
}

async fn stream_archives_v15(
    conn: &mut ReplicationConn,
    events: &mpsc::Sender<Result<BackupEvent>>,
    tablespaces: &[Tablespace],
) -> Result<()> {
    // singleton CopyOutResponse opens; tagged CopyData inside until CopyDone
    match conn.recv_message().await? {
        Message::CopyOutResponse(_) => {}
        Message::ErrorResponse(e) => bail!("v15 copy-out open: {}", error_message(&e)),
        m => bail!(
            "v15 copy-out open: unexpected message {:?}",
            message_kind(&m)
        ),
    }

    let mut current_tx: Option<mpsc::Sender<std::io::Result<Bytes>>> = None;
    let mut in_manifest = false;

    loop {
        match conn.recv_message().await? {
            Message::CopyData(d) => {
                let bytes = d.into_bytes();
                if bytes.is_empty() {
                    bail!("v15 copy: empty CopyData payload");
                }
                let tag = bytes[0];
                let body = bytes.slice(1..);
                match tag {
                    b'd' => {
                        if in_manifest {
                            continue;
                        }
                        if let Some(tx) = current_tx.as_ref()
                            && tx.send(Ok(body)).await.is_err()
                        {
                            current_tx = None;
                        }
                    }
                    b'p' => {} // progress counter, ignored
                    b'n' => {
                        if in_manifest {
                            bail!("v15 copy: 'n' tag inside manifest stream");
                        }
                        // dropping prior tx (if any) signals EOF on prior archive reader
                        let (name, path) = parse_archive_header(&body)?;
                        let meta = make_archive(tablespaces, name, path)?;
                        let (tx, rx) = mpsc::channel::<std::io::Result<Bytes>>(8);
                        current_tx = Some(tx);
                        if events
                            .send(Ok(BackupEvent::Archive { meta, body: rx }))
                            .await
                            .is_err()
                        {
                            // consumer dropped; drain stream until CopyDone
                            current_tx = None;
                        }
                    }
                    b'm' => {
                        // manifest stream - we did not request it; swallow
                        tracing::warn!(
                            target = "base_backup",
                            "manifest stream received unexpectedly; ignoring"
                        );
                        in_manifest = true;
                        let _ = current_tx.take();
                    }
                    other => bail!("v15 copy: unexpected CopyData tag {:?}", other as char),
                }
            }
            Message::CopyDone => return Ok(()),
            Message::ErrorResponse(e) => bail!("v15 copy: {}", error_message(&e)),
            m => bail!("v15 copy: unexpected message {:?}", message_kind(&m)),
        }
    }
}

pub fn parse_archive_header(body: &[u8]) -> Result<(String, String)> {
    let name_end = body
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| anyhow!("archive header missing NUL after name"))?;
    let rest = &body[name_end + 1..];
    let path_end = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| anyhow!("archive header missing NUL after path"))?;
    let name = std::str::from_utf8(&body[..name_end])
        .context("archive name utf8")?
        .to_string();
    let path = std::str::from_utf8(&rest[..path_end])
        .context("archive path utf8")?
        .to_string();
    Ok((name, path))
}

pub fn make_archive(tablespaces: &[Tablespace], name: String, path: String) -> Result<ArchiveMeta> {
    if name == "base.tar" {
        return Ok(ArchiveMeta { name, oid: 0, path });
    }
    let oid_str = name
        .strip_suffix(".tar")
        .ok_or_else(|| anyhow!("unrecognized archive name: {name}"))?;
    let oid: u32 = oid_str
        .parse()
        .with_context(|| format!("non-numeric OID in archive name: {name}"))?;
    if !tablespaces.iter().any(|t| t.oid == oid) {
        bail!("archive {name} for unknown tablespace OID {oid}");
    }
    Ok(ArchiveMeta { name, oid, path })
}

async fn expect_command_complete(conn: &mut ReplicationConn) -> Result<()> {
    match conn.recv_message().await? {
        Message::CommandComplete(_) => Ok(()),
        Message::ErrorResponse(e) => bail!("expect CommandComplete: {}", error_message(&e)),
        m => bail!("expected CommandComplete, got {}", message_kind(&m)),
    }
}

async fn expect_ready_for_query(conn: &mut ReplicationConn) -> Result<()> {
    match conn.recv_message().await? {
        Message::ReadyForQuery(_) => Ok(()),
        Message::ErrorResponse(e) => bail!("expect ReadyForQuery: {}", error_message(&e)),
        m => bail!("expected ReadyForQuery, got {}", message_kind(&m)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _ReadExt;

    /// Regression: PG can send empty CopyData frames (eg sparse-file padding
    /// boundaries); the channel-fed reader must not treat them as EOF.
    /// Bug discovered on PG 13 BASE_BACKUP where a 1.5 KB tar replaced what
    /// should have been a 50 MB data dir
    #[tokio::test]
    async fn channel_reader_skips_empty_payloads() {
        let (tx, rx) = mpsc::channel::<std::io::Result<Bytes>>(16);
        let mut reader = ChannelReader::new(rx);

        // pump: real, empty, real, real, close
        tokio::spawn(async move {
            tx.send(Ok(Bytes::from_static(b"hello "))).await.unwrap();
            tx.send(Ok(Bytes::new())).await.unwrap();
            tx.send(Ok(Bytes::from_static(b"world"))).await.unwrap();
            tx.send(Ok(Bytes::from_static(b"!"))).await.unwrap();
            // drop tx -> EOF
        });

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(&out, b"hello world!");
    }

    #[test]
    fn parses_archive_header_data_dir() {
        // base.tar\0\0
        let body = b"base.tar\0\0";
        let (n, p) = parse_archive_header(body).unwrap();
        assert_eq!(n, "base.tar");
        assert_eq!(p, "");
    }

    #[test]
    fn parses_archive_header_with_path() {
        let mut body = Vec::new();
        body.extend_from_slice(b"16384.tar");
        body.push(0);
        body.extend_from_slice(b"/var/lib/pg/ts1");
        body.push(0);
        let (n, p) = parse_archive_header(&body).unwrap();
        assert_eq!(n, "16384.tar");
        assert_eq!(p, "/var/lib/pg/ts1");
    }

    #[test]
    fn rejects_archive_header_missing_path_nul() {
        let mut body = Vec::new();
        body.extend_from_slice(b"base.tar");
        body.push(0);
        body.extend_from_slice(b"/path-no-nul");
        assert!(parse_archive_header(&body).is_err());
    }

    #[test]
    fn rejects_archive_header_missing_name_nul() {
        assert!(parse_archive_header(b"base").is_err());
    }

    #[test]
    fn make_archive_data_dir() {
        let a = make_archive(&[], "base.tar".into(), String::new()).unwrap();
        assert_eq!(a.name, "base.tar");
        assert_eq!(a.oid, 0);
        assert!(a.is_data_dir());
    }

    #[test]
    fn make_archive_known_tablespace() {
        let ts = vec![Tablespace {
            oid: 16384,
            location: "/loc".into(),
            size: None,
        }];
        let a = make_archive(&ts, "16384.tar".into(), "/path".into()).unwrap();
        assert_eq!(a.oid, 16384);
        assert!(!a.is_data_dir());
    }

    #[test]
    fn make_archive_rejects_unknown_oid() {
        let ts: Vec<Tablespace> = vec![];
        assert!(make_archive(&ts, "99999.tar".into(), String::new()).is_err());
    }

    #[test]
    fn make_archive_rejects_bad_name() {
        assert!(make_archive(&[], "manifest.json".into(), String::new()).is_err());
        assert!(make_archive(&[], "xyz.tar".into(), String::new()).is_err());
    }

    #[test]
    fn build_sql_v15_paren_form() {
        let opts = BaseBackupOpts {
            label: "wal-rs".into(),
            fast_checkpoint: true,
            no_verify_checksums: false,
            max_rate_kib: None,
            wal: false,
        };
        let s = build_base_backup_sql(&opts, 150000);
        assert!(s.starts_with("BASE_BACKUP ("));
        assert!(s.contains("LABEL 'wal-rs'"));
        assert!(s.contains("CHECKPOINT 'fast'"));
        assert!(s.contains("WAL false"));
        assert!(s.contains("MANIFEST 'no'"));
        assert!(!s.contains("VERIFY_CHECKSUMS"));
    }

    /// PG15+ `WAL true` flips the option from default-false to inlining
    /// WAL segments in the data-dir tar. Server parses via `defGetBoolean`
    /// so the lowercase literal is required
    #[test]
    fn build_sql_v15_wal_true() {
        let opts = BaseBackupOpts {
            label: "wal-rs".into(),
            fast_checkpoint: true,
            no_verify_checksums: false,
            max_rate_kib: None,
            wal: true,
        };
        let s = build_base_backup_sql(&opts, 150000);
        assert!(s.contains("WAL true"));
        assert!(!s.contains("WAL false"));
    }

    #[test]
    fn build_sql_compat_form() {
        let opts = BaseBackupOpts {
            label: "wal-rs".into(),
            fast_checkpoint: true,
            no_verify_checksums: true,
            max_rate_kib: Some(8192),
            wal: false,
        };
        let s = build_base_backup_sql(&opts, 140005);
        assert!(s.starts_with("BASE_BACKUP "));
        assert!(!s.contains("("));
        assert!(s.contains("LABEL 'wal-rs'"));
        assert!(s.contains("FAST"));
        assert!(s.contains("TABLESPACE_MAP"));
        assert!(s.contains("NOVERIFY_CHECKSUMS"));
        assert!(s.contains("MAX_RATE 8192"));
        // bare `WAL` keyword omitted when off (PG12-14 grammar: presence ≡ true)
        assert!(!s.split_whitespace().any(|tok| tok == "WAL"));
    }

    /// PG14- positional form has no `WAL false` spelling — keyword's
    /// presence alone toggles inline-WAL on
    #[test]
    fn build_sql_compat_wal_true() {
        let opts = BaseBackupOpts {
            label: "wal-rs".into(),
            fast_checkpoint: false,
            no_verify_checksums: false,
            max_rate_kib: None,
            wal: true,
        };
        let s = build_base_backup_sql(&opts, 140005);
        assert!(s.starts_with("BASE_BACKUP "));
        assert!(s.split_whitespace().any(|tok| tok == "WAL"));
    }

    #[test]
    fn quotes_label_with_apostrophe() {
        assert_eq!(quote_pg_str("it's"), "'it''s'");
    }

    use bytes::{BufMut, BytesMut};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    use crate::pg::replication::conn::ReplicationConn;

    fn write_msg(buf: &mut BytesMut, tag: u8, body: &[u8]) {
        buf.put_u8(tag);
        buf.put_i32(4 + body.len() as i32);
        buf.put_slice(body);
    }

    fn write_cstr(buf: &mut BytesMut, s: &str) {
        buf.put_slice(s.as_bytes());
        buf.put_u8(0);
    }

    fn fake_row_description(buf: &mut BytesMut, fields: &[&str]) {
        let mut body = BytesMut::new();
        body.put_i16(fields.len() as i16);
        for f in fields {
            write_cstr(&mut body, f);
            body.put_i32(0);
            body.put_i16(0);
            body.put_i32(25);
            body.put_i16(-1);
            body.put_i32(-1);
            body.put_i16(0);
        }
        write_msg(buf, b'T', &body);
    }

    fn fake_data_row(buf: &mut BytesMut, cols: &[&[u8]]) {
        let mut body = BytesMut::new();
        body.put_i16(cols.len() as i16);
        for c in cols {
            body.put_i32(c.len() as i32);
            body.put_slice(c);
        }
        write_msg(buf, b'D', &body);
    }

    fn fake_data_row_nulls(buf: &mut BytesMut, ncols: u16) {
        let mut body = BytesMut::new();
        body.put_i16(ncols as i16);
        for _ in 0..ncols {
            body.put_i32(-1);
        }
        write_msg(buf, b'D', &body);
    }

    fn fake_command_complete(buf: &mut BytesMut, tag: &str) {
        let mut body = BytesMut::new();
        write_cstr(&mut body, tag);
        write_msg(buf, b'C', &body);
    }

    fn fake_ready_for_query(buf: &mut BytesMut) {
        write_msg(buf, b'Z', b"I");
    }

    fn fake_copy_out_response(buf: &mut BytesMut, ncols: u16) {
        let mut body = BytesMut::new();
        body.put_u8(0);
        body.put_i16(ncols as i16);
        for _ in 0..ncols {
            body.put_i16(0);
        }
        write_msg(buf, b'H', &body);
    }

    fn fake_copy_done(buf: &mut BytesMut) {
        write_msg(buf, b'c', &[]);
    }

    fn fake_copy_data(buf: &mut BytesMut, payload: &[u8]) {
        write_msg(buf, b'd', payload);
    }

    fn fake_archive_header(name: &str, path: &str) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(b'n');
        v.extend_from_slice(name.as_bytes());
        v.push(0);
        v.extend_from_slice(path.as_bytes());
        v.push(0);
        v
    }

    /// Drives a scripted PG15+ BASE_BACKUP response and verifies our pump emits
    /// the expected archives + finish info
    #[tokio::test]
    async fn v15_pump_streams_data_dir() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = BytesMut::new();

            fake_row_description(&mut buf, &["recptr", "tli"]);
            fake_data_row(&mut buf, &[b"0/3000000", b"1"]);
            fake_command_complete(&mut buf, "BASE_BACKUP");

            fake_row_description(&mut buf, &["spcoid", "spclocation", "size"]);
            fake_command_complete(&mut buf, "BASE_BACKUP");

            fake_copy_out_response(&mut buf, 0);

            let header = fake_archive_header("base.tar", "");
            fake_copy_data(&mut buf, &header);

            let mut chunk1 = vec![b'd'];
            chunk1.extend_from_slice(b"hello-tar-bytes-1");
            fake_copy_data(&mut buf, &chunk1);

            let mut chunk2 = vec![b'd'];
            chunk2.extend_from_slice(b"and-more-2");
            fake_copy_data(&mut buf, &chunk2);

            fake_copy_done(&mut buf);

            fake_row_description(&mut buf, &["recptr", "tli"]);
            fake_data_row(&mut buf, &[b"0/3001000", b"1"]);
            fake_command_complete(&mut buf, "BASE_BACKUP");

            fake_command_complete(&mut buf, "BASE_BACKUP");
            fake_ready_for_query(&mut buf);

            sock.write_all(&buf).await.unwrap();

            let _ = tokio::time::timeout(Duration::from_millis(500), async {
                let mut tmp = [0u8; 4096];
                let _ = sock.read(&mut tmp).await;
            })
            .await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let conn = ReplicationConn::from_test_socket(client, 160003);

        let (tx, mut rx) = mpsc::channel::<anyhow::Result<BackupEvent>>(2);
        let opts = BaseBackupOpts {
            label: "test".into(),
            fast_checkpoint: true,
            no_verify_checksums: false,
            max_rate_kib: None,
            wal: false,
        };
        let pump = tokio::spawn(run_base_backup(conn, opts, tx));

        let mut start_seen = false;
        let mut archives: Vec<(String, Vec<u8>)> = Vec::new();
        let mut finish_lsn: Option<u64> = None;
        while let Some(evt) = rx.recv().await {
            match evt.unwrap() {
                BackupEvent::Start(info) => {
                    assert_eq!(info.start_lsn, 0x0300_0000);
                    assert_eq!(info.timeline, 1);
                    assert!(info.tablespaces.is_empty());
                    start_seen = true;
                }
                BackupEvent::Archive { meta, mut body } => {
                    let mut data = Vec::new();
                    while let Some(chunk) = body.recv().await {
                        data.extend_from_slice(&chunk.unwrap());
                    }
                    archives.push((meta.name, data));
                }
                BackupEvent::Finish(info) => {
                    finish_lsn = Some(info.end_lsn);
                }
            }
        }
        pump.await.unwrap();

        assert!(start_seen, "missing Start event");
        assert_eq!(archives.len(), 1);
        assert_eq!(archives[0].0, "base.tar");
        assert_eq!(archives[0].1, b"hello-tar-bytes-1and-more-2");
        assert_eq!(finish_lsn, Some(0x0300_1000));
    }

    /// PG14- compat path: per-tablespace CopyOuts in the order PG sends them —
    /// one user tablespace first, then the default (data dir / base.tar) last.
    /// Confirmed against PG13/14 source (basebackup.c: tablespaces list
    /// appends the default tablespace at the tail before iterating)
    #[tokio::test]
    async fn compat_pump_orders_tablespace_then_base() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = BytesMut::new();

            fake_row_description(&mut buf, &["recptr", "tli"]);
            fake_data_row(&mut buf, &[b"0/4000000", b"2"]);
            fake_command_complete(&mut buf, "BASE_BACKUP");

            // tablespace list: one user TS, then the default (NULL oid / NULL location)
            fake_row_description(&mut buf, &["spcoid", "spclocation", "size"]);
            fake_data_row(&mut buf, &[b"16384", b"/var/lib/pg/ts1", b"4096"]);
            fake_data_row_nulls(&mut buf, 3);
            fake_command_complete(&mut buf, "BASE_BACKUP");

            // CopyOuts in same order: user TS, then base
            fake_copy_out_response(&mut buf, 0);
            fake_copy_data(&mut buf, b"tablespace-bytes");
            fake_copy_done(&mut buf);

            fake_copy_out_response(&mut buf, 0);
            fake_copy_data(&mut buf, b"basetar-bytes");
            fake_copy_done(&mut buf);

            fake_row_description(&mut buf, &["recptr", "tli"]);
            fake_data_row(&mut buf, &[b"0/4000100", b"2"]);
            fake_command_complete(&mut buf, "BASE_BACKUP");

            fake_command_complete(&mut buf, "BASE_BACKUP");
            fake_ready_for_query(&mut buf);

            sock.write_all(&buf).await.unwrap();
            let _ = tokio::time::timeout(Duration::from_millis(500), async {
                let mut tmp = [0u8; 4096];
                let _ = sock.read(&mut tmp).await;
            })
            .await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let conn = ReplicationConn::from_test_socket(client, 140005);

        let (tx, mut rx) = mpsc::channel::<anyhow::Result<BackupEvent>>(2);
        let opts = BaseBackupOpts::default();
        let pump = tokio::spawn(run_base_backup(conn, opts, tx));

        let mut archives: Vec<(String, Vec<u8>)> = Vec::new();
        let mut finish: Option<u64> = None;
        while let Some(evt) = rx.recv().await {
            match evt.unwrap() {
                BackupEvent::Start(info) => {
                    assert_eq!(info.tablespaces.len(), 2);
                    assert_eq!(info.tablespaces[0].oid, 16384);
                    assert_eq!(info.tablespaces[1].oid, 0, "default tablespace last");
                }
                BackupEvent::Archive { meta, mut body } => {
                    let mut data = Vec::new();
                    while let Some(chunk) = body.recv().await {
                        data.extend_from_slice(&chunk.unwrap());
                    }
                    archives.push((meta.name, data));
                }
                BackupEvent::Finish(info) => finish = Some(info.end_lsn),
            }
        }
        pump.await.unwrap();

        assert_eq!(archives.len(), 2);
        assert_eq!(archives[0].0, "16384.tar");
        assert_eq!(archives[0].1, b"tablespace-bytes");
        assert_eq!(archives[1].0, "base.tar");
        assert_eq!(archives[1].1, b"basetar-bytes");
        assert_eq!(finish, Some(0x0400_0100));
    }
}
