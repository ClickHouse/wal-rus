//! Tar encoding & decoding for backup parts. Decoding uses tar-framing's
//! logical reader (ustar/pax or GNU streams). Encoding writes GNU headers with
//! `L`/`K` long name extensions, covering regular files, dirs & symlinks, the
//! subset PG basebackup and walrus produce

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tar_framing::header::{
    CHECKSUM_RANGE, GID_RANGE, GNU_IDENTITY, IDENTITY_RANGE, LINK_NAME_RANGE, MODE_RANGE,
    MTIME_RANGE, NAME_RANGE, SIZE_RANGE, TYPEFLAG_OFFSET, UID_RANGE,
};
use tar_framing::logical::{MemberPayload, TarReader};
use tar_framing::write::{end_marker_bytes, payload_padding};
use tar_framing::{BLOCK_SIZE, FrameError, FrameErrorInner, UstarKind};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// Body bytes requested per tar-framing read
const CHUNK_BYTES: usize = 256 * 1024;
const GNU_LONG_NAME: &[u8] = b"././@LongLink";

/// Member kinds walrus acts on, owned here so tar-framing bumps stay out of
/// walrus's public API
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Regular,
    HardLink,
    SymbolicLink,
    Directory,
    /// Devices, FIFOs & contiguous files, skipped on fetch
    Other,
}

impl From<UstarKind> for Kind {
    fn from(k: UstarKind) -> Self {
        match k {
            UstarKind::Regular => Kind::Regular,
            UstarKind::HardLink => Kind::HardLink,
            UstarKind::SymbolicLink => Kind::SymbolicLink,
            UstarKind::Directory => Kind::Directory,
            UstarKind::CharacterDevice
            | UstarKind::BlockDevice
            | UstarKind::Fifo
            | UstarKind::Contiguous => Kind::Other,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub path: String,
    pub kind: Kind,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: u64,
    /// Symlink / hard link target, empty for other kinds
    pub link: String,
}

impl Entry {
    pub fn new(path: impl Into<String>, kind: Kind, size: u64, mode: u32, mtime: u64) -> Self {
        Self {
            path: path.into(),
            kind,
            size,
            mode,
            uid: 0,
            gid: 0,
            mtime,
            link: String::new(),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.kind == Kind::Directory
    }
}

fn io_err(e: FrameError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Writes octal with a NUL terminator, or GNU base-256 when octal overflows
fn put_num(field: &mut [u8], v: u64) {
    let digits = field.len() - 1;
    if v < 1 << (digits * 3) {
        field[..digits].copy_from_slice(format!("{v:0digits$o}").as_bytes());
        field[digits] = 0;
    } else {
        field.fill(0);
        field[0] = 0x80;
        let be = v.to_be_bytes();
        let n = be.len().min(field.len() - 1);
        let len = field.len();
        field[len - n..].copy_from_slice(&be[be.len() - n..]);
    }
}

/// Copies up to the field width; longer values travel in a preceding `L`/`K`
fn put_str(field: &mut [u8], v: &[u8]) {
    let n = v.len().min(field.len());
    field[..n].copy_from_slice(&v[..n]);
}

#[allow(clippy::too_many_arguments)]
fn gnu_header(
    name: &[u8],
    link: &[u8],
    typeflag: u8,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: u64,
) -> [u8; BLOCK_SIZE] {
    let mut h = [0u8; BLOCK_SIZE];
    put_str(&mut h[NAME_RANGE], name);
    put_num(&mut h[MODE_RANGE], mode.into());
    put_num(&mut h[UID_RANGE], uid.into());
    put_num(&mut h[GID_RANGE], gid.into());
    put_num(&mut h[SIZE_RANGE], size);
    put_num(&mut h[MTIME_RANGE], mtime);
    h[TYPEFLAG_OFFSET] = typeflag;
    put_str(&mut h[LINK_NAME_RANGE], link);
    h[IDENTITY_RANGE].copy_from_slice(GNU_IDENTITY);
    h[CHECKSUM_RANGE].fill(b' ');
    let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
    h[CHECKSUM_RANGE][..7].copy_from_slice(format!("{sum:06o}\0").as_bytes());
    h
}

fn typeflag(kind: Kind) -> io::Result<u8> {
    Ok(match kind {
        Kind::Regular => b'0',
        Kind::HardLink => b'1',
        Kind::SymbolicLink => b'2',
        Kind::Directory => b'5',
        Kind::Other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported tar member kind",
            ));
        }
    })
}

pub struct Writer<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> Writer<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    /// Appends `e` with exactly `e.size` body bytes from `body`
    pub async fn append<B: AsyncBufRead + Unpin>(&mut self, e: &Entry, body: B) -> io::Result<()> {
        let flag = typeflag(e.kind)?;
        let (name, link) = (e.path.as_bytes(), e.link.as_bytes());
        if name.len() > NAME_RANGE.len() {
            self.long_name(b'L', name).await?;
        }
        if link.len() > LINK_NAME_RANGE.len() {
            self.long_name(b'K', link).await?;
        }
        let h = gnu_header(name, link, flag, e.size, e.mode, e.uid, e.gid, e.mtime);
        self.inner.write_all(&h).await?;
        let copied = tokio::io::copy_buf(&mut body.take(e.size), &mut self.inner).await?;
        if copied != e.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{}: body {copied} bytes, header {}", e.path, e.size),
            ));
        }
        self.inner.write_all(payload_padding(e.size)).await
    }

    async fn long_name(&mut self, flag: u8, value: &[u8]) -> io::Result<()> {
        let size = value.len() as u64 + 1;
        let h = gnu_header(GNU_LONG_NAME, b"", flag, size, 0o644, 0, 0, 0);
        self.inner.write_all(&h).await?;
        self.inner.write_all(value).await?;
        self.inner.write_all(&[0]).await?;
        self.inner.write_all(payload_padding(size)).await
    }

    /// Writes the end-of-archive marker and returns the sink unflushed
    pub async fn finish(mut self) -> io::Result<W> {
        self.inner.write_all(end_marker_bytes()).await?;
        Ok(self.inner)
    }
}

pub struct Reader<R> {
    inner: TarReader<R>,
}

impl<R: AsyncRead + Unpin + Send> Reader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: TarReader::new(inner),
        }
    }

    /// Next member and its body. Unread body bytes are skipped on the next call
    pub async fn next(&mut self) -> io::Result<Option<(Entry, Body<'_, R>)>> {
        let frame = match self.inner.next_frame().await {
            Ok(Some(frame)) => frame,
            // PG 14 & older BASE_BACKUP omit end marker, pg_basebackup appends it
            Ok(None)
            | Err(FrameError {
                inner: FrameErrorInner::MissingEndMarker,
                ..
            }) => return Ok(None),
            Err(e) => return Err(io_err(e)),
        };
        let h = frame.header;
        let kind = Kind::from(h.kind);
        let path = String::from_utf8_lossy(&frame.effective_path().map_err(io_err)?).into_owned();
        let link = match kind {
            Kind::SymbolicLink | Kind::HardLink => {
                String::from_utf8_lossy(&frame.effective_link_path().map_err(io_err)?).into_owned()
            }
            _ => String::new(),
        };
        let default_mode = if kind == Kind::Directory {
            0o700
        } else {
            0o600
        };
        let entry = Entry {
            path,
            kind,
            size: h.effective_size,
            mode: h.mode.map_or(default_mode, |m| (m & 0o7777) as u32),
            uid: h.uid.unwrap_or(0) as u32,
            gid: h.gid.unwrap_or(0) as u32,
            mtime: h.mtime.unwrap_or(0),
            link,
        };
        Ok(Some((entry, Body::new(frame.payload))))
    }
}

type ChunkFuture<'a, R> =
    Pin<Box<dyn Future<Output = (MemberPayload<'a, R>, Vec<u8>, io::Result<bool>)> + Send + 'a>>;

enum BodyState<'a, R> {
    Idle(MemberPayload<'a, R>),
    Reading(ChunkFuture<'a, R>),
    Done,
}

/// One member body as `AsyncBufRead`. tar-framing only offers an async chunk
/// method, so each refill runs as a boxed future owning the payload cursor
pub struct Body<'a, R> {
    state: BodyState<'a, R>,
    buf: Vec<u8>,
    pos: usize,
}

impl<'a, R: AsyncRead + Unpin + Send> Body<'a, R> {
    fn new(payload: MemberPayload<'a, R>) -> Self {
        Self {
            state: BodyState::Idle(payload),
            buf: Vec::new(),
            pos: 0,
        }
    }

    /// Replaces `buf` with the next `len` body bytes, rounded up to a tar block
    /// and fewer at body end, read straight into `buf`'s allocation. Bytes
    /// left buffered by `AsyncBufRead` use come out first as a shorter chunk.
    /// False once the body is exhausted, leaving `buf` untouched. Refilling a
    /// `buf` at least that long skips zeroing it
    pub async fn next_chunk(&mut self, buf: &mut Vec<u8>, len: usize) -> io::Result<bool> {
        if let BodyState::Reading(fut) = &mut self.state {
            let (payload, read, more) = fut.await;
            self.state = BodyState::Done;
            self.buf = read;
            self.pos = 0;
            if more? {
                self.state = BodyState::Idle(payload);
            } else {
                self.buf.clear();
            }
        }
        if self.pos < self.buf.len() {
            buf.clear();
            buf.extend_from_slice(&self.buf[self.pos..]);
            self.pos = self.buf.len();
            return Ok(true);
        }
        let BodyState::Idle(payload) = &mut self.state else {
            return Ok(false);
        };
        let more = payload.next_chunk(buf, len).await.map_err(io_err)?;
        if !more {
            self.state = BodyState::Done;
        }
        Ok(more)
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncBufRead for Body<'_, R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let me = self.get_mut();
        while me.pos == me.buf.len() {
            match std::mem::replace(&mut me.state, BodyState::Done) {
                BodyState::Idle(mut payload) => {
                    let mut buf = std::mem::take(&mut me.buf);
                    me.pos = 0;
                    me.state = BodyState::Reading(Box::pin(async move {
                        let more = payload.next_chunk(&mut buf, CHUNK_BYTES).await;
                        (payload, buf, more.map_err(io_err))
                    }));
                }
                BodyState::Reading(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        me.state = BodyState::Reading(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((payload, buf, more)) => {
                        me.buf = buf;
                        me.pos = 0;
                        if !more? {
                            me.buf.clear();
                            return Poll::Ready(Ok(&[]));
                        }
                        me.state = BodyState::Idle(payload);
                    }
                },
                BodyState::Done => return Poll::Ready(Ok(&[])),
            }
        }
        Poll::Ready(Ok(&me.buf[me.pos..]))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let me = self.get_mut();
        me.pos = (me.pos + amt).min(me.buf.len());
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncRead for Body<'_, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        poll_read_via_fill_buf(self, cx, out)
    }
}

/// `AsyncRead::poll_read` for types whose reads are served by their `AsyncBufRead` impl
pub(crate) fn poll_read_via_fill_buf<B: AsyncBufRead + ?Sized>(
    mut b: Pin<&mut B>,
    cx: &mut Context<'_>,
    out: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>> {
    let avail = ready!(b.as_mut().poll_fill_buf(cx))?;
    let n = avail.len().min(out.remaining());
    out.put_slice(&avail[..n]);
    b.consume(n);
    Poll::Ready(Ok(()))
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;

    async fn build(entries: &[(Entry, &[u8])]) -> Vec<u8> {
        let mut w = Writer::new(Vec::new());
        for (e, body) in entries {
            w.append(e, *body).await.unwrap();
        }
        w.finish().await.unwrap()
    }

    #[test]
    fn put_num_switches_to_base256() {
        let mut f = [0u8; 12];
        put_num(&mut f, 0o777);
        assert_eq!(&f, b"00000000777\0");
        put_num(&mut f, 1 << 33);
        assert_eq!(f[0], 0x80);
        assert_eq!(&f[7..], &[2, 0, 0, 0, 0]);
    }

    /// Output must read back through the sync `tar` crate, incl. long names,
    /// long link targets, leading-slash names & base-256 sizes in headers
    #[tokio::test]
    async fn tar_crate_reads_output() {
        let long = format!("pg_tblspc/16384/{}", "d/".repeat(60)) + "16385";
        let mut link = Entry::new("pg_tblspc/16384", Kind::SymbolicLink, 0, 0o777, 0);
        link.link = format!("/mnt/{}", "x".repeat(150));
        let tar_bytes = build(&[
            (Entry::new("base", Kind::Directory, 0, 0o700, 1), b""),
            (
                Entry::new(long.as_str(), Kind::Regular, 3, 0o600, 2),
                b"abc",
            ),
            (
                Entry::new("/global/pg_control", Kind::Regular, 1, 0o600, 0),
                b"z",
            ),
            (link.clone(), b""),
        ])
        .await;
        let mut a = tar::Archive::new(tar_bytes.as_slice());
        let mut seen = Vec::new();
        for e in a.entries().unwrap() {
            let mut e = e.unwrap();
            let name = String::from_utf8_lossy(&e.path_bytes()).into_owned();
            let target = e
                .link_name_bytes()
                .map(|l| String::from_utf8_lossy(&l).into_owned());
            let mode = e.header().mode().unwrap();
            let mut body = Vec::new();
            e.read_to_end(&mut body).unwrap();
            seen.push((name, mode, body, target));
        }
        assert_eq!(seen[0], ("base".into(), 0o700, vec![], None));
        assert_eq!(seen[1], (long, 0o600, b"abc".to_vec(), None));
        assert_eq!(seen[2].0, "/global/pg_control");
        assert_eq!(seen[3].3.as_deref(), Some(link.link.as_str()));
    }

    #[tokio::test]
    async fn roundtrip_through_reader() {
        let long = "a/".repeat(80) + "f";
        let body: Vec<u8> = (0..CHUNK_BYTES * 2 + 77).map(|i| i as u8).collect();
        let tar_bytes = build(&[
            (Entry::new("skipped", Kind::Regular, 5, 0o640, 9), b"12345"),
            (
                Entry::new(long.as_str(), Kind::Regular, body.len() as u64, 0o600, 7),
                &body,
            ),
        ])
        .await;
        let mut r = Reader::new(tar_bytes.as_slice());
        let (e, _) = r.next().await.unwrap().unwrap();
        assert_eq!((e.path.as_str(), e.mode, e.mtime), ("skipped", 0o640, 9));
        let (e, mut b) = r.next().await.unwrap().unwrap();
        assert_eq!(e.path, long);
        let mut got = Vec::new();
        b.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, body);
        drop(b);
        assert!(r.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reader_accepts_missing_end_marker() {
        let mut tar_bytes = build(&[(Entry::new("f", Kind::Regular, 1, 0o600, 0), b"x")]).await;
        tar_bytes.truncate(tar_bytes.len() - 2 * BLOCK_SIZE);
        let mut r = Reader::new(tar_bytes.as_slice());
        assert_eq!(r.next().await.unwrap().unwrap().0.path, "f");
        assert!(r.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn short_body_errors() {
        let mut w = Writer::new(Vec::new());
        let e = Entry::new("f", Kind::Regular, 4, 0o600, 0);
        let err = w.append(&e, &b"ab"[..]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// Chunks after partial `AsyncBufRead` use start with the buffered rest
    #[tokio::test]
    async fn next_chunk_follows_buffered_bytes() {
        use tokio::io::AsyncBufReadExt as _;

        let body: Vec<u8> = (0..CHUNK_BYTES + 3000).map(|i| i as u8).collect();
        let e = Entry::new("f", Kind::Regular, body.len() as u64, 0o600, 0);
        let tar_bytes = build(&[(e, &body)]).await;
        let mut r = Reader::new(tar_bytes.as_slice());
        let (_, mut b) = r.next().await.unwrap().unwrap();
        b.fill_buf().await.unwrap();
        b.consume(10);
        let mut buf = Vec::new();
        let mut lens = Vec::new();
        let mut got = Vec::new();
        while b.next_chunk(&mut buf, 1024).await.unwrap() {
            lens.push(buf.len());
            got.extend_from_slice(&buf);
        }
        assert_eq!(got, body[10..]);
        assert_eq!(lens[..2], [CHUNK_BYTES - 10, 1024]);
        assert_eq!(lens.last(), Some(&(3000 % 1024)));
        assert!(!b.next_chunk(&mut buf, 1024).await.unwrap());
    }
}
