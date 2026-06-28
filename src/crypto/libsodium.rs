//! libsodium `crypto_secretstream_xchacha20poly1305` adapter (XChaCha20-Poly1305)
//!
//! Wire-compatible with wal-g `internal/crypto/libsodium`:
//!   - 24-byte header (XChaCha20 subkey nonce + inonce), written first
//!   - chunks of 8192 plaintext bytes -> 8192 + 17 ciphertext bytes (1 byte
//!     tag prepended + 16 byte Poly1305 MAC appended), FINAL tag on the last
//!     chunk. Matches wal-g `chunkSize = 8192` exactly
//!
//! Env vars (mirror wal-g):
//!   WALG_LIBSODIUM_KEY            inline key
//!   WALG_LIBSODIUM_KEY_PATH       key file (trimmed of surrounding whitespace)
//!   WALG_LIBSODIUM_KEY_TRANSFORM  none (default) | hex | base64
//!
//! `none` transform mirrors wal-g's legacy padding: short keys (>=25 bytes)
//! are right-padded with 0x00 to 32 bytes; long keys are truncated. `hex` and
//! `base64` require the decoded bytes to be exactly 32 bytes long

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use dryoc::dryocstream::{DryocStream, Header, Key, Pull, Push, Tag};
use dryoc::types::ByteArray;
use tokio::io::{AsyncRead, ReadBuf};

use crate::compression::AsyncReader;
use crate::crypto::{Crypter, DynCrypter};

/// libsodium chunk size: 8192 plaintext bytes per push. Matches wal-g
const CHUNK_SIZE: usize = 8192;
/// Per-chunk ciphertext overhead: 1 byte tag + 16 byte Poly1305 MAC
const ABYTES: usize = 17;
/// XChaCha20 secretstream header
const HEADER_BYTES: usize = 24;
const KEY_BYTES: usize = 32;
const MIN_NONE_TRANSFORM_KEY: usize = 25;

#[derive(Clone, Debug)]
pub struct LibsodiumCrypter {
    key: [u8; KEY_BYTES],
}

impl LibsodiumCrypter {
    pub fn new(key: [u8; KEY_BYTES]) -> Self {
        Self { key }
    }

    pub fn from_inline(input: &str, transform: KeyTransform) -> Result<Self> {
        Ok(Self::new(transform.apply(input)?))
    }

    pub fn from_path(path: &str, transform: KeyTransform) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("read libsodium key from {path}"))?;
        let s = std::str::from_utf8(&bytes)
            .with_context(|| format!("libsodium key at {path} is not UTF-8"))?;
        Self::from_inline(s.trim(), transform)
    }
}

impl Crypter for LibsodiumCrypter {
    fn name(&self) -> &'static str {
        "libsodium"
    }

    fn encrypt_reader(&self, plain: AsyncReader) -> AsyncReader {
        Box::pin(EncryptReader::new(self.key, plain))
    }

    fn decrypt_reader(&self, cipher: AsyncReader) -> AsyncReader {
        Box::pin(DecryptReader::new(self.key, cipher))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyTransform {
    None,
    Hex,
    Base64,
}

impl KeyTransform {
    pub fn from_name(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "none" => Ok(Self::None),
            "hex" => Ok(Self::Hex),
            "base64" => Ok(Self::Base64),
            other => bail!("unknown libsodium key transform {other:?} (none|hex|base64)"),
        }
    }

    fn apply(self, input: &str) -> Result<[u8; KEY_BYTES]> {
        match self {
            KeyTransform::None => {
                if input.len() < MIN_NONE_TRANSFORM_KEY {
                    bail!(
                        "libsodium key length must be at least {MIN_NONE_TRANSFORM_KEY} bytes (got {})",
                        input.len()
                    );
                }
                let mut out = [0u8; KEY_BYTES];
                let take = input.len().min(KEY_BYTES);
                out[..take].copy_from_slice(&input.as_bytes()[..take]);
                Ok(out)
            }
            KeyTransform::Hex => {
                let decoded = hex::decode(input.trim()).context("decode libsodium key as hex")?;
                fixed_len(&decoded)
            }
            KeyTransform::Base64 => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(input.trim())
                    .context("decode libsodium key as base64")?;
                fixed_len(&decoded)
            }
        }
    }
}

fn fixed_len(decoded: &[u8]) -> Result<[u8; KEY_BYTES]> {
    if decoded.len() != KEY_BYTES {
        bail!(
            "libsodium key must decode to exactly {KEY_BYTES} bytes (got {})",
            decoded.len()
        );
    }
    let mut out = [0u8; KEY_BYTES];
    out.copy_from_slice(decoded);
    Ok(out)
}

pub fn resolve(vars: &crate::config::Vars) -> Result<Option<DynCrypter>> {
    let key_inline = vars.get("WALG_LIBSODIUM_KEY");
    let key_path = vars.get("WALG_LIBSODIUM_KEY_PATH");
    if key_inline.is_none() && key_path.is_none() {
        return Ok(None);
    }
    let transform = KeyTransform::from_name(
        vars.get("WALG_LIBSODIUM_KEY_TRANSFORM")
            .unwrap_or_default()
            .as_str(),
    )?;
    let crypter = match (key_inline, key_path) {
        (Some(k), _) => LibsodiumCrypter::from_inline(&k, transform)?,
        (_, Some(p)) => LibsodiumCrypter::from_path(&p, transform)?,
        _ => unreachable!(),
    };
    Ok(Some(Arc::new(crypter)))
}

// ─── Async streaming wrappers ──────────────────────────────────────────────
//
// Encrypt:
//   - On first poll, derive header from key and emit it as the first 24 bytes
//   - Top up an 8 KiB plaintext buffer from `inner`; on full buffer or EOF,
//     push a MESSAGE (or FINAL) chunk into `out`, drain `out` into caller
//
// Decrypt:
//   - Read 24-byte header from `inner`, init pull stream
//   - Read up to 8192+17 ciphertext bytes; on full or EOF, pull a chunk,
//     append plaintext to `out`, drain into caller
//   - FINAL tag flips `finalized = true`; subsequent reads return EOF

/// Copy queued bytes from `out[*out_pos..]` into `buf`, resetting the buffer
/// once fully drained. Returns true when there were bytes pending (caller then
/// yields `Ready`), mirroring the secretstream readers' drain-first step
fn drain_into(out: &mut Vec<u8>, out_pos: &mut usize, buf: &mut ReadBuf<'_>) -> bool {
    if *out_pos >= out.len() {
        return false;
    }
    let want = buf.remaining().min(out.len() - *out_pos);
    buf.put_slice(&out[*out_pos..*out_pos + want]);
    *out_pos += want;
    if *out_pos == out.len() {
        out.clear();
        *out_pos = 0;
    }
    true
}

struct EncryptReader {
    inner: AsyncReader,
    stream: Option<DryocStream<Push>>,
    key: [u8; KEY_BYTES],
    /// Ciphertext (and the leading header) waiting to be drained
    out: Vec<u8>,
    out_pos: usize,
    /// Plaintext scratch sized once at construction so polls read
    /// directly into [`Self::in_buf`] via a slice of `in_buf[in_filled..]`
    /// without allocating per-poll. `in_filled` tracks the meaningful
    /// prefix; the rest of `in_buf` is initialised but garbage
    in_buf: Vec<u8>,
    in_filled: usize,
    eof: bool,
    finalized: bool,
}

impl EncryptReader {
    fn new(key: [u8; KEY_BYTES], inner: AsyncReader) -> Self {
        Self {
            inner,
            stream: None,
            key,
            out: Vec::with_capacity(CHUNK_SIZE + ABYTES + HEADER_BYTES),
            out_pos: 0,
            in_buf: vec![0u8; CHUNK_SIZE],
            in_filled: 0,
            eof: false,
            finalized: false,
        }
    }

    fn init_if_needed(&mut self) {
        if self.stream.is_some() {
            return;
        }
        let key: Key = self.key.into();
        let (push, header): (DryocStream<Push>, Header) = DryocStream::init_push(&key);
        self.out.extend_from_slice(header.as_array());
        self.stream = Some(push);
    }

    fn push_chunk(&mut self, last: bool) -> std::io::Result<()> {
        let s = self.stream.as_mut().expect("init_if_needed called");
        let tag = if last { Tag::FINAL } else { Tag::MESSAGE };
        // dryoc's `Bytes` impl needs a `Sized` Input — `&[u8]` qualifies,
        // so pass-by-double-reference here to keep the call zero-copy
        let plaintext: &[u8] = &self.in_buf[..self.in_filled];
        let ct: Vec<u8> = s
            .push(&plaintext, None, tag)
            .map_err(|e| std::io::Error::other(format!("libsodium push: {e}")))?;
        // When out is fully drained (out_pos == len), adopt dryoc's Vec
        // directly instead of copying via extend_from_slice. The header
        // path & rare partial-drain interleavings still extend
        if self.out_pos == self.out.len() {
            self.out = ct;
            self.out_pos = 0;
        } else {
            self.out.extend_from_slice(&ct);
        }
        self.in_filled = 0;
        if last {
            self.finalized = true;
        }
        Ok(())
    }
}

impl AsyncRead for EncryptReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = &mut *self;
        me.init_if_needed();

        loop {
            // 1) Drain any ready ciphertext (or header bytes)
            if drain_into(&mut me.out, &mut me.out_pos, buf) {
                return Poll::Ready(Ok(()));
            }
            if me.finalized {
                return Poll::Ready(Ok(()));
            }

            // 2) Top up the plaintext buffer — read into the
            //    pre-allocated slice me.in_buf[in_filled..], no
            //    per-poll alloc and no copy into in_buf afterwards
            let mut tmp = ReadBuf::new(&mut me.in_buf[me.in_filled..]);
            match Pin::new(&mut me.inner).poll_read(cx, &mut tmp) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
            let n = tmp.filled().len();
            if n == 0 {
                me.eof = true;
            } else {
                me.in_filled += n;
            }

            // 3) Push a chunk
            if me.in_filled == CHUNK_SIZE && !me.eof {
                me.push_chunk(false)?;
            } else if me.eof {
                me.push_chunk(true)?;
            }
            // loop back to drain
        }
    }
}

struct DecryptReader {
    inner: AsyncReader,
    stream: Option<DryocStream<Pull>>,
    key: [u8; KEY_BYTES],
    /// Header bytes accumulated until init_pull runs. Pre-allocated,
    /// length-tracked by `header_filled` to avoid per-poll allocs
    header_buf: [u8; HEADER_BYTES],
    header_filled: usize,
    /// Plaintext queued for the caller
    out: Vec<u8>,
    out_pos: usize,
    /// Ciphertext scratch sized once at construction; `in_filled` is
    /// the meaningful prefix
    in_buf: Vec<u8>,
    in_filled: usize,
    eof: bool,
    finalized: bool,
}

impl DecryptReader {
    fn new(key: [u8; KEY_BYTES], inner: AsyncReader) -> Self {
        Self {
            inner,
            stream: None,
            key,
            header_buf: [0u8; HEADER_BYTES],
            header_filled: 0,
            out: Vec::with_capacity(CHUNK_SIZE),
            out_pos: 0,
            in_buf: vec![0u8; CHUNK_SIZE + ABYTES],
            in_filled: 0,
            eof: false,
            finalized: false,
        }
    }

    fn pull_chunk(&mut self) -> std::io::Result<()> {
        let s = self.stream.as_mut().expect("init done");
        // See comment in `push_chunk` re: `&&[u8]` shape
        let ciphertext: &[u8] = &self.in_buf[..self.in_filled];
        let (pt, tag): (Vec<u8>, Tag) = s
            .pull(&ciphertext, None)
            .map_err(|e| std::io::Error::other(format!("libsodium pull: {e}")))?;
        if self.out_pos == self.out.len() {
            // Drained; adopt dryoc's Vec without copying
            self.out = pt;
            self.out_pos = 0;
        } else {
            self.out.extend_from_slice(&pt);
        }
        self.in_filled = 0;
        if matches!(tag, Tag::FINAL) {
            self.finalized = true;
        }
        Ok(())
    }
}

impl AsyncRead for DecryptReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = &mut *self;
        loop {
            // 1) Drain plaintext
            if drain_into(&mut me.out, &mut me.out_pos, buf) {
                return Poll::Ready(Ok(()));
            }
            if me.finalized {
                return Poll::Ready(Ok(()));
            }
            // 2) Header phase — read directly into the pre-allocated
            //    header_buf slice, no per-poll scratch
            if me.stream.is_none() {
                let mut tmp = ReadBuf::new(&mut me.header_buf[me.header_filled..]);
                match Pin::new(&mut me.inner).poll_read(cx, &mut tmp) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {}
                }
                let n = tmp.filled().len();
                if n == 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "libsodium: EOF before 24-byte header",
                    )));
                }
                me.header_filled += n;
                if me.header_filled == HEADER_BYTES {
                    let key: Key = me.key.into();
                    let hdr: Header = me.header_buf.into();
                    me.stream = Some(DryocStream::<Pull>::init_pull(&key, &hdr));
                }
                continue;
            }

            // 3) Read up to one full ciphertext chunk — into the
            //    pre-allocated tail of in_buf
            let target = CHUNK_SIZE + ABYTES;
            let mut tmp = ReadBuf::new(&mut me.in_buf[me.in_filled..]);
            match Pin::new(&mut me.inner).poll_read(cx, &mut tmp) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
            let n = tmp.filled().len();
            if n == 0 {
                me.eof = true;
            } else {
                me.in_filled += n;
            }
            if me.in_filled == target {
                me.pull_chunk()?;
                continue; // loop back to drain
            }
            if me.eof {
                if me.in_filled == 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "libsodium: ciphertext ended without FINAL tag",
                    )));
                }
                if me.in_filled < ABYTES {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "libsodium: truncated tail chunk",
                    )));
                }
                me.pull_chunk()?;
                continue;
            }
            // not enough bytes yet, loop and read more
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    fn key() -> [u8; KEY_BYTES] {
        let mut k = [0u8; KEY_BYTES];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    async fn roundtrip(plain: &[u8]) {
        let c = LibsodiumCrypter::new(key());
        let enc = c.encrypt_reader(Box::pin(Cursor::new(plain.to_vec())));
        let mut dec = c.decrypt_reader(enc);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, plain);
    }

    #[tokio::test]
    async fn empty_payload_roundtrip() {
        roundtrip(&[]).await;
    }

    #[tokio::test]
    async fn small_payload_roundtrip() {
        roundtrip(b"hello libsodium").await;
    }

    #[tokio::test]
    async fn chunk_boundary_roundtrip() {
        let mut p = vec![0u8; CHUNK_SIZE];
        for (i, b) in p.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        roundtrip(&p).await;
    }

    #[tokio::test]
    async fn many_chunks_roundtrip() {
        let mut p = vec![0u8; CHUNK_SIZE * 5 + 17];
        for (i, b) in p.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        roundtrip(&p).await;
    }

    #[tokio::test]
    async fn ciphertext_is_not_plaintext() {
        let plain = vec![0xABu8; CHUNK_SIZE * 3];
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(Cursor::new(plain.clone())));
        let mut out = Vec::new();
        enc.read_to_end(&mut out).await.unwrap();
        assert!(out.len() > plain.len(), "ciphertext must grow");
        assert_ne!(&out[HEADER_BYTES..HEADER_BYTES + 16], &plain[..16]);
    }

    #[tokio::test]
    async fn wrong_key_fails_decrypt() {
        let plain = b"secret".to_vec();
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(Cursor::new(plain)));
        let mut ct = Vec::new();
        enc.read_to_end(&mut ct).await.unwrap();

        let mut bad_key = key();
        bad_key[0] ^= 0xFF;
        let bad = LibsodiumCrypter::new(bad_key);
        let mut dec = bad.decrypt_reader(Box::pin(Cursor::new(ct)));
        let mut out = Vec::new();
        let r = dec.read_to_end(&mut out).await;
        assert!(r.is_err(), "decryption must fail with wrong key");
    }

    #[tokio::test]
    async fn tampered_ciphertext_fails() {
        let plain = b"important data to protect".repeat(100);
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(Cursor::new(plain)));
        let mut ct = Vec::new();
        enc.read_to_end(&mut ct).await.unwrap();
        ct[HEADER_BYTES + 10] ^= 0x01;
        let mut dec = c.decrypt_reader(Box::pin(Cursor::new(ct)));
        let mut out = Vec::new();
        let r = dec.read_to_end(&mut out).await;
        assert!(r.is_err(), "tampered ciphertext must fail Poly1305 check");
    }

    #[tokio::test]
    async fn truncated_tail_fails() {
        let plain = vec![0u8; CHUNK_SIZE * 2 + 5];
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(Cursor::new(plain)));
        let mut ct = Vec::new();
        enc.read_to_end(&mut ct).await.unwrap();
        let chop = ct.len() - 1;
        ct.truncate(chop);
        let mut dec = c.decrypt_reader(Box::pin(Cursor::new(ct)));
        let mut out = Vec::new();
        let r = dec.read_to_end(&mut out).await;
        assert!(r.is_err(), "truncated ciphertext must fail");
    }

    #[test]
    fn key_transform_none_pads_short_key() {
        let t = KeyTransform::None;
        let k = t
            .apply("123456789012345678901234567")
            .expect("27 bytes >= 25");
        assert_eq!(&k[..27], b"123456789012345678901234567");
        for b in &k[27..] {
            assert_eq!(*b, 0);
        }
    }

    #[test]
    fn key_transform_none_truncates_long_key() {
        let t = KeyTransform::None;
        let k = t
            .apply("abcdefghijklmnopqrstuvwxyz0123456789")
            .expect("36 bytes truncate to 32");
        assert_eq!(&k[..], &b"abcdefghijklmnopqrstuvwxyz012345"[..]);
    }

    #[test]
    fn key_transform_none_rejects_too_short() {
        assert!(KeyTransform::None.apply("short").is_err());
    }

    #[test]
    fn key_transform_hex_strict() {
        let t = KeyTransform::Hex;
        let s = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(t.apply(s).is_ok());
        assert!(t.apply("00").is_err());
        assert!(t.apply("zzzz").is_err());
    }

    #[test]
    fn key_transform_base64_strict() {
        let t = KeyTransform::Base64;
        let raw = [7u8; 32];
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        assert!(t.apply(&b64).is_ok());
        assert!(t.apply("aGVsbG8=").is_err());
    }

    #[test]
    fn crypter_reports_its_name() {
        assert_eq!(LibsodiumCrypter::new(key()).name(), "libsodium");
    }

    #[test]
    fn key_transform_from_name_maps_aliases_and_rejects_unknown() {
        assert_eq!(KeyTransform::from_name("").unwrap(), KeyTransform::None);
        assert_eq!(KeyTransform::from_name("none").unwrap(), KeyTransform::None);
        // case-insensitive
        assert_eq!(KeyTransform::from_name("HEX").unwrap(), KeyTransform::Hex);
        assert_eq!(
            KeyTransform::from_name("Base64").unwrap(),
            KeyTransform::Base64
        );
        assert!(KeyTransform::from_name("rot13").is_err());
    }

    #[test]
    fn from_path_reads_trims_and_validates_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let key_file = dir.path().join("key");
        // surrounding whitespace is trimmed before the none-transform padding
        std::fs::write(&key_file, b"  0123456789012345678901234567  \n").unwrap();
        let c =
            LibsodiumCrypter::from_path(key_file.to_str().unwrap(), KeyTransform::None).unwrap();
        assert_eq!(&c.key[..28], b"0123456789012345678901234567");

        // missing file surfaces a read error
        assert!(
            LibsodiumCrypter::from_path(
                dir.path().join("absent").to_str().unwrap(),
                KeyTransform::None
            )
            .is_err()
        );

        // non-UTF-8 contents rejected before transform
        let bad = dir.path().join("bin");
        std::fs::write(&bad, [0xff, 0xfe, 0x00]).unwrap();
        assert!(LibsodiumCrypter::from_path(bad.to_str().unwrap(), KeyTransform::None).is_err());
    }

    /// Yields `data` verbatim, then errors on the next poll. Drives the
    /// inner-read error and EOF branches of the secretstream readers
    struct FailAfter {
        data: Vec<u8>,
        pos: usize,
    }

    impl AsyncRead for FailAfter {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.pos < self.data.len() {
                let n = buf.remaining().min(self.data.len() - self.pos);
                let start = self.pos;
                buf.put_slice(&self.data[start..start + n]);
                self.pos += n;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(std::io::Error::other("boom")))
        }
    }

    #[tokio::test]
    async fn encrypt_propagates_inner_read_error() {
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(FailAfter {
            data: Vec::new(),
            pos: 0,
        }));
        let mut out = Vec::new();
        // header drains first, then the inner error surfaces
        assert!(enc.read_to_end(&mut out).await.is_err());
    }

    #[tokio::test]
    async fn decrypt_errors_on_eof_before_header() {
        let c = LibsodiumCrypter::new(key());
        let mut dec = c.decrypt_reader(Box::pin(Cursor::new(Vec::new())));
        let mut out = Vec::new();
        assert!(dec.read_to_end(&mut out).await.is_err());
    }

    #[tokio::test]
    async fn decrypt_propagates_inner_error_after_header() {
        let c = LibsodiumCrypter::new(key());
        // 24 arbitrary header bytes init the pull stream; the next read errors
        let mut dec = c.decrypt_reader(Box::pin(FailAfter {
            data: vec![0u8; HEADER_BYTES],
            pos: 0,
        }));
        let mut out = Vec::new();
        assert!(dec.read_to_end(&mut out).await.is_err());
    }

    #[tokio::test]
    async fn decrypt_errors_on_header_only_stream() {
        // a valid header followed by no chunk at all: ends without a FINAL tag
        let plain: &[u8] = &[];
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(Cursor::new(plain.to_vec())));
        let mut ct = Vec::new();
        enc.read_to_end(&mut ct).await.unwrap();
        let header_only = ct[..HEADER_BYTES].to_vec();
        let mut dec = c.decrypt_reader(Box::pin(Cursor::new(header_only)));
        let mut out = Vec::new();
        assert!(dec.read_to_end(&mut out).await.is_err());
    }

    #[tokio::test]
    async fn decrypt_errors_on_sub_abytes_tail() {
        // header + a tail shorter than the per-chunk overhead can't authenticate
        let c = LibsodiumCrypter::new(key());
        let mut enc = c.encrypt_reader(Box::pin(Cursor::new(Vec::new())));
        let mut ct = Vec::new();
        enc.read_to_end(&mut ct).await.unwrap();
        let mut short = ct[..HEADER_BYTES].to_vec();
        short.extend_from_slice(&ct[HEADER_BYTES..HEADER_BYTES + 4]); // 4 < ABYTES
        let mut dec = c.decrypt_reader(Box::pin(Cursor::new(short)));
        let mut out = Vec::new();
        assert!(dec.read_to_end(&mut out).await.is_err());
    }
}
