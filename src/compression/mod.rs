//! Streaming compression / decompression backed by async-compression
//!
//! Reader-to-reader transforms via tokio::bufread adapters; no thread bridge,
//! no full-segment buffering. Memory cost ~= codec window size + tokio BufReader.
//!
//! Each codec sits behind its own cargo feature (`zstd`, `lz4`, `brotli`,
//! `lzma`, `gzip`). Every [`Method`] still parses, so object names & metadata
//! of any codec stay readable; encoding or decoding with a compiled-out codec
//! yields a reader that errors on first read

use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "lzma",
    feature = "gzip"
))]
use async_compression::Level;
#[cfg(feature = "lz4")]
use async_compression::lz4::{BlockSize, EncoderParams};
#[cfg(feature = "brotli")]
use async_compression::tokio::bufread::{BrotliDecoder, BrotliEncoder};
#[cfg(feature = "gzip")]
use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder};
#[cfg(feature = "lz4")]
use async_compression::tokio::bufread::{Lz4Decoder, Lz4Encoder};
#[cfg(feature = "lzma")]
use async_compression::tokio::bufread::{LzmaDecoder, LzmaEncoder};
#[cfg(feature = "zstd")]
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncRead, BufReader, ReadBuf};

const BUF_CAPACITY: usize = 256 * 1024;

pub type AsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;
pub type AsyncBufReader = Pin<Box<dyn AsyncBufRead + Send + Unpin>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    None,
    Zstd,
    Brotli,
    Lz4,
    Lzma,
    Gz,
}

impl Method {
    pub fn from_name(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "none" => Some(Method::None),
            "zstd" => Some(Method::Zstd),
            "brotli" => Some(Method::Brotli),
            "lz4" => Some(Method::Lz4),
            "lzma" => Some(Method::Lzma),
            "gz" | "gzip" => Some(Method::Gz),
            _ => None,
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Method::None => "",
            Method::Zstd => "zst",
            Method::Brotli => "br",
            Method::Lz4 => "lz4",
            Method::Lzma => "lzma",
            Method::Gz => "gz",
        }
    }

    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.trim_start_matches('.') {
            "" => Some(Method::None),
            "zst" | "zstd" => Some(Method::Zstd),
            "br" | "brotli" => Some(Method::Brotli),
            "lz4" => Some(Method::Lz4),
            "lzma" => Some(Method::Lzma),
            "gz" | "gzip" => Some(Method::Gz),
            _ => None,
        }
    }

    /// Cargo feature that provides this codec (`None` needs no feature)
    pub fn feature(self) -> &'static str {
        match self {
            Method::None => "",
            Method::Zstd => "zstd",
            Method::Brotli => "brotli",
            Method::Lz4 => "lz4",
            Method::Lzma => "lzma",
            Method::Gz => "gzip",
        }
    }

    /// Whether this build includes the codec
    pub fn is_available(self) -> bool {
        match self {
            Method::None => true,
            Method::Zstd => cfg!(feature = "zstd"),
            Method::Brotli => cfg!(feature = "brotli"),
            Method::Lz4 => cfg!(feature = "lz4"),
            Method::Lzma => cfg!(feature = "lzma"),
            Method::Gz => cfg!(feature = "gzip"),
        }
    }

    fn unavailable_error(self) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "{self:?} compression requires walrus built with the `{}` feature",
                self.feature()
            ),
        )
    }
}

impl Default for Method {
    /// lz4 (wal-g's default) when built, else zstd, else no compression
    fn default() -> Self {
        if cfg!(feature = "lz4") {
            Method::Lz4
        } else if cfg!(feature = "zstd") {
            Method::Zstd
        } else {
            Method::None
        }
    }
}

/// Stand-in for a compiled-out codec: fails the first read, so `encode` /
/// `decode` keep their infallible signatures
struct Unavailable(Method);

impl AsyncRead for Unavailable {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(self.0.unavailable_error()))
    }
}

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub fn encode(method: Method, input: AsyncReader, level: i32) -> AsyncReader {
    match method {
        Method::None => input,
        _ => encode_buffered(
            method,
            Box::pin(BufReader::with_capacity(BUF_CAPACITY, input)),
            level,
        ),
    }
}

#[cfg_attr(
    not(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "lzma",
        feature = "gzip"
    )),
    allow(unused_variables)
)]
pub fn encode_buffered(method: Method, input: AsyncBufReader, level: i32) -> AsyncReader {
    match method {
        Method::None => Box::pin(input),
        #[cfg(feature = "zstd")]
        Method::Zstd => Box::pin(ZstdEncoder::with_quality(input, Level::Precise(level))),
        #[cfg(feature = "brotli")]
        Method::Brotli => Box::pin(BrotliEncoder::with_quality(
            input,
            Level::Precise(brotli_quality(level)),
        )),
        #[cfg(feature = "lz4")]
        Method::Lz4 => Box::pin(Lz4Encoder::with_quality_and_params(
            input,
            Level::Precise(level),
            EncoderParams::default().block_size(BlockSize::Max256KB),
        )),
        #[cfg(feature = "lzma")]
        Method::Lzma => Box::pin(LzmaEncoder::with_quality(
            input,
            Level::Precise(lzma_preset(level)),
        )),
        #[cfg(feature = "gzip")]
        Method::Gz => Box::pin(GzipEncoder::with_quality(
            input,
            Level::Precise(gzip_level(level)),
        )),
        #[allow(unreachable_patterns)]
        m => Box::pin(Unavailable(m)),
    }
}

pub fn decode(method: Method, input: AsyncReader) -> AsyncReader {
    match method {
        Method::None => input,
        #[cfg(feature = "zstd")]
        Method::Zstd => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(ZstdDecoder::new(buffered))
        }
        #[cfg(feature = "brotli")]
        Method::Brotli => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(BrotliDecoder::new(buffered))
        }
        #[cfg(feature = "lz4")]
        Method::Lz4 => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(Lz4Decoder::new(buffered))
        }
        #[cfg(feature = "lzma")]
        Method::Lzma => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(LzmaDecoder::new(buffered))
        }
        #[cfg(feature = "gzip")]
        Method::Gz => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(GzipDecoder::new(buffered))
        }
        #[allow(unreachable_patterns)]
        m => Box::pin(Unavailable(m)),
    }
}

// brotli quality 0..=11; clamp caller-supplied zstd-shaped level into range
#[cfg(feature = "brotli")]
fn brotli_quality(level: i32) -> i32 {
    level.clamp(0, 11)
}

// lzma preset 0..=9
#[cfg(feature = "lzma")]
fn lzma_preset(level: i32) -> i32 {
    level.clamp(0, 9)
}

// gzip level 0..=9 (flate2 / async-compression accept 0..=9)
#[cfg(feature = "gzip")]
fn gzip_level(level: i32) -> i32 {
    level.clamp(0, 9)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    fn reader(b: &[u8]) -> AsyncReader {
        Box::pin(Cursor::new(b.to_vec()))
    }

    #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "lzma",
        feature = "gzip"
    ))]
    fn payload() -> Vec<u8> {
        (0..200_000u32).map(|i| (i % 251) as u8).collect()
    }

    #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "lzma",
        feature = "gzip"
    ))]
    async fn roundtrip(method: Method) {
        let original = payload();
        let enc = encode(method, reader(&original), 3);
        let mut dec = decode(method, enc);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, original, "{method:?} roundtrip mismatch");
    }

    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn zstd_roundtrip() {
        roundtrip(Method::Zstd).await;
    }

    #[cfg(feature = "brotli")]
    #[tokio::test]
    async fn brotli_roundtrip() {
        roundtrip(Method::Brotli).await;
    }

    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn lz4_roundtrip() {
        roundtrip(Method::Lz4).await;
    }

    #[cfg(feature = "lzma")]
    #[tokio::test]
    async fn lzma_roundtrip() {
        roundtrip(Method::Lzma).await;
    }

    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn gz_roundtrip() {
        roundtrip(Method::Gz).await;
    }

    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn encode_buffered_matches_encode() {
        // encode_buffered feeds the codec an AsyncBufRead directly (no internal
        // BufReader); output must still decode back to the original
        let original = payload();
        let buffered: AsyncBufReader = Box::pin(Cursor::new(original.clone()));
        let enc = encode_buffered(Method::Lz4, buffered, 3);
        let mut dec = decode(Method::Lz4, enc);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn none_passthrough() {
        let mut r = encode(Method::None, reader(b"hello"), 3);
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello");
    }

    #[test]
    fn extension_mapping() {
        assert_eq!(Method::from_name("zstd"), Some(Method::Zstd));
        assert_eq!(Method::from_name("brotli"), Some(Method::Brotli));
        assert_eq!(Method::from_name("lz4"), Some(Method::Lz4));
        assert_eq!(Method::from_name("lzma"), Some(Method::Lzma));
        assert_eq!(Method::from_name("none"), Some(Method::None));
        assert_eq!(Method::from_name("gz"), Some(Method::Gz));
        assert_eq!(Method::from_name("gzip"), Some(Method::Gz));

        assert_eq!(Method::from_extension(".zst"), Some(Method::Zstd));
        assert_eq!(Method::from_extension(".br"), Some(Method::Brotli));
        assert_eq!(Method::from_extension(".lz4"), Some(Method::Lz4));
        assert_eq!(Method::from_extension(".lzma"), Some(Method::Lzma));
        assert_eq!(Method::from_extension(".gz"), Some(Method::Gz));
        assert_eq!(Method::from_extension(".gzip"), Some(Method::Gz));

        assert_eq!(Method::Zstd.extension(), "zst");
        assert_eq!(Method::Brotli.extension(), "br");
        assert_eq!(Method::Lz4.extension(), "lz4");
        assert_eq!(Method::Lzma.extension(), "lzma");
        assert_eq!(Method::Gz.extension(), "gz");
    }

    #[test]
    fn availability_tracks_features() {
        assert!(Method::None.is_available());
        assert_eq!(Method::Lz4.is_available(), cfg!(feature = "lz4"));
        assert_eq!(Method::Gz.feature(), "gzip");
        assert!(Method::default().is_available());
    }

    #[cfg(not(all(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "lzma",
        feature = "gzip"
    )))]
    #[tokio::test]
    async fn unavailable_codec_errors_on_read() {
        let m = [
            Method::Zstd,
            Method::Lz4,
            Method::Brotli,
            Method::Lzma,
            Method::Gz,
        ]
        .into_iter()
        .find(|m| !m.is_available())
        .unwrap();
        for mut r in [encode(m, reader(b"x"), 3), decode(m, reader(b"x"))] {
            let err = r.read_to_end(&mut Vec::new()).await.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
            assert!(err.to_string().contains(m.feature()), "{err}");
        }
    }
}
