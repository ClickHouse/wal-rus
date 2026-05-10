//! Streaming compression / decompression backed by async-compression
//!
//! Reader-to-reader transforms via tokio::bufread adapters; no thread bridge,
//! no full-segment buffering. Memory cost ~= codec window size + tokio BufReader.
//!
//! Legacy codecs (brotli/lz4/lzma) exist for compatibility with buckets written
//! by older wal-g deployments; zstd remains the default.

use std::pin::Pin;

use async_compression::Level;
use async_compression::tokio::bufread::{
    BrotliDecoder, BrotliEncoder, Lz4Decoder, Lz4Encoder, LzmaDecoder, LzmaEncoder, ZstdDecoder,
    ZstdEncoder,
};
use thiserror::Error;
use tokio::io::{AsyncRead, BufReader};

const BUF_CAPACITY: usize = 64 * 1024;

pub type AsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    None,
    Zstd,
    Brotli,
    Lz4,
    Lzma,
}

impl Method {
    pub fn from_name(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "none" => Some(Method::None),
            "zstd" => Some(Method::Zstd),
            "brotli" => Some(Method::Brotli),
            "lz4" => Some(Method::Lz4),
            "lzma" => Some(Method::Lzma),
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
        }
    }

    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.trim_start_matches('.') {
            "" => Some(Method::None),
            "zst" | "zstd" => Some(Method::Zstd),
            "br" | "brotli" => Some(Method::Brotli),
            "lz4" => Some(Method::Lz4),
            "lzma" => Some(Method::Lzma),
            _ => None,
        }
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
        Method::Zstd => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(ZstdEncoder::with_quality(buffered, Level::Precise(level)))
        }
        Method::Brotli => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(BrotliEncoder::with_quality(
                buffered,
                Level::Precise(brotli_quality(level)),
            ))
        }
        Method::Lz4 => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(Lz4Encoder::new(buffered))
        }
        Method::Lzma => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(LzmaEncoder::with_quality(
                buffered,
                Level::Precise(lzma_preset(level)),
            ))
        }
    }
}

pub fn decode(method: Method, input: AsyncReader) -> AsyncReader {
    match method {
        Method::None => input,
        Method::Zstd => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(ZstdDecoder::new(buffered))
        }
        Method::Brotli => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(BrotliDecoder::new(buffered))
        }
        Method::Lz4 => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(Lz4Decoder::new(buffered))
        }
        Method::Lzma => {
            let buffered = BufReader::with_capacity(BUF_CAPACITY, input);
            Box::pin(LzmaDecoder::new(buffered))
        }
    }
}

// brotli quality 0..=11; clamp caller-supplied zstd-shaped level into range
fn brotli_quality(level: i32) -> i32 {
    level.clamp(0, 11)
}

// lzma preset 0..=9
fn lzma_preset(level: i32) -> i32 {
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

    fn payload() -> Vec<u8> {
        (0..200_000u32).map(|i| (i % 251) as u8).collect()
    }

    async fn roundtrip(method: Method) {
        let original = payload();
        let enc = encode(method, reader(&original), 3);
        let mut dec = decode(method, enc);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, original, "{method:?} roundtrip mismatch");
    }

    #[tokio::test]
    async fn zstd_roundtrip() {
        roundtrip(Method::Zstd).await;
    }

    #[tokio::test]
    async fn brotli_roundtrip() {
        roundtrip(Method::Brotli).await;
    }

    #[tokio::test]
    async fn lz4_roundtrip() {
        roundtrip(Method::Lz4).await;
    }

    #[tokio::test]
    async fn lzma_roundtrip() {
        roundtrip(Method::Lzma).await;
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

        assert_eq!(Method::from_extension(".zst"), Some(Method::Zstd));
        assert_eq!(Method::from_extension(".br"), Some(Method::Brotli));
        assert_eq!(Method::from_extension(".lz4"), Some(Method::Lz4));
        assert_eq!(Method::from_extension(".lzma"), Some(Method::Lzma));

        assert_eq!(Method::Zstd.extension(), "zst");
        assert_eq!(Method::Brotli.extension(), "br");
        assert_eq!(Method::Lz4.extension(), "lz4");
        assert_eq!(Method::Lzma.extension(), "lzma");
    }
}
