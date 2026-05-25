//! Suffix classification & async-decompressing open for on-disk WAL segment files
//!
//! WAL segments live on disk under several name shapes depending on the
//! producer:
//!
//! - `00000001000000000000001A`           — raw 16 MiB segment (PG default)
//! - `00000001000000000000001A.partial`   — pg_receivewal in-progress segment
//! - `00000001000000000000001A.zst`       — zstd-compressed archive
//! - `00000001000000000000001A.zst.partial` — pg_receivewal mid-segment, zstd
//! - `.gz`, `.lz4`, `.lzma`, `.br`        — archive_command / wal-g variants
//!
//! `classify_segment_name` peels exactly one `.partial` suffix then exactly
//! one compression suffix and parses the bare name strictly. `open_segment_file`
//! wires the classification through `compression::decode` so callers see an
//! `AsyncRead` of plaintext bytes regardless of on-disk codec
//!
//! No magic-bytes sniffing. Operators never rename WAL files in archive; an
//! unrecognised suffix is a configuration error, not a recoverable case

use std::path::Path;

use thiserror::Error;
use tokio::fs::File;

use crate::compression::{self, AsyncReader, Method};

use super::segment::{SegmentError, SegmentName};

const PARTIAL_SUFFIX: &str = "partial";

#[derive(Debug, Error)]
pub enum ClassifyError {
    #[error("path has no file name component: {0}")]
    NoFileName(String),
    #[error("unrecognised compression suffix `{ext}` on {name}")]
    UnknownSuffix { name: String, ext: String },
    #[error("segment name: {0}")]
    Segment(#[from] SegmentError),
}

/// Pure suffix classifier. Peels `.partial` then one compression suffix; the
/// remaining stem must be 24 hex chars
pub fn classify_segment_name(name: &str) -> Result<(SegmentName, Method), ClassifyError> {
    let after_partial = match name.rsplit_once('.') {
        Some((stem, PARTIAL_SUFFIX)) => stem,
        _ => name,
    };

    let (bare, method) = match after_partial.rsplit_once('.') {
        Some((stem, ext)) => match Method::from_extension(ext) {
            Some(m) => (stem, m),
            // Stem is bare-24-hex with a trailing `.something` that isn't a
            // known codec: hard error. Avoids silently treating
            // `name.bogus` as raw uncompressed
            None => {
                return Err(ClassifyError::UnknownSuffix {
                    name: name.to_string(),
                    ext: ext.to_string(),
                });
            }
        },
        None => (after_partial, Method::None),
    };

    Ok((SegmentName::parse(bare)?, method))
}

/// `classify_segment_name` over `Path::file_name`
pub fn classify_segment_path(path: &Path) -> Result<(SegmentName, Method), ClassifyError> {
    path.file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| ClassifyError::NoFileName(path.display().to_string()))
        .and_then(classify_segment_name)
}

/// Open a WAL segment file & return an async reader of plaintext bytes
pub async fn open_segment_file(path: &Path) -> Result<(SegmentName, AsyncReader), OpenError> {
    let (name, method) = classify_segment_path(path)?;
    let file = File::open(path).await.map_err(OpenError::Io)?;
    let reader: AsyncReader = Box::pin(file);
    Ok((name, compression::decode(method, reader)))
}

#[derive(Debug, Error)]
pub enum OpenError {
    #[error("classify: {0}")]
    Classify(#[from] ClassifyError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::io::AsyncReadExt;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn classify_raw() {
        let (s, m) = classify_segment_path(&p("/x/000000010000000000000001")).unwrap();
        assert_eq!(s.format(), "000000010000000000000001");
        assert_eq!(m, Method::None);
    }

    #[test]
    fn classify_partial_no_codec() {
        let (s, m) = classify_segment_path(&p("/x/000000010000000000000001.partial")).unwrap();
        assert_eq!(s.format(), "000000010000000000000001");
        assert_eq!(m, Method::None);
    }

    #[test]
    fn classify_each_codec() {
        for (ext, method) in [
            ("zst", Method::Zstd),
            ("br", Method::Brotli),
            ("lz4", Method::Lz4),
            ("lzma", Method::Lzma),
            ("gz", Method::Gz),
        ] {
            let path = format!("/x/00000001000000000000001A.{ext}");
            let (s, m) = classify_segment_path(&p(&path)).unwrap();
            assert_eq!(s.format(), "00000001000000000000001A");
            assert_eq!(m, method, "{ext}");
        }
    }

    #[test]
    fn classify_codec_then_partial() {
        let (s, m) = classify_segment_path(&p("/x/00000001000000000000001A.zst.partial")).unwrap();
        assert_eq!(s.format(), "00000001000000000000001A");
        assert_eq!(m, Method::Zstd);
    }

    #[test]
    fn classify_partial_then_codec_rejected() {
        // `name.partial.zst` would be an unusual layout — `.partial` should be
        // outermost. After peeling the outer `.zst`, `name.partial` reaches
        // SegmentName::parse which rejects "partial" as non-hex
        let err = classify_segment_path(&p("/x/00000001000000000000001A.partial.zst")).unwrap_err();
        match err {
            ClassifyError::Segment(_) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn classify_unknown_suffix_loud() {
        let err = classify_segment_path(&p("/x/00000001000000000000001A.bogus")).unwrap_err();
        matches!(err, ClassifyError::UnknownSuffix { .. });
    }

    #[test]
    fn classify_non_hex_stem_rejected() {
        let err = classify_segment_path(&p("/x/notasegment.zst")).unwrap_err();
        matches!(err, ClassifyError::Segment(_));
    }

    async fn write_temp(bytes: &[u8], path: &Path) {
        tokio::fs::write(path, bytes).await.unwrap();
    }

    async fn round_trip(method: Method, ext: &str) {
        let dir = tempfile::tempdir().unwrap();
        let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();

        let mut encoded = Vec::new();
        let mut enc =
            compression::encode(method, Box::pin(std::io::Cursor::new(payload.clone())), 3);
        enc.read_to_end(&mut encoded).await.unwrap();

        let name = "000000010000000000000007";
        let file_name = if ext.is_empty() {
            name.to_string()
        } else {
            format!("{name}.{ext}")
        };
        let p = dir.path().join(&file_name);
        write_temp(&encoded, &p).await;

        let (parsed, mut reader) = open_segment_file(&p).await.unwrap();
        assert_eq!(parsed.format(), name);
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await.unwrap();
        assert_eq!(decoded, payload, "{method:?} round-trip mismatch");
    }

    #[tokio::test]
    async fn open_round_trip_all_codecs() {
        round_trip(Method::None, "").await;
        round_trip(Method::Zstd, "zst").await;
        round_trip(Method::Brotli, "br").await;
        round_trip(Method::Lz4, "lz4").await;
        round_trip(Method::Lzma, "lzma").await;
        round_trip(Method::Gz, "gz").await;
    }
}
