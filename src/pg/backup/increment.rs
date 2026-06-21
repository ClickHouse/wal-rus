//! Wire format for incremental backup file payloads
//!
//! Two flavors supported, both interoperable with wal-g:
//!
//! ### `wi1` (wal-g native, magic `[w,i,1,0x55]`)
//! ```text
//! 4  bytes  magic [b'w', b'i', b'1', 0x55]
//! 8  bytes  u64 LE: original file size (lets fetch pre-truncate)
//! 4  bytes  u32 LE: count of changed pages N
//! N*4 bytes block numbers of changed pages (u32 LE)
//! N*8192    changed page bodies (8 KiB each)
//! ```
//!
//! ### PG17 native INCREMENTAL (postgres source, magic `0xd3ae1f0d`)
//! Per `src/include/backup/basebackup_incremental.h` and
//! `src/backend/backup/basebackup.c:1625`. Emitted by `pg_basebackup
//! --incremental`, & PG17+ servers when INCREMENTAL option in effect:
//! ```text
//! 4  bytes  u32 LE: magic 0xd3ae1f0d
//! 4  bytes  u32 LE: num_blocks
//! 4  bytes  u32 LE: truncation_block_length (in BLCKSZ-blocks)
//! N*4 bytes block_numbers (u32 LE)
//! pad       zero bytes to next BLCKSZ boundary (only when num_blocks > 0
//!           and header isn't already aligned)
//! N*8192    block bodies
//! ```
//!
//! Used both ways:
//! - backup-push writes one of these per paged file that had any blocks
//!   modified since the parent backup's LSN
//! - backup-fetch dispatches by magic & applies each page at its target offset

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::delta::PG_PAGE_SIZE;

/// wal-g `wi1` magic bytes — `'w'`, `'i'`, version `'1'`, `0x55`
pub const INCREMENT_MAGIC: [u8; 4] = [b'w', b'i', b'1', 0x55];

/// PG17 native INCREMENTAL magic (`src/include/backup/basebackup_incremental.h`)
pub const NATIVE_INCREMENT_MAGIC: u32 = 0xd3ae_1f0d;

#[derive(Debug, Error)]
pub enum IncrementError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("invalid increment magic bytes: {0:?}")]
    BadMagic([u8; 4]),
    #[error("unknown increment version: {0}")]
    UnknownVersion(u8),
    #[error("unexpected trailing bytes after increment")]
    UnexpectedTrailing,
}

/// Serializes as `"wi1"` / `"native"`, matching the `--increment-format` CLI
/// value names. Recorded per-backup in the sentinel; absent fields read as
/// `Wi1` (wal-g sentinels and pre-field walrus backups)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    /// wal-g `wi1`
    #[default]
    Wi1,
    /// PG17 native INCREMENTAL
    Native,
}

/// Decoded `wi1` increment header (everything except page bodies)
#[derive(Debug, Clone)]
pub struct IncrementHeader {
    pub file_size: u64,
    pub blocks: Vec<u32>,
}

impl IncrementHeader {
    /// Compute total on-disk size: 4 (magic) + 8 (size) + 4 (count) +
    /// N*4 (block nos) + N*8192 (page bodies)
    pub fn total_size(&self) -> u64 {
        let n = self.blocks.len() as u64;
        4 + 8 + 4 + n * 4 + n * PG_PAGE_SIZE
    }
}

/// Decoded native INCREMENTAL header (everything except page bodies)
#[derive(Debug, Clone)]
pub struct NativeIncrementHeader {
    /// Truncation point (in BLCKSZ blocks). Target should be truncated to
    /// `truncation_block_length * BLCKSZ` after apply
    pub truncation_block_length: u32,
    pub blocks: Vec<u32>,
}

impl NativeIncrementHeader {
    /// Per postgres `GetIncrementalHeaderSize`: 3 * u32 + N * u32, padded to
    /// BLCKSZ only when `N > 0` and not already a multiple of BLCKSZ
    pub fn header_size_padded(&self) -> u64 {
        let raw = 12 + (self.blocks.len() as u64) * 4;
        if self.blocks.is_empty() {
            return raw;
        }
        let rem = raw % PG_PAGE_SIZE;
        if rem == 0 {
            raw
        } else {
            raw + (PG_PAGE_SIZE - rem)
        }
    }

    pub fn total_size(&self) -> u64 {
        self.header_size_padded() + (self.blocks.len() as u64) * PG_PAGE_SIZE
    }
}

// ─── wi1 reader / writer ────────────────────────────────────────────────────

/// Read & validate the `wi1` increment file header. Caller must have already
/// consumed (or not, see `apply_increment_in_place`) the 4-byte magic.
/// After this returns, the reader is positioned at the first page body
pub fn read_increment_header<R: Read>(mut r: R) -> Result<IncrementHeader, IncrementError> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if magic[0] != b'w' || magic[1] != b'i' || magic[3] != 0x55 {
        return Err(IncrementError::BadMagic(magic));
    }
    if magic[2] != b'1' {
        return Err(IncrementError::UnknownVersion(magic[2]));
    }
    read_wi1_after_magic(r)
}

fn read_wi1_after_magic<R: Read>(mut r: R) -> Result<IncrementHeader, IncrementError> {
    let mut buf8 = [0u8; 8];
    r.read_exact(&mut buf8)?;
    let file_size = u64::from_le_bytes(buf8);
    let mut buf4 = [0u8; 4];
    r.read_exact(&mut buf4)?;
    let n = u32::from_le_bytes(buf4) as usize;
    let mut blocks = Vec::with_capacity(n);
    for _ in 0..n {
        r.read_exact(&mut buf4)?;
        blocks.push(u32::from_le_bytes(buf4));
    }
    Ok(IncrementHeader { file_size, blocks })
}

/// Write the `wi1` increment header (magic + file_size + count + block_nos).
/// Caller writes the page bodies next, in the same block_no order
pub fn write_increment_header<W: Write>(
    mut w: W,
    file_size: u64,
    blocks: &[u32],
) -> Result<(), IncrementError> {
    w.write_all(&INCREMENT_MAGIC)?;
    w.write_all(&file_size.to_le_bytes())?;
    w.write_all(&(blocks.len() as u32).to_le_bytes())?;
    for &b in blocks {
        w.write_all(&b.to_le_bytes())?;
    }
    Ok(())
}

// ─── PG17 native reader / writer ────────────────────────────────────────────

/// Read native INCREMENTAL header (post-magic: caller has already consumed
/// the 4-byte magic). After this, the reader is positioned at the first
/// page body (any header padding bytes are consumed)
fn read_native_after_magic<R: Read>(mut r: R) -> Result<NativeIncrementHeader, IncrementError> {
    let mut buf4 = [0u8; 4];
    r.read_exact(&mut buf4)?;
    let num_blocks = u32::from_le_bytes(buf4) as usize;
    r.read_exact(&mut buf4)?;
    let truncation_block_length = u32::from_le_bytes(buf4);

    let mut blocks = Vec::with_capacity(num_blocks);
    if num_blocks > 0 {
        let mut blocks_buf = vec![0u8; num_blocks * 4];
        r.read_exact(&mut blocks_buf)?;
        for i in 0..num_blocks {
            blocks.push(u32::from_le_bytes(
                blocks_buf[i * 4..i * 4 + 4].try_into().unwrap(),
            ));
        }
        // Pad to BLCKSZ boundary (matches `make_incremental_rfile` in
        // `src/bin/pg_combinebackup/reconstruct.c`)
        let header_len = 4u64 + 4 + 4 + (num_blocks as u64) * 4;
        let rem = header_len % PG_PAGE_SIZE;
        if rem != 0 {
            let pad = PG_PAGE_SIZE - rem;
            let mut sink = vec![0u8; pad as usize];
            r.read_exact(&mut sink)?;
        }
    }
    Ok(NativeIncrementHeader {
        truncation_block_length,
        blocks,
    })
}

/// Write a complete native INCREMENTAL header (magic + num_blocks +
/// truncation + block_numbers + padding). Caller writes block bodies next
pub fn write_native_increment_header<W: Write>(
    mut w: W,
    truncation_block_length: u32,
    blocks: &[u32],
) -> Result<(), IncrementError> {
    w.write_all(&NATIVE_INCREMENT_MAGIC.to_le_bytes())?;
    w.write_all(&(blocks.len() as u32).to_le_bytes())?;
    w.write_all(&truncation_block_length.to_le_bytes())?;
    for &b in blocks {
        w.write_all(&b.to_le_bytes())?;
    }
    if !blocks.is_empty() {
        let header_len = 12u64 + (blocks.len() as u64) * 4;
        let rem = header_len % PG_PAGE_SIZE;
        if rem != 0 {
            let pad = vec![0u8; (PG_PAGE_SIZE - rem) as usize];
            w.write_all(&pad)?;
        }
    }
    Ok(())
}

// ─── unified apply (magic-based dispatch) ───────────────────────────────────

/// Apply an increment of either format on top of an existing file, in place.
/// Detects format by reading the first 4 bytes & dispatching by magic
///
/// Returns `(file_size_after_truncation, blocks_written, format)`. Callers
/// should `set_len(file_size_after_truncation)` on `target` afterwards
pub fn apply_increment_in_place<R, W>(
    increment: &mut R,
    target: &mut W,
) -> Result<(u64, usize, Format), IncrementError>
where
    R: Read,
    W: io::Write + io::Seek,
{
    let mut magic = [0u8; 4];
    increment.read_exact(&mut magic)?;

    // wi1 dispatch: 'w','i', version '1', 0x55
    if magic[0] == b'w' && magic[1] == b'i' && magic[3] == 0x55 {
        if magic[2] != b'1' {
            return Err(IncrementError::UnknownVersion(magic[2]));
        }
        let header = read_wi1_after_magic(&mut *increment)?;
        let n = apply_block_bodies(increment, target, &header.blocks)?;
        check_no_trailing(increment)?;
        return Ok((header.file_size, n, Format::Wi1));
    }

    // Native dispatch: 0xd3ae1f0d (LE)
    let magic_u32 = u32::from_le_bytes(magic);
    if magic_u32 == NATIVE_INCREMENT_MAGIC {
        let header = read_native_after_magic(&mut *increment)?;
        let n = apply_block_bodies(increment, target, &header.blocks)?;
        check_no_trailing(increment)?;
        let truncated_size = header.truncation_block_length as u64 * PG_PAGE_SIZE;
        return Ok((truncated_size, n, Format::Native));
    }

    Err(IncrementError::BadMagic(magic))
}

fn apply_block_bodies<R, W>(
    increment: &mut R,
    target: &mut W,
    blocks: &[u32],
) -> Result<usize, IncrementError>
where
    R: Read,
    W: io::Write + io::Seek,
{
    let mut page = vec![0u8; PG_PAGE_SIZE as usize];
    for &block_no in blocks {
        increment.read_exact(&mut page)?;
        target.seek(io::SeekFrom::Start(block_no as u64 * PG_PAGE_SIZE))?;
        target.write_all(&page)?;
    }
    Ok(blocks.len())
}

fn check_no_trailing<R: Read>(increment: &mut R) -> Result<(), IncrementError> {
    let mut probe = [0u8; 1];
    if increment.read(&mut probe)? != 0 {
        return Err(IncrementError::UnexpectedTrailing);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek, SeekFrom};

    // ─── wi1 ────────────────────────────────────────────────────────────────

    #[test]
    fn wi1_header_round_trip() {
        let mut buf = Vec::new();
        write_increment_header(&mut buf, 16384 * 8, &[0, 3, 100]).unwrap();
        let mut r = Cursor::new(buf);
        let h = read_increment_header(&mut r).unwrap();
        assert_eq!(h.file_size, 16384 * 8);
        assert_eq!(h.blocks, vec![0, 3, 100]);
    }

    #[test]
    fn wi1_bad_magic_rejected() {
        let mut buf = vec![b'x', b'y', b'1', 0x55];
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        let err = read_increment_header(&mut Cursor::new(buf)).unwrap_err();
        assert!(matches!(err, IncrementError::BadMagic(_)));
    }

    #[test]
    fn wi1_unknown_version_rejected() {
        let mut buf = vec![b'w', b'i', b'2', 0x55];
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        let err = read_increment_header(&mut Cursor::new(buf)).unwrap_err();
        assert!(matches!(err, IncrementError::UnknownVersion(b'2')));
    }

    #[test]
    fn wi1_apply_writes_at_block_offsets() {
        let mut target = Cursor::new(vec![0u8; PG_PAGE_SIZE as usize * 3]);
        let mut inc = Vec::new();
        write_increment_header(&mut inc, PG_PAGE_SIZE * 3, &[1]).unwrap();
        inc.extend(std::iter::repeat_n(0xAA, PG_PAGE_SIZE as usize));

        let mut inc_cursor = Cursor::new(inc);
        let (size, n, fmt) = apply_increment_in_place(&mut inc_cursor, &mut target).unwrap();
        assert_eq!(size, PG_PAGE_SIZE * 3);
        assert_eq!(n, 1);
        assert_eq!(fmt, Format::Wi1);

        target.seek(SeekFrom::Start(0)).unwrap();
        let mut b = vec![0u8; PG_PAGE_SIZE as usize];
        target.read_exact(&mut b).unwrap();
        assert!(b.iter().all(|&x| x == 0));
        target.read_exact(&mut b).unwrap();
        assert!(b.iter().all(|&x| x == 0xAA));
        target.read_exact(&mut b).unwrap();
        assert!(b.iter().all(|&x| x == 0));
    }

    #[test]
    fn wi1_trailing_data_rejected() {
        let mut target = Cursor::new(vec![0u8; PG_PAGE_SIZE as usize * 2]);
        let mut inc = Vec::new();
        write_increment_header(&mut inc, PG_PAGE_SIZE * 2, &[0]).unwrap();
        inc.extend(std::iter::repeat_n(0xCC, PG_PAGE_SIZE as usize));
        inc.push(0x42);
        let err = apply_increment_in_place(&mut Cursor::new(inc), &mut target).unwrap_err();
        assert!(matches!(err, IncrementError::UnexpectedTrailing));
    }

    // ─── native (PG17) ──────────────────────────────────────────────────────

    #[test]
    fn native_header_padding_aligned_to_blcksz() {
        // 1 block: raw = 12 + 4 = 16 bytes, padded to 8192
        let h = NativeIncrementHeader {
            truncation_block_length: 5,
            blocks: vec![3],
        };
        assert_eq!(h.header_size_padded(), PG_PAGE_SIZE);
        assert_eq!(h.total_size(), PG_PAGE_SIZE + PG_PAGE_SIZE);

        // 0 blocks: header stays at raw 12 bytes (matches postgres
        // `GetIncrementalHeaderSize` "keep it small")
        let h = NativeIncrementHeader {
            truncation_block_length: 5,
            blocks: vec![],
        };
        assert_eq!(h.header_size_padded(), 12);
        assert_eq!(h.total_size(), 12);
    }

    #[test]
    fn native_round_trip() {
        // Build a native increment that rewrites block 1 of a 5-block file,
        // & truncates to 5 blocks. Then apply on top of a base file with
        // marker 0xAA in every byte, & check block 1 is overwritten with 0xBB
        let blocks = [1u32];
        let mut inc = Vec::new();
        write_native_increment_header(&mut inc, 5, &blocks).unwrap();
        // Sanity: header padded to BLCKSZ
        assert_eq!(inc.len() as u64, PG_PAGE_SIZE);
        // block body for block 1
        inc.extend(std::iter::repeat_n(0xBB, PG_PAGE_SIZE as usize));

        let mut target = Cursor::new(vec![0xAA; PG_PAGE_SIZE as usize * 5]);
        let (size, n, fmt) = apply_increment_in_place(&mut Cursor::new(inc), &mut target).unwrap();
        assert_eq!(size, PG_PAGE_SIZE * 5);
        assert_eq!(n, 1);
        assert_eq!(fmt, Format::Native);

        target.seek(SeekFrom::Start(PG_PAGE_SIZE)).unwrap();
        let mut buf = vec![0u8; PG_PAGE_SIZE as usize];
        target.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn native_truncation_size_reflects_block_length() {
        // truncation_block_length = 3 ⇒ caller should truncate target to 3*BLCKSZ
        let blocks = [0u32, 2u32];
        let mut inc = Vec::new();
        write_native_increment_header(&mut inc, 3, &blocks).unwrap();
        inc.extend(std::iter::repeat_n(0x11, PG_PAGE_SIZE as usize));
        inc.extend(std::iter::repeat_n(0x22, PG_PAGE_SIZE as usize));

        let mut target = Cursor::new(vec![0u8; PG_PAGE_SIZE as usize * 4]);
        let (size, _, _) = apply_increment_in_place(&mut Cursor::new(inc), &mut target).unwrap();
        assert_eq!(size, PG_PAGE_SIZE * 3);
    }

    #[test]
    fn native_zero_blocks_unpadded_header() {
        // num_blocks=0 case: header is exactly 12 bytes, no padding, no body
        let mut inc = Vec::new();
        write_native_increment_header(&mut inc, 10, &[]).unwrap();
        assert_eq!(inc.len(), 12);

        let mut target = Cursor::new(vec![0u8; PG_PAGE_SIZE as usize * 10]);
        let (size, n, fmt) = apply_increment_in_place(&mut Cursor::new(inc), &mut target).unwrap();
        assert_eq!(size, PG_PAGE_SIZE * 10);
        assert_eq!(n, 0);
        assert_eq!(fmt, Format::Native);
    }

    #[test]
    fn native_trailing_data_rejected() {
        let blocks = [0u32];
        let mut inc = Vec::new();
        write_native_increment_header(&mut inc, 1, &blocks).unwrap();
        inc.extend(std::iter::repeat_n(0xCC, PG_PAGE_SIZE as usize));
        inc.push(0x42);
        let mut target = Cursor::new(vec![0u8; PG_PAGE_SIZE as usize]);
        let err = apply_increment_in_place(&mut Cursor::new(inc), &mut target).unwrap_err();
        assert!(matches!(err, IncrementError::UnexpectedTrailing));
    }

    #[test]
    fn apply_rejects_unknown_magic() {
        let mut target = Cursor::new(vec![0u8; PG_PAGE_SIZE as usize]);
        let buf = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00];
        let err = apply_increment_in_place(&mut Cursor::new(buf), &mut target).unwrap_err();
        assert!(matches!(err, IncrementError::BadMagic(_)));
    }

    #[test]
    fn apply_dispatches_both_formats() {
        // Same test as wal-g's TestApplyFileIncrementDualFormat — confirm
        // a single apply entry-point handles wi1 & native correctly
        let blcksz = PG_PAGE_SIZE as usize;
        let mut native_inc = Vec::new();
        write_native_increment_header(&mut native_inc, 3, &[1]).unwrap();
        native_inc.extend(std::iter::repeat_n(0xCC, blcksz));
        let mut t1 = Cursor::new(vec![0xAA; blcksz * 4]);
        let (size1, n1, fmt1) =
            apply_increment_in_place(&mut Cursor::new(native_inc), &mut t1).unwrap();
        assert_eq!(size1, PG_PAGE_SIZE * 3);
        assert_eq!(n1, 1);
        assert_eq!(fmt1, Format::Native);

        let mut wi1_inc = Vec::new();
        write_increment_header(&mut wi1_inc, PG_PAGE_SIZE * 4, &[2]).unwrap();
        wi1_inc.extend(std::iter::repeat_n(0xDD, blcksz));
        let mut t2 = Cursor::new(vec![0xAA; blcksz * 4]);
        let (size2, n2, fmt2) =
            apply_increment_in_place(&mut Cursor::new(wi1_inc), &mut t2).unwrap();
        assert_eq!(size2, PG_PAGE_SIZE * 4);
        assert_eq!(n2, 1);
        assert_eq!(fmt2, Format::Wi1);
    }
}
