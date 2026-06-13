//! Synchronous binary readers for XLOG records & pages
//!
//! Mirrors wal-g read_xlog_record.go / read_xlog_page.go. Operates on
//! `&[u8]` (cheap, no alloc beyond the record body) rather than Go's
//! reader-of-readers — same logic, fewer indirections
//!
//! All multi-byte fields are little-endian. Inputs from the WAL stream
//! are 8-byte aligned (`X_LOG_RECORD_ALIGNMENT`); the `AlignedReader`
//! helper hides that padding from the record-walking code

use thiserror::Error;

use super::all_zero;
use super::types::{
    BKP_IMAGE_HAS_HOLE, BLOCK_SIZE, BlockLocation, RM_NEXT_FREE_ID, RelFileNode, RmId,
    WAL_PAGE_SIZE, X_LOG_RECORD_ALIGNMENT, X_LOG_RECORD_HEADER_SIZE, X_LOG_SWITCH,
    XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT, XLR_BLOCK_ID_ORIGIN,
    XLR_BLOCK_ID_TOPLEVEL_XID, XLR_INFO_MASK, XLR_MAX_BLOCK_ID, XLogPageHeader, XLogRecord,
    XLogRecordBlock, XLogRecordBlockHeader, XLogRecordBlockImageHeader, XLogRecordHeader,
};

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("short read parsing {field}: need {need} bytes, have {have}")]
    Short {
        field: &'static str,
        need: usize,
        have: usize,
    },
    #[error("inconsistent total record length {0}, must be >= {X_LOG_RECORD_HEADER_SIZE}")]
    BadRecordLength(u32),
    #[error("invalid resource manager id {0}, must be < {RM_NEXT_FREE_ID}")]
    BadRmId(u8),
    #[error("zero record header: likely end of .partial or post WAL-switch padding")]
    ZeroRecordHeader,
    #[error("zero page header: end of .partial file or post WAL-switch")]
    ZeroPageHeader,
    #[error("page header has invalid flags/RemainingDataLen")]
    InvalidPageHeader,
    #[error("page is partial (last non-zero page of .partial file)")]
    PartialPage,
    #[error("whole page is zero bytes")]
    ZeroPage,
    #[error("invalid block id {0} (> {XLR_MAX_BLOCK_ID})")]
    BadBlockId(u8),
    #[error("out-of-order block id: got {actual}, last was {last}")]
    OutOfOrderBlock { actual: i32, last: i32 },
    #[error("block has_data={has_data} but data_length={data_length}")]
    InconsistentBlockData { has_data: bool, data_length: u16 },
    #[error("block image hole state inconsistent")]
    InconsistentImageHole,
    #[error(
        "block image length inconsistent: hasHole={has_hole} compressed={compressed} len={len}"
    )]
    InconsistentImageLength {
        has_hole: bool,
        compressed: bool,
        len: u16,
    },
    #[error("no previous RelFileNode but BkpBlockSameRel set")]
    NoPrevRelFileNode,
    #[error("not enough data to shrink: remaining {remaining}, requested {requested}")]
    ShrinkUnderflow { remaining: usize, requested: usize },
    #[error("expected continuation of current record, found new records instead")]
    ContinuationNotFound,
}

/// Header-area cursor: wraps a buffer with a side-channel "remaining bytes
/// in the header section" counter. Mirrors wal-g's ShrinkableReader.
/// Every `read_*` consumes from both buf & remaining; `shrink` reserves
/// future bytes (data + image bodies that follow the header walk) by
/// decrementing only remaining
struct HdrCursor<'a> {
    buf: &'a [u8],
    remaining: usize,
}

impl<'a> HdrCursor<'a> {
    fn new(buf: &'a [u8], remaining: usize) -> Self {
        Self { buf, remaining }
    }

    fn take(&mut self, n: usize, field: &'static str) -> Result<&'a [u8], ParseError> {
        if self.buf.len() < n {
            return Err(ParseError::Short {
                field,
                need: n,
                have: self.buf.len(),
            });
        }
        if self.remaining < n {
            return Err(ParseError::ShrinkUnderflow {
                remaining: self.remaining,
                requested: n,
            });
        }
        let (h, r) = self.buf.split_at(n);
        self.buf = r;
        self.remaining -= n;
        Ok(h)
    }

    fn read_u8(&mut self, field: &'static str) -> Result<u8, ParseError> {
        Ok(self.take(1, field)?[0])
    }
    fn read_u16(&mut self, field: &'static str) -> Result<u16, ParseError> {
        Ok(u16::from_le_bytes(self.take(2, field)?.try_into().unwrap()))
    }
    fn read_u32(&mut self, field: &'static str) -> Result<u32, ParseError> {
        Ok(u32::from_le_bytes(self.take(4, field)?.try_into().unwrap()))
    }

    /// Reserve future bytes (decrement remaining only, leave buf alone)
    fn shrink(&mut self, n: usize) -> Result<(), ParseError> {
        if self.remaining < n {
            return Err(ParseError::ShrinkUnderflow {
                remaining: self.remaining,
                requested: n,
            });
        }
        self.remaining -= n;
        Ok(())
    }
}

/// Convenience: read a fixed slice off the front of `buf`, advancing it.
/// Returns `ParseError::Short` if there's not enough data
fn take<'a>(buf: &mut &'a [u8], n: usize, field: &'static str) -> Result<&'a [u8], ParseError> {
    if buf.len() < n {
        return Err(ParseError::Short {
            field,
            need: n,
            have: buf.len(),
        });
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

fn read_u8(buf: &mut &[u8], field: &'static str) -> Result<u8, ParseError> {
    Ok(take(buf, 1, field)?[0])
}
fn read_u16(buf: &mut &[u8], field: &'static str) -> Result<u16, ParseError> {
    Ok(u16::from_le_bytes(take(buf, 2, field)?.try_into().unwrap()))
}
fn read_u32(buf: &mut &[u8], field: &'static str) -> Result<u32, ParseError> {
    Ok(u32::from_le_bytes(take(buf, 4, field)?.try_into().unwrap()))
}
fn read_u64(buf: &mut &[u8], field: &'static str) -> Result<u64, ParseError> {
    Ok(u64::from_le_bytes(take(buf, 8, field)?.try_into().unwrap()))
}

pub(crate) fn read_xlog_record_header(buf: &mut &[u8]) -> Result<XLogRecordHeader, ParseError> {
    let total_record_length = read_u32(buf, "totalRecordLength")?;
    let xact_id = read_u32(buf, "xactID")?;
    let prev_record_ptr = read_u64(buf, "prevRecordPtr")?;
    let info = read_u8(buf, "info")?;
    let resource_manager_id = read_u8(buf, "resourceManagerID")?;
    let _pad0 = read_u8(buf, "padding")?;
    let _pad1 = read_u8(buf, "padding")?;
    let crc32_hash = read_u32(buf, "crc32Hash")?;

    let h = XLogRecordHeader {
        total_record_length,
        xact_id,
        prev_record_ptr,
        info,
        resource_manager_id,
        crc32_hash,
    };
    check_record_header(&h)?;
    Ok(h)
}

fn check_record_header(h: &XLogRecordHeader) -> Result<(), ParseError> {
    if (h.total_record_length as usize) < X_LOG_RECORD_HEADER_SIZE {
        if h.is_zero() {
            return Err(ParseError::ZeroRecordHeader);
        }
        return Err(ParseError::BadRecordLength(h.total_record_length));
    }
    if h.resource_manager_id >= RM_NEXT_FREE_ID {
        return Err(ParseError::BadRmId(h.resource_manager_id));
    }
    Ok(())
}

fn read_rel_file_node(c: &mut HdrCursor<'_>) -> Result<RelFileNode, ParseError> {
    Ok(RelFileNode {
        spc_node: c.read_u32("spcNode")?,
        db_node: c.read_u32("dbNode")?,
        rel_node: c.read_u32("relNode")?,
    })
}

fn read_block_image_header(
    c: &mut HdrCursor<'_>,
    page_magic: u16,
) -> Result<XLogRecordBlockImageHeader, ParseError> {
    let image_length = c.read_u16("imageLength")?;
    let hole_offset = c.read_u16("imageHoleOffset")?;
    let info = c.read_u8("imageInfo")?;
    let mut h = XLogRecordBlockImageHeader {
        image_length,
        hole_offset,
        hole_length: 0,
        info,
    };
    if h.is_compressed(page_magic) {
        if h.has_hole() {
            h.hole_length = c.read_u16("imageHoleLength")?;
        }
    } else {
        h.hole_length = BLOCK_SIZE.saturating_sub(h.image_length);
    }
    check_image_header(&h, page_magic)?;
    Ok(h)
}

fn check_image_header(h: &XLogRecordBlockImageHeader, page_magic: u16) -> Result<(), ParseError> {
    let has_hole = h.info & BKP_IMAGE_HAS_HOLE != 0;
    let compressed = h.is_compressed(page_magic);
    if has_hole && (h.hole_offset == 0 || h.hole_length == 0 || h.image_length == BLOCK_SIZE) {
        return Err(ParseError::InconsistentImageHole);
    }
    if !has_hole && (h.hole_offset != 0 || h.hole_length != 0) {
        return Err(ParseError::InconsistentImageHole);
    }
    if compressed && h.image_length == BLOCK_SIZE {
        return Err(ParseError::InconsistentImageLength {
            has_hole,
            compressed: true,
            len: h.image_length,
        });
    }
    if !has_hole && !compressed && h.image_length != BLOCK_SIZE {
        return Err(ParseError::InconsistentImageLength {
            has_hole,
            compressed: false,
            len: h.image_length,
        });
    }
    Ok(())
}

fn read_block_location(
    same_rel: bool,
    last_rel: Option<RelFileNode>,
    c: &mut HdrCursor<'_>,
) -> Result<BlockLocation, ParseError> {
    let rel = if same_rel {
        last_rel.ok_or(ParseError::NoPrevRelFileNode)?
    } else {
        read_rel_file_node(c)?
    };
    let block_no = c.read_u32("blockNo")?;
    Ok(BlockLocation { rel, block_no })
}

fn read_block_header(
    last_rel: &mut Option<RelFileNode>,
    block_id: u8,
    max_block_id: &mut i32,
    c: &mut HdrCursor<'_>,
    page_magic: u16,
) -> Result<XLogRecordBlockHeader, ParseError> {
    if block_id > XLR_MAX_BLOCK_ID {
        return Err(ParseError::BadBlockId(block_id));
    }
    if (block_id as i32) <= *max_block_id {
        return Err(ParseError::OutOfOrderBlock {
            actual: block_id as i32,
            last: *max_block_id,
        });
    }
    *max_block_id = block_id as i32;
    let mut h = XLogRecordBlockHeader::new(block_id);
    h.fork_flags = c.read_u8("forkFlags")?;
    h.data_length = c.read_u16("dataLength")?;
    if (h.has_data() && h.data_length == 0) || (!h.has_data() && h.data_length != 0) {
        return Err(ParseError::InconsistentBlockData {
            has_data: h.has_data(),
            data_length: h.data_length,
        });
    }
    // Reserve the future body bytes so the header-walk loop terminates
    // after the right number of header bytes (matches wal-g `Shrink`)
    c.shrink(h.data_length as usize)?;

    if h.has_image() {
        h.image_header = read_block_image_header(c, page_magic)?;
        c.shrink(h.image_header.image_length as usize)?;
    }
    let loc = read_block_location(h.has_same_rel(), *last_rel, c)?;
    *last_rel = Some(loc.rel);
    h.location = loc;
    Ok(h)
}

/// Walk the header area of a record. Caller has already consumed the
/// 24-byte header; `buf` points at the start of the body
fn read_block_header_part<'a>(
    record: &mut XLogRecord<'a>,
    buf: &mut &'a [u8],
    page_magic: u16,
) -> Result<(), ParseError> {
    let total = record.header.total_record_length as usize;
    if total < X_LOG_RECORD_HEADER_SIZE {
        return Err(ParseError::BadRecordLength(
            record.header.total_record_length,
        ));
    }
    let body_len = total - X_LOG_RECORD_HEADER_SIZE;
    let mut c = HdrCursor::new(buf, body_len);

    let mut last_rel: Option<RelFileNode> = None;
    let mut max_block_id: i32 = -1;

    while c.remaining > 0 {
        let block_id = c.read_u8("blockId")?;
        match block_id {
            XLR_BLOCK_ID_DATA_SHORT => {
                let len = c.read_u8("mainDataLen8")?;
                record.main_data_len = len as u32;
                c.shrink(len as usize)?;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                let len = c.read_u32("mainDataLen32")?;
                record.main_data_len = len;
                c.shrink(len as usize)?;
            }
            XLR_BLOCK_ID_ORIGIN => {
                let o = c.read_u16("origin")?;
                record.origin = o;
            }
            XLR_BLOCK_ID_TOPLEVEL_XID => {
                let xid = c.read_u32("toplevelXid")?;
                record.toplevel_xid = xid;
            }
            id => {
                let h =
                    read_block_header(&mut last_rel, id, &mut max_block_id, &mut c, page_magic)?;
                record.blocks.push(XLogRecordBlock {
                    header: h,
                    image: std::borrow::Cow::Borrowed(&[]),
                    data: std::borrow::Cow::Borrowed(&[]),
                });
            }
        }
    }
    *buf = c.buf;
    Ok(())
}

fn read_block_data_and_images<'a>(
    record: &mut XLogRecord<'a>,
    buf: &mut &'a [u8],
) -> Result<(), ParseError> {
    for b in record.blocks.iter_mut() {
        if b.header.has_image() {
            let n = b.header.image_header.image_length as usize;
            let head = take(buf, n, "blockImage")?;
            b.image = std::borrow::Cow::Borrowed(head);
        }
        if b.header.has_data() {
            let n = b.header.data_length as usize;
            let head = take(buf, n, "blockData")?;
            b.data = std::borrow::Cow::Borrowed(head);
        }
    }
    Ok(())
}

fn read_xlog_record_main_data<'a>(len: u32, buf: &mut &'a [u8]) -> Result<&'a [u8], ParseError> {
    take(buf, len as usize, "mainData")
}

pub(crate) fn read_xlog_record_body<'a>(
    header: XLogRecordHeader,
    buf: &mut &'a [u8],
    page_magic: u16,
) -> Result<XLogRecord<'a>, ParseError> {
    let mut record = XLogRecord {
        header,
        ..Default::default()
    };
    read_block_header_part(&mut record, buf, page_magic)?;
    read_block_data_and_images(&mut record, buf)?;
    let main_data = read_xlog_record_main_data(record.main_data_len, buf)?;
    record.main_data = std::borrow::Cow::Borrowed(main_data);
    Ok(record)
}

/// Parse a complete record body from raw bytes (caller already concatenated
/// the header + body across any page boundaries). `page_magic` is the
/// `XLogPageHeader.magic` of the page that *started* this record — controls
/// FPI flag interpretation (PG 15 reshuffled bimg_info bits).
///
/// The returned record borrows zero-copy from `data` for the block
/// images / block data / main_data payloads — call
/// [`XLogRecord::into_owned`] to bump to `'static` if the record must
/// outlive `data`.
pub fn parse_record_from_bytes(data: &[u8], page_magic: u16) -> Result<XLogRecord<'_>, ParseError> {
    let mut buf = data;
    let h = read_xlog_record_header(&mut buf)?;
    read_xlog_record_body(h, &mut buf, page_magic)
}

/// Walk a record's header area emitting `BlockLocation`s via `f`, without
/// allocating per-block `Cow` payloads. Image / data / main_data bodies are
/// skipped via the `HdrCursor::shrink` accounting just like the records
/// path, but never materialised. Used by `extract_locations_from_wal_file`
/// (delta-map build) where only locations are needed
pub fn for_each_block_location_in_record<F: FnMut(BlockLocation)>(
    record_data: &[u8],
    page_magic: u16,
    mut f: F,
) -> Result<(), ParseError> {
    let mut buf = record_data;
    let header = read_xlog_record_header(&mut buf)?;
    // WAL_SWITCH: header-only record, no blocks
    if header.resource_manager_id == RmId::Xlog as u8
        && (header.info & !XLR_INFO_MASK) == X_LOG_SWITCH
    {
        return Ok(());
    }
    let total = header.total_record_length as usize;
    if total < X_LOG_RECORD_HEADER_SIZE {
        return Err(ParseError::BadRecordLength(header.total_record_length));
    }
    let body_len = total - X_LOG_RECORD_HEADER_SIZE;
    let mut c = HdrCursor::new(buf, body_len);
    let mut last_rel: Option<RelFileNode> = None;
    let mut max_block_id: i32 = -1;
    while c.remaining > 0 {
        let block_id = c.read_u8("blockId")?;
        match block_id {
            XLR_BLOCK_ID_DATA_SHORT => {
                let len = c.read_u8("mainDataLen8")?;
                c.shrink(len as usize)?;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                let len = c.read_u32("mainDataLen32")?;
                c.shrink(len as usize)?;
            }
            XLR_BLOCK_ID_ORIGIN => {
                let _ = c.read_u16("origin")?;
            }
            XLR_BLOCK_ID_TOPLEVEL_XID => {
                let _ = c.read_u32("toplevelXid")?;
            }
            id => {
                let h =
                    read_block_header(&mut last_rel, id, &mut max_block_id, &mut c, page_magic)?;
                f(h.location);
            }
        }
    }
    Ok(())
}

// ─── page helpers ───────────────────────────────────────────────────────────

pub(crate) fn read_xlog_page_header(buf: &mut &[u8]) -> Result<XLogPageHeader, ParseError> {
    let magic = read_u16(buf, "magic")?;
    let info = read_u16(buf, "info")?;
    let timeline_id = read_u32(buf, "timelineID")?;
    let page_address = read_u64(buf, "pageAddress")?;
    let remaining_data_len = read_u32(buf, "remainingDataLen")?;
    let h = XLogPageHeader {
        magic,
        info,
        timeline_id,
        page_address,
        remaining_data_len,
    };
    if h.is_zero() {
        return Err(ParseError::ZeroPageHeader);
    }
    if !h.is_valid() {
        return Err(ParseError::InvalidPageHeader);
    }
    if h.is_long() {
        // long header trailer: systemID(8) + segmentSize(4) + xLogBlockSize(4)
        let _ = take(buf, 16, "longPageHeaderData")?;
    }
    Ok(h)
}

/// Page cursor with 8-byte alignment tracking (records start at aligned offsets)
pub(crate) struct AlignedReader<'a> {
    pub buf: &'a [u8],
    pub consumed: usize,
}

impl<'a> AlignedReader<'a> {
    pub fn take(&mut self, n: usize, field: &'static str) -> Result<&'a [u8], ParseError> {
        if self.buf.len() < n {
            return Err(ParseError::Short {
                field,
                need: n,
                have: self.buf.len(),
            });
        }
        let (h, r) = self.buf.split_at(n);
        self.buf = r;
        self.consumed += n;
        Ok(h)
    }

    /// Read & discard alignment padding so consumed % alignment == 0
    pub fn read_to_alignment(&mut self) -> Result<(), ParseError> {
        let pad = X_LOG_RECORD_ALIGNMENT - self.consumed % X_LOG_RECORD_ALIGNMENT;
        if pad == X_LOG_RECORD_ALIGNMENT {
            return Ok(());
        }
        if self.buf.len() < pad {
            // EOF mid-padding: silently absorb (matches wal-g EOF-on-alignment)
            self.consumed += self.buf.len();
            self.buf = &[];
            return Ok(());
        }
        self.buf = &self.buf[pad..];
        self.consumed += pad;
        Ok(())
    }
}

/// Try to pull one record off `ar` starting at the next aligned offset.
/// Returns `(record_data, whole)`:
///   - whole=true  → record body fits entirely on this page
///   - whole=false → only header (or part of body) fits; caller stitches
///     `record_data` together with the next page's data to complete
///
/// Empty `record_data` means EOF on this page before any read
pub(crate) fn try_read_xlog_record_data(
    ar: &mut AlignedReader<'_>,
) -> Result<(Vec<u8>, bool), ParseError> {
    ar.read_to_alignment()?;
    if ar.buf.is_empty() {
        return Ok((Vec::new(), false));
    }
    let want = X_LOG_RECORD_HEADER_SIZE.min(ar.buf.len());
    let header_bytes = ar.take(want, "recordHeader")?;
    if header_bytes.len() < X_LOG_RECORD_HEADER_SIZE {
        if !header_bytes.is_empty() && all_zero(header_bytes) {
            return Err(ParseError::ZeroRecordHeader);
        }
        return Ok((header_bytes.to_vec(), false));
    }
    let mut parse_buf = header_bytes;
    let header = read_xlog_record_header(&mut parse_buf)?;
    let body_size = header.total_record_length as usize - X_LOG_RECORD_HEADER_SIZE;
    let body_want = body_size.min(WAL_PAGE_SIZE as usize);
    let body_take = body_want.min(ar.buf.len());
    let body_bytes = ar.take(body_take, "recordBody")?;
    let whole = body_take == body_size;
    let mut data = Vec::with_capacity(header_bytes.len() + body_bytes.len());
    data.extend_from_slice(header_bytes);
    data.extend_from_slice(body_bytes);
    Ok((data, whole))
}

// ─── high-level extraction ──────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ExtractError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Flatten the block-locations referenced by a slice of records.
/// Equivalent to wal-g's `ExtractBlockLocations`
pub fn extract_block_locations(records: &[XLogRecord<'_>]) -> Vec<BlockLocation> {
    let mut out = Vec::new();
    for r in records {
        if r.is_zero() {
            continue;
        }
        for b in &r.blocks {
            out.push(b.header.location);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg::walparser::types::{RmId, X_LOG_SWITCH};

    pub(crate) fn encode_record_header(h: &XLogRecordHeader) -> Vec<u8> {
        let mut v = Vec::with_capacity(X_LOG_RECORD_HEADER_SIZE);
        v.extend_from_slice(&h.total_record_length.to_le_bytes());
        v.extend_from_slice(&h.xact_id.to_le_bytes());
        v.extend_from_slice(&h.prev_record_ptr.to_le_bytes());
        v.push(h.info);
        v.push(h.resource_manager_id);
        v.push(0);
        v.push(0);
        v.extend_from_slice(&h.crc32_hash.to_le_bytes());
        v
    }

    #[test]
    fn round_trip_header() {
        let h = XLogRecordHeader {
            total_record_length: 64,
            xact_id: 1234,
            prev_record_ptr: 0xdeadbeefcafebabe,
            info: 0x10,
            resource_manager_id: RmId::Heap as u8,
            crc32_hash: 0x12345678,
        };
        let bytes = encode_record_header(&h);
        let mut buf = bytes.as_slice();
        let h2 = read_xlog_record_header(&mut buf).unwrap();
        assert_eq!(h, h2);
        assert!(buf.is_empty());
    }

    #[test]
    fn zero_header_classified() {
        let bytes = vec![0u8; X_LOG_RECORD_HEADER_SIZE];
        let mut buf = bytes.as_slice();
        let err = read_xlog_record_header(&mut buf).unwrap_err();
        assert!(matches!(err, ParseError::ZeroRecordHeader));
    }

    #[test]
    fn bad_rmid_rejected() {
        let h = XLogRecordHeader {
            total_record_length: 64,
            resource_manager_id: 99,
            ..Default::default()
        };
        let bytes = encode_record_header(&h);
        let mut buf = bytes.as_slice();
        let err = read_xlog_record_header(&mut buf).unwrap_err();
        assert!(matches!(err, ParseError::BadRmId(99)));
    }

    #[test]
    fn short_header_returns_short() {
        let mut buf: &[u8] = &[0u8; 10];
        let err = read_xlog_record_header(&mut buf).unwrap_err();
        assert!(matches!(err, ParseError::Short { .. }));
    }

    #[test]
    fn extract_skips_zero_records() {
        let mut r = XLogRecord::default();
        r.blocks.push(XLogRecordBlock {
            header: XLogRecordBlockHeader {
                location: BlockLocation::new(1, 2, 3, 4),
                ..Default::default()
            },
            ..Default::default()
        });
        let zero = XLogRecord::default();
        let v = extract_block_locations(&[zero, r]);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], BlockLocation::new(1, 2, 3, 4));
    }

    #[test]
    fn wal_switch_extracts_nothing() {
        let mut r = XLogRecord::default();
        r.header.total_record_length = 24;
        r.header.resource_manager_id = RmId::Xlog as u8;
        r.header.info = X_LOG_SWITCH;
        assert!(r.is_wal_switch());
        let v = extract_block_locations(&[r]);
        assert!(v.is_empty());
    }

    /// Encode a complete record (one block, full-page image excluded, with
    /// data and main_data short) and parse it back
    #[test]
    fn round_trip_simple_record_with_one_block() {
        // Layout in body:
        //   block_id=0 (1 byte), forkFlags (1 byte), dataLength (2 bytes),
        //   rel(12 bytes) + blockNo(4 bytes) = location 16 bytes,
        //   short-data marker XLR_BLOCK_ID_DATA_SHORT (1) + len (1) = 2 bytes,
        //   then bodies in order: blockData (data_length bytes), mainData (len bytes)
        let block_data = [0xaa, 0xbb, 0xcc, 0xdd];
        let main_data = [0x11, 0x22];

        let mut body: Vec<u8> = Vec::new();
        // block 0 header
        body.push(0); // block_id 0
        body.push(super::super::types::BKP_BLOCK_HAS_DATA); // fork_flags: HAS_DATA only
        body.extend_from_slice(&(block_data.len() as u16).to_le_bytes());
        // rel file node (12) + block no (4)
        body.extend_from_slice(&100u32.to_le_bytes()); // spc
        body.extend_from_slice(&200u32.to_le_bytes()); // db
        body.extend_from_slice(&300u32.to_le_bytes()); // rel
        body.extend_from_slice(&7u32.to_le_bytes()); // blockNo
        // short-data marker
        body.push(XLR_BLOCK_ID_DATA_SHORT);
        body.push(main_data.len() as u8);
        // then block data, then main data
        body.extend_from_slice(&block_data);
        body.extend_from_slice(&main_data);

        let total_len = X_LOG_RECORD_HEADER_SIZE + body.len();
        let mut bytes = encode_record_header(&XLogRecordHeader {
            total_record_length: total_len as u32,
            xact_id: 0,
            prev_record_ptr: 0,
            info: 0,
            resource_manager_id: RmId::Heap as u8,
            crc32_hash: 0,
        });
        bytes.extend_from_slice(&body);

        let r = parse_record_from_bytes(&bytes, super::super::types::XLP_PAGE_MAGIC_PG14).unwrap();
        assert_eq!(r.blocks.len(), 1);
        let b = &r.blocks[0];
        assert_eq!(b.header.location.rel.spc_node, 100);
        assert_eq!(b.header.location.rel.db_node, 200);
        assert_eq!(b.header.location.rel.rel_node, 300);
        assert_eq!(b.header.location.block_no, 7);
        assert_eq!(&*b.data, &block_data[..]);
        assert_eq!(&*r.main_data, &main_data[..]);
        assert_eq!(r.main_data_len, main_data.len() as u32);
    }

    /// Regression: PG ≥ 15 sets bimg_info bit 0x02 (APPLY) on every FPI.
    /// Pre-fix walross treated 0x02 as IS_COMPRESSED → the strict
    /// check_image_header path rejected `compressed && image_length ==
    /// BLOCK_SIZE`. Encode a FPI with info=APPLY only, image_length =
    /// BLOCK_SIZE, no hole. With PG-15+ magic the record must parse;
    /// with PG-14 magic the same bytes must fail
    #[test]
    fn fpi_apply_bit_parses_on_pg15_rejects_on_pg14() {
        use crate::pg::walparser::types::{
            _BKP_IMAGE_APPLY_PG15, BKP_BLOCK_HAS_IMAGE, BLOCK_SIZE, RmId, X_LOG_RECORD_HEADER_SIZE,
            XLP_PAGE_MAGIC_PG14, XLP_PAGE_MAGIC_PG15,
        };

        let image = vec![0xABu8; BLOCK_SIZE as usize];

        let mut body: Vec<u8> = Vec::new();
        // block 0 header: id=0, HAS_IMAGE, data_length=0
        body.push(0);
        body.push(BKP_BLOCK_HAS_IMAGE);
        body.extend_from_slice(&0u16.to_le_bytes());
        // image header: length=BLOCK_SIZE, hole_offset=0, info=APPLY only
        body.extend_from_slice(&(BLOCK_SIZE).to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        body.push(_BKP_IMAGE_APPLY_PG15);
        // rel(12) + block_no(4)
        body.extend_from_slice(&100u32.to_le_bytes());
        body.extend_from_slice(&200u32.to_le_bytes());
        body.extend_from_slice(&300u32.to_le_bytes());
        body.extend_from_slice(&7u32.to_le_bytes());
        // image bytes
        body.extend_from_slice(&image);

        let total = X_LOG_RECORD_HEADER_SIZE + body.len();
        let mut bytes = encode_record_header(&XLogRecordHeader {
            total_record_length: total as u32,
            resource_manager_id: RmId::Heap as u8,
            ..Default::default()
        });
        bytes.extend_from_slice(&body);

        let pg15 = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).expect("pg15+ accepts");
        assert_eq!(pg15.blocks.len(), 1);
        assert_eq!(pg15.blocks[0].image.len(), BLOCK_SIZE as usize);
        assert!(
            !pg15.blocks[0]
                .header
                .image_header
                .is_compressed(XLP_PAGE_MAGIC_PG15)
        );

        let err = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG14).unwrap_err();
        assert!(
            matches!(
                err,
                ParseError::InconsistentImageLength {
                    compressed: true,
                    ..
                }
            ),
            "want InconsistentImageLength(compressed=true), got {err:?}"
        );
    }

    /// PG 14-style FPI: IS_COMPRESSED bit set (0x02), image shorter than
    /// BLOCK_SIZE → must parse cleanly under PG-14 magic. Same record
    /// under PG-15+ magic must reject (0x02 means APPLY, "uncompressed",
    /// and `image_length != BLOCK_SIZE && !has_hole` is inconsistent)
    #[test]
    fn fpi_pg14_compressed_bit_parses_on_pg14_rejects_on_pg15() {
        use crate::pg::walparser::types::{
            BKP_BLOCK_HAS_IMAGE, BKP_IMAGE_IS_COMPRESSED_PG14, RmId, X_LOG_RECORD_HEADER_SIZE,
            XLP_PAGE_MAGIC_PG14, XLP_PAGE_MAGIC_PG15,
        };

        let compressed = vec![0xCDu8; 2048];

        let mut body: Vec<u8> = Vec::new();
        body.push(0);
        body.push(BKP_BLOCK_HAS_IMAGE);
        body.extend_from_slice(&0u16.to_le_bytes());
        // image header: length=2048, hole_offset=0, info=IS_COMPRESSED
        body.extend_from_slice(&(compressed.len() as u16).to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        body.push(BKP_IMAGE_IS_COMPRESSED_PG14);
        body.extend_from_slice(&100u32.to_le_bytes());
        body.extend_from_slice(&200u32.to_le_bytes());
        body.extend_from_slice(&300u32.to_le_bytes());
        body.extend_from_slice(&7u32.to_le_bytes());
        body.extend_from_slice(&compressed);

        let total = X_LOG_RECORD_HEADER_SIZE + body.len();
        let mut bytes = encode_record_header(&XLogRecordHeader {
            total_record_length: total as u32,
            resource_manager_id: RmId::Heap as u8,
            ..Default::default()
        });
        bytes.extend_from_slice(&body);

        let pg14 = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG14).expect("pg14 accepts");
        assert_eq!(pg14.blocks[0].image.len(), compressed.len());
        assert!(
            pg14.blocks[0]
                .header
                .image_header
                .is_compressed(XLP_PAGE_MAGIC_PG14)
        );

        // PG15 reading PG14-compressed bytes: 0x02 means APPLY (uncompressed),
        // so parser computes hole_length = BLOCK_SIZE - image_length on the
        // !compressed branch, then check_image_header rejects because
        // !has_hole but hole_length != 0
        let err = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).unwrap_err();
        assert!(
            matches!(err, ParseError::InconsistentImageHole),
            "want InconsistentImageHole, got {err:?}"
        );
    }

    /// PG 15+ pglz-compressed FPI (info=COMPRESS_PGLZ, bit 0x04). Must
    /// parse under PG-15+ magic; under PG-14 magic the bit 0x04 was
    /// "APPLY" (advisory, no compression) so the parser would expect a
    /// full-BLOCK image, mismatching the 2048-byte payload
    #[test]
    fn fpi_pg15_pglz_parses_on_pg15() {
        use crate::pg::walparser::types::{
            BKP_BLOCK_HAS_IMAGE, BKP_IMAGE_COMPRESS_PGLZ, RmId, X_LOG_RECORD_HEADER_SIZE,
            XLP_PAGE_MAGIC_PG15,
        };

        let compressed = vec![0xEFu8; 2048];
        let mut body: Vec<u8> = Vec::new();
        body.push(0);
        body.push(BKP_BLOCK_HAS_IMAGE);
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&(compressed.len() as u16).to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        body.push(BKP_IMAGE_COMPRESS_PGLZ);
        body.extend_from_slice(&100u32.to_le_bytes());
        body.extend_from_slice(&200u32.to_le_bytes());
        body.extend_from_slice(&300u32.to_le_bytes());
        body.extend_from_slice(&7u32.to_le_bytes());
        body.extend_from_slice(&compressed);

        let total = X_LOG_RECORD_HEADER_SIZE + body.len();
        let mut bytes = encode_record_header(&XLogRecordHeader {
            total_record_length: total as u32,
            resource_manager_id: RmId::Heap as u8,
            ..Default::default()
        });
        bytes.extend_from_slice(&body);

        let r = parse_record_from_bytes(&bytes, XLP_PAGE_MAGIC_PG15).expect("pg15 accepts pglz");
        assert!(
            r.blocks[0]
                .header
                .image_header
                .is_compressed(XLP_PAGE_MAGIC_PG15)
        );
        assert_eq!(r.blocks[0].image.len(), compressed.len());
    }
}
