//! XLOG primitive types & flag predicates
//!
//! Layout & semantics match postgres
//! src/include/access/{xlogrecord,xlog_internal,storage/relfilenode}.h
//! Field order kept identical to wal-g so the binary readers in `parse.rs`
//! map 1:1 onto the postgres on-disk format
//!
//! All multi-byte fields little-endian (postgres native on every supported
//! arch). 8-byte alignment between records is handled by `parse::AlignedReader`

pub type Oid = u32;
pub type TimeLineId = u32;
pub type XLogRecordPtr = u64;

/// pg compile-time defaults — non-default values not supported (wal-g same)
pub const WAL_PAGE_SIZE: u16 = 8192;
pub const BLOCK_SIZE: u16 = 8192;

pub const X_LOG_RECORD_HEADER_SIZE: usize = 24;
pub const X_LOG_RECORD_ALIGNMENT: usize = 8;

// XLogRecordHeader.Info flag bits
pub const XLR_INFO_MASK: u8 = 0x0F;
pub const _XLR_RMGR_INFO_MASK: u8 = 0xF0;
pub const _XLR_SPECIAL_REL_UPDATE: u8 = 0x01;
pub const _XLR_CHECK_CONSISTENCY: u8 = 0x02;
pub const X_LOG_SWITCH: u8 = 0x40;

// XLogRecordBlockHeader special block IDs
pub const XLR_MAX_BLOCK_ID: u8 = 32;
pub const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: u8 = 253;

// XLogRecordBlockHeader.ForkFlags bit layout
pub const BKP_BLOCK_FORK_MASK: u8 = 0x0F;
pub const _BKP_BLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKP_BLOCK_HAS_IMAGE: u8 = 0x10;
pub const BKP_BLOCK_HAS_DATA: u8 = 0x20;
pub const BKP_BLOCK_WILL_INIT: u8 = 0x40;
pub const BKP_BLOCK_SAME_REL: u8 = 0x80;

// XLogRecordBlockImageHeader.Info bits.
//
// Bit layout shifted in PG 15 (commit a14354c, "Add WAL compression
// methods"). Caller passes `pg15_or_later` derived from page magic so
// `is_compressed` reads the right bits.
//
// PG ≤ 14:
//   0x01 HAS_HOLE
//   0x02 IS_COMPRESSED (pglz only)
//   0x04 APPLY (advisory, PG 13/14)
//
// PG ≥ 15:
//   0x01 HAS_HOLE
//   0x02 APPLY              <-- bit moved
//   0x04 COMPRESS_PGLZ
//   0x08 COMPRESS_LZ4
//   0x10 COMPRESS_ZSTD
pub const BKP_IMAGE_HAS_HOLE: u8 = 0x01;
pub const BKP_IMAGE_IS_COMPRESSED_PG14: u8 = 0x02;
pub const _BKP_IMAGE_APPLY_PG15: u8 = 0x02;
pub const BKP_IMAGE_COMPRESS_PGLZ: u8 = 0x04;
pub const BKP_IMAGE_COMPRESS_LZ4: u8 = 0x08;
pub const BKP_IMAGE_COMPRESS_ZSTD: u8 = 0x10;
pub const BKP_IMAGE_COMPRESS_MASK_PG15: u8 =
    BKP_IMAGE_COMPRESS_PGLZ | BKP_IMAGE_COMPRESS_LZ4 | BKP_IMAGE_COMPRESS_ZSTD;

/// Page magic per PG major, monotonic. Only the values walparser uses
/// are listed; `magic >= XLP_PAGE_MAGIC_PG15` reads "stream uses the
/// PG-15-style FPI bit layout"
pub const XLP_PAGE_MAGIC_PG14: u16 = 0xD10D;
pub const XLP_PAGE_MAGIC_PG15: u16 = 0xD110;

// XLogPageHeader.Info flag bits
pub const XLP_FIRST_IS_CONT_RECORD: u16 = 0x0001;
pub const XLP_LONG_HEADER: u16 = 0x0002;
pub const _XLP_BKP_REMOVABLE: u16 = 0x0004;
pub const XLP_ALL_FLAGS: u16 = 0x0007;

/// Resource Manager IDs. PG 13 baseline. PG 17 adds RM_LOGICAL_MESSAGE_ID
/// already covered, PG 14 introduced no new RMs. List ordered to match
/// pg src/include/access/rmgrlist.h
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RmId {
    Xlog = 0,
    Xact = 1,
    Smgr = 2,
    Clog = 3,
    Dbase = 4,
    Tblspc = 5,
    MultiXact = 6,
    RelMap = 7,
    Standby = 8,
    Heap2 = 9,
    Heap = 10,
    Btree = 11,
    Hash = 12,
    Gin = 13,
    Gist = 14,
    Seq = 15,
    Spgist = 16,
    Brin = 17,
    CommitTs = 18,
    ReplOrigin = 19,
    Generic = 20,
    LogicalMsg = 21,
}

pub const RM_NEXT_FREE_ID: u8 = 22;

/// Postgres RelFileNode — uniquely identifies an on-disk relation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Ord, PartialOrd)]
pub struct RelFileNode {
    pub spc_node: Oid,
    pub db_node: Oid,
    pub rel_node: Oid,
}

/// `(RelFileNode, BlockNo)` — a single page in a single relfile
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Ord, PartialOrd)]
pub struct BlockLocation {
    pub rel: RelFileNode,
    pub block_no: u32,
}

impl BlockLocation {
    pub fn new(spc: Oid, db: Oid, rel: Oid, block_no: u32) -> Self {
        Self {
            rel: RelFileNode {
                spc_node: spc,
                db_node: db,
                rel_node: rel,
            },
            block_no,
        }
    }

    /// All-zero sentinel: terminator in delta file streams
    pub fn terminal() -> Self {
        Self::default()
    }

    pub fn is_terminal(&self) -> bool {
        self == &Self::terminal()
    }
}

/// XLogRecordHeader — fixed 24 bytes preceding every record
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct XLogRecordHeader {
    pub total_record_length: u32,
    pub xact_id: u32,
    pub prev_record_ptr: XLogRecordPtr,
    pub info: u8,
    pub resource_manager_id: u8,
    /* 2 bytes of zero padding follow on disk */
    pub crc32_hash: u32,
}

impl XLogRecordHeader {
    pub fn is_zero(&self) -> bool {
        self.total_record_length == 0
            && self.xact_id == 0
            && self.prev_record_ptr == 0
            && self.info == 0
            && self.resource_manager_id == 0
            && self.crc32_hash == 0
    }
}

/// XLogRecord — header + decoded block headers + main data.
///
/// The bulk byte fields (`main_data`, per-block `image` / `data`) use
/// [`Cow<'a, [u8]>`] so the parser can borrow zero-copy into the
/// caller's input buffer; constructors that genuinely need ownership
/// (tests, callers that store records across borrow lifetimes) wrap
/// `Cow::Owned`. Use [`Self::into_owned`] to materialise a
/// `XLogRecord<'static>` when crossing a borrow boundary.
#[derive(Debug, Clone)]
pub struct XLogRecord<'a> {
    pub header: XLogRecordHeader,
    pub main_data_len: u32,
    pub origin: u16,
    pub blocks: Vec<XLogRecordBlock<'a>>,
    pub main_data: std::borrow::Cow<'a, [u8]>,
}

impl<'a> Default for XLogRecord<'a> {
    fn default() -> Self {
        Self {
            header: XLogRecordHeader::default(),
            main_data_len: 0,
            origin: 0,
            blocks: Vec::new(),
            main_data: std::borrow::Cow::Borrowed(&[]),
        }
    }
}

impl<'a> XLogRecord<'a> {
    pub fn is_zero(&self) -> bool {
        self.header.is_zero()
            && self.main_data_len == 0
            && self.origin == 0
            && self.blocks.is_empty()
            && self.main_data.is_empty()
    }

    /// XLOG_SWITCH (info=0x40, rmid=RM_XLOG): rest of segment is padding
    pub fn is_wal_switch(&self) -> bool {
        self.header.resource_manager_id == RmId::Xlog as u8
            && (self.header.info & !XLR_INFO_MASK) == X_LOG_SWITCH
    }

    /// Collapse every borrowed payload into owned `Vec<u8>`s so the
    /// record outlives the source buffer. Test sinks that store
    /// records call this to bump the lifetime to `'static`.
    pub fn into_owned(self) -> XLogRecord<'static> {
        XLogRecord {
            header: self.header,
            main_data_len: self.main_data_len,
            origin: self.origin,
            blocks: self.blocks.into_iter().map(|b| b.into_owned()).collect(),
            main_data: std::borrow::Cow::Owned(self.main_data.into_owned()),
        }
    }
}

/// One block reference inside an XLogRecord
#[derive(Debug, Clone)]
pub struct XLogRecordBlock<'a> {
    pub header: XLogRecordBlockHeader,
    pub image: std::borrow::Cow<'a, [u8]>,
    pub data: std::borrow::Cow<'a, [u8]>,
}

impl<'a> Default for XLogRecordBlock<'a> {
    fn default() -> Self {
        Self {
            header: XLogRecordBlockHeader::default(),
            image: std::borrow::Cow::Borrowed(&[]),
            data: std::borrow::Cow::Borrowed(&[]),
        }
    }
}

impl<'a> XLogRecordBlock<'a> {
    pub fn into_owned(self) -> XLogRecordBlock<'static> {
        XLogRecordBlock {
            header: self.header,
            image: std::borrow::Cow::Owned(self.image.into_owned()),
            data: std::borrow::Cow::Owned(self.data.into_owned()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct XLogRecordBlockHeader {
    pub block_id: u8,
    pub fork_flags: u8,
    pub data_length: u16,
    pub image_header: XLogRecordBlockImageHeader,
    pub location: BlockLocation,
}

impl XLogRecordBlockHeader {
    pub fn new(block_id: u8) -> Self {
        Self {
            block_id,
            ..Default::default()
        }
    }

    pub fn fork_num(&self) -> u8 {
        self.fork_flags & BKP_BLOCK_FORK_MASK
    }
    pub fn has_image(&self) -> bool {
        self.fork_flags & BKP_BLOCK_HAS_IMAGE != 0
    }
    pub fn has_data(&self) -> bool {
        self.fork_flags & BKP_BLOCK_HAS_DATA != 0
    }
    pub fn will_init(&self) -> bool {
        self.fork_flags & BKP_BLOCK_WILL_INIT != 0
    }
    pub fn has_same_rel(&self) -> bool {
        self.fork_flags & BKP_BLOCK_SAME_REL != 0
    }
}

#[derive(Debug, Clone, Default)]
pub struct XLogRecordBlockImageHeader {
    pub image_length: u16,
    pub hole_offset: u16,
    pub hole_length: u16,
    pub info: u8,
}

impl XLogRecordBlockImageHeader {
    pub fn has_hole(&self) -> bool {
        self.info & BKP_IMAGE_HAS_HOLE != 0
    }
    /// FPI compression predicate. PG 15 reshuffled bimg_info bits; pass
    /// the page magic from `XLogPageHeader.magic` so the right mask is
    /// applied. Future bit shifts add another comparison here
    pub fn is_compressed(&self, page_magic: u16) -> bool {
        if page_magic >= XLP_PAGE_MAGIC_PG15 {
            self.info & BKP_IMAGE_COMPRESS_MASK_PG15 != 0
        } else {
            self.info & BKP_IMAGE_IS_COMPRESSED_PG14 != 0
        }
    }
    /// Resolve PG-15+ compression method, or `None` for uncompressed.
    /// PG-14 fixtures collapse to `Some(Pglz)` when IS_COMPRESSED_PG14
    /// set
    pub fn compression_method(&self, page_magic: u16) -> Option<FpiCompressionMethod> {
        if page_magic >= XLP_PAGE_MAGIC_PG15 {
            if self.info & BKP_IMAGE_COMPRESS_PGLZ != 0 {
                Some(FpiCompressionMethod::Pglz)
            } else if self.info & BKP_IMAGE_COMPRESS_LZ4 != 0 {
                Some(FpiCompressionMethod::Lz4)
            } else if self.info & BKP_IMAGE_COMPRESS_ZSTD != 0 {
                Some(FpiCompressionMethod::Zstd)
            } else {
                None
            }
        } else {
            compression_method_pg14(self.info)
        }
    }
}

#[cold]
fn compression_method_pg14(info: u8) -> Option<FpiCompressionMethod> {
    if info & BKP_IMAGE_IS_COMPRESSED_PG14 != 0 {
        Some(FpiCompressionMethod::Pglz)
    } else {
        None
    }
}

/// FPI codec dispatch; mirrors `BKP_IMAGE_COMPRESS_*` flag set
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FpiCompressionMethod {
    Pglz,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, Default)]
pub struct XLogPageHeader {
    pub magic: u16,
    pub info: u16,
    pub timeline_id: TimeLineId,
    pub page_address: XLogRecordPtr,
    pub remaining_data_len: u32,
}

impl XLogPageHeader {
    pub fn is_long(&self) -> bool {
        self.info & XLP_LONG_HEADER != 0
    }
    pub fn has_continuation_record(&self) -> bool {
        self.info & XLP_FIRST_IS_CONT_RECORD != 0
    }
    pub fn is_zero(&self) -> bool {
        self.magic == 0
            && self.info == 0
            && self.timeline_id == 0
            && self.page_address == 0
            && self.remaining_data_len == 0
    }
    pub fn has_valid_flags(&self) -> bool {
        self.info & !XLP_ALL_FLAGS == 0
    }
    pub fn has_consistent_remaining_data_len(&self) -> bool {
        if self.has_continuation_record() {
            self.remaining_data_len != 0
        } else {
            self.remaining_data_len == 0
        }
    }
    pub fn is_valid(&self) -> bool {
        self.has_valid_flags() && self.has_consistent_remaining_data_len()
    }
}

/// One decoded WAL page: header + the partial trailing record from the
/// previous page (if any) + complete records on this page + start of a
/// record that overflows into the next page (if any)
#[derive(Debug, Clone, Default)]
pub struct XLogPage<'a> {
    pub header: XLogPageHeader,
    pub prev_record_trailing_data: Vec<u8>,
    pub records: Vec<XLogRecord<'a>>,
    pub next_record_heading_data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_location_terminal_round_trip() {
        let t = BlockLocation::terminal();
        assert!(t.is_terminal());
        let nt = BlockLocation::new(1663, 16384, 16385, 0);
        assert!(!nt.is_terminal());
    }

    #[test]
    fn block_header_predicates() {
        let mut h = XLogRecordBlockHeader::new(0);
        h.fork_flags = BKP_BLOCK_HAS_IMAGE | BKP_BLOCK_SAME_REL | 0x03; // forknum 3
        assert!(h.has_image());
        assert!(!h.has_data());
        assert!(!h.will_init());
        assert!(h.has_same_rel());
        assert_eq!(h.fork_num(), 3);
    }

    #[test]
    fn page_header_consistency() {
        let mut h = XLogPageHeader {
            info: XLP_FIRST_IS_CONT_RECORD,
            remaining_data_len: 100,
            ..Default::default()
        };
        assert!(h.has_consistent_remaining_data_len());
        h.remaining_data_len = 0;
        assert!(!h.has_consistent_remaining_data_len());
        h.info = 0;
        assert!(h.has_consistent_remaining_data_len());
    }

    #[test]
    fn compression_method_pg15() {
        let mut h = XLogRecordBlockImageHeader {
            info: 0,
            ..Default::default()
        };
        // no bits set -> None
        assert_eq!(h.compression_method(XLP_PAGE_MAGIC_PG15), None);

        h.info = BKP_IMAGE_COMPRESS_PGLZ;
        assert_eq!(
            h.compression_method(XLP_PAGE_MAGIC_PG15),
            Some(FpiCompressionMethod::Pglz),
        );

        h.info = BKP_IMAGE_COMPRESS_LZ4;
        assert_eq!(
            h.compression_method(XLP_PAGE_MAGIC_PG15),
            Some(FpiCompressionMethod::Lz4),
        );

        h.info = BKP_IMAGE_COMPRESS_ZSTD;
        assert_eq!(
            h.compression_method(XLP_PAGE_MAGIC_PG15),
            Some(FpiCompressionMethod::Zstd),
        );

        // APPLY-only (0x02) alongside HAS_HOLE is uncompressed
        h.info = _BKP_IMAGE_APPLY_PG15 | BKP_IMAGE_HAS_HOLE;
        assert_eq!(h.compression_method(XLP_PAGE_MAGIC_PG15), None);

        // codec bit + APPLY + HAS_HOLE still resolves codec
        h.info = _BKP_IMAGE_APPLY_PG15 | BKP_IMAGE_HAS_HOLE | BKP_IMAGE_COMPRESS_ZSTD;
        assert_eq!(
            h.compression_method(XLP_PAGE_MAGIC_PG15),
            Some(FpiCompressionMethod::Zstd),
        );
    }

    #[test]
    fn compression_method_pg14() {
        // PG-14 layout: bit 0x02 means IS_COMPRESSED (pglz)
        let mut h = XLogRecordBlockImageHeader {
            info: BKP_IMAGE_IS_COMPRESSED_PG14,
            ..Default::default()
        };
        assert_eq!(
            h.compression_method(XLP_PAGE_MAGIC_PG14),
            Some(FpiCompressionMethod::Pglz),
        );

        // PG-14 magic ignores PG-15 codec bits
        h.info = BKP_IMAGE_COMPRESS_LZ4;
        assert_eq!(h.compression_method(XLP_PAGE_MAGIC_PG14), None);

        h.info = 0;
        assert_eq!(h.compression_method(XLP_PAGE_MAGIC_PG14), None);
    }

    #[test]
    fn compression_method_magic_split() {
        // Same info byte resolves differently across the magic split.
        // Bit 0x02: PG-14 -> Pglz, PG-15 -> APPLY (no codec, None)
        let h = XLogRecordBlockImageHeader {
            info: BKP_IMAGE_IS_COMPRESSED_PG14,
            ..Default::default()
        };
        assert_eq!(
            h.compression_method(XLP_PAGE_MAGIC_PG14),
            Some(FpiCompressionMethod::Pglz),
        );
        assert_eq!(h.compression_method(XLP_PAGE_MAGIC_PG15), None);
    }

    #[test]
    fn wal_switch_classification() {
        let mut r = XLogRecord::default();
        r.header.resource_manager_id = RmId::Xlog as u8;
        r.header.info = X_LOG_SWITCH;
        assert!(r.is_wal_switch());
        r.header.info = X_LOG_SWITCH | 0x02; // rmgr-info bits set is fine, mask strips them
        assert!(r.is_wal_switch());
        r.header.resource_manager_id = RmId::Heap as u8;
        assert!(!r.is_wal_switch());
    }
}
