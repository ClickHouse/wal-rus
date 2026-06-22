//! `WalParser` — stateful record reader that stitches records across pages
//!
//! A single XLogRecord can span two or more 8 KiB pages. `WalParser` holds
//! the bytes accumulated for a record that started on an earlier page;
//! when the next page's header is read it walks the continuation data &
//! emits the joined record. The state is checkpointable via `save`/`load`
//! so wal-g (and we) can stash partial state in a delta_NN sidecar file
//!
//! On disk: a delta file stores a list of `BlockLocation` tuples (16 bytes
//! each, LE u32×4) terminated by an all-zero tuple, then a u32 length
//! prefix + the parser's `current_record_data` bytes. See wal-g
//! `block_location_writer.go` & `wal_parser.go::Save`

use std::io::{self, Read, Write};

use thiserror::Error;

use super::all_zero;
use super::parse::{
    AlignedReader, ExtractError, ParseError, for_each_block_location_in_record,
    parse_record_from_bytes, read_xlog_page_header, read_xlog_record_header,
    try_read_xlog_record_data,
};
use super::types::{
    BlockLocation, RmId, WAL_PAGE_SIZE, X_LOG_SWITCH, XLR_INFO_MASK, XLogPage, XLogPageHeader,
    XLogRecord,
};

#[derive(Debug, Error)]
pub enum ParsePageError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("cannot save partial parser: no record beginning present")]
    CantSavePartialParser,
}

#[derive(Debug, Error)]
pub enum ReadLocationsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Parse(#[from] ParseError),
}

#[derive(Debug, Default)]
pub struct WalParser {
    current_record_data: Vec<u8>,
    has_current_record_beginning: bool,
    /// `XLogPageHeader.magic` observed on most recent valid page. Drives
    /// FPI bit-layout selection (PG 15 reshuffled bimg_info). `None`
    /// before any page is parsed; defaults to PG-14 layout when missing
    page_magic: Option<u16>,
}

impl WalParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn invalidate(&mut self) {
        self.set_current_record_data(Vec::new());
    }

    /// Seed a parser with the leftover head of a record that continues past
    /// where parsing stopped (wal-g `LoadWalParserFromCurrentRecordHead`).
    /// Used to resume cross-segment stitching from a delta sidecar's stored
    /// head. `page_magic` repopulates from the first page parsed afterward
    pub fn from_current_record_head(head: Vec<u8>) -> Self {
        let mut p = Self::new();
        p.set_current_record_data(head);
        p
    }

    fn set_current_record_data(&mut self, data: Vec<u8>) {
        self.has_current_record_beginning = !data.is_empty();
        self.current_record_data = data;
    }

    pub fn current_record_data(&self) -> &[u8] {
        &self.current_record_data
    }

    pub fn has_current_record_beginning(&self) -> bool {
        self.has_current_record_beginning
    }

    /// Most recent page magic observed. Defaults to PG-14 layout if no
    /// page header has been seen yet
    pub fn page_magic(&self) -> u16 {
        self.page_magic.unwrap_or(super::types::XLP_PAGE_MAGIC_PG14)
    }

    /// Parse all records starting on `page_data` (exactly one 8 KiB page).
    /// Returns `(prev_record_tail, records)`:
    ///   - `prev_record_tail`: a stitched-together record body whose
    ///     *beginning* the parser missed (e.g. first page of an
    ///     ExtractLocations call). Caller chooses to discard or use
    ///   - `records`: complete records that ended on or before this page
    ///
    /// `Ok((empty, empty))` on partial / zero pages so callers can keep
    /// walking; `Err` only on truly malformed data
    pub fn parse_records_from_page(
        &mut self,
        page_data: &[u8],
    ) -> Result<(Vec<u8>, Vec<XLogRecord<'static>>), ParsePageError> {
        let page = match self.parse_page(page_data) {
            Ok(p) => p,
            Err(ParsePageError::Parse(ParseError::PartialPage))
            | Err(ParsePageError::Parse(ParseError::ZeroPage)) => {
                return Ok((Vec::new(), Vec::new()));
            }
            Err(e) => return Err(e),
        };

        // Cross-page record's continuation didn't fully land yet: buffer
        if (page.prev_record_trailing_data.len() as u32) < page.header.remaining_data_len {
            self.current_record_data
                .extend_from_slice(&page.prev_record_trailing_data);
            return Ok((Vec::new(), Vec::new()));
        }

        let mut current_record_data = std::mem::take(&mut self.current_record_data);
        current_record_data.extend_from_slice(&page.prev_record_trailing_data);

        if !self.has_current_record_beginning {
            // Tail without beginning: emit for caller to discard
            self.set_current_record_data(page.next_record_heading_data);
            return Ok((current_record_data, page.records));
        }

        let header = read_xlog_record_header(&mut current_record_data.as_slice())?;
        if header.total_record_length as usize != current_record_data.len() {
            return Err(ParseError::ContinuationNotFound.into());
        }
        // state.rs returns owned records — the input slices it parses
        // from are scratch buffers that don't outlive this function,
        // so we materialise.
        let current_record =
            parse_record_from_bytes(&current_record_data, self.page_magic())?.into_owned();

        let mut records = Vec::with_capacity(page.records.len() + 1);
        records.push(current_record);
        records.extend(page.records);
        self.set_current_record_data(page.next_record_heading_data);
        Ok((Vec::new(), records))
    }

    fn parse_page(&mut self, page_data: &[u8]) -> Result<XLogPage<'static>, ParsePageError> {
        if page_data.len() < WAL_PAGE_SIZE as usize / 2 {
            return Err(ParseError::PartialPage.into());
        }

        // Pass 1: page header (consumes 20 or 36 bytes off the page front)
        let mut cursor: &[u8] = page_data;
        let header = match read_xlog_page_header(&mut cursor) {
            Ok(h) => h,
            Err(ParseError::ZeroPageHeader) => {
                if all_zero(page_data) {
                    return Err(ParseError::ZeroPage.into());
                }
                return Err(ParseError::ZeroPageHeader.into());
            }
            Err(e) => return Err(e.into()),
        };
        self.page_magic = Some(header.magic);
        let consumed = page_data.len() - cursor.len();
        let mut ar = AlignedReader {
            buf: cursor,
            consumed,
        };
        ar.read_to_alignment()?;

        // Bytes carrying over a previous record's tail
        let want = std::cmp::min(header.remaining_data_len as usize, WAL_PAGE_SIZE as usize);
        let take_n = want.min(ar.buf.len());
        let remaining_data = ar.take(take_n, "pageRemainingData")?.to_vec();

        // Short tail: page ended before we got the full RemainingDataLen
        if remaining_data.len() != header.remaining_data_len as usize {
            return Ok(XLogPage {
                header,
                prev_record_trailing_data: remaining_data,
                records: Vec::new(),
                next_record_heading_data: Vec::new(),
            });
        }

        // If we had a buffered start AND the tail bytes form a WAL-switch
        // record together with it, the rest of the page is padding
        if self.has_current_record_beginning {
            let mut joined = self.current_record_data.clone();
            joined.extend_from_slice(&remaining_data);
            if let Ok(record) = parse_record_from_bytes(&joined, self.page_magic())
                && record.is_wal_switch()
            {
                return Ok(XLogPage {
                    header,
                    prev_record_trailing_data: remaining_data,
                    records: Vec::new(),
                    next_record_heading_data: Vec::new(),
                });
            }
        }

        read_xlog_page_inner(&mut ar, header, remaining_data)
    }

    pub fn save<W: Write>(&self, mut w: W) -> Result<(), ParsePageError> {
        if !self.current_record_data.is_empty() && !self.has_current_record_beginning {
            return Err(ParsePageError::CantSavePartialParser);
        }
        let len = self.current_record_data.len() as u32;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&self.current_record_data)?;
        Ok(())
    }

    pub fn load<R: Read>(mut r: R) -> Result<Self, ParsePageError> {
        let mut len_bytes = [0u8; 4];
        r.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes);
        let mut data = vec![0u8; len as usize];
        r.read_exact(&mut data)?;
        let has = !data.is_empty();
        Ok(Self {
            current_record_data: data,
            has_current_record_beginning: has,
            // save/load format is unchanged; page_magic is repopulated
            // from the first page header observed after load
            page_magic: None,
        })
    }
}

fn read_xlog_page_inner(
    ar: &mut AlignedReader<'_>,
    header: XLogPageHeader,
    remaining_data: Vec<u8>,
) -> Result<XLogPage<'static>, ParsePageError> {
    let page_magic = header.magic;
    let mut records = Vec::new();
    loop {
        let res = try_read_xlog_record_data(ar);
        match res {
            Ok((data, whole)) => {
                if data.is_empty() && !whole {
                    return Ok(XLogPage {
                        header,
                        prev_record_trailing_data: remaining_data,
                        records,
                        next_record_heading_data: Vec::new(),
                    });
                }
                if whole {
                    let record = parse_record_from_bytes(&data, page_magic)?.into_owned();
                    let is_switch = record.is_wal_switch();
                    records.push(record);
                    if is_switch {
                        return Ok(XLogPage {
                            header,
                            prev_record_trailing_data: remaining_data,
                            records,
                            next_record_heading_data: Vec::new(),
                        });
                    }
                    continue;
                }
                return Ok(XLogPage {
                    header,
                    prev_record_trailing_data: remaining_data,
                    records,
                    next_record_heading_data: data,
                });
            }
            Err(ParseError::ZeroRecordHeader) => {
                if all_zero(ar.buf) {
                    return Ok(XLogPage {
                        header,
                        prev_record_trailing_data: remaining_data,
                        records,
                        next_record_heading_data: Vec::new(),
                    });
                }
                return Err(ParseError::ZeroRecordHeader.into());
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Walk a WAL segment file (or any `Read` that yields raw page bytes),
/// extracting every `BlockLocation` referenced. Tolerates Partial / Zero
/// page errors as end-of-valid-data signals
pub fn extract_locations_from_wal_file<R: Read>(
    parser: &mut WalParser,
    mut r: R,
) -> Result<Vec<BlockLocation>, ExtractError> {
    let mut out = Vec::new();
    let mut page_buf = vec![0u8; WAL_PAGE_SIZE as usize];
    loop {
        match read_exact_or_eof(&mut r, &mut page_buf)? {
            ReadStatus::Eof => return Ok(out),
            ReadStatus::Short(n) => {
                process_locations_from_page(parser, &page_buf[..n], |loc| out.push(loc))
                    .map_err(parse_to_extract)?;
                return Ok(out);
            }
            ReadStatus::Full => {
                process_locations_from_page(parser, &page_buf, |loc| out.push(loc))
                    .map_err(parse_to_extract)?;
            }
        }
    }
}

/// Locations-only sibling of [`WalParser::parse_records_from_page`].
/// Walks the same page-/record-stitching state machine but emits
/// `BlockLocation`s through `f` instead of materialising every record's
/// block image / data / main_data into owned `Vec`s. Reduces the
/// allocation cost of `extract_locations_from_wal_file` from
/// O(record bodies) to O(#records) header-walks + the existing partial
/// record stitching buffer
pub fn process_locations_from_page<F: FnMut(BlockLocation)>(
    parser: &mut WalParser,
    page_data: &[u8],
    mut f: F,
) -> Result<(), ParsePageError> {
    if page_data.len() < WAL_PAGE_SIZE as usize / 2 {
        return Ok(());
    }
    let mut cursor: &[u8] = page_data;
    let header = match read_xlog_page_header(&mut cursor) {
        Ok(h) => h,
        Err(ParseError::ZeroPageHeader) => {
            if all_zero(page_data) {
                return Ok(());
            }
            return Err(ParseError::ZeroPageHeader.into());
        }
        Err(e) => return Err(e.into()),
    };
    parser.page_magic = Some(header.magic);
    let consumed = page_data.len() - cursor.len();
    let mut ar = AlignedReader {
        buf: cursor,
        consumed,
    };
    ar.read_to_alignment()?;

    let want = std::cmp::min(header.remaining_data_len as usize, WAL_PAGE_SIZE as usize);
    let take_n = want.min(ar.buf.len());
    let remaining_data = ar.take(take_n, "pageRemainingData")?;
    let page_magic = header.magic;

    // Short tail: prev record continues but we ran out of bytes — buffer
    // & wait for the next page (matches parse_records_from_page)
    if remaining_data.len() != header.remaining_data_len as usize {
        parser.current_record_data.extend_from_slice(remaining_data);
        return Ok(());
    }

    // Stitch buffered head (if any) with this page's trailing bytes.
    // Take parser.current_record_data unconditionally so orphan
    // tail-without-head bytes get drained, matching the existing
    // parse_records_from_page semantics
    let had_beginning = parser.has_current_record_beginning;
    let mut stitched = std::mem::take(&mut parser.current_record_data);
    stitched.extend_from_slice(remaining_data);
    parser.has_current_record_beginning = false;

    if had_beginning {
        let rec_header = read_xlog_record_header(&mut stitched.as_slice())?;
        if rec_header.total_record_length as usize != stitched.len() {
            return Err(ParseError::ContinuationNotFound.into());
        }
        for_each_block_location_in_record(&stitched, page_magic, &mut f)?;
        // WAL_SWITCH: rest of page (and segment) is padding
        if rec_header.resource_manager_id == RmId::Xlog as u8
            && (rec_header.info & !XLR_INFO_MASK) == X_LOG_SWITCH
        {
            return Ok(());
        }
    }

    walk_locations_xlog_page_inner(parser, &mut ar, page_magic, f)
}

fn walk_locations_xlog_page_inner<F: FnMut(BlockLocation)>(
    parser: &mut WalParser,
    ar: &mut AlignedReader<'_>,
    page_magic: u16,
    mut f: F,
) -> Result<(), ParsePageError> {
    loop {
        match try_read_xlog_record_data(ar) {
            Ok((data, whole)) => {
                if data.is_empty() && !whole {
                    return Ok(());
                }
                if whole {
                    let rec_header = read_xlog_record_header(&mut data.as_slice())?;
                    for_each_block_location_in_record(&data, page_magic, &mut f)?;
                    if rec_header.resource_manager_id == RmId::Xlog as u8
                        && (rec_header.info & !XLR_INFO_MASK) == X_LOG_SWITCH
                    {
                        return Ok(());
                    }
                    continue;
                }
                // Partial record: buffer head for stitching on the next page
                parser.set_current_record_data(data);
                return Ok(());
            }
            Err(ParseError::ZeroRecordHeader) => {
                if all_zero(ar.buf) {
                    return Ok(());
                }
                return Err(ParseError::ZeroRecordHeader.into());
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn parse_to_extract(e: ParsePageError) -> ExtractError {
    match e {
        ParsePageError::Parse(p) => ExtractError::Parse(p),
        ParsePageError::Io(i) => ExtractError::Io(i),
        ParsePageError::CantSavePartialParser => ExtractError::Parse(ParseError::PartialPage),
    }
}

enum ReadStatus {
    Eof,
    Short(usize),
    Full,
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<ReadStatus> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..])?;
        if n == 0 {
            return Ok(if filled == 0 {
                ReadStatus::Eof
            } else {
                ReadStatus::Short(filled)
            });
        }
        filled += n;
    }
    Ok(ReadStatus::Full)
}

// ─── delta file BlockLocation list I/O (wal-g block_location_writer/reader) ─

/// Write locations as 16-byte LE u32×4 tuples, no terminator. Lets a sidecar
/// be built by appending segment fragments before the terminator is written
pub fn write_location_tuples<W: Write>(mut w: W, locations: &[BlockLocation]) -> io::Result<()> {
    for loc in locations {
        w.write_all(&loc.rel.spc_node.to_le_bytes())?;
        w.write_all(&loc.rel.db_node.to_le_bytes())?;
        w.write_all(&loc.rel.rel_node.to_le_bytes())?;
        w.write_all(&loc.block_no.to_le_bytes())?;
    }
    Ok(())
}

/// Write a list of locations as 16-byte LE u32×4 tuples + an all-zero
/// terminator. Format consumed by wal-g's `ReadLocationsFrom`
pub fn write_locations_to<W: Write>(mut w: W, locations: &[BlockLocation]) -> io::Result<()> {
    write_location_tuples(&mut w, locations)?;
    w.write_all(&[0u8; 16])?;
    Ok(())
}

/// Read locations until the terminal (all-zero) tuple. EOF before terminal
/// is tolerated (returns what we got) — matches wal-g behavior
pub fn read_locations_from<R: Read>(mut r: R) -> Result<Vec<BlockLocation>, ReadLocationsError> {
    let mut out = Vec::new();
    let mut buf = [0u8; 16];
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(out),
            Err(e) => return Err(e.into()),
        }
        let loc = BlockLocation::new(
            u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        );
        if loc.is_terminal() {
            return Ok(out);
        }
        out.push(loc);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locations_round_trip() {
        let locs = vec![
            BlockLocation::new(1663, 16384, 16385, 7),
            BlockLocation::new(1664, 1, 2, 0),
            BlockLocation::new(0, 0, 0, 1),
        ];
        let mut buf = Vec::new();
        write_locations_to(&mut buf, &locs).unwrap();
        assert_eq!(buf.len(), (locs.len() + 1) * 16);
        let parsed = read_locations_from(buf.as_slice()).unwrap();
        assert_eq!(parsed, locs);
    }

    #[test]
    fn parser_save_load_empty() {
        let p = WalParser::new();
        let mut buf = Vec::new();
        p.save(&mut buf).unwrap();
        let p2 = WalParser::load(buf.as_slice()).unwrap();
        assert!(p2.current_record_data().is_empty());
        assert!(!p2.has_current_record_beginning());
    }

    #[test]
    fn parser_save_with_data_round_trips() {
        let mut p = WalParser::new();
        p.set_current_record_data(vec![1, 2, 3, 4, 5]);
        let mut buf = Vec::new();
        p.save(&mut buf).unwrap();
        let p2 = WalParser::load(buf.as_slice()).unwrap();
        assert_eq!(p2.current_record_data(), &[1u8, 2, 3, 4, 5]);
        assert!(p2.has_current_record_beginning());
    }

    #[test]
    fn cannot_save_tail_without_beginning() {
        let mut p = WalParser::new();
        p.current_record_data = vec![1, 2, 3];
        p.has_current_record_beginning = false;
        let err = p.save(&mut Vec::new()).unwrap_err();
        assert!(matches!(err, ParsePageError::CantSavePartialParser));
    }

    #[test]
    fn terminator_at_eof_treated_as_terminator() {
        let buf = [0u8; 16];
        let parsed = read_locations_from(buf.as_slice()).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn empty_wal_file_yields_no_locations() {
        let mut p = WalParser::new();
        let v = extract_locations_from_wal_file(&mut p, std::io::Cursor::new(Vec::new())).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn all_zero_wal_file_yields_no_locations() {
        // Common end-of-segment shape for .partial files
        let buf = vec![0u8; (WAL_PAGE_SIZE as usize) * 4];
        let mut p = WalParser::new();
        let v = extract_locations_from_wal_file(&mut p, std::io::Cursor::new(buf)).unwrap();
        assert!(v.is_empty());
    }

    /// Builds a synthetic 8 KiB WAL page containing one record that references
    /// two blocks: the first w/ explicit RelFileNode, the second w/ SameRel
    /// flag. Validates the parser walks the record headers, applies SameRel
    /// reuse, and surfaces both block locations via extract_block_locations
    #[test]
    fn synthetic_page_with_two_blocks_and_same_rel() {
        use crate::pg::walparser::parse::extract_block_locations;
        use crate::pg::walparser::types::{
            BKP_BLOCK_SAME_REL, RmId, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER,
        };

        // Record body: two block-0 / block-1 headers (no data, no image) and
        // a 0-byte short main-data marker. Block 0 uses explicit
        // RelFileNode, block 1 uses SameRel and just emits the block number.
        let mut body = Vec::new();
        // block 0 header: id=0, fork=0 (no data, no image), data_length=0,
        // rel(12), block_no(4) = 4-byte loc
        body.push(0u8);
        body.push(0u8); // fork_flags
        body.extend_from_slice(&0u16.to_le_bytes()); // data_length
        body.extend_from_slice(&100u32.to_le_bytes()); // spc
        body.extend_from_slice(&200u32.to_le_bytes()); // db
        body.extend_from_slice(&300u32.to_le_bytes()); // rel
        body.extend_from_slice(&7u32.to_le_bytes()); // block_no
        // block 1 header: id=1, fork=SameRel, data_length=0, just block_no
        body.push(1u8);
        body.push(BKP_BLOCK_SAME_REL);
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&42u32.to_le_bytes()); // block_no
        // No main-data marker — main_data_len stays 0 since record header
        // says total_record_length = 24 + body.len()

        let total = X_LOG_RECORD_HEADER_SIZE + body.len();

        // record header bytes
        let mut record = Vec::new();
        record.extend_from_slice(&(total as u32).to_le_bytes());
        record.extend_from_slice(&0u32.to_le_bytes()); // xact
        record.extend_from_slice(&0u64.to_le_bytes()); // prev
        record.push(0u8); // info
        record.push(RmId::Heap as u8); // rmid
        record.push(0u8); // pad
        record.push(0u8); // pad
        record.extend_from_slice(&0u32.to_le_bytes()); // crc
        record.extend_from_slice(&body);

        // Build the page: long page header (36 B) + 4 B align padding +
        // record + zero pad to 8192
        let mut page = Vec::with_capacity(WAL_PAGE_SIZE as usize);
        // page header
        page.extend_from_slice(&0xD117u16.to_le_bytes()); // magic
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes()); // info
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
        // long header trailer
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
        // 4 bytes pad to 8-byte alignment (36 → 40)
        page.extend_from_slice(&[0u8; 4]);
        // record
        page.extend_from_slice(&record);
        // pad up to one full page
        page.resize(WAL_PAGE_SIZE as usize, 0);

        let mut parser = WalParser::new();
        let (tail, records) = parser.parse_records_from_page(&page).unwrap();
        assert!(tail.is_empty(), "no carryover tail expected");
        assert_eq!(records.len(), 1);
        let locs = extract_block_locations(&records);
        assert_eq!(locs.len(), 2);
        assert_eq!(locs[0], BlockLocation::new(100, 200, 300, 7));
        assert_eq!(locs[1], BlockLocation::new(100, 200, 300, 42));

        // process_locations_from_page must emit the same locations
        let mut parser2 = WalParser::new();
        let mut locs2 = Vec::new();
        process_locations_from_page(&mut parser2, &page, |loc| locs2.push(loc)).unwrap();
        assert_eq!(locs2, locs);
    }

    use super::super::types::{
        XLP_FIRST_IS_CONT_RECORD, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG14, XLR_BLOCK_ID_DATA_LONG,
    };

    const PAGE: usize = WAL_PAGE_SIZE as usize;

    /// 36-byte long page header + 16-byte trailer; `remaining` carries a
    /// prev-record tail (0 = none)
    fn long_page_header(remaining: u32) -> Vec<u8> {
        let mut h = Vec::new();
        h.extend_from_slice(&XLP_PAGE_MAGIC_PG14.to_le_bytes());
        h.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        h.extend_from_slice(&1u32.to_le_bytes()); // timeline
        h.extend_from_slice(&0u64.to_le_bytes()); // page_address
        h.extend_from_slice(&remaining.to_le_bytes());
        h.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        h.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes()); // seg size
        h.extend_from_slice(&8192u32.to_le_bytes()); // xlog block size
        h
    }

    /// 20-byte short page header (mid-segment pages)
    fn short_page_header(info: u16, remaining: u32) -> Vec<u8> {
        let mut h = Vec::new();
        h.extend_from_slice(&XLP_PAGE_MAGIC_PG14.to_le_bytes());
        h.extend_from_slice(&info.to_le_bytes());
        h.extend_from_slice(&1u32.to_le_bytes()); // timeline
        h.extend_from_slice(&(PAGE as u64).to_le_bytes()); // page_address
        h.extend_from_slice(&remaining.to_le_bytes());
        h
    }

    /// Minimal complete record: 24-byte header, no body (rmid Heap)
    fn minimal_record() -> Vec<u8> {
        let mut r = Vec::new();
        r.extend_from_slice(&(X_LOG_RECORD_HEADER_SIZE as u32).to_le_bytes());
        r.extend_from_slice(&0u32.to_le_bytes()); // xact
        r.extend_from_slice(&0u64.to_le_bytes()); // prev
        r.push(0u8); // info
        r.push(RmId::Heap as u8);
        r.push(0);
        r.push(0);
        r.extend_from_slice(&0u32.to_le_bytes()); // crc
        r
    }
    use super::super::types::X_LOG_RECORD_HEADER_SIZE;

    #[test]
    fn record_continuation_across_page_boundary() {
        // 9029-byte record (24 header + 5-byte LONG-data marker + 9000 main
        // data). Long header(36)+align(4)=40 used on page 1, leaving 8152 for
        // the record; the trailing 877 bytes land on page 2
        let main = vec![0x5Au8; 9000];
        let mut record = Vec::new();
        record.extend_from_slice(&9029u32.to_le_bytes()); // total_record_length
        record.extend_from_slice(&0u32.to_le_bytes()); // xact
        record.extend_from_slice(&0u64.to_le_bytes()); // prev
        record.push(0u8); // info
        record.push(RmId::Heap as u8);
        record.push(0);
        record.push(0);
        record.extend_from_slice(&0u32.to_le_bytes()); // crc
        record.push(XLR_BLOCK_ID_DATA_LONG);
        record.extend_from_slice(&9000u32.to_le_bytes());
        record.extend_from_slice(&main);
        assert_eq!(record.len(), 9029);

        let split = 8152; // record bytes that fit on page 1 after header+align
        let mut page1 = long_page_header(0);
        page1.extend_from_slice(&[0u8; 4]); // 36 -> 40 alignment pad
        page1.extend_from_slice(&record[..split]);
        assert_eq!(page1.len(), PAGE);

        let mut page2 = short_page_header(XLP_FIRST_IS_CONT_RECORD, (9029 - split) as u32);
        page2.extend_from_slice(&[0u8; 4]); // 20 -> 24 alignment pad
        page2.extend_from_slice(&record[split..]);
        page2.resize(PAGE, 0);

        let mut parser = WalParser::new();
        let (tail1, recs1) = parser.parse_records_from_page(&page1).unwrap();
        assert!(tail1.is_empty());
        assert!(recs1.is_empty(), "record incomplete after first page");
        assert!(parser.has_current_record_beginning());
        assert_eq!(parser.page_magic(), XLP_PAGE_MAGIC_PG14);

        let (tail2, recs2) = parser.parse_records_from_page(&page2).unwrap();
        assert!(tail2.is_empty());
        assert_eq!(recs2.len(), 1, "stitched record emitted on second page");
        assert_eq!(recs2[0].main_data_len, 9000);
        assert_eq!(recs2[0].main_data.len(), 9000);
        assert!(!parser.has_current_record_beginning());
    }

    #[test]
    fn multi_record_page_emits_all_records() {
        let rec = minimal_record();
        let mut page = long_page_header(0);
        page.extend_from_slice(&[0u8; 4]);
        page.extend_from_slice(&rec);
        page.extend_from_slice(&rec); // 24-byte records stay 8-aligned
        page.resize(PAGE, 0);

        let mut parser = WalParser::new();
        let (tail, recs) = parser.parse_records_from_page(&page).unwrap();
        assert!(tail.is_empty());
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn zero_page_yields_nothing() {
        let mut parser = WalParser::new();
        let page = vec![0u8; PAGE];
        let (tail, recs) = parser.parse_records_from_page(&page).unwrap();
        assert!(tail.is_empty() && recs.is_empty());
    }

    #[test]
    fn zero_page_header_with_nonzero_body_errors() {
        // header bytes all zero (-> ZeroPageHeader) but page not all-zero, so
        // it is a real corruption rather than end-of-data padding
        let mut page = vec![0u8; PAGE];
        page[2000] = 0xFF;
        let mut parser = WalParser::new();
        let err = parser.parse_records_from_page(&page).unwrap_err();
        assert!(matches!(
            err,
            ParsePageError::Parse(ParseError::ZeroPageHeader)
        ));
    }

    #[test]
    fn invalidate_clears_buffered_head() {
        let mut p = WalParser::from_current_record_head(vec![1, 2, 3]);
        assert!(p.has_current_record_beginning());
        p.invalidate();
        assert!(p.current_record_data().is_empty());
        assert!(!p.has_current_record_beginning());
    }

    #[test]
    fn parse_to_extract_maps_each_variant() {
        assert!(matches!(
            parse_to_extract(ParsePageError::Parse(ParseError::PartialPage)),
            ExtractError::Parse(ParseError::PartialPage)
        ));
        assert!(matches!(
            parse_to_extract(ParsePageError::Io(io::Error::other("x"))),
            ExtractError::Io(_)
        ));
        // CantSavePartialParser folds into a PartialPage parse error
        assert!(matches!(
            parse_to_extract(ParsePageError::CantSavePartialParser),
            ExtractError::Parse(ParseError::PartialPage)
        ));
    }
}
