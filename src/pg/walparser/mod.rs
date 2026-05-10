//! XLOG record parser. Port of wal-g internal/walparser
//!
//! Parses pg WAL files to extract (RelFileNode, BlockNumber) of every
//! page-modifying record. Used by delta backups (Phase C) to know which
//! blocks of which relfiles changed between two LSNs
//!
//! On-disk binary format documented in postgres
//! src/include/access/xlogrecord.h & xlog_internal.h. Stable since PG 11;
//! covered up to PG 18
//!
//! Layout
//! - `types` — primitive Oids, RelFileNode, BlockLocation, record / page /
//!   block headers + their flag predicates
//! - `parse` — synchronous binary readers for those headers + records
//! - `state` — `WalParser` which threads continuation records across page
//!   boundaries (a single XLogRecord can span 2+ pages)
//!
//! Page size assumed 8 KiB (`BLOCK_SIZE`). Non-default WAL/block sizes are
//! unsupported & match wal-g's behavior

mod parse;
mod state;
mod types;

pub use parse::{ExtractError, ParseError, extract_block_locations, parse_record_from_bytes};
pub use state::{
    ParsePageError, ReadLocationsError, WalParser, extract_locations_from_wal_file,
    read_locations_from, write_locations_to,
};
pub use types::{
    BLOCK_SIZE, BlockLocation, Oid, RelFileNode, RmId, TimeLineId, WAL_PAGE_SIZE, XLogPage,
    XLogPageHeader, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordBlockImageHeader,
    XLogRecordHeader, XLogRecordPtr,
};
