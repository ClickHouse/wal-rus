//! XLOG segment filename parsing
//!
//! Format: TTTTTTTTLLLLLLLLSSSSSSSS where each is 8 hex chars
//! T=timeline, L=log id (high 32 of LSN/segsize), S=segment number within log
//! Default segment size = 16MB; configurable via initdb --wal-segsize
//!
//! Reference: postgresql src/include/access/xlog_internal.h

use thiserror::Error;

pub const DEFAULT_WAL_SEG_SIZE: u64 = 16 * 1024 * 1024;
pub const SEGMENT_NAME_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentName {
    // Order matters: timeline → log_id → seg_no so a sorted iteration matches
    // PG's natural archive order (timeline boundaries split a chain)
    pub timeline: u32,
    pub log_id: u32,
    pub seg_no: u32,
}

#[derive(Debug, Error)]
pub enum SegmentError {
    #[error("expected 24 hex chars, got {0}")]
    BadLength(usize),
    #[error("non-hex char in segment name: {0}")]
    NonHex(String),
}

impl SegmentName {
    pub fn parse(s: &str) -> Result<Self, SegmentError> {
        if s.len() != SEGMENT_NAME_LEN {
            return Err(SegmentError::BadLength(s.len()));
        }
        let timeline =
            u32::from_str_radix(&s[0..8], 16).map_err(|_| SegmentError::NonHex(s.into()))?;
        let log_id =
            u32::from_str_radix(&s[8..16], 16).map_err(|_| SegmentError::NonHex(s.into()))?;
        let seg_no =
            u32::from_str_radix(&s[16..24], 16).map_err(|_| SegmentError::NonHex(s.into()))?;
        Ok(SegmentName {
            timeline,
            log_id,
            seg_no,
        })
    }

    pub fn format(&self) -> String {
        format!(
            "{:08X}{:08X}{:08X}",
            self.timeline, self.log_id, self.seg_no
        )
    }

    /// Starting LSN of the segment given seg size in bytes
    pub fn start_lsn(&self, seg_size: u64) -> u64 {
        ((self.log_id as u64) << 32) | (self.seg_no as u64).wrapping_mul(seg_size)
    }

    /// Successor segment on the same timeline (rolls log_id when seg_no caps)
    pub fn next(&self, seg_size: u64) -> Self {
        debug_assert!(seg_size > 0 && seg_size.is_power_of_two());
        let xlog_segs_per_xlog_id = (0x1_0000_0000u64 / seg_size) as u32;
        let next_seg = self.seg_no + 1;
        if next_seg >= xlog_segs_per_xlog_id {
            SegmentName {
                timeline: self.timeline,
                log_id: self.log_id + 1,
                seg_no: 0,
            }
        } else {
            SegmentName {
                timeline: self.timeline,
                log_id: self.log_id,
                seg_no: next_seg,
            }
        }
    }
}

/// Recognize backup-related auxiliary files (.partial, .backup, .history)
pub fn is_wal_filename(name: &str) -> bool {
    name.len() == SEGMENT_NAME_LEN && name.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn is_history_filename(name: &str) -> bool {
    name.ends_with(".history")
        && name.len() >= ".history".len() + 8
        && name[..8].chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_name() {
        let s = SegmentName::parse("000000010000000000000001").unwrap();
        assert_eq!(s.timeline, 1);
        assert_eq!(s.log_id, 0);
        assert_eq!(s.seg_no, 1);
        assert_eq!(s.format(), "000000010000000000000001");
    }

    #[test]
    fn parse_lowercase_hex() {
        // postgres always uppercase but tolerate
        assert!(SegmentName::parse("0000000100000000000000ab").is_ok());
    }

    #[test]
    fn rejects_short_name() {
        assert!(matches!(
            SegmentName::parse("00000001").unwrap_err(),
            SegmentError::BadLength(8)
        ));
    }

    #[test]
    fn start_lsn_computation() {
        let s = SegmentName::parse("000000010000000200000003").unwrap();
        // log_id=2, seg_no=3, seg_size=16MB
        // expected = (2 << 32) + 3*16MB
        let expected = (2u64 << 32) + 3 * 16 * 1024 * 1024;
        assert_eq!(s.start_lsn(DEFAULT_WAL_SEG_SIZE), expected);
    }

    #[test]
    fn classifies_filenames() {
        assert!(is_wal_filename("000000010000000000000001"));
        assert!(!is_wal_filename("000000010000000000000001.partial"));
        assert!(is_history_filename("00000002.history"));
        assert!(!is_history_filename("readme.history"));
    }

    #[test]
    fn next_segment_increments_seg_no() {
        let s = SegmentName::parse("000000010000000000000005").unwrap();
        let n = s.next(DEFAULT_WAL_SEG_SIZE);
        assert_eq!(n.format(), "000000010000000000000006");
    }

    #[test]
    fn next_segment_rolls_log_id() {
        // 16 MiB segs: 256 per log_id, max seg_no = 0xFF
        let s = SegmentName::parse("0000000100000007000000FF").unwrap();
        let n = s.next(DEFAULT_WAL_SEG_SIZE);
        assert_eq!(n.format(), "000000010000000800000000");
    }
}
