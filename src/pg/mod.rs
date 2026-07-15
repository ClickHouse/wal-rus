pub mod backup;
pub mod replication;
pub mod wal;
pub mod wal_summaries;
pub mod walparser;

pub const WAL_FOLDER: &str = "wal_005";
pub const BASEBACKUP_FOLDER: &str = "basebackups_005";

/// Fold 1..=max_digits ascii hex bytes into u64; rejects sign, prefix,
/// whitespace. Callers slice with `.get(..n)` to stay panic-free on
/// arbitrary byte-length input
pub(crate) fn parse_hex(b: &[u8], max_digits: usize) -> Option<u64> {
    debug_assert!(max_digits <= 16);
    if b.is_empty() || b.len() > max_digits {
        return None;
    }
    b.iter().try_fold(0u64, |acc, &c| {
        Some((acc << 4) | (c as char).to_digit(16)? as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_strict() {
        assert_eq!(parse_hex(b"0", 8), Some(0));
        assert_eq!(parse_hex(b"DeadBeef", 8), Some(0xDEAD_BEEF));
        assert_eq!(parse_hex(b"FFFFFFFFFFFFFFFF", 16), Some(u64::MAX));
        assert_eq!(parse_hex(b"", 8), None);
        assert_eq!(parse_hex(b"123456789", 8), None);
        assert_eq!(parse_hex(b"+1", 8), None);
        assert_eq!(parse_hex(b"-1", 8), None);
        assert_eq!(parse_hex(b" 1", 8), None);
        assert_eq!(parse_hex(b"0x1", 8), None);
        assert_eq!(parse_hex("é1".as_bytes(), 8), None);
    }
}
