//! Physical-replication CopyData wire helpers.
//!
//! `START_REPLICATION` flips the connection into CopyBoth mode. Each
//! direction frames messages as `CopyData` payloads whose first byte
//! tags the variant. This module decodes the server-to-client frames
//! (`'w'` WAL data, `'k'` keepalive) and builds the client-to-server
//! standby status update (`'r'`)
//!
//! Wire layout (postgres `src/backend/replication/walsender.c`):
//!
//! ```text
//! 'w' | u64 start_lsn | u64 server_wal_end | i64 send_time | bytes...
//! 'k' | u64 server_wal_end | i64 send_time | u8 reply_requested
//! 'r' | u64 write_lsn | u64 flush_lsn | u64 apply_lsn | i64 client_time | u8 reply_requested
//! ```
//!
//! Timestamps use the postgres microsecond-since-2000 epoch ([`PG_EPOCH_USEC`])

use anyhow::{Result, bail};

/// Microseconds between 1970-01-01 and 2000-01-01 — pg's epoch offset
pub const PG_EPOCH_USEC: i64 = 946_684_800_000_000;

/// `'w'` CopyData frame: server delivering WAL bytes to the client
#[derive(Debug, Clone, Copy)]
pub struct WalDataFrame<'a> {
    pub start_lsn: u64,
    pub server_wal_end: u64,
    pub send_time: i64,
    pub data: &'a [u8],
}

/// `'k'` CopyData frame: server keepalive
#[derive(Debug, Clone, Copy)]
pub struct KeepaliveFrame {
    pub server_wal_end: u64,
    pub send_time: i64,
    pub reply_requested: bool,
}

/// Decoded server-to-client CopyData variant
#[derive(Debug, Clone, Copy)]
pub enum Frame<'a> {
    Wal(WalDataFrame<'a>),
    Keepalive(KeepaliveFrame),
}

/// Decode a server CopyData payload. Errors on unknown tag or short frame
pub fn decode_frame(payload: &[u8]) -> Result<Frame<'_>> {
    if payload.is_empty() {
        bail!("empty CopyData payload");
    }
    match payload[0] {
        b'w' => {
            if payload.len() < 1 + 24 {
                bail!("WAL data frame too short: {} bytes", payload.len());
            }
            let p = &payload[1..];
            let start_lsn = u64::from_be_bytes(p[0..8].try_into().unwrap());
            let server_wal_end = u64::from_be_bytes(p[8..16].try_into().unwrap());
            let send_time = i64::from_be_bytes(p[16..24].try_into().unwrap());
            Ok(Frame::Wal(WalDataFrame {
                start_lsn,
                server_wal_end,
                send_time,
                data: &p[24..],
            }))
        }
        b'k' => {
            if payload.len() < 1 + 17 {
                bail!("keepalive frame too short: {} bytes", payload.len());
            }
            let p = &payload[1..];
            let server_wal_end = u64::from_be_bytes(p[0..8].try_into().unwrap());
            let send_time = i64::from_be_bytes(p[8..16].try_into().unwrap());
            let reply_requested = p[16] != 0;
            Ok(Frame::Keepalive(KeepaliveFrame {
                server_wal_end,
                send_time,
                reply_requested,
            }))
        }
        tag => bail!("unknown CopyData tag: {:?}", tag as char),
    }
}

/// Build a server-direction `'w'` XLogData payload. walsender
/// server frames each per-record byte slice into this envelope;
/// the walreceiver on the other side decodes it via [`decode_frame`].
/// Mirrors postgres's `XLogSendPhysical` wire layout (1 tag byte +
/// u64 start_lsn + u64 server_wal_end + i64 send_time + payload).
/// `send_time` is set from [`now_pg_microseconds`].
pub fn encode_wal_data_frame(start_lsn: u64, server_wal_end: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 24 + data.len());
    out.push(b'w');
    out.extend_from_slice(&start_lsn.to_be_bytes());
    out.extend_from_slice(&server_wal_end.to_be_bytes());
    out.extend_from_slice(&now_pg_microseconds().to_be_bytes());
    out.extend_from_slice(data);
    out
}

/// Build a server-direction `'k'` keepalive payload carrying the
/// current `server_wal_end` high-water mark. `reply_requested` set
/// only when the listener task wants to demand a `'r'` status from
/// a slow client.
pub fn encode_keepalive_frame(server_wal_end: u64, reply_requested: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 17);
    out.push(b'k');
    out.extend_from_slice(&server_wal_end.to_be_bytes());
    out.extend_from_slice(&now_pg_microseconds().to_be_bytes());
    out.push(if reply_requested { 1 } else { 0 });
    out
}

/// Build a `'r'` standby status update payload. `reply_requested = 0`
/// since clients never demand a server response to a status update
pub fn build_status_update(write_lsn: u64, flush_lsn: u64, apply_lsn: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(34);
    out.push(b'r');
    out.extend_from_slice(&write_lsn.to_be_bytes());
    out.extend_from_slice(&flush_lsn.to_be_bytes());
    out.extend_from_slice(&apply_lsn.to_be_bytes());
    out.extend_from_slice(&now_pg_microseconds().to_be_bytes());
    out.push(0);
    out
}

/// Current wall-clock in pg's microsecond-since-2000 epoch
pub fn now_pg_microseconds() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    now - PG_EPOCH_USEC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_wal_frame() {
        let mut p = Vec::new();
        p.push(b'w');
        p.extend_from_slice(&0x100u64.to_be_bytes());
        p.extend_from_slice(&0x200u64.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        p.extend_from_slice(b"hello");
        match decode_frame(&p).unwrap() {
            Frame::Wal(w) => {
                assert_eq!(w.start_lsn, 0x100);
                assert_eq!(w.server_wal_end, 0x200);
                assert_eq!(w.data, b"hello");
            }
            _ => panic!("expected WAL frame"),
        }
    }

    #[test]
    fn decode_keepalive_frame() {
        let mut p = Vec::new();
        p.push(b'k');
        p.extend_from_slice(&0x300u64.to_be_bytes());
        p.extend_from_slice(&12345i64.to_be_bytes());
        p.push(1);
        match decode_frame(&p).unwrap() {
            Frame::Keepalive(k) => {
                assert!(k.reply_requested);
                assert_eq!(k.server_wal_end, 0x300);
            }
            _ => panic!("expected keepalive"),
        }
    }

    #[test]
    fn rejects_short_frames() {
        assert!(decode_frame(b"w").is_err());
        assert!(decode_frame(b"k\x00").is_err());
        assert!(decode_frame(b"").is_err());
        assert!(decode_frame(b"x\x00\x00").is_err());
    }

    #[test]
    fn status_update_encoding() {
        let bytes = build_status_update(0x1, 0x2, 0x3);
        assert_eq!(bytes[0], b'r');
        assert_eq!(bytes.len(), 1 + 8 * 4 + 1);
        let write = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        let flush = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let apply = u64::from_be_bytes(bytes[17..25].try_into().unwrap());
        assert_eq!((write, flush, apply), (0x1, 0x2, 0x3));
        assert_eq!(bytes[33], 0);
    }
}
