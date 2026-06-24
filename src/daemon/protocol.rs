//! Wire format for wal-g daemon socket
//!
//! Header: 1 byte type, 2 byte big-endian length (covering header + body, so body=len-3)
//! Body interpretation depends on type and arg count, see daemon/mod.rs

use anyhow::{Result, anyhow, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Check = b'C',
    Ok = b'O',
    Error = b'E',
    ArchiveNonExistence = b'N',
    WalPush = b'F',
    WalFetch = b'f',
}

impl MessageType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'C' => Some(Self::Check),
            b'O' => Some(Self::Ok),
            b'E' => Some(Self::Error),
            b'N' => Some(Self::ArchiveNonExistence),
            b'F' => Some(Self::WalPush),
            b'f' => Some(Self::WalFetch),
            _ => None,
        }
    }
}

pub async fn read_message<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<(MessageType, Vec<u8>)> {
    let mut header = [0u8; 3];
    r.read_exact(&mut header).await?;
    let msg_type = MessageType::from_byte(header[0])
        .ok_or_else(|| anyhow!("unknown message type byte: {}", header[0]))?;
    let total_len = u16::from_be_bytes([header[1], header[2]]) as usize;
    if total_len < 3 {
        bail!("invalid message length {total_len}");
    }
    let body_len = total_len - 3;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        r.read_exact(&mut body).await?;
    }
    Ok((msg_type, body))
}

pub async fn write_message<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    msg_type: MessageType,
    args: &[&str],
) -> Result<()> {
    let body = match args.len() {
        0 => Vec::new(),
        1 => args[0].as_bytes().to_vec(),
        _ => encode_args(args)?,
    };
    let total = 3 + body.len();
    if total > u16::MAX as usize {
        bail!("message too large: {total} bytes");
    }
    let total = total as u16;
    let mut buf = Vec::with_capacity(3 + body.len());
    buf.push(msg_type as u8);
    buf.extend_from_slice(&total.to_be_bytes());
    buf.extend_from_slice(&body);
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

pub fn encode_args(args: &[&str]) -> Result<Vec<u8>> {
    if args.len() > 255 {
        bail!("too many args: {}", args.len());
    }
    let mut out = Vec::with_capacity(1 + args.iter().map(|a| a.len() + 2).sum::<usize>());
    out.push(args.len() as u8);
    for a in args {
        if a.len() > u16::MAX as usize {
            bail!("arg too long: {} bytes", a.len());
        }
        out.extend_from_slice(&(a.len() as u16).to_be_bytes());
        out.extend_from_slice(a.as_bytes());
    }
    Ok(out)
}

pub fn parse_args(body: &[u8]) -> Result<Vec<String>> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let count = body[0] as usize;
    let mut out = Vec::with_capacity(count);
    let mut idx = 1;
    for _ in 0..count {
        if idx + 2 > body.len() {
            bail!("truncated arg length");
        }
        let l = u16::from_be_bytes([body[idx], body[idx + 1]]) as usize;
        idx += 2;
        if idx + l > body.len() {
            bail!("truncated arg body");
        }
        out.push(String::from_utf8(body[idx..idx + l].to_vec())?);
        idx += l;
    }
    if idx != body.len() {
        bail!("trailing bytes after args: {} extra", body.len() - idx);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn roundtrip_no_args() {
        let mut buf: Vec<u8> = Vec::new();
        write_message(&mut buf, MessageType::Ok, &[]).await.unwrap();
        let mut cur = Cursor::new(buf);
        let (t, body) = read_message(&mut cur).await.unwrap();
        assert_eq!(t, MessageType::Ok);
        assert_eq!(body.len(), 0);
    }

    #[tokio::test]
    async fn roundtrip_single_arg() {
        let mut buf: Vec<u8> = Vec::new();
        write_message(&mut buf, MessageType::WalPush, &["/path/to/wal"])
            .await
            .unwrap();
        let mut cur = Cursor::new(buf);
        let (t, body) = read_message(&mut cur).await.unwrap();
        assert_eq!(t, MessageType::WalPush);
        assert_eq!(body, b"/path/to/wal");
    }

    #[tokio::test]
    async fn roundtrip_multi_args() {
        let mut buf: Vec<u8> = Vec::new();
        write_message(
            &mut buf,
            MessageType::WalFetch,
            &["000000010000000000000001", "/dst/path"],
        )
        .await
        .unwrap();
        let mut cur = Cursor::new(buf);
        let (t, body) = read_message(&mut cur).await.unwrap();
        assert_eq!(t, MessageType::WalFetch);
        let args = parse_args(&body).unwrap();
        assert_eq!(args, vec!["000000010000000000000001", "/dst/path"]);
    }

    #[test]
    fn from_byte_maps_all_types_and_rejects_unknown() {
        assert_eq!(MessageType::from_byte(b'C'), Some(MessageType::Check));
        assert_eq!(MessageType::from_byte(b'O'), Some(MessageType::Ok));
        assert_eq!(MessageType::from_byte(b'E'), Some(MessageType::Error));
        assert_eq!(
            MessageType::from_byte(b'N'),
            Some(MessageType::ArchiveNonExistence)
        );
        assert_eq!(MessageType::from_byte(b'F'), Some(MessageType::WalPush));
        assert_eq!(MessageType::from_byte(b'f'), Some(MessageType::WalFetch));
        assert_eq!(MessageType::from_byte(b'?'), None);
    }

    #[tokio::test]
    async fn read_message_rejects_length_below_header() {
        // total_len=2 is < the 3-byte header, so body math would underflow
        let bytes = vec![b'O', 0x00, 0x02];
        let mut cur = Cursor::new(bytes);
        assert!(read_message(&mut cur).await.is_err());
    }

    #[tokio::test]
    async fn read_message_rejects_unknown_type_byte() {
        let bytes = vec![b'?', 0x00, 0x03];
        let mut cur = Cursor::new(bytes);
        assert!(read_message(&mut cur).await.is_err());
    }

    #[tokio::test]
    async fn write_message_rejects_oversized_body() {
        // one arg pushes total past u16::MAX
        let big = "x".repeat(u16::MAX as usize);
        let mut buf: Vec<u8> = Vec::new();
        assert!(
            write_message(&mut buf, MessageType::WalPush, &[&big])
                .await
                .is_err()
        );
    }

    #[test]
    fn encode_args_rejects_too_many_and_too_long() {
        let many: Vec<&str> = vec!["x"; 256];
        assert!(encode_args(&many).is_err());
        let long = "y".repeat(u16::MAX as usize + 1);
        assert!(encode_args(&[&long]).is_err());
    }

    #[test]
    fn parse_args_empty_body_is_no_args() {
        assert!(parse_args(&[]).unwrap().is_empty());
    }

    #[test]
    fn parse_args_rejects_truncation_and_trailing() {
        // count=2 but no length bytes follow
        assert!(parse_args(&[2]).is_err());
        // declares a 5-byte arg with only 1 byte present
        assert!(parse_args(&[1, 0, 5, b'a']).is_err());
        // a valid 1-byte arg followed by a stray trailing byte
        assert!(parse_args(&[1, 0, 1, b'a', b'x']).is_err());
    }
}
