//! Walsender server side of the physical replication protocol.
//!
//! Pairs with [`super::conn`] (client side) so wal-rs can play either
//! role.
//!
//! | inbound query | reply |
//! |---|---|
//! | `StartupMessage` with `replication=true` | `AuthenticationOk` + ParameterStatus + `BackendKeyData` + `ReadyForQuery` |
//! | `IDENTIFY_SYSTEM` | `(systemid, timeline, xlogpos, dbname)` row |
//! | `TIMELINE_HISTORY <tli>` | empty history (single-timeline source) |
//! | `START_REPLICATION [SLOT _] PHYSICAL <lsn> [TIMELINE <n>]` | `CopyBothResponse` then `'w'` frames |
//! | other simple queries | `CommandComplete` + `ReadyForQuery` |
//!
//! Auth: trust only, runs over a shared unix socket against PG.
//! The `Authentication*` messages a real PG walreceiver
//! understands are coded inline rather than via postgres-protocol's
//! `frontend` module since the latter is client-side.
//!
//! Frame encoding for the CopyBoth body (`'w'` XLogData, `'k'`
//! keepalive) lives in [`super::stream`]; this module orchestrates the
//! startup-to-CopyBoth transition.

use std::collections::HashMap;

use bytes::{Buf, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("unsupported query: {0}")]
    Unsupported(String),
}

impl From<anyhow::Error> for ServerError {
    fn from(e: anyhow::Error) -> Self {
        ServerError::Protocol(format!("{e:#}"))
    }
}

/// `IDENTIFY_SYSTEM` reply payload + `xlogpos`. Cached at startup
/// from source's reply, refreshed on timeline switch
#[derive(Debug, Clone)]
pub struct Identity {
    pub system_id: String,
    pub timeline: u32,
    pub xlogpos: u64,
    pub dbname: Option<String>,
}

/// Output of the handshake: which LSN the walreceiver wants to begin
/// at, and on which timeline.
#[derive(Debug, Clone)]
pub struct StartReplication {
    pub start_lsn: u64,
    pub timeline: u32,
    pub slot: Option<String>,
}

/// Drive the startup conversation up to and including
/// `START_REPLICATION`. Returns the receiver's chosen start LSN +
/// timeline; the caller then transitions to CopyBoth streaming.
pub async fn handshake_and_await_start<S>(
    sock: &mut S,
    identity: &Identity,
) -> Result<StartReplication, ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _params = read_startup(sock).await?;
    write_auth_ok(sock).await?;
    write_parameter_status(sock, "server_version", "16.3").await?;
    write_parameter_status(sock, "server_encoding", "UTF8").await?;
    write_parameter_status(sock, "client_encoding", "UTF8").await?;
    write_parameter_status(sock, "DateStyle", "ISO, MDY").await?;
    write_parameter_status(sock, "integer_datetimes", "on").await?;
    write_parameter_status(sock, "TimeZone", "UTC").await?;
    write_parameter_status(sock, "standard_conforming_strings", "on").await?;
    write_parameter_status(sock, "in_hot_standby", "off").await?;
    write_backend_key_data(sock, 1, 1).await?;
    write_ready_for_query(sock, b'I').await?;

    let mut rx = BytesMut::with_capacity(8192);
    loop {
        let msg = read_typed_message(sock, &mut rx).await?;
        match msg.kind {
            b'Q' => {
                let query = parse_simple_query(&msg.body)?;
                match dispatch_query(sock, &query, identity).await? {
                    Some(start) => return Ok(start),
                    None => {}
                }
            }
            b'X' => return Err(ServerError::Protocol("client closed during startup".into())),
            other => {
                return Err(ServerError::Protocol(format!(
                    "unexpected startup message tag {:?}",
                    other as char
                )));
            }
        }
    }
}

/// One framed message read from the wire (tag + body).
#[derive(Debug)]
struct TypedMessage {
    kind: u8,
    body: Vec<u8>,
}

async fn read_typed_message<S>(sock: &mut S, rx: &mut BytesMut) -> Result<TypedMessage, ServerError>
where
    S: AsyncRead + Unpin,
{
    while rx.len() < 5 {
        let n = sock.read_buf(rx).await?;
        if n == 0 {
            return Err(ServerError::Protocol("eof reading message header".into()));
        }
    }
    let kind = rx[0];
    let len = u32::from_be_bytes(rx[1..5].try_into().unwrap()) as usize;
    if len < 4 {
        return Err(ServerError::Protocol(format!("message length {len} < 4")));
    }
    let total = 1 + len;
    while rx.len() < total {
        let n = sock.read_buf(rx).await?;
        if n == 0 {
            return Err(ServerError::Protocol("eof inside message body".into()));
        }
    }
    let mut frame = rx.split_to(total);
    frame.advance(5); // tag + length consumed
    Ok(TypedMessage {
        kind,
        body: frame.to_vec(),
    })
}

/// Read the initial `StartupMessage` (untyped — length + protocol
/// version + null-terminated key/value pairs).
async fn read_startup<S>(sock: &mut S) -> Result<HashMap<String, String>, ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut header = [0u8; 8];
    sock.read_exact(&mut header).await?;
    let len = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
    let protocol = u32::from_be_bytes(header[4..8].try_into().unwrap());
    if len < 8 {
        return Err(ServerError::Protocol(format!(
            "startup length {len} too short"
        )));
    }
    // Negotiate SSL: client sends 0x04D2_16 2F. Reply with 'N' (no SSL)
    // and re-read the actual StartupMessage.
    const SSL_REQUEST_CODE: u32 = 80877103;
    const GSSENC_REQUEST_CODE: u32 = 80877104;
    if protocol == SSL_REQUEST_CODE || protocol == GSSENC_REQUEST_CODE {
        sock.write_all(&[b'N']).await?;
        sock.flush().await?;
        return Box::pin(read_startup(sock)).await;
    }
    // Walreceiver speaks protocol 3.0 (= 196608). PG18 uses 0x00030000.
    if protocol >> 16 != 3 {
        return Err(ServerError::Protocol(format!(
            "unsupported protocol version {:#X}",
            protocol
        )));
    }
    let body_len = len - 8;
    let mut body = vec![0u8; body_len];
    sock.read_exact(&mut body).await?;
    let mut params = HashMap::new();
    let mut i = 0;
    while i < body.len() {
        let key_end = body[i..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| ServerError::Protocol("startup key not null-terminated".into()))?
            + i;
        if key_end == i {
            break;
        }
        let key = String::from_utf8(body[i..key_end].to_vec())
            .map_err(|_| ServerError::Protocol("startup key not utf8".into()))?;
        i = key_end + 1;
        let val_end = body[i..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| ServerError::Protocol("startup value not null-terminated".into()))?
            + i;
        let val = String::from_utf8(body[i..val_end].to_vec())
            .map_err(|_| ServerError::Protocol("startup value not utf8".into()))?;
        i = val_end + 1;
        params.insert(key, val);
    }
    Ok(params)
}

fn parse_simple_query(body: &[u8]) -> Result<String, ServerError> {
    if body.last() != Some(&0) {
        return Err(ServerError::Protocol(
            "simple query not null-terminated".into(),
        ));
    }
    let bytes = &body[..body.len() - 1];
    String::from_utf8(bytes.to_vec())
        .map_err(|_| ServerError::Protocol("simple query not utf8".into()))
}

/// Handle a single simple-query message. Returns `Some(start)` if the
/// query was `START_REPLICATION` (the handshake completes); `None`
/// for `IDENTIFY_SYSTEM`, `TIMELINE_HISTORY`, and any inert query
/// (the caller loops for the next query).
async fn dispatch_query<S>(
    sock: &mut S,
    query: &str,
    identity: &Identity,
) -> Result<Option<StartReplication>, ServerError>
where
    S: AsyncWrite + Unpin,
{
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("IDENTIFY_SYSTEM") {
        write_identify_system(sock, identity).await?;
        write_ready_for_query(sock, b'I').await?;
        Ok(None)
    } else if upper.starts_with("TIMELINE_HISTORY") {
        write_timeline_history(sock, identity).await?;
        write_ready_for_query(sock, b'I').await?;
        Ok(None)
    } else if upper.starts_with("START_REPLICATION") {
        let start = parse_start_replication(trimmed)?;
        // Switch to CopyBoth.
        write_copy_both_response(sock).await?;
        Ok(Some(start))
    } else if upper.starts_with("SHOW ") || upper.starts_with("BEGIN") || upper.starts_with("END") {
        // PG walreceiver issues SHOW data_directory_mode (or similar)
        // probes on startup with newer versions; ack with empty result.
        write_empty_query(sock).await?;
        write_ready_for_query(sock, b'I').await?;
        Ok(None)
    } else {
        write_error_response(sock, "0A000", &format!("unsupported query: {trimmed}")).await?;
        write_ready_for_query(sock, b'I').await?;
        Err(ServerError::Unsupported(trimmed.to_string()))
    }
}

fn parse_start_replication(query: &str) -> Result<StartReplication, ServerError> {
    // Forms:
    //   START_REPLICATION [SLOT slotname] [PHYSICAL] lsn [TIMELINE tli]
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(|s| s.trim_end_matches(';').to_string())
        .collect();
    let mut i = 1; // skip START_REPLICATION
    let mut slot: Option<String> = None;
    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("SLOT") {
        if i + 1 >= tokens.len() {
            return Err(ServerError::Protocol("SLOT requires a name".into()));
        }
        slot = Some(tokens[i + 1].trim_matches('"').to_string());
        i += 2;
    }
    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("PHYSICAL") {
        i += 1;
    } else if i < tokens.len() && tokens[i].eq_ignore_ascii_case("LOGICAL") {
        return Err(ServerError::Unsupported("LOGICAL".into()));
    }
    if i >= tokens.len() {
        return Err(ServerError::Protocol(
            "START_REPLICATION missing LSN".into(),
        ));
    }
    let start_lsn = crate::pg::backup::parse_pg_lsn(&tokens[i])
        .map_err(|e| ServerError::Protocol(format!("parse LSN {:?}: {e:#}", tokens[i])))?;
    i += 1;
    let mut timeline: u32 = 1;
    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("TIMELINE") {
        if i + 1 >= tokens.len() {
            return Err(ServerError::Protocol("TIMELINE requires a value".into()));
        }
        timeline = tokens[i + 1]
            .parse()
            .map_err(|e| ServerError::Protocol(format!("parse timeline: {e}")))?;
    }
    Ok(StartReplication {
        start_lsn,
        timeline,
        slot,
    })
}

// --- wire-encoder helpers ---------------------------------------------------

async fn write_auth_ok<S: AsyncWrite + Unpin>(sock: &mut S) -> Result<(), ServerError> {
    let mut buf = Vec::with_capacity(9);
    buf.push(b'R');
    buf.extend_from_slice(&8u32.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_parameter_status<S: AsyncWrite + Unpin>(
    sock: &mut S,
    name: &str,
    value: &str,
) -> Result<(), ServerError> {
    let payload_len = 4 + name.len() + 1 + value.len() + 1;
    let mut buf = Vec::with_capacity(1 + payload_len);
    buf.push(b'S');
    buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_backend_key_data<S: AsyncWrite + Unpin>(
    sock: &mut S,
    pid: u32,
    key: u32,
) -> Result<(), ServerError> {
    let mut buf = Vec::with_capacity(13);
    buf.push(b'K');
    buf.extend_from_slice(&12u32.to_be_bytes());
    buf.extend_from_slice(&pid.to_be_bytes());
    buf.extend_from_slice(&key.to_be_bytes());
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_ready_for_query<S: AsyncWrite + Unpin>(
    sock: &mut S,
    txn_status: u8,
) -> Result<(), ServerError> {
    let mut buf = Vec::with_capacity(6);
    buf.push(b'Z');
    buf.extend_from_slice(&5u32.to_be_bytes());
    buf.push(txn_status);
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_identify_system<S: AsyncWrite + Unpin>(
    sock: &mut S,
    identity: &Identity,
) -> Result<(), ServerError> {
    // RowDescription: 4 fields (systemid text, timeline int4, xlogpos text, dbname text)
    let fields = [
        ("systemid", 25u32), // text
        ("timeline", 23u32), // int4
        ("xlogpos", 25u32),
        ("dbname", 25u32),
    ];
    let mut row_desc = Vec::new();
    row_desc.push(b'T');
    let row_desc_len_pos = row_desc.len();
    row_desc.extend_from_slice(&0u32.to_be_bytes()); // placeholder length
    row_desc.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for (name, oid) in fields {
        row_desc.extend_from_slice(name.as_bytes());
        row_desc.push(0);
        row_desc.extend_from_slice(&0u32.to_be_bytes()); // table oid
        row_desc.extend_from_slice(&0u16.to_be_bytes()); // attnum
        row_desc.extend_from_slice(&oid.to_be_bytes());
        row_desc.extend_from_slice(&(-1i16).to_be_bytes()); // type length
        row_desc.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        row_desc.extend_from_slice(&0u16.to_be_bytes()); // format = text
    }
    let payload_len = row_desc.len() - row_desc_len_pos - 4 + 4;
    row_desc[row_desc_len_pos..row_desc_len_pos + 4]
        .copy_from_slice(&((payload_len + 4 - 4) as u32).to_be_bytes());
    let payload_len = row_desc.len() - 1 - 4; // bytes after header
    row_desc[row_desc_len_pos..row_desc_len_pos + 4]
        .copy_from_slice(&((payload_len + 4) as u32).to_be_bytes());
    sock.write_all(&row_desc).await?;

    // DataRow with the 4 column values.
    let xlogpos_str = format_pg_lsn(identity.xlogpos);
    let columns: [Option<String>; 4] = [
        Some(identity.system_id.clone()),
        Some(identity.timeline.to_string()),
        Some(xlogpos_str),
        identity.dbname.clone(),
    ];
    let mut row = Vec::new();
    row.push(b'D');
    let row_len_pos = row.len();
    row.extend_from_slice(&0u32.to_be_bytes());
    row.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for col in columns {
        match col {
            Some(s) => {
                row.extend_from_slice(&(s.len() as i32).to_be_bytes());
                row.extend_from_slice(s.as_bytes());
            }
            None => row.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    let payload_len = row.len() - 1 - 4;
    row[row_len_pos..row_len_pos + 4].copy_from_slice(&((payload_len + 4) as u32).to_be_bytes());
    sock.write_all(&row).await?;

    write_command_complete(sock, "IDENTIFY_SYSTEM").await?;
    Ok(())
}

async fn write_timeline_history<S: AsyncWrite + Unpin>(
    sock: &mut S,
    identity: &Identity,
) -> Result<(), ServerError> {
    // RowDescription: 2 fields (filename text, content bytea)
    let fields = [("filename", 25u32), ("content", 17u32)];
    let mut row_desc = Vec::new();
    row_desc.push(b'T');
    let row_desc_len_pos = row_desc.len();
    row_desc.extend_from_slice(&0u32.to_be_bytes());
    row_desc.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for (name, oid) in fields {
        row_desc.extend_from_slice(name.as_bytes());
        row_desc.push(0);
        row_desc.extend_from_slice(&0u32.to_be_bytes());
        row_desc.extend_from_slice(&0u16.to_be_bytes());
        row_desc.extend_from_slice(&oid.to_be_bytes());
        row_desc.extend_from_slice(&(-1i16).to_be_bytes());
        row_desc.extend_from_slice(&(-1i32).to_be_bytes());
        row_desc.extend_from_slice(&0u16.to_be_bytes());
    }
    let payload_len = row_desc.len() - 1 - 4;
    row_desc[row_desc_len_pos..row_desc_len_pos + 4]
        .copy_from_slice(&((payload_len + 4) as u32).to_be_bytes());
    sock.write_all(&row_desc).await?;

    // DataRow: filename = "<timeline>.history", content = "".
    let filename = format!("{:08X}.history", identity.timeline);
    let content: &[u8] = b"";
    let mut row = Vec::new();
    row.push(b'D');
    let row_len_pos = row.len();
    row.extend_from_slice(&0u32.to_be_bytes());
    row.extend_from_slice(&2u16.to_be_bytes());
    row.extend_from_slice(&(filename.len() as i32).to_be_bytes());
    row.extend_from_slice(filename.as_bytes());
    row.extend_from_slice(&(content.len() as i32).to_be_bytes());
    row.extend_from_slice(content);
    let payload_len = row.len() - 1 - 4;
    row[row_len_pos..row_len_pos + 4].copy_from_slice(&((payload_len + 4) as u32).to_be_bytes());
    sock.write_all(&row).await?;

    write_command_complete(sock, "TIMELINE_HISTORY").await?;
    Ok(())
}

async fn write_command_complete<S: AsyncWrite + Unpin>(
    sock: &mut S,
    tag: &str,
) -> Result<(), ServerError> {
    let payload_len = 4 + tag.len() + 1;
    let mut buf = Vec::with_capacity(1 + payload_len);
    buf.push(b'C');
    buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
    buf.extend_from_slice(tag.as_bytes());
    buf.push(0);
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_empty_query<S: AsyncWrite + Unpin>(sock: &mut S) -> Result<(), ServerError> {
    let mut buf = Vec::with_capacity(5);
    buf.push(b'I');
    buf.extend_from_slice(&4u32.to_be_bytes());
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_copy_both_response<S: AsyncWrite + Unpin>(sock: &mut S) -> Result<(), ServerError> {
    // 'W' | u32 length | u8 format (0 = text) | u16 ncols (0)
    let payload_len = 4 + 1 + 2;
    let mut buf = Vec::with_capacity(1 + payload_len);
    buf.push(b'W');
    buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(&0u16.to_be_bytes());
    sock.write_all(&buf).await?;
    Ok(())
}

async fn write_error_response<S: AsyncWrite + Unpin>(
    sock: &mut S,
    code: &str,
    message: &str,
) -> Result<(), ServerError> {
    let payload = {
        let mut v = Vec::new();
        v.push(b'S');
        v.extend_from_slice(b"ERROR\0");
        v.push(b'C');
        v.extend_from_slice(code.as_bytes());
        v.push(0);
        v.push(b'M');
        v.extend_from_slice(message.as_bytes());
        v.push(0);
        v.push(0);
        v
    };
    let len = 4 + payload.len();
    let mut buf = Vec::with_capacity(1 + len);
    buf.push(b'E');
    buf.extend_from_slice(&(len as u32).to_be_bytes());
    buf.extend_from_slice(&payload);
    sock.write_all(&buf).await?;
    Ok(())
}

fn format_pg_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

/// Decoded `'r'` standby status frame.
#[derive(Debug, Clone, Copy)]
pub struct StandbyStatusFrame {
    pub write_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    pub client_time: i64,
    pub reply_requested: bool,
}

/// Parse a `'r'` standby status update payload (the CopyData body
/// excluding the leading `'d'` framing byte that the conn layer
/// strips).
pub fn decode_standby_status(payload: &[u8]) -> Option<StandbyStatusFrame> {
    if payload.first().copied() != Some(b'r') {
        return None;
    }
    if payload.len() < 1 + 8 * 4 + 1 {
        return None;
    }
    let p = &payload[1..];
    let write_lsn = u64::from_be_bytes(p[0..8].try_into().unwrap());
    let flush_lsn = u64::from_be_bytes(p[8..16].try_into().unwrap());
    let apply_lsn = u64::from_be_bytes(p[16..24].try_into().unwrap());
    let client_time = i64::from_be_bytes(p[24..32].try_into().unwrap());
    let reply_requested = p[32] != 0;
    Some(StandbyStatusFrame {
        write_lsn,
        flush_lsn,
        apply_lsn,
        client_time,
        reply_requested,
    })
}

/// Per-connection writer + CopyData decoder used while replication is
/// active. Built once `handshake_and_await_start` returns; the caller
/// pumps `'w'`/`'k'` bytes via `write_raw` and reads inbound `'r'`
/// via `try_recv_frame`.
pub struct WalSenderConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    sock: S,
    rx: BytesMut,
}

impl<S> WalSenderConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(sock: S) -> Self {
        Self {
            sock,
            rx: BytesMut::with_capacity(8192),
        }
    }

    /// Frame `bytes` (a server-direction CopyData payload —
    /// `'w'` XLogData or `'k'` keepalive) under PG's `d` CopyData
    /// envelope and ship.
    pub async fn write_raw(&mut self, bytes: &[u8]) -> Result<(), ServerError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let payload_len = 4 + bytes.len();
        let mut buf = Vec::with_capacity(1 + payload_len);
        buf.push(b'd');
        buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
        buf.extend_from_slice(bytes);
        self.sock.write_all(&buf).await?;
        Ok(())
    }

    /// Ship already-CopyData-framed bytes verbatim (no further
    /// wrapping). Used when the caller pre-frames frames at
    /// enqueue time so multiple frames can be concatenated in a
    /// single send buffer.
    pub async fn write_framed(&mut self, bytes: &[u8]) -> Result<(), ServerError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.sock.write_all(bytes).await?;
        Ok(())
    }

    /// Drain inbound bytes, returning the next complete CopyData
    /// payload's body (without the `'d'` envelope) once available.
    /// Returns `Ok(None)` on clean close.
    pub async fn try_recv_frame(&mut self) -> Result<Option<Vec<u8>>, ServerError> {
        loop {
            if let Some(body) = parse_one_copy_data(&mut self.rx)? {
                return Ok(Some(body));
            }
            let n = self.sock.read_buf(&mut self.rx).await?;
            if n == 0 {
                return Ok(None);
            }
        }
    }

    pub fn into_inner(self) -> S {
        self.sock
    }
}

fn parse_one_copy_data(rx: &mut BytesMut) -> Result<Option<Vec<u8>>, ServerError> {
    if rx.len() < 5 {
        return Ok(None);
    }
    let kind = rx[0];
    let len = u32::from_be_bytes(rx[1..5].try_into().unwrap()) as usize;
    if len < 4 {
        return Err(ServerError::Protocol(format!(
            "copy-data length {len} too short"
        )));
    }
    let total = 1 + len;
    if rx.len() < total {
        return Ok(None);
    }
    match kind {
        b'd' => {
            let mut frame = rx.split_to(total);
            frame.advance(5);
            Ok(Some(frame.to_vec()))
        }
        b'c' => {
            let _ = rx.split_to(total);
            Err(ServerError::Protocol("client sent CopyDone".into()))
        }
        b'f' => {
            let _ = rx.split_to(total);
            Err(ServerError::Protocol("client sent CopyFail".into()))
        }
        b'X' => {
            let _ = rx.split_to(total);
            Err(ServerError::Protocol("client sent Terminate".into()))
        }
        other => {
            let _ = rx.split_to(total);
            Err(ServerError::Protocol(format!(
                "unexpected CopyBoth message tag {:?}",
                other as char
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    fn build_startup_message(params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let len = 8 + body.len();
        let mut buf = Vec::with_capacity(len);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(&(196608u32).to_be_bytes()); // protocol 3.0
        buf.extend_from_slice(&body);
        buf
    }

    fn build_simple_query(q: &str) -> Vec<u8> {
        let payload_len = 4 + q.len() + 1;
        let mut buf = Vec::with_capacity(1 + payload_len);
        buf.push(b'Q');
        buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
        buf.extend_from_slice(q.as_bytes());
        buf.push(0);
        buf
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handshake_identifies_system_and_starts_replication() {
        let (client, server) = tokio::io::duplex(8192);
        let client_task = tokio::spawn(async move {
            let mut client = client;
            client
                .write_all(&build_startup_message(&[
                    ("user", "u"),
                    ("database", "u"),
                    ("replication", "true"),
                ]))
                .await
                .unwrap();
            // Drain the startup response until ReadyForQuery 'Z'.
            let mut tag = [0u8; 1];
            loop {
                client.read_exact(&mut tag).await.unwrap();
                let mut len_buf = [0u8; 4];
                client.read_exact(&mut len_buf).await.unwrap();
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; len - 4];
                if !body.is_empty() {
                    client.read_exact(&mut body).await.unwrap();
                }
                if tag[0] == b'Z' {
                    break;
                }
            }
            client
                .write_all(&build_simple_query("IDENTIFY_SYSTEM"))
                .await
                .unwrap();
            // Drain IDENTIFY_SYSTEM response (T, D, C, Z).
            loop {
                client.read_exact(&mut tag).await.unwrap();
                let mut len_buf = [0u8; 4];
                client.read_exact(&mut len_buf).await.unwrap();
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; len - 4];
                if !body.is_empty() {
                    client.read_exact(&mut body).await.unwrap();
                }
                if tag[0] == b'Z' {
                    break;
                }
            }
            client
                .write_all(&build_simple_query("START_REPLICATION PHYSICAL 0/16B3750"))
                .await
                .unwrap();
            // Drain CopyBothResponse 'W'.
            client.read_exact(&mut tag).await.unwrap();
            assert_eq!(tag[0], b'W');
            let mut len_buf = [0u8; 4];
            client.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len - 4];
            client.read_exact(&mut body).await.unwrap();
        });
        let identity = Identity {
            system_id: "7340000000000000000".into(),
            timeline: 1,
            xlogpos: 0x016B_3750,
            dbname: None,
        };
        let mut server = server;
        let started = handshake_and_await_start(&mut server, &identity)
            .await
            .expect("handshake");
        assert_eq!(started.start_lsn, 0x016B_3750);
        assert_eq!(started.timeline, 1);
        client_task.await.unwrap();
    }

    #[test]
    fn parse_start_replication_forms() {
        let s = parse_start_replication("START_REPLICATION PHYSICAL 0/16B3750").unwrap();
        assert_eq!(s.start_lsn, 0x016B_3750);
        assert_eq!(s.timeline, 1);
        let s =
            parse_start_replication("START_REPLICATION SLOT phys PHYSICAL 1/0 TIMELINE 2").unwrap();
        assert_eq!(s.start_lsn, 1u64 << 32);
        assert_eq!(s.timeline, 2);
        assert_eq!(s.slot.as_deref(), Some("phys"));
    }

    #[test]
    fn decode_standby_status_roundtrip() {
        // Mirror what wal-rs builds on the client side.
        let payload = crate::pg::replication::stream::build_status_update(0x10, 0x08, 0x04);
        let parsed = decode_standby_status(&payload).expect("decode");
        assert_eq!(parsed.write_lsn, 0x10);
        assert_eq!(parsed.flush_lsn, 0x08);
        assert_eq!(parsed.apply_lsn, 0x04);
    }
}
