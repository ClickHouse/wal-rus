//! Walsender server side of the physical replication protocol.
//!
//! Pairs with [`super::conn`] (client side) so walrus can play either
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

use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::pg::backup::format_pg_lsn;

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
    // Batch the startup-response messages into one BytesMut and flush
    // once. Each encode_* helper appends without allocating a private
    // Vec / issuing its own syscall
    let mut tx = BytesMut::with_capacity(512);
    encode_auth_ok(&mut tx);
    encode_parameter_status(&mut tx, "server_version", "16.3");
    encode_parameter_status(&mut tx, "server_encoding", "UTF8");
    encode_parameter_status(&mut tx, "client_encoding", "UTF8");
    encode_parameter_status(&mut tx, "DateStyle", "ISO, MDY");
    encode_parameter_status(&mut tx, "integer_datetimes", "on");
    encode_parameter_status(&mut tx, "TimeZone", "UTC");
    encode_parameter_status(&mut tx, "standard_conforming_strings", "on");
    encode_parameter_status(&mut tx, "in_hot_standby", "off");
    encode_backend_key_data(&mut tx, 1, 1);
    encode_ready_for_query(&mut tx, b'I');
    flush_tx(sock, &mut tx).await?;

    let mut rx = BytesMut::with_capacity(8192);
    loop {
        let msg = read_typed_message(sock, &mut rx).await?;
        match msg.kind {
            b'Q' => {
                let query = parse_simple_query(&msg.body)?;
                if let Some(start) = dispatch_query(sock, &mut tx, &query, identity).await? {
                    return Ok(start);
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

async fn flush_tx<S: AsyncWrite + Unpin>(
    sock: &mut S,
    tx: &mut BytesMut,
) -> Result<(), ServerError> {
    if tx.is_empty() {
        return Ok(());
    }
    sock.write_all(tx).await?;
    tx.clear();
    Ok(())
}

/// One framed message read from the wire (tag + body).
#[derive(Debug)]
struct TypedMessage {
    kind: u8,
    body: Bytes,
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
    let mut frame = rx.split_to(total).freeze();
    frame.advance(5); // tag + length consumed; freeze gave us a Bytes
    Ok(TypedMessage { kind, body: frame })
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
        sock.write_all(b"N").await?;
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
///
/// All response bytes are appended to the shared `tx` buffer and
/// flushed once per query — replaces N small per-message syscalls
/// (and per-helper Vec allocs) with one
async fn dispatch_query<S>(
    sock: &mut S,
    tx: &mut BytesMut,
    query: &str,
    identity: &Identity,
) -> Result<Option<StartReplication>, ServerError>
where
    S: AsyncWrite + Unpin,
{
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("IDENTIFY_SYSTEM") {
        encode_identify_system(tx, identity);
        encode_ready_for_query(tx, b'I');
        flush_tx(sock, tx).await?;
        Ok(None)
    } else if upper.starts_with("TIMELINE_HISTORY") {
        encode_timeline_history(tx, identity);
        encode_ready_for_query(tx, b'I');
        flush_tx(sock, tx).await?;
        Ok(None)
    } else if upper.starts_with("START_REPLICATION") {
        let start = parse_start_replication(trimmed)?;
        // Switch to CopyBoth.
        encode_copy_both_response(tx);
        flush_tx(sock, tx).await?;
        Ok(Some(start))
    } else if upper.starts_with("SHOW ") || upper.starts_with("BEGIN") || upper.starts_with("END") {
        // PG walreceiver issues SHOW data_directory_mode (or similar)
        // probes on startup with newer versions; ack with empty result.
        encode_empty_query(tx);
        encode_ready_for_query(tx, b'I');
        flush_tx(sock, tx).await?;
        Ok(None)
    } else {
        encode_error_response(tx, "0A000", &format!("unsupported query: {trimmed}"));
        encode_ready_for_query(tx, b'I');
        flush_tx(sock, tx).await?;
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
//
// Encoders append directly into a shared BytesMut so the handshake /
// query dispatch flushes once per phase, instead of one syscall + one
// fresh Vec per message

fn encode_auth_ok(tx: &mut BytesMut) {
    tx.extend_from_slice(b"R");
    tx.extend_from_slice(&8u32.to_be_bytes());
    tx.extend_from_slice(&0u32.to_be_bytes());
}

fn encode_parameter_status(tx: &mut BytesMut, name: &str, value: &str) {
    let payload_len = 4 + name.len() + 1 + value.len() + 1;
    tx.extend_from_slice(b"S");
    tx.extend_from_slice(&(payload_len as u32).to_be_bytes());
    tx.extend_from_slice(name.as_bytes());
    tx.extend_from_slice(b"\0");
    tx.extend_from_slice(value.as_bytes());
    tx.extend_from_slice(b"\0");
}

fn encode_backend_key_data(tx: &mut BytesMut, pid: u32, key: u32) {
    tx.extend_from_slice(b"K");
    tx.extend_from_slice(&12u32.to_be_bytes());
    tx.extend_from_slice(&pid.to_be_bytes());
    tx.extend_from_slice(&key.to_be_bytes());
}

fn encode_ready_for_query(tx: &mut BytesMut, txn_status: u8) {
    tx.extend_from_slice(b"Z");
    tx.extend_from_slice(&5u32.to_be_bytes());
    tx.extend_from_slice(&[txn_status]);
}

fn encode_identify_system(tx: &mut BytesMut, identity: &Identity) {
    // RowDescription: 4 fields (systemid text, timeline int4, xlogpos text, dbname text)
    let fields = [
        ("systemid", 25u32), // text
        ("timeline", 23u32), // int4
        ("xlogpos", 25u32),
        ("dbname", 25u32),
    ];
    let row_desc_tag_pos = tx.len();
    tx.extend_from_slice(b"T");
    let row_desc_len_pos = tx.len();
    tx.extend_from_slice(&0u32.to_be_bytes()); // placeholder length
    tx.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for (name, oid) in fields {
        tx.extend_from_slice(name.as_bytes());
        tx.extend_from_slice(b"\0");
        tx.extend_from_slice(&0u32.to_be_bytes()); // table oid
        tx.extend_from_slice(&0u16.to_be_bytes()); // attnum
        tx.extend_from_slice(&oid.to_be_bytes());
        tx.extend_from_slice(&(-1i16).to_be_bytes()); // type length
        tx.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        tx.extend_from_slice(&0u16.to_be_bytes()); // format = text
    }
    let payload_len = (tx.len() - row_desc_tag_pos - 1) as u32;
    tx[row_desc_len_pos..row_desc_len_pos + 4].copy_from_slice(&payload_len.to_be_bytes());

    // DataRow with the 4 column values.
    let xlogpos_str = format_pg_lsn(identity.xlogpos).to_string();
    let columns: [Option<&str>; 4] = [
        Some(identity.system_id.as_str()),
        None, // timeline rendered below (needs a String)
        Some(xlogpos_str.as_str()),
        identity.dbname.as_deref(),
    ];
    let timeline_str = identity.timeline.to_string();
    let row_tag_pos = tx.len();
    tx.extend_from_slice(b"D");
    let row_len_pos = tx.len();
    tx.extend_from_slice(&0u32.to_be_bytes());
    tx.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for (idx, col) in columns.iter().enumerate() {
        let val = if idx == 1 {
            Some(timeline_str.as_str())
        } else {
            *col
        };
        match val {
            Some(s) => {
                tx.extend_from_slice(&(s.len() as i32).to_be_bytes());
                tx.extend_from_slice(s.as_bytes());
            }
            None => tx.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    let payload_len = (tx.len() - row_tag_pos - 1) as u32;
    tx[row_len_pos..row_len_pos + 4].copy_from_slice(&payload_len.to_be_bytes());

    encode_command_complete(tx, "IDENTIFY_SYSTEM");
}

fn encode_timeline_history(tx: &mut BytesMut, identity: &Identity) {
    // RowDescription: 2 fields (filename text, content bytea)
    let fields = [("filename", 25u32), ("content", 17u32)];
    let row_desc_tag_pos = tx.len();
    tx.extend_from_slice(b"T");
    let row_desc_len_pos = tx.len();
    tx.extend_from_slice(&0u32.to_be_bytes());
    tx.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for (name, oid) in fields {
        tx.extend_from_slice(name.as_bytes());
        tx.extend_from_slice(b"\0");
        tx.extend_from_slice(&0u32.to_be_bytes());
        tx.extend_from_slice(&0u16.to_be_bytes());
        tx.extend_from_slice(&oid.to_be_bytes());
        tx.extend_from_slice(&(-1i16).to_be_bytes());
        tx.extend_from_slice(&(-1i32).to_be_bytes());
        tx.extend_from_slice(&0u16.to_be_bytes());
    }
    let payload_len = (tx.len() - row_desc_tag_pos - 1) as u32;
    tx[row_desc_len_pos..row_desc_len_pos + 4].copy_from_slice(&payload_len.to_be_bytes());

    // DataRow: filename = "<timeline>.history", content = "".
    let filename = format!("{:08X}.history", identity.timeline);
    let content: &[u8] = b"";
    let row_tag_pos = tx.len();
    tx.extend_from_slice(b"D");
    let row_len_pos = tx.len();
    tx.extend_from_slice(&0u32.to_be_bytes());
    tx.extend_from_slice(&2u16.to_be_bytes());
    tx.extend_from_slice(&(filename.len() as i32).to_be_bytes());
    tx.extend_from_slice(filename.as_bytes());
    tx.extend_from_slice(&(content.len() as i32).to_be_bytes());
    tx.extend_from_slice(content);
    let payload_len = (tx.len() - row_tag_pos - 1) as u32;
    tx[row_len_pos..row_len_pos + 4].copy_from_slice(&payload_len.to_be_bytes());

    encode_command_complete(tx, "TIMELINE_HISTORY");
}

fn encode_command_complete(tx: &mut BytesMut, tag: &str) {
    let payload_len = 4 + tag.len() + 1;
    tx.extend_from_slice(b"C");
    tx.extend_from_slice(&(payload_len as u32).to_be_bytes());
    tx.extend_from_slice(tag.as_bytes());
    tx.extend_from_slice(b"\0");
}

fn encode_empty_query(tx: &mut BytesMut) {
    tx.extend_from_slice(b"I");
    tx.extend_from_slice(&4u32.to_be_bytes());
}

fn encode_copy_both_response(tx: &mut BytesMut) {
    // 'W' | u32 length | u8 format (0 = text) | u16 ncols (0)
    let payload_len = 4 + 1 + 2;
    tx.extend_from_slice(b"W");
    tx.extend_from_slice(&(payload_len as u32).to_be_bytes());
    tx.extend_from_slice(&[0]);
    tx.extend_from_slice(&0u16.to_be_bytes());
}

fn encode_error_response(tx: &mut BytesMut, code: &str, message: &str) {
    let payload_len = 1 + b"ERROR\0".len() + 1 + code.len() + 1 + 1 + message.len() + 1 + 1;
    let len = 4 + payload_len;
    tx.extend_from_slice(b"E");
    tx.extend_from_slice(&(len as u32).to_be_bytes());
    tx.extend_from_slice(b"S");
    tx.extend_from_slice(b"ERROR\0");
    tx.extend_from_slice(b"C");
    tx.extend_from_slice(code.as_bytes());
    tx.extend_from_slice(b"\0");
    tx.extend_from_slice(b"M");
    tx.extend_from_slice(message.as_bytes());
    tx.extend_from_slice(b"\0");
    tx.extend_from_slice(b"\0");
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
    /// Reused send buffer so `write_raw` doesn't allocate per frame.
    /// Multiple frames can be staged via [`Self::enqueue_raw`] /
    /// [`Self::enqueue_framed`] and shipped together with
    /// [`Self::flush`]
    tx: BytesMut,
}

impl<S> WalSenderConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(sock: S) -> Self {
        Self {
            sock,
            rx: BytesMut::with_capacity(8192),
            tx: BytesMut::with_capacity(8192),
        }
    }

    /// Append a server-direction CopyData payload (`'w'` XLogData or
    /// `'k'` keepalive) into the send buffer under the `'d'` CopyData
    /// envelope. Does not flush — call [`Self::flush`] explicitly when
    /// staging multiple frames
    pub fn enqueue_raw(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let payload_len = (4 + bytes.len()) as u32;
        self.tx.extend_from_slice(b"d");
        self.tx.extend_from_slice(&payload_len.to_be_bytes());
        self.tx.extend_from_slice(bytes);
    }

    /// Append already-CopyData-framed bytes (caller pre-built the `'d'`
    /// envelope). Used when callers frame ahead of the conn to batch
    /// multiple frames without staging copies
    pub fn enqueue_framed(&mut self, bytes: &[u8]) {
        self.tx.extend_from_slice(bytes);
    }

    /// Drain the staged tx buffer onto the wire and clear it
    pub async fn flush(&mut self) -> Result<(), ServerError> {
        if self.tx.is_empty() {
            return Ok(());
        }
        self.sock.write_all(&self.tx).await?;
        self.tx.clear();
        Ok(())
    }

    /// Frame `bytes` (a server-direction CopyData payload —
    /// `'w'` XLogData or `'k'` keepalive) under PG's `d` CopyData
    /// envelope and ship. Convenience: equivalent to
    /// `enqueue_raw(bytes); flush()`
    pub async fn write_raw(&mut self, bytes: &[u8]) -> Result<(), ServerError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.enqueue_raw(bytes);
        self.flush().await
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
    /// Returns `Ok(None)` on clean close. Body is a `Bytes` slice into
    /// the read buffer (refcounted, no copy)
    pub async fn try_recv_frame(&mut self) -> Result<Option<Bytes>, ServerError> {
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

fn parse_one_copy_data(rx: &mut BytesMut) -> Result<Option<Bytes>, ServerError> {
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
            let mut frame = rx.split_to(total).freeze();
            frame.advance(5);
            Ok(Some(frame))
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
        // Mirror what walrus builds on the client side.
        let payload = crate::pg::replication::stream::build_status_update(0x10, 0x08, 0x04);
        let parsed = decode_standby_status(&payload).expect("decode");
        assert_eq!(parsed.write_lsn, 0x10);
        assert_eq!(parsed.flush_lsn, 0x08);
        assert_eq!(parsed.apply_lsn, 0x04);
    }

    #[test]
    fn decode_standby_status_rejects_bad_input() {
        // Wrong leading tag, even at the right length
        assert!(decode_standby_status(&[b'x'; 1 + 8 * 4 + 1]).is_none());
        // Right tag but truncated
        assert!(decode_standby_status(b"r").is_none());
        assert!(decode_standby_status(&[]).is_none());
    }

    /// Untyped startup frame with an arbitrary protocol code + body
    fn build_startup_raw(protocol: u32, body: &[u8]) -> Vec<u8> {
        let len = 8 + body.len();
        let mut buf = Vec::with_capacity(len);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(&protocol.to_be_bytes());
        buf.extend_from_slice(body);
        buf
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_startup_negotiates_ssl_then_gssenc_then_startup() {
        // SSLRequest -> 'N', GSSENCRequest -> 'N', then the real StartupMessage
        let (client, server) = tokio::io::duplex(4096);
        let client_task = tokio::spawn(async move {
            let mut client = client;
            let mut n = [0u8; 1];
            client
                .write_all(&build_startup_raw(80877103, &[]))
                .await
                .unwrap();
            client.read_exact(&mut n).await.unwrap();
            assert_eq!(n[0], b'N');
            client
                .write_all(&build_startup_raw(80877104, &[]))
                .await
                .unwrap();
            client.read_exact(&mut n).await.unwrap();
            assert_eq!(n[0], b'N');
            client
                .write_all(&build_startup_message(&[
                    ("user", "u"),
                    ("replication", "true"),
                ]))
                .await
                .unwrap();
        });
        let mut server = server;
        let params = read_startup(&mut server).await.expect("read_startup");
        assert_eq!(params.get("user").map(String::as_str), Some("u"));
        assert_eq!(params.get("replication").map(String::as_str), Some("true"));
        client_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_startup_rejects_old_protocol() {
        let (client, server) = tokio::io::duplex(256);
        let writer = tokio::spawn(async move {
            let mut client = client;
            // protocol 2.0 — unsupported
            client
                .write_all(&build_startup_raw(0x0002_0000, b"user\0u\0\0"))
                .await
                .unwrap();
        });
        let mut server = server;
        let err = read_startup(&mut server).await.unwrap_err();
        assert!(
            format!("{err}").contains("unsupported protocol version"),
            "{err}"
        );
        writer.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_startup_rejects_short_length() {
        let (client, server) = tokio::io::duplex(64);
        let writer = tokio::spawn(async move {
            let mut client = client;
            let mut buf = Vec::new();
            buf.extend_from_slice(&4u32.to_be_bytes()); // length < 8
            buf.extend_from_slice(&196608u32.to_be_bytes());
            client.write_all(&buf).await.unwrap();
        });
        let mut server = server;
        let err = read_startup(&mut server).await.unwrap_err();
        assert!(format!("{err}").contains("too short"), "{err}");
        writer.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_startup_rejects_unterminated_key_and_value() {
        for (body, needle) in [
            (&b"keynoterminator"[..], "key not null-terminated"),
            (&b"user\0valnoterminator"[..], "value not null-terminated"),
        ] {
            let (client, server) = tokio::io::duplex(256);
            let raw = build_startup_raw(196608, body);
            let writer = tokio::spawn(async move {
                let mut client = client;
                client.write_all(&raw).await.unwrap();
            });
            let mut server = server;
            let err = read_startup(&mut server).await.unwrap_err();
            assert!(format!("{err}").contains(needle), "{err}");
            writer.await.unwrap();
        }
    }

    #[test]
    fn parse_simple_query_arms() {
        assert_eq!(
            parse_simple_query(b"IDENTIFY_SYSTEM\0").unwrap(),
            "IDENTIFY_SYSTEM"
        );
        assert!(parse_simple_query(b"no-nul").is_err());
        assert!(parse_simple_query(&[0xff, 0xfe, 0x00]).is_err());
    }

    #[test]
    fn parse_start_replication_error_arms() {
        assert!(parse_start_replication("START_REPLICATION SLOT").is_err());
        assert!(matches!(
            parse_start_replication("START_REPLICATION LOGICAL 0/0"),
            Err(ServerError::Unsupported(_))
        ));
        assert!(parse_start_replication("START_REPLICATION PHYSICAL").is_err());
        assert!(parse_start_replication("START_REPLICATION PHYSICAL notanlsn").is_err());
        assert!(parse_start_replication("START_REPLICATION PHYSICAL 0/0 TIMELINE").is_err());
        assert!(parse_start_replication("START_REPLICATION PHYSICAL 0/0 TIMELINE xx").is_err());
    }

    #[test]
    fn parse_one_copy_data_arms() {
        // incomplete header -> None
        let mut rx = BytesMut::from(&[b'd', 0, 0][..]);
        assert!(parse_one_copy_data(&mut rx).unwrap().is_none());
        // declared length < 4 -> error
        let mut rx = BytesMut::from(&[b'd', 0, 0, 0, 3][..]);
        assert!(parse_one_copy_data(&mut rx).is_err());
        // header present but body short -> None (await more)
        let mut rx = BytesMut::from(&[b'd', 0, 0, 0, 8, 1, 2][..]);
        assert!(parse_one_copy_data(&mut rx).unwrap().is_none());
        // complete 'd' frame -> body without the envelope
        let mut rx = BytesMut::from(&[b'd', 0, 0, 0, 8, 1, 2, 3, 4][..]);
        let body = parse_one_copy_data(&mut rx).unwrap().unwrap();
        assert_eq!(&body[..], &[1, 2, 3, 4]);
        // control tags surface as protocol errors
        for tag in [b'c', b'f', b'X', b'q'] {
            let mut rx = BytesMut::from(&[tag, 0, 0, 0, 4][..]);
            assert!(parse_one_copy_data(&mut rx).is_err(), "tag {}", tag as char);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn walsender_conn_write_paths() {
        let (client, server) = tokio::io::duplex(4096);
        let mut conn = WalSenderConn::new(server);
        // empty inputs are no-ops
        conn.write_raw(&[]).await.unwrap();
        conn.write_framed(&[]).await.unwrap();
        conn.enqueue_raw(&[]);
        conn.flush().await.unwrap();
        // stage a raw payload (gets the 'd' envelope) then a pre-framed frame
        conn.enqueue_raw(&[1, 2, 3]);
        let mut framed = Vec::new();
        framed.extend_from_slice(b"d");
        framed.extend_from_slice(&6u32.to_be_bytes());
        framed.extend_from_slice(&[9, 9]);
        conn.enqueue_framed(&framed);
        conn.flush().await.unwrap();

        let mut client = client;
        let mut buf = [0u8; 15];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf[0], b'd');
        assert_eq!(u32::from_be_bytes(buf[1..5].try_into().unwrap()), 7);
        assert_eq!(&buf[5..8], &[1, 2, 3]);
        assert_eq!(buf[8], b'd');
        assert_eq!(u32::from_be_bytes(buf[9..13].try_into().unwrap()), 6);
        assert_eq!(&buf[13..15], &[9, 9]);

        let _sock = conn.into_inner();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn walsender_conn_recv_clean_close() {
        let (client, server) = tokio::io::duplex(64);
        drop(client);
        let mut conn = WalSenderConn::new(server);
        assert!(conn.try_recv_frame().await.unwrap().is_none());
    }

    async fn run_dispatch(
        query: &str,
        identity: &Identity,
    ) -> (Result<Option<StartReplication>, ServerError>, Vec<u8>) {
        let (client, mut server) = tokio::io::duplex(8192);
        let mut tx = BytesMut::new();
        let res = dispatch_query(&mut server, &mut tx, query, identity).await;
        drop(server); // close the write half so read_to_end terminates
        let mut client = client;
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        (res, buf)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_query_arms() {
        let identity = Identity {
            system_id: "7340000000000000000".into(),
            timeline: 1,
            xlogpos: 0x10,
            dbname: Some("db".into()),
        };

        let (res, buf) = run_dispatch("IDENTIFY_SYSTEM", &identity).await;
        assert!(matches!(res, Ok(None)));
        assert_eq!(buf[0], b'T'); // RowDescription first

        let (res, buf) = run_dispatch("TIMELINE_HISTORY 1", &identity).await;
        assert!(matches!(res, Ok(None)));
        assert_eq!(buf[0], b'T');
        assert!(
            buf.windows(b"00000001.history".len())
                .any(|w| w == b"00000001.history"),
            "timeline history filename missing"
        );

        let (res, buf) = run_dispatch("START_REPLICATION PHYSICAL 0/0", &identity).await;
        let start = res.unwrap().expect("START_REPLICATION yields start");
        assert_eq!(start.start_lsn, 0);
        assert_eq!(buf[0], b'W'); // CopyBothResponse

        for q in ["SHOW data_directory_mode", "BEGIN", "END"] {
            let (res, buf) = run_dispatch(q, &identity).await;
            assert!(matches!(res, Ok(None)), "{q}");
            assert_eq!(buf[0], b'I', "{q} should emit EmptyQueryResponse");
        }

        let (res, buf) = run_dispatch("VACUUM", &identity).await;
        assert!(matches!(res, Err(ServerError::Unsupported(_))));
        assert_eq!(buf[0], b'E'); // ErrorResponse
    }
}
