//! Synchronous (blocking) replication-mode pg connection.
//!
//! The sync sibling of [`super::conn::ReplicationConn`], used by the
//! `SyncReplica` receive hot path so the whole loop runs on one OS thread with
//! direct syscalls — no tokio, no `spawn_blocking`. It reuses the same
//! transport-agnostic `postgres_protocol` codecs (`Message::parse`, `frontend`,
//! the SCRAM SASL type) and the same rustls `ClientConfig`; only the socket I/O
//! differs (`std::net::TcpStream` + a blocking `rustls::StreamOwned`).
//!
//! Reads carry a `SO_RCVTIMEO`-style timeout (std `set_read_timeout`) so a quiet
//! stream still wakes the loop to send keepalives and poll shutdown / retarget.

use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use super::conn::{PgConfig, error_code, error_message, message_kind};
use super::tls::{SyncStream, maybe_upgrade_sync};

/// Outcome of a blocking `recv_message`: a parsed backend message, or the read
/// timed out (the stream was quiet for the configured interval).
pub(crate) enum RecvOutcome {
    Message(Message),
    Timeout,
}

/// Blocking replication connection. Same wire protocol as
/// [`super::conn::ReplicationConn`], driven over a `std::net` socket.
pub(crate) struct SyncReplicationConn {
    socket: SyncStream,
    rx: BytesMut,
    tx: BytesMut,
}

impl SyncReplicationConn {
    /// Replication-mode connection (`replication=true`): connect TCP, negotiate
    /// TLS, send the startup packet, authenticate, and await ReadyForQuery.
    /// Unix-socket hosts (PGHOST starting with `/`) are unsupported on the sync
    /// path — the SyncReplica receiver always streams over TCP.
    pub(crate) fn connect(cfg: &PgConfig, read_timeout: Duration) -> Result<Self> {
        Self::connect_with(cfg, true, read_timeout)
    }

    /// Connect with or without the `replication` startup parameter. Mirrors
    /// [`super::conn::ReplicationConn::connect_with`].
    pub(crate) fn connect_with(
        cfg: &PgConfig,
        replication: bool,
        read_timeout: Duration,
    ) -> Result<Self> {
        if cfg.host.starts_with('/') {
            bail!(
                "sync replication connection requires a TCP host, got unix path {}",
                cfg.host
            );
        }
        let addr = format!("{}:{}", cfg.host, cfg.port);
        let raw =
            std::net::TcpStream::connect(&addr).with_context(|| format!("connect to {addr}"))?;
        let (socket, _tls) = maybe_upgrade_sync(raw, &cfg.host, cfg.sslmode, &cfg.tls)
            .with_context(|| format!("tls negotiation against {addr}"))?;
        // Disable Nagle so the small status ack every commit blocks on isn't
        // buffered (sole-acker latency), and bound reads so a quiet stream wakes.
        if let Err(e) = socket.set_nodelay(true) {
            tracing::warn!(target = "wal_receive", %addr, "set TCP_NODELAY failed: {e}");
        }
        socket
            .set_read_timeout(Some(read_timeout))
            .with_context(|| format!("set read timeout on {addr}"))?;

        let mut conn = SyncReplicationConn {
            socket,
            rx: BytesMut::with_capacity(64 * 1024),
            tx: BytesMut::with_capacity(8 * 1024),
        };

        let params: [(&str, &str); 5] = [
            ("user", cfg.user.as_str()),
            ("database", cfg.database.as_str()),
            ("application_name", cfg.application_name.as_str()),
            ("client_encoding", "UTF8"),
            ("replication", "true"),
        ];
        let params = if replication {
            &params[..]
        } else {
            &params[..4]
        };
        frontend::startup_message(params.iter().copied(), &mut conn.tx)?;
        conn.flush()?;

        conn.do_auth(cfg)?;
        conn.await_ready_for_query()?;
        Ok(conn)
    }

    fn send_query(&mut self, q: &str) -> Result<()> {
        frontend::query(q, &mut self.tx)?;
        self.flush()
    }

    /// Send a CopyData payload (standby status update during a stream).
    pub(crate) fn send_copy_data(&mut self, payload: &[u8]) -> Result<()> {
        let msg = frontend::CopyData::new(payload).map_err(|e| anyhow!("copy-data frame: {e}"))?;
        msg.write(&mut self.tx);
        self.flush()
    }

    /// Run a simple query, collecting every `DataRow` as text columns.
    pub(crate) fn query_rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.send_query(sql)?;
        let mut rows = Vec::new();
        loop {
            match self.recv_message_blocking()? {
                Message::DataRow(row) => rows.push(data_row_text(&row)?),
                Message::ReadyForQuery(_) => break,
                Message::ErrorResponse(e) => bail!("query `{sql}`: {}", error_message(&e)),
                _ => {}
            }
        }
        Ok(rows)
    }

    /// `CREATE_REPLICATION_SLOT <name> PHYSICAL`. A pre-existing slot (42710) is
    /// treated as success (idempotent across a crash between check and create).
    pub(crate) fn create_physical_replication_slot(&mut self, name: &str) -> Result<()> {
        self.send_query(&format!("CREATE_REPLICATION_SLOT {name} PHYSICAL"))?;
        loop {
            match self.recv_message_blocking()? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::ErrorResponse(e) => {
                    if error_code(&e) == "42710" {
                        self.await_ready_for_query()?;
                        return Ok(());
                    }
                    bail!("CREATE_REPLICATION_SLOT {name}: {}", error_message(&e));
                }
                _ => {}
            }
        }
    }

    /// `TIMELINE_HISTORY <tli>` -> (filename, contents), `None` when the server
    /// has no history file for the timeline (58P01).
    pub(crate) fn timeline_history(&mut self, timeline: u32) -> Result<Option<(String, Vec<u8>)>> {
        self.send_query(&format!("TIMELINE_HISTORY {timeline}"))?;
        let mut out = None;
        loop {
            match self.recv_message_blocking()? {
                Message::DataRow(row) => {
                    let cols = data_row_bytes(&row)?;
                    let name = cols
                        .first()
                        .and_then(|c| c.as_ref())
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_else(|| format!("{timeline:08X}.history"));
                    let content = cols.get(1).and_then(|c| c.clone()).unwrap_or_default();
                    out = Some((name, content));
                }
                Message::ReadyForQuery(_) => break,
                Message::ErrorResponse(e) => {
                    if error_code(&e) == "58P01" {
                        self.await_ready_for_query()?;
                        return Ok(None);
                    }
                    bail!("TIMELINE_HISTORY {timeline}: {}", error_message(&e));
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// `START_REPLICATION ... <lsn> TIMELINE <tli>` then consume the
    /// `CopyBothResponse` so the connection is in CopyBoth mode.
    pub(crate) fn start_replication(
        &mut self,
        slot_name: Option<&str>,
        start_lsn: u64,
        timeline: u32,
    ) -> Result<()> {
        let lsn = crate::pg::backup::format_pg_lsn(start_lsn);
        let cmd = match slot_name {
            Some(slot) => {
                format!("START_REPLICATION SLOT {slot} PHYSICAL {lsn} TIMELINE {timeline}")
            }
            None => format!("START_REPLICATION {lsn} TIMELINE {timeline}"),
        };
        self.send_query(&cmd)?;
        self.expect_copy_both_open()
    }

    /// Send a client `CopyDone` and drain to `ReadyForQuery` (returns to
    /// simple-query mode after a server-initiated CopyDone / timeline switch).
    pub(crate) fn end_copy(&mut self) -> Result<()> {
        frontend::copy_done(&mut self.tx);
        self.flush()?;
        loop {
            match self.recv_message_blocking()? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::ErrorResponse(e) => bail!("end copy: {}", error_message(&e)),
                _ => {}
            }
        }
    }

    /// Blocking recv that returns [`RecvOutcome::Timeout`] when the read timed
    /// out (stream quiet) instead of erroring — the streaming loop branches on
    /// it to tick keepalives and poll signals. WouldBlock / TimedOut from the
    /// SO_RCVTIMEO map to `Timeout`.
    pub(crate) fn recv_message(&mut self) -> Result<RecvOutcome> {
        loop {
            if let Some(msg) = self.take_parsed()? {
                return Ok(RecvOutcome::Message(msg));
            }
            match self.fill() {
                Ok(()) => {}
                Err(e) if is_timeout(&e) => return Ok(RecvOutcome::Timeout),
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Non-blocking recv: return the next message ALREADY buffered in `rx` from
    /// a prior socket read, or `None` if no complete frame is buffered. Does NOT
    /// touch the socket. The receive loop uses this to drain every frame
    /// delivered by one `recv_message` read into a single fdatasync + ack (group
    /// commit) — coalescing adds no latency because it only consumes bytes that
    /// already arrived; a caught-up sole-acker sees one frame and flushes at once.
    pub(crate) fn recv_buffered(&mut self) -> Result<Option<Message>> {
        self.take_parsed()
    }

    /// Override the blocking-read timeout (`SO_RCVTIMEO`). The receive loop sets a
    /// short timeout to bound the batch-accumulation window in 2-acker mode, then
    /// restores the keepalive cadence.
    pub(crate) fn set_read_timeout(&self, dur: Duration) -> Result<()> {
        self.socket
            .set_read_timeout(Some(dur))
            .context("set read timeout")
    }

    /// Blocking recv that treats a read timeout as a retryable wait (used for
    /// the setup queries, where there's no keepalive cadence to honor). Mirrors
    /// the async `recv_message`'s "loop until a message" contract.
    fn recv_message_blocking(&mut self) -> Result<Message> {
        loop {
            if let Some(msg) = self.take_parsed()? {
                return Ok(msg);
            }
            match self.fill() {
                Ok(()) => {}
                Err(e) if is_timeout(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Parse one message out of `rx` if a whole frame is buffered, transparently
    /// swallowing ParameterStatus / NoticeResponse like the async path does. The
    /// SyncReplica path reads no server params (the version-gated seg-size query
    /// runs on the async side connection before this loop starts), so they're
    /// just dropped.
    fn take_parsed(&mut self) -> Result<Option<Message>> {
        loop {
            let Some(msg) = Message::parse(&mut self.rx)? else {
                return Ok(None);
            };
            match &msg {
                Message::ParameterStatus(_) | Message::NoticeResponse(_) => {}
                _ => return Ok(Some(msg)),
            }
        }
    }

    /// One blocking socket read into `rx`. EOF is a closed connection.
    fn fill(&mut self) -> std::io::Result<()> {
        let mut buf = [0u8; 64 * 1024];
        let n = self.socket.read(&mut buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "postgres connection closed unexpectedly",
            ));
        }
        self.rx.extend_from_slice(&buf[..n]);
        Ok(())
    }

    /// Pre-parse `CopyBothResponse` ('W'), which `Message::parse` doesn't
    /// recognize; mirrors `ReplicationConn::expect_copy_both_open`.
    fn expect_copy_both_open(&mut self) -> Result<()> {
        while self.rx.len() < 5 {
            self.fill_retry_timeout()
                .context("read during START_REPLICATION")?;
        }
        if self.rx[0] != b'W' {
            let msg = self.recv_message_blocking()?;
            return match msg {
                Message::CopyOutResponse(_) => Ok(()),
                Message::ErrorResponse(e) => bail!("START_REPLICATION: {}", error_message(&e)),
                other => bail!(
                    "START_REPLICATION: unexpected message {:?}",
                    message_kind(&other)
                ),
            };
        }
        let len = u32::from_be_bytes(self.rx[1..5].try_into().unwrap()) as usize;
        let total = 1 + len;
        while self.rx.len() < total {
            self.fill_retry_timeout()
                .context("read inside CopyBothResponse")?;
        }
        let _ = self.rx.split_to(total);
        Ok(())
    }

    /// `fill` that loops past read-timeouts (for setup phases without keepalive).
    fn fill_retry_timeout(&mut self) -> Result<()> {
        loop {
            match self.fill() {
                Ok(()) => return Ok(()),
                Err(e) if is_timeout(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.tx.is_empty() {
            return Ok(());
        }
        self.socket.write_all(&self.tx)?;
        self.socket.flush()?;
        self.tx.clear();
        Ok(())
    }

    fn do_auth(&mut self, cfg: &PgConfig) -> Result<()> {
        loop {
            match self.recv_message_blocking()? {
                Message::AuthenticationOk => return Ok(()),
                Message::AuthenticationCleartextPassword => {
                    let pw = cfg
                        .password
                        .as_deref()
                        .ok_or_else(|| anyhow!("server requires password but PGPASSWORD unset"))?;
                    frontend::password_message(pw.as_bytes(), &mut self.tx)?;
                    self.flush()?;
                }
                Message::AuthenticationSasl(body) => {
                    self.do_sasl(cfg, body)?;
                    return Ok(());
                }
                Message::AuthenticationMd5Password(_) => {
                    bail!("MD5 password auth not supported (use SCRAM-SHA-256 or trust)");
                }
                Message::ErrorResponse(e) => bail!("auth: {}", error_message(&e)),
                m => bail!("unexpected auth message: {:?}", message_kind(&m)),
            }
        }
    }

    fn do_sasl(
        &mut self,
        cfg: &PgConfig,
        body: postgres_protocol::message::backend::AuthenticationSaslBody,
    ) -> Result<()> {
        use fallible_iterator::FallibleIterator as _;
        let mut found = false;
        let mut mechs = body.mechanisms();
        while let Some(m) = mechs.next()? {
            if m == SCRAM_SHA_256 {
                found = true;
                break;
            }
        }
        if !found {
            bail!("server did not advertise SCRAM-SHA-256 SASL mechanism");
        }
        let pw = cfg
            .password
            .as_deref()
            .ok_or_else(|| anyhow!("server requires SCRAM auth but PGPASSWORD unset"))?;
        let mut scram = ScramSha256::new(pw.as_bytes(), ChannelBinding::unsupported());
        frontend::sasl_initial_response(SCRAM_SHA_256, scram.message(), &mut self.tx)?;
        self.flush()?;

        loop {
            match self.recv_message_blocking()? {
                Message::AuthenticationSaslContinue(c) => {
                    scram.update(c.data())?;
                    frontend::sasl_response(scram.message(), &mut self.tx)?;
                    self.flush()?;
                }
                Message::AuthenticationSaslFinal(f) => {
                    scram.finish(f.data())?;
                }
                Message::AuthenticationOk => return Ok(()),
                Message::ErrorResponse(e) => bail!("scram: {}", error_message(&e)),
                m => bail!("unexpected SCRAM message: {:?}", message_kind(&m)),
            }
        }
    }

    fn await_ready_for_query(&mut self) -> Result<()> {
        loop {
            match self.recv_message_blocking()? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::BackendKeyData(_) => continue,
                Message::ErrorResponse(e) => bail!("startup: {}", error_message(&e)),
                m => bail!("unexpected startup message: {:?}", message_kind(&m)),
            }
        }
    }
}

/// True when an I/O error is a `SO_RCVTIMEO` read timeout. POSIX surfaces it as
/// `WouldBlock` (EAGAIN) or `TimedOut` depending on platform.
fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

/// Decode a `DataRow`'s columns to owned byte vectors, preserving NULLs.
fn data_row_bytes(
    row: &postgres_protocol::message::backend::DataRowBody,
) -> Result<Vec<Option<Vec<u8>>>> {
    use fallible_iterator::FallibleIterator as _;
    let buf = row.buffer_bytes();
    let mut ranges = row.ranges();
    let mut out = Vec::new();
    while let Some(range) = ranges.next()? {
        out.push(range.map(|r| buf[r].to_vec()));
    }
    Ok(out)
}

/// Decode a `DataRow`'s columns to UTF-8 strings, preserving NULLs.
fn data_row_text(
    row: &postgres_protocol::message::backend::DataRowBody,
) -> Result<Vec<Option<String>>> {
    data_row_bytes(row)?
        .into_iter()
        .map(|c| {
            c.map(|b| String::from_utf8(b).context("non-utf8 column"))
                .transpose()
        })
        .collect()
}
