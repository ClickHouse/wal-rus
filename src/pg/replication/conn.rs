//! Replication-mode pg connection: startup, auth, framed message I/O
//!
//! Auth supports trust, cleartext password, and SCRAM-SHA-256. MD5 password
//! is rejected (deprecated; modern PG defaults to SCRAM)
//!
//! Transport: TCP by default; if PGHOST begins with `/` it's interpreted as a
//! libpq-style Unix socket directory & we connect to `<host>/.s.PGSQL.<port>`.
//! TLS negotiation is skipped on Unix sockets to mirror libpq

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};

use crate::config::Vars;

use super::tls::{SocketStream, SslMode, TlsParams, maybe_upgrade};

#[derive(Debug, Clone)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub application_name: String,
    pub sslmode: SslMode,
    pub tls: TlsParams,
}

impl PgConfig {
    pub fn resolve(vars: &Vars) -> Result<Self> {
        let host = vars.get("PGHOST").unwrap_or_else(|| "localhost".into());
        let port: u16 = vars
            .get("PGPORT")
            .unwrap_or_else(|| "5432".into())
            .parse()
            .context("PGPORT")?;
        let user = vars
            .get("PGUSER")
            .or_else(|| vars.get("USER"))
            .ok_or_else(|| anyhow!("PGUSER not set"))?;
        let password = vars.get("PGPASSWORD");
        let database = vars.get("PGDATABASE").unwrap_or_else(|| user.clone());
        let sslmode = match vars.get("PGSSLMODE") {
            None => SslMode::Prefer,
            Some(s) => SslMode::parse(&s)?,
        };
        let tls = TlsParams::resolve(vars);
        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            application_name: "walrus".into(),
            sslmode,
            tls,
        })
    }
}

pub struct ReplicationConn {
    socket: Box<dyn SocketStream>,
    rx: BytesMut,
    tx: BytesMut,
    pub server_version_num: i32,
    pub server_params: Vec<(String, String)>,
    pub tls: bool,
}

impl ReplicationConn {
    #[cfg(test)]
    pub(crate) fn from_test_socket(socket: TcpStream, server_version_num: i32) -> Self {
        Self {
            socket: Box::new(socket),
            rx: BytesMut::with_capacity(64 * 1024),
            tx: BytesMut::with_capacity(8 * 1024),
            server_version_num,
            server_params: vec![("server_version".into(), "16.3".into())],
            tls: false,
        }
    }

    /// Replication-mode connection (`replication=true`); the streaming path
    pub async fn connect(cfg: &PgConfig) -> Result<Self> {
        Self::connect_with(cfg, true).await
    }

    /// Connect with or without the `replication` startup parameter. A normal
    /// (non-replication) connection is needed to read `wal_segment_size` and
    /// `pg_replication_slots`, which physical replication mode forbids — wal-g
    /// opens the same kind of side connection in `getCurrentWalInfo`
    pub async fn connect_with(cfg: &PgConfig, replication: bool) -> Result<Self> {
        let (socket, tls): (Box<dyn SocketStream>, bool) = if cfg.host.starts_with('/') {
            let path = format!("{}/.s.PGSQL.{}", cfg.host.trim_end_matches('/'), cfg.port);
            let sock = UnixStream::connect(&path)
                .await
                .with_context(|| format!("connect to unix:{path}"))?;
            tracing::debug!(path = %path, "replication socket via unix domain");
            (Box::new(sock), false)
        } else {
            let addr = format!("{}:{}", cfg.host, cfg.port);
            let raw = TcpStream::connect(&addr)
                .await
                .with_context(|| format!("connect to {addr}"))?;
            let (sock, used_tls) = maybe_upgrade(raw, &cfg.host, cfg.sslmode, &cfg.tls)
                .await
                .with_context(|| format!("tls negotiation against {addr}"))?;
            if used_tls {
                tracing::debug!(host = %cfg.host, "tls established for replication socket");
            } else if cfg.sslmode != SslMode::Disable {
                tracing::debug!(host = %cfg.host, "replication socket continued unencrypted");
            }
            (sock, used_tls)
        };
        let mut conn = ReplicationConn {
            socket,
            rx: BytesMut::with_capacity(64 * 1024),
            tx: BytesMut::with_capacity(8 * 1024),
            server_version_num: 0,
            server_params: Vec::new(),
            tls,
        };

        let params: [(&str, &str); _] = [
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
        conn.flush().await?;

        conn.do_auth(cfg).await?;
        conn.await_ready_for_query().await?;

        let ver = conn
            .server_param("server_version")
            .ok_or_else(|| anyhow!("server did not send server_version"))?;
        conn.server_version_num = parse_server_version(ver)
            .ok_or_else(|| anyhow!("cannot parse server_version: {ver}"))?;
        Ok(conn)
    }

    pub fn server_param(&self, name: &str) -> Option<&str> {
        self.server_params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| &v[..])
    }

    pub async fn send_query(&mut self, q: &str) -> Result<()> {
        frontend::query(q, &mut self.tx)?;
        self.flush().await
    }

    /// Send a CopyData payload (used to deliver standby status updates
    /// during a START_REPLICATION session)
    pub async fn send_copy_data(&mut self, payload: &[u8]) -> Result<()> {
        let msg = frontend::CopyData::new(payload)
            .map_err(|e| anyhow::anyhow!("copy-data frame: {e}"))?;
        msg.write(&mut self.tx);
        self.flush().await
    }

    /// Run a simple query, collecting every `DataRow` as text columns. For the
    /// small metadata queries wal-receive issues (`wal_segment_size`, slot
    /// info) over a non-replication connection. NULL columns map to `None`
    pub async fn query_rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.send_query(sql).await?;
        let mut rows = Vec::new();
        loop {
            match self.recv_message().await? {
                Message::DataRow(row) => rows.push(data_row_text(&row)?),
                Message::ReadyForQuery(_) => break,
                Message::ErrorResponse(e) => bail!("query `{sql}`: {}", error_message(&e)),
                _ => {}
            }
        }
        Ok(rows)
    }

    /// Collect the current result set's `DataRow`s as text columns, consuming
    /// up to and including the terminating `CommandComplete`. Unlike
    /// [`query_rows`](Self::query_rows) it neither sends a query nor waits for
    /// `ReadyForQuery`, so it suits replication commands like `BASE_BACKUP`
    /// that emit several result sets interleaved with CopyOut streams. NULL
    /// columns map to `None`
    pub async fn collect_command_rows(&mut self) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = Vec::new();
        loop {
            match self.recv_message().await? {
                Message::RowDescription(_) => {}
                Message::DataRow(row) => rows.push(data_row_text(&row)?),
                Message::CommandComplete(_) => break,
                Message::ErrorResponse(e) => bail!("result set: {}", error_message(&e)),
                m => bail!("result set: unexpected message {:?}", message_kind(&m)),
            }
        }
        Ok(rows)
    }

    /// `CREATE_REPLICATION_SLOT <name> PHYSICAL`. A pre-existing slot
    /// (`duplicate_object`, 42710) is treated as success so a restart after a
    /// crash between the existence check and creation is idempotent
    pub async fn create_physical_replication_slot(&mut self, name: &str) -> Result<()> {
        self.send_query(&format!("CREATE_REPLICATION_SLOT {name} PHYSICAL"))
            .await?;
        loop {
            match self.recv_message().await? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::ErrorResponse(e) => {
                    if error_code(&e) == "42710" {
                        // already exists; drain the trailing ReadyForQuery
                        self.await_ready_for_query().await?;
                        return Ok(());
                    }
                    bail!("CREATE_REPLICATION_SLOT {name}: {}", error_message(&e));
                }
                _ => {}
            }
        }
    }

    /// `TIMELINE_HISTORY <tli>` -> (history filename, file contents). Returns
    /// `Ok(None)` when the server has no history file for the timeline
    /// (`undefined_file`, 58P01), mirroring wal-g's `getStartTimeline` guard
    pub async fn timeline_history(&mut self, timeline: u32) -> Result<Option<(String, Vec<u8>)>> {
        self.send_query(&format!("TIMELINE_HISTORY {timeline}"))
            .await?;
        let mut out = None;
        loop {
            match self.recv_message().await? {
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
                        self.await_ready_for_query().await?;
                        return Ok(None);
                    }
                    bail!("TIMELINE_HISTORY {timeline}: {}", error_message(&e));
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// Send a client `CopyDone` ('c') and drain the server's
    /// `CommandComplete`/`ReadyForQuery` so the connection returns to simple-
    /// query mode (used after a server `CopyDone` ends a replication stream)
    pub async fn end_copy(&mut self) -> Result<()> {
        frontend::copy_done(&mut self.tx);
        self.flush().await?;
        loop {
            match self.recv_message().await? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::ErrorResponse(e) => bail!("end copy: {}", error_message(&e)),
                _ => {}
            }
        }
    }

    pub async fn recv_message(&mut self) -> Result<Message> {
        loop {
            if let Some(msg) = Message::parse(&mut self.rx)? {
                if let Message::ParameterStatus(ps) = &msg {
                    let n = ps.name()?.to_string();
                    let v = ps.value()?.to_string();
                    if let Some(slot) = self.server_params.iter_mut().find(|(k, _)| k == &n) {
                        slot.1 = v;
                    } else {
                        self.server_params.push((n, v));
                    }
                    continue;
                }
                if let Message::NoticeResponse(_) = &msg {
                    continue;
                }
                return Ok(msg);
            }
            let n = self.socket.read_buf(&mut self.rx).await?;
            if n == 0 {
                bail!("postgres connection closed unexpectedly");
            }
        }
    }

    /// Pre-parse `CopyBothResponse` ('W') — sent by START_REPLICATION but not
    /// recognized by postgres-protocol's `Message::parse`. Drains the message
    /// from the read buffer & returns Ok; if the next message is anything
    /// else, defers to `recv_message` so callers see a typed error/event
    pub async fn expect_copy_both_open(&mut self) -> Result<()> {
        // Ensure at least 5 bytes (1 tag + 4 length) in rx
        while self.rx.len() < 5 {
            let n = self.socket.read_buf(&mut self.rx).await?;
            if n == 0 {
                bail!("postgres connection closed during START_REPLICATION");
            }
        }
        if self.rx[0] != b'W' {
            // Delegate to the normal parser for non-W tags (ErrorResponse,
            // ParameterStatus, NoticeResponse, etc.)
            let msg = self.recv_message().await?;
            return match msg {
                Message::CopyOutResponse(_) => Ok(()),
                Message::ErrorResponse(e) => bail!("START_REPLICATION: {}", error_message(&e)),
                other => bail!(
                    "START_REPLICATION: unexpected message {:?}",
                    message_kind(&other)
                ),
            };
        }
        // 'W' CopyBothResponse: tag(1) + len(4) + format(1) + ncols(2) + ncols*i16
        let len = u32::from_be_bytes(self.rx[1..5].try_into().unwrap()) as usize;
        let total = 1 + len;
        while self.rx.len() < total {
            let n = self.socket.read_buf(&mut self.rx).await?;
            if n == 0 {
                bail!("postgres connection closed inside CopyBothResponse");
            }
        }
        let _ = self.rx.split_to(total);
        Ok(())
    }

    pub fn server_pg_version(&self) -> i32 {
        self.server_version_num
    }

    async fn flush(&mut self) -> Result<()> {
        if self.tx.is_empty() {
            return Ok(());
        }
        self.socket.write_all(&self.tx).await?;
        self.tx.clear();
        Ok(())
    }

    async fn do_auth(&mut self, cfg: &PgConfig) -> Result<()> {
        loop {
            match self.recv_message().await? {
                Message::AuthenticationOk => return Ok(()),
                Message::AuthenticationCleartextPassword => {
                    let pw = cfg
                        .password
                        .as_deref()
                        .ok_or_else(|| anyhow!("server requires password but PGPASSWORD unset"))?;
                    frontend::password_message(pw.as_bytes(), &mut self.tx)?;
                    self.flush().await?;
                }
                Message::AuthenticationSasl(body) => {
                    self.do_sasl(cfg, body).await?;
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

    async fn do_sasl(
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
        self.flush().await?;

        loop {
            match self.recv_message().await? {
                Message::AuthenticationSaslContinue(c) => {
                    scram.update(c.data())?;
                    frontend::sasl_response(scram.message(), &mut self.tx)?;
                    self.flush().await?;
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

    async fn await_ready_for_query(&mut self) -> Result<()> {
        loop {
            match self.recv_message().await? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::BackendKeyData(_) => continue,
                Message::ErrorResponse(e) => bail!("startup: {}", error_message(&e)),
                m => bail!("unexpected startup message: {:?}", message_kind(&m)),
            }
        }
    }
}

pub fn error_message(body: &postgres_protocol::message::backend::ErrorResponseBody) -> String {
    use fallible_iterator::FallibleIterator as _;
    let mut fields = body.fields();
    let mut sev = String::new();
    let mut code = String::new();
    let mut msg = String::new();
    while let Ok(Some(f)) = fields.next() {
        let v = String::from_utf8_lossy(f.value_bytes()).into_owned();
        match f.type_() as char {
            'S' | 'V' => sev = v,
            'C' => code = v,
            'M' => msg = v,
            _ => {}
        }
    }
    format!("{sev} {code}: {msg}")
}

/// SQLSTATE ('C' field) of an error response, empty when absent
pub fn error_code(body: &postgres_protocol::message::backend::ErrorResponseBody) -> String {
    use fallible_iterator::FallibleIterator as _;
    let mut fields = body.fields();
    while let Ok(Some(f)) = fields.next() {
        if f.type_() == b'C' {
            return String::from_utf8_lossy(f.value_bytes()).into_owned();
        }
    }
    String::new()
}

/// Decode a `DataRow`'s columns to owned byte vectors, preserving NULLs
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

/// Decode a `DataRow`'s columns to UTF-8 strings, preserving NULLs
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

pub fn message_kind(m: &Message) -> &'static str {
    match m {
        Message::AuthenticationOk => "AuthenticationOk",
        Message::AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
        Message::AuthenticationMd5Password(_) => "AuthenticationMd5Password",
        Message::AuthenticationSasl(_) => "AuthenticationSasl",
        Message::AuthenticationSaslContinue(_) => "AuthenticationSaslContinue",
        Message::AuthenticationSaslFinal(_) => "AuthenticationSaslFinal",
        Message::BackendKeyData(_) => "BackendKeyData",
        Message::CommandComplete(_) => "CommandComplete",
        Message::CopyData(_) => "CopyData",
        Message::CopyDone => "CopyDone",
        Message::CopyInResponse(_) => "CopyInResponse",
        Message::CopyOutResponse(_) => "CopyOutResponse",
        Message::DataRow(_) => "DataRow",
        Message::ErrorResponse(_) => "ErrorResponse",
        Message::NoticeResponse(_) => "NoticeResponse",
        Message::ParameterStatus(_) => "ParameterStatus",
        Message::ReadyForQuery(_) => "ReadyForQuery",
        Message::RowDescription(_) => "RowDescription",
        _ => "Other",
    }
}

fn parse_server_version(s: &str) -> Option<i32> {
    // accept "16.3", "17beta1", "9.6.24"
    let mut parts = s.split(|c: char| !c.is_ascii_digit());
    let major: i32 = parts.next()?.parse().ok()?;
    if major >= 10 {
        let minor: i32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        Some(major * 10000 + minor)
    } else {
        let minor: i32 = parts.next()?.parse().ok()?;
        let patch: i32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        Some(major * 10000 + minor * 100 + patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BufMut;

    /// Frame `tag` + length + `payload` and run it through the backend parser
    fn framed(tag: u8, payload: &[u8]) -> Message {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_i32((payload.len() + 4) as i32);
        buf.extend_from_slice(payload);
        Message::parse(&mut buf).unwrap().unwrap()
    }

    /// Build an ErrorResponse/NoticeResponse field block: each (type, value)
    /// then the trailing NUL terminator
    fn error_fields(fields: &[(u8, &str)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (t, v) in fields {
            out.push(*t);
            out.extend_from_slice(v.as_bytes());
            out.push(0);
        }
        out.push(0);
        out
    }

    fn error_body(fields: &[(u8, &str)]) -> postgres_protocol::message::backend::ErrorResponseBody {
        match framed(b'E', &error_fields(fields)) {
            Message::ErrorResponse(b) => b,
            other => panic!("expected ErrorResponse, got {}", message_kind(&other)),
        }
    }

    #[test]
    fn parses_server_version() {
        assert_eq!(parse_server_version("16.3"), Some(160003));
        assert_eq!(parse_server_version("18"), Some(180000));
        assert_eq!(parse_server_version("17beta1"), Some(170000));
        assert_eq!(parse_server_version("9.6.24"), Some(90624));
    }

    #[test]
    fn message_kind_labels_each_tag() {
        assert_eq!(
            message_kind(&framed(b'R', &[0, 0, 0, 0])),
            "AuthenticationOk"
        );
        assert_eq!(
            message_kind(&framed(b'K', &[0, 0, 0, 1, 0, 0, 0, 2])),
            "BackendKeyData"
        );
        assert_eq!(
            message_kind(&framed(b'C', b"SELECT 1\0")),
            "CommandComplete"
        );
        assert_eq!(message_kind(&framed(b'c', &[])), "CopyDone");
        assert_eq!(message_kind(&framed(b'Z', b"I")), "ReadyForQuery");
        assert_eq!(
            message_kind(&framed(b'N', &error_fields(&[(b'M', "n")]))),
            "NoticeResponse"
        );
        assert_eq!(message_kind(&framed(b'D', &[0, 0])), "DataRow");
        assert_eq!(message_kind(&framed(b'T', &[0, 0])), "RowDescription");
        assert_eq!(
            message_kind(&framed(b'S', b"server_version\x0016.3\0")),
            "ParameterStatus"
        );
        assert_eq!(
            message_kind(&Message::ErrorResponse(error_body(&[(b'C', "X")]))),
            "ErrorResponse"
        );
    }

    #[test]
    fn error_message_and_code_extract_fields() {
        let body = error_body(&[
            (b'S', "ERROR"),
            (b'C', "42710"),
            (b'M', "duplicate object"),
            (b'D', "ignored detail"),
        ]);
        assert_eq!(error_message(&body), "ERROR 42710: duplicate object");
        assert_eq!(error_code(&body), "42710");

        // 'V' (localized severity) also populates the severity slot
        let v_only = error_body(&[(b'V', "FATAL"), (b'M', "boom")]);
        assert_eq!(error_message(&v_only), "FATAL : boom");

        // missing fields -> empty pieces, no panic; absent code is ""
        let bare = error_body(&[(b'M', "lonely")]);
        assert_eq!(error_message(&bare), " : lonely");
        assert_eq!(error_code(&bare), "");
    }

    #[tokio::test]
    async fn unix_socket_host_dispatches_to_unix_transport() {
        // PGHOST starting with `/` routes through UnixStream::connect against
        // <host>/.s.PGSQL.<port>. With a bogus directory, connect must fail
        // with the unix-prefixed context (proves dispatch, no TCP attempt)
        let cfg = PgConfig {
            host: "/nonexistent/walrus-unix-test".into(),
            port: 5432,
            user: "u".into(),
            password: None,
            database: "u".into(),
            application_name: "walrus-test".into(),
            sslmode: SslMode::Prefer,
            tls: TlsParams::default(),
        };
        let err = ReplicationConn::connect(&cfg).await.err().unwrap();
        let s = format!("{err:#}");
        assert!(
            s.contains("unix:"),
            "expected unix-socket context, got: {s}"
        );
    }

    // ── Auth handshakes against a scripted backend peer ───────────────────────
    //
    // `from_test_socket` skips startup (already sent by `connect_with`), so
    // `do_auth` is driven directly: the peer emits the Authentication* messages
    // a real PG backend would, and we assert the client's reply + outcome.

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// `'R'` Authentication message: u32 subtype `code` + `extra` payload
    fn auth_msg(code: i32, extra: &[u8]) -> Vec<u8> {
        let mut v = vec![b'R'];
        v.extend_from_slice(&((8 + extra.len()) as u32).to_be_bytes());
        v.extend_from_slice(&code.to_be_bytes());
        v.extend_from_slice(extra);
        v
    }
    fn auth_ok() -> Vec<u8> {
        auth_msg(0, &[])
    }
    fn auth_sasl(mechs: &[&str]) -> Vec<u8> {
        let mut extra = Vec::new();
        for m in mechs {
            extra.extend_from_slice(m.as_bytes());
            extra.push(0);
        }
        extra.push(0); // terminating empty mechanism
        auth_msg(10, &extra)
    }

    /// Read one typed backend-bound message (tag + length-prefixed body)
    async fn read_typed(sock: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes(hdr[1..5].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        (hdr[0], body)
    }

    fn test_cfg(password: Option<&str>) -> PgConfig {
        PgConfig {
            host: "127.0.0.1".into(),
            port: 0,
            user: "u".into(),
            password: password.map(str::to_string),
            database: "u".into(),
            application_name: "walrus-auth-test".into(),
            sslmode: SslMode::Disable,
            tls: TlsParams::default(),
        }
    }

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn cleartext_password_auth_succeeds() {
        let (client, mut server) = connected_pair().await;
        let peer = tokio::spawn(async move {
            server.write_all(&auth_msg(3, &[])).await.unwrap(); // AuthenticationCleartextPassword
            let (tag, body) = read_typed(&mut server).await;
            assert_eq!(tag, b'p');
            assert_eq!(&body[..body.len() - 1], b"hunter2");
            assert_eq!(*body.last().unwrap(), 0, "password is NUL-terminated");
            server.write_all(&auth_ok()).await.unwrap();
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        conn.do_auth(&test_cfg(Some("hunter2")))
            .await
            .expect("auth");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn cleartext_password_missing_bails() {
        let (client, mut server) = connected_pair().await;
        // Trigger message buffers in the socket; the client reads it before the
        // dropped `server` half delivers EOF, so no drain read is needed.
        let peer = tokio::spawn(async move {
            server.write_all(&auth_msg(3, &[])).await.unwrap();
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        let err = conn.do_auth(&test_cfg(None)).await.unwrap_err();
        assert!(format!("{err:#}").contains("password"), "{err:#}");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn md5_password_rejected() {
        let (client, mut server) = connected_pair().await;
        let peer = tokio::spawn(async move {
            server.write_all(&auth_msg(5, &[1, 2, 3, 4])).await.unwrap(); // AuthenticationMD5Password
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        let err = conn.do_auth(&test_cfg(Some("pw"))).await.unwrap_err();
        assert!(format!("{err:#}").contains("MD5"), "{err:#}");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn sasl_without_scram_mechanism_bails() {
        let (client, mut server) = connected_pair().await;
        let peer = tokio::spawn(async move {
            server
                .write_all(&auth_sasl(&["SCRAM-SHA-256-PLUS"]))
                .await
                .unwrap();
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        let err = conn.do_auth(&test_cfg(Some("pw"))).await.unwrap_err();
        assert!(format!("{err:#}").contains("SCRAM-SHA-256"), "{err:#}");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn scram_without_password_bails() {
        let (client, mut server) = connected_pair().await;
        let peer = tokio::spawn(async move {
            server
                .write_all(&auth_sasl(&["SCRAM-SHA-256"]))
                .await
                .unwrap();
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        let err = conn.do_auth(&test_cfg(None)).await.unwrap_err();
        assert!(format!("{err:#}").contains("PGPASSWORD"), "{err:#}");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn scram_sha256_full_handshake_succeeds() {
        let (client, server) = connected_pair().await;
        let peer = tokio::spawn(scram_backend(server, "s3cr3t".to_string()));
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        conn.do_auth(&test_cfg(Some("s3cr3t")))
            .await
            .expect("SCRAM auth");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn scram_sha256_wrong_password_fails() {
        let (client, server) = connected_pair().await;
        // Backend salts/signs against "right"; client offers "wrong" — the
        // client's own ServerSignature check (scram.finish) must reject.
        let peer = tokio::spawn(scram_backend(server, "right".to_string()));
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        conn.do_auth(&test_cfg(Some("wrong")))
            .await
            .expect_err("wrong password must fail the server-signature check");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn expect_copy_both_open_accepts_copy_out_response() {
        let (client, mut server) = connected_pair().await;
        let peer = tokio::spawn(async move {
            // CopyOutResponse 'H': format(0) + ncols(0)
            server
                .write_all(&[b'H', 0, 0, 0, 7, 0, 0, 0])
                .await
                .unwrap();
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        conn.expect_copy_both_open()
            .await
            .expect("CopyOutResponse accepted");
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn expect_copy_both_open_surfaces_error_response() {
        let (client, mut server) = connected_pair().await;
        let peer = tokio::spawn(async move {
            let body = error_fields(&[(b'S', "ERROR"), (b'C', "55000"), (b'M', "no slot")]);
            let mut msg = vec![b'E'];
            msg.extend_from_slice(&((4 + body.len()) as u32).to_be_bytes());
            msg.extend_from_slice(&body);
            server.write_all(&msg).await.unwrap();
        });
        let mut conn = ReplicationConn::from_test_socket(client, 160003);
        let err = conn.expect_copy_both_open().await.unwrap_err();
        assert!(format!("{err:#}").contains("no slot"), "{err:#}");
        peer.await.unwrap();
    }

    // ── Minimal SCRAM-SHA-256 server (RFC 5802) for the auth peer ─────────────

    const SCRAM_ITERS: u32 = 4096;
    const SCRAM_SALT: &[u8] = b"0123456789abcdef";

    fn b64_encode(b: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(b)
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        let k = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, key);
        aws_lc_rs::hmac::sign(&k, data).as_ref().to_vec()
    }

    fn salted_password(password: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        aws_lc_rs::pbkdf2::derive(
            aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256,
            std::num::NonZeroU32::new(SCRAM_ITERS).unwrap(),
            SCRAM_SALT,
            password.as_bytes(),
            &mut out,
        );
        out
    }

    /// client-first-message-bare = everything after the gs2 header (2nd comma)
    fn client_first_bare(client_first: &str) -> &str {
        let second = client_first
            .match_indices(',')
            .nth(1)
            .expect("gs2 header has two commas")
            .0;
        &client_first[second + 1..]
    }

    /// Drive the server side of a SCRAM-SHA-256 exchange against `password`,
    /// emitting SASLContinue (server-first), SASLFinal (server-signature), and
    /// AuthenticationOk. The client verifies the server signature itself, so a
    /// mismatched client password surfaces as a client-side error.
    async fn scram_backend(mut sock: TcpStream, password: String) {
        sock.write_all(&auth_sasl(&["SCRAM-SHA-256"]))
            .await
            .unwrap();

        // SASLInitialResponse: mechanism NUL + i32 len + client-first-message
        let (tag, body) = read_typed(&mut sock).await;
        assert_eq!(tag, b'p');
        let nul = body.iter().position(|&b| b == 0).unwrap();
        assert_eq!(&body[..nul], b"SCRAM-SHA-256");
        let client_first = String::from_utf8(body[nul + 5..].to_vec()).unwrap();
        let cf_bare = client_first_bare(&client_first).to_string();
        let cnonce = cf_bare
            .split(',')
            .find_map(|t| t.strip_prefix("r="))
            .unwrap()
            .to_string();

        let combined = format!("{cnonce}serverNONCE");
        let server_first = format!("r={combined},s={},i={SCRAM_ITERS}", b64_encode(SCRAM_SALT));
        sock.write_all(&auth_msg(11, server_first.as_bytes()))
            .await
            .unwrap();

        // SASLResponse: client-final-message (c=...,r=...,p=<proof>)
        let (tag, body) = read_typed(&mut sock).await;
        assert_eq!(tag, b'p');
        let client_final = String::from_utf8(body).unwrap();
        let proof_at = client_final.rfind(",p=").unwrap();
        let client_final_no_proof = &client_final[..proof_at];

        let auth_message = format!("{cf_bare},{server_first},{client_final_no_proof}");
        let salted = salted_password(&password);
        let server_key = hmac_sha256(&salted, b"Server Key");
        let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
        let server_final = format!("v={}", b64_encode(&server_sig));
        sock.write_all(&auth_msg(12, server_final.as_bytes()))
            .await
            .unwrap();
        sock.write_all(&auth_ok()).await.unwrap();
    }
}
