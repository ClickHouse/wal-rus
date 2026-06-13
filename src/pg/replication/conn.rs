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

use super::tls::{SocketStream, SslMode, maybe_upgrade};

#[derive(Debug, Clone)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub application_name: String,
    pub sslmode: SslMode,
}

impl PgConfig {
    pub fn from_env() -> Result<Self> {
        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".into());
        let port: u16 = std::env::var("PGPORT")
            .unwrap_or_else(|_| "5432".into())
            .parse()
            .context("PGPORT")?;
        let user = std::env::var("PGUSER")
            .or_else(|_| std::env::var("USER"))
            .map_err(|_| anyhow!("PGUSER not set"))?;
        let password = std::env::var("PGPASSWORD").ok();
        let database = std::env::var("PGDATABASE").unwrap_or_else(|_| user.clone());
        let sslmode = SslMode::from_env()?;
        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            application_name: "walross".into(),
            sslmode,
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

    pub async fn connect(cfg: &PgConfig) -> Result<Self> {
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
            let (sock, used_tls) = maybe_upgrade(raw, &cfg.host, cfg.sslmode)
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

        let params: &[(&str, &str)] = &[
            ("user", cfg.user.as_str()),
            ("database", cfg.database.as_str()),
            ("application_name", cfg.application_name.as_str()),
            ("replication", "true"),
            ("client_encoding", "UTF8"),
        ];
        frontend::startup_message(params.iter().copied(), &mut conn.tx)?;
        conn.flush().await?;

        conn.do_auth(cfg).await?;
        conn.await_ready_for_query().await?;

        let ver = conn
            .server_param("server_version")
            .ok_or_else(|| anyhow!("server did not send server_version"))?;
        conn.server_version_num = parse_server_version(&ver)
            .ok_or_else(|| anyhow!("cannot parse server_version: {ver}"))?;
        Ok(conn)
    }

    pub fn server_param(&self, name: &str) -> Option<String> {
        self.server_params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
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

    #[test]
    fn parses_server_version() {
        assert_eq!(parse_server_version("16.3"), Some(160003));
        assert_eq!(parse_server_version("18"), Some(180000));
        assert_eq!(parse_server_version("17beta1"), Some(170000));
        assert_eq!(parse_server_version("9.6.24"), Some(90624));
    }

    #[tokio::test]
    async fn unix_socket_host_dispatches_to_unix_transport() {
        // PGHOST starting with `/` routes through UnixStream::connect against
        // <host>/.s.PGSQL.<port>. With a bogus directory, connect must fail
        // with the unix-prefixed context (proves dispatch, no TCP attempt)
        let cfg = PgConfig {
            host: "/nonexistent/walross-unix-test".into(),
            port: 5432,
            user: "u".into(),
            password: None,
            database: "u".into(),
            application_name: "walross-test".into(),
            sslmode: SslMode::Prefer,
        };
        let err = ReplicationConn::connect(&cfg).await.err().unwrap();
        let s = format!("{err:#}");
        assert!(
            s.contains("unix:"),
            "expected unix-socket context, got: {s}"
        );
    }
}
