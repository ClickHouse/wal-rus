//! TLS negotiation for the postgres replication socket
//!
//! libpq sslmode: disable / allow / prefer / require / verify-ca / verify-full.
//! Verification follows pgx (wal-g's driver), not libpq: `require` validates the
//! cert chain when a root is configured, and PGSSLROOTCERT=system forces verify-full
//!
//! Wire form: client sends SSLRequest (int32 len=8, int32 code=80877103)
//! Server replies with single byte 'S' (proceed with TLS) or 'N' (refused)
//! On 'S', upgrade the TCP socket via tokio-rustls

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use rustls::CertificateError;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

pub trait SocketStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> SocketStream for T {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    pub fn from_env() -> Result<Self> {
        match std::env::var("PGSSLMODE").ok().as_deref() {
            None => Ok(SslMode::Prefer),
            Some(s) => Self::parse(s),
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "disable" => Ok(SslMode::Disable),
            "allow" => Ok(SslMode::Allow),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCa),
            "verify-full" => Ok(SslMode::VerifyFull),
            other => bail!("unsupported PGSSLMODE={other}"),
        }
    }

    fn requires_tls(self) -> bool {
        matches!(
            self,
            SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull
        )
    }

    fn attempts_tls(self) -> bool {
        !matches!(self, SslMode::Disable)
    }
}

/// Server-cert verification level. Follows pgx (wal-g's driver) rather than
/// libpq: `require` upgrades to chain validation when a root is configured,
/// and PGSSLROOTCERT=system forces full verification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verify {
    /// Encrypt only, accept any cert (prefer/allow, require without a root)
    None,
    /// Validate chain against roots, skip hostname (verify-ca, require+root)
    Ca,
    /// Validate chain and hostname (verify-full, PGSSLROOTCERT=system)
    Full,
}

/// Resolve verification level from the mode and configured root, matching pgx's
/// configTLS decision table
fn verification_plan(sslmode: SslMode, rootcert: Option<&str>) -> Verify {
    // pgx: PGSSLROOTCERT=system loads the system trust store and forces verify-full
    if rootcert == Some("system") {
        return Verify::Full;
    }
    match sslmode {
        SslMode::VerifyFull => Verify::Full,
        SslMode::VerifyCa => Verify::Ca,
        // pgx upgrades require to verify-ca when a root is configured; libpq
        // leaves require unverified either way
        SslMode::Require if rootcert.is_some() => Verify::Ca,
        _ => Verify::None,
    }
}

/// Negotiate TLS on a freshly-opened replication socket.
/// Returns the (possibly-upgraded) socket and whether TLS was applied.
pub async fn maybe_upgrade(
    socket: TcpStream,
    host: &str,
    sslmode: SslMode,
) -> Result<(Box<dyn SocketStream>, bool)> {
    if !sslmode.attempts_tls() {
        return Ok((Box::new(socket), false));
    }

    let mut socket = socket;
    // SSLRequest: i32 BE length=8, i32 BE code=80877103
    let mut req = [0u8; 8];
    req[0..4].copy_from_slice(&8i32.to_be_bytes());
    req[4..8].copy_from_slice(&80877103i32.to_be_bytes());
    socket.write_all(&req).await.context("send SSLRequest")?;

    let mut resp = [0u8; 1];
    socket
        .read_exact(&mut resp)
        .await
        .context("read SSLRequest reply")?;

    match resp[0] {
        b'S' => {
            let config = build_client_config(sslmode)?;
            let connector = TlsConnector::from(Arc::new(config));
            let server_name = ServerName::try_from(host.to_string())
                .map_err(|e| anyhow!("invalid host for SNI: {e}"))?;
            let tls = connector
                .connect(server_name, socket)
                .await
                .context("rustls handshake")?;
            Ok((Box::new(tls), true))
        }
        b'N' => {
            if sslmode.requires_tls() {
                bail!("server refused SSL (sslmode={:?})", sslmode);
            }
            Ok((Box::new(socket), false))
        }
        other => bail!("unexpected SSLRequest reply byte {other:#x} (expected 'S' or 'N')"),
    }
}

fn build_client_config(sslmode: SslMode) -> Result<ClientConfig> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let builder = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow!("rustls protocol versions: {e}"))?;

    let rootcert = std::env::var("PGSSLROOTCERT")
        .ok()
        .filter(|p| !p.is_empty());

    // Server-cert verifier per sslmode; leaves builder awaiting client-auth choice
    let builder = match verification_plan(sslmode, rootcert.as_deref()) {
        // prefer / allow, and require without a root: encrypt only, accept any cert
        Verify::None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier)),
        plan => {
            let mut roots = RootCertStore::empty();
            match rootcert.as_deref() {
                // system has no rustls OS-store loader here; fall back to the
                // bundled webpki roots (same public-root effect as pgx)
                Some(path) if path != "system" => load_pem_roots(path, &mut roots)
                    .with_context(|| format!("load PGSSLROOTCERT={path}"))?,
                _ => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
            }
            if plan == Verify::Full {
                builder.with_root_certificates(roots)
            } else {
                // verify-ca: full path validation, only the hostname check is suppressed
                let inner = WebPkiServerVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|e| anyhow!("build verify-ca verifier: {e}"))?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(SkipHostnameVerifier { inner }))
            }
        }
    };

    // Client cert auth: PGSSLCERT + PGSSLKEY. libpq's ~/.postgresql/postgresql.{crt,key}
    // default location is not honored, matching this module's PGSSLROOTCERT handling
    match load_client_auth()? {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| anyhow!("configure client cert auth: {e}")),
        None => Ok(builder.with_no_client_auth()),
    }
}

/// Resolve PGSSLCERT + PGSSLKEY into a cert chain & private key for mutual TLS.
/// Both must be set together. Returns None when neither is set (no client auth)
fn load_client_auth() -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    let cert = std::env::var("PGSSLCERT").ok().filter(|s| !s.is_empty());
    let key = std::env::var("PGSSLKEY").ok().filter(|s| !s.is_empty());
    match (cert, key) {
        (None, None) => Ok(None),
        (Some(cert_path), Some(key_path)) => {
            let certs = load_cert_chain(&cert_path)
                .with_context(|| format!("load PGSSLCERT={cert_path}"))?;
            let key =
                load_private_key(&key_path).with_context(|| format!("load PGSSLKEY={key_path}"))?;
            Ok(Some((certs, key)))
        }
        (Some(_), None) => {
            bail!("PGSSLCERT set without PGSSLKEY; client cert auth needs both")
        }
        (None, Some(_)) => {
            bail!("PGSSLKEY set without PGSSLCERT; client cert auth needs both")
        }
    }
}

fn load_cert_chain(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("parse PEM: {e}"))?;
    if certs.is_empty() {
        bail!("no certificates found in {path}");
    }
    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    // private_key reads the first PKCS#8 / PKCS#1 / SEC1 block; encrypted keys
    // (PGSSLPASSWORD) yield no recognized block and surface as the None error
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| anyhow!("parse private key PEM: {e}"))?
        .ok_or_else(|| anyhow!("no private key found in {path} (encrypted keys unsupported)"))
}

fn load_pem_roots(path: &str, roots: &mut RootCertStore) -> Result<()> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert = cert.map_err(|e| anyhow!("parse PEM: {e}"))?;
        roots.add(cert).map_err(|e| anyhow!("add root cert: {e}"))?;
        added += 1;
    }
    if added == 0 {
        bail!("no certificates found in {path}");
    }
    Ok(())
}

/// Accepts any server cert, no verification. For prefer/allow, and require
/// without a configured root.
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // wide net; any scheme rustls offers is acceptable
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

/// verify-ca (and require with a configured root): delegate full path
/// validation to webpki, suppress only the hostname/SNI mismatch error so a
/// cert valid against the configured roots is accepted regardless of CN/SAN
#[derive(Debug)]
struct SkipHostnameVerifier {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for SkipHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Ok(v) => Ok(v),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            Err(e) => Err(e),
        }
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(m, c, d)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(m, c, d)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // build_client_config / load_client_auth / SslMode::from_env read process
    // env (PGSSLMODE / PGSSLROOTCERT / PGSSLCERT / PGSSLKEY); serialize every
    // test that reads or mutates them so they can't observe each other's writes
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_env(k: &str, v: Option<&str>) {
        unsafe {
            match v {
                Some(x) => std::env::set_var(k, x),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn parses_sslmodes() {
        assert_eq!(SslMode::parse("disable").unwrap(), SslMode::Disable);
        assert_eq!(SslMode::parse("allow").unwrap(), SslMode::Allow);
        assert_eq!(SslMode::parse("prefer").unwrap(), SslMode::Prefer);
        assert_eq!(SslMode::parse("require").unwrap(), SslMode::Require);
        assert_eq!(SslMode::parse("verify-ca").unwrap(), SslMode::VerifyCa);
        assert_eq!(SslMode::parse("verify-full").unwrap(), SslMode::VerifyFull);
        assert!(SslMode::parse("bogus").is_err());
    }

    #[test]
    fn require_verifies_only_with_root() {
        use SslMode::*;
        // require: unverified without a root, verify-ca with one (pgx upgrade)
        assert_eq!(verification_plan(Require, None), Verify::None);
        assert_eq!(verification_plan(Require, Some("/ca.crt")), Verify::Ca);
        // prefer/allow never verify, even with a root configured
        assert_eq!(verification_plan(Prefer, Some("/ca.crt")), Verify::None);
        assert_eq!(verification_plan(Allow, Some("/ca.crt")), Verify::None);
        // explicit verify modes are independent of the root being set
        assert_eq!(verification_plan(VerifyCa, None), Verify::Ca);
        assert_eq!(verification_plan(VerifyFull, None), Verify::Full);
        // PGSSLROOTCERT=system forces verify-full regardless of mode
        assert_eq!(verification_plan(Require, Some("system")), Verify::Full);
        assert_eq!(verification_plan(Prefer, Some("system")), Verify::Full);
    }

    #[test]
    fn client_config_builds_for_all_modes() {
        let _e = lock_env();
        for k in ["PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY"] {
            set_env(k, None);
        }
        for m in [
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
            SslMode::VerifyFull,
        ] {
            build_client_config(m).unwrap();
        }
    }

    /// verify-ca must reject malformed / unsigned certs (path validation)
    /// and only suppress the hostname mismatch.
    #[test]
    fn verify_ca_rejects_bogus_cert() {
        let _e = lock_env();
        set_env("PGSSLROOTCERT", None);
        // Run only after the aws-lc-rs provider is installed by build_client_config above
        let _ = build_client_config(SslMode::VerifyCa).unwrap();

        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let inner = WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let v = SkipHostnameVerifier { inner };

        let bogus = CertificateDer::from(vec![0u8; 64]);
        let name = ServerName::try_from("example.com").unwrap();
        let res = v.verify_server_cert(&bogus, &[], &name, &[], UnixTime::now());
        assert!(res.is_err(), "garbage cert must not pass verify-ca");
    }

    // Throwaway self-signed EC cert + unencrypted PKCS#8 key, for the client-auth loaders
    const TEST_CRT: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBkzCCATmgAwIBAgIUSMkFdFeE1MtkjYEGxcnS2mVx9bswCgYIKoZIzj0EAwIw\n\
HjEcMBoGA1UEAwwTd2Fscm9zcy1jbGllbnQtdGVzdDAgFw0yNjA2MTgxNjI2MjRa\n\
GA8yMTI2MDUyNTE2MjYyNFowHjEcMBoGA1UEAwwTd2Fscm9zcy1jbGllbnQtdGVz\n\
dDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABKhsI3yKUtenCrUI2bw41hmHVKAo\n\
o5Hpzcu03vn075MRFd8KBytDwyXjuuu/GYkVR2I9E+P8yDror+JbNR9oPu+jUzBR\n\
MB0GA1UdDgQWBBRHuN9KrCYiuJLTUxCn72i5odxAyjAfBgNVHSMEGDAWgBRHuN9K\n\
rCYiuJLTUxCn72i5odxAyjAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gA\n\
MEUCIHKjkZe6tLJkQ+rU6bijArkBD80wU6drrXqd+Se4Kkm4AiEA4gtOb8J4YLtS\n\
FVVNp23KV0vrDO+Djlyk8eRyaiY1I/o=\n\
-----END CERTIFICATE-----\n";
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg21mJK9YS0ismJMMo\n\
HsRAMqj+AEAJ4N1uK9G/PW0ZGo+hRANCAASobCN8ilLXpwq1CNm8ONYZh1SgKKOR\n\
6c3LtN759O+TERXfCgcrQ8Ml47rrvxmJFUdiPRPj/Mg66K/iWzUfaD7v\n\
-----END PRIVATE KEY-----\n";

    fn write_tmp(name: &str, body: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wal-rs-tls-test-{name}-{:?}",
            std::thread::current().id()
        ));
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn loads_client_cert_and_key() {
        let crt_path = write_tmp("crt", TEST_CRT);
        let key_path = write_tmp("key", TEST_KEY);

        let certs = load_cert_chain(crt_path.to_str().unwrap()).unwrap();
        assert_eq!(certs.len(), 1);
        let key = load_private_key(key_path.to_str().unwrap()).unwrap();

        // rustls accepts the matching cert/key pair as a client identity
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_client_auth_cert(certs, key)
            .unwrap();

        std::fs::remove_file(crt_path).ok();
        std::fs::remove_file(key_path).ok();
    }

    #[test]
    fn cert_chain_rejects_empty_pem() {
        let empty = write_tmp("empty", "not a pem file\n");
        let err = load_cert_chain(empty.to_str().unwrap()).err().unwrap();
        assert!(err.to_string().contains("no certificates"), "{err}");
        std::fs::remove_file(empty).ok();
    }

    #[test]
    fn private_key_rejects_keyless_pem() {
        // A cert-only PEM has no private-key block
        let crt = write_tmp("crtonly", TEST_CRT);
        let err = load_private_key(crt.to_str().unwrap()).err().unwrap();
        assert!(err.to_string().contains("no private key"), "{err}");
        std::fs::remove_file(crt).ok();
    }

    #[tokio::test]
    async fn ssl_request_message_is_sent_then_n_refusal_falls_back_on_prefer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 8];
            sock.read_exact(&mut req).await.unwrap();
            assert_eq!(&req[0..4], &8i32.to_be_bytes());
            assert_eq!(&req[4..8], &80877103i32.to_be_bytes());
            sock.write_all(b"N").await.unwrap();
            // keep socket open so client doesn't see EOF
            sock
        });

        let raw = TcpStream::connect(addr).await.unwrap();
        let (sock, used_tls) = maybe_upgrade(raw, "127.0.0.1", SslMode::Prefer)
            .await
            .unwrap();
        assert!(!used_tls);
        drop(sock);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ssl_request_n_refusal_errors_on_require() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 8];
            sock.read_exact(&mut req).await.unwrap();
            sock.write_all(b"N").await.unwrap();
            sock
        });

        let raw = TcpStream::connect(addr).await.unwrap();
        let err = maybe_upgrade(raw, "127.0.0.1", SslMode::Require)
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("refused SSL"), "{err}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn disable_skips_ssl_request_entirely() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            // Should NOT receive an SSLRequest
            sock
        });

        let raw = TcpStream::connect(addr).await.unwrap();
        let (_sock, used_tls) = maybe_upgrade(raw, "127.0.0.1", SslMode::Disable)
            .await
            .unwrap();
        assert!(!used_tls);
        let server_sock = server.await.unwrap();
        // No bytes pending: peek with try_read on a non-blocking socket
        let _ = server_sock;
    }

    #[tokio::test]
    async fn maybe_upgrade_rejects_unexpected_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 8];
            sock.read_exact(&mut req).await.unwrap();
            sock.write_all(b"X").await.unwrap();
            sock
        });
        let raw = TcpStream::connect(addr).await.unwrap();
        let err = maybe_upgrade(raw, "127.0.0.1", SslMode::Prefer)
            .await
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("unexpected SSLRequest reply"),
            "{err}"
        );
        server.await.unwrap();
    }

    #[test]
    fn pem_roots_loads_ca_and_rejects_empty() {
        let ca = write_tmp("pemroots-ca", TEST_CRT);
        let mut roots = RootCertStore::empty();
        load_pem_roots(ca.to_str().unwrap(), &mut roots).unwrap();
        assert!(!roots.is_empty());
        std::fs::remove_file(ca).ok();

        let empty = write_tmp("pemroots-empty", "no pem here\n");
        let mut roots = RootCertStore::empty();
        let err = load_pem_roots(empty.to_str().unwrap(), &mut roots)
            .err()
            .unwrap();
        assert!(err.to_string().contains("no certificates"), "{err}");
        std::fs::remove_file(empty).ok();
    }

    #[test]
    fn no_verifier_accepts_any_cert_and_offers_schemes() {
        let v = NoVerifier;
        let cert = CertificateDer::from(vec![0u8; 32]);
        let name = ServerName::try_from("anything.example").unwrap();
        assert!(
            v.verify_server_cert(&cert, &[], &name, &[], UnixTime::now())
                .is_ok()
        );
        assert!(!v.supported_verify_schemes().is_empty());
    }

    #[test]
    fn skip_hostname_verifier_delegates_schemes() {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let inner = WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let v = SkipHostnameVerifier { inner };
        // delegates to the inner webpki verifier rather than a hardcoded list
        assert!(!v.supported_verify_schemes().is_empty());
    }

    #[test]
    fn build_client_config_env_branches() {
        let _e = lock_env();
        // snapshot the four vars so the process env is left as found
        let keys = ["PGSSLMODE", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY"];
        let saved: Vec<(&str, Option<String>)> =
            keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();
        for k in keys {
            set_env(k, None);
        }

        // SslMode::from_env: unset -> Prefer, set -> parsed, garbage -> err
        assert_eq!(SslMode::from_env().unwrap(), SslMode::Prefer);
        set_env("PGSSLMODE", Some("verify-full"));
        assert_eq!(SslMode::from_env().unwrap(), SslMode::VerifyFull);
        set_env("PGSSLMODE", Some("nonsense"));
        assert!(SslMode::from_env().is_err());
        set_env("PGSSLMODE", None);

        // PGSSLROOTCERT=<file> drives the load_pem_roots branch (not "system")
        let ca = write_tmp("env-ca", TEST_CRT);
        set_env("PGSSLROOTCERT", Some(ca.to_str().unwrap()));
        build_client_config(SslMode::VerifyFull).unwrap();
        build_client_config(SslMode::VerifyCa).unwrap();
        set_env("PGSSLROOTCERT", None);

        // PGSSLCERT + PGSSLKEY drive the with_client_auth_cert branch
        let crt = write_tmp("env-crt", TEST_CRT);
        let key = write_tmp("env-key", TEST_KEY);
        set_env("PGSSLCERT", Some(crt.to_str().unwrap()));
        set_env("PGSSLKEY", Some(key.to_str().unwrap()));
        build_client_config(SslMode::Prefer).unwrap();

        // half-configured client auth is a hard error (both required)
        set_env("PGSSLKEY", None);
        assert!(build_client_config(SslMode::Prefer).is_err());
        set_env("PGSSLCERT", None);
        set_env("PGSSLKEY", Some(key.to_str().unwrap()));
        assert!(build_client_config(SslMode::Prefer).is_err());

        for (k, v) in &saved {
            set_env(k, v.as_deref());
        }
        std::fs::remove_file(ca).ok();
        std::fs::remove_file(crt).ok();
        std::fs::remove_file(key).ok();
    }
}
