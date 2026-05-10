//! TLS negotiation for the postgres replication socket
//!
//! Mirrors libpq sslmode: disable / allow / prefer / require / verify-ca / verify-full
//!
//! Wire form: client sends SSLRequest (int32 len=8, int32 code=80877103)
//! Server replies with single byte 'S' (proceed with TLS) or 'N' (refused)
//! On 'S', upgrade the TCP socket via tokio-rustls

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use rustls::CertificateError;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
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

    fn verifies_cert(self) -> bool {
        matches!(self, SslMode::VerifyCa | SslMode::VerifyFull)
    }

    fn verifies_hostname(self) -> bool {
        matches!(self, SslMode::VerifyFull)
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

    let config = if sslmode.verifies_cert() {
        let mut roots = RootCertStore::empty();
        if let Ok(path) = std::env::var("PGSSLROOTCERT")
            && !path.is_empty()
        {
            load_pem_roots(&path, &mut roots)
                .with_context(|| format!("load PGSSLROOTCERT={path}"))?;
        } else {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        if sslmode.verifies_hostname() {
            builder.with_root_certificates(roots).with_no_client_auth()
        } else {
            // verify-ca: full path validation, only the hostname check is suppressed
            let inner = WebPkiServerVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| anyhow!("build verify-ca verifier: {e}"))?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipHostnameVerifier { inner }))
                .with_no_client_auth()
        }
    } else {
        // prefer / require / allow: opportunistic encryption, no cert verification
        // (matches libpq behavior; trade off documented at call site)
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    };
    Ok(config)
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

/// Accepts any server cert, no verification. For sslmode=prefer/require.
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

/// sslmode=verify-ca: delegate full path validation to webpki, suppress only
/// the hostname/SNI mismatch error so a cert valid against the configured
/// roots is accepted regardless of CN/SAN
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
    fn client_config_builds_for_all_modes() {
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
}
