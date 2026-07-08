//! The sync-replica control API the Ubicloud control plane calls.
//!
//! Lives in the `sync_replica` domain and reads the controller's [`Shared`]
//! bridge directly — it never touches `SegmentAccumulator` or the receive
//! handler. Served on the controller's runtime via `hyper`.
//!
//! **Security: mTLS.** When the control-TLS env is set (`build_server_config`
//! from `WALG_WAL_RECEIVE_CONTROL_TLS_CERT` / `_TLS_KEY` / `_CLIENT_CA`) the
//! accepted `TcpStream` is wrapped in a `tokio_rustls::TlsAcceptor`: we present
//! `control-server.crt` and require + verify the client against `client-ca.crt`.
//! Hostname/SAN checking is not applicable server-side — `WebPkiClientVerifier`
//! does CA-chain + clientAuth validation only, matching the CP's
//! verify-CA-without-hostname client (`sync_pair/docs/sync-replica-controller.md`
//! §7). With the env unset we fall back to plain HTTP (local/tests).
//!
//! Endpoints (port 8444, base `/v1`): `GET /v1/status` (the CP's promote gate),
//! `POST /v1/dr-catchup` (push the retained tail to the DR-tail S3 lane — see
//! [`super::dr_tail`]), and `POST /v1/failover-primary` (flush the tail + re-
//! target the receiver hot path at the new primary without a restart).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Deserialize;
use tokio::net::TcpListener;

use super::{DrTail, Retarget, Shared};
use crate::pg::backup::{format_pg_lsn, parse_pg_lsn};

/// What the API serves — the controller's shared state, the retained-partial
/// directory, and (when DR-tail S3 is enabled) the dr-catchup uploader.
/// Decoupled from the receiver internals: only [`Shared`] + a path + [`DrTail`].
pub(crate) struct ApiState {
    pub shared: Arc<Shared>,
    pub partial_dir: String,
    pub dr: Option<Arc<DrTail>>,
}

/// Serve the control API on a pre-bound listener until the task is dropped. One
/// detached connection task each; HTTP/1 with a 10s header-read timeout
/// (matching the fork's `ReadHeaderTimeout`). `tls = Some(_)` wraps every
/// accepted stream in mTLS; `None` serves plain HTTP.
pub(crate) async fn serve(
    listener: TcpListener,
    state: Arc<ApiState>,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<()> {
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    loop {
        let (tcp, _peer) = listener.accept().await?;
        let state = state.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            match acceptor {
                Some(acc) => match acc.accept(tcp).await {
                    Ok(stream) => serve_conn(TokioIo::new(stream), state).await,
                    // A failed handshake (no/!valid client cert) is a normal
                    // probe; log at debug and drop the connection.
                    Err(e) => tracing::debug!(target = "sync_replica_api", "tls handshake: {e}"),
                },
                None => serve_conn(TokioIo::new(tcp), state).await,
            }
        });
    }
}

/// Run one HTTP/1 connection to completion. Generic over the IO so the plain-TCP
/// and TLS-wrapped streams share the same routing.
async fn serve_conn<I>(io: I, state: Arc<ApiState>)
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let service = service_fn(move |req| route(req, state.clone()));
    if let Err(e) = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(10))
        .serve_connection(io, service)
        .await
    {
        tracing::debug!(target = "sync_replica_api", "connection: {e}");
    }
}

/// Build the mTLS server config: present `cert`/`key`, require + verify the
/// client against the CA chain in `client_ca` (clientAuth, no hostname check).
fn build_server_config(
    cert_path: &str,
    key_path: &str,
    client_ca_path: &str,
) -> Result<rustls::ServerConfig> {
    use crate::pg::replication::tls::{load_cert_chain, load_pem_roots, load_private_key};

    let mut roots = rustls::RootCertStore::empty();
    load_pem_roots(client_ca_path, &mut roots)
        .with_context(|| format!("control client-ca {client_ca_path}"))?;

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .map_err(|e| anyhow::anyhow!("build client-cert verifier: {e}"))?;

    let certs = load_cert_chain(cert_path).with_context(|| format!("control cert {cert_path}"))?;
    let key = load_private_key(key_path).with_context(|| format!("control key {key_path}"))?;

    rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| anyhow::anyhow!("tls protocol versions: {e}"))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("control server cert/key: {e}"))
}

/// All-or-nothing classification of the three control-TLS path vars. `Ok(None)`
/// = none set (plain HTTP); `Ok(Some(..))` = all three; `Err` = a partial set
/// (a misconfiguration we refuse rather than silently serve in the clear).
fn control_tls_paths(
    cert: Option<String>,
    key: Option<String>,
    ca: Option<String>,
) -> Result<Option<(String, String, String)>> {
    let present = |v: Option<String>| v.filter(|s| !s.is_empty());
    match (present(cert), present(key), present(ca)) {
        (None, None, None) => Ok(None),
        (Some(c), Some(k), Some(a)) => Ok(Some((c, k, a))),
        _ => bail!(
            "control-API mTLS needs all of WALG_WAL_RECEIVE_CONTROL_TLS_CERT / _TLS_KEY / _CLIENT_CA"
        ),
    }
}

/// Resolve the control-API TLS config from the environment. `None` → plain HTTP.
pub(crate) fn control_tls_from_env() -> Result<Option<Arc<rustls::ServerConfig>>> {
    let cert = std::env::var("WALG_WAL_RECEIVE_CONTROL_TLS_CERT").ok();
    let key = std::env::var("WALG_WAL_RECEIVE_CONTROL_TLS_KEY").ok();
    let ca = std::env::var("WALG_WAL_RECEIVE_CONTROL_CLIENT_CA").ok();
    match control_tls_paths(cert, key, ca)? {
        None => Ok(None),
        Some((c, k, a)) => Ok(Some(Arc::new(build_server_config(&c, &k, &a)?))),
    }
}

/// `POST /v1/dr-catchup` body. `fromLsn` is the candidate's replay start (the
/// standby's `pg_last_wal_receive_lsn`); we anchor the upload there so stale low
/// cruft can't truncate the run. `toLsn` is the gate.
#[derive(Deserialize)]
struct DrCatchupReq {
    #[serde(rename = "fromLsn", default)]
    from_lsn: Option<String>,
    #[serde(rename = "toLsn")]
    to_lsn: String,
}

/// `POST /v1/failover-primary` body. `fromLsn` is advisory (ignored — the gate
/// is always our fsync frontier).
#[derive(Deserialize)]
struct FailoverReq {
    #[serde(rename = "newPrimary")]
    new_primary: NewPrimary,
}

#[derive(Deserialize)]
struct NewPrimary {
    host: String,
    #[serde(default)]
    port: Option<String>,
}

async fn route(
    req: Request<Incoming>,
    state: Arc<ApiState>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&Method::GET, "/v1/status") => {
            let lsn = format_pg_lsn(state.shared.fsyncd_lsn.load(Ordering::Acquire)).to_string();
            json(
                StatusCode::OK,
                serde_json::json!({ "lastAcceptedLsn": lsn, "partialDir": state.partial_dir }),
            )
        }
        (&Method::POST, "/v1/dr-catchup") => dr_catchup(req, &state).await,
        (&Method::POST, "/v1/failover-primary") => failover_primary(req, &state).await,
        _ => json(StatusCode::NOT_FOUND, serde_json::json!({})),
    };
    Ok(resp)
}

/// Push the retained tail up to `toLsn` to the DR-tail S3 lane and report the
/// durable+contiguous gate the CP should promote at. `409` when `toLsn` is past
/// our fsync frontier; `500` when DR-tail S3 isn't configured.
async fn dr_catchup(req: Request<Incoming>, state: &ApiState) -> Response<Full<Bytes>> {
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": format!("read body: {e}") }),
            );
        }
    };
    let parsed: DrCatchupReq = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": format!("bad json: {e}") }),
            );
        }
    };
    let to_lsn = match parse_pg_lsn(&parsed.to_lsn) {
        Ok(l) => l,
        Err(e) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": format!("bad toLsn: {e}") }),
            );
        }
    };
    // fromLsn is the candidate's replay start; absent/blank → 0 (no anchor).
    let from_lsn = match parsed.from_lsn.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match parse_pg_lsn(s) {
            Ok(l) => l,
            Err(e) => {
                return json(
                    StatusCode::BAD_REQUEST,
                    serde_json::json!({ "error": format!("bad fromLsn: {e}") }),
                );
            }
        },
        None => 0,
    };

    // The gate must not exceed what we've durably fsync'd: if asked for more, the
    // CP is ahead of our frontier — report the frontier so it backs off. (Checked
    // before the S3-config gate so a legitimate 409 isn't masked as a 500.)
    let frontier = state.shared.fsyncd_lsn.load(Ordering::Acquire);
    if to_lsn > frontier {
        return json(
            StatusCode::CONFLICT,
            serde_json::json!({
                "lastAcceptedLsn": format_pg_lsn(frontier).to_string(),
                "partialDir": state.partial_dir,
            }),
        );
    }

    let Some(dr) = state.dr.clone() else {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({
                "error": "dr-tail S3 delivery not configured: set WALG_WAL_RECEIVE_DR_S3 + WALG_S3_PREFIX (+ AWS creds) on the receiver"
            }),
        );
    };

    match dr
        .upload(Path::new(&state.partial_dir), from_lsn, to_lsn)
        .await
    {
        Ok((n, durable)) => json(
            StatusCode::OK,
            serde_json::json!({ "pushedThroughLsn": format_pg_lsn(durable).to_string(), "segments": n }),
        ),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({ "error": format!("dr-tail upload: {e:#}") }),
        ),
    }
}

/// Flush the receiver's tail to S3 (best-effort) and re-target the hot path at
/// the new primary without restarting — preserving the in-memory fsync frontier.
/// Returns the durable-flushed gate so the CP can confirm the tail is safe.
async fn failover_primary(req: Request<Incoming>, state: &ApiState) -> Response<Full<Bytes>> {
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": format!("read body: {e}") }),
            );
        }
    };
    let parsed: FailoverReq = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": format!("bad json: {e}") }),
            );
        }
    };
    if parsed.new_primary.host.is_empty() {
        return json(
            StatusCode::BAD_REQUEST,
            serde_json::json!({ "error": "newPrimary.host is required" }),
        );
    }
    let port: u16 = parsed
        .new_primary
        .port
        .as_deref()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5432);

    // The gate is our durable fsync frontier (not the advisory fromLsn).
    let frontier = state.shared.fsyncd_lsn.load(Ordering::Acquire);
    // Flush the tail to the DR-tail S3 lane so it survives total loss of both the
    // old primary and the receiver. Best-effort: when S3 isn't configured we
    // re-target anyway and report the raw frontier (matching wal-g).
    let (segments, pushed) = match state.dr.clone() {
        // Flush the whole retained tail (from 0 — no replay anchor here).
        Some(dr) => match dr.upload(Path::new(&state.partial_dir), 0, frontier).await {
            Ok((n, durable)) => (n, durable),
            Err(e) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({ "error": format!("dr-tail flush: {e:#}") }),
                );
            }
        },
        None => (0usize, frontier),
    };

    // Re-target the hot path: store the new primary and wake the recv loop to
    // break the (dead) stream and reconnect there.
    *state.shared.retarget.lock().expect("retarget mutex") = Some(Retarget {
        host: parsed.new_primary.host,
        port,
    });
    state.shared.retarget_signal.notify_one();

    json(
        StatusCode::OK,
        serde_json::json!({ "pushedThroughLsn": format_pg_lsn(pushed).to_string(), "segments": segments }),
    )
}

fn json(status: StatusCode, body: serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("static response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn write_tmp(body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    /// Self-signed CA (a valid trust anchor for the client-cert verifier).
    fn gen_ca() -> (rcgen::Certificate, rcgen::KeyPair) {
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.self_signed(&key).unwrap();
        (cert, key)
    }

    /// Leaf signed by `ca`, carrying the given EKU (e.g. clientAuth).
    fn gen_leaf(
        ca: &rcgen::Certificate,
        ca_key: &rcgen::KeyPair,
        cn: &str,
        eku: rcgen::ExtendedKeyUsagePurpose,
    ) -> (rcgen::Certificate, rcgen::KeyPair) {
        let mut p = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        p.extended_key_usages = vec![eku];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, ca, ca_key).unwrap();
        (cert, key)
    }

    #[tokio::test]
    async fn status_serves_the_durable_frontier() {
        let shared = Arc::new(Shared::default());
        shared.fsyncd_lsn.store(0x16B3750, Ordering::Release); // 0/16B3750
        let state = Arc::new(ApiState {
            shared,
            partial_dir: "/var/lib/walg/partials".into(),
            dr: None,
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state, None));

        let body = reqwest::get(format!("http://{addr}/v1/status"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["lastAcceptedLsn"], "0/16B3750");
        assert_eq!(v["partialDir"], "/var/lib/walg/partials");
    }

    #[tokio::test]
    async fn unknown_path_is_404() {
        let state = Arc::new(ApiState {
            shared: Arc::new(Shared::default()),
            partial_dir: String::new(),
            dr: None,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state, None));
        let status = reqwest::get(format!("http://{addr}/nope"))
            .await
            .unwrap()
            .status();
        assert_eq!(status, 404);
    }

    async fn serve_state(state: Arc<ApiState>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state, None));
        addr
    }

    #[tokio::test]
    async fn dr_catchup_409_when_asked_past_the_frontier() {
        let shared = Arc::new(Shared::default());
        shared.fsyncd_lsn.store(0x1000, Ordering::Release); // 0/1000
        let addr = serve_state(Arc::new(ApiState {
            shared,
            partial_dir: "/p".into(),
            dr: None,
        }))
        .await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/dr-catchup"))
            .body(r#"{"toLsn":"0/2000"}"#) // past the 0/1000 frontier
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["lastAcceptedLsn"], "0/1000");
    }

    #[tokio::test]
    async fn dr_catchup_500_when_s3_not_configured() {
        let shared = Arc::new(Shared::default());
        shared.fsyncd_lsn.store(0x2000, Ordering::Release);
        let addr = serve_state(Arc::new(ApiState {
            shared,
            partial_dir: "/p".into(),
            dr: None, // dr-tail S3 disabled
        }))
        .await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/dr-catchup"))
            .body(r#"{"toLsn":"0/1000"}"#) // within frontier → reaches the dr gate
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 500);
    }

    #[tokio::test]
    async fn dr_catchup_400_on_bad_json() {
        let addr = serve_state(Arc::new(ApiState {
            shared: Arc::new(Shared::default()),
            partial_dir: "/p".into(),
            dr: None,
        }))
        .await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/dr-catchup"))
            .body("not json")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn failover_primary_retargets_and_reports_frontier() {
        let shared = Arc::new(Shared::default());
        shared.fsyncd_lsn.store(0x3000, Ordering::Release); // 0/3000
        let addr = serve_state(Arc::new(ApiState {
            shared: shared.clone(),
            partial_dir: "/p".into(),
            dr: None, // no S3 flush → reports the raw frontier, still re-targets
        }))
        .await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/failover-primary"))
            .body(r#"{"newPrimary":{"host":"10.0.0.9","port":"5433"}}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["pushedThroughLsn"], "0/3000");
        assert_eq!(v["segments"], 0);
        // the re-target was recorded for the hot path to apply
        let rt = shared.retarget.lock().unwrap().clone().unwrap();
        assert_eq!(rt.host, "10.0.0.9");
        assert_eq!(rt.port, 5433);
    }

    #[tokio::test]
    async fn failover_primary_400_without_host() {
        let addr = serve_state(Arc::new(ApiState {
            shared: Arc::new(Shared::default()),
            partial_dir: "/p".into(),
            dr: None,
        }))
        .await;
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/failover-primary"))
            .body(r#"{"newPrimary":{"host":""}}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
    }

    #[test]
    fn control_tls_paths_is_all_or_nothing() {
        let s = |v: &str| Some(v.to_string());
        assert!(control_tls_paths(None, None, None).unwrap().is_none());
        assert!(control_tls_paths(s("c"), s("k"), s("a")).unwrap().is_some());
        assert!(control_tls_paths(s("c"), None, s("a")).is_err());
        assert!(control_tls_paths(s("c"), s("k"), None).is_err());
        // empty strings count as unset
        assert!(control_tls_paths(s(""), s(""), s("")).unwrap().is_none());
    }

    #[test]
    fn build_server_config_accepts_ca_and_matching_identity() {
        let (ca, ca_key) = gen_ca();
        // A self-signed server identity (the client side isn't exercised here).
        let server = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let ca_f = write_tmp(&ca.pem());
        let _ = &ca_key; // ca cert alone is the trust anchor
        let crt_f = write_tmp(&server.cert.pem());
        let key_f = write_tmp(&server.key_pair.serialize_pem());

        build_server_config(
            crt_f.path().to_str().unwrap(),
            key_f.path().to_str().unwrap(),
            ca_f.path().to_str().unwrap(),
        )
        .expect("config builds from a valid CA + identity");
    }

    /// Full mTLS handshake: a client presenting a CA-signed clientAuth cert is
    /// served `/v1/status`; a client presenting no cert is rejected at the
    /// handshake. Uses the crate's own tokio-rustls client primitives (reqwest
    /// lacks client-cert support in our feature set).
    #[tokio::test]
    async fn mtls_requires_a_valid_client_cert() {
        use crate::pg::replication::tls::{load_cert_chain, load_private_key};

        let (ca, ca_key) = gen_ca();
        let server = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let (client, client_key) = gen_leaf(
            &ca,
            &ca_key,
            "cp-client",
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        );

        let ca_f = write_tmp(&ca.pem());
        let srv_crt = write_tmp(&server.cert.pem());
        let srv_key = write_tmp(&server.key_pair.serialize_pem());
        let cfg = build_server_config(
            srv_crt.path().to_str().unwrap(),
            srv_key.path().to_str().unwrap(),
            ca_f.path().to_str().unwrap(),
        )
        .unwrap();

        let shared = Arc::new(Shared::default());
        shared.fsyncd_lsn.store(0x4FE2000, Ordering::Release); // 0/4FE2000
        let state = Arc::new(ApiState {
            shared,
            partial_dir: "/p".into(),
            dr: None,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state, Some(Arc::new(cfg))));

        // Client trusts any server cert (the CP verifies CA-chain only, not
        // hostname); what we exercise here is the *server's* client-cert gate.
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();

        // (1) With a valid client cert → 200 + the durable frontier.
        let cli_crt = write_tmp(&client.pem());
        let cli_key = write_tmp(&client_key.serialize_pem());
        let chain = load_cert_chain(cli_crt.path().to_str().unwrap()).unwrap();
        let key = load_private_key(cli_key.path().to_str().unwrap()).unwrap();
        let with_cert = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyServerCert))
            .with_client_auth_cert(chain, key)
            .unwrap();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(with_cert));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut tls = connector.connect(server_name.clone(), tcp).await.unwrap();
        tls.write_all(b"GET /v1/status HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        tls.read_to_end(&mut resp).await.unwrap();
        let resp = String::from_utf8_lossy(&resp);
        assert!(resp.contains("200 OK"), "expected 200, got:\n{resp}");
        assert!(
            resp.contains("0/4FE2000"),
            "expected frontier, got:\n{resp}"
        );

        // (2) No client cert → the server rejects at the handshake.
        let no_cert = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyServerCert))
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(no_cert));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        // TLS 1.2 rejects at the handshake; TLS 1.3 completes the client flight
        // and the server's missing-cert alert surfaces on the first read. Either
        // way the no-cert client must never receive a 200.
        let served_200 = match connector.connect(server_name, tcp).await {
            Err(_) => false,
            Ok(mut tls) => {
                let _ = tls
                    .write_all(b"GET /v1/status HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .await;
                let mut resp = Vec::new();
                tls.read_to_end(&mut resp).await.is_ok()
                    && String::from_utf8_lossy(&resp).contains("200 OK")
            }
        };
        assert!(!served_200, "server must reject a client with no cert");
    }

    /// Client-side verifier that accepts any server cert (the CP does CA-chain
    /// validation but not hostname; here we only test the server's client gate).
    #[derive(Debug)]
    struct AnyServerCert;

    impl rustls::client::danger::ServerCertVerifier for AnyServerCert {
        fn verify_server_cert(
            &self,
            _end: &rustls::pki_types::CertificateDer<'_>,
            _inter: &[rustls::pki_types::CertificateDer<'_>],
            _name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error>
        {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _m: &[u8],
            _c: &rustls::pki_types::CertificateDer<'_>,
            _d: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _m: &[u8],
            _c: &rustls::pki_types::CertificateDer<'_>,
            _d: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            use rustls::SignatureScheme::*;
            vec![
                RSA_PKCS1_SHA256,
                RSA_PKCS1_SHA384,
                RSA_PKCS1_SHA512,
                ECDSA_NISTP256_SHA256,
                ECDSA_NISTP384_SHA384,
                ECDSA_NISTP521_SHA512,
                RSA_PSS_SHA256,
                RSA_PSS_SHA384,
                RSA_PSS_SHA512,
                ED25519,
            ]
        }
    }
}
