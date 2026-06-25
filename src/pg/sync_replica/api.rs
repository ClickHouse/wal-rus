//! The sync-replica control API the Ubicloud control plane calls.
//!
//! Lives in the `sync_replica` domain and reads the controller's [`Shared`]
//! bridge directly — it never touches `SegmentAccumulator` or the receive
//! handler. Served on the controller's runtime via `hyper`.
//!
//! **Security: plain HTTP for now.** The wire contract is mTLS (the CP presents
//! a client cert, we present `control-server.crt` and verify the client against
//! `client-ca.crt`, hostname/SAN checking disabled — see
//! `sync_pair/docs/sync-replica-controller.md` §7). That wraps the accepted
//! `TcpStream` in a `tokio_rustls::TlsAcceptor` before `TokioIo` — a localized
//! follow-up; the routing below is unchanged by it.
//!
//! Endpoints (port 8444, base `/v1`): `GET /v1/status` is implemented (the CP's
//! promote gate). `POST /v1/dr-catchup` / `POST /v1/failover-primary` need the
//! dr-tail push + re-target machinery and are stubbed `501` for now.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;

use super::Shared;
use crate::pg::backup::format_pg_lsn;

/// What the API serves — the controller's shared state + the retained-partial
/// directory. Decoupled from the receiver internals: only [`Shared`] + a path.
pub(crate) struct ApiState {
    pub shared: Arc<Shared>,
    pub partial_dir: String,
}

/// Serve the control API on a pre-bound listener until the task is dropped. One
/// detached connection task each; HTTP/1 with a 10s header-read timeout
/// (matching the fork's `ReadHeaderTimeout`).
pub(crate) async fn serve(listener: TcpListener, state: Arc<ApiState>) -> Result<()> {
    loop {
        let (tcp, _peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(tcp);
            let service = service_fn(move |req| route(req, state.clone()));
            if let Err(e) = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(io, service)
                .await
            {
                tracing::debug!(target = "sync_replica_api", "connection: {e}");
            }
        });
    }
}

async fn route(
    req: Request<Incoming>,
    state: Arc<ApiState>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&Method::GET, "/v1/status") => {
            let lsn = format_pg_lsn(state.shared.fsyncd_lsn.load(Ordering::Acquire));
            json(
                StatusCode::OK,
                serde_json::json!({ "lastAcceptedLsn": lsn, "partialDir": state.partial_dir }),
            )
        }
        (&Method::POST, "/v1/dr-catchup") | (&Method::POST, "/v1/failover-primary") => {
            // TODO: dr-tail S3 push + no-restart re-target
            json(
                StatusCode::NOT_IMPLEMENTED,
                serde_json::json!({ "error": "not implemented" }),
            )
        }
        _ => json(StatusCode::NOT_FOUND, serde_json::json!({})),
    };
    Ok(resp)
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

    #[tokio::test]
    async fn status_serves_the_durable_frontier() {
        let shared = Arc::new(Shared::default());
        shared.fsyncd_lsn.store(0x16B3750, Ordering::Release); // 0/16B3750
        let state = Arc::new(ApiState {
            shared,
            partial_dir: "/var/lib/walg/partials".into(),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state));

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
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, state));
        let status = reqwest::get(format!("http://{addr}/nope"))
            .await
            .unwrap()
            .status();
        assert_eq!(status, 404);
    }
}
