//! In-process HTTP/1.1 server mocking S3 / GCS REST for storage backend
//! tests. Signature/auth headers are accepted blindly so signing code runs
//! without the mock validating it

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use super::{AsyncReader, ObjectStream, Storage};

pub(crate) struct Req {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Req {
    pub fn query(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn has_query(&self, key: &str) -> bool {
        self.query.iter().any(|(k, _)| k == key)
    }
}

pub(crate) struct Resp {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Resp {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn body(mut self, b: impl Into<Vec<u8>>) -> Self {
        self.body = b.into();
        self
    }

    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.to_string(), v.to_string()));
        self
    }
}

/// Bind an ephemeral port, serve `handler` until the test runtime drops.
/// Returns the base URL (`http://127.0.0.1:PORT`).
pub(crate) async fn serve<H>(handler: H) -> String
where
    H: Fn(&Req) -> Resp + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handler = Arc::new(handler);
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            let handler = handler.clone();
            let svc = service_fn(move |req| {
                let handler = handler.clone();
                async move { respond(req, &*handler).await }
            });
            tokio::spawn(http1::Builder::new().serve_connection(TokioIo::new(sock), svc));
        }
    });
    format!("http://{addr}")
}

async fn respond<H>(req: Request<Incoming>, handler: &H) -> hyper::Result<Response<Full<Bytes>>>
where
    H: Fn(&Req) -> Resp,
{
    let (parts, body) = req.into_parts();
    let headers = parts
        .headers
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
        .collect();
    let req = Req {
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query: parse_query(parts.uri.query().unwrap_or("")),
        headers,
        body: body.collect().await?.to_bytes().to_vec(),
    };
    let resp = handler(&req);
    let mut out = Response::builder().status(resp.status);
    for (k, v) in &resp.headers {
        out = out.header(k, v);
    }
    Ok(out.body(Full::new(Bytes::from(resp.body))).unwrap())
}

fn parse_query(q: &str) -> Vec<(String, String)> {
    if q.is_empty() {
        return Vec::new();
    }
    q.split('&')
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (pct_decode(k), pct_decode(v)),
            None => (pct_decode(kv), String::new()),
        })
        .collect()
}

/// Decode `%XX` escapes; leaves other bytes verbatim
pub(crate) fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 3 <= b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// In-memory AsyncReader over `bytes`
pub(crate) fn reader(bytes: &[u8]) -> AsyncReader {
    Box::pin(std::io::Cursor::new(bytes.to_vec()))
}

/// Drain an AsyncReader to a Vec
pub(crate) async fn read_all(mut r: AsyncReader) -> Vec<u8> {
    let mut b = Vec::new();
    r.read_to_end(&mut b).await.unwrap();
    b
}

/// Collect every key a `list` stream yields, erroring on the first failure
pub(crate) async fn drain_keys(s: &dyn Storage, prefix: &str) -> Vec<String> {
    let mut st: ObjectStream = s.list(prefix).await.unwrap();
    let mut out = Vec::new();
    while let Some(item) = st.next().await {
        out.push(item.unwrap().key);
    }
    out
}

/// Deterministic payload of `n` bytes; large enough sizes walk the S3
/// multipart part loop
pub(crate) fn payload(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}
