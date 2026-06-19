//! Minimal in-process HTTP/1.1 server for storage backend tests.
//!
//! Speaks just enough to mock S3 / GCS REST against the reqwest client:
//! one request per connection, `Connection: close`, Content-Length or
//! chunked request bodies, optional `Expect: 100-continue`. Test-only; the
//! signature/auth headers the backends emit are accepted blindly so the
//! signing code runs without the mock having to validate it.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

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
            tokio::spawn(async move {
                let _ = handle_conn(sock, handler).await;
            });
        }
    });
    format!("http://{addr}")
}

async fn handle_conn<H>(sock: TcpStream, handler: Arc<H>) -> std::io::Result<()>
where
    H: Fn(&Req) -> Resp + Send + Sync + 'static,
{
    let (rd, mut wr) = tokio::io::split(sock);
    let mut reader = BufReader::new(rd);

    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let mut it = line.trim_end().split(' ');
    let method = it.next().unwrap_or("").to_string();
    let target = it.next().unwrap_or("").to_string();

    let mut headers = HashMap::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).await? == 0 {
            break;
        }
        let t = h.trim_end();
        if t.is_empty() {
            break;
        }
        if let Some((k, v)) = t.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    if headers
        .get("expect")
        .is_some_and(|v| v.eq_ignore_ascii_case("100-continue"))
    {
        wr.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
        wr.flush().await?;
    }

    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.contains("chunked"))
    {
        read_chunked(&mut reader).await?
    } else if let Some(n) = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
    {
        let mut b = vec![0u8; n];
        reader.read_exact(&mut b).await?;
        b
    } else {
        Vec::new()
    };

    let (path, qs) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q),
        None => (target.clone(), ""),
    };
    let req = Req {
        method,
        path,
        query: parse_query(qs),
        headers,
        body,
    };
    let resp = handler(&req);
    write_resp(&mut wr, resp).await
}

async fn read_chunked(
    reader: &mut BufReader<tokio::io::ReadHalf<TcpStream>>,
) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let mut size_line = String::new();
        reader.read_line(&mut size_line).await?;
        let size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
        if size == 0 {
            // consume trailing CRLF / any trailers
            loop {
                let mut l = String::new();
                if reader.read_line(&mut l).await? == 0 || l.trim().is_empty() {
                    break;
                }
            }
            break;
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk).await?;
        out.append(&mut chunk);
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;
    }
    Ok(out)
}

async fn write_resp(wr: &mut tokio::io::WriteHalf<TcpStream>, resp: Resp) -> std::io::Result<()> {
    let mut head = format!("HTTP/1.1 {} {}\r\n", resp.status, reason(resp.status));
    let mut has_len = false;
    for (k, v) in &resp.headers {
        if k.eq_ignore_ascii_case("content-length") {
            has_len = true;
        }
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    if !has_len {
        head.push_str(&format!("content-length: {}\r\n", resp.body.len()));
    }
    head.push_str("connection: close\r\n\r\n");
    wr.write_all(head.as_bytes()).await?;
    wr.write_all(&resp.body).await?;
    wr.flush().await
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
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
