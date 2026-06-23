//! S3 backend, hand-rolled SigV4 request signing (see `s3_signing_headers`)
//!
//! UNSIGNED-PAYLOAD on HTTPS so we don't buffer or hash request bodies
//!
//! Env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
//! AWS_REGION (default us-east-1), AWS_ENDPOINT_URL or WALG_S3_ENDPOINT,
//! WALG_S3_FORCE_PATH_STYLE. Without static keys, credentials come from the
//! EC2 metadata service (see [`super::creds`])

use std::io::Cursor;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_lc_rs::{digest, hmac};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::Client;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;
use url::Url;

pub use super::creds::{CredentialSource, Credentials, ImdsProvider};
use super::{AsyncReader, CopySource, ObjectMeta, ObjectStream, Result, Storage, StorageError};
use crate::retry::{RetryPolicy, with_retry};

const MULTIPART_THRESHOLD: u64 = 32 * 1024 * 1024;
const PART_SIZE: usize = 8 * 1024 * 1024;

/// Path component encoding per SigV4 spec
/// Same set as URL path-segment, but '/' kept literal
const PATH_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');

const QUERY_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub creds: CredentialSource,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
}

pub struct S3Storage {
    cfg: S3Config,
    client: Client,
    base: String,
    retry_policy: RetryPolicy,
}

impl S3Storage {
    pub fn new(cfg: S3Config) -> Result<Self> {
        Self::with_retry_policy(cfg, RetryPolicy::default())
    }

    pub fn with_retry_policy(cfg: S3Config, retry_policy: RetryPolicy) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| StorageError::Config(e.to_string()))?;
        let base = build_base_url(&cfg);
        Ok(Self {
            cfg,
            client,
            base,
            retry_policy,
        })
    }

    fn full_key(&self, key: &str) -> String {
        super::join_prefix_key(&self.cfg.prefix, key)
    }

    /// Server-side copy identity: same endpoint/region + same credential.
    /// Conservative: AWS allows cross-region CopyObject, but mismatched
    /// region ids fall back to stream-through rather than risk custom
    /// endpoints (minio, ceph) that don't
    fn backend_id(&self) -> String {
        format!(
            "s3:{}:{}",
            self.cfg.endpoint.as_deref().unwrap_or(&self.cfg.region),
            self.cfg.creds.identity(),
        )
    }

    /// Request URL with the query baked in, so the string the signer signs and
    /// the string reqwest sends are byte-identical (both read path+query off
    /// this one `Url`). Path-style endpoints carry the bucket in `base`, so it
    /// lands in the signed path automatically.
    fn build_url(&self, key_path: &str, query: &[(&str, &str)]) -> Result<Url> {
        let mut s = if key_path.is_empty() {
            self.base.clone()
        } else {
            format!(
                "{}/{}",
                self.base,
                utf8_percent_encode(key_path, PATH_ENCODE)
            )
        };
        if !query.is_empty() {
            s.push('?');
            let qs = query
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        utf8_percent_encode(k, QUERY_ENCODE),
                        utf8_percent_encode(v, QUERY_ENCODE)
                    )
                })
                .collect::<Vec<_>>()
                .join("&");
            s.push_str(&qs);
        }
        Url::parse(&s).map_err(|e| StorageError::Config(format!("bad url {s}: {e}")))
    }

    async fn signed_request(
        &self,
        method: &str,
        key_path: &str,
        query: &[(&str, &str)],
        body: Bytes,
        extra_headers: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let url = self.build_url(key_path, query)?;
        let creds = self.cfg.creds.get().await?;
        let signed = s3_signing_headers(
            &creds,
            &self.cfg.region,
            method,
            url.as_str(),
            extra_headers,
            SystemTime::now(),
        )?;

        let mut req = self.client.request(
            method
                .parse()
                .map_err(|_| StorageError::Config(format!("bad method {method}")))?,
            url,
        );
        // headers we set and sign; host is derived from the URI by the signer
        // and set on the wire by reqwest, so it isn't threaded through here
        for (k, v) in extra_headers {
            req = req.header(*k, *v);
        }
        // signer output: authorization, x-amz-date, x-amz-content-sha256, and
        // x-amz-security-token when the credential carries a session token
        for (k, v) in &signed {
            req = req.header(k.as_str(), v.as_str());
        }
        let req = if body.is_empty() { req } else { req.body(body) };
        let resp = req.send().await?;
        Ok(resp)
    }

    async fn put_single(&self, key: &str, body: Bytes) -> Result<()> {
        let resp = self
            .signed_request("PUT", &self.full_key(key), &[], body, &[])
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    /// Single PUT retrying transients in place; `body` is buffered so replayable,
    /// matching multipart's per-part retry
    async fn put_single_retrying(&self, key: &str, body: Bytes) -> Result<()> {
        with_retry(&self.retry_policy, StorageError::is_transient, || {
            let body = body.clone();
            async move { self.put_single(key, body).await }
        })
        .await
    }

    async fn put_multipart(&self, key: &str, mut body: AsyncReader) -> Result<()> {
        // initiate
        let init_resp = self
            .signed_request(
                "POST",
                &self.full_key(key),
                &[("uploads", "")],
                Bytes::new(),
                &[],
            )
            .await?;
        let init_resp = check_status(init_resp).await?;
        let init_body = init_resp.text().await?;
        let upload_id = first_tag_text(&init_body, b"UploadId").ok_or_else(|| {
            StorageError::InvalidResponse("missing UploadId in CreateMultipartUpload".into())
        })?;

        let mut parts: Vec<(u32, String)> = Vec::new();
        let mut part_no: u32 = 0;
        let mut buf = vec![0u8; PART_SIZE];

        loop {
            // fill buf up to PART_SIZE or EOF
            let mut filled = 0usize;
            while filled < buf.len() {
                let n = body.read(&mut buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 && part_no > 0 {
                break;
            }
            part_no += 1;
            let part_no_str = part_no.to_string();
            let chunk = Bytes::copy_from_slice(&buf[..filled]);

            // Per-part retry: chunk is already buffered, so transient failures
            // (5xx, transport) replay the same body without re-reading source
            let key_full = self.full_key(key);
            let result = with_retry(&self.retry_policy, StorageError::is_transient, || async {
                let resp = self
                    .signed_request(
                        "PUT",
                        &key_full,
                        &[
                            ("partNumber", part_no_str.as_str()),
                            ("uploadId", upload_id.as_str()),
                        ],
                        chunk.clone(),
                        &[],
                    )
                    .await?;
                let resp = check_status(resp).await?;
                let etag = resp
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| StorageError::InvalidResponse("missing ETag".into()))?
                    .to_string();
                Ok::<String, StorageError>(etag)
            })
            .await;

            let etag = match result {
                Ok(e) => e,
                Err(e) => {
                    let _ = self.abort_multipart(key, &upload_id).await;
                    return Err(e);
                }
            };
            parts.push((part_no, etag));

            if filled < PART_SIZE {
                break;
            }
        }

        if parts.is_empty() {
            // empty body, send a single empty part
            part_no += 1;
            let resp = self
                .signed_request(
                    "PUT",
                    &self.full_key(key),
                    &[
                        ("partNumber", part_no.to_string().as_str()),
                        ("uploadId", upload_id.as_str()),
                    ],
                    Bytes::new(),
                    &[],
                )
                .await?;
            let resp = check_status(resp).await?;
            let etag = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("\"d41d8cd98f00b204e9800998ecf8427e\"")
                .to_string();
            parts.push((part_no, etag));
        }

        // complete
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in &parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                n, etag
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let resp = self
            .signed_request(
                "POST",
                &self.full_key(key),
                &[("uploadId", upload_id.as_str())],
                Bytes::from(xml),
                &[("content-type", "application/xml")],
            )
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        let resp = self
            .signed_request(
                "DELETE",
                &self.full_key(key),
                &[("uploadId", upload_id)],
                Bytes::new(),
                &[],
            )
            .await?;
        let _ = resp.status();
        Ok(())
    }
}

#[async_trait]
impl Storage for S3Storage {
    fn describe(&self) -> String {
        format!("s3://{}/{}", self.cfg.bucket, self.cfg.prefix)
    }

    async fn put(&self, key: &str, mut body: AsyncReader, size_hint: Option<u64>) -> Result<()> {
        match size_hint {
            // known small: buffer & single PUT, retrying transients in place
            Some(s) if s <= MULTIPART_THRESHOLD => {
                let mut buf = Vec::with_capacity(s as usize);
                body.read_to_end(&mut buf).await?;
                self.put_single_retrying(key, Bytes::from(buf)).await
            }
            // known large: stream to multipart
            Some(_) => self.put_multipart(key, body).await,
            // unknown size (compressed/encrypted WAL, tar parts): buffer up to
            // the multipart threshold. Bodies under it, every WAL segment since
            // 16 MiB raw compresses smaller, go out as one PUT with a known
            // Content-Length instead of multipart's create/upload/complete trio.
            // Read one past the cap to tell a fitting EOF from overflow.
            None => {
                let limit = MULTIPART_THRESHOLD as usize;
                let mut limited = body.take(limit as u64 + 1);
                let mut buf = Vec::new();
                limited.read_to_end(&mut buf).await?;
                if buf.len() <= limit {
                    self.put_single_retrying(key, Bytes::from(buf)).await
                } else {
                    // overflow: prepend buffered prefix to the unread remainder
                    let combined = Cursor::new(buf).chain(limited.into_inner());
                    self.put_multipart(key, Box::pin(combined)).await
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Result<AsyncReader> {
        let resp = self
            .signed_request("GET", &self.full_key(key), &[], Bytes::new(), &[])
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StorageError::NotFound(key.to_string()));
        }
        let resp = check_status(resp).await?;
        let stream = resp
            .bytes_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        Ok(Box::pin(StreamReader::new(stream)))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let resp = self
            .signed_request("HEAD", &self.full_key(key), &[], Bytes::new(), &[])
            .await?;
        Ok(resp.status().is_success())
    }

    async fn list(&self, prefix: &str) -> Result<ObjectStream> {
        let full_prefix = self.full_key(prefix);
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let base = self.base.clone();
        let retry_policy = self.retry_policy;

        let s = stream::unfold(
            (Some(String::new()), full_prefix, cfg, client, base),
            move |(token, prefix, cfg, client, base)| async move {
                let token = token?;
                let s = S3Storage {
                    cfg: cfg.clone(),
                    client: client.clone(),
                    base: base.clone(),
                    retry_policy,
                };
                let q: [(&str, &str); _] = [
                    ("list-type", "2"),
                    ("prefix", prefix.as_str()),
                    ("continuation-token", token.as_str()),
                ];
                let q = if token.is_empty() { &q[..2] } else { &q[..] };
                let resp = match s.signed_request("GET", "", q, Bytes::new(), &[]).await {
                    Ok(r) => r,
                    Err(e) => return Some((Err(e), (None, prefix, cfg, client, base))),
                };
                let resp = match check_status(resp).await {
                    Ok(r) => r,
                    Err(e) => return Some((Err(e), (None, prefix, cfg, client, base))),
                };
                let body = match resp.text().await {
                    Ok(b) => b,
                    Err(e) => {
                        return Some((Err(e.into()), (None, prefix, cfg, client, base)));
                    }
                };
                match parse_list_v2(&body, &cfg.prefix) {
                    Ok((objects, next)) => {
                        let next_state = (next, prefix, cfg, client, base);
                        Some((Ok(objects), next_state))
                    }
                    Err(e) => Some((Err(e), (None, prefix, cfg, client, base))),
                }
            },
        )
        .flat_map(|res| match res {
            Ok(v) => stream::iter(v.into_iter().map(Ok)).left_stream(),
            Err(e) => stream::iter(std::iter::once(Err(e))).right_stream(),
        });

        Ok(Box::pin(s))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let resp = self
            .signed_request("DELETE", &self.full_key(key), &[], Bytes::new(), &[])
            .await?;
        // 204 or 404 both ok
        let st = resp.status();
        if st.is_success() || st == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(StorageError::Http {
                status: st.as_u16(),
                body: format!("delete {key}: {body}"),
            })
        }
    }

    fn copy_source(&self, key: &str) -> Option<CopySource> {
        Some(CopySource {
            backend: self.backend_id(),
            bucket: self.cfg.bucket.clone(),
            key: self.full_key(key),
        })
    }

    async fn copy_within(&self, src: &CopySource, dst_key: &str) -> Result<()> {
        if src.backend != self.backend_id() {
            return Err(StorageError::Unimplemented("copy_within backend mismatch"));
        }
        // CopyObject caps at 5 GiB per request; larger sources fail with 400
        // and caller falls back to stream-through
        let header = copy_source_header(&src.bucket, &src.key);
        let resp = self
            .signed_request(
                "PUT",
                &self.full_key(dst_key),
                &[],
                Bytes::new(),
                &[("x-amz-copy-source", header.as_str())],
            )
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StorageError::NotFound(src.key.clone()));
        }
        let resp = check_status(resp).await?;
        copy_object_result(&resp.text().await?)
    }
}

/// x-amz-copy-source value: /bucket/key, key path-encoded per SigV4
fn copy_source_header(bucket: &str, key: &str) -> String {
    format!("/{}/{}", bucket, utf8_percent_encode(key, PATH_ENCODE))
}

/// CopyObject returns 200 before copy completes; failures past that point
/// surface as <Error> in the body. Mapped to 500 (transient) so the retry
/// wrapper replays, per AWS guidance to retry embedded copy errors
fn copy_object_result(body: &str) -> Result<()> {
    if body.contains("<CopyObjectResult") {
        Ok(())
    } else {
        Err(StorageError::Http {
            status: 500,
            body: format!("copy object: {body}"),
        })
    }
}

fn build_base_url(cfg: &S3Config) -> String {
    if let Some(ep) = &cfg.endpoint {
        let ep = ep.trim_end_matches('/');
        if cfg.force_path_style {
            format!("{}/{}", ep, cfg.bucket)
        } else {
            // virtual-host style on custom endpoint: prepend bucket
            // most setups (minio, ceph) want path-style; default conservatively path
            format!("{}/{}", ep, cfg.bucket)
        }
    } else {
        format!("https://{}.s3.{}.amazonaws.com", cfg.bucket, cfg.region)
    }
}

const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(digest::digest(&digest::SHA256, data))
}

/// SigV4 headers (authorization, x-amz-date, x-amz-content-sha256, and
/// x-amz-security-token when a session token is present) for one request.
/// `url` must be byte-identical to the wire URL. S3 specifics: single
/// percent-encoding (path+query already encoded by `build_url`, never
/// re-encoded here), UNSIGNED-PAYLOAD, no path normalization. Explicit
/// credentials only, never profile discovery.
///
/// host is omitted from the result: it's signed but reqwest sets it on the
/// wire from the URL authority. `extra_headers` are signed but returned by
/// the caller, not here.
fn s3_signing_headers(
    creds: &Credentials,
    region: &str,
    method: &str,
    url: &str,
    extra_headers: &[(&str, &str)],
    time: SystemTime,
) -> Result<Vec<(String, String)>> {
    let parsed =
        Url::parse(url).map_err(|e| StorageError::Auth(format!("sigv4 url {url}: {e}")))?;
    let host = match parsed.port() {
        Some(p) => format!("{}:{p}", parsed.host_str().unwrap_or_default()),
        None => parsed.host_str().unwrap_or_default().to_string(),
    };

    let dt: DateTime<Utc> = time.into();
    let amz_date = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = dt.format("%Y%m%d").to_string();
    let scope = format!("{date_stamp}/{region}/s3/aws4_request");

    // headers to sign: auto headers + caller extras, lowercased, value-trimmed
    let mut signed: Vec<(String, String)> = vec![
        ("host".into(), host),
        ("x-amz-content-sha256".into(), UNSIGNED_PAYLOAD.into()),
        ("x-amz-date".into(), amz_date.clone()),
    ];
    if let Some(tok) = &creds.session_token {
        signed.push(("x-amz-security-token".into(), tok.clone()));
    }
    for (k, v) in extra_headers {
        signed.push((k.to_ascii_lowercase(), v.trim().to_string()));
    }
    signed.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = signed.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = signed
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    // already single-encoded by build_url; sort query params by encoded string
    let canonical_uri = if parsed.path().is_empty() {
        "/"
    } else {
        parsed.path()
    };
    let canonical_query = match parsed.query() {
        Some(q) if !q.is_empty() => {
            let mut parts: Vec<&str> = q.split('&').collect();
            parts.sort_unstable();
            parts.join("&")
        }
        _ => String::new(),
    };

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{UNSIGNED_PAYLOAD}"
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", creds.secret_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), b"s3");
    let k_signing = hmac_sha256(k_service.as_ref(), b"aws4_request");
    let signature = hex::encode(hmac_sha256(k_signing.as_ref(), string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );

    let mut out = vec![
        (
            "x-amz-content-sha256".to_string(),
            UNSIGNED_PAYLOAD.to_string(),
        ),
        ("x-amz-date".to_string(), amz_date),
        ("authorization".to_string(), authorization),
    ];
    if let Some(tok) = &creds.session_token {
        out.push(("x-amz-security-token".to_string(), tok.clone()));
    }
    Ok(out)
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response> {
    let st = resp.status();
    if st.is_success() {
        Ok(resp)
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(StorageError::Http {
            status: st.as_u16(),
            body,
        })
    }
}

/// Charset-decode then resolve XML entities. quick-xml 0.40 split unescaping
/// out of the text event, so both steps are explicit.
fn decode_text(t: &quick_xml::events::BytesText) -> Result<String> {
    let decoded = t
        .decode()
        .map_err(|e| StorageError::InvalidResponse(format!("xml decode: {e}")))?;
    let unescaped = quick_xml::escape::unescape(&decoded)
        .map_err(|e| StorageError::InvalidResponse(format!("xml unescape: {e}")))?;
    Ok(unescaped.into_owned())
}

/// Text of the first element whose local name matches `tag`. Used for the
/// single-valued CreateMultipartUpload `UploadId`.
fn first_tag_text(xml: &str, tag: &[u8]) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    let mut capture = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if e.local_name().as_ref() == tag => capture = true,
            Ok(Event::Text(t)) if capture => return decode_text(&t).ok(),
            Ok(Event::End(e)) if e.local_name().as_ref() == tag => capture = false,
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

/// Field currently being read; selects where the next `Text` event lands.
#[derive(Clone, Copy, PartialEq)]
enum ListField {
    None,
    Key,
    Size,
    LastModified,
    IsTruncated,
    NextToken,
}

fn parse_list_v2(xml: &str, strip_prefix: &str) -> Result<(Vec<ObjectMeta>, Option<String>)> {
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    let mut truncated = false;
    let mut next_token: Option<String> = None;

    let mut in_contents = false;
    let mut field = ListField::None;
    let mut key = String::new();
    let mut size: u64 = 0;
    let mut last_modified = None;

    loop {
        match reader
            .read_event()
            .map_err(|e| StorageError::InvalidResponse(format!("list xml: {e}")))?
        {
            Event::Eof => break,
            Event::Start(e) => match e.local_name().as_ref() {
                b"Contents" => {
                    in_contents = true;
                    key.clear();
                    size = 0;
                    last_modified = None;
                }
                b"Key" if in_contents => field = ListField::Key,
                b"Size" if in_contents => field = ListField::Size,
                b"LastModified" if in_contents => field = ListField::LastModified,
                b"IsTruncated" => field = ListField::IsTruncated,
                b"NextContinuationToken" => field = ListField::NextToken,
                _ => {}
            },
            Event::Text(t) if field != ListField::None => {
                let txt = decode_text(&t)?;
                let txt = txt.trim();
                match field {
                    ListField::Key => key = txt.to_string(),
                    ListField::Size => size = txt.parse().unwrap_or(0),
                    ListField::LastModified => {
                        last_modified = chrono::DateTime::parse_from_rfc3339(txt)
                            .ok()
                            .map(|d| d.with_timezone(&Utc));
                    }
                    ListField::IsTruncated => truncated = txt == "true",
                    ListField::NextToken if !txt.is_empty() => next_token = Some(txt.to_string()),
                    _ => {}
                }
            }
            Event::End(e) => {
                if e.local_name().as_ref() == b"Contents" {
                    in_contents = false;
                    let trimmed = if strip_prefix.is_empty() {
                        std::mem::take(&mut key)
                    } else {
                        match key.strip_prefix(strip_prefix.trim_end_matches('/')) {
                            Some(s) => s.trim_start_matches('/').to_string(),
                            None => std::mem::take(&mut key),
                        }
                    };
                    out.push(ObjectMeta {
                        key: trimmed,
                        size,
                        last_modified,
                    });
                }
                field = ListField::None;
            }
            _ => {}
        }
    }

    let next = if truncated { next_token } else { None };
    Ok((out, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use tokio::io::AsyncRead;

    const TEST_REGION: &str = "us-east-1";

    fn test_creds() -> Credentials {
        Credentials {
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
            expires_at: None,
        }
    }

    fn test_cfg() -> S3Config {
        S3Config {
            bucket: "bkt".into(),
            prefix: "p".into(),
            region: TEST_REGION.into(),
            creds: CredentialSource::Static(test_creds()),
            endpoint: None,
            force_path_style: false,
        }
    }

    #[test]
    fn signing_emits_sigv4_headers() {
        // Deterministic time so the scope date is stable; structural wiring
        // here, cryptographic parity in signing_matches_aws_sigv4_golden.
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_440_938_160); // 20150830T123600Z
        let headers = s3_signing_headers(
            &test_creds(),
            TEST_REGION,
            "GET",
            "https://bkt.s3.us-east-1.amazonaws.com/p/a.zst",
            &[],
            time,
        )
        .unwrap();
        let get = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("x-amz-content-sha256"), Some("UNSIGNED-PAYLOAD"));
        assert!(get("x-amz-date").unwrap().starts_with("20150830T123600Z"));
        let auth = get("authorization").expect("authorization header");
        assert!(auth.starts_with("AWS4-HMAC-SHA256 "));
        assert!(auth.contains("Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request"));
        assert!(auth.contains("SignedHeaders=host;"));
        // no session token configured => no security-token header
        assert!(get("x-amz-security-token").is_none());
    }

    #[test]
    fn build_url_bakes_path_and_query() {
        let mut cfg = test_cfg();
        cfg.endpoint = Some("http://127.0.0.1:9000".into());
        cfg.force_path_style = true;
        let s = S3Storage::new(cfg).unwrap();
        let u = s
            .build_url(
                "wal_005/x.zst",
                &[("list-type", "2"), ("continuation-token", "1/a+b=")],
            )
            .unwrap();
        assert_eq!(u.path(), "/bkt/wal_005/x.zst");
        let q = u.query().unwrap();
        assert!(q.contains("list-type=2"), "{q}");
        // reserved chars stay percent-encoded so the signed and wire query match
        assert!(q.contains("continuation-token=1%2Fa%2Bb%3D"), "{q}");
    }

    #[test]
    fn signing_a_url_with_query_succeeds() {
        let headers = s3_signing_headers(
            &test_creds(),
            TEST_REGION,
            "GET",
            "https://bkt.s3.us-east-1.amazonaws.com/?list-type=2&continuation-token=1%2Fa%2Bb%3D",
            &[],
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_440_938_160),
        )
        .unwrap();
        assert!(
            headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        );
    }

    #[test]
    fn signing_includes_session_token() {
        let mut creds = test_creds();
        creds.session_token = Some("FwoTOKEN".into());
        let headers = s3_signing_headers(
            &creds,
            TEST_REGION,
            "GET",
            "https://bkt.s3.us-east-1.amazonaws.com/p/a.zst",
            &[],
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_440_938_160),
        )
        .unwrap();
        let tok = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-security-token"));
        assert_eq!(tok.map(|(_, v)| v.as_str()), Some("FwoTOKEN"));
    }

    #[test]
    fn upload_id_extraction() {
        let xml = "<InitiateMultipartUploadResult><UploadId>abc123</UploadId></InitiateMultipartUploadResult>";
        assert_eq!(first_tag_text(xml, b"UploadId"), Some("abc123".into()));
        assert_eq!(first_tag_text(xml, b"Missing"), None);
    }

    #[test]
    fn list_parses_contents() {
        let xml = r#"<?xml version="1.0"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>p/a.zst</Key><Size>5</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>
  <Contents><Key>p/b.zst</Key><Size>9</Size><LastModified>2026-01-02T00:00:00Z</LastModified></Contents>
</ListBucketResult>"#;
        let (out, next) = parse_list_v2(xml, "p").unwrap();
        assert_eq!(next, None);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].key, "a.zst");
        assert_eq!(out[0].size, 5);
        assert_eq!(out[1].key, "b.zst");
    }

    #[test]
    fn list_returns_continuation_token_when_truncated() {
        let xml = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>1/abc+def=</NextContinuationToken>
  <Contents><Key>p/a.zst</Key><Size>5</Size></Contents>
</ListBucketResult>"#;
        let (out, next) = parse_list_v2(xml, "p").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(next.as_deref(), Some("1/abc+def="));
    }

    #[test]
    fn copy_source_header_encodes_key() {
        assert_eq!(
            copy_source_header("bkt", "p/wal_005/000000010000000000000001.zst"),
            "/bkt/p/wal_005/000000010000000000000001.zst"
        );
        assert_eq!(copy_source_header("bkt", "a b+c"), "/bkt/a%20b%2Bc");
    }

    #[test]
    fn copy_object_result_detects_embedded_error() {
        assert!(copy_object_result("<CopyObjectResult><ETag>x</ETag></CopyObjectResult>").is_ok());
        // whitespace keep-alive prefix before result is fine
        assert!(copy_object_result("\n\n<CopyObjectResult/>").is_ok());
        match copy_object_result("<Error><Code>InternalError</Code></Error>") {
            Err(StorageError::Http { status: 500, .. }) => {}
            other => panic!("expected Http 500, got {:?}", other.err()),
        }
    }

    /// Golden Authorization values captured from `aws-sigv4` before the
    /// hand-rolled signer replaced it; pins byte-for-byte parity across the
    /// header shapes the codebase actually signs (plain, session token,
    /// query, content-type, copy-source).
    #[test]
    fn signing_matches_aws_sigv4_golden() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_440_938_160);
        let auth = |creds: &Credentials, m: &str, u: &str, eh: &[(&str, &str)]| {
            s3_signing_headers(creds, TEST_REGION, m, u, eh, t)
                .unwrap()
                .into_iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
                .map(|(_, v)| v)
                .unwrap()
        };
        let mut tok = test_creds();
        tok.session_token = Some("FwoTOKEN".into());

        let cred = "Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request";
        assert_eq!(
            auth(
                &test_creds(),
                "GET",
                "https://bkt.s3.us-east-1.amazonaws.com/p/a.zst",
                &[]
            ),
            format!(
                "AWS4-HMAC-SHA256 {cred}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=a3fa24177c78f0fe6dde93d8cd7a42c15f618091bcd6ed0d03dbc5f35c877ce6"
            )
        );
        assert_eq!(
            auth(
                &tok,
                "GET",
                "https://bkt.s3.us-east-1.amazonaws.com/p/a.zst",
                &[]
            ),
            format!(
                "AWS4-HMAC-SHA256 {cred}, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=dead50163c66e73ab2ea9b15e088446f9b8d47da20d3b693979f4b894e544b95"
            )
        );
        assert_eq!(
            auth(
                &test_creds(),
                "GET",
                "https://bkt.s3.us-east-1.amazonaws.com/?list-type=2&continuation-token=1%2Fa%2Bb%3D",
                &[],
            ),
            format!(
                "AWS4-HMAC-SHA256 {cred}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=04a8b4b81e2bfb6ad0ad029281c526680284a44464cd804ee38dd84a5ff525b9"
            )
        );
        assert_eq!(
            auth(
                &test_creds(),
                "POST",
                "https://bkt.s3.us-east-1.amazonaws.com/p/a.zst?uploadId=xyz",
                &[("content-type", "application/xml")],
            ),
            format!(
                "AWS4-HMAC-SHA256 {cred}, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=ecdb0664fe05d69c683f8dbec37bf00c1cfc3cdceb59c44b50f586328ed8ee6c"
            )
        );
        assert_eq!(
            auth(
                &test_creds(),
                "PUT",
                "https://bkt.s3.us-east-1.amazonaws.com/p/a.zst",
                &[("x-amz-copy-source", "/bkt/p/b.zst")],
            ),
            format!(
                "AWS4-HMAC-SHA256 {cred}, SignedHeaders=host;x-amz-content-sha256;x-amz-copy-source;x-amz-date, Signature=5ee06fb0e05be8b694bdcb85cea1f7ee0b8ea171fc4a70cb985d8e7b1d06faa1"
            )
        );
    }

    /// Full roundtrip against an in-process mock speaking minimal S3 REST,
    /// reached via the path-style endpoint override. Drives every network op
    /// (single PUT, multipart, get, exists, list pagination, delete, copy)
    /// so the signer + request plumbing actually execute; the mock accepts
    /// the signature blindly.
    #[tokio::test]
    async fn s3_roundtrip_through_mock() {
        use crate::storage::test_http::{
            Req, Resp, drain_keys, payload, pct_decode, read_all, reader, serve,
        };
        use std::collections::BTreeMap;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::{Arc, Mutex};

        type Objects = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;
        type Uploads = Arc<Mutex<BTreeMap<String, BTreeMap<u32, Vec<u8>>>>>;

        let objects: Objects = Arc::new(Mutex::new(BTreeMap::new()));
        let uploads: Uploads = Arc::new(Mutex::new(BTreeMap::new()));
        let upload_seq = Arc::new(AtomicU32::new(0));

        let (o, u, seq) = (objects.clone(), uploads.clone(), upload_seq.clone());
        let base = serve(move |req: &Req| {
            let rest = req.path.trim_start_matches('/');
            let key = rest.split_once('/').map(|(_, k)| k).unwrap_or("");
            match req.method.as_str() {
                "PUT" if req.has_query("partNumber") && req.has_query("uploadId") => {
                    let part: u32 = req.query("partNumber").unwrap().parse().unwrap();
                    // transient on the 2nd part for "boom" keys -> exercises abort
                    if key.contains("boom") && part >= 2 {
                        return Resp::new(503).body(b"<Error/>".to_vec());
                    }
                    let id = req.query("uploadId").unwrap().to_string();
                    u.lock()
                        .unwrap()
                        .entry(id)
                        .or_default()
                        .insert(part, req.body.clone());
                    Resp::new(200).header("etag", &format!("\"etag-{part}\""))
                }
                "PUT" if req.headers.contains_key("x-amz-copy-source") => {
                    let hdr = req.headers.get("x-amz-copy-source").unwrap();
                    let src_key =
                        pct_decode(hdr.trim_start_matches('/').split_once('/').map_or("", |(_, k)| k));
                    let mut objs = o.lock().unwrap();
                    match objs.get(&src_key).cloned() {
                        Some(bytes) => {
                            objs.insert(key.to_string(), bytes);
                            Resp::new(200)
                                .body(b"<CopyObjectResult><ETag>e</ETag></CopyObjectResult>".to_vec())
                        }
                        None => Resp::new(404).body(b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
                    }
                }
                "PUT" => {
                    o.lock().unwrap().insert(key.to_string(), req.body.clone());
                    Resp::new(200)
                }
                "POST" if req.has_query("uploads") => {
                    let n = seq.fetch_add(1, Ordering::SeqCst);
                    let id = format!("upload-{n}");
                    u.lock().unwrap().insert(id.clone(), BTreeMap::new());
                    Resp::new(200).body(
                        format!(
                            "<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
                        )
                        .into_bytes(),
                    )
                }
                "POST" if req.has_query("uploadId") => {
                    let parts = u
                        .lock()
                        .unwrap()
                        .remove(req.query("uploadId").unwrap())
                        .unwrap_or_default();
                    let buf: Vec<u8> = parts.into_values().flatten().collect();
                    o.lock().unwrap().insert(key.to_string(), buf);
                    Resp::new(200).body(b"<CompleteMultipartUploadResult/>".to_vec())
                }
                "GET" if key.is_empty() || req.has_query("list-type") => {
                    let prefix = req.query("prefix").unwrap_or("");
                    let start: usize = req
                        .query("continuation-token")
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(0);
                    let objs = o.lock().unwrap();
                    let matching: Vec<(String, usize)> = objs
                        .iter()
                        .filter(|(k, _)| k.starts_with(prefix))
                        .map(|(k, v)| (k.clone(), v.len()))
                        .collect();
                    const PAGE: usize = 2;
                    let end = (start + PAGE).min(matching.len());
                    let truncated = end < matching.len();
                    let mut xml = String::from("<ListBucketResult>");
                    xml.push_str(if truncated {
                        "<IsTruncated>true</IsTruncated>"
                    } else {
                        "<IsTruncated>false</IsTruncated>"
                    });
                    if truncated {
                        xml.push_str(&format!("<NextContinuationToken>{end}</NextContinuationToken>"));
                    }
                    for (k, len) in &matching[start..end] {
                        xml.push_str(&format!(
                            "<Contents><Key>{k}</Key><Size>{len}</Size>\
                             <LastModified>2026-01-01T00:00:00Z</LastModified></Contents>"
                        ));
                    }
                    xml.push_str("</ListBucketResult>");
                    Resp::new(200).body(xml.into_bytes())
                }
                "GET" => match o.lock().unwrap().get(key) {
                    Some(b) => Resp::new(200).body(b.clone()),
                    None => Resp::new(404).body(b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
                },
                "HEAD" => {
                    if o.lock().unwrap().contains_key(key) {
                        Resp::new(200)
                    } else {
                        Resp::new(404)
                    }
                }
                "DELETE" if req.has_query("uploadId") => {
                    u.lock().unwrap().remove(req.query("uploadId").unwrap());
                    Resp::new(204)
                }
                "DELETE" => {
                    o.lock().unwrap().remove(key);
                    Resp::new(204)
                }
                _ => Resp::new(400),
            }
        })
        .await;

        let cfg = S3Config {
            bucket: "bkt".into(),
            prefix: "p".into(),
            region: "us-east-1".into(),
            creds: CredentialSource::Static(Credentials {
                access_key: "AKID".into(),
                secret_key: "sek".into(),
                session_token: None,
                expires_at: None,
            }),
            endpoint: Some(base),
            force_path_style: true,
        };
        let policy = RetryPolicy {
            max_attempts: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
        };
        let s = S3Storage::with_retry_policy(cfg, policy).unwrap();

        // single PUT (known small size), get, exists, missing
        s.put("a.zst", reader(b"hello"), Some(5)).await.unwrap();
        assert_eq!(read_all(s.get("a.zst").await.unwrap()).await, b"hello");
        assert!(s.exists("a.zst").await.unwrap());
        assert!(!s.exists("nope").await.unwrap());
        assert!(matches!(
            s.get("nope").await,
            Err(StorageError::NotFound(_))
        ));

        // unknown-size small body buffers to a single PUT
        s.put("b.zst", reader(b"world!!"), None).await.unwrap();
        s.put("c.zst", reader(b"three"), Some(5)).await.unwrap();

        // list walks the continuation token (PAGE=2 over 3 objects)
        let mut keys = drain_keys(&s, "").await;
        keys.sort();
        assert_eq!(keys, ["a.zst", "b.zst", "c.zst"]);

        // server-side copy (matching backend id)
        let src = s.copy_source("b.zst").unwrap();
        s.copy_within(&src, "d.zst").await.unwrap();
        assert_eq!(read_all(s.get("d.zst").await.unwrap()).await, b"world!!");

        s.delete("a.zst").await.unwrap();
        assert!(!s.exists("a.zst").await.unwrap());

        // multipart: size_hint over the 32 MiB threshold walks the part loop
        let big = payload(33 * 1024 * 1024);
        s.put("big.zst", reader(&big), Some(big.len() as u64))
            .await
            .unwrap();
        assert_eq!(read_all(s.get("big.zst").await.unwrap()).await, big);

        // transient part failure -> abort_multipart, surfaced error
        let err = s
            .put("boom.zst", reader(&big), Some(big.len() as u64))
            .await;
        assert!(matches!(err, Err(StorageError::Http { status: 503, .. })));
        assert!(uploads.lock().unwrap().is_empty(), "abort must clean up");
    }
}
