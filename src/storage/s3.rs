//! S3 backend with hand-rolled SigV4
//!
//! UNSIGNED-PAYLOAD on HTTPS so we don't buffer or hash request bodies
//!
//! Env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
//! AWS_REGION (default us-east-1), AWS_ENDPOINT_URL or WALG_S3_ENDPOINT,
//! WALG_S3_FORCE_PATH_STYLE

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

use super::{AsyncReader, CopySource, ObjectMeta, ObjectStream, Result, Storage, StorageError};
use crate::retry::{RetryPolicy, with_retry};

type HmacSha256 = Hmac<Sha256>;

const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
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
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
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
        if self.cfg.prefix.is_empty() {
            key.to_string()
        } else {
            format!(
                "{}/{}",
                self.cfg.prefix.trim_end_matches('/'),
                key.trim_start_matches('/')
            )
        }
    }

    /// Server-side copy identity: same endpoint/region + same credential.
    /// Conservative: AWS allows cross-region CopyObject, but mismatched
    /// region ids fall back to stream-through rather than risk custom
    /// endpoints (minio, ceph) that don't
    fn backend_id(&self) -> String {
        format!(
            "s3:{}:{}",
            self.cfg.endpoint.as_deref().unwrap_or(&self.cfg.region),
            self.cfg.access_key,
        )
    }

    fn host(&self) -> String {
        // host header excludes scheme and port=443/80
        host_from_base(&self.base)
    }

    async fn signed_request(
        &self,
        method: &str,
        key_path: &str,
        query: &[(&str, &str)],
        body: Bytes,
        extra_headers: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let url = if key_path.is_empty() {
            self.base.clone()
        } else {
            format!(
                "{}/{}",
                self.base,
                utf8_percent_encode(key_path, PATH_ENCODE)
            )
        };
        let host = self.host();
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_only = now.format("%Y%m%d").to_string();

        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host.clone()),
            (
                "x-amz-content-sha256".to_string(),
                UNSIGNED_PAYLOAD.to_string(),
            ),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(t) = &self.cfg.session_token {
            headers.push(("x-amz-security-token".to_string(), t.clone()));
        }
        for (k, v) in extra_headers {
            headers.push((k.to_lowercase(), v.to_string()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_query = canonical_query(query);
        // Sign the full request path. With a custom endpoint the bucket lives in
        // `base` (path-style: http://host/bucket), so it must appear in the
        // canonical path or the server-recomputed signature won't match.
        let base_path = url::Url::parse(&self.base)
            .ok()
            .map(|u| u.path().trim_end_matches('/').to_string())
            .unwrap_or_default();
        let canonical_path = canonical_path(&base_path, key_path);
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
            .collect::<String>();

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_path,
            canonical_query,
            canonical_headers,
            signed_headers,
            UNSIGNED_PAYLOAD,
        );

        let scope = format!("{}/{}/s3/aws4_request", date_only, self.cfg.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes())),
        );

        let signing_key =
            derive_signing_key(&self.cfg.secret_key, &date_only, &self.cfg.region, "s3");
        let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders={},Signature={}",
            self.cfg.access_key, scope, signed_headers, signature,
        );

        let mut req = self
            .client
            .request(
                method
                    .parse()
                    .map_err(|_| StorageError::Config(format!("bad method {method}")))?,
                &url,
            )
            .header("authorization", auth);
        for (k, v) in &headers {
            // host is set automatically by reqwest, skip
            if k == "host" {
                continue;
            }
            req = req.header(k.as_str(), v.as_str());
        }
        if !query.is_empty() {
            req = req.query(query);
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
        let upload_id = extract_xml_tag(&init_body, "UploadId").ok_or_else(|| {
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
        let single = match size_hint {
            Some(s) if s <= MULTIPART_THRESHOLD => true,
            None => false,
            _ => false,
        };
        if single {
            // small known-size body: buffer & single PUT
            let mut buf = Vec::new();
            body.read_to_end(&mut buf).await?;
            self.put_single(key, Bytes::from(buf)).await
        } else {
            self.put_multipart(key, body).await
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
                let mut q: Vec<(&str, &str)> =
                    vec![("list-type", "2"), ("prefix", prefix.as_str())];
                if !token.is_empty() {
                    q.push(("continuation-token", token.as_str()));
                }
                let resp = match s.signed_request("GET", "", &q, Bytes::new(), &[]).await {
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
                let (objects, next) = parse_list_v2(&body, &cfg.prefix);
                let next_token = if next.is_some() { next } else { None };
                let next_state = (next_token, prefix, cfg, client, base);
                Some((Ok(objects), next_state))
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

fn host_from_base(base: &str) -> String {
    let url = url::Url::parse(base).unwrap();
    match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => String::new(),
    }
}

fn canonical_path(base_path: &str, key_path: &str) -> String {
    if key_path.is_empty() {
        if base_path.is_empty() {
            "/".into()
        } else {
            base_path.to_string()
        }
    } else {
        format!(
            "{}/{}",
            base_path,
            utf8_percent_encode(key_path, PATH_ENCODE)
        )
    }
}

fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| {
            (
                utf8_percent_encode(k, QUERY_ENCODE).to_string(),
                utf8_percent_encode(v, QUERY_ENCODE).to_string(),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
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

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let i = xml.find(&open)? + open.len();
    let j = xml[i..].find(&close)?;
    Some(xml[i..i + j].to_string())
}

fn parse_list_v2(xml: &str, strip_prefix: &str) -> (Vec<ObjectMeta>, Option<String>) {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(start) = xml[cursor..].find("<Contents>") {
        let s = cursor + start;
        let Some(end_rel) = xml[s..].find("</Contents>") else {
            break;
        };
        let end = s + end_rel;
        let block = &xml[s..end];
        let key = extract_xml_tag(block, "Key").unwrap_or_default();
        let size: u64 = extract_xml_tag(block, "Size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let last_modified = extract_xml_tag(block, "LastModified")
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|d| d.with_timezone(&Utc));
        let trimmed = if !strip_prefix.is_empty() {
            key.strip_prefix(strip_prefix.trim_end_matches('/'))
                .map(|s| s.trim_start_matches('/').to_string())
                .unwrap_or(key)
        } else {
            key
        };
        out.push(ObjectMeta {
            key: trimmed,
            size,
            last_modified,
        });
        cursor = end;
    }
    let truncated = extract_xml_tag(xml, "IsTruncated")
        .map(|s| s == "true")
        .unwrap_or(false);
    let next = if truncated {
        extract_xml_tag(xml, "NextContinuationToken")
    } else {
        None
    };
    (out, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use tokio::io::AsyncRead;

    #[test]
    fn signing_key_derivation_matches_aws_sample() {
        // sample from AWS SigV4 docs
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        let expected =
            hex::decode("c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9")
                .unwrap();
        assert_eq!(key, expected);
    }

    #[test]
    fn xml_extraction() {
        let xml = "<UploadId>abc123</UploadId><Foo>bar</Foo>";
        assert_eq!(extract_xml_tag(xml, "UploadId"), Some("abc123".into()));
        assert_eq!(extract_xml_tag(xml, "Missing"), None);
    }

    #[test]
    fn list_parses_contents() {
        let xml = r#"<?xml version="1.0"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>p/a.zst</Key><Size>5</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>
  <Contents><Key>p/b.zst</Key><Size>9</Size><LastModified>2026-01-02T00:00:00Z</LastModified></Contents>
</ListBucketResult>"#;
        let (out, next) = parse_list_v2(xml, "p");
        assert_eq!(next, None);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].key, "a.zst");
        assert_eq!(out[0].size, 5);
        assert_eq!(out[1].key, "b.zst");
    }

    #[test]
    fn canonical_query_is_sorted() {
        let q = canonical_query(&[("b", "1"), ("a", "2")]);
        assert_eq!(q, "a=2&b=1");
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
}
