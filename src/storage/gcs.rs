//! GCS backend
//!
//! Auth: service-account JSON file pointed to by GOOGLE_APPLICATION_CREDENTIALS
//! Streaming uploads via chunked transfer encoding (uploadType=media)
//!
//! Env: GOOGLE_APPLICATION_CREDENTIALS, WALG_GS_PREFIX (parsed by config layer)

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{KeyPair, RSA_PKCS1_SHA256, RsaKeyPair};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Body, Client};
use rustls_pki_types::PrivateKeyDer;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

use super::{AsyncReader, CopySource, ObjectMeta, ObjectStream, Result, Storage, StorageError};

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const STORAGE_HOST: &str = "https://storage.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

#[derive(Debug, Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: Option<String>,
}

struct CachedToken {
    token: String,
    expires_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub bucket: String,
    pub prefix: String,
    pub credentials_path: Option<String>,
}

pub struct GcsStorage {
    cfg: GcsConfig,
    client: Client,
    host: String,
    /// None in emulator mode (fake-gcs-server): no service account, no oauth2
    sa: Option<ServiceAccount>,
    token: Arc<Mutex<Option<CachedToken>>>,
}

impl GcsStorage {
    pub fn new(cfg: GcsConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| StorageError::Config(e.to_string()))?;

        // Emulator mode: WALG_GS_ENDPOINT / STORAGE_EMULATOR_HOST point at a
        // fake-gcs-server which serves the JSON API over plain HTTP and ignores
        // auth. Skip credentials + the oauth2 token mint entirely.
        if let Some(ep) = std::env::var("WALG_GS_ENDPOINT")
            .or_else(|_| std::env::var("STORAGE_EMULATOR_HOST"))
            .ok()
            .filter(|s| !s.is_empty())
        {
            let host = if ep.starts_with("http://") || ep.starts_with("https://") {
                ep.trim_end_matches('/').to_string()
            } else {
                format!("http://{}", ep.trim_end_matches('/'))
            };
            return Ok(Self {
                cfg,
                client,
                host,
                sa: None,
                token: Arc::new(Mutex::new(None)),
            });
        }

        let path = cfg
            .credentials_path
            .clone()
            .or_else(|| std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok())
            .ok_or_else(|| {
                StorageError::Config(
                    "GOOGLE_APPLICATION_CREDENTIALS not set; metadata-server auth not yet supported"
                        .into(),
                )
            })?;
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| StorageError::Config(format!("read credentials {}: {}", path, e)))?;
        let sa: ServiceAccount = serde_json::from_str(&raw)
            .map_err(|e| StorageError::Config(format!("parse credentials: {e}")))?;
        Ok(Self {
            cfg,
            client,
            host: STORAGE_HOST.to_string(),
            sa: Some(sa),
            token: Arc::new(Mutex::new(None)),
        })
    }

    fn full_key(&self, key: &str) -> String {
        super::join_prefix_key(&self.cfg.prefix, key)
    }

    async fn access_token(&self) -> Result<String> {
        // Emulator mode: fake-gcs-server ignores the bearer token
        let Some(sa) = self.sa.as_ref() else {
            return Ok("emulator".into());
        };
        let mut guard = self.token.lock().await;
        let now = SystemTime::now();
        if let Some(c) = guard.as_ref()
            && c.expires_at > now + Duration::from_secs(60)
        {
            return Ok(c.token.clone());
        }

        let now_secs = now
            .duration_since(UNIX_EPOCH)
            .map_err(|e| StorageError::Auth(e.to_string()))?
            .as_secs();
        let header = serde_json::json!({"alg":"RS256","typ":"JWT"});
        let claims = serde_json::json!({
            "iss": sa.client_email,
            "scope": SCOPE,
            "aud": sa.token_uri.clone().unwrap_or_else(|| TOKEN_URL.into()),
            "iat": now_secs,
            "exp": now_secs + 3600,
        });

        let h_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let c_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
        let signing_input = format!("{h_b64}.{c_b64}");

        let key_pair = parse_pkcs8_or_pkcs1(&sa.private_key)?;
        let rng = SystemRandom::new();
        let mut sig = vec![0u8; key_pair.public_key().modulus_len()];
        key_pair
            .sign(&RSA_PKCS1_SHA256, &rng, signing_input.as_bytes(), &mut sig)
            .map_err(|e| StorageError::Auth(format!("rsa sign: {e}")))?;
        let s_b64 = URL_SAFE_NO_PAD.encode(&sig);
        let jwt = format!("{signing_input}.{s_b64}");

        let token_url = sa.token_uri.as_deref().unwrap_or(TOKEN_URL);
        let resp = self
            .client
            .post(token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", jwt.as_str()),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            let st = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::Auth(format!("token endpoint {st}: {body}")));
        }

        #[derive(Deserialize)]
        struct TokenResp {
            access_token: String,
            expires_in: u64,
        }
        let tr: TokenResp = resp.json().await?;
        let exp = now + Duration::from_secs(tr.expires_in);
        *guard = Some(CachedToken {
            token: tr.access_token.clone(),
            expires_at: exp,
        });
        Ok(tr.access_token)
    }

    fn object_url(&self, key: &str) -> String {
        let full = self.full_key(key);
        let enc = utf8_percent_encode(&full, NON_ALPHANUMERIC);
        format!("{}/storage/v1/b/{}/o/{}", self.host, self.cfg.bucket, enc)
    }

    /// Server-side copy identity: rewriteTo authorizes both sides with one
    /// token, so same service account (or same emulator host) is the safe
    /// equivalence
    fn backend_id(&self) -> String {
        match self.sa.as_ref() {
            Some(sa) => format!("gs:{}", sa.client_email),
            None => format!("gs:emulator:{}", self.host),
        }
    }
}

/// rewriteTo URL; object names percent-encoded as single path segments
fn rewrite_url(
    host: &str,
    src_bucket: &str,
    src_key: &str,
    dst_bucket: &str,
    dst_key: &str,
) -> String {
    format!(
        "{}/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}",
        host,
        src_bucket,
        utf8_percent_encode(src_key, NON_ALPHANUMERIC),
        dst_bucket,
        utf8_percent_encode(dst_key, NON_ALPHANUMERIC),
    )
}

#[async_trait]
impl Storage for GcsStorage {
    fn describe(&self) -> String {
        format!("gs://{}/{}", self.cfg.bucket, self.cfg.prefix)
    }

    async fn put(&self, key: &str, body: AsyncReader, _size_hint: Option<u64>) -> Result<()> {
        let token = self.access_token().await?;
        let full = self.full_key(key);
        let url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.host,
            self.cfg.bucket,
            utf8_percent_encode(&full, NON_ALPHANUMERIC),
        );
        let stream = ReaderStream::new(body);
        let resp = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header("content-type", "application/octet-stream")
            .body(Body::wrap_stream(stream))
            .send()
            .await?;
        if !resp.status().is_success() {
            let st = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::Http {
                status: st.as_u16(),
                body: format!("gcs put: {body}"),
            });
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<AsyncReader> {
        let token = self.access_token().await?;
        let url = format!("{}?alt=media", self.object_url(key));
        let resp = self.client.get(&url).bearer_auth(token).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StorageError::NotFound(key.to_string()));
        }
        if !resp.status().is_success() {
            let st = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::Http {
                status: st.as_u16(),
                body: format!("gcs get: {body}"),
            });
        }
        let stream = resp
            .bytes_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        Ok(Box::pin(tokio_util::io::StreamReader::new(stream)))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let token = self.access_token().await?;
        let resp = self
            .client
            .get(self.object_url(key))
            .bearer_auth(token)
            .send()
            .await?;
        Ok(resp.status().is_success())
    }

    async fn list(&self, prefix: &str) -> Result<ObjectStream> {
        let full = self.full_key(prefix);
        let cfg_prefix = self.cfg.prefix.clone();
        let bucket = self.cfg.bucket.clone();
        let client = self.client.clone();
        let host = self.host.clone();
        let token = self.access_token().await?;

        let s = stream::unfold(
            (
                Some(String::new()),
                full,
                cfg_prefix,
                bucket,
                client,
                host,
                token,
            ),
            |(token_page, prefix, strip, bucket, client, host, auth)| async move {
                let token_page = token_page?;
                let mut url = format!(
                    "{}/storage/v1/b/{}/o?prefix={}",
                    host,
                    bucket,
                    utf8_percent_encode(&prefix, NON_ALPHANUMERIC),
                );
                if !token_page.is_empty() {
                    url.push_str(&format!(
                        "&pageToken={}",
                        utf8_percent_encode(&token_page, NON_ALPHANUMERIC)
                    ));
                }
                let resp = match client.get(&url).bearer_auth(&auth).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        return Some((
                            Err(e.into()),
                            (None, prefix, strip, bucket, client, host, auth),
                        ));
                    }
                };
                if !resp.status().is_success() {
                    let st = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Some((
                        Err(StorageError::Http {
                            status: st.as_u16(),
                            body: format!("gcs list: {body}"),
                        }),
                        (None, prefix, strip, bucket, client, host, auth),
                    ));
                }
                #[derive(Deserialize)]
                struct ListResp {
                    items: Option<Vec<Item>>,
                    #[serde(rename = "nextPageToken")]
                    next_page_token: Option<String>,
                }
                #[derive(Deserialize)]
                struct Item {
                    name: String,
                    #[serde(default)]
                    size: Option<String>,
                    #[serde(default)]
                    updated: Option<String>,
                }
                let lr: ListResp = match resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        return Some((
                            Err(e.into()),
                            (None, prefix, strip, bucket, client, host, auth),
                        ));
                    }
                };
                let items = lr.items.unwrap_or_default();
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    let key = if !strip.is_empty() {
                        it.name
                            .strip_prefix(strip.trim_end_matches('/'))
                            .map(|s| s.trim_start_matches('/').to_string())
                            .unwrap_or(it.name)
                    } else {
                        it.name
                    };
                    let size = it
                        .size
                        .as_deref()
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    let last_modified = it
                        .updated
                        .as_deref()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|d| d.with_timezone(&Utc));
                    out.push(ObjectMeta {
                        key,
                        size,
                        last_modified,
                    });
                }
                let next = lr.next_page_token;
                Some((Ok(out), (next, prefix, strip, bucket, client, host, auth)))
            },
        )
        .flat_map(|res| match res {
            Ok(v) => stream::iter(v.into_iter().map(Ok)).left_stream(),
            Err(e) => stream::iter(std::iter::once(Err(e))).right_stream(),
        });

        Ok(Box::pin(s))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let token = self.access_token().await?;
        let resp = self
            .client
            .delete(self.object_url(key))
            .bearer_auth(token)
            .send()
            .await?;
        let st = resp.status();
        if st.is_success() || st == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(StorageError::Http {
                status: st.as_u16(),
                body: format!("gcs delete: {body}"),
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
        let token = self.access_token().await?;
        let base = rewrite_url(
            &self.host,
            &src.bucket,
            &src.key,
            &self.cfg.bucket,
            &self.full_key(dst_key),
        );
        // large objects rewrite in chunks; loop until done
        let mut rewrite_token: Option<String> = None;
        loop {
            let url = match &rewrite_token {
                Some(t) => format!(
                    "{}?rewriteToken={}",
                    base,
                    utf8_percent_encode(t, NON_ALPHANUMERIC)
                ),
                None => base.clone(),
            };
            let resp = self.client.post(&url).bearer_auth(&token).send().await?;
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(StorageError::NotFound(src.key.clone()));
            }
            if !resp.status().is_success() {
                let st = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(StorageError::Http {
                    status: st.as_u16(),
                    body: format!("gcs rewrite: {body}"),
                });
            }
            #[derive(Deserialize)]
            struct RewriteResp {
                done: bool,
                #[serde(rename = "rewriteToken")]
                rewrite_token: Option<String>,
            }
            let rr: RewriteResp = resp.json().await?;
            if rr.done {
                return Ok(());
            }
            if rr.rewrite_token.is_none() {
                return Err(StorageError::InvalidResponse(
                    "rewrite not done, no rewriteToken".into(),
                ));
            }
            rewrite_token = rr.rewrite_token;
        }
    }
}

fn parse_pkcs8_or_pkcs1(pem: &str) -> Result<RsaKeyPair> {
    // service account keys come as PKCS#8 PEM by default; rustls-pemfile labels
    // the section so we route to the matching aws-lc-rs constructor
    let mut reader = std::io::Cursor::new(pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| StorageError::Auth(format!("read private key: {e}")))?
        .ok_or_else(|| StorageError::Auth("no private key in credentials".into()))?;
    match key {
        PrivateKeyDer::Pkcs8(der) => RsaKeyPair::from_pkcs8(der.secret_pkcs8_der()),
        PrivateKeyDer::Pkcs1(der) => RsaKeyPair::from_der(der.secret_pkcs1_der()),
        other => {
            return Err(StorageError::Auth(format!(
                "unsupported private key format: {other:?}"
            )));
        }
    }
    .map_err(|e| StorageError::Auth(format!("rsa key parse: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_key_reads_pkcs8_armor() {
        // rustls-pemfile decodes the base64 body and tags the section kind; the
        // 3-byte payload isn't a real key, only the armor handling is exercised
        let pem = "-----BEGIN PRIVATE KEY-----\nAAEC\n-----END PRIVATE KEY-----\n";
        let mut rd = std::io::Cursor::new(pem.as_bytes());
        match rustls_pemfile::private_key(&mut rd).unwrap().unwrap() {
            PrivateKeyDer::Pkcs8(der) => assert_eq!(der.secret_pkcs8_der(), &[0, 1, 2]),
            other => panic!("expected pkcs8, got {other:?}"),
        }
    }

    #[test]
    fn rewrite_url_encodes_object_names() {
        let url = rewrite_url(
            STORAGE_HOST,
            "src-b",
            "p/wal_005/x.zst",
            "dst-b",
            "q/wal_005/x.zst",
        );
        assert_eq!(
            url,
            format!(
                "{STORAGE_HOST}/storage/v1/b/src-b/o/p%2Fwal%5F005%2Fx%2Ezst\
                 /rewriteTo/b/dst-b/o/q%2Fwal%5F005%2Fx%2Ezst"
            )
        );
    }

    #[test]
    fn emulator_endpoint_overrides_host_and_skips_auth() {
        // set_var unsafe in edition 2024; this test mutates process env so it
        // must not run concurrently with other gcs env readers — there are none
        unsafe {
            std::env::set_var("WALG_GS_ENDPOINT", "http://127.0.0.1:4443");
        }
        let s = GcsStorage::new(GcsConfig {
            bucket: "b".into(),
            prefix: "p".into(),
            credentials_path: None,
        })
        .expect("emulator mode needs no credentials");
        unsafe {
            std::env::remove_var("WALG_GS_ENDPOINT");
        }
        assert!(s.sa.is_none());
        assert_eq!(s.host, "http://127.0.0.1:4443");
        assert!(
            s.object_url("wal_005/x")
                .starts_with("http://127.0.0.1:4443/")
        );
    }

    /// Emulator-mode GcsStorage (sa = None) built directly against an
    /// in-process mock of the GCS JSON API; drives put/get/exists/list/
    /// delete/rewrite without env or credentials.
    fn emulator(host: String) -> GcsStorage {
        GcsStorage {
            cfg: GcsConfig {
                bucket: "b".into(),
                prefix: "p".into(),
                credentials_path: None,
            },
            client: Client::builder().build().unwrap(),
            host,
            sa: None,
            token: Arc::new(Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn gcs_emulator_roundtrip() {
        use crate::storage::test_http::{
            Req, Resp, drain_keys, pct_decode, read_all, reader, serve,
        };
        use std::collections::BTreeMap;

        let objects = Arc::new(std::sync::Mutex::new(BTreeMap::<String, Vec<u8>>::new()));
        let o = objects.clone();
        let base = serve(move |req: &Req| {
            let p = req.path.as_str();
            match req.method.as_str() {
                "POST" if p.starts_with("/upload/storage/v1/") => {
                    let name = pct_decode(req.query("name").unwrap_or(""));
                    o.lock().unwrap().insert(name, req.body.clone());
                    Resp::new(200).body(b"{}".to_vec())
                }
                "POST" if p.contains("/rewriteTo/") => {
                    let (left, right) = p.split_once("/rewriteTo/").unwrap();
                    let src_key = pct_decode(left.rsplit("/o/").next().unwrap_or(""));
                    let dst_key = pct_decode(right.rsplit("/o/").next().unwrap_or(""));
                    let mut objs = o.lock().unwrap();
                    if !objs.contains_key(&src_key) {
                        return Resp::new(404).body(b"{\"error\":{\"code\":404}}".to_vec());
                    }
                    // exercise the multi-step rewrite loop for "multi" targets
                    if dst_key.contains("multi") && !req.has_query("rewriteToken") {
                        return Resp::new(200)
                            .body(b"{\"done\":false,\"rewriteToken\":\"tok1\"}".to_vec());
                    }
                    let bytes = objs.get(&src_key).cloned().unwrap();
                    objs.insert(dst_key, bytes);
                    Resp::new(200).body(b"{\"done\":true}".to_vec())
                }
                "GET" if p.ends_with("/o") => {
                    let prefix = pct_decode(req.query("prefix").unwrap_or(""));
                    let start: usize =
                        req.query("pageToken").and_then(|t| t.parse().ok()).unwrap_or(0);
                    let objs = o.lock().unwrap();
                    let matching: Vec<(String, usize)> = objs
                        .iter()
                        .filter(|(k, _)| k.starts_with(&prefix))
                        .map(|(k, v)| (k.clone(), v.len()))
                        .collect();
                    const PAGE: usize = 2;
                    let end = (start + PAGE).min(matching.len());
                    let mut json = String::from("{\"items\":[");
                    for (i, (k, len)) in matching[start..end].iter().enumerate() {
                        if i > 0 {
                            json.push(',');
                        }
                        json.push_str(&format!(
                            "{{\"name\":\"{k}\",\"size\":\"{len}\",\"updated\":\"2026-01-01T00:00:00Z\"}}"
                        ));
                    }
                    json.push(']');
                    if end < matching.len() {
                        json.push_str(&format!(",\"nextPageToken\":\"{end}\""));
                    }
                    json.push('}');
                    Resp::new(200).body(json.into_bytes())
                }
                "GET" => {
                    let key = pct_decode(p.rsplit("/o/").next().unwrap_or(""));
                    match o.lock().unwrap().get(&key) {
                        Some(b) => Resp::new(200).body(b.clone()),
                        None => Resp::new(404).body(b"{\"error\":{\"code\":404}}".to_vec()),
                    }
                }
                "DELETE" => {
                    let key = pct_decode(p.rsplit("/o/").next().unwrap_or(""));
                    if o.lock().unwrap().remove(&key).is_some() {
                        Resp::new(204)
                    } else {
                        Resp::new(404)
                    }
                }
                _ => Resp::new(400),
            }
        })
        .await;

        let s = emulator(base);
        s.put("a.zst", reader(b"hello"), None).await.unwrap();
        assert_eq!(read_all(s.get("a.zst").await.unwrap()).await, b"hello");
        assert!(s.exists("a.zst").await.unwrap());
        assert!(!s.exists("nope").await.unwrap());
        assert!(matches!(
            s.get("nope").await,
            Err(StorageError::NotFound(_))
        ));

        s.put("b.zst", reader(b"world!!"), None).await.unwrap();
        s.put("c.zst", reader(b"three"), None).await.unwrap();
        let mut keys = drain_keys(&s, "").await;
        keys.sort();
        assert_eq!(keys, ["a.zst", "b.zst", "c.zst"]);

        let src = s.copy_source("b.zst").unwrap();
        s.copy_within(&src, "d.zst").await.unwrap();
        assert_eq!(read_all(s.get("d.zst").await.unwrap()).await, b"world!!");

        // multi-step rewrite (done=false + rewriteToken, then done=true)
        let src2 = s.copy_source("c.zst").unwrap();
        s.copy_within(&src2, "multi.zst").await.unwrap();
        assert_eq!(read_all(s.get("multi.zst").await.unwrap()).await, b"three");

        s.delete("a.zst").await.unwrap();
        assert!(!s.exists("a.zst").await.unwrap());
        // delete missing is a no-op success (404 treated as ok)
        s.delete("ghost").await.unwrap();
    }

    /// Generate a throwaway 2048-bit RSA key via openssl; None when openssl
    /// is unavailable so local runs without it skip rather than fail (the
    /// coverage CI lane has openssl)
    fn openssl_rsa_key() -> Option<String> {
        let out = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
            ])
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8(out.stdout).ok())
            .flatten()
    }

    #[tokio::test]
    async fn access_token_mints_jwt_and_caches() {
        use crate::storage::test_http::{Resp, serve};
        use std::sync::atomic::{AtomicU32, Ordering};

        let Some(pem) = openssl_rsa_key() else {
            eprintln!("skip access_token_mints_jwt_and_caches: openssl unavailable");
            return;
        };

        let hits = Arc::new(AtomicU32::new(0));
        let h = hits.clone();
        let token_base = serve(move |_req| {
            h.fetch_add(1, Ordering::SeqCst);
            Resp::new(200).body(b"{\"access_token\":\"tok123\",\"expires_in\":3600}".to_vec())
        })
        .await;

        let s = GcsStorage {
            cfg: GcsConfig {
                bucket: "b".into(),
                prefix: "p".into(),
                credentials_path: None,
            },
            client: Client::builder().build().unwrap(),
            host: "http://127.0.0.1:1".into(), // unused: access_token only hits token_uri
            sa: Some(ServiceAccount {
                client_email: "svc@test.iam.gserviceaccount.com".into(),
                private_key: pem,
                token_uri: Some(format!("{token_base}/token")),
            }),
            token: Arc::new(Mutex::new(None)),
        };

        assert_eq!(s.access_token().await.unwrap(), "tok123");
        // second call rides the in-memory cache, no fresh mint
        assert_eq!(s.access_token().await.unwrap(), "tok123");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "second token request must hit the cache"
        );
    }
}
