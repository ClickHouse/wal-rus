//! GCS backend
//!
//! Auth: service-account JSON file pointed to by GOOGLE_APPLICATION_CREDENTIALS
//! when set, else the GCE/GKE metadata server (the VM's attached service
//! account). Uploads use uploadType=media. Buffer bodies up to
//! BUFFERED_UPLOAD_LIMIT so failed uploads can be retried. Stream larger
//! bodies via chunked transfer encoding in one attempt
//!
//! Env: GOOGLE_APPLICATION_CREDENTIALS, WALG_GS_PREFIX (parsed by config layer)

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{KeyPair, RSA_PKCS1_SHA256, RsaKeyPair};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::{StreamExt, TryStreamExt, stream};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Body, Client, Method, RequestBuilder, Response, StatusCode};
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::pem::{self, PemObject};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

use super::{
    AsyncReader, CopySource, ObjectMeta, ObjectStream, PutIfAbsentOutcome, Result, Storage,
    StorageError, UploadBody, is_transient_status,
};
use crate::retry::{RetryPolicy, with_retry};

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const STORAGE_HOST: &str = "https://storage.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const METADATA_HOST: &str = "http://metadata.google.internal";
const METADATA_TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
/// Match S3's single PUT limit
const BUFFERED_UPLOAD_LIMIT: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
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
    /// Emulator endpoint (fake-gcs-server); resolved from `WALG_GS_ENDPOINT` /
    /// `STORAGE_EMULATOR_HOST` in the config layer. `Some` disables auth
    pub endpoint: Option<String>,
}

#[derive(Clone)]
enum TokenSource {
    /// fake-gcs-server (emulator): no auth at all
    None,
    /// Service-account key file: self-signed JWT exchanged at its token endpoint
    Static(ServiceAccount),
    /// GCE/GKE instance metadata server.
    Imds { endpoint: String, client: Client },
}

#[derive(Clone)]
pub struct GcsStorage {
    cfg: GcsConfig,
    client: Client,
    host: String,
    token_source: TokenSource,
    token: Arc<Mutex<Option<CachedToken>>>,
    retry_policy: RetryPolicy,
}

impl GcsStorage {
    pub fn new(cfg: GcsConfig) -> Result<Self> {
        Self::with_retry_policy(cfg, RetryPolicy::default())
    }

    pub fn with_retry_policy(cfg: GcsConfig, retry_policy: RetryPolicy) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(crate::storage::CONNECT_TIMEOUT)
            .read_timeout(crate::storage::READ_TIMEOUT)
            .build()
            .map_err(|e| StorageError::Config(e.to_string()))?;

        // Emulator mode: a fake-gcs-server endpoint serves the JSON API over
        // plain HTTP and ignores auth. Skip credentials + the oauth2 token mint
        // entirely.
        if let Some(ep) = cfg.endpoint.as_deref().filter(|s| !s.is_empty()) {
            let host = if ep.starts_with("http://") || ep.starts_with("https://") {
                ep.trim_end_matches('/').to_string()
            } else {
                format!("http://{}", ep.trim_end_matches('/'))
            };
            return Ok(Self {
                cfg,
                client,
                host,
                token_source: TokenSource::None,
                token: Arc::new(Mutex::new(None)),
                retry_policy,
            });
        }

        // No key file: fall back to the metadata server, mirroring the S3
        // "no static keys -> IMDS" default
        let token_source = match cfg.credentials_path.as_deref().filter(|s| !s.is_empty()) {
            Some(path) => {
                let raw = std::fs::read_to_string(path)
                    .map_err(|e| StorageError::Config(format!("read credentials {path}: {e}")))?;
                let sa: ServiceAccount = serde_json::from_str(&raw)
                    .map_err(|e| StorageError::Config(format!("parse credentials: {e}")))?;
                TokenSource::Static(sa)
            }
            None => imds_source(METADATA_HOST.to_string())?,
        };
        Ok(Self {
            cfg,
            client,
            host: STORAGE_HOST.to_string(),
            token_source,
            token: Arc::new(Mutex::new(None)),
            retry_policy,
        })
    }

    fn full_key(&self, key: &str) -> String {
        super::join_prefix_key(&self.cfg.prefix, key)
    }

    async fn access_token(&self) -> Result<String> {
        // Emulator mode: fake-gcs-server ignores the bearer token
        if matches!(self.token_source, TokenSource::None) {
            return Ok("emulator".into());
        }
        let mut guard = self.token.lock().await;
        let now = SystemTime::now();
        if let Some(c) = guard.as_ref()
            && c.expires_at > now + Duration::from_secs(60)
        {
            return Ok(c.token.clone());
        }

        // Both sources answer with the same OAuth2 token document
        let req = match &self.token_source {
            TokenSource::None => unreachable!("handled above"),
            TokenSource::Static(sa) => self.jwt_bearer_request(sa, now)?,
            TokenSource::Imds { endpoint, client } => client
                .get(format!("{endpoint}{METADATA_TOKEN_PATH}"))
                .header("Metadata-Flavor", "Google"),
        };
        let resp = req.send().await?;

        let st = resp.status();
        if is_transient_status(st.as_u16()) {
            return Err(http_error(resp, "token endpoint").await);
        }
        if !st.is_success() {
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

    /// JWT-bearer assertion signed with the service-account key
    fn jwt_bearer_request(
        &self,
        sa: &ServiceAccount,
        now: SystemTime,
    ) -> Result<reqwest::RequestBuilder> {
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
        Ok(self.client.post(token_url).form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", jwt.as_str()),
        ]))
    }

    /// Fetch token on each attempt so token endpoint failures can be retried too
    async fn request(&self, method: Method, url: &str) -> Result<RequestBuilder> {
        let token = self.access_token().await?;
        Ok(self.client.request(method, url).bearer_auth(token))
    }

    fn upload_url(&self, key: &str, precondition: &str) -> String {
        format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media{precondition}&name={}",
            self.host,
            self.cfg.bucket,
            utf8_percent_encode(&self.full_key(key), NON_ALPHANUMERIC),
        )
    }

    /// Retry temporary failures for buffered bodies; return other responses as is
    async fn upload(&self, url: &str, body: UploadBody) -> Result<Response> {
        let send = |body: Body| async move {
            let resp = self
                .request(Method::POST, url)
                .await?
                .header("content-type", "application/octet-stream")
                .body(body)
                .send()
                .await?;
            if is_transient_status(resp.status().as_u16()) {
                return Err(http_error(resp, "gcs upload").await);
            }
            Ok(resp)
        };
        match body {
            UploadBody::Buffered(bytes) => {
                with_retry(&self.retry_policy, StorageError::is_transient, || {
                    send(Body::from(bytes.clone()))
                })
                .await
            }
            UploadBody::Stream(r) => send(Body::wrap_stream(ReaderStream::new(r))).await,
        }
    }

    async fn list_page(
        &self,
        prefix: &str,
        page: &str,
    ) -> Result<(Vec<ObjectMeta>, Option<String>)> {
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
        let mut url = format!(
            "{}/storage/v1/b/{}/o?prefix={}",
            self.host,
            self.cfg.bucket,
            utf8_percent_encode(prefix, NON_ALPHANUMERIC),
        );
        if !page.is_empty() {
            url.push_str(&format!(
                "&pageToken={}",
                utf8_percent_encode(page, NON_ALPHANUMERIC)
            ));
        }
        let lr: ListResp = with_retry(&self.retry_policy, StorageError::is_transient, || async {
            let resp = self.request(Method::GET, &url).await?.send().await?;
            Ok(check_status(resp, "gcs list").await?.json().await?)
        })
        .await?;
        let strip = &self.cfg.prefix;
        let items = lr
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|it| ObjectMeta {
                key: match it.name.strip_prefix(strip.trim_end_matches('/')) {
                    Some(rest) if !strip.is_empty() => rest.trim_start_matches('/').to_string(),
                    _ => it.name,
                },
                size: it.size.and_then(|s| s.parse().ok()).unwrap_or(0),
                last_modified: it.updated.and_then(|s| s.parse().ok()),
            });
        Ok((items.collect(), lr.next_page_token))
    }

    fn object_url(&self, key: &str) -> String {
        let full = self.full_key(key);
        let enc = utf8_percent_encode(&full, NON_ALPHANUMERIC);
        format!("{}/storage/v1/b/{}/o/{}", self.host, self.cfg.bucket, enc)
    }

    /// Server-side copy identity: rewriteTo authorizes both sides with one
    /// token, so same service account (or same emulator host) is the safe
    /// equivalence. IMDS is one identity per host
    fn backend_id(&self) -> String {
        match &self.token_source {
            TokenSource::Static(sa) => format!("gs:{}", sa.client_email),
            TokenSource::Imds { .. } => "gs:imds".into(),
            TokenSource::None => format!("gs:emulator:{}", self.host),
        }
    }
}

fn imds_source(endpoint: String) -> Result<TokenSource> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(1))
        .no_proxy()
        .build()
        .map_err(|e| StorageError::Config(format!("metadata client: {e}")))?;
    Ok(TokenSource::Imds { endpoint, client })
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

async fn http_error(resp: Response, what: &str) -> StorageError {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    StorageError::Http {
        status,
        body: format!("{what}: {body}"),
    }
}

async fn check_status(resp: Response, what: &str) -> Result<Response> {
    if resp.status().is_success() {
        Ok(resp)
    } else {
        Err(http_error(resp, what).await)
    }
}

#[async_trait]
impl Storage for GcsStorage {
    fn describe(&self) -> String {
        format!("gs://{}/{}", self.cfg.bucket, self.cfg.prefix)
    }

    async fn put(&self, key: &str, body: AsyncReader, size_hint: Option<u64>) -> Result<()> {
        let body = UploadBody::read(body, size_hint, BUFFERED_UPLOAD_LIMIT).await?;
        let resp = self.upload(&self.upload_url(key, ""), body).await?;
        check_status(resp, "gcs put").await.map(drop)
    }

    async fn put_if_absent(
        &self,
        key: &str,
        body: AsyncReader,
        size_hint: Option<u64>,
    ) -> Result<PutIfAbsentOutcome> {
        // ifGenerationMatch=0 returns 412 when an object already exists, including
        // when an earlier attempt wrote it but lost its response
        let body = UploadBody::read(body, size_hint, BUFFERED_UPLOAD_LIMIT).await?;
        let url = self.upload_url(key, "&ifGenerationMatch=0");
        let resp = self.upload(&url, body).await?;
        match resp.status() {
            st if st.is_success() => Ok(PutIfAbsentOutcome::Created),
            StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => {
                Ok(PutIfAbsentOutcome::AlreadyExists)
            }
            _ => Err(http_error(resp, "gcs put_if_absent").await),
        }
    }

    async fn get(&self, key: &str) -> Result<AsyncReader> {
        let url = format!("{}?alt=media", self.object_url(key));
        let resp = with_retry(&self.retry_policy, StorageError::is_transient, || async {
            let resp = self.request(Method::GET, &url).await?.send().await?;
            if resp.status() == StatusCode::NOT_FOUND {
                return Err(StorageError::NotFound(key.to_string()));
            }
            check_status(resp, "gcs get").await
        })
        .await?;
        let stream = resp
            .bytes_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        Ok(Box::pin(tokio_util::io::StreamReader::new(stream)))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let url = self.object_url(key);
        with_retry(&self.retry_policy, StorageError::is_transient, || async {
            let resp = self.request(Method::GET, &url).await?.send().await?;
            if resp.status() == StatusCode::NOT_FOUND {
                return Ok(false);
            }
            check_status(resp, "gcs exists").await.map(|_| true)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<ObjectStream> {
        let s = stream::unfold(
            (Some(String::new()), self.clone(), self.full_key(prefix)),
            |(page, gcs, prefix)| async move {
                match gcs.list_page(&prefix, &page?).await {
                    Ok((items, next)) => Some((Ok(items), (next, gcs, prefix))),
                    Err(e) => Some((Err(e), (None, gcs, prefix))),
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
        let url = self.object_url(key);
        with_retry(&self.retry_policy, StorageError::is_transient, || async {
            let resp = self.request(Method::DELETE, &url).await?.send().await?;
            if resp.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }
            check_status(resp, "gcs delete").await.map(drop)
        })
        .await
    }

    fn copy_source(&self, key: &str) -> Option<CopySource> {
        Some(CopySource {
            backend: self.backend_id(),
            bucket: self.cfg.bucket.clone(),
            key: self.full_key(key),
        })
    }

    async fn copy_within(&self, src: &CopySource, dst_key: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct RewriteResp {
            done: bool,
            #[serde(rename = "rewriteToken")]
            rewrite_token: Option<String>,
        }
        if src.backend != self.backend_id() {
            return Err(StorageError::Unimplemented("copy_within backend mismatch"));
        }
        let base = rewrite_url(
            &self.host,
            &src.bucket,
            &src.key,
            &self.cfg.bucket,
            &self.full_key(dst_key),
        );
        // GCS copies large objects in steps; retry each step with same rewriteToken
        let mut url = base.clone();
        loop {
            let rr: RewriteResp =
                with_retry(&self.retry_policy, StorageError::is_transient, || async {
                    let resp = self.request(Method::POST, &url).await?.send().await?;
                    if resp.status() == StatusCode::NOT_FOUND {
                        return Err(StorageError::NotFound(src.key.clone()));
                    }
                    Ok(check_status(resp, "gcs rewrite").await?.json().await?)
                })
                .await?;
            if rr.done {
                return Ok(());
            }
            let Some(t) = rr.rewrite_token else {
                return Err(StorageError::InvalidResponse(
                    "rewrite not done, no rewriteToken".into(),
                ));
            };
            url = format!(
                "{base}?rewriteToken={}",
                utf8_percent_encode(&t, NON_ALPHANUMERIC)
            );
        }
    }
}

fn parse_pkcs8_or_pkcs1(pem: &str) -> Result<RsaKeyPair> {
    // service account keys come as PKCS#8 PEM by default; the PEM decoder labels
    // the section so we route to the matching aws-lc-rs constructor
    let key = PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(|e| match e {
        pem::Error::NoItemsFound => StorageError::Auth("no private key in credentials".into()),
        e => StorageError::Auth(format!("read private key: {e}")),
    })?;
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
        // The PEM decoder decodes the base64 body and tags the section kind; the
        // 3-byte payload isn't a real key, only the armor handling is exercised
        let pem = "-----BEGIN PRIVATE KEY-----\nAAEC\n-----END PRIVATE KEY-----\n";
        match PrivateKeyDer::from_pem_slice(pem.as_bytes()).unwrap() {
            PrivateKeyDer::Pkcs8(der) => assert_eq!(der.secret_pkcs8_der(), &[0, 1, 2]),
            other => panic!("expected pkcs8, got {other:?}"),
        }
    }

    #[test]
    fn private_key_missing_is_auth_error() {
        let pem = "-----BEGIN CERTIFICATE-----\nAAEC\n-----END CERTIFICATE-----\n";
        let err = parse_pkcs8_or_pkcs1(pem).err().unwrap();
        assert!(
            err.to_string().contains("no private key in credentials"),
            "{err}"
        );
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
        let s = GcsStorage::new(GcsConfig {
            bucket: "b".into(),
            prefix: "p".into(),
            credentials_path: None,
            endpoint: Some("http://127.0.0.1:4443".into()),
        })
        .expect("emulator mode needs no credentials");
        assert!(matches!(s.token_source, TokenSource::None));
        assert_eq!(s.host, "http://127.0.0.1:4443");
        assert!(
            s.object_url("wal_005/x")
                .starts_with("http://127.0.0.1:4443/")
        );
    }

    #[test]
    fn no_credentials_falls_back_to_metadata_server() {
        for credentials_path in [None, Some(String::new())] {
            let s = GcsStorage::new(GcsConfig {
                bucket: "b".into(),
                prefix: "p".into(),
                credentials_path,
                endpoint: None,
            })
            .expect("no key file selects metadata auth");
            assert!(
                matches!(&s.token_source, TokenSource::Imds { endpoint, .. } if endpoint == METADATA_HOST)
            );
            assert_eq!(s.host, STORAGE_HOST);
            assert_eq!(s.backend_id(), "gs:imds");
        }
    }

    #[tokio::test]
    async fn access_token_from_metadata_server_and_caches() {
        use crate::storage::test_http::{Resp, serve};
        use std::sync::atomic::{AtomicU32, Ordering};

        let hits = Arc::new(AtomicU32::new(0));
        let h = hits.clone();
        let base = serve(move |req| {
            h.fetch_add(1, Ordering::SeqCst);
            if req.path != METADATA_TOKEN_PATH
                || req.headers.get("metadata-flavor").map(String::as_str) != Some("Google")
            {
                return Resp::new(403);
            }
            Resp::new(200).body(
                b"{\"access_token\":\"vmtok\",\"expires_in\":3599,\"token_type\":\"Bearer\"}"
                    .to_vec(),
            )
        })
        .await;

        let s = GcsStorage {
            cfg: GcsConfig {
                bucket: "b".into(),
                prefix: "p".into(),
                credentials_path: None,
                endpoint: None,
            },
            client: Client::builder().build().unwrap(),
            host: STORAGE_HOST.into(),
            token_source: imds_source(base).unwrap(),
            token: Arc::new(Mutex::new(None)),
            retry_policy: RetryPolicy::default(),
        };

        assert_eq!(s.access_token().await.unwrap(), "vmtok");
        assert_eq!(s.access_token().await.unwrap(), "vmtok");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "second token request must hit the cache"
        );
    }

    /// Emulator-mode GcsStorage built directly against an
    /// in-process mock of the GCS JSON API; drives put/get/exists/list/
    /// delete/rewrite without env or credentials.
    fn emulator(host: String) -> GcsStorage {
        GcsStorage {
            cfg: GcsConfig {
                bucket: "b".into(),
                prefix: "p".into(),
                credentials_path: None,
                endpoint: None,
            },
            client: Client::builder().build().unwrap(),
            host,
            token_source: TokenSource::None,
            token: Arc::new(Mutex::new(None)),
            retry_policy: quick_retries(),
        }
    }

    fn quick_retries() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
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
                endpoint: None,
            },
            client: Client::builder().build().unwrap(),
            host: "http://127.0.0.1:1".into(), // unused: access_token only hits token_uri
            token_source: TokenSource::Static(ServiceAccount {
                client_email: "svc@test.iam.gserviceaccount.com".into(),
                private_key: pem,
                token_uri: Some(format!("{token_base}/token")),
            }),
            token: Arc::new(Mutex::new(None)),
            retry_policy: RetryPolicy::default(),
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

    #[tokio::test]
    async fn gcs_buffered_uploads_retry_transient() {
        use crate::storage::test_http::{Req, Resp, reader, serve};
        use std::sync::atomic::{AtomicU32, Ordering};

        // Fail each upload once with 503, then return 412 for conditional retries
        let puts = Arc::new(AtomicU32::new(0));
        let conditional = Arc::new(AtomicU32::new(0));
        let (p, c) = (puts.clone(), conditional.clone());
        let base = serve(move |req: &Req| {
            if req.query("ifGenerationMatch") == Some("0") {
                match c.fetch_add(1, Ordering::SeqCst) {
                    0 => Resp::new(503),
                    _ => Resp::new(412),
                }
            } else {
                match p.fetch_add(1, Ordering::SeqCst) {
                    0 => Resp::new(503),
                    _ => Resp::new(200).body(b"{}".to_vec()),
                }
            }
        })
        .await;

        let s = emulator(base);
        s.put("a", reader(b"hello"), None).await.unwrap();
        assert_eq!(puts.load(Ordering::SeqCst), 2);
        assert_eq!(
            s.put_if_absent("b", reader(b"hello"), None).await.unwrap(),
            PutIfAbsentOutcome::AlreadyExists
        );
        assert_eq!(conditional.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn gcs_token_endpoint_blip_is_retried() {
        use crate::storage::test_http::{Req, Resp, read_all, serve};
        use std::sync::atomic::{AtomicU32, Ordering};

        let mints = Arc::new(AtomicU32::new(0));
        let m = mints.clone();
        let base = serve(move |req: &Req| {
            if req.path != METADATA_TOKEN_PATH {
                return Resp::new(200).body(b"ok".to_vec());
            }
            match m.fetch_add(1, Ordering::SeqCst) {
                0 => Resp::new(503),
                _ => Resp::new(200)
                    .body(b"{\"access_token\":\"vmtok\",\"expires_in\":3599}".to_vec()),
            }
        })
        .await;

        let s = GcsStorage {
            token_source: imds_source(base.clone()).unwrap(),
            ..emulator(base)
        };
        assert_eq!(read_all(s.get("a").await.unwrap()).await, b"ok");
        assert_eq!(mints.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn gcs_exists_errors_unless_found_or_missing() {
        use crate::storage::test_http::{Req, Resp, serve};
        use std::sync::atomic::{AtomicU32, Ordering};

        let flaky = Arc::new(AtomicU32::new(0));
        let f = flaky.clone();
        let base = serve(move |req: &Req| match req.path.rsplit("%2F").next() {
            Some("denied") => Resp::new(403),
            Some("flaky") if f.fetch_add(1, Ordering::SeqCst) == 0 => Resp::new(503),
            _ => Resp::new(404),
        })
        .await;

        let s = emulator(base);
        assert!(!s.exists("flaky").await.unwrap());
        assert_eq!(flaky.load(Ordering::SeqCst), 2);
        assert!(matches!(
            s.exists("denied").await,
            Err(StorageError::Http { status: 403, .. })
        ));
    }
}
