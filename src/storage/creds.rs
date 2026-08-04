//! S3 credential sources: long-lived keys plus three temporary-credential
//! providers, chained in the same order as the AWS SDKs.
//!
//! Static keys come straight from env. The rest are temporary: cached and
//! refreshed shortly before expiry.
//! - IMDS: the link-local metadata service via IMDSv2 (token-authenticated,
//!   falling back to v1 when the token PUT is refused).
//! - Container credentials (EKS Pod Identity / ECS): one authenticated GET
//!   returning the same document shape as IMDS.
//! - STS web-identity (IRSA): the projected ServiceAccount JWT exchanged for
//!   temporary creds via AssumeRoleWithWebIdentity.
//!
//! The last two are ambient per-pod identity — no key material on disk — and
//! share `AmbientProvider`, since both are a single authenticated HTTP call.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::{Result, StorageError};

/// Link-local IMDS address; override with AWS_EC2_METADATA_SERVICE_ENDPOINT
const DEFAULT_ENDPOINT: &str = "http://169.254.169.254";
const TOKEN_PATH: &str = "/latest/api/token";
const IAM_PATH: &str = "/latest/meta-data/iam/security-credentials/";
/// Max TTL IMDSv2 grants a session token
const TOKEN_TTL_SECS: u32 = 21600;
/// Refetch this far ahead of expiry so signing never races a stale credential
const REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Resolved credentials for SigV4. Temporary creds (IMDS, STS) carry a session
/// token and an expiry; long-lived keys leave both empty/None.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub expires_at: Option<SystemTime>,
}

impl Credentials {
    /// Within `margin` of expiry (or already past). Always false for
    /// non-expiring static keys
    fn expires_within(&self, margin: Duration) -> bool {
        match self.expires_at {
            Some(exp) => SystemTime::now() + margin >= exp,
            None => false,
        }
    }
}

/// How S3 obtains credentials for signing. `Static` holds keys verbatim; the
/// other two fetch temporary creds on demand — `Imds` from the EC2 metadata
/// service, `Ambient` from a per-pod identity endpoint.
#[derive(Debug, Clone)]
pub enum CredentialSource {
    Static(Credentials),
    Imds(Arc<ImdsProvider>),
    Ambient(Arc<AmbientProvider>),
}

impl CredentialSource {
    /// Current credentials, fetching/refreshing from the provider when needed
    pub async fn get(&self) -> Result<Credentials> {
        match self {
            CredentialSource::Static(c) => Ok(c.clone()),
            CredentialSource::Imds(p) => p.credentials().await,
            CredentialSource::Ambient(p) => p.credentials().await,
        }
    }

    /// Stable identity for server-side-copy eligibility. Temporary creds
    /// rotate, so a key-based identity would spuriously fail; fold every
    /// refreshing source to a constant
    pub fn identity(&self) -> &str {
        match self {
            CredentialSource::Static(c) => &c.access_key,
            CredentialSource::Imds(_) => "imds",
            CredentialSource::Ambient(p) => p.kind.label(),
        }
    }
}

/// EC2 instance-metadata credential provider, caching the last fetch until it
/// nears expiry.
#[derive(Debug)]
pub struct ImdsProvider {
    client: Client,
    endpoint: String,
    cached: Mutex<Option<Credentials>>,
}

impl ImdsProvider {
    /// `endpoint` overrides the link-local default (`AWS_EC2_METADATA_SERVICE_ENDPOINT`),
    /// resolved in the config layer
    pub fn new(endpoint: Option<String>) -> Result<Self> {
        let endpoint = endpoint
            .map(|e| e.trim_end_matches('/').to_string())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        Self::with_endpoint(endpoint)
    }

    /// `no_proxy` + short timeouts: the link-local address must never traverse
    /// an HTTP proxy, and non-EC2 hosts should fail fast rather than hang
    fn with_endpoint(endpoint: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(1))
            .no_proxy()
            .build()
            .map_err(|e| StorageError::Config(format!("imds client: {e}")))?;
        Ok(Self {
            client,
            endpoint,
            cached: Mutex::new(None),
        })
    }

    /// Single-flight: the lock spans the fetch so concurrent signers don't
    /// stampede the metadata service. Cache hits clone and return immediately.
    pub async fn credentials(&self) -> Result<Credentials> {
        let mut guard = self.cached.lock().await;
        if let Some(c) = guard.as_ref()
            && !c.expires_within(REFRESH_MARGIN)
        {
            return Ok(c.clone());
        }
        let fresh = self.fetch().await?;
        *guard = Some(fresh.clone());
        Ok(fresh)
    }

    async fn fetch(&self) -> Result<Credentials> {
        let token = self.fetch_token().await;
        let role = self.get(IAM_PATH, token.as_deref()).await?;
        let role = role.trim();
        if role.is_empty() {
            return Err(StorageError::Auth("imds: no IAM role on instance".into()));
        }
        let body = self
            .get(&format!("{IAM_PATH}{role}"), token.as_deref())
            .await?;
        parse_creds(&body)
    }

    /// IMDSv2 session token. None when the PUT is refused, so the caller still
    /// attempts the IMDSv1 unauthenticated path (a 401 where v2 is enforced)
    async fn fetch_token(&self) -> Option<String> {
        let resp = self
            .client
            .put(format!("{}{TOKEN_PATH}", self.endpoint))
            .header(
                "x-aws-ec2-metadata-token-ttl-seconds",
                TOKEN_TTL_SECS.to_string(),
            )
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.text().await.ok()
    }

    async fn get(&self, path: &str, token: Option<&str>) -> Result<String> {
        let mut req = self.client.get(format!("{}{path}", self.endpoint));
        if let Some(t) = token {
            req = req.header("x-aws-ec2-metadata-token", t);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(StorageError::Http {
                status: status.as_u16(),
                body,
            });
        }
        Ok(body)
    }
}

/// Which ambient endpoint mints the credentials. Both flavors are a single
/// authenticated HTTP call, so they share one provider and differ only in how
/// the request is built and how the response is parsed.
#[derive(Debug)]
pub enum AmbientKind {
    /// Container credentials endpoint (EKS Pod Identity / ECS). The response is
    /// the same document shape as IMDS, so `parse_creds` handles it.
    Container {
        /// `AWS_CONTAINER_CREDENTIALS_FULL_URI`, or `..._RELATIVE_URI` joined to
        /// the ECS link-local base
        url: String,
        /// `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` — rotates, so it is re-read
        /// on every refresh
        token_file: Option<String>,
        /// `AWS_CONTAINER_AUTHORIZATION_TOKEN`
        static_token: Option<String>,
    },
    /// STS web-identity (IRSA): exchange the projected ServiceAccount JWT for
    /// temporary creds. The AWS Query protocol answers with XML.
    WebIdentity {
        sts_endpoint: String,
        role_arn: String,
        session_name: String,
        /// `AWS_WEB_IDENTITY_TOKEN_FILE` — rotates, so it is re-read on every
        /// refresh
        token_file: String,
    },
}

impl AmbientKind {
    /// Rotation-stable name, used both for `CredentialSource::identity` and to
    /// label client-construction errors
    fn label(&self) -> &'static str {
        match self {
            AmbientKind::Container { .. } => "container-creds",
            AmbientKind::WebIdentity { .. } => "web-identity",
        }
    }
}

/// Ambient (keyless) per-pod credential provider, caching the last fetch until
/// it nears expiry — same shape as `ImdsProvider`.
#[derive(Debug)]
pub struct AmbientProvider {
    client: Client,
    kind: AmbientKind,
    cached: Mutex<Option<Credentials>>,
}

impl AmbientProvider {
    pub fn new(kind: AmbientKind) -> Result<Self> {
        // container creds live on a link-local address, which must never
        // traverse a proxy, and should fail fast off-cluster. STS is a public
        // endpoint: normal timeout, proxy allowed.
        let builder = match &kind {
            AmbientKind::Container { .. } => Client::builder()
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(1))
                .no_proxy(),
            AmbientKind::WebIdentity { .. } => Client::builder().timeout(Duration::from_secs(10)),
        };
        let client = builder
            .build()
            .map_err(|e| StorageError::Config(format!("{} client: {e}", kind.label())))?;
        Ok(Self {
            client,
            kind,
            cached: Mutex::new(None),
        })
    }

    /// Single-flight: the lock spans the fetch so concurrent signers don't
    /// stampede the endpoint. Cache hits clone and return immediately.
    pub async fn credentials(&self) -> Result<Credentials> {
        let mut guard = self.cached.lock().await;
        if let Some(c) = guard.as_ref()
            && !c.expires_within(REFRESH_MARGIN)
        {
            return Ok(c.clone());
        }
        let fresh = self.fetch().await?;
        *guard = Some(fresh.clone());
        Ok(fresh)
    }

    async fn fetch(&self) -> Result<Credentials> {
        let req = match &self.kind {
            AmbientKind::Container {
                url,
                token_file,
                static_token,
            } => {
                let mut req = self.client.get(url);
                // a rotating token file wins over a static token; both optional
                match (token_file, static_token) {
                    (Some(path), _) => req = req.header("Authorization", read_token(path)?),
                    (None, Some(tok)) => req = req.header("Authorization", tok),
                    (None, None) => {}
                }
                req
            }
            AmbientKind::WebIdentity {
                sts_endpoint,
                role_arn,
                session_name,
                token_file,
            } => {
                let token = read_token(token_file)?;
                self.client.post(sts_endpoint).form(&[
                    ("Action", "AssumeRoleWithWebIdentity"),
                    ("Version", "2011-06-15"),
                    ("RoleArn", role_arn.as_str()),
                    ("RoleSessionName", session_name.as_str()),
                    ("WebIdentityToken", token.as_str()),
                ])
            }
        };
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            // Http (not Auth) so a 5xx from the endpoint stays retryable
            return Err(StorageError::Http {
                status: status.as_u16(),
                body,
            });
        }
        match &self.kind {
            AmbientKind::Container { .. } => parse_creds(&body),
            AmbientKind::WebIdentity { .. } => parse_sts_creds(&body),
        }
    }
}

/// Projected/rotating token files are re-read on every refresh, never cached
fn read_token(path: &str) -> Result<String> {
    std::fs::read_to_string(path)
        .map(|t| t.trim().to_string())
        .map_err(|e| StorageError::Auth(format!("read token {path}: {e}")))
}

/// Temporary creds from an `AssumeRoleWithWebIdentity` XML response.
/// `first_tag_text` matches on local element name, so nesting is irrelevant.
fn parse_sts_creds(xml: &str) -> Result<Credentials> {
    use crate::storage::s3::first_tag_text;
    let need = |tag: &'static [u8]| {
        first_tag_text(xml, tag).ok_or_else(|| {
            StorageError::Auth(format!("sts: missing {}", String::from_utf8_lossy(tag)))
        })
    };
    Ok(Credentials {
        access_key: need(b"AccessKeyId")?,
        secret_key: need(b"SecretAccessKey")?,
        // Some(..) for temporary creds -> x-amz-security-token gets signed
        session_token: first_tag_text(xml, b"SessionToken"),
        expires_at: Some(parse_expiry(&need(b"Expiration")?)?),
    })
}

/// Expiry as reported by IMDS, container credentials, and STS
fn parse_expiry(raw: &str) -> Result<SystemTime> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(SystemTime::from)
        .map_err(|e| StorageError::Auth(format!("expiration {raw:?}: {e}")))
}

/// IMDS credential document shape (`.../iam/security-credentials/<role>`),
/// also returned verbatim by the container credentials endpoint
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ImdsCreds {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: String,
    #[serde(default)]
    code: Option<String>,
}

fn parse_creds(body: &str) -> Result<Credentials> {
    let raw: ImdsCreds = serde_json::from_str(body)
        .map_err(|e| StorageError::Auth(format!("imds creds json: {e}")))?;
    if let Some(code) = &raw.code
        && code != "Success"
    {
        return Err(StorageError::Auth(format!("imds creds code {code}")));
    }
    let expires_at = parse_expiry(&raw.expiration)?;
    Ok(Credentials {
        access_key: raw.access_key_id,
        secret_key: raw.secret_access_key,
        session_token: Some(raw.token),
        expires_at: Some(expires_at),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_http::{Req, Resp, serve};
    use chrono::Utc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn creds_json(expiration: &str) -> String {
        format!(
            r#"{{"Code":"Success","AccessKeyId":"ASIAEXAMPLE","SecretAccessKey":"secret","Token":"sessiontok","Expiration":"{expiration}"}}"#
        )
    }

    /// Mock IMDS counting credential-document fetches so caching is observable.
    /// `require_token` rejects the v1 (token-less) GET with 401.
    async fn provider(expiration: String, require_token: bool) -> (ImdsProvider, Arc<AtomicU32>) {
        let fetches = Arc::new(AtomicU32::new(0));
        let f = fetches.clone();
        let base = serve(move |req: &Req| {
            let has_token = req.headers.contains_key("x-aws-ec2-metadata-token");
            match (req.method.as_str(), req.path.as_str()) {
                ("PUT", TOKEN_PATH) => Resp::new(200).body(b"TOKEN".to_vec()),
                ("GET", _) if require_token && !has_token => Resp::new(401),
                ("GET", IAM_PATH) => Resp::new(200).body(b"myrole".to_vec()),
                ("GET", p) if p == format!("{IAM_PATH}myrole") => {
                    f.fetch_add(1, Ordering::SeqCst);
                    Resp::new(200).body(creds_json(&expiration).into_bytes())
                }
                _ => Resp::new(404),
            }
        })
        .await;
        (ImdsProvider::with_endpoint(base).unwrap(), fetches)
    }

    #[tokio::test]
    async fn fetches_and_parses_temporary_creds() {
        let exp = (Utc::now() + chrono::Duration::hours(6)).to_rfc3339();
        let (p, _) = provider(exp, true).await;
        let c = p.credentials().await.unwrap();
        assert_eq!(c.access_key, "ASIAEXAMPLE");
        assert_eq!(c.secret_key, "secret");
        assert_eq!(c.session_token.as_deref(), Some("sessiontok"));
        assert!(c.expires_at.is_some());
    }

    #[tokio::test]
    async fn caches_until_near_expiry() {
        let exp = (Utc::now() + chrono::Duration::hours(6)).to_rfc3339();
        let (p, fetches) = provider(exp, true).await;
        p.credentials().await.unwrap();
        p.credentials().await.unwrap();
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            1,
            "second call served from cache"
        );
    }

    #[tokio::test]
    async fn refetches_when_expiring_within_margin() {
        // expiry inside REFRESH_MARGIN -> every call refetches
        let exp = (Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        let (p, fetches) = provider(exp, true).await;
        p.credentials().await.unwrap();
        p.credentials().await.unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn falls_back_to_imdsv1_when_token_refused() {
        // token PUT 404s in this mock; v1 GET (no token) must still work
        let fetches = Arc::new(AtomicU32::new(0));
        let f = fetches.clone();
        let base = serve(
            move |req: &Req| match (req.method.as_str(), req.path.as_str()) {
                ("PUT", TOKEN_PATH) => Resp::new(404),
                ("GET", IAM_PATH) => Resp::new(200).body(b"myrole".to_vec()),
                ("GET", p) if p == format!("{IAM_PATH}myrole") => {
                    f.fetch_add(1, Ordering::SeqCst);
                    Resp::new(200).body(
                        creds_json(&(Utc::now() + chrono::Duration::hours(6)).to_rfc3339())
                            .into_bytes(),
                    )
                }
                _ => Resp::new(404),
            },
        )
        .await;
        let p = ImdsProvider::with_endpoint(base).unwrap();
        assert_eq!(p.credentials().await.unwrap().access_key, "ASIAEXAMPLE");
    }

    #[tokio::test]
    async fn errors_when_no_role_attached() {
        let base = serve(|req: &Req| match (req.method.as_str(), req.path.as_str()) {
            ("PUT", TOKEN_PATH) => Resp::new(200).body(b"TOKEN".to_vec()),
            ("GET", IAM_PATH) => Resp::new(200).body(Vec::new()),
            _ => Resp::new(404),
        })
        .await;
        let p = ImdsProvider::with_endpoint(base).unwrap();
        assert!(matches!(p.credentials().await, Err(StorageError::Auth(_))));
    }

    #[tokio::test]
    async fn credential_source_imds_fetches_and_identity_is_constant() {
        let exp = (Utc::now() + chrono::Duration::hours(6)).to_rfc3339();
        let (p, _) = provider(exp, true).await;
        let src = CredentialSource::Imds(Arc::new(p));
        // identity folds to a constant so rotating IMDS keys don't break copy
        assert_eq!(src.identity(), "imds");
        let c = src.get().await.unwrap();
        assert_eq!(c.access_key, "ASIAEXAMPLE");
    }

    #[test]
    fn static_identity_is_the_access_key() {
        let src = CredentialSource::Static(Credentials {
            access_key: "AKIAEXAMPLE".into(),
            secret_key: "secret".into(),
            session_token: None,
            expires_at: None,
        });
        assert_eq!(src.identity(), "AKIAEXAMPLE");
    }

    #[tokio::test]
    async fn http_error_surfaces_when_role_fetch_fails() {
        // token PUT succeeds; the IAM role GET 500s, so get() returns Http
        let base = serve(|req: &Req| match (req.method.as_str(), req.path.as_str()) {
            ("PUT", TOKEN_PATH) => Resp::new(200).body(b"TOKEN".to_vec()),
            ("GET", IAM_PATH) => Resp::new(500).body(b"boom".to_vec()),
            _ => Resp::new(404),
        })
        .await;
        let p = ImdsProvider::with_endpoint(base).unwrap();
        assert!(matches!(
            p.credentials().await,
            Err(StorageError::Http { status: 500, .. })
        ));
    }

    #[test]
    fn expires_within_honors_margin_and_static_keys() {
        let soon = Credentials {
            access_key: "a".into(),
            secret_key: "b".into(),
            session_token: Some("t".into()),
            expires_at: Some(SystemTime::now() + Duration::from_secs(60)),
        };
        assert!(soon.expires_within(REFRESH_MARGIN));
        let far = Credentials {
            expires_at: Some(SystemTime::now() + Duration::from_secs(REFRESH_MARGIN.as_secs() * 4)),
            ..soon.clone()
        };
        assert!(!far.expires_within(REFRESH_MARGIN));
        // static keys never expire
        let stat = Credentials {
            expires_at: None,
            ..soon
        };
        assert!(!stat.expires_within(REFRESH_MARGIN));
    }

    #[test]
    fn parse_creds_rejects_non_success_code() {
        // all key fields present so deserialization passes and the Code guard
        // is what rejects it
        let body = r#"{"Code":"AssumeRoleUnauthorizedAccess","AccessKeyId":"x","SecretAccessKey":"y","Token":"z","Expiration":"2030-01-01T00:00:00Z"}"#;
        assert!(matches!(parse_creds(body), Err(StorageError::Auth(_))));
    }

    /// Mock container-credentials endpoint counting fetches (so caching is
    /// observable) and recording the Authorization header it saw.
    async fn container_endpoint(
        expiration: String,
    ) -> (
        String,
        Arc<AtomicU32>,
        Arc<std::sync::Mutex<Option<String>>>,
    ) {
        let fetches = Arc::new(AtomicU32::new(0));
        // std mutex: the handler is sync, called from inside an async task
        let seen = Arc::new(std::sync::Mutex::new(None));
        let (f, s) = (fetches.clone(), seen.clone());
        let base = serve(move |req: &Req| {
            f.fetch_add(1, Ordering::SeqCst);
            *s.lock().unwrap() = req.headers.get("authorization").cloned();
            Resp::new(200).body(creds_json(&expiration).into_bytes())
        })
        .await;
        (base, fetches, seen)
    }

    #[tokio::test]
    async fn container_creds_parse_cache_and_send_static_token() {
        let exp = (Utc::now() + chrono::Duration::hours(6)).to_rfc3339();
        let (base, fetches, seen) = container_endpoint(exp).await;
        let p = AmbientProvider::new(AmbientKind::Container {
            url: format!("{base}/v1/credentials"),
            token_file: None,
            static_token: Some("tok".into()),
        })
        .unwrap();

        // Pod Identity / ECS answer with the IMDS document shape
        let c = p.credentials().await.unwrap();
        assert_eq!(c.access_key, "ASIAEXAMPLE");
        assert_eq!(c.session_token.as_deref(), Some("sessiontok"));
        assert!(c.expires_at.is_some());

        p.credentials().await.unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 1, "second call must cache");
        assert_eq!(seen.lock().unwrap().as_deref(), Some("tok"));

        let src = CredentialSource::Ambient(Arc::new(p));
        assert_eq!(src.identity(), "container-creds");
        assert_eq!(src.get().await.unwrap().access_key, "ASIAEXAMPLE");
    }

    #[tokio::test]
    async fn container_creds_token_file_wins_over_static_token() {
        let exp = (Utc::now() + chrono::Duration::hours(6)).to_rfc3339();
        let (base, _, seen) = container_endpoint(exp).await;
        // EKS Pod Identity projects a rotating token file; it must take priority
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), "  from-file\n").unwrap();
        let p = AmbientProvider::new(AmbientKind::Container {
            url: format!("{base}/v1/credentials"),
            token_file: Some(f.path().to_string_lossy().into()),
            static_token: Some("static".into()),
        })
        .unwrap();

        p.credentials().await.unwrap();
        // trimmed, so a trailing newline in the projected file isn't sent
        assert_eq!(seen.lock().unwrap().as_deref(), Some("from-file"));
    }

    #[tokio::test]
    async fn container_creds_surface_http_errors() {
        let base = serve(|_req: &Req| Resp::new(500).body(b"boom".to_vec())).await;
        let p = AmbientProvider::new(AmbientKind::Container {
            url: base,
            token_file: None,
            static_token: None,
        })
        .unwrap();
        // Http, not Auth, so the outer retry treats a 5xx as transient
        assert!(matches!(
            p.credentials().await,
            Err(StorageError::Http { status: 500, .. })
        ));
    }

    fn sts_xml(expiration: &str) -> String {
        format!(
            r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult><Credentials>
    <AccessKeyId>ASIAWEBID</AccessKeyId><SecretAccessKey>websecret</SecretAccessKey>
    <SessionToken>webtok</SessionToken><Expiration>{expiration}</Expiration>
  </Credentials></AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#
        )
    }

    /// Mock STS returning an AssumeRoleWithWebIdentity document, plus a projected
    /// token file for the provider to read.
    async fn web_identity(
        expiration: String,
    ) -> (AmbientProvider, Arc<AtomicU32>, tempfile::NamedTempFile) {
        let fetches = Arc::new(AtomicU32::new(0));
        let f = fetches.clone();
        let base = serve(move |req: &Req| {
            // Query protocol: form-encoded POST, XML response
            let body = String::from_utf8_lossy(&req.body).to_string();
            if req.method != "POST" || !body.contains("Action=AssumeRoleWithWebIdentity") {
                return Resp::new(400);
            }
            f.fetch_add(1, Ordering::SeqCst);
            Resp::new(200).body(sts_xml(&expiration).into_bytes())
        })
        .await;
        let token = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(token.path(), "eyJhbGciOiJSUzI1NiJ9.payload.sig\n").unwrap();
        let p = AmbientProvider::new(AmbientKind::WebIdentity {
            sts_endpoint: base,
            role_arn: "arn:aws:iam::1:role/r".into(),
            session_name: "wal-rus".into(),
            token_file: token.path().to_string_lossy().into(),
        })
        .unwrap();
        (p, fetches, token)
    }

    #[tokio::test]
    async fn web_identity_exchanges_token_caches_and_identity_is_constant() {
        let exp = (Utc::now() + chrono::Duration::hours(6)).to_rfc3339();
        let (p, fetches, _token) = web_identity(exp).await;

        let c = p.credentials().await.unwrap();
        assert_eq!(c.access_key, "ASIAWEBID");
        assert_eq!(c.secret_key, "websecret");
        // Some(..) is what makes the signer emit x-amz-security-token
        assert_eq!(c.session_token.as_deref(), Some("webtok"));
        assert!(c.expires_at.is_some());

        p.credentials().await.unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 1, "second call must cache");

        let src = CredentialSource::Ambient(Arc::new(p));
        assert_eq!(src.identity(), "web-identity");
    }

    #[tokio::test]
    async fn web_identity_refetches_when_expiring_within_margin() {
        // expiry inside REFRESH_MARGIN -> every call re-reads the rotating token
        let exp = (Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        let (p, fetches, _token) = web_identity(exp).await;
        p.credentials().await.unwrap();
        p.credentials().await.unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn web_identity_missing_token_file_is_an_auth_error() {
        let p = AmbientProvider::new(AmbientKind::WebIdentity {
            sts_endpoint: "http://127.0.0.1:1".into(),
            role_arn: "arn:aws:iam::1:role/r".into(),
            session_name: "wal-rus".into(),
            token_file: "/nonexistent/projected/token".into(),
        })
        .unwrap();
        assert!(matches!(p.credentials().await, Err(StorageError::Auth(_))));
    }

    #[test]
    fn parse_sts_creds_requires_the_key_fields() {
        let missing = "<AssumeRoleWithWebIdentityResponse><Credentials>\
            <SecretAccessKey>s</SecretAccessKey></Credentials></AssumeRoleWithWebIdentityResponse>";
        assert!(matches!(
            parse_sts_creds(missing),
            Err(StorageError::Auth(_))
        ));
        // SessionToken is the only optional field
        let no_token = format!(
            "<Credentials><AccessKeyId>a</AccessKeyId><SecretAccessKey>s</SecretAccessKey>\
             <Expiration>{}</Expiration></Credentials>",
            (Utc::now() + chrono::Duration::hours(1)).to_rfc3339()
        );
        let c = parse_sts_creds(&no_token).unwrap();
        assert_eq!(c.access_key, "a");
        assert!(c.session_token.is_none());
    }
}
