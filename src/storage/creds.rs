//! S3 credential sources: long-lived keys and EC2 instance-metadata (IMDS).
//!
//! Static keys come straight from env. IMDS creds are temporary: fetched from
//! the link-local metadata service via IMDSv2 (token-authenticated, falling
//! back to v1 when the token PUT is refused), cached, and refreshed shortly
//! before expiry. ECS/EKS container creds and STS web-identity are not (yet)
//! covered; the `CredentialSource` enum leaves room for them.

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

/// How S3 obtains credentials for signing. `Static` holds keys verbatim;
/// `Imds` fetches temporary creds from the metadata service on demand.
#[derive(Debug, Clone)]
pub enum CredentialSource {
    Static(Credentials),
    Imds(Arc<ImdsProvider>),
}

impl CredentialSource {
    /// Current credentials, fetching/refreshing from IMDS when needed
    pub async fn get(&self) -> Result<Credentials> {
        match self {
            CredentialSource::Static(c) => Ok(c.clone()),
            CredentialSource::Imds(p) => p.credentials().await,
        }
    }

    /// Stable identity for server-side-copy eligibility. IMDS keys rotate, so
    /// a key-based identity would spuriously fail; fold IMDS to a constant
    pub fn identity(&self) -> &str {
        match self {
            CredentialSource::Static(c) => &c.access_key,
            CredentialSource::Imds(_) => "imds",
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
    pub fn from_env() -> Result<Self> {
        let endpoint = std::env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT")
            .ok()
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

/// IMDS credential document shape (`.../iam/security-credentials/<role>`)
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
    let expires_at = chrono::DateTime::parse_from_rfc3339(&raw.expiration)
        .map(SystemTime::from)
        .map_err(|e| StorageError::Auth(format!("imds expiration {:?}: {e}", raw.expiration)))?;
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
}
