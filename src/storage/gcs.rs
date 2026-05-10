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
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

use super::{AsyncReader, ObjectMeta, ObjectStream, Result, Storage, StorageError};

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
    sa: ServiceAccount,
    token: Arc<Mutex<Option<CachedToken>>>,
}

impl GcsStorage {
    pub fn new(cfg: GcsConfig) -> Result<Self> {
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
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| StorageError::Config(e.to_string()))?;
        Ok(Self {
            cfg,
            client,
            sa,
            token: Arc::new(Mutex::new(None)),
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

    async fn access_token(&self) -> Result<String> {
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
            "iss": self.sa.client_email,
            "scope": SCOPE,
            "aud": self.sa.token_uri.clone().unwrap_or_else(|| TOKEN_URL.into()),
            "iat": now_secs,
            "exp": now_secs + 3600,
        });

        let h_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let c_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
        let signing_input = format!("{h_b64}.{c_b64}");

        let key_pair = parse_pkcs8_or_pkcs1(&self.sa.private_key)?;
        let rng = SystemRandom::new();
        let mut sig = vec![0u8; key_pair.public_key().modulus_len()];
        key_pair
            .sign(&RSA_PKCS1_SHA256, &rng, signing_input.as_bytes(), &mut sig)
            .map_err(|e| StorageError::Auth(format!("rsa sign: {e}")))?;
        let s_b64 = URL_SAFE_NO_PAD.encode(&sig);
        let jwt = format!("{signing_input}.{s_b64}");

        let token_url = self.sa.token_uri.as_deref().unwrap_or(TOKEN_URL);
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
        format!(
            "{}/storage/v1/b/{}/o/{}",
            STORAGE_HOST, self.cfg.bucket, enc
        )
    }
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
            STORAGE_HOST,
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
        let token = self.access_token().await?;

        let s = stream::unfold(
            (Some(String::new()), full, cfg_prefix, bucket, client, token),
            |(token_page, prefix, strip, bucket, client, auth)| async move {
                let token_page = token_page?;
                let mut url = format!(
                    "{}/storage/v1/b/{}/o?prefix={}",
                    STORAGE_HOST,
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
                        return Some((Err(e.into()), (None, prefix, strip, bucket, client, auth)));
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
                        (None, prefix, strip, bucket, client, auth),
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
                        return Some((Err(e.into()), (None, prefix, strip, bucket, client, auth)));
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
                Some((Ok(out), (next, prefix, strip, bucket, client, auth)))
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
}

fn parse_pkcs8_or_pkcs1(pem: &str) -> Result<RsaKeyPair> {
    // service account keys come as PKCS#8 PEM by default
    let der = pem_to_der(pem).map_err(StorageError::Auth)?;
    if let Ok(kp) = RsaKeyPair::from_pkcs8(&der) {
        return Ok(kp);
    }
    RsaKeyPair::from_der(&der).map_err(|e| StorageError::Auth(format!("rsa key parse: {e}")))
}

fn pem_to_der(pem: &str) -> std::result::Result<Vec<u8>, String> {
    let mut started = false;
    let mut b64 = String::new();
    for line in pem.lines() {
        if line.starts_with("-----BEGIN") {
            started = true;
            continue;
        }
        if line.starts_with("-----END") {
            break;
        }
        if started {
            b64.push_str(line.trim());
        }
    }
    base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .map_err(|e| format!("pem b64: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pem_to_der_strips_armor() {
        let pem = "-----BEGIN PRIVATE KEY-----\nAAEC\n-----END PRIVATE KEY-----\n";
        let der = pem_to_der(pem).unwrap();
        assert_eq!(der, vec![0, 1, 2]);
    }
}
