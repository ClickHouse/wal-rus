//! Config loading from env, mirroring wal-g WALG_/AWS_/GOOGLE_ vars

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};

use crate::compression;
use crate::retry::RetryPolicy;
use crate::storage::{DynStorage, Storage, fs::FsStorage, gcs, retrying::RetryingStorage, s3};

#[derive(Debug, Clone)]
pub struct Settings {
    pub storage: StorageSettings,
    pub compression: compression::Method,
    pub compression_level: i32,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
    pub prevent_wal_overwrite: bool,
    pub retry: RetryPolicy,
    /// WALG_NETWORK_RATE_LIMIT in bytes/sec, 0 = unthrottled
    pub network_rate_limit: u64,
    /// WALG_DISK_RATE_LIMIT in bytes/sec, 0 = unthrottled
    pub disk_rate_limit: u64,
}

#[derive(Debug, Clone)]
pub enum StorageSettings {
    Fs { path: String },
    S3(s3::S3Config),
    Gcs(gcs::GcsConfig),
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let storage = detect_storage()?;
        let compression = match std::env::var("WALG_COMPRESSION_METHOD").ok().as_deref() {
            None => compression::Method::Zstd,
            Some(s) => compression::Method::from_name(s)
                .ok_or_else(|| anyhow!("unsupported WALG_COMPRESSION_METHOD={s}"))?,
        };
        let compression_level = parse_env_int("WALG_COMPRESSION_LEVEL", 3)? as i32;
        let upload_concurrency = parse_env_int("WALG_UPLOAD_CONCURRENCY", 4)?.max(1) as usize;
        let download_concurrency = parse_env_int("WALG_DOWNLOAD_CONCURRENCY", 4)?.max(1) as usize;
        let prevent_wal_overwrite = parse_env_bool("WALG_PREVENT_WAL_OVERWRITE", false)?;
        let retry = RetryPolicy::from_env();
        let network_rate_limit = parse_env_int("WALG_NETWORK_RATE_LIMIT", 0)?.max(0) as u64;
        let disk_rate_limit = parse_env_int("WALG_DISK_RATE_LIMIT", 0)?.max(0) as u64;
        Ok(Settings {
            storage,
            compression,
            compression_level,
            upload_concurrency,
            download_concurrency,
            prevent_wal_overwrite,
            retry,
            network_rate_limit,
            disk_rate_limit,
        })
    }

    /// Wrap an AsyncRead with WALG_NETWORK_RATE_LIMIT throttling. No-op when unset
    pub fn throttle_network(
        &self,
        reader: crate::compression::AsyncReader,
    ) -> crate::compression::AsyncReader {
        if self.network_rate_limit == 0 {
            reader
        } else {
            Box::pin(crate::throttle::RateLimited::new(
                reader,
                self.network_rate_limit,
            ))
        }
    }

    /// Wrap an AsyncRead with WALG_DISK_RATE_LIMIT throttling. No-op when unset
    pub fn throttle_disk(
        &self,
        reader: crate::compression::AsyncReader,
    ) -> crate::compression::AsyncReader {
        if self.disk_rate_limit == 0 {
            reader
        } else {
            Box::pin(crate::throttle::RateLimited::new(
                reader,
                self.disk_rate_limit,
            ))
        }
    }

    pub fn build_storage(&self) -> Result<DynStorage> {
        let policy = self.retry;
        match &self.storage {
            StorageSettings::Fs { path } => {
                // local fs: skip retry wrapper; no transient failures worth retrying
                let s = FsStorage::new(path).context("init fs storage")?;
                Ok(Arc::new(s) as Arc<dyn Storage>)
            }
            StorageSettings::S3(c) => {
                let s = s3::S3Storage::new(c.clone()).context("init s3 storage")?;
                Ok(Arc::new(RetryingStorage::new(s, policy)) as Arc<dyn Storage>)
            }
            StorageSettings::Gcs(c) => {
                let cfg = gcs::GcsConfig {
                    bucket: c.bucket.clone(),
                    prefix: c.prefix.clone(),
                    credentials_path: c.credentials_path.clone(),
                };
                let s = gcs::GcsStorage::new(cfg).context("init gcs storage")?;
                Ok(Arc::new(RetryingStorage::new(s, policy)) as Arc<dyn Storage>)
            }
        }
    }
}

fn detect_storage() -> Result<StorageSettings> {
    if let Ok(prefix) = std::env::var("WALG_FILE_PREFIX") {
        return Ok(StorageSettings::Fs { path: prefix });
    }
    if let Ok(s3_prefix) = std::env::var("WALG_S3_PREFIX") {
        let (bucket, prefix) = parse_uri_prefix(&s3_prefix, "s3://")?;
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("WALG_S3_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("AWS_ACCESS_KEY"))
            .map_err(|_| anyhow!("AWS_ACCESS_KEY_ID not set"))?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("AWS_SECRET_KEY"))
            .map_err(|_| anyhow!("AWS_SECRET_ACCESS_KEY not set"))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        let endpoint = std::env::var("AWS_ENDPOINT_URL")
            .or_else(|_| std::env::var("WALG_S3_ENDPOINT"))
            .ok();
        let force_path_style = parse_env_bool("WALG_S3_FORCE_PATH_STYLE", endpoint.is_some())?;
        return Ok(StorageSettings::S3(s3::S3Config {
            bucket,
            prefix,
            region,
            access_key,
            secret_key,
            session_token,
            endpoint,
            force_path_style,
        }));
    }
    if let Ok(gs_prefix) = std::env::var("WALG_GS_PREFIX") {
        let (bucket, prefix) = parse_uri_prefix(&gs_prefix, "gs://")?;
        let credentials_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
        return Ok(StorageSettings::Gcs(gcs::GcsConfig {
            bucket,
            prefix,
            credentials_path,
        }));
    }
    bail!("no storage configured: set WALG_FILE_PREFIX, WALG_S3_PREFIX, or WALG_GS_PREFIX")
}

fn parse_uri_prefix(uri: &str, scheme: &str) -> Result<(String, String)> {
    let rest = uri
        .strip_prefix(scheme)
        .ok_or_else(|| anyhow!("expected {scheme} prefix on {uri}"))?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b.to_string(), p.trim_end_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        bail!("bucket is empty in {uri}");
    }
    Ok((bucket, prefix))
}

fn parse_env_int(key: &str, default: i64) -> Result<i64> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(v) => v.parse().with_context(|| format!("parse {key}={v}")),
    }
}

fn parse_env_bool(key: &str, default: bool) -> Result<bool> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => bail!("parse {key}={v} as bool"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_uri() {
        let (b, p) = parse_uri_prefix("s3://my-bucket/some/prefix", "s3://").unwrap();
        assert_eq!(b, "my-bucket");
        assert_eq!(p, "some/prefix");

        let (b, p) = parse_uri_prefix("s3://just-bucket", "s3://").unwrap();
        assert_eq!(b, "just-bucket");
        assert_eq!(p, "");
    }

    #[test]
    fn rejects_wrong_scheme() {
        assert!(parse_uri_prefix("gs://x/y", "s3://").is_err());
    }
}
