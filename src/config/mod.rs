//! Config loading from env, mirroring wal-g WALG_/AWS_/GOOGLE_ vars

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};

use crate::compression;
use crate::crypto::{self, DynCrypter};
use crate::retry::RetryPolicy;
use crate::storage::{DynStorage, Storage, fs::FsStorage, gcs, retrying::RetryingStorage, s3};

#[derive(Debug, Clone)]
pub struct Settings {
    pub storage: StorageSettings,
    pub compression: compression::Method,
    pub compression_level: i32,
    pub upload_concurrency: usize,
    /// `WALG_UPLOAD_QUEUE`: buffer between part producer & uploader workers.
    /// Caps how many parts may sit fully-finalized waiting for an uploader
    pub upload_queue: usize,
    pub download_concurrency: usize,
    pub prevent_wal_overwrite: bool,
    /// `WALG_USE_WAL_DELTA`: record `<group>_delta` sidecars during wal-push so
    /// delta backups read changed-block sets per 16-segment group instead of
    /// re-parsing every segment. Off by default, matching wal-g
    pub use_wal_delta: bool,
    pub retry: RetryPolicy,
    /// WALG_NETWORK_RATE_LIMIT in bytes/sec, 0 = unthrottled
    pub network_rate_limit: u64,
    /// WALG_DISK_RATE_LIMIT in bytes/sec, 0 = unthrottled
    pub disk_rate_limit: u64,
    pub delta: DeltaSettings,
    /// Optional libsodium crypter — set via `WALG_LIBSODIUM_KEY` / `_KEY_PATH`.
    /// OpenPGP is intentionally not supported (see `src/crypto/mod.rs`);
    /// detection of `WALG_PGP_*` is a hard error so plaintext writes can't
    /// silently happen when the operator intended encryption
    pub crypter: Option<DynCrypter>,
}

/// Delta-backup config: WALG_DELTA_MAX_STEPS / _ORIGIN / _FROM_NAME / _FROM_USER_DATA
///
/// `max_steps == 0` means deltas are disabled (default). `from_full=true`
/// (`WALG_DELTA_ORIGIN=LATEST_FULL`) means delta from the chain's root
/// full backup, vs `LATEST` (default) which means delta from whichever
/// backup is most recent — full or delta
#[derive(Debug, Clone, Default)]
pub struct DeltaSettings {
    pub max_steps: u32,
    pub from_full: bool,
    pub from_name: Option<String>,
    pub from_user_data: Option<String>,
}

#[derive(Debug, Clone)]
pub enum StorageSettings {
    Fs { path: String },
    S3(s3::S3Config),
    Gcs(gcs::GcsConfig),
}

impl Default for Settings {
    /// Convenience defaults: single-worker fs pipeline at lz4, no throttling
    /// or encryption. Production constructs via [`Settings::from_env`]; this
    /// lets tests vary only the fields they exercise via `..Default::default()`
    fn default() -> Self {
        Settings {
            storage: StorageSettings::Fs {
                path: String::new(),
            },
            compression: compression::Method::Lz4,
            compression_level: 3,
            upload_concurrency: 1,
            upload_queue: 1,
            download_concurrency: 1,
            prevent_wal_overwrite: false,
            use_wal_delta: false,
            retry: RetryPolicy::default(),
            network_rate_limit: 0,
            disk_rate_limit: 0,
            delta: DeltaSettings::default(),
            crypter: None,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let storage = detect_storage()?;
        let compression = match std::env::var("WALG_COMPRESSION_METHOD").ok().as_deref() {
            None => compression::Method::Lz4,
            Some(s) => compression::Method::from_name(s)
                .ok_or_else(|| anyhow!("unsupported WALG_COMPRESSION_METHOD={s}"))?,
        };
        let compression_level = parse_env_int("WALG_COMPRESSION_LEVEL", 1)? as i32;
        let upload_concurrency = upload_concurrency_from_env()?;
        let upload_queue = parse_env_int("WALG_UPLOAD_QUEUE", 2)?.max(1) as usize;
        let download_concurrency = download_concurrency_from_env()?;
        let prevent_wal_overwrite = parse_env_bool("WALG_PREVENT_WAL_OVERWRITE", false)?;
        let use_wal_delta = parse_env_bool("WALG_USE_WAL_DELTA", false)?;
        let retry = RetryPolicy::from_env();
        let network_rate_limit = parse_env_int("WALG_NETWORK_RATE_LIMIT", 0)?.max(0) as u64;
        let disk_rate_limit = parse_env_int("WALG_DISK_RATE_LIMIT", 0)?.max(0) as u64;
        let delta = DeltaSettings::from_env()?;
        let crypter = crypto::from_env()?;
        Ok(Settings {
            storage,
            compression,
            compression_level,
            upload_concurrency,
            upload_queue,
            download_concurrency,
            prevent_wal_overwrite,
            use_wal_delta,
            retry,
            network_rate_limit,
            disk_rate_limit,
            delta,
            crypter,
        })
    }

    /// Wrap a plaintext reader with the configured encryption. No-op when
    /// no crypter is configured
    pub fn encrypt(
        &self,
        reader: crate::compression::AsyncReader,
    ) -> crate::compression::AsyncReader {
        match self.crypter.as_ref() {
            Some(c) => c.encrypt_reader(reader),
            None => reader,
        }
    }

    /// Wrap a ciphertext reader with the configured decryption. No-op when
    /// no crypter is configured. Bucket layout doesn't tell us whether a
    /// given object is encrypted, so callers must apply this consistently;
    /// mixed plaintext/ciphertext buckets are not supported (matches wal-g)
    pub fn decrypt(
        &self,
        reader: crate::compression::AsyncReader,
    ) -> crate::compression::AsyncReader {
        match self.crypter.as_ref() {
            Some(c) => c.decrypt_reader(reader),
            None => reader,
        }
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
        Self::build_storage_for(&self.storage, self.retry)
    }

    /// Construct a storage handle for a destination URI like `file:///tmp/x`,
    /// `s3://bucket/prefix`, `gs://bucket/prefix`. Inherits credentials &
    /// retry policy from the current Settings; lets `copy` target a different
    /// prefix or bucket without reconfiguring the global env
    pub fn build_dst_storage(&self, uri: &str) -> Result<DynStorage> {
        let dst = storage_from_uri(uri, &self.storage)?;
        Self::build_storage_for(&dst, self.retry)
    }

    fn build_storage_for(s: &StorageSettings, policy: RetryPolicy) -> Result<DynStorage> {
        match s {
            StorageSettings::Fs { path } => {
                // local fs: skip retry wrapper; no transient failures worth retrying
                let s = FsStorage::new(path).context("init fs storage")?;
                Ok(Arc::new(s) as Arc<dyn Storage>)
            }
            StorageSettings::S3(c) => {
                let s = s3::S3Storage::with_retry_policy(c.clone(), policy)
                    .context("init s3 storage")?;
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

/// Build `StorageSettings` from a destination URI, inheriting credentials
/// from the source settings. Cross-scheme is allowed; cross-bucket within
/// the same scheme is allowed too. Bare paths (`/tmp/foo`) are treated as fs
fn storage_from_uri(uri: &str, src: &StorageSettings) -> Result<StorageSettings> {
    if let Some(rest) = uri.strip_prefix("file://") {
        return Ok(StorageSettings::Fs {
            path: rest.to_string(),
        });
    }
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = split_bucket_prefix(rest);
        let s3_src = match src {
            StorageSettings::S3(c) => Some(c),
            _ => None,
        };
        return Ok(StorageSettings::S3(s3_config_from_env(
            bucket, prefix, s3_src,
        )?));
    }
    if let Some(rest) = uri.strip_prefix("gs://") {
        let (bucket, prefix) = split_bucket_prefix(rest);
        let credentials_path = match src {
            StorageSettings::Gcs(c) => c.credentials_path.clone(),
            _ => None,
        }
        .or_else(|| std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok());
        return Ok(StorageSettings::Gcs(gcs::GcsConfig {
            bucket,
            prefix,
            credentials_path,
        }));
    }
    // bare path falls back to fs
    Ok(StorageSettings::Fs {
        path: uri.to_string(),
    })
}

fn split_bucket_prefix(rest: &str) -> (String, String) {
    match rest.split_once('/') {
        Some((b, p)) => (b.to_string(), p.trim_end_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    }
}

/// Resolve an `S3Config` for `bucket`/`prefix`, layering credential fields.
/// `src` (an existing S3 source for `backup-copy`) takes priority; otherwise
/// fall back to env honoring every wal-g alias so detection & destination
/// resolution read the same names: AWS_REGION/WALG_S3_REGION,
/// AWS_ACCESS_KEY_ID/AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY/AWS_SECRET_KEY,
/// AWS_SESSION_TOKEN, AWS_ENDPOINT_URL/WALG_S3_ENDPOINT, WALG_S3_FORCE_PATH_STYLE
fn s3_config_from_env(
    bucket: String,
    prefix: String,
    src: Option<&s3::S3Config>,
) -> Result<s3::S3Config> {
    let region = src
        .map(|c| c.region.clone())
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("WALG_S3_REGION").ok())
        .unwrap_or_else(|| "us-east-1".into());
    let creds = s3_credentials(src)?;
    let endpoint = src
        .and_then(|c| c.endpoint.clone())
        .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
        .or_else(|| std::env::var("WALG_S3_ENDPOINT").ok());
    let force_path_style = match src {
        Some(c) => c.force_path_style,
        None => parse_env_bool("WALG_S3_FORCE_PATH_STYLE", endpoint.is_some())?,
    };
    Ok(s3::S3Config {
        bucket,
        prefix,
        region,
        creds,
        endpoint,
        force_path_style,
    })
}

/// Pick a credential source: inherit `src`, else explicit static env keys,
/// else the EC2 metadata service. IMDS is skipped (surfacing the missing-keys
/// error) when AWS_EC2_METADATA_DISABLED is set. One static key without the
/// other is a hard error rather than a silent IMDS fallback
fn s3_credentials(src: Option<&s3::S3Config>) -> Result<s3::CredentialSource> {
    if let Some(c) = src {
        return Ok(c.creds.clone());
    }
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| std::env::var("AWS_ACCESS_KEY").ok());
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .or_else(|| std::env::var("AWS_SECRET_KEY").ok());
    match (access_key, secret_key) {
        (Some(access_key), Some(secret_key)) => Ok(s3::CredentialSource::Static(s3::Credentials {
            access_key,
            secret_key,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
            expires_at: None,
        })),
        (None, None) if parse_env_bool("AWS_EC2_METADATA_DISABLED", false)? => {
            Err(anyhow!("AWS_ACCESS_KEY_ID not set and IMDS disabled"))
        }
        (None, None) => Ok(s3::CredentialSource::Imds(Arc::new(
            s3::ImdsProvider::from_env().map_err(|e| anyhow!("{e}"))?,
        ))),
        _ => Err(anyhow!(
            "incomplete static credentials: set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        )),
    }
}

impl DeltaSettings {
    pub fn from_env() -> Result<Self> {
        let max_steps = parse_env_int("WALG_DELTA_MAX_STEPS", 0)?.max(0) as u32;
        let origin = std::env::var("WALG_DELTA_ORIGIN").ok();
        let from_full = match origin.as_deref() {
            None | Some("LATEST") => false,
            Some("LATEST_FULL") => true,
            Some(s) => bail!("WALG_DELTA_ORIGIN={s} must be LATEST or LATEST_FULL"),
        };
        let from_name = std::env::var("WALG_DELTA_FROM_NAME").ok();
        let from_user_data = std::env::var("WALG_DELTA_FROM_USER_DATA").ok();
        Ok(Self {
            max_steps,
            from_full,
            from_name,
            from_user_data,
        })
    }

    pub fn enabled(&self) -> bool {
        self.max_steps > 0
    }
}

fn detect_storage() -> Result<StorageSettings> {
    if let Ok(prefix) = std::env::var("WALG_FILE_PREFIX") {
        return Ok(StorageSettings::Fs { path: prefix });
    }
    if let Ok(s3_prefix) = std::env::var("WALG_S3_PREFIX") {
        let (bucket, prefix) = parse_uri_prefix(&s3_prefix, "s3://")?;
        return Ok(StorageSettings::S3(s3_config_from_env(
            bucket, prefix, None,
        )?));
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

/// `WALG_UPLOAD_CONCURRENCY`; read before runtime construction to cap
/// worker threads for backup-push
pub fn upload_concurrency_from_env() -> Result<usize> {
    Ok(parse_env_int("WALG_UPLOAD_CONCURRENCY", 4)?.max(1) as usize)
}

/// `WALG_DOWNLOAD_CONCURRENCY`; read before runtime construction to cap
/// worker threads for fetch-side commands
pub fn download_concurrency_from_env() -> Result<usize> {
    Ok(parse_env_int("WALG_DOWNLOAD_CONCURRENCY", 4)?.max(1) as usize)
}

/// Parse a Go-style duration (`time.ParseDuration`): one or more
/// `<number><unit>` segments, units ns/us/µs/ms/s/m/h, e.g. `60s`, `1h30m`,
/// `300ms`. `0` is the only unitless value. Used for `WALG_*_TIMEOUT` env +
/// daemon-client flags so values stay copy-paste compatible with wal-g.
/// Returns a `String` error so it doubles as a clap `value_parser`
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty duration".into());
    }
    if t == "0" {
        return Ok(Duration::ZERO);
    }
    let mut rest = t;
    let mut total = Duration::ZERO;
    let mut saw_unit = false;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(format!("invalid duration {s:?}: expected number"));
        }
        let value: f64 = rest[..num_end]
            .parse()
            .map_err(|_| format!("invalid duration {s:?}: bad number {:?}", &rest[..num_end]))?;
        rest = &rest[num_end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let scale_ns: f64 = match &rest[..unit_end] {
            "ns" => 1.0,
            "us" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            "" => return Err(format!("invalid duration {s:?}: missing unit")),
            other => return Err(format!("invalid duration {s:?}: unknown unit {other:?}")),
        };
        total += Duration::from_nanos((value * scale_ns) as u64);
        saw_unit = true;
        rest = &rest[unit_end..];
    }
    if !saw_unit {
        return Err(format!("invalid duration {s:?}"));
    }
    Ok(total)
}

/// Read a Go-style duration env var, falling back to `default` when unset
pub fn duration_env(key: &str, default: Duration) -> Result<Duration> {
    match std::env::var(key) {
        Err(_) => Ok(default),
        Ok(v) => parse_duration(&v).map_err(|e| anyhow!("{key}: {e}")),
    }
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
    use std::sync::Mutex;

    // set_var/remove_var are unsafe in edition 2024 and process-global;
    // serialize env-touching tests so they can't observe each other's writes
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn static_creds(c: &s3::S3Config) -> &s3::Credentials {
        match &c.creds {
            s3::CredentialSource::Static(cr) => cr,
            other => panic!("expected static creds, got {other:?}"),
        }
    }

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        saved: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn new(vars: &[(&str, Option<&str>)]) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let saved = vars
                .iter()
                .map(|(k, v)| {
                    let prev = std::env::var(k).ok();
                    unsafe {
                        match v {
                            Some(val) => std::env::set_var(k, val),
                            None => std::env::remove_var(k),
                        }
                    }
                    (k.to_string(), prev)
                })
                .collect();
            EnvGuard { _lock: lock, saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.saved {
                unsafe {
                    match v {
                        Some(val) => std::env::set_var(k, val),
                        None => std::env::remove_var(k),
                    }
                }
            }
        }
    }

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

    #[test]
    fn parse_uri_prefix_trims_trailing_slash_and_rejects_empty_bucket() {
        let (b, p) = parse_uri_prefix("s3://bucket/some/prefix/", "s3://").unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(p, "some/prefix");
        assert!(parse_uri_prefix("s3:///prefix", "s3://").is_err());
    }

    #[test]
    fn s3_dst_from_non_s3_src_honors_walg_aliases() {
        // file://->s3:// copy: no S3 source to inherit, so credential fields
        // come from env. Must read the same aliases as detect_storage, not just
        // the bare AWS_* names
        let vars = [
            ("AWS_REGION", None),
            ("WALG_S3_REGION", Some("eu-west-2")),
            ("AWS_ACCESS_KEY_ID", None),
            ("AWS_ACCESS_KEY", Some("AKIA_ALIAS")),
            ("AWS_SECRET_ACCESS_KEY", None),
            ("AWS_SECRET_KEY", Some("secret_alias")),
            ("AWS_SESSION_TOKEN", None),
            ("AWS_ENDPOINT_URL", None),
            ("WALG_S3_ENDPOINT", Some("http://minio:9000")),
            ("WALG_S3_FORCE_PATH_STYLE", Some("true")),
        ];
        let _g = EnvGuard::new(&vars);
        let src = StorageSettings::Fs { path: "/x".into() };
        match storage_from_uri("s3://bkt/pre/fix", &src).unwrap() {
            StorageSettings::S3(c) => {
                assert_eq!(c.bucket, "bkt");
                assert_eq!(c.prefix, "pre/fix");
                assert_eq!(c.region, "eu-west-2");
                assert_eq!(static_creds(&c).access_key, "AKIA_ALIAS");
                assert_eq!(static_creds(&c).secret_key, "secret_alias");
                assert_eq!(c.endpoint.as_deref(), Some("http://minio:9000"));
                assert!(c.force_path_style);
            }
            other => panic!("expected S3, got {other:?}"),
        }
    }

    #[test]
    fn parse_env_int_default_valid_and_malformed() {
        let key = "WALRUS_TEST_PARSE_INT";
        {
            let _g = EnvGuard::new(&[(key, None)]);
            assert_eq!(parse_env_int(key, 7).unwrap(), 7);
        }
        {
            let _g = EnvGuard::new(&[(key, Some("42"))]);
            assert_eq!(parse_env_int(key, 7).unwrap(), 42);
        }
        {
            let _g = EnvGuard::new(&[(key, Some("-5"))]);
            assert_eq!(parse_env_int(key, 7).unwrap(), -5);
        }
        for bad in ["abc", "", "1.5", "9999999999999999999999"] {
            let _g = EnvGuard::new(&[(key, Some(bad))]);
            assert!(parse_env_int(key, 7).is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn parse_env_bool_tokens_and_rejection() {
        let key = "WALRUS_TEST_PARSE_BOOL";
        for t in ["1", "true", "TRUE", "yes", "On"] {
            let _g = EnvGuard::new(&[(key, Some(t))]);
            assert!(parse_env_bool(key, false).unwrap(), "{t:?} should be true");
        }
        for f in ["0", "false", "NO", "off"] {
            let _g = EnvGuard::new(&[(key, Some(f))]);
            assert!(!parse_env_bool(key, true).unwrap(), "{f:?} should be false");
        }
        for bad in ["maybe", "", "2"] {
            let _g = EnvGuard::new(&[(key, Some(bad))]);
            assert!(parse_env_bool(key, false).is_err(), "{bad:?} should error");
        }
        {
            let _g = EnvGuard::new(&[(key, None)]);
            assert!(parse_env_bool(key, true).unwrap());
            assert!(!parse_env_bool(key, false).unwrap());
        }
    }

    #[test]
    fn parse_duration_units_and_compounds() {
        assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("300ms").unwrap(), Duration::from_millis(300));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("1.5h").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("500us").unwrap(), Duration::from_micros(500));
        assert_eq!(parse_duration("100ns").unwrap(), Duration::from_nanos(100));
        for bad in ["", "abc", "10", "5sec", "-5s", "s", "1x"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn split_bucket_prefix_variants() {
        assert_eq!(
            split_bucket_prefix("bkt/some/prefix"),
            ("bkt".into(), "some/prefix".into())
        );
        // trailing slash trimmed
        assert_eq!(
            split_bucket_prefix("bkt/some/prefix/"),
            ("bkt".into(), "some/prefix".into())
        );
        // bucket only, no slash -> empty prefix
        assert_eq!(split_bucket_prefix("bkt"), ("bkt".into(), String::new()));
    }

    #[test]
    fn storage_from_uri_file_and_bare_path() {
        let src = StorageSettings::Fs { path: "/x".into() };
        match storage_from_uri("file:///tmp/dst", &src).unwrap() {
            StorageSettings::Fs { path } => assert_eq!(path, "/tmp/dst"),
            other => panic!("expected Fs, got {other:?}"),
        }
        // bare path with no scheme falls back to fs verbatim
        match storage_from_uri("/var/backups", &src).unwrap() {
            StorageSettings::Fs { path } => assert_eq!(path, "/var/backups"),
            other => panic!("expected Fs, got {other:?}"),
        }
    }

    #[test]
    fn storage_from_uri_gs_inherits_then_falls_back_to_env() {
        // gs src carries credentials_path -> inherited, env ignored
        {
            let _g = EnvGuard::new(&[("GOOGLE_APPLICATION_CREDENTIALS", Some("/env/sa.json"))]);
            let src = StorageSettings::Gcs(gcs::GcsConfig {
                bucket: "srcb".into(),
                prefix: "srcp".into(),
                credentials_path: Some("/src/sa.json".into()),
            });
            match storage_from_uri("gs://dstb/dst/pre", &src).unwrap() {
                StorageSettings::Gcs(c) => {
                    assert_eq!(c.bucket, "dstb");
                    assert_eq!(c.prefix, "dst/pre");
                    assert_eq!(c.credentials_path.as_deref(), Some("/src/sa.json"));
                }
                other => panic!("expected Gcs, got {other:?}"),
            }
        }
        // non-gcs src -> credentials_path falls back to env
        {
            let _g = EnvGuard::new(&[("GOOGLE_APPLICATION_CREDENTIALS", Some("/env/sa.json"))]);
            let src = StorageSettings::Fs { path: "/x".into() };
            match storage_from_uri("gs://b", &src).unwrap() {
                StorageSettings::Gcs(c) => {
                    assert_eq!(c.bucket, "b");
                    assert_eq!(c.prefix, "");
                    assert_eq!(c.credentials_path.as_deref(), Some("/env/sa.json"));
                }
                other => panic!("expected Gcs, got {other:?}"),
            }
        }
    }

    #[test]
    fn storage_from_uri_s3_inherits_credentials_from_s3_src() {
        // S3 source -> every credential field copied, env not consulted
        let _g = EnvGuard::new(&[
            ("AWS_REGION", None),
            ("AWS_ACCESS_KEY_ID", None),
            ("AWS_SECRET_ACCESS_KEY", None),
        ]);
        let src = StorageSettings::S3(s3::S3Config {
            bucket: "srcb".into(),
            prefix: "srcp".into(),
            region: "ap-south-1".into(),
            creds: s3::CredentialSource::Static(s3::Credentials {
                access_key: "AKIASRC".into(),
                secret_key: "secretsrc".into(),
                session_token: Some("toksrc".into()),
                expires_at: None,
            }),
            endpoint: Some("http://ceph:7480".into()),
            force_path_style: true,
        });
        match storage_from_uri("s3://dstb/dst", &src).unwrap() {
            StorageSettings::S3(c) => {
                assert_eq!(c.bucket, "dstb");
                assert_eq!(c.prefix, "dst");
                assert_eq!(c.region, "ap-south-1");
                assert_eq!(static_creds(&c).access_key, "AKIASRC");
                assert_eq!(static_creds(&c).secret_key, "secretsrc");
                assert_eq!(static_creds(&c).session_token.as_deref(), Some("toksrc"));
                assert_eq!(c.endpoint.as_deref(), Some("http://ceph:7480"));
                assert!(c.force_path_style);
            }
            other => panic!("expected S3, got {other:?}"),
        }
    }

    #[test]
    fn detect_storage_arms() {
        // file prefix wins
        {
            let _g = EnvGuard::new(&[
                ("WALG_FILE_PREFIX", Some("/srv/wal")),
                ("WALG_S3_PREFIX", None),
                ("WALG_GS_PREFIX", None),
            ]);
            match detect_storage().unwrap() {
                StorageSettings::Fs { path } => assert_eq!(path, "/srv/wal"),
                other => panic!("expected Fs, got {other:?}"),
            }
        }
        // s3 prefix with credential env
        {
            let _g = EnvGuard::new(&[
                ("WALG_FILE_PREFIX", None),
                ("WALG_S3_PREFIX", Some("s3://mybkt/walg")),
                ("WALG_GS_PREFIX", None),
                ("AWS_REGION", Some("us-west-1")),
                ("WALG_S3_REGION", None),
                ("AWS_ACCESS_KEY_ID", Some("AKID")),
                ("AWS_ACCESS_KEY", None),
                ("AWS_SECRET_ACCESS_KEY", Some("SEKRIT")),
                ("AWS_SECRET_KEY", None),
                ("AWS_SESSION_TOKEN", None),
                ("AWS_ENDPOINT_URL", None),
                ("WALG_S3_ENDPOINT", None),
                ("WALG_S3_FORCE_PATH_STYLE", None),
            ]);
            match detect_storage().unwrap() {
                StorageSettings::S3(c) => {
                    assert_eq!(c.bucket, "mybkt");
                    assert_eq!(c.prefix, "walg");
                    assert_eq!(c.region, "us-west-1");
                    assert_eq!(static_creds(&c).access_key, "AKID");
                    assert_eq!(static_creds(&c).secret_key, "SEKRIT");
                    // no endpoint -> path style defaults off
                    assert!(!c.force_path_style);
                }
                other => panic!("expected S3, got {other:?}"),
            }
        }
        // s3 prefix, no static keys -> IMDS credential source (no network here,
        // the provider only builds its client)
        {
            let _g = EnvGuard::new(&[
                ("WALG_FILE_PREFIX", None),
                ("WALG_S3_PREFIX", Some("s3://mybkt")),
                ("WALG_GS_PREFIX", None),
                ("AWS_ACCESS_KEY_ID", None),
                ("AWS_ACCESS_KEY", None),
                ("AWS_SECRET_ACCESS_KEY", None),
                ("AWS_SECRET_KEY", None),
                ("AWS_EC2_METADATA_DISABLED", None),
            ]);
            match detect_storage().unwrap() {
                StorageSettings::S3(c) => {
                    assert!(matches!(c.creds, s3::CredentialSource::Imds(_)));
                }
                other => panic!("expected S3, got {other:?}"),
            }
        }
        // s3 prefix, no static keys, IMDS disabled -> error
        {
            let _g = EnvGuard::new(&[
                ("WALG_FILE_PREFIX", None),
                ("WALG_S3_PREFIX", Some("s3://mybkt")),
                ("WALG_GS_PREFIX", None),
                ("AWS_ACCESS_KEY_ID", None),
                ("AWS_ACCESS_KEY", None),
                ("AWS_SECRET_ACCESS_KEY", None),
                ("AWS_SECRET_KEY", None),
                ("AWS_EC2_METADATA_DISABLED", Some("true")),
            ]);
            assert!(detect_storage().is_err());
        }
        // gs prefix, credentials path from env (path not opened here)
        {
            let _g = EnvGuard::new(&[
                ("WALG_FILE_PREFIX", None),
                ("WALG_S3_PREFIX", None),
                ("WALG_GS_PREFIX", Some("gs://gbkt/walg/")),
                ("GOOGLE_APPLICATION_CREDENTIALS", Some("/creds/sa.json")),
            ]);
            match detect_storage().unwrap() {
                StorageSettings::Gcs(c) => {
                    assert_eq!(c.bucket, "gbkt");
                    assert_eq!(c.prefix, "walg");
                    assert_eq!(c.credentials_path.as_deref(), Some("/creds/sa.json"));
                }
                other => panic!("expected Gcs, got {other:?}"),
            }
        }
        // nothing configured -> error
        {
            let _g = EnvGuard::new(&[
                ("WALG_FILE_PREFIX", None),
                ("WALG_S3_PREFIX", None),
                ("WALG_GS_PREFIX", None),
            ]);
            assert!(detect_storage().is_err());
        }
    }

    #[test]
    fn build_storage_for_each_backend() {
        let dir = tempfile::tempdir().unwrap();
        // fs: no retry wrapper
        let fs = Settings::build_storage_for(
            &StorageSettings::Fs {
                path: dir.path().to_string_lossy().into(),
            },
            RetryPolicy::default(),
        )
        .unwrap();
        assert!(fs.describe().starts_with("file://"));

        // s3: client construction only, no IO
        let s3 = Settings::build_storage_for(
            &StorageSettings::S3(s3::S3Config {
                bucket: "b".into(),
                prefix: "p".into(),
                region: "us-east-1".into(),
                creds: s3::CredentialSource::Static(s3::Credentials {
                    access_key: "AKID".into(),
                    secret_key: "sek".into(),
                    session_token: None,
                    expires_at: None,
                }),
                endpoint: None,
                force_path_style: false,
            }),
            RetryPolicy::default(),
        )
        .unwrap();
        assert_eq!(s3.describe(), "s3://b/p");

        // gcs: a credentials file lets new() succeed without env or network
        // (avoids racing the gcs WALG_GS_ENDPOINT unit test)
        let sa = dir.path().join("sa.json");
        std::fs::write(&sa, r#"{"client_email":"x@y","private_key":"dummy"}"#).unwrap();
        let gcs = Settings::build_storage_for(
            &StorageSettings::Gcs(gcs::GcsConfig {
                bucket: "gb".into(),
                prefix: "gp".into(),
                credentials_path: Some(sa.to_string_lossy().into()),
            }),
            RetryPolicy::default(),
        )
        .unwrap();
        assert_eq!(gcs.describe(), "gs://gb/gp");
    }

    #[test]
    fn build_dst_storage_resolves_uri() {
        let dir = tempfile::tempdir().unwrap();
        let settings = Settings {
            storage: StorageSettings::Fs {
                path: dir.path().to_string_lossy().into(),
            },
            ..Settings::default()
        };
        let dst = settings
            .build_dst_storage(&format!("file://{}", dir.path().display()))
            .unwrap();
        assert!(dst.describe().starts_with("file://"));

        // build_storage (instance) rides the same path
        assert!(
            settings
                .build_storage()
                .unwrap()
                .describe()
                .starts_with("file://")
        );
    }

    #[test]
    fn delta_settings_origin_and_steps() {
        let keys = [
            "WALG_DELTA_MAX_STEPS",
            "WALG_DELTA_ORIGIN",
            "WALG_DELTA_FROM_NAME",
            "WALG_DELTA_FROM_USER_DATA",
        ];
        let clear: Vec<(&str, Option<&str>)> = keys.iter().map(|k| (*k, None)).collect();
        // Unset → disabled, LATEST semantics
        {
            let _g = EnvGuard::new(&clear);
            let d = DeltaSettings::from_env().unwrap();
            assert!(!d.enabled());
            assert!(!d.from_full);
            assert_eq!(d.max_steps, 0);
        }
        // LATEST_FULL with steps
        {
            let mut v = clear.clone();
            v[0] = ("WALG_DELTA_MAX_STEPS", Some("3"));
            v[1] = ("WALG_DELTA_ORIGIN", Some("LATEST_FULL"));
            let _g = EnvGuard::new(&v);
            let d = DeltaSettings::from_env().unwrap();
            assert!(d.enabled());
            assert!(d.from_full);
            assert_eq!(d.max_steps, 3);
        }
        // Explicit LATEST → not from_full
        {
            let mut v = clear.clone();
            v[1] = ("WALG_DELTA_ORIGIN", Some("LATEST"));
            let _g = EnvGuard::new(&v);
            assert!(!DeltaSettings::from_env().unwrap().from_full);
        }
        // Garbage origin → error
        {
            let mut v = clear.clone();
            v[1] = ("WALG_DELTA_ORIGIN", Some("SIDEWAYS"));
            let _g = EnvGuard::new(&v);
            assert!(DeltaSettings::from_env().is_err());
        }
    }
}
