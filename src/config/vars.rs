//! Layered config source mirroring wal-g's viper: process env over a parsed
//! `--config` file. Env wins (viper `AutomaticEnv`). Never mutates process env,
//! so resolution is sound after the runtime starts (no `setenv` race)

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};

#[derive(Debug, Clone, Default)]
pub struct Vars {
    file: HashMap<String, String>,
}

impl Vars {
    /// Parse a wal-g `--config` file. dotenv `KEY=VALUE` by default (doubles as
    /// the systemd `EnvironmentFile`); JSON when the extension is `.json`. Both
    /// yield a flat UPPER_SNAKE keyspace matching wal-g/viper settings
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        let file = match path.extension().and_then(|e| e.to_str()) {
            Some("json") => parse_json(&content, path)?,
            _ => parse_dotenv(&content, path)?,
        };
        Ok(Self { file })
    }

    /// env-first, then config file (viper `AutomaticEnv` precedence). Non-UTF-8
    /// env values read as absent, matching `std::env::var`
    pub fn get(&self, key: &str) -> Option<String> {
        match std::env::var_os(key) {
            Some(v) => v.into_string().ok(),
            None => self.file.get(key).cloned(),
        }
    }

    pub fn get_os(&self, key: &str) -> Option<OsString> {
        std::env::var_os(key).or_else(|| self.file.get(key).map(OsString::from))
    }

    pub fn contains(&self, key: &str) -> bool {
        std::env::var_os(key).is_some() || self.file.contains_key(key)
    }

    pub fn int(&self, key: &str, default: i64) -> Result<i64> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => v.parse().with_context(|| format!("parse {key}={v}")),
        }
    }

    pub fn bool(&self, key: &str, default: bool) -> Result<bool> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => bail!("parse {key}={v} as bool"),
            },
        }
    }

    pub fn duration(&self, key: &str, default: Duration) -> Result<Duration> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => super::parse_duration(&v).map_err(|e| anyhow!("{key}: {e}")),
        }
    }
}

/// dotenv `KEY=VALUE`, `export ` prefix, `#` comments. Errors on a non-comment
/// line lacking `=` or with an empty key (wal-g's viper would silently skip,
/// but a malformed wal-g.env almost always signals operator error)
fn parse_dotenv(content: &str, path: &Path) -> Result<HashMap<String, String>> {
    let mut file = HashMap::new();
    for (i, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (key, val) = line
            .split_once('=')
            .ok_or_else(|| anyhow!("{}:{}: expected KEY=VALUE", path.display(), i + 1))?;
        let key = key.trim();
        if key.is_empty() {
            bail!("{}:{}: empty key", path.display(), i + 1);
        }
        file.insert(key.to_string(), unquote(val.trim()).into_owned());
    }
    Ok(file)
}

/// Flat JSON object of scalar values (wal-g/viper settings are flat keys).
/// Nested objects/arrays are unsupported and error rather than flatten silently
fn parse_json(content: &str, path: &Path) -> Result<HashMap<String, String>> {
    let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(content)
        .with_context(|| format!("parse JSON config {}", path.display()))?;
    let mut file = HashMap::with_capacity(obj.len());
    for (key, val) in obj {
        let s = match val {
            serde_json::Value::String(s) => s,
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Null => continue,
            other => bail!(
                "{}: {key} has unsupported nested value {other}; settings must be scalar",
                path.display()
            ),
        };
        file.insert(key, s);
    }
    Ok(file)
}

/// Strip one layer of matching single or double quotes, dotenv-style, and
/// unescape double-quoted values. Mirrors gotenv (viper's dotenv parser used by
/// wal-g): inside double quotes `\n`/`\r` become newline/carriage-return and
/// any other `\X` collapses to `X`, except `\$` which is preserved. Single
/// quotes and bare values are literal
fn unquote(s: &str) -> std::borrow::Cow<'_, str> {
    let b = s.as_bytes();
    let quoted = b.len() >= 2 && (b[0] == b'"' || b[0] == b'\'') && b[b.len() - 1] == b[0];
    if !quoted {
        return std::borrow::Cow::Borrowed(s);
    }
    let s = &s[1..s.len() - 1];
    if b[0] == b'\'' || !s.contains('\\') {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('$') => out.push_str("\\$"),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    std::borrow::Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unquote_unescapes_double_quoted() {
        // bare + single-quoted: literal
        assert_eq!(unquote("bare"), "bare");
        assert_eq!(unquote(r"'a\nb'"), r"a\nb");
        // double-quoted: gotenv escapes
        assert_eq!(unquote(r#""a\nb""#), "a\nb");
        assert_eq!(unquote(r#""a\rb""#), "a\rb");
        assert_eq!(unquote(r#""a\tb""#), "atb"); // \X -> X, not a tab
        assert_eq!(unquote(r#""a\\b""#), r"a\b");
        assert_eq!(unquote(r#""a\"b""#), "a\"b");
        assert_eq!(unquote(r#""a\$b""#), r"a\$b"); // \$ preserved
        assert_eq!(unquote(r#""plain""#), "plain"); // no escapes: borrows
    }

    #[test]
    fn dotenv_parses_quotes_export_and_comments() {
        let v = parse_dotenv(
            "# comment\n\
             export AWS_REGION=eu-west-3\n\
             \n\
             WALG_S3_PREFIX=\"s3://bkt\"\n\
             LIT='a\\nb'\n",
            Path::new("t.env"),
        )
        .unwrap();
        assert_eq!(v["AWS_REGION"], "eu-west-3");
        assert_eq!(v["WALG_S3_PREFIX"], "s3://bkt");
        assert_eq!(v["LIT"], "a\\nb");
    }

    #[test]
    fn dotenv_rejects_malformed_line() {
        assert!(parse_dotenv("OK=1\nnot_a_pair\n", Path::new("t.env")).is_err());
    }

    #[test]
    fn json_parses_scalars_and_rejects_nested() {
        let v = parse_json(
            r#"{"WALG_S3_PREFIX":"s3://bkt","WALG_THREADS":4,"WALG_USE_WAL_DELTA":true}"#,
            Path::new("t.json"),
        )
        .unwrap();
        assert_eq!(v["WALG_S3_PREFIX"], "s3://bkt");
        assert_eq!(v["WALG_THREADS"], "4");
        assert_eq!(v["WALG_USE_WAL_DELTA"], "true");
        assert!(parse_json(r#"{"A":{"nested":1}}"#, Path::new("t.json")).is_err());
    }

    #[test]
    fn get_prefers_env_over_file() {
        // SAFETY: single-threaded test, unique key, cleaned up
        let key = "WALG_VARS_TEST_PREFERS_ENV";
        let vars = Vars {
            file: HashMap::from([(key.to_string(), "from_file".to_string())]),
        };
        assert_eq!(vars.get(key).as_deref(), Some("from_file"));
        unsafe { std::env::set_var(key, "from_env") };
        assert_eq!(vars.get(key).as_deref(), Some("from_env"));
        unsafe { std::env::remove_var(key) };
    }

    #[test]
    fn typed_accessors_fall_back_to_default() {
        let vars = Vars {
            file: HashMap::from([
                ("N".to_string(), "7".to_string()),
                ("B".to_string(), "yes".to_string()),
            ]),
        };
        assert_eq!(vars.int("N", 1).unwrap(), 7);
        assert_eq!(vars.int("MISSING", 3).unwrap(), 3);
        assert!(vars.bool("B", false).unwrap());
        assert!(!vars.bool("MISSING", false).unwrap());
    }
}
