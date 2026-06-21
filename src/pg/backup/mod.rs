//! Base backup objects: storage layout, name parsing, sentinel & metadata DTOs
//!
//! Wire format mirrors wal-g so walrus and wal-g can share buckets

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod copy;
pub mod delete;
pub mod delta;
pub mod fetch;
pub mod increment;
pub mod list;
pub mod push;
pub mod show;
pub mod tar_streamer;
pub mod wal_delta;

pub const SENTINEL_SUFFIX: &str = "_backup_stop_sentinel.json";
pub const METADATA_FILENAME: &str = "metadata.json";
pub const FILES_METADATA_FILENAME: &str = "files_metadata.json";
pub const TAR_PARTITIONS: &str = "tar_partitions";
pub const PG_CONTROL_TARNAME: &str = "pg_control.tar";
pub const BACKUP_NAME_PREFIX: &str = "base_";
pub const LATEST: &str = "LATEST";
pub const METADATA_DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S.%fZ";

/// Storage path of the sentinel JSON for `name`
pub fn sentinel_key(name: &str) -> String {
    format!(
        "{}/{}{}",
        crate::pg::BASEBACKUP_FOLDER,
        name,
        SENTINEL_SUFFIX
    )
}

pub fn metadata_key(name: &str) -> String {
    format!(
        "{}/{}/{}",
        crate::pg::BASEBACKUP_FOLDER,
        name,
        METADATA_FILENAME
    )
}

pub fn files_metadata_key(name: &str) -> String {
    format!(
        "{}/{}/{}",
        crate::pg::BASEBACKUP_FOLDER,
        name,
        FILES_METADATA_FILENAME
    )
}

pub fn tar_partitions_prefix(name: &str) -> String {
    format!(
        "{}/{}/{}",
        crate::pg::BASEBACKUP_FOLDER,
        name,
        TAR_PARTITIONS
    )
}

pub fn tar_part_key(name: &str, file_no: u32, ext: &str) -> String {
    let base = format!("part_{:03}.tar", file_no);
    if ext.is_empty() {
        format!(
            "{}/{}/{}/{}",
            crate::pg::BASEBACKUP_FOLDER,
            name,
            TAR_PARTITIONS,
            base
        )
    } else {
        format!(
            "{}/{}/{}/{}.{}",
            crate::pg::BASEBACKUP_FOLDER,
            name,
            TAR_PARTITIONS,
            base,
            ext
        )
    }
}

/// `base_TTTTTTTTLLLLLLLLSSSSSSSS` from start LSN, using xlog_internal.h math
pub fn format_backup_name(timeline: u32, start_lsn: u64, seg_size: u64) -> String {
    assert!(seg_size > 0 && seg_size.is_power_of_two());
    let seg_no = start_lsn / seg_size;
    let xlog_segs_per_xlog_id = 0x1_0000_0000u64 / seg_size;
    let log_id = (seg_no / xlog_segs_per_xlog_id) as u32;
    let seg_low = (seg_no % xlog_segs_per_xlog_id) as u32;
    format!(
        "{}{:08X}{:08X}{:08X}",
        BACKUP_NAME_PREFIX, timeline, log_id, seg_low
    )
}

/// Inverse of [`format_backup_name`]: parse the timeline ID from the first
/// 8 hex chars after the `base_` prefix. Returns `None` when the name lacks
/// the prefix, is too short, or contains non-hex digits.
pub fn parse_timeline_from_backup_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix(BACKUP_NAME_PREFIX)?;
    if rest.len() < 8 {
        return None;
    }
    u32::from_str_radix(&rest[..8], 16).ok()
}

/// Parse `0/1A2B3C4D` (postgres pg_lsn text form) into u64
pub fn parse_pg_lsn(s: &str) -> Result<u64> {
    let s = s.trim();
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow!("bad LSN format: {s}"))?;
    let hi = u64::from_str_radix(hi, 16).with_context(|| format!("bad LSN hi: {hi}"))?;
    let lo = u64::from_str_radix(lo, 16).with_context(|| format!("bad LSN lo: {lo}"))?;
    Ok((hi << 32) | lo)
}

pub fn format_pg_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

/// Match `base_<24hex>` and optional `_D_<24hex>` delta and `_<8hex>` LSN
pub fn looks_like_backup_name(s: &str) -> bool {
    let Some(rest) = s.strip_prefix(BACKUP_NAME_PREFIX) else {
        return false;
    };
    if rest.len() < 24 {
        return false;
    }
    rest[..24].chars().all(|c| c.is_ascii_hexdigit())
}

/// Strip wal-g sentinel suffix to recover backup name
pub fn name_from_sentinel_key(key: &str) -> Option<&str> {
    let bare = key.rsplit('/').next().unwrap_or(key);
    bare.strip_suffix(SENTINEL_SUFFIX)
}

/// Extract the leftmost backup name from an object key under the basebackups
/// prefix. Mirrors wal-g's `utility.StripLeftmostBackupName`:
/// split on `/`, take the first segment, drop the `_backup*` sentinel suffix
///
/// `basebackups_005/base_X_backup_stop_sentinel.json` -> `Some("base_X")`
/// `basebackups_005/base_X/tar_partitions/part_001.tar.zst` -> `Some("base_X")`
/// `basebackups_005/base_X_D_Y/files_metadata.json` -> `Some("base_X_D_Y")`
pub fn strip_leftmost_backup_name(key: &str) -> Option<&str> {
    let prefix = format!("{}/", crate::pg::BASEBACKUP_FOLDER);
    let rel = key.strip_prefix(&prefix).unwrap_or(key);
    let rel = rel.trim_start_matches('/');
    let first = rel.split('/').next()?;
    // Drop sentinel & metadata suffixes that share `_backup` (sentinel,
    // backup_log, etc). Delta backup names contain `_D_` which doesn't match
    let stripped = first.split("_backup").next().unwrap_or(first);
    if stripped.is_empty() {
        None
    } else {
        Some(stripped)
    }
}

/// Fetch `key` from `storage` and deserialize as JSON into `T`. `buf_hint` is
/// the initial allocation for the in-memory buffer (callers know the rough
/// blob size). Error chain: `get {key}` → underlying read error → `parse {key}`
pub(crate) async fn load_json<T: serde::de::DeserializeOwned>(
    storage: &crate::storage::DynStorage,
    key: &str,
    buf_hint: usize,
) -> Result<T> {
    use tokio::io::AsyncReadExt;
    let mut r = storage
        .get(key)
        .await
        .with_context(|| format!("get {key}"))?;
    let mut buf = Vec::with_capacity(buf_hint);
    r.read_to_end(&mut buf).await?;
    serde_json::from_slice(&buf).with_context(|| format!("parse {key}"))
}

/// Tablespace map mirrored from wal-g `TablespaceSpec`. JSON shape:
/// ```json
/// {
///   "base_prefix": "/var/lib/pg/16/main",
///   "tablespaces": ["16384", "16385"],
///   "16384": {"loc": "/srv/tblspc/a", "link": "pg_tblspc/16384"},
///   "16385": {"loc": "/srv/tblspc/b", "link": "pg_tblspc/16385"}
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TablespaceSpec {
    pub base_prefix: String,
    pub tablespace_names: Vec<String>,
    pub locations: HashMap<String, TablespaceLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablespaceLocation {
    #[serde(rename = "loc")]
    pub location: String,
    #[serde(rename = "link")]
    pub symlink: String,
}

impl TablespaceSpec {
    pub fn new(base_prefix: impl Into<String>) -> Self {
        Self {
            base_prefix: base_prefix.into(),
            tablespace_names: Vec::new(),
            locations: HashMap::new(),
        }
    }

    pub fn add(&mut self, oid: u32, location: impl Into<String>) {
        let name = oid.to_string();
        let loc = TablespaceLocation {
            location: location.into(),
            symlink: format!("pg_tblspc/{name}"),
        };
        if !self.tablespace_names.iter().any(|n| n == &name) {
            self.tablespace_names.push(name.clone());
        }
        self.locations.insert(name, loc);
    }

    pub fn is_empty(&self) -> bool {
        self.tablespace_names.is_empty()
    }
}

impl Serialize for TablespaceSpec {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(Some(2 + self.locations.len()))?;
        m.serialize_entry("base_prefix", &self.base_prefix)?;
        m.serialize_entry("tablespaces", &self.tablespace_names)?;
        for (k, v) in &self.locations {
            m.serialize_entry(k, v)?;
        }
        m.end()
    }
}

impl<'de> Deserialize<'de> for TablespaceSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let mut raw: serde_json::Map<String, serde_json::Value> = Deserialize::deserialize(d)?;
        let base_prefix = raw
            .remove("base_prefix")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .ok_or_else(|| serde::de::Error::missing_field("base_prefix"))?;
        let names: Vec<String> = match raw.remove("tablespaces") {
            Some(v) => serde_json::from_value(v).map_err(serde::de::Error::custom)?,
            None => Vec::new(),
        };
        let mut locations = HashMap::new();
        for name in &names {
            if let Some(v) = raw.remove(name) {
                let loc: TablespaceLocation =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                locations.insert(name.clone(), loc);
            }
        }
        Ok(TablespaceSpec {
            base_prefix,
            tablespace_names: names,
            locations,
        })
    }
}

/// Sidecar emitted under `<backup>/files_metadata.json`. Mirrors wal-g's
/// `FilesMetadataDto` field-for-field
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilesMetadataDto {
    #[serde(rename = "Files", default, skip_serializing_if = "HashMap::is_empty")]
    pub files: HashMap<String, FileDescription>,
    #[serde(
        rename = "TarFileSets",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub tar_file_sets: HashMap<String, Vec<String>>,
    #[serde(
        rename = "DatabasesByNames",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub databases_by_names: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileDescription {
    #[serde(rename = "IsIncremented", default)]
    pub is_incremented: bool,
    #[serde(rename = "IsSkipped", default)]
    pub is_skipped: bool,
    #[serde(rename = "MTime")]
    pub mtime: DateTime<Utc>,
    #[serde(rename = "UpdatesCount", default)]
    pub updates_count: u64,
}

/// Sentinel: subset of wal-g BackupSentinelDto. Skips delta-backup fields we do not produce
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackupSentinelDto {
    #[serde(rename = "LSN", default)]
    pub backup_start_lsn: Option<u64>,
    #[serde(rename = "DeltaLSN", default, skip_serializing_if = "Option::is_none")]
    pub increment_from_lsn: Option<u64>,
    #[serde(rename = "DeltaFrom", default, skip_serializing_if = "Option::is_none")]
    pub increment_from: Option<String>,
    #[serde(
        rename = "DeltaFullName",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub increment_full_name: Option<String>,
    #[serde(
        rename = "DeltaCount",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub increment_count: Option<i32>,
    /// Wire format of this backup's increment files. Omitted (= `Wi1`) for
    /// full backups & wal-g-compatible `wi1` deltas; present only for native.
    /// Absent on read defaults to `Wi1` (wal-g & pre-field walrus sentinels)
    #[serde(
        rename = "IncrementFormat",
        default,
        skip_serializing_if = "is_default"
    )]
    pub increment_format: increment::Format,

    #[serde(rename = "PgVersion", default)]
    pub pg_version: i32,
    #[serde(rename = "FinishLSN", default)]
    pub backup_finish_lsn: Option<u64>,
    #[serde(
        rename = "SystemIdentifier",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub system_identifier: Option<u64>,

    #[serde(rename = "UncompressedSize")]
    pub uncompressed_size: i64,
    #[serde(rename = "CompressedSize")]
    pub compressed_size: i64,
    #[serde(
        rename = "DataCatalogSize",
        default,
        skip_serializing_if = "is_zero_i64"
    )]
    pub data_catalog_size: i64,

    #[serde(rename = "UserData", default, skip_serializing_if = "Option::is_none")]
    pub user_data: Option<serde_json::Value>,

    #[serde(
        rename = "FilesMetadataDisabled",
        default,
        skip_serializing_if = "is_false"
    )]
    pub files_metadata_disabled: bool,

    #[serde(rename = "Spec", default, skip_serializing_if = "Option::is_none")]
    pub tablespace_spec: Option<TablespaceSpec>,

    #[serde(rename = "ChkpNum", default)]
    pub backup_start_chkp_num: Option<u32>,
    #[serde(
        rename = "DeltaChkpNum",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub increment_from_chkp_num: Option<u32>,
}

/// Extended metadata file emitted alongside sentinel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendedMetadataDto {
    pub start_time: DateTime<Utc>,
    pub finish_time: DateTime<Utc>,
    pub date_fmt: String,
    pub hostname: String,
    pub data_dir: String,
    pub pg_version: i32,
    pub start_lsn: u64,
    pub finish_lsn: u64,
    pub is_permanent: bool,
    #[serde(default)]
    pub system_identifier: Option<u64>,
    pub uncompressed_size: i64,
    pub compressed_size: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_data: Option<serde_json::Value>,
}

/// V2 sentinel union — wal-g writes this form into the sentinel file. Restoring
/// tools accept both V1 and V2 by ignoring extra fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSentinelDtoV2 {
    #[serde(flatten)]
    pub sentinel: BackupSentinelDto,
    #[serde(rename = "Version")]
    pub version: i32,
    #[serde(rename = "StartTime")]
    pub start_time: DateTime<Utc>,
    #[serde(rename = "FinishTime")]
    pub finish_time: DateTime<Utc>,
    #[serde(rename = "DateFmt")]
    pub date_fmt: String,
    #[serde(rename = "Hostname")]
    pub hostname: String,
    #[serde(rename = "DataDir")]
    pub data_dir: String,
    #[serde(rename = "IsPermanent")]
    pub is_permanent: bool,
}

impl Default for BackupSentinelDtoV2 {
    /// Epoch timestamps + empty host/dir; `version` 2 and the standard date
    /// format. Tests override only the fields under test via struct-update
    fn default() -> Self {
        let epoch = DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch valid");
        Self {
            sentinel: BackupSentinelDto::default(),
            version: 2,
            start_time: epoch,
            finish_time: epoch,
            date_fmt: METADATA_DATETIME_FORMAT.into(),
            hostname: String::new(),
            data_dir: String::new(),
            is_permanent: false,
        }
    }
}

/// Shared fixture builders for the `file://`-backed backup/wal command tests
/// (list, show, wal-verify). Seeds a temp FsStorage with sentinels, files-
/// metadata sidecars and WAL segments in the wal-g object layout
#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::*;
    use crate::storage::{AsyncReader, DynStorage, fs::FsStorage};
    use std::sync::Arc;

    pub(crate) fn fs_store(dir: &std::path::Path) -> DynStorage {
        Arc::new(FsStorage::new(dir).unwrap())
    }

    fn reader(bytes: Vec<u8>) -> AsyncReader {
        Box::pin(std::io::Cursor::new(bytes))
    }

    pub(crate) async fn put_bytes(s: &DynStorage, key: &str, bytes: Vec<u8>) {
        let len = bytes.len() as u64;
        s.put(key, reader(bytes), Some(len)).await.unwrap();
    }

    pub(crate) async fn put_sentinel(s: &DynStorage, name: &str, sentinel: &BackupSentinelDtoV2) {
        put_bytes(
            s,
            &sentinel_key(name),
            serde_json::to_vec(sentinel).unwrap(),
        )
        .await;
    }

    pub(crate) async fn put_files_metadata(s: &DynStorage, name: &str, fm: &FilesMetadataDto) {
        put_bytes(
            s,
            &files_metadata_key(name),
            serde_json::to_vec(fm).unwrap(),
        )
        .await;
    }

    pub(crate) async fn put_wal_segment(s: &DynStorage, seg: &str) {
        put_bytes(s, &format!("{}/{seg}", crate::pg::WAL_FOLDER), Vec::new()).await;
    }

    /// 16 MiB-aligned start LSN for segment `seg_no` on log 0
    pub(crate) fn lsn_for_seg(seg_no: u64) -> u64 {
        seg_no * 16 * 1024 * 1024
    }
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    v == &T::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_backup_name() {
        // 16MB segments, LSN 0/3000000 → segment 3, log_id 0, seg_low 3
        let n = format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
        assert_eq!(n, "base_000000010000000000000003");
    }

    #[test]
    fn formats_backup_name_high_logid() {
        // LSN 2/3000000 → log_id 2, seg_low 3
        let lsn = (2u64 << 32) | 0x0300_0000;
        let n = format_backup_name(1, lsn, 16 * 1024 * 1024);
        assert_eq!(n, "base_000000010000000200000003");
    }

    #[test]
    fn parses_lsn() {
        assert_eq!(parse_pg_lsn("0/3000000").unwrap(), 0x0300_0000);
        assert_eq!(
            parse_pg_lsn("2/3000000").unwrap(),
            (2u64 << 32) | 0x0300_0000
        );
        // high word > 10: hex parse must not collapse to decimal
        assert_eq!(parse_pg_lsn("2A/16").unwrap(), (0x2A_u64 << 32) | 0x16);
        assert_eq!(parse_pg_lsn("FF/FF").unwrap(), (0xFF_u64 << 32) | 0xFF);
    }

    #[test]
    fn formats_lsn_uppercase() {
        assert_eq!(format_pg_lsn(0x0300_0000), "0/3000000");
        assert_eq!(format_pg_lsn((2u64 << 32) | 0xab), "2/AB");
        // high word > 10 separates hex from decimal: "2A" vs decimal "42"
        assert_eq!(format_pg_lsn((0x2A_u64 << 32) | 0x16), "2A/16");
        assert_eq!(format_pg_lsn((0xFF_u64 << 32) | 0xFF), "FF/FF");
        assert_eq!(format_pg_lsn(u64::MAX), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn lsn_format_parse_round_trip() {
        for lsn in [
            0,
            0x0300_0000,
            (2u64 << 32) | 0xab,
            (0x2A_u64 << 32) | 0x16,
            (0xA_u64 << 32) | 0xDEAD_BEEF,
            u64::MAX,
        ] {
            assert_eq!(parse_pg_lsn(&format_pg_lsn(lsn)).unwrap(), lsn);
        }
    }

    #[test]
    fn classifies_backup_names() {
        assert!(looks_like_backup_name("base_000000010000000000000003"));
        assert!(looks_like_backup_name(
            "base_000000010000000000000003_D_000000010000000000000001"
        ));
        assert!(!looks_like_backup_name("foo"));
        assert!(!looks_like_backup_name("base_xyz"));
    }

    #[test]
    fn extracts_name_from_sentinel_key() {
        let k = "basebackups_005/base_000000010000000000000003_backup_stop_sentinel.json";
        assert_eq!(
            name_from_sentinel_key(k),
            Some("base_000000010000000000000003")
        );
    }

    #[test]
    fn sentinel_v1_serde_roundtrip() {
        let s = BackupSentinelDto {
            backup_start_lsn: Some(0x0300_0000),
            pg_version: 160003,
            backup_finish_lsn: Some(0x0300_1000),
            system_identifier: Some(7000000000000000000),
            uncompressed_size: 1024,
            compressed_size: 512,
            files_metadata_disabled: true,
            ..Default::default()
        };
        let j = serde_json::to_string(&s).unwrap();
        // wal-g compatibility: keys must be PascalCase JSON
        assert!(j.contains("\"LSN\":50331648"));
        assert!(j.contains("\"FinishLSN\":50335744"));
        assert!(j.contains("\"PgVersion\":160003"));
        assert!(j.contains("\"FilesMetadataDisabled\":true"));
        let back: BackupSentinelDto = serde_json::from_str(&j).unwrap();
        assert_eq!(back.backup_start_lsn, Some(0x0300_0000));
        assert_eq!(back.system_identifier, Some(7000000000000000000));
    }

    #[test]
    fn increment_format_sentinel_field() {
        use increment::Format;
        let mut s = BackupSentinelDto {
            backup_start_lsn: Some(1),
            increment_from_lsn: Some(0),
            increment_from: Some("base_x".into()),
            increment_full_name: Some("base_x".into()),
            increment_count: Some(1),
            increment_format: Format::Native,
            pg_version: 170000,
            backup_finish_lsn: Some(2),
            ..Default::default()
        };
        // Native deltas record the format
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"IncrementFormat\":\"native\""), "{j}");

        // wi1 (default) omits the field — wal-g-compatible sentinel
        s.increment_format = Format::Wi1;
        let j = serde_json::to_string(&s).unwrap();
        assert!(!j.contains("IncrementFormat"), "{j}");

        // Absent field reads as wi1 (wal-g & pre-field walrus sentinels)
        let back: BackupSentinelDto = serde_json::from_str(&j).unwrap();
        assert_eq!(back.increment_format, Format::Wi1);

        // Explicit native parses back
        let back: BackupSentinelDto = serde_json::from_str(
            r#"{"IncrementFormat":"native","UncompressedSize":0,"CompressedSize":0}"#,
        )
        .unwrap();
        assert_eq!(back.increment_format, Format::Native);
    }

    #[test]
    fn tablespace_spec_roundtrips() {
        let mut spec = TablespaceSpec::new("/var/lib/pg/16/main");
        spec.add(16384, "/srv/ts_a");
        spec.add(16385, "/srv/ts_b");
        let j = serde_json::to_value(&spec).unwrap();
        assert_eq!(j["base_prefix"], "/var/lib/pg/16/main");
        let names: Vec<String> = serde_json::from_value(j["tablespaces"].clone()).unwrap();
        assert_eq!(names, vec!["16384", "16385"]);
        assert_eq!(j["16384"]["loc"], "/srv/ts_a");
        assert_eq!(j["16384"]["link"], "pg_tblspc/16384");

        let s = serde_json::to_string(&spec).unwrap();
        let back: TablespaceSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(back.tablespace_names, spec.tablespace_names);
        assert_eq!(back.locations.get("16385").unwrap().location, "/srv/ts_b");
    }

    #[test]
    fn sentinel_v2_extra_fields_present() {
        let s = BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: Some(1),
                pg_version: 160003,
                backup_finish_lsn: Some(2),
                files_metadata_disabled: true,
                ..Default::default()
            },
            hostname: "h".into(),
            data_dir: "/d".into(),
            ..Default::default()
        };
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"Version\":2"));
        assert!(j.contains("\"Hostname\":\"h\""));
        // and the embedded V1 fields
        assert!(j.contains("\"PgVersion\":160003"));
    }
}
