//! Base backup objects: storage layout, name parsing, sentinel & metadata DTOs
//!
//! Wire format mirrors wal-g so wal-rs and wal-g can share buckets

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod fetch;
pub mod list;
pub mod push;
pub mod show;
pub mod tar_streamer;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
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
    }

    #[test]
    fn formats_lsn_uppercase() {
        assert_eq!(format_pg_lsn(0x0300_0000), "0/3000000");
        assert_eq!(format_pg_lsn((2u64 << 32) | 0xab), "2/AB");
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
            increment_from_lsn: None,
            increment_from: None,
            increment_full_name: None,
            increment_count: None,
            pg_version: 160003,
            backup_finish_lsn: Some(0x0300_1000),
            system_identifier: Some(7000000000000000000),
            uncompressed_size: 1024,
            compressed_size: 512,
            data_catalog_size: 0,
            user_data: None,
            files_metadata_disabled: true,
            tablespace_spec: None,
            backup_start_chkp_num: Some(0),
            increment_from_chkp_num: None,
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
                increment_from_lsn: None,
                increment_from: None,
                increment_full_name: None,
                increment_count: None,
                pg_version: 160003,
                backup_finish_lsn: Some(2),
                system_identifier: None,
                uncompressed_size: 0,
                compressed_size: 0,
                data_catalog_size: 0,
                user_data: None,
                files_metadata_disabled: true,
                tablespace_spec: None,
                backup_start_chkp_num: Some(0),
                increment_from_chkp_num: None,
            },
            version: 2,
            start_time: chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            finish_time: chrono::DateTime::parse_from_rfc3339("2024-01-01T00:01:00Z")
                .unwrap()
                .with_timezone(&Utc),
            date_fmt: METADATA_DATETIME_FORMAT.into(),
            hostname: "h".into(),
            data_dir: "/d".into(),
            is_permanent: false,
        };
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"Version\":2"));
        assert!(j.contains("\"Hostname\":\"h\""));
        // and the embedded V1 fields
        assert!(j.contains("\"PgVersion\":160003"));
    }
}
