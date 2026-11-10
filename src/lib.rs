//! Rust port of wal-g for PostgreSQL
//!
//! All cargo features are on by default. A library user can disable
//! `default-features` and pick from:
//!
//! - `cli`: the `walrus` binary, `cli`, `daemon`, `log`
//! - `s3`, `gcs`: storage backends; [`storage::fs`] is always built
//! - `zstd`, `lz4`, `brotli`, `lzma`, `gzip`: [`compression`] codecs
//! - `libsodium`: [`crypto`] encryption
//! - `replication`: streaming replication protocol, `pg::wal::receive`
//! - `backup`: base backup push/fetch, delta backups, WAL summaries
//!   (implies `replication`)
//!
//! [`pg::walparser`] re-exports the `pg-walparser` crate, which is
//! `no_std` + `alloc` on its own

#[cfg(feature = "cli")]
pub mod cli;
pub mod compression;
pub mod concurrency;
pub mod config;
pub mod crypto;
#[cfg(feature = "cli")]
pub mod daemon;
#[cfg(feature = "cli")]
pub mod log;
pub mod pg;
pub mod retry;
pub mod storage;
pub mod throttle;
pub mod time;
