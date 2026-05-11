//! Postgres replication-protocol client, just enough for BASE_BACKUP
//!
//! Drives the wire protocol via postgres-protocol codecs. Mirrors what
//! pglogrepl does in Go for wal-g, plus PR #2262's PG15+ tagged-CopyData
//! framing (`d`/`p`/`n`/`m` tags within a singleton CopyOut session)

pub mod base_backup;
pub mod conn;
pub mod stream;
pub mod tls;

pub use base_backup::{ArchiveMeta, BackupEvent, BaseBackupOpts, run_base_backup};
pub use conn::{PgConfig, ReplicationConn};
pub use stream::{
    Frame, KeepaliveFrame, PG_EPOCH_USEC, WalDataFrame, build_status_update, decode_frame,
    now_pg_microseconds,
};
pub use tls::SslMode;
