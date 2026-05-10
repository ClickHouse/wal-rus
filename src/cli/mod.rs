//! CLI surface mirroring wal-g pg subcommands; only wired ones do real work

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::Settings;
use crate::pg::backup;
use crate::pg::wal;

#[derive(Parser, Debug)]
#[command(name = "wal-rs", version, about = "Rust port of wal-g for PostgreSQL")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Upload WAL segment to storage
    WalPush { wal_filepath: PathBuf },
    /// Download WAL segment from storage to dst path
    WalFetch { name: String, dst: PathBuf },
    /// Pre-stage upcoming WAL segments into <pg_wal>/.wal-g/prefetch so
    /// subsequent `wal-fetch` calls promote by rename
    WalPrefetch {
        /// Segment name to walk forward from (downloads next `--count` segments)
        seed: String,
        /// Path to PostgreSQL `pg_wal` directory
        pg_wal: PathBuf,
        /// Number of segments to prefetch
        #[arg(long, default_value_t = 8)]
        count: u32,
    },
    /// Print archived timelines, segment ranges, gaps, & known backups
    WalShow {
        /// Output JSON instead of the human-readable table
        #[arg(long)]
        json: bool,
    },
    /// Verify continuity & timeline alignment for archived WAL
    WalVerify {
        #[command(subcommand)]
        op: WalVerifyOp,
    },
    /// Inverse of `wal-show` gaps: fetch missing segments into a local dir
    WalRestore {
        /// Destination directory for restored segments
        dst: PathBuf,
        /// Restrict restore to a single timeline (defaults to every timeline
        /// that has at least one archived segment)
        #[arg(long)]
        timeline: Option<u32>,
    },
    /// Long-running START_REPLICATION consumer that archives segments
    /// directly (alternative to archive_command)
    WalReceive {
        /// Directory used to assemble segments mid-flight; each completed
        /// segment is shipped via the regular wal-push pipeline
        archive_dir: PathBuf,
    },
    /// List backups under basebackups_005/
    BackupList {
        /// Print summaries as JSON instead of a table
        #[arg(long)]
        json: bool,
    },
    /// Restore a base backup to a directory
    BackupFetch {
        /// Backup name, or LATEST
        name: String,
        /// Destination directory (created if missing)
        dst: PathBuf,
    },
    /// Take a streaming base backup via the replication BASE_BACKUP protocol
    ///
    /// Uses libpq env vars (PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE).
    /// Without --pgdata, the sentinel records the server-reported data_directory.
    BackupPush {
        /// Optional path to local PostgreSQL data directory (sentinel only)
        #[arg(long)]
        pgdata: Option<PathBuf>,
        /// Mark this backup as permanent
        #[arg(long)]
        permanent: bool,
        /// Optional JSON object stored under sentinel.UserData
        #[arg(long)]
        user_data: Option<String>,
        /// Use CHECKPOINT 'fast' (default: spread)
        #[arg(long, default_value_t = true)]
        fast: bool,
        /// Pass NOVERIFY_CHECKSUMS / VERIFY_CHECKSUMS false to BASE_BACKUP
        #[arg(long)]
        no_verify_checksums: bool,
        /// Override `WALG_TAR_SIZE_THRESHOLD` (bytes); 0 = default 1 GiB
        #[arg(long, default_value_t = 0u64, env = "WALG_TAR_SIZE_THRESHOLD")]
        tar_size_threshold: u64,
        /// Build the delta map from `$PGDATA/pg_wal/summaries` (PG17+,
        /// requires `summarize_wal=on`). Emits PG-native INCREMENTAL files
        /// instead of wal-g's `wi1` format. Mutually exclusive with `--full`
        #[arg(long)]
        delta_from_wal_summaries: bool,
        /// Force a full (non-delta) backup, ignoring `WALG_DELTA_MAX_STEPS`
        #[arg(long, conflicts_with = "delta_from_wal_summaries")]
        full: bool,
    },
    /// Show sentinel + files_metadata summary for one backup
    BackupShow {
        /// Backup name, or LATEST
        name: String,
        /// Print as JSON
        #[arg(long)]
        json: bool,
    },
    /// Mark or unmark a backup as permanent (flips sentinel.IsPermanent)
    BackupMark {
        /// Backup name, or LATEST
        name: String,
        /// Set IsPermanent=false
        #[arg(long)]
        impermanent: bool,
    },
    /// Run as a long-lived daemon over a unix socket
    Daemon {
        #[arg(long)]
        socket: PathBuf,
    },
    /// Send a single command to the daemon
    DaemonClient {
        #[arg(long)]
        socket: PathBuf,
        #[command(subcommand)]
        op: DaemonOp,
    },
}

#[derive(Subcommand, Debug)]
pub enum DaemonOp {
    Check,
    WalPush { wal_filepath: PathBuf },
    WalFetch { name: String, dst: PathBuf },
}

#[derive(Subcommand, Debug)]
pub enum WalVerifyOp {
    /// Latest backup's start LSN forward through the freshest archived
    /// segment must contain no gaps
    Integrity {
        #[arg(long)]
        json: bool,
    },
    /// HEAD timeline (latest archived) must match the latest backup's timeline
    Timeline {
        #[arg(long)]
        json: bool,
    },
    /// Run every check
    All {
        #[arg(long)]
        json: bool,
    },
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        match self.cmd {
            Cmd::WalPush { wal_filepath } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                wal::push::handle(&s, storage, &wal_filepath).await
            }
            Cmd::WalFetch { name, dst } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                wal::fetch::handle(&s, storage, &name, &dst).await
            }
            Cmd::WalPrefetch {
                seed,
                pg_wal,
                count,
            } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                wal::prefetch::handle(&s, storage, &seed, &pg_wal, count).await
            }
            Cmd::WalShow { json } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let format = if json {
                    wal::show::Format::Json
                } else {
                    wal::show::Format::Plain
                };
                wal::show::handle(storage, format).await
            }
            Cmd::WalVerify { op } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                wal::verify::run(storage, op).await
            }
            Cmd::WalRestore { dst, timeline } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                wal::restore::handle(&s, storage, &dst, timeline).await
            }
            Cmd::WalReceive { archive_dir } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                wal::receive::handle(&s, storage, &archive_dir).await
            }
            Cmd::BackupList { json } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let format = if json {
                    backup::list::Format::Json
                } else {
                    backup::list::Format::Plain
                };
                backup::list::handle(storage, format).await
            }
            Cmd::BackupFetch { name, dst } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                backup::fetch::handle(&s, storage, &name, &dst).await
            }
            Cmd::BackupPush {
                pgdata,
                permanent,
                user_data,
                fast,
                no_verify_checksums,
                tar_size_threshold,
                delta_from_wal_summaries,
                full,
            } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let user_data = user_data
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("--user-data is not valid JSON: {e}"))?;
                let args = backup::push::PushArgs {
                    pgdata,
                    is_permanent: permanent,
                    user_data,
                    fast_checkpoint: fast,
                    no_verify_checksums,
                    tar_size_threshold,
                    delta_from_wal_summaries,
                    full,
                };
                backup::push::handle(&s, storage, args).await
            }
            Cmd::BackupShow { name, json } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let format = if json {
                    backup::show::Format::Json
                } else {
                    backup::show::Format::Plain
                };
                backup::show::show(storage, &name, format).await
            }
            Cmd::BackupMark { name, impermanent } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                backup::show::mark(storage, &name, !impermanent).await
            }
            Cmd::Daemon { socket } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                crate::daemon::serve(&socket, s, storage).await
            }
            Cmd::DaemonClient { socket, op } => crate::daemon::client::run(&socket, op).await,
        }
    }
}
