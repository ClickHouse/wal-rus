//! CLI surface mirroring wal-g pg subcommands; only wired ones do real work

use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::Settings;
use crate::pg::backup;
use crate::pg::wal;

#[derive(Parser, Debug)]
#[command(name = "walross", version, about = "Rust port of wal-g for PostgreSQL")]
pub struct Cli {
    /// Tokio worker threads; 1 = single-threaded runtime. Defaults per
    /// command: backup-push min(cores, WALG_UPLOAD_CONCURRENCY),
    /// backup-fetch/wal-prefetch/wal-restore min(cores,
    /// WALG_DOWNLOAD_CONCURRENCY), 1 elsewhere
    #[arg(long, global = true, env = "WALG_THREADS")]
    pub threads: Option<NonZeroUsize>,
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
        /// Segments to prefetch (default: WALG_DOWNLOAD_CONCURRENCY, as wal-g)
        #[arg(long)]
        count: Option<u32>,
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
    /// Mark or unmark a backup as permanent (flips sentinel.IsPermanent).
    /// Provide either a positional `name` (or `LATEST`) or `--target-user-data
    /// <json>` to select by sentinel.UserData
    BackupMark {
        /// Backup name, or LATEST
        #[arg(conflicts_with = "target_user_data")]
        name: Option<String>,
        /// Set IsPermanent=false
        #[arg(long)]
        impermanent: bool,
        /// Select target backup by sentinel.UserData (JSON, deep-equal)
        #[arg(long)]
        target_user_data: Option<String>,
    },
    /// Retention. Default is dry-run; pass `--confirm` to execute
    Delete {
        #[command(subcommand)]
        op: DeleteCli,
        /// Actually delete (default is dry-run)
        #[arg(long, global = true)]
        confirm: bool,
    },
    /// Copy backups (and optionally their WAL window) to a destination prefix.
    /// `--to` accepts `file:///path`, `s3://bucket/prefix`, `gs://bucket/prefix`,
    /// or a bare path (treated as fs)
    Copy {
        /// Copy a single backup (name or LATEST)
        #[arg(long, short = 'b', conflicts_with = "all")]
        backup_name: Option<String>,
        /// Copy every backup
        #[arg(long, conflicts_with = "backup_name")]
        all: bool,
        /// Copy WAL segments older than the backup's start LSN
        #[arg(long, short = 'w')]
        with_history: bool,
        /// Destination URI
        #[arg(long, short = 't')]
        to: String,
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
pub enum DeleteCli {
    /// Delete every object older than the resolved target.
    /// Accepts `[FIND_FULL] <backup_name|RFC3339_timestamp>`
    Before { args: Vec<String> },
    /// Keep N most-recent backups; remove older.
    /// Accepts `[FULL|FIND_FULL] <N>`. `--after <ts|name>` additionally keeps
    /// every backup at-or-newer than the boundary
    Retain {
        args: Vec<String>,
        /// RFC3339 timestamp or backup-name prefix; survives in addition to the N newest
        #[arg(long, short = 'a')]
        after: Option<String>,
    },
    /// Wipe basebackups + WAL. Refuses when any permanent backup exists unless `FORCE`
    Everything { args: Vec<String> },
    /// Delete a single backup and (default) its dependants;
    /// `FIND_FULL <name>` deletes the whole increment chain
    Target { args: Vec<String> },
    /// Find oldest non-permanent backup; delete everything older.
    /// `ARCHIVES` / `BACKUPS` narrows the scope
    Garbage { args: Vec<String> },
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
    /// Resolve runtime worker count; must run before runtime construction.
    /// Multi-thread only where concurrent CPU work (compress/encrypt/TLS)
    /// exists today; everything else keeps the single-thread footprint
    /// (one malloc arena, no worker stacks)
    pub fn worker_threads(&self) -> Result<usize> {
        if let Some(n) = self.threads {
            return Ok(n.get());
        }
        let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        Ok(match self.cmd {
            Cmd::BackupPush { .. } => cores.min(crate::config::upload_concurrency_from_env()?),
            Cmd::BackupFetch { .. } | Cmd::WalPrefetch { .. } | Cmd::WalRestore { .. } => {
                cores.min(crate::config::download_concurrency_from_env()?)
            }
            _ => 1,
        })
    }

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
                wal::fetch::handle(&s, storage, &name, &dst, wal::fetch::Prefetch::Fork).await
            }
            Cmd::WalPrefetch {
                seed,
                pg_wal,
                count,
            } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let count = count.unwrap_or(s.download_concurrency as u32);
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
            Cmd::BackupMark {
                name,
                impermanent,
                target_user_data,
            } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let resolved = match (name, target_user_data) {
                    (Some(n), None) => n,
                    (None, Some(ud)) => backup::show::resolve_by_user_data(&storage, &ud).await?,
                    (Some(_), Some(_)) => {
                        anyhow::bail!("specify backup name OR --target-user-data, not both")
                    }
                    (None, None) => {
                        anyhow::bail!("backup name or --target-user-data required")
                    }
                };
                backup::show::mark(storage, &resolved, !impermanent).await
            }
            Cmd::Delete { op, confirm } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                let delete_op = match op {
                    DeleteCli::Before { args } => {
                        let (modifier, target) = backup::delete::parse_modifier_args(&args)?;
                        if matches!(modifier, backup::delete::DeleteModifier::Full) {
                            anyhow::bail!("`delete before FULL` is not supported");
                        }
                        backup::delete::DeleteOp::Before { target, modifier }
                    }
                    DeleteCli::Retain { args, after } => {
                        let (modifier, value) = backup::delete::parse_modifier_args(&args)?;
                        let count: usize = value
                            .parse()
                            .map_err(|e| anyhow::anyhow!("retain count: {e}"))?;
                        backup::delete::DeleteOp::Retain {
                            count,
                            modifier,
                            after,
                        }
                    }
                    DeleteCli::Everything { args } => {
                        let force = backup::delete::parse_everything_force(&args)?;
                        backup::delete::DeleteOp::Everything { force }
                    }
                    DeleteCli::Target { args } => {
                        let (modifier, name) = backup::delete::parse_target_modifier(&args)?;
                        backup::delete::DeleteOp::Target { name, modifier }
                    }
                    DeleteCli::Garbage { args } => {
                        let scope = backup::delete::parse_garbage_scope(&args)?;
                        backup::delete::DeleteOp::Garbage { scope }
                    }
                };
                backup::delete::handle(storage, delete_op, confirm)
                    .await
                    .map(|_| ())
            }
            Cmd::Copy {
                backup_name,
                all,
                with_history,
                to,
            } => {
                let s = Settings::from_env()?;
                let src = s.build_storage()?;
                let dst = s.build_dst_storage(&to)?;
                backup::copy::handle(
                    &s,
                    src,
                    dst,
                    backup::copy::CopyArgs {
                        backup_name,
                        all,
                        with_history,
                    },
                )
                .await
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
