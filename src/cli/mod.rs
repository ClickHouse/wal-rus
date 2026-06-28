//! CLI surface mirroring wal-g pg subcommands; only wired ones do real work

use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::Settings;
use crate::pg::backup;
use crate::pg::wal;

/// `--increment-format`: delta file wire format. wal-g restores only `wi1`;
/// `native` (PG17 INCREMENTAL) restores via pg_combinebackup, not wal-g
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum IncrementFormatArg {
    #[value(name = "wi1")]
    Wi1,
    #[value(name = "native")]
    Native,
}

impl From<IncrementFormatArg> for backup::increment::Format {
    fn from(f: IncrementFormatArg) -> Self {
        match f {
            IncrementFormatArg::Wi1 => Self::Wi1,
            IncrementFormatArg::Native => Self::Native,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "walrus", version, about = "Rust port of wal-g for PostgreSQL")]
pub struct Cli {
    /// Tokio worker threads; 1 = single-threaded runtime. Defaults per
    /// command: backup-push min(cores, WALG_UPLOAD_CONCURRENCY),
    /// backup-fetch/wal-prefetch/wal-restore min(cores,
    /// WALG_DOWNLOAD_CONCURRENCY), 1 elsewhere
    #[arg(long, global = true, env = "WALG_THREADS")]
    pub threads: Option<NonZeroUsize>,
    /// load `KEY=VALUE` settings into the environment before running
    /// (existing env vars win). Accepts wal-g's `wal-g.env` files
    #[arg(long, global = true, value_name = "FILE")]
    pub config: Option<PathBuf>,
    #[command(subcommand)]
    pub cmd: Cmd,
}

/// Parse argv, honoring the `walg-daemon-client` multicall name. wal-g ships a
/// separate `walg-daemon-client SOCKET COMMAND [ARGS]` binary; when argv[0]'s
/// basename is `walg-daemon-client` (a symlink to this binary) we expose the
/// same behavior by rewriting to the native `daemon-client` subcommand
pub fn parse() -> Cli {
    parse_from(std::env::args_os())
}

fn parse_from<I, T>(args: I) -> Cli
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let argv: Vec<OsString> = args.into_iter().map(Into::into).collect();
    match walg_daemon_client_argv(&argv) {
        Some(rewritten) => Cli::parse_from(rewritten),
        None => Cli::parse_from(argv),
    }
}

/// Map `walg-daemon-client SOCKET COMMAND [ARGS]` to the native
/// `daemon-client --socket SOCKET COMMAND [ARGS]`. Returns `None` for any other
/// program name. wal-g's `-timeout`/`-connection-timeout` flags are not
/// translated (archive_command callers never pass them); walrus defaults apply
fn walg_daemon_client_argv(argv: &[OsString]) -> Option<Vec<OsString>> {
    let prog = argv.first()?;
    let name = std::path::Path::new(prog).file_name()?.to_str()?;
    if name != "walg-daemon-client" {
        return None;
    }
    let socket = argv.get(1)?;
    let mut out: Vec<OsString> = vec![
        "walrus".into(),
        "daemon-client".into(),
        "--socket".into(),
        socket.clone(),
    ];
    out.extend(argv.iter().skip(2).cloned());
    Some(out)
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
    /// Restore a base backup to a directory.
    /// Select via positional `name` (or `LATEST`) or `--target-user-data <json>`
    BackupFetch {
        /// Destination directory (created if missing)
        dst: PathBuf,
        /// Backup name, or LATEST
        #[arg(conflicts_with = "target_user_data")]
        name: Option<String>,
        /// Select target backup by sentinel.UserData (JSON, deep-equal).
        /// Falls back to WALG_FETCH_TARGET_USER_DATA
        #[arg(long)]
        target_user_data: Option<String>,
    },
    /// Take a base backup
    ///
    /// Uses libpq env vars (PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE).
    /// With PGDATA, reads local filesystem like wal-g. Without PGDATA, streams
    /// through replication BASE_BACKUP and records server-reported data_directory.
    BackupPush {
        /// Optional path to local PostgreSQL data directory
        #[arg(value_name = "PGDATA")]
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
        /// wal-g `--verify`/`-v`: accepted for CLI parity. walrus relies on
        /// BASE_BACKUP server-side checksum verification and does not implement
        /// wal-g's corrupt-block storing (`WALG_VERIFY_PAGE_CHECKSUMS`)
        #[arg(long, short = 'v')]
        verify: bool,
        /// Override `WALG_TAR_SIZE_THRESHOLD` (bytes); 0 = default 1 GiB
        #[arg(long, default_value_t = 0u64, env = "WALG_TAR_SIZE_THRESHOLD")]
        tar_size_threshold: u64,
        /// Build the delta map from `$PGDATA/pg_wal/summaries` (PG17+,
        /// requires `summarize_wal=on`) instead of walking archived WAL.
        /// Source only; output format is `--increment-format`. Mutually
        /// exclusive with `--full`
        #[arg(long)]
        delta_from_wal_summaries: bool,
        /// Delta file wire format: `wi1` (wal-g native, default) or `native`
        /// (PG17 INCREMENTAL). Independent of the delta-map source
        #[arg(long, default_value = "wi1")]
        increment_format: IncrementFormatArg,
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
        /// Socket path. wal-g positional form `daemon <socket>`; `--socket`
        /// also accepted for the walrus-native form
        #[arg(value_name = "SOCKET", conflicts_with = "socket_flag")]
        socket: Option<PathBuf>,
        #[arg(long = "socket", value_name = "SOCKET")]
        socket_flag: Option<PathBuf>,
    },
    /// Send a single command to the daemon
    DaemonClient {
        #[arg(long)]
        socket: PathBuf,
        /// Operation execution timeout (Go-style duration); 0 disables
        #[arg(long, default_value = "60s", value_parser = crate::config::parse_duration)]
        timeout: Duration,
        /// Socket dial timeout (Go-style duration); 0 disables
        #[arg(long = "connection-timeout", default_value = "5s", value_parser = crate::config::parse_duration)]
        connection_timeout: Duration,
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
    /// `FIND_FULL <name>` deletes the whole increment chain.
    /// Select via positional name or `--target-user-data <json>`
    Target {
        args: Vec<String>,
        /// Select target backup by sentinel.UserData (JSON, deep-equal)
        #[arg(long)]
        target_user_data: Option<String>,
    },
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
        // WALG_PG_WAL_SIZE applies to every command's segment math (wal-g sets
        // its WalSegmentSize global in the same pre-run hook)
        wal::segment::configure_from_env()?;
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
            Cmd::BackupFetch {
                dst,
                name,
                target_user_data,
            } => {
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                // flag wins, else WALG_FETCH_TARGET_USER_DATA (wal-g parity)
                let target_user_data =
                    target_user_data.or_else(|| std::env::var("WALG_FETCH_TARGET_USER_DATA").ok());
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
                backup::fetch::handle(&s, storage, &resolved, &dst).await
            }
            Cmd::BackupPush {
                pgdata,
                permanent,
                user_data,
                fast,
                no_verify_checksums,
                tar_size_threshold,
                delta_from_wal_summaries,
                increment_format,
                full,
                verify,
            } => {
                if verify {
                    tracing::debug!(
                        "--verify accepted; relying on BASE_BACKUP server-side checksum verification"
                    );
                }
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
                    increment_format: increment_format.into(),
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
                    DeleteCli::Target {
                        args,
                        target_user_data,
                    } => {
                        let (modifier, maybe_name) = backup::delete::parse_target_modifier(&args)?;
                        let name = match (maybe_name, target_user_data) {
                            (Some(n), None) => n,
                            (None, Some(ud)) => {
                                backup::show::resolve_by_user_data(&storage, &ud).await?
                            }
                            (Some(_), Some(_)) => {
                                anyhow::bail!("specify backup name OR --target-user-data, not both")
                            }
                            (None, None) => {
                                anyhow::bail!("backup name or --target-user-data required")
                            }
                        };
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
            Cmd::Daemon {
                socket,
                socket_flag,
            } => {
                let socket = socket
                    .or(socket_flag)
                    .ok_or_else(|| anyhow::anyhow!("daemon requires a socket path"))?;
                let s = Settings::from_env()?;
                let storage = s.build_storage()?;
                crate::daemon::serve(&socket, s, storage).await
            }
            Cmd::DaemonClient {
                socket,
                op,
                timeout,
                connection_timeout,
            } => crate::daemon::client::run(&socket, op, timeout, connection_timeout).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn increment_format_arg_maps_to_wire_format() {
        use backup::increment::Format;
        assert_eq!(Format::from(IncrementFormatArg::Wi1), Format::Wi1);
        assert_eq!(Format::from(IncrementFormatArg::Native), Format::Native);
    }

    #[test]
    fn backup_push_accepts_positional_pgdata() {
        let cli = Cli::parse_from(["walrus", "backup-push", "/dat/18/data", "--full"]);
        match cli.cmd {
            Cmd::BackupPush { pgdata, full, .. } => {
                assert_eq!(pgdata, Some(PathBuf::from("/dat/18/data")));
                assert!(full);
            }
            _ => panic!("expected backup-push"),
        }
    }

    #[test]
    fn backup_push_accepts_walg_verify_flag() {
        // ubicloud take-backup passes `--verify`; parity flag, not an error
        let cli = Cli::parse_from(["walrus", "backup-push", "/dat/18/data", "--verify"]);
        match cli.cmd {
            Cmd::BackupPush { verify, .. } => assert!(verify),
            _ => panic!("expected backup-push"),
        }
        assert!(matches!(
            Cli::parse_from(["walrus", "backup-push", "/d", "-v"]).cmd,
            Cmd::BackupPush { verify: true, .. }
        ));
    }

    #[test]
    fn config_is_global_after_positionals() {
        // archive_command/restore_command append `--config` after the subcommand args
        let cli = Cli::parse_from(["walrus", "wal-push", "/p", "--config", "/etc/walg.env"]);
        assert_eq!(cli.config, Some(PathBuf::from("/etc/walg.env")));
        assert!(matches!(cli.cmd, Cmd::WalPush { .. }));
    }

    #[test]
    fn walg_daemon_client_name_rewrites_to_daemon_client() {
        let argv: Vec<OsString> = [
            "/usr/bin/walg-daemon-client",
            "/tmp/wal-g",
            "wal-push",
            "000000010000000000000001",
        ]
        .iter()
        .map(OsString::from)
        .collect();
        let rewritten = walg_daemon_client_argv(&argv).expect("multicall rewrite");
        match Cli::parse_from(rewritten).cmd {
            Cmd::DaemonClient { socket, op, .. } => {
                assert_eq!(socket, PathBuf::from("/tmp/wal-g"));
                match op {
                    DaemonOp::WalPush { wal_filepath } => {
                        assert_eq!(wal_filepath, PathBuf::from("000000010000000000000001"));
                    }
                    _ => panic!("expected wal-push op"),
                }
            }
            _ => panic!("expected daemon-client"),
        }
    }

    #[test]
    fn other_program_names_are_not_rewritten() {
        let argv: Vec<OsString> = ["/usr/bin/wal-g", "wal-show"]
            .iter()
            .map(OsString::from)
            .collect();
        assert!(walg_daemon_client_argv(&argv).is_none());
    }

    #[test]
    fn daemon_accepts_positional_and_flag_socket() {
        // wal-g positional form (systemd ExecStart + `wal-g daemon <socket>`)
        match Cli::parse_from(["walrus", "daemon", "/tmp/wal-g"]).cmd {
            Cmd::Daemon {
                socket,
                socket_flag,
            } => {
                assert_eq!(socket, Some(PathBuf::from("/tmp/wal-g")));
                assert_eq!(socket_flag, None);
            }
            _ => panic!("expected daemon"),
        }
        // walrus-native flag form stays valid
        match Cli::parse_from(["walrus", "daemon", "--socket", "/tmp/wal-g"]).cmd {
            Cmd::Daemon {
                socket,
                socket_flag,
            } => {
                assert_eq!(socket, None);
                assert_eq!(socket_flag, Some(PathBuf::from("/tmp/wal-g")));
            }
            _ => panic!("expected daemon"),
        }
        // both at once is rejected
        assert!(Cli::try_parse_from(["walrus", "daemon", "/a", "--socket", "/b"]).is_err());
    }

    fn worker_threads_of(args: &[&str]) -> usize {
        Cli::parse_from(args).worker_threads().unwrap()
    }

    #[test]
    fn explicit_threads_override_per_command_default() {
        assert_eq!(
            worker_threads_of(&["walrus", "--threads", "3", "wal-show"]),
            3
        );
    }

    #[test]
    fn default_commands_stay_single_threaded() {
        assert_eq!(worker_threads_of(&["walrus", "wal-show"]), 1);
        assert_eq!(
            worker_threads_of(&["walrus", "wal-fetch", "seg", "/dst"]),
            1
        );
    }

    #[test]
    fn concurrent_commands_scale_with_cores() {
        // min(cores, concurrency-from-env); both factors are >=1
        for args in [
            vec!["walrus", "backup-push"],
            vec!["walrus", "backup-fetch", "LATEST", "/dst"],
            vec!["walrus", "wal-prefetch", "seg", "/pg_wal"],
            vec!["walrus", "wal-restore", "/dst"],
        ] {
            assert!(worker_threads_of(&args) >= 1);
        }
    }
}
