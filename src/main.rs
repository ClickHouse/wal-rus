use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    let filter = EnvFilter::try_from_env("WALG_LOG_LEVEL")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    let cli = walross::cli::Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: walross::cli::Cli) -> anyhow::Result<()> {
    let threads = cli.worker_threads()?;
    // current_thread when 1: no worker threads, single glibc malloc arena
    // (see docs/DESIGN.md Runtime)
    let mut builder = if threads > 1 {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(threads);
        b
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().build()?.block_on(cli.run())
}
