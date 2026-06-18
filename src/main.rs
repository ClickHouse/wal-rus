use std::process::ExitCode;

use clap::Parser;

fn main() -> ExitCode {
    walross::log::init();

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
    let stack_size = if cfg!(debug_assertions) {
        512 * 1024
    } else {
        256 * 1024
    };
    builder
        .thread_stack_size(stack_size)
        .enable_all()
        .build()?
        .block_on(cli.run())
}
