use std::process::ExitCode;

fn main() -> ExitCode {
    walrus::log::init();

    let cli = walrus::cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: walrus::cli::Cli) -> anyhow::Result<()> {
    if let Some(path) = cli.config.as_deref() {
        walrus::config::load_env_file(path)?;
    }
    let threads = cli.worker_threads()?;
    cap_malloc_arenas(threads);
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

/// Cap glibc malloc arenas to the CPU count. glibc otherwise grows to 8*ncpu
/// arenas, each reserving a 64 MiB heap by mmap; once the multi-thread runtime
/// drives concurrent allocation that inflates virtual memory far past the
/// resident set. One arena per core keeps VSZ bounded without measurably
/// hurting allocator throughput. Must run before any worker thread spawns
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn cap_malloc_arenas(n: usize) {
    // SAFETY: mallopt is thread-safe; called once on the main thread pre-runtime
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, n as libc::c_int);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn cap_malloc_arenas(_: usize) {}
