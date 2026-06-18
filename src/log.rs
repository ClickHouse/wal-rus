//! Process-wide logging init with a runtime-reloadable filter.
//!
//! Daemon mode re-reads `WALG_LOG_LEVEL` and swaps the filter on SIGUSR1,
//! matching wal-g's ConfigureLogging-on-signal behavior

use std::sync::OnceLock;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry, fmt, reload};

type Handle = reload::Handle<EnvFilter, Registry>;

static RELOAD: OnceLock<Handle> = OnceLock::new();

fn filter() -> EnvFilter {
    EnvFilter::try_from_env("WALG_LOG_LEVEL")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info"))
}

pub fn init() {
    let (layer, handle) = reload::Layer::new(filter());
    Registry::default()
        .with(layer)
        .with(fmt::layer().with_writer(std::io::stderr).with_target(false))
        .init();
    let _ = RELOAD.set(handle);
}

/// Re-read `WALG_LOG_LEVEL` and swap the active filter. No-op when logging
/// was not initialized through `init`
pub fn reconfigure() {
    if let Some(h) = RELOAD.get()
        && let Err(e) = h.reload(filter())
    {
        tracing::warn!("reload log filter: {e}");
    }
}
