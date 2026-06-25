//! Sync-replica control plane.
//!
//! Runs on its OWN tokio runtime (a dedicated OS thread), isolated from the
//! receiver hot path, so control work (primary-poller → sole-acker; later the
//! janitor and the mTLS control API) can't be starved by — and can't starve —
//! the recv loop. The two runtimes communicate only through [`Shared`], whose
//! fields are lock-free atomics (single-writer, so plain load/store, no locks).
//!
//! Design: `sync_pair/docs/sync-replica-controller.md`. This file currently
//! defines the shared-state bridge; the controller struct + primary-poller land
//! with the sole-acker milestone.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use tokio::sync::Notify;

/// State shared across the runtime boundary between the receiver hot path and
/// the sync-replica controller. Every field is single-writer.
#[derive(Default)]
pub(crate) struct Shared {
    /// Durable fsync frontier. WRITER: hot path (`acc.sync`). READER: controller.
    /// `Arc` so the accumulator can hold just the frontier, not all of `Shared`
    pub fsyncd_lsn: Arc<AtomicU64>,
    /// Receiver is the only live sync acker. WRITER: primary-poller (controller).
    /// READER: hot path drain gate (sole acker ⇒ per-frame fsync)
    pub sole_acker: AtomicBool,
    /// Shutdown. WRITER: hot path on SIGINT/SIGTERM. READER: controller `serve`
    pub stop: Notify,
}
