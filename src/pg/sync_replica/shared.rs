//! The bridge between the receiver hot path and the sync-replica controller.
//!
//! Lives in its own module so the `controller` and (forthcoming) `api` are
//! symmetric siblings that both read/write this state, without either owning it.
//! Every field is single-writer, so the hot path uses plain load/store, no locks
//! (see `sync_pair/docs/sync-replica-controller.md`).

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64};

use tokio::sync::Notify;

/// A new primary to stream from, set by `POST /v1/failover-primary`.
#[derive(Clone, Debug)]
pub(crate) struct Retarget {
    pub host: String,
    pub port: u16,
}

/// State shared across the runtime boundary between the receiver hot path and
/// the sync-replica controller. The atomic fields are single-writer (plain
/// load/store, no locks); `retarget` is a `Mutex` because it carries a `String`
/// and is touched only on failover (off the per-frame path).
pub(crate) struct Shared {
    /// Durable fsync frontier. WRITER: hot path (`acc.sync` writes it at fsync
    /// time). READERS: controller (poller) + hot path `send_status`
    pub fsyncd_lsn: AtomicU64,
    /// Receiver is the only live sync acker. WRITER: primary-poller (controller).
    /// READER: hot path drain gate (sole acker ⇒ per-frame fsync)
    pub sole_acker: AtomicBool,
    /// Max flush the hot path may advertise (back-pressure). `u64::MAX` = no cap.
    /// WRITER: janitor (controller). READER: hot path `send_status`
    pub ack_ceiling: AtomicU64,
    /// Shutdown. WRITER: hot path on SIGINT/SIGTERM. READER: controller `serve`
    pub stop: Notify,
    /// Pending re-target. WRITER: control API (`failover-primary`). READER: hot
    /// path reconnect loop, which applies + clears it on its next connect.
    pub retarget: Mutex<Option<Retarget>>,
    /// Wakes the hot path to break a live stream and reconnect to the new
    /// primary. WRITER: control API. READER: hot path recv select.
    pub retarget_signal: Notify,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            fsyncd_lsn: AtomicU64::new(0),
            sole_acker: AtomicBool::new(false),
            ack_ceiling: AtomicU64::new(u64::MAX),
            stop: Notify::new(),
            retarget: Mutex::new(None),
            retarget_signal: Notify::new(),
        }
    }
}
