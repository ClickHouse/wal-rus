//! The sync-replica control plane: the durable synchronous-standby receiver's
//! out-of-band control work, isolated on its own tokio runtime.
//!
//! - [`shared`] — the lock-free bridge to the receiver hot path ([`Shared`]).
//! - [`controller`] — the runtime owner + primary-poller (sole-acker) + janitor.
//! - (next) `api` — the mTLS control API the Ubicloud control plane calls.

pub mod controller;
pub mod shared;

pub(crate) use controller::SyncReplicaController;
pub(crate) use shared::Shared;
