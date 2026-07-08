//! The sync-replica control plane: the durable synchronous-standby receiver's
//! out-of-band control work, isolated on its own tokio runtime.
//!
//! - [`shared`] — the lock-free bridge to the receiver hot path ([`Shared`]).
//! - [`controller`] — the runtime owner + primary-poller (sole-acker) + janitor.
//! - [`api`] — the mTLS control API the Ubicloud control plane calls.
//! - [`dr_tail`] — the DR-tail S3 push behind `POST /v1/dr-catchup`.
//! - [`receive`] — the synchronous (tokio-free) WAL receive hot path.

pub mod api;
pub mod controller;
pub mod dr_tail;
pub mod receive;
pub mod service;
pub mod shared;

pub(crate) use controller::SyncReplicaController;
pub(crate) use dr_tail::DrTail;
pub(crate) use service::run;
pub(crate) use shared::{Retarget, Shared};
