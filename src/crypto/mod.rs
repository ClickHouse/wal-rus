//! Encryption layer between compression and storage
//!
//! Pipeline order matches wal-g:
//!   push:  raw → compress → encrypt → storage
//!   fetch: storage → decrypt → decompress → consumer
//!
//! Sentinel/metadata JSON paths bypass this layer (matches wal-g `UploadDto`,
//! which short-circuits the compress+encrypt pipeline). Only WAL segments and
//! basebackup tar parts pass through encryption.
//!
//! # OpenPGP intentionally not supported
//!
//! wal-g supports both libsodium (XChaCha20-Poly1305 secretstream) and OpenPGP
//! (`WALG_PGP_KEY` / `_PATH` / `_PASSPHRASE`). wal-rs ships libsodium only,
//! by design:
//!
//! - The pure-Rust OpenPGP options (`pgp` aka rPGP) pull a heavy dependency
//!   tree (RSA / DSA / ECDSA / curve25519 / bzip2 / armor parser / dozens of
//!   transitive crates) for a feature whose threat model is already covered
//!   by libsodium's symmetric AEAD. The async wrapper crate `pgp-lib` is
//!   buffer-based (no streaming), which forces full backups into memory.
//! - libsodium round-trips against wal-g's libsodium output byte-for-byte
//!   (`crypto_secretstream_xchacha20poly1305`, 8 KiB chunks, 24-byte header),
//!   so a deployment migrating between the tools picks libsodium and the
//!   migration is symmetric.
//! - OpenPGP's value-add over libsodium in wal-g is multi-recipient key
//!   distribution, which our target deployment (single-tenant on-prem PG)
//!   doesn't need.
//!
//! A user with a wal-g bucket that's *already* encrypted with OpenPGP must
//! re-encrypt to libsodium before switching to wal-rs (or stay on wal-g).
//! `WALG_PGP_*` env vars are detected and produce a hard error directing the
//! user here rather than silently writing plaintext — see [`forbid_pgp_env`]

use std::sync::Arc;

use anyhow::{Result, bail};

use crate::compression::AsyncReader;

pub mod libsodium;

pub trait Crypter: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    /// Wrap a plaintext reader; bytes read from the returned reader are ciphertext
    fn encrypt_reader(&self, plain: AsyncReader) -> AsyncReader;
    /// Wrap a ciphertext reader; bytes read from the returned reader are plaintext
    fn decrypt_reader(&self, cipher: AsyncReader) -> AsyncReader;
}

pub type DynCrypter = Arc<dyn Crypter>;

/// Build a crypter from env. Returns Ok(None) when no crypto vars are set
pub fn from_env() -> Result<Option<DynCrypter>> {
    forbid_pgp_env()?;
    libsodium::from_env()
}

/// Hard-error if any `WALG_PGP_*` env is set. Silently writing plaintext when
/// the operator believes they configured encryption would be unsafe; the error
/// directs them to libsodium (or wal-g) explicitly
pub fn forbid_pgp_env() -> Result<()> {
    const PGP_VARS: &[&str] = &[
        "WALG_PGP_KEY",
        "WALG_PGP_KEY_PATH",
        "WALG_PGP_KEY_PASSPHRASE",
    ];
    let set: Vec<&str> = PGP_VARS
        .iter()
        .copied()
        .filter(|v| std::env::var(v).is_ok())
        .collect();
    if !set.is_empty() {
        bail!(
            "OpenPGP encryption is not supported by wal-rs ({set:?} set). \
             Use WALG_LIBSODIUM_KEY / _KEY_PATH instead, or run wal-g for the \
             PGP-encrypted bucket. See src/crypto/mod.rs for rationale."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pgp_env_is_rejected() {
        // SAFETY: tests serialize on env in the same process; set + clean up
        unsafe {
            std::env::set_var("WALG_PGP_KEY", "...");
        }
        let r = forbid_pgp_env();
        unsafe {
            std::env::remove_var("WALG_PGP_KEY");
        }
        assert!(r.is_err(), "WALG_PGP_KEY must surface as a hard error");
    }
}
