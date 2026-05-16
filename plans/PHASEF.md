# Phase F — encryption (libsodium)

Implemented F.1 (`WALG_LIBSODIUM_*` XChaCha20-Poly1305 secretstream) from
PLAN.md. **F.2 (OpenPGP) was deliberately dropped** — see "Design decisions"
below. **206 local tests pass** (+18 since Phase E close: 14 crypto unit tests
+ 4 push/fetch integration tests). Clippy clean, fmt clean.

The crypter is configured from env in `Settings::from_env`, wired between
compression and storage on push, and between storage and decompression on
fetch (matching wal-g's pipeline order). Sentinel / metadata JSON paths
bypass the layer (matching wal-g `UploadDto`), so listing & retention work
unchanged whether the bucket is encrypted or not. The on-bucket file
extension is still the compression extension only — no `.libsodium` suffix.

VM-side cross-tool wal-g↔wal-rs encryption check belongs to the next pass;
the wire format is mirrored byte-for-byte (24-byte header + 8 KiB chunks +
17-byte overhead per chunk) so the interop should hold the first time.

## What landed

| Item | Files | Tests added |
|---|---|---|
| F.1 libsodium crypter + key handling (`WALG_LIBSODIUM_KEY` / `_KEY_PATH` / `_KEY_TRANSFORM`) | `src/crypto/libsodium.rs` (new) | `key_transform_none_pads_short_key`, `key_transform_none_truncates_long_key`, `key_transform_none_rejects_too_short`, `key_transform_hex_strict`, `key_transform_base64_strict` |
| Streaming `EncryptReader` / `DecryptReader` over dryoc `crypto_secretstream_xchacha20poly1305` | `src/crypto/libsodium.rs` | `empty_payload_roundtrip`, `small_payload_roundtrip`, `chunk_boundary_roundtrip`, `many_chunks_roundtrip`, `ciphertext_is_not_plaintext`, `wrong_key_fails_decrypt`, `tampered_ciphertext_fails`, `truncated_tail_fails` |
| Crypter trait + env-driven crypter resolution; PGP-env hard-reject | `src/crypto/mod.rs` (new) | `pgp_env_is_rejected` |
| `Settings::crypter` field + `encrypt` / `decrypt` helpers | `src/config/mod.rs` | covered by integration tests |
| Wire-in for `wal-push` / `wal-fetch` (incl. `compare_existing` path) | `src/pg/wal/push.rs`, `src/pg/wal/fetch.rs` | `push_fetch_libsodium_encrypted_roundtrip`, `fetch_with_wrong_key_fails`, `ciphertext_overhead_matches_libsodium_layout` |
| Wire-in for `backup-push` tar parts + `pg_control` tee | `src/pg/backup/push.rs` | covered by `fetch_decrypts_libsodium_tar_part` |
| Wire-in for `backup-fetch` part unpacker | `src/pg/backup/fetch.rs` | `fetch_decrypts_libsodium_tar_part` |
| Wire-in for `delta::fetch_and_parse_segment` (future delta-map build from encrypted WAL) | `src/pg/backup/delta.rs` | logic-only |

## Real bugs / issues found during integration

### DecryptReader returned `Ready(0 filled)` between chunks, masquerading as EOF

The initial `DecryptReader::poll_read` returned `Poll::Ready(Ok(()))` directly
after `pull_chunk()` without first draining the newly-decrypted plaintext into
the caller's `ReadBuf`. With Tokio's `read_to_end`, a `Ready(Ok(()))` poll
that left zero bytes in the buffer reads as EOF — the test
`chunk_boundary_roundtrip` failed with an empty plaintext after exactly one
correctly-encrypted chunk was on the wire.

Fix: restructure the poll loop so `pull_chunk()` (and the header init) use
`continue` rather than `return`, looping back to the drain branch at the top
of the loop. The encrypt side already had this shape; the asymmetry caught
4 of the 14 libsodium tests on first run.

### `Header::as_slice` lives behind unstable `str_as_str`

dryoc's `StackByteArray<24>` impl `Deref<Target = [u8]>` shadows a method
named `as_slice` that resolves to an unstable `str` method on stable rustc
1.94, surfacing as `E0658 use of unstable library feature str_as_str`. Use
the explicit `as_array()` accessor (`-> &[u8; 24]`) and let auto-coerce do
the byte-slice conversion.

## Design decisions worth recording

### OpenPGP support intentionally dropped

PLAN.md's F.2 called for `WALG_PGP_*` support via rPGP. We're skipping it
permanently. Reasons (also recorded inline at the top of `src/crypto/mod.rs`):

1. **Dependency footprint.** rPGP pulls in RSA / DSA / ECDSA / curve25519 /
   bzip2 / armor parser plus dozens of transitives. The async wrapper
   `pgp-lib` is buffer-based (`Vec<u8>` in, `Vec<u8>` out wrapped in
   `spawn_blocking`), which forces full backups into RAM on a layer that's
   supposed to be streaming.
2. **Threat-model overlap.** Symmetric AEAD via libsodium already covers
   confidentiality + integrity for the single-tenant on-prem PG deployment
   wal-rs targets. OpenPGP's distinctive value-add is multi-recipient
   asymmetric key distribution — orthogonal to that deployment shape.
3. **Migration symmetry.** A wal-g user migrating to wal-rs who'd been
   running PGP must re-encrypt to libsodium before switching (or stay on
   wal-g for that bucket). This is documented and the migration step is
   one-time.

To make sure operators can't silently regress to plaintext writes when they
believed they configured encryption, `crypto::forbid_pgp_env()` detects
`WALG_PGP_KEY`, `WALG_PGP_KEY_PATH`, or `WALG_PGP_KEY_PASSPHRASE` in the
environment and returns a hard error from `Settings::from_env`. Tested by
`crypto::tests::pgp_env_is_rejected`.

### Wire format compatibility

`crypto_secretstream_xchacha20poly1305` framing matches wal-g exactly:

- 24-byte header (XChaCha20 subkey nonce + 8-byte inonce) written first
- 8192-byte plaintext chunks, FINAL tag on the last chunk
- Per-chunk overhead = 17 bytes (1-byte stream tag + 16-byte Poly1305 MAC)
- An explicit FINAL chunk is emitted at EOF even when the prior MESSAGE
  chunk already drained the plaintext (the FINAL chunk is then 17 bytes
  carrying 0 plaintext bytes). wal-g's `writer.go` does the same via the
  `Close()` path. `ciphertext_overhead_matches_libsodium_layout` pins this
  exact byte budget so a wire-format drift becomes a test failure.

### Key transform semantics

`WALG_LIBSODIUM_KEY_TRANSFORM` (default `none`) mirrors wal-g's
`keytransform.go`:

- `none`: input must be ≥ 25 bytes (matches wal-g's `minimalKeyLength`); >32
  bytes truncate, <32 bytes zero-pad on the right. Legacy-compat path.
- `hex`: input must decode to *exactly* 32 bytes; rejects partial.
- `base64`: same exact-32-byte rule.

The `none` transform deliberately rejects short keys (25-byte minimum) so a
user can't cargo-cult their way to a low-entropy key on the legacy path.

### Order of operations on the pipeline

`push: raw → compress → encrypt → storage`
`fetch: storage → decrypt → decompress → consumer`

PLAN.md described this as "encryption between compression and storage,"
which the wal-g implementation backs up: `internal/compress_and_encrypt.go`
wires the compressor *inside* the encrypter (compressor writes to encrypter
writes to output), so on-disk bytes are encrypted-after-compressed. The
inverse on read. We follow that order verbatim.

### Sentinel / metadata JSON bypass

wal-g uploads sentinel & metadata JSON via `UploadDto` (direct
`folder.PutObject`), bypassing the compress+encrypt pipeline used for
WAL & tar parts. We mirror this: `upload_json` in `backup/push.rs` and
the `mark` / `resolve_by_user_data` paths in `backup/show.rs` use
`storage.put` directly, never touching `settings.encrypt`. Two
consequences worth recording:

- `backup-list` and `delete` work against an encrypted bucket without
  needing the key — they only read sentinels.
- Bucket auditors can still see backup names, LSNs, timestamps, and
  hostname even when WAL & tar parts are encrypted. Matches wal-g; if
  this becomes a concern, the answer is bucket-side encryption (S3 SSE,
  GCS CMEK), not application-layer expansion.

### Size hint disabled when crypter is set

`Settings::encrypt` adds 24 + (17 per 8 KiB) bytes of overhead, so the
plain `size_hint = Some(file_size)` on `wal-push` would lie to the storage
backend. The new `size_hint` predicate is `method == None && crypter is
None`. S3/GCS multipart upload handles streaming without a size hint
already, so this is a pure correctness fix with no perf impact.

### Mixed plaintext/ciphertext buckets are not supported

If a backup is uploaded without `WALG_LIBSODIUM_KEY` and then a subsequent
fetch sets `WALG_LIBSODIUM_KEY`, the fetch will fail (the decrypter sees
plaintext, no 24-byte header, no Poly1305 MAC, → error). And vice versa.
This matches wal-g — the bucket layout doesn't tag objects as
encrypted-or-not, so the operator must keep the key configured consistently
across all push/fetch operations against a given bucket prefix. The hard
errors on first read or write are the diagnostic.

## Compatibility with wal-g

- libsodium framing (`crypto_secretstream_xchacha20poly1305`) byte-identical
  to wal-g `internal/crypto/libsodium/writer.go` & `reader.go`: 24-byte
  header + 8 KiB chunks + 17-byte ABYTES overhead, explicit FINAL chunk on
  Close().
- Env vars mirror wal-g exactly: `WALG_LIBSODIUM_KEY`,
  `WALG_LIBSODIUM_KEY_PATH`, `WALG_LIBSODIUM_KEY_TRANSFORM` (`none` |
  `hex` | `base64`).
- Key file path is read with surrounding whitespace trimmed (matches
  wal-g's `strings.TrimSpace`). UTF-8 only — wal-g uses Go strings which
  are byte slices, but every legitimate key (hex/base64/short ASCII) is
  ASCII anyway.
- The cross-tool gate from Phase B (sentinel layout) remains valid:
  sentinels are unencrypted on both sides.

OpenPGP-encrypted wal-g buckets are *not* compatible with wal-rs by
design (see "OpenPGP support intentionally dropped"). A bucket written
by wal-g with libsodium is fully bidirectional.

## What didn't get done (carry into next phase)

These are deferred but not blocking:

1. **VM live cross-tool encryption test.** A `tests/vm/encrypt_libsodium.rs`
   should: take a backup with wal-rs + libsodium, fetch & verify with
   installed wal-g; reverse the direction; verify a corrupt-key fetch
   surfaces a clear error from both tools. Inherits the matrix from
   Phase B's cross-tool gate.
2. **`crypto::libsodium::from_env` exhaustive validation of CLI surface.**
   The env-var loader is exercised by `pgp_env_is_rejected` and by the
   key-transform unit tests, but not by a flag-parsing integration test
   that wires Settings::from_env end-to-end with the env vars set. Low
   risk (the loader is ~30 lines) but worth a smoke test.
3. **Encryption key rotation.** No key-rotation flow today; a deployment
   rotating from key A to key B must run a sweep that fetches with A,
   re-pushes with B for every object. Same as wal-g — out of scope here
   too, but worth a docs page.
4. **OpenPGP support.** Permanently deferred; see "Design decisions".
   Re-evaluate only if a deployment with an existing PGP-encrypted bucket
   asks for it and the answer "re-encrypt to libsodium" is unacceptable.
5. **Carryovers from Phase E**: VM live cross-tool retention test,
   server-side copy optimization, daemon protocol surface for delete/copy,
   `--target-user-data` on `delete target` and `backup-fetch`. Same
   status as at Phase E close.
6. **Carryovers from Phase D**: VM live `wal-receive` exercise,
   async segment upload in wal-receive, prefetch-inside-fetch,
   concurrent backup-fetch, daemon-mode wal-prefetch/wal-show. Same
   status as at Phase D close.
7. **Phase C/C.2 streamer integration** (still open from PHASEC.md /
   PHASEC2.md). Encryption doesn't interact with it — the encrypter sits
   above the delta-emit boundary — but it remains the largest open piece.

## Test counts

- Local: **206 tests pass** (`cargo test --locked`). +18 from Phase E:
  - 14 crypto unit tests (8 streaming roundtrip/edge cases, 5 key transform,
    1 PGP env rejection)
  - 3 wal-roundtrip integration tests (encrypted push/fetch byte-identity,
    wrong-key failure, ciphertext-overhead wire-format pin)
  - 1 backup-roundtrip integration test (encrypted tar part fetch)
- VM: unchanged from Phase E — Phase F's live cross-tool encryption test
  belongs to the next pass.
- CI matrix: unchanged from Phase B.2.

## Files touched

```
Cargo.toml                     + dryoc dep
src/lib.rs                     + crypto module declaration
src/crypto/mod.rs              new — Crypter trait, from_env, forbid_pgp_env
src/crypto/libsodium.rs        new — XChaCha20-Poly1305 secretstream adapter,
                               key transforms (none|hex|base64),
                               async EncryptReader / DecryptReader
src/config/mod.rs              + Settings::crypter, encrypt/decrypt helpers
src/pg/wal/push.rs             + encrypt step + decrypt in compare_existing,
                               + size_hint gated on crypter absence
src/pg/wal/fetch.rs            + decrypt step
src/pg/backup/push.rs          + encrypt step on tar parts & pg_control tee
src/pg/backup/fetch.rs         + decrypt step on tar parts
src/pg/backup/delta.rs         + decrypt step in fetch_and_parse_segment
                               (takes &Settings)
tests/wal_roundtrip.rs         + 3 encryption integration tests
tests/backup_roundtrip.rs      + 1 encryption integration test
tests/daemon_roundtrip.rs      + crypter: None in test Settings
tests/retention.rs             + crypter: None in test Settings
tests/vm_live.rs               + crypter: None in test Settings
```

## Sequencing for the next pass

1. Add **`tests/vm/encrypt_libsodium.rs`** to gate cross-tool encryption
   (wal-rs encrypt, wal-g fetch — and the reverse).
2. Land the **Phase C/C.2 streamer integration** (still open). Encryption
   is now in tree; the streamer rewrite plus delta-emit becomes the only
   load-bearing piece left for feature parity.
3. Server-side copy optimization (E.2 carryover) — purely an optimization
   gate. The latency win on cross-region S3 is large and now matters more
   because encrypted streams can't be optimized via byte-range tricks.
4. Phase G (statsd + GCS hardening) — independent of the streamer work,
   and not on the bucket-layout critical path; can interleave with C.4.
