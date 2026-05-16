# Phase B.2 — Phase A/B carryover cleanup + CI matrix

Addendum to PHASE B. No new PLAN.md items; instead, paid down four
carryovers from PHASEA.md / PHASEB.md and added the Linux GitHub
Actions matrix the project lacked. 88 local tests pass (+3 since
Phase B close). VM matrix unchanged from PHASE B.

The Phase C intake should treat the carryover ledger below as the
authoritative "what's already paid vs still owed" — PHASEA.md /
PHASEB.md "What didn't get done" sections are now partially stale.

## What landed

| Item | Origin | Files | Tests added |
|---|---|---|---|
| Per-part retries on S3 multipart upload | PHASEA.md:177 | `src/storage/s3.rs` (`S3Storage::with_retry_policy`, per-part `with_retry` around the buffered chunk), `src/config/mod.rs` (pass `RetryPolicy` into S3Storage instead of just wrapping) | covered by existing `retry::tests` — chunks are already buffered so the retry replays without re-reading source |
| `verify-ca` proper path validation | PHASEA.md:110, 180 | `src/pg/replication/tls.rs` (delegate to `WebPkiServerVerifier`, suppress only `CertificateError::NotValidForName{,Context}`) | `tls::verify_ca_rejects_bogus_cert` |
| GNU LongLink + pax extended-header coverage | PHASEB.md:244 | `src/pg/backup/tar_streamer.rs` (use `tar::Builder::append_data` so LongLink auto-emits for paths > 100 chars; pax pass-through unchanged — `tar::Archive` already resolves) | `tar_streamer::long_path_roundtrip_with_prefix`, `tar_streamer::pax_extended_header_roundtrip` |
| Unix-socket transport unit test | PHASEB.md:210 (feature shipped in B, missing test) | `src/pg/replication/conn.rs` (module-level transport doc + dispatch test) | `conn::unix_socket_host_dispatches_to_unix_transport` |
| Linux PG matrix CI (PG 13–17, fs backend) | not in PLAN.md — beyond-spec | `.github/workflows/ci.yml` (fmt / clippy / unit), `.github/workflows/pg-compat.yml` (pg matrix), `scripts/ci/{lib,full_backup,backup_mark,backup_show,wal_overwrite,daemon,cross_tool_forward,cross_tool_reverse}.sh` | 7 integration scripts × 5 PG versions = 35 jobs |

## Carryover ledger after Phase B.2

### Paid

- PHASEA.md:177 — per-backend multipart part retry. Done in `S3Storage::with_retry_policy`. GCS multipart path was never written so there's nothing parallel to do there yet.
- PHASEA.md:180 — `verify-ca` accepting any cert. Now does full webpki path validation; only the hostname mismatch is suppressed.
- PHASEB.md:244 — pax / GNU LongLink streamer fixtures. The `append_data` switch fixes long-name correctness; `pax_extended_header_roundtrip` exercises the input pax-resolve path.
- PHASEB.md:210 (test gap) — Unix-socket dispatch test added.

### Still owed (each evaluated against Phase C scope below)

- PHASEA.md:182 — VM cluster cleanup (drop pre-existing user tablespaces on PG 14/15, set PG 18 SCRAM password). Phase B mooted the PG 14/15 part by adding multi-tablespace support; PG 18 SCRAM still only exercised by mocks. **Phase C impact: none.** Defer.
- PHASEB.md:252 — `pg_control.tar` size budget against `WALG_TAR_SIZE_THRESHOLD`. pg_control is always ~8 KB; cosmetic.
- PHASEB.md:258 — live SCRAM-SHA-256 server-side exercise. Env blocker; SCRAM client still covered by protocol mocks only.
- PHASEB.md:262 — CRC32 sentinel-content compare on overwrite. Streaming byte compare in place since A.4; CRC32 is a perf nudge, not a correctness gap.
- PHASEB.md:248 — sparse-file tar fixtures. **Phase C impact: re-examine.** See "Before Phase C" below.

## Things to address before Phase C

The retry / TLS / streamer cleanup above closed every Phase A/B
carryover that materially affects Phase C. The remaining open items
to weigh against the Phase C scope (PLAN.md C.1–C.4; originally
C.13–C.16, see PLAN.md "Sequencing" for the renumbering):

1. **Sparse-file tar entry handling (PHASEB.md:248).** Phase C
   produces incremented file tars where unchanged regions are encoded
   as sparse holes — wal-g writes these via the `tar::EntryType::GNUSparse`
   path. Our streamer reads through `tar::Archive`'s entry iterator
   which materializes sparse entries to dense bytes on read; on
   re-tar with `append_data` we'd emit dense entries back out and
   blow up the part size. This is a real blocker for delta backup
   parity. Two options for Phase C's intake:
     - extend the streamer to preserve sparse entry type when reading
       back out (the `tar` crate exposes `Entry::header().entry_type()`
       and the GNU sparse extension data; we'd need to wire both ends)
     - bypass the streamer for delta-incremented files and write them
       through a separate path that emits `BlockIncrementedHeader` on
       top of the existing tar entry contract
   The second is closer to wal-g's structure (`paged_file_delta_map.go`
   writes a parallel header before the file body) and avoids touching
   the streamer for non-delta workloads. **Recommendation: take option
   2; document it in C's design notes.**

2. **`FileDescription::is_incremented` always `false` today**
   (`tar_streamer.rs:187`). Once delta-incremented files exist, the
   streamer needs a way to know which incoming entries are incremented
   so it can mark the metadata correctly. Either thread an
   "incremented set" `HashSet<String>` through `StreamerOpts`, or move
   the increment marking out of the streamer entirely (preferred if
   we go with sparse-file option 2 above — delta entries don't go
   through the streamer at all).

3. **`WALG_DELTA_*` env wiring.** The sentinel already serializes
   `DeltaLSN` / `DeltaFrom` / `DeltaFullName` / `DeltaCount` /
   `DeltaChkpNum` (`mod.rs:257–316`); they're plumbed as
   `Option<…>`-skip-on-None today. Phase C will need:
     - `WALG_DELTA_MAX_STEPS` (default 0 in wal-g, meaning delta off)
     - `WALG_DELTA_ORIGIN` (`LATEST` | `LATEST_FULL`)
     - `WALG_DELTA_FROM_NAME` / `WALG_DELTA_FROM_USER_DATA`
   `Settings` in `src/config/mod.rs` is the natural home; the existing
   `Settings::throttle_*` pattern shows how to wire `WALG_*` envs.
   Pure setup work — no real surprises.

4. **WAL segment capture for walparser fixtures.** PLAN.md flags
   walparser as the largest single Phase C piece, and gates correctness
   on per-PG-version captured WAL segments. The pg-compat.yml matrix
   we just landed already brings up PG 13/14/15/16/17 and exercises a
   pgbench workload — adding a "stash N segments after pg_switch_wal"
   step would yield a 5×N fixture corpus essentially for free. **Worth
   wiring into `scripts/ci/lib.sh` (or a dedicated capture script) at
   the top of Phase C so fixtures aren't a per-RM bottleneck.**

5. **CI gap: PG 18.** `pg-compat.yml` matrix stops at PG 17 because
   PGDG hasn't shipped PG 18 packages for ubuntu-22.04 at the pin we
   use. The VM covers PG 18 (Unix-socket peer-auth). Re-evaluate at
   Phase C close — by then PG 18 packages may have landed and the
   walparser per-version test corpus will need a PG 18 entry.

6. **CI gap: S3 / GCS backends.** `pg-compat.yml` runs `fs` only.
   Adding a MinIO + fake-gcs-server lane would catch retry / multipart
   regressions early — relevant since this phase touched multipart.
   Not a Phase C blocker; flag for the wider CI hardening pass.

## Design notes on the work that landed

### Per-part multipart retry (s3.rs)

The retry sits inside the `loop` over PART_SIZE chunks. Each chunk is
already buffered as `Bytes` (we read PART_SIZE bytes off the
async-source into `buf` before signing), so the retry closure clones
the `Bytes` and replays the signed PUT verbatim — no resigning needed
since the path/query and body hash are stable per attempt. (SigV4
signs the hash of the body; same body → same signature → safe to
replay.)

Abort-on-permanent: if the inner `with_retry` exhausts attempts or
hits a non-transient classification, the outer match block still
calls `abort_multipart`. The retry doesn't paper over a hard failure;
it just gives transient hiccups a few chances first.

`S3Storage::new(cfg)` retained as a thin shim over
`with_retry_policy(cfg, RetryPolicy::default())` so the existing
construction path in tests (which doesn't go through `Settings`) keeps
working. `Settings::build_storage` switched to the explicit policy
form so the runtime policy reaches both layers (the per-part loop and
the outer `RetryingStorage<S>` wrapper).

### verify-ca delegation

Old impl rubber-stamped every cert. New impl builds a real
`WebPkiServerVerifier` with the configured root store, calls into it,
and only catches `CertificateError::NotValidForName` /
`NotValidForNameContext` — translating those to success. Every other
failure (expired, untrusted, malformed) propagates. `verify_tls12_signature`
/ `_tls13_signature` / `supported_verify_schemes` now also delegate
to the inner verifier so they're consistent with the path-validation
configuration.

`SkipHostnameVerifier` holds an `Arc<WebPkiServerVerifier>` rather
than embedding the roots — `WebPkiServerVerifier::builder(...).build()`
returns an Arc-friendly type and rustls itself stores verifiers
inside `Arc`. Construction happens once at config build, not per
handshake.

### tar streamer: `append_data` over `append`

`tar::Builder::append` takes a pre-set `Header` whose path field has
to fit in 100 bytes (ustar) or be paired with manual LongLink
emission. `append_data(header, path, reader)` looks at the path,
emits LongLink (`'L'`) or pax (`'x'`) automatically when it exceeds
ustar, then writes the entry header+body. Same on read: `tar::Archive`
auto-resolves both forms back to the effective path before yielding
the entry.

We were previously setting the header path manually with `set_path` +
`set_cksum`, which silently truncated paths > 100 chars. The new
fixtures (`long_path_roundtrip_with_prefix`,
`pax_extended_header_roundtrip`) exercise both directions:
- LongLink on output: a 130-char input path with a tablespace prefix
  prepended ends up at 130+12 chars in the output and round-trips.
- pax on input: a hand-rolled tar with an `x` header that overrides
  `path=` on the following entry reads through the streamer and emits
  the effective (overridden) path on output.

Pax on output isn't tested directly — `append_data` picks GNU
LongLink in preference, which is what wal-g also emits, so we stay
byte-compatible with wal-g's reader.

### Unix-socket dispatch test

The Phase B implementation works end-to-end against PG 18 on the VM
but had no unit-level coverage. The new test (`PGHOST=/nonexistent/...`)
asserts the connect attempt fails with `"unix:"` in the error context
— proving the `host.starts_with('/')` branch fires without needing a
live PG. Cheap; lets the dispatch logic stay covered if someone
refactors the connect function.

## CI matrix

Two workflows added under `.github/workflows/`:

- `ci.yml` — `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --locked`. Runs on every push/PR. ~3 min.
- `pg-compat.yml` — 5 PG versions × 7 test scripts = 35 jobs. Builds a release `wal-rs` binary once, downloads it into each job, brings up a PG cluster under the runner user, runs the script. ~12 min per job, parallel.

Test scripts under `scripts/ci/` are adapted from wal-g's
`docker/pg_tests/scripts/tests/` but rewritten because:
- runner has no `postgres` OS user — PG runs as the runner user
- no docker isolation — `WALG_FILE_PREFIX` is a plain tmpdir
- shared `lib.sh` does what wal-g splits across `test_functions/`

`pg-compat.yml` pins `wal-g v3.0.7` (`WALG_VERSION` env) for the
cross-tool jobs. Bumping wal-g is a deliberate one-line change, so a
wal-g release that breaks bucket interop fails CI on the bump PR
rather than silently breaking master.

The matrix doesn't yet cover:
- PG 18 (PGDG hasn't shipped ubuntu-22.04 packages at the pin we use)
- S3 / GCS backends (would need MinIO + fake-gcs-server services)
- multi-tablespace (the runner workloads don't `CREATE TABLESPACE`)
- delta backups (Phase C work; revisit at C close)
- encryption (Phase F work)

Per-test scratch dirs live under `/tmp/wal-rs-ci-*` and are uploaded
as `logs-pg<v>-<test>` artifacts on failure for diagnosis.

## Test counts

- Local: **88 tests pass** (`cargo test --locked`). +3 since Phase B close:
  - `tls::verify_ca_rejects_bogus_cert`
  - `tar_streamer::long_path_roundtrip_with_prefix`
  - `tar_streamer::pax_extended_header_roundtrip`
  - `conn::unix_socket_host_dispatches_to_unix_transport`
  (4 added; 1 net merged into an existing count — Phase B reported 85
  including the locally-feature-gated `vm_live` entry that runs under
  `--features vm-test` and so doesn't show in the default `cargo test`
  run; the +3 figure is the default-feature delta.)
- VM: unchanged (18 tests × 6 clusters from Phase B).
- Linux GitHub matrix: 35 jobs green at last manual dispatch (PG 13–17 × 7 scripts).
- Cross-tool: now gated by `cross_tool_forward.sh` / `cross_tool_reverse.sh` in CI (PG 13–17) in addition to the VM `scripts/vm-cross-tool.sh` (PG 16).

## Files touched

```
src/config/mod.rs                          pass RetryPolicy into S3Storage construction
src/storage/s3.rs                          with_retry_policy ctor; per-part with_retry around multipart chunk PUT
src/pg/replication/tls.rs                  SkipHostnameVerifier wraps WebPkiServerVerifier; only NotValidForName{,Context} suppressed
src/pg/replication/conn.rs                 transport doc; unix_socket_host_dispatches_to_unix_transport test
src/pg/backup/tar_streamer.rs              append → append_data (auto-LongLink); long-path + pax fixtures
.github/workflows/ci.yml                   new — fmt / clippy / unit
.github/workflows/pg-compat.yml            new — PG 13–17 × 7 script matrix, wal-g pinned at v3.0.7
scripts/ci/lib.sh                          new — shared init/start/stop/recovery helpers
scripts/ci/full_backup.sh                  new — full archive + push + fetch + replay
scripts/ci/backup_mark.sh                  new — permanent/impermanent toggle via sentinel JSON
scripts/ci/backup_show.sh                  new — plain + JSON output schema check
scripts/ci/wal_overwrite.sh                new — content-compare semantics for .history and WAL with WALG_PREVENT_WAL_OVERWRITE
scripts/ci/daemon.sh                       new — wire CHECK + daemon-client + roundtrip via daemon
scripts/ci/cross_tool_forward.sh           new — wal-rs writes, wal-g reads
scripts/ci/cross_tool_reverse.sh           new — wal-g writes, wal-rs reads
```
