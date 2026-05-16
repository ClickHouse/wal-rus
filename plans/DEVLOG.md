# wal-rs DEVLOG

Session: 2026-05-10
Goal: phase 1 of wal-g Postgres port, optimized for no-overcommit hosts

## What landed

End-to-end `wal-push` / `wal-fetch` and `backup-push` / `backup-fetch` /
`backup-list` working against fs/s3/gcs backends, plus wal-g-wire-compatible
daemon mode. `backup-push` streams BASE_BACKUP over the postgres replication
protocol (PG14- and PG15+ wire forms) — no disk spool, supports remote
backups (sidecar host, no local pgdata). 55 tests pass (`cargo test`).
Single 16MB WAL segment push: **3.9MB RSS, 7.3MB VSZ** without
`MALLOC_ARENA_MAX` set.

```
src/
  main.rs            tokio current-thread entry, env-filter logging
  lib.rs             module roots
  cli/               clap subcommand surface
  config/            WALG_*/AWS_*/GOOGLE_* env parsing & storage selection
  compression/       async-compression reader-to-reader zstd
  storage/           Storage trait, fs/s3/gcs implementations
  pg/wal/            segment naming, push handler, fetch handler
  pg/backup/         sentinel/metadata DTOs, list / fetch / push handlers
  pg/replication/    minimal pg replication client + BASE_BACKUP iterator
  daemon/            wal-g binary protocol server + client
tests/
  wal_roundtrip.rs       push -> fetch byte-identity, compression fallback
  daemon_roundtrip.rs    socket lifecycle + push/fetch via daemon
  backup_roundtrip.rs    list, fetch (raw + zstd tar), LATEST resolution
```

In-tree mock-pg-server tests in `pg/replication/base_backup.rs` script
the PG14- and PG15+ BASE_BACKUP wire responses and verify our pump
emits the right archives in the right order.

## Design decisions

### Async runtime: tokio current-thread by default

`#[tokio::main(flavor = "current_thread")]` for the CLI entry. wal-push as
`archive_command` runs once per 16MB segment; multi-thread runtime would spawn
N worker threads + thread-local malloc arenas for nothing. Daemon mode reuses
the same flavor since I/O is the bottleneck, not CPU.

If basebackup parallelism (phase 5) needs it we can flip flavor per-subcommand
or carve a multi-thread runtime just for that path.

### Storage trait: stream-oriented, no buffer-the-whole-segment

```rust
async fn put(&self, key: &str, body: AsyncReader, size_hint: Option<u64>) -> Result<()>;
async fn get(&self, key: &str) -> Result<AsyncReader>;
```

`AsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>`. Compression is also an
`AsyncReader`, so push pipelines as
`File -> zstd encode -> storage.put` without ever materializing the full
segment in memory.

`size_hint` exists so the s3 backend picks single-PUT vs. multipart for known-size
small objects (sentinels, history files). Unknown size means streaming
multipart unconditionally.

### S3: hand-rolled SigV4 with UNSIGNED-PAYLOAD

Avoided `aws-sdk-rust` (pulls hyper-1, smithy, multi-MB binary footprint) and
`object_store` (arrow deps; abstracts away streaming control). 280-line SigV4
implementation, validated against AWS sample vector
(`signing_key_derivation_matches_aws_sample`).

UNSIGNED-PAYLOAD (allowed on HTTPS) lets us stream request bodies without
hashing the body up front. Trade: relies on TLS for body integrity. Fine for
S3 / TLS-fronted MinIO. If we ever need air-gapped HTTP we'd need
`aws-chunked` streaming sigv4, which is significantly more code.

Multipart: 8MB part size, single PUT under 32MB. Aborts on partial failure
(`abort_multipart`). Ordered ETags collected in completion XML.

Listing: V2 (`list-type=2`) with continuation tokens.

### GCS: service-account JWT, streaming uploadType=media

Reads `GOOGLE_APPLICATION_CREDENTIALS` JSON, signs RS256 JWT via `ring`,
exchanges for OAuth bearer at `oauth2.googleapis.com/token`, caches token until
60s before expiry under a `tokio::sync::Mutex`.

Uploads use `uploadType=media` with chunked transfer encoding via
`reqwest::Body::wrap_stream`. No `content-length` header → server accepts
unbounded streaming. Resumable upload not needed for WAL (16MB segments fit
fine in a single streamed request). Will revisit for basebackup tar parts.

Metadata-server auth (workload identity / GCE) not implemented. First-cut
target is on-prem PG hosts with explicit credential files.

### Compression: async-compression reader-to-reader

```rust
let buffered = BufReader::with_capacity(64 * 1024, input);
Box::pin(ZstdEncoder::with_quality(buffered, Level::Precise(level)))
```

`async_compression::tokio::bufread::ZstdEncoder<R: AsyncBufRead>` is itself an
`AsyncRead`, so wrapping turns the reader pipeline into a single chain
`File -> BufReader -> ZstdEncoder -> Storage::put`. No extra thread per
stream, no mpsc shuttling, no shared blocking pool contention.

First iteration used `tokio::task::spawn_blocking` plus two `tokio::sync::mpsc`
channels to wrap sync `zstd::stream::Encoder`. That worked but spawned a
blocking thread per concurrent compression and added ~500KB pipe buffering.
Switching to async-compression dropped 16MB-segment push from 143MB VmPeak
to 7.3MB VmPeak.

Adds one dep (`async-compression` and its transitive `compression-codecs`),
trade I'm happy with for the memory and code-clarity wins.

### Daemon protocol: wire-compatible with wal-g

Same byte format as `internal/daemon/common.go`:
```
[1 byte type][2 byte BE total length including these 3 bytes][optional body]
```
Body for >=2 args: 1 byte count, then per-arg `[u16 BE len][bytes]`. Single-arg
body is the raw arg bytes per wal-g's `getMessage()`.

Implemented types: Check ('C'), Ok ('O'), Error ('E'), WalPush ('F'), WalFetch ('f').
Defined but unhandled: ArchiveNonExistence ('N'). All others reject.

This means PG hosts can switch `archive_command` between `wal-g daemon-client`
and `wal-rs daemon-client` without changing the daemon socket protocol.

### Object key layout: identical to wal-g

```
<prefix>/wal_005/<segment>           uncompressed
<prefix>/wal_005/<segment>.zst       zstd
<prefix>/basebackups_005/<name>/...  basebackups (phase 5)
```

`005` is wal-g's storage layout version; matching it lets the same bucket be
read by either tool. Also lets retention policies, monitoring, and
existing `wal-show` runs not need adjustment during migration.

### Fallback fetch

`wal-fetch` tries the configured-method extension first, then `.zst`, then no
extension. This handles buckets with mixed-method writes (a common situation
when migrating between compression settings). `Storage::exists` is cheap on
all three backends so the extra HEAD/lookup is acceptable.

### Atomic local writes

`fs` backend writes to `<final>.<ext>.tmp.<pid>` then `rename`. wal-fetch
similarly writes to `<dst>.tmp.<pid>` then renames over `dst`. Postgres'
`restore_command` requires the target file to be intact when it appears.
`fsync` happens before rename.

### Backups: streaming BASE_BACKUP over the replication protocol

`backup-push` speaks the postgres replication wire protocol directly,
mirroring wal-g's `runRemoteBackup` plus the PG14- / PG15+ split from
[wal-g PR #2262](https://github.com/wal-g/wal-g/pull/2262). No
`pg_basebackup` subprocess, no temp-dir spool. The path:

```
TCP connect → StartupMessage(replication=true)
            → AuthenticationOk | Cleartext | SCRAM-SHA-256
            → ReadyForQuery
IDENTIFY_SYSTEM        → system_identifier
SHOW data_directory    → sentinel.data_dir (when --pgdata not given)
BASE_BACKUP (...)      → start LSN + timeline + tablespace list
                       → per-archive bytes (one CopyOut per tablespace
                          on PG14-, singleton CopyOut with tagged
                          'd'/'p'/'n'/'m' CopyData on PG15+)
                       → end LSN + timeline
                       → CommandComplete + ReadyForQuery
```

A tokio task owns the connection and drives the protocol. It emits a
stream of `BackupEvent`s (Start / Archive / Finish) over an mpsc. Each
`Archive` event carries an mpsc receiver of `Bytes` chunks. The
controller wraps that receiver as `ChannelReader` (an `AsyncRead`),
pipes through `compression::encode` (zstd), and hands the pinned reader
to `Storage::put` for the wal-g-format
`basebackups_005/<name>/tar_partitions/part_NNN.tar.<ext>` key. Backpressure
flows naturally: if the upload stalls, the channel fills, the pump's
`tx.send().await` blocks, the protocol pump stops reading the socket,
and TCP's window closes — no in-memory growth.

PG version dispatch happens at SQL build time. PG15+ uses the new
parenthesized syntax (`BASE_BACKUP (LABEL '...', CHECKPOINT 'fast',
WAL false, MANIFEST 'no', TABLESPACE_MAP true)`); PG14- uses the
legacy space-separated form (`BASE_BACKUP LABEL '...' FAST
TABLESPACE_MAP`). We do not request the manifest stream (`MANIFEST 'no'`):
the server reports start/end LSN and timeline directly through the
result-set framing — there's nothing in the manifest we need
in-band.

Auth is trust + cleartext password + SCRAM-SHA-256 (via
`postgres_protocol::authentication::sasl::ScramSha256`). MD5 password
auth is rejected with a clear error since modern PG defaults to SCRAM.
TLS is not yet wired: replication connections are plain TCP for now.

**Remote support**: with no `--pgdata`, `backup-push` is purely
network-driven. PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE select the
target server. The sentinel's `data_dir` is filled from `SHOW
data_directory` rather than the local FS. This lets a sidecar host run
`backup-push` against the primary without filesystem access.

**Limitations** of v1 backup-push:
- Multi-tablespace bails before any upload happens. Supporting it
  requires a path-rewriting tar streamer that re-tars the
  per-tablespace archive stream with a `pg_tblspc/<oid>/` prefix,
  plus a `TablespaceSpec` on the sentinel and symlink restore in
  `backup-fetch`. wal-g's `TarballStreamer` does this in Go; we'll
  port it once single-tablespace bakes.
- `FilesMetadataDisabled = true` always. We never inspect tar contents,
  so per-file metadata (size, mtime, mode) is not collected. wal-g's
  partial-restore-by-mask is unavailable for backups we produce; full
  restore works fine.
- No delta backups. Adding delta requires walparser to compute page
  delta maps, plus the on-disk format work in wal-g's bundle.
- `compressed_size` in the sentinel reports the uncompressed bytes —
  the zstd-encoded length is not surfaced through the streaming
  pipeline. Cosmetic.
- No part rotation: each archive becomes one tar part. wal-g splits at
  1GB by default for parallel upload/download. Adding rotation needs a
  re-tarring streamer (same code as multi-tablespace).
- TLS not implemented. Connections are plain TCP. Critical to fix
  before non-loopback deployments — currently password-on-the-wire.
- No retries. Network blips fail the backup. Add the storage-layer
  retry shim plus replication-side reconnection together.

`backup-fetch` lists `tar_partitions/`, downloads each part, decompresses
based on extension, untars via the sync `tar` crate inside
`spawn_blocking` with `tokio_util::io::SyncIoBridge` wrapping the async
reader. Tar parts are bounded so the bridge thread isn't long-lived
per part. Order: data parts ascending, then `pg_control` last so a
restore that crashes mid-fetch can't end up with a misleading pg_control
file.

`backup-list` lists `basebackups_005/` directly, filters for the
`_backup_stop_sentinel.json` suffix, fetches each sentinel JSON, sorts by
`StartTime`, and prints a fixed-width table or pretty JSON. The DTO
mirrors wal-g's `BackupSentinelDtoV2` field-for-field including PascalCase
JSON keys; deserialization is tolerant (every Option field has
`#[serde(default)]`) so we read sentinels written by either tool.

### WAL segment naming (`pg/wal/segment.rs`)

Parses the 24-char hex name into `{timeline, log_id, seg_no}` and computes
start LSN given segment size. No XLOG record parsing yet — `wal-push` just
treats segments as opaque blobs. Page-level delta backups (phase 6) will
need the walparser port.

### Memory budget verification

```
$ env WALG_FILE_PREFIX=... WALG_COMPRESSION_METHOD=zstd \
    target/release/wal-rs wal-push <16MB-segment>
VmPeak: 7260 kB  VmSize: 7260 kB  VmHWM: 3924 kB  VmRSS: 3924 kB
```

3.9MB RSS, 7.3MB VSZ peak. No `MALLOC_ARENA_MAX` needed — the current-thread
tokio runtime never spawns workers, so glibc only ever sees the main thread
and allocates one arena. Targets in the plan (30MB RSS, 200MB VSZ) are
loose by ~10x, leaving headroom for retries, telemetry, and concurrent
push fan-out.

### Dependency choices

reqwest 0.13's `rustls` feature pulls aws-lc-rs as the crypto provider.
GCS JWT signing also needs RSA-PKCS1-SHA256, so consolidated on aws-lc-rs
for both rather than carrying ring + aws-lc-rs side-by-side. aws-lc-rs is
documented as API-compatible with ring; the swap was two import changes
(`ring::signature::*` → `aws_lc_rs::signature::*` plus the `KeyPair` trait
import for `public_key()`). `cargo tree --invert ring` is now empty.

Single-stack crypto, FIPS-eligible, no transitive ring. Binary stays at
8.3MB. If we ever needed to drop aws-lc-rs (e.g., to build on platforms
without a C toolchain), the alternative is rustls' `rustls-no-provider`
feature + manual ring provider install.

`cargo outdated -R` was clean as of the 2026-05-10 snapshot:
- hmac 0.12 → 0.13 (`KeyInit` moved out of `Mac` trait, easy fix)
- sha2 0.10 → 0.11 (no API change consumed by us)
- reqwest 0.12 → 0.13 (`rustls-tls` → `rustls`, `query`/`form` now require
  explicit feature flags)

## What's deferred

| Area | Status | Reason |
|---|---|---|
| backup-push (streaming, remote) | done via replication BASE_BACKUP | PG14- and PG15+ wire forms; SCRAM-SHA-256 auth; full only, single tablespace, no files metadata, no TLS |
| backup-fetch / backup-list | done | wal-g-compat sentinel + tar_partitions layout |
| TLS for replication | not started | plain TCP; need rustls wrap on the socket |
| delta backups | not started | requires walparser port |
| multi-tablespace backups | not started | needs path-rewriting tar streamer + TablespaceSpec + symlink restore in fetch |
| tar part rotation (1GB splits) | not started | shares re-tarring streamer with multi-tablespace |
| `backup-show` / `backup-mark` | not started | DTOs ready, just needs CLI surface |
| encryption (libsodium / openpgp) | not started | wal-g format is well-defined; defer |
| GCS resumable uploads | not started | not needed for 16MB WAL segments; needed for tar parts |
| GCS metadata-server auth | not started | service-account file covers most on-prem |
| azure / oss / sh / swift backends | not started | out of scope per user direction |
| brotli / lzma / lz4 | decode + encode landed | for cross-tool bucket compat with older wal-g installs; zstd remains default |
| lzo / gzip | not started | wal-g itself doesn't write these for WAL/basebackups |
| `WALG_PREVENT_WAL_OVERWRITE` content compare | not started | currently rejects on bare existence; wal-g compares contents on `.history` |
| `archive_status/.ready` rename | not started | wal-g renames `.ready` -> `.done` after success |
| retention / `delete` | not started | needs backup metadata schema |
| concurrent prefetch | not started | wal-g `WALG_DOWNLOAD_CONCURRENCY` for `wal-prefetch` |
| `wal-verify`, `wal-show` | not started | needs backup index parsing |
| Windows | won't do | non-goal |

## Known rough edges to revisit

1. `S3Storage::list` uses `stream::unfold` and re-builds a temporary `S3Storage`
   inside the closure to reuse `signed_request`. This works but is uglier than
   I'd like. Cleaner refactor: split sigv4 logic into a free function so the
   closure doesn't need a self-borrowed receiver.

2. The compression bridge spawns a thread per stream. Fine for current
   concurrency caps, but if `WALG_UPLOAD_CONCURRENCY` is bumped to 32+ for
   bulk WAL backfills we'd want a shared zstd thread pool. Defer until proven.

3. `parse_list_v2` does string-based XML extraction. Brittle if S3 starts
   inserting new tags between `<Contents>` blocks. A real XML parser
   (`quick-xml`) is the right call before phase 4. Currently saves a dep.

4. `GcsStorage::object_url` percent-encodes with `NON_ALPHANUMERIC` which is
   stricter than GCS requires (it would accept `/` literal). Test against a
   real bucket before phase 4 to confirm slashes-in-keys round-trip.

5. `prevent_wal_overwrite` check is a HEAD per WAL push. Adds one round-trip
   per archive_command invocation. Daemon mode amortizes this over the
   process lifetime; CLI mode pays it every time.

6. No retries / backoff anywhere. Networks fail. Need a retry layer with
   jittered exponential backoff before this is production-shaped.

7. No bandwidth limiter (`WALG_NETWORK_RATE_LIMIT`). Trivial to add via
   `tokio_util` `Throttle`.

## Next steps in priority order

1. **Retry/backoff layer** wrapping `Storage` trait. Single retry policy struct
   reused across backends. Required before any real-world deployment.

2. **TLS for replication**. Wrap `TcpStream` with rustls (we already pull
   aws-lc-rs) and make `pg_sslmode=require` the default. Plain TCP is
   only safe over localhost / private networks today.

3. **End-to-end backup test against a live PG**. `backup-push` is covered
   today by unit tests + a mock-server pump test. A docker-postgres GH
   action that runs `backup-push -> backup-fetch -> initdb-replace ->
   pg_isready` against PG13/14/15/17/18 (covering both wire forms) would
   catch protocol drift. Bonus: cross-check a wal-g `backup-fetch`
   against a wal-rs `backup-push` for bidirectional compat.

4. **Multi-tablespace + tar rotation**. Both share a re-tarring tar
   streamer port from wal-g's `TarballStreamer`. Once that lands:
   path-rewrite tablespace tars to `pg_tblspc/<oid>/`, emit a
   TablespaceSpec on the sentinel, restore symlinks in fetch, and split
   parts at 1GB while we're in the streamer.

5. **`backup-show` and `backup-mark`**. DTOs already exist; both are pure
   sentinel mutations / queries.

6. **`archive_status/.ready` rename + content-compare on overwrite** to bring
   wal-push to parity with wal-g for the hot path.

7. **Encryption**. Once backup paths exist, wire libsodium and openpgp
   compatibility layers in front of compression in the pipeline.

## Open questions for review

- brotli / lz4 / lzma are now wired through `compression::Method` for parity
  with legacy wal-g buckets; wal-fetch's fallback probes all five extensions.
  zstd remains the default. Have not cross-validated lzma against a wal-g
  writer (async-compression uses xz2's LZMA1 alone format, wal-g uses
  ulikunitz/xz/lzma which is also LZMA1 alone — should match, but unverified
  against a real bucket).
- Do we want bidirectional bucket compatibility (write with wal-rs, read with
  wal-g and vice versa) as a hard guarantee? It currently works because we
  use the same key layout and zstd format, but no test enforces it. We could
  add a `tests_func`-style integration test that stages a wal-g binary.
- For the daemon: is wire compatibility worth it, or do we want to introduce
  a new (richer) protocol with versioning, multiplexed ops, and richer error
  reporting? Current choice is wire-compat to ease migration.
