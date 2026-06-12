# Design notes

Decisions not derivable from reading code, kept terse. Companion docs:
[WALG_COMPAT.md](WALG_COMPAT.md) for interop guarantees,
[TESTING.md](TESTING.md) for test strategy.

## Goal

Functional parity with wal-g's Postgres surface so an on-prem shop can
swap binaries without touching `archive_command`, sentinels, bucket
layout, or operator runbooks. North star: a backup written by either
tool restorable by the other.

Optimized for no-overcommit hosts: every pipeline stage is streaming,
no full-segment or full-file buffering.

## Runtime

Runtime flavor is picked per command before construction
(`Cli::worker_threads`), overridable via `--threads` / `WALG_THREADS`;
1 builds current-thread, >1 multi-thread with that many workers.

Default 1 for most commands: `wal-push` as `archive_command` runs once
per 16 MB segment; multi-thread runtime would spawn worker threads +
per-thread malloc arenas for nothing. Daemon mode stays at 1 since I/O
is the bottleneck.

Commands whose fan-out does real CPU work per task (compress, encrypt,
checksum, TLS) default to multi-thread capped by the matching
concurrency knob, otherwise `WALG_UPLOAD_CONCURRENCY` tasks timeshare
one core and uploads overlap only on network: `backup-push`
min(cores, upload concurrency); `backup-fetch` / `wal-prefetch` /
`wal-restore` min(cores, download concurrency). Worker count stays
bounded so arenas + stacks don't balloon and postgres keeps its cores.

## Storage trait

```rust
async fn put(&self, key: &str, body: AsyncReader, size_hint: Option<u64>) -> Result<()>;
async fn get(&self, key: &str) -> Result<AsyncReader>;
```

`AsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>`. Compression and
encryption are also `AsyncReader`s, so push pipelines as
`File → compress → encrypt → storage.put` without materializing
anything. `size_hint` lets s3 pick single-PUT vs multipart; disabled
whenever a crypter is set since ciphertext overhead would make the hint
lie.

Pipeline order matches wal-g: push `raw → compress → encrypt → storage`,
fetch inverse. Sentinel / metadata JSON bypass compress+encrypt entirely
(wal-g `UploadDto` behavior), so `backup-list` and `delete` work against
an encrypted bucket without the key.

### S3

Hand-rolled SigV4 instead of `aws-sdk-rust` (multi-MB dependency
footprint) or `object_store` (arrow deps, abstracts away streaming
control). UNSIGNED-PAYLOAD over HTTPS streams request bodies without
hashing up front; relies on TLS for body integrity. Multipart at 8 MB
parts, single PUT under 32 MB, abort on permanent failure, per-part
retry on transient (SigV4 signs the body hash, same body → same
signature → safe replay). Listing via list-type=2 continuation tokens.

### GCS

Service-account JWT (RS256 via aws-lc-rs) exchanged for OAuth bearer,
cached until 60 s before expiry. Uploads stream via `uploadType=media`
chunked transfer. Resumable uploads and metadata-server auth not
implemented (see PLAN.md).

### Retry classification

`StorageError::Http { status, body }` + `Transport` give `is_transient()`
a clean predicate: 408/425/429/5xx, transport errors, curated io kinds.
Reads retried unconditionally on transient; put only retried when
`size_hint` ≤ 8 MB by buffering the body once (sentinels, manifests,
history files), streaming uploads rely on the per-part retry inside s3
multipart. `fs` backend skips the wrapper, no transient classes worth
wrapping.

## Compression

`async_compression` bufread encoders chain as
`File → BufReader → Encoder → put`, no thread per stream. First
iteration used `spawn_blocking` + mpsc around sync zstd: worked, but
143 MB VmPeak vs 7.3 MB after the switch.

`wal-fetch` probes the configured extension first, then `.zst`, then
bare, then remaining codec extensions, handling buckets with
mixed-method writes across a compression-setting migration.

## Replication client

Speaks the PG replication wire protocol directly, no `pg_basebackup`
subprocess, no disk spool. PG14- and PG15+ BASE_BACKUP wire forms both
handled. Auth: trust, cleartext, SCRAM-SHA-256; MD5 rejected. Without
`--pgdata`, `backup-push` is purely network-driven (sidecar host needs
no filesystem access, `data_dir` filled from `SHOW data_directory`).
`PGHOST` starting with `/` dials a Unix socket per libpq convention,
skipping TLS.

A tokio task owns the connection and emits `BackupEvent`s over mpsc;
each archive carries an mpsc of `Bytes` chunks wrapped as `ChannelReader`.
Backpressure flows naturally: upload stalls → channel fills → pump's
send blocks → TCP window closes. `ChannelReader` loops on empty chunks,
a real PG 13 stream contains empty CopyData frames mid-stream and an
empty poll-fill reads as EOF per the AsyncRead contract.

### TLS

`sslmode` mirrors libpq exactly: `disable | allow | prefer (default) |
require | verify-ca | verify-full`. `prefer`/`require` encrypt without
authenticating (matches libpq, same operator surprise). `verify-ca`
delegates to `WebPkiServerVerifier`, suppressing only
`NotValidForName{,Context}`.

## Tar streamer

One `spawn_blocking` task per archive bridges async→sync via
`SyncIoBridge`, re-tars with tablespace path remap
(`pg_tblspc/<oid>/…`), rotates parts at `WALG_TAR_SIZE_THRESHOLD`
(1 GiB default, single oversize entry gets its own part), tees
`global/pg_control` into `pg_control.tar.<ext>` uploaded last, collects
per-file metadata. `append_data` auto-emits GNU LongLink for paths
> 100 chars; pax extended headers resolve on read.

`backup-fetch` extracts manually rather than via `Archive::unpack`: the
tar crate's canonicalize guard refuses writes through `pg_tblspc/<oid>`
symlinks, which legitimate PG restores require. `..`-traversal still
blocked; only file/dir/symlink entry types restored. Tablespace
symlinks created before extraction so the first entry under
`pg_tblspc/<oid>/` can't materialize a real directory there.

Uploads drain through a `JoinSet` bounded by
`Semaphore(WALG_UPLOAD_CONCURRENCY)`; JoinSet over `FuturesUnordered`
so the bail path aborts in-flight tasks instead of detaching them.

## Delta backups

Two per-file payload formats, magic-dispatched on apply:

- `wi1`, wal-g's increment format
- PG17 native INCREMENTAL (magic `0xd3ae1f0d`), built from
  `pg_wal/summaries/*.summary` via `--delta-from-wal-summaries`

`IncrementBodyReader` streams header + dirty pages with one BLCKSZ
scratch page, no file-sized buffer regardless of dirty density (naive
buffering worst case: 1 GiB resident per concurrent paged file). Three
outcomes per paged file: incremented (≥1 dirty block in range), skipped
(unchanged, entry omitted, metadata record kept), passthrough
(non-paged paths, files < BLCKSZ). Dirty blocks past EOF filtered,
apply-side `read_exact` would underflow otherwise.

Map build fails closed: on any WAL-walk error, warn + fall back to full
*and* leave `increment_from` unset. The sentinel never claims a delta
the bucket can't deliver. Fetch walks `increment_from` root→leaf,
capped at 64 steps + visited-set against cyclic sentinels; only the
leaf's tablespace `Spec` is applied (it's a property of pgdata, not
LSN).

In-memory delta map is `BTreeMap<RelFileNode, BTreeSet<u32>>` rather
than wal-g's RoaringBitmap: stdlib, on-disk format is a flat tuple list
either way, typical deltas touch < 1 % of pages. Swappable if profiles
disagree.

Walparser operates on byte slices rather than wal-g's reader-of-reader
chains; one segment is 16 MiB and already in memory. wal summaries
parsing cross-referenced field-by-field against postgres
`src/common/blkreftable.c` (see WALG_COMPAT.md).

## Encryption

libsodium `crypto_secretstream_xchacha20poly1305` via `dryoc`
(pure Rust, no C toolchain). Key transforms `none | hex | base64`
mirror wal-g, `none` requires ≥ 25 bytes so low-entropy keys can't
sneak through the legacy path.

OpenPGP intentionally unsupported. rPGP pulls dozens of transitives and
its async wrapper buffers whole payloads, breaking the streaming
contract; symmetric AEAD already covers the single-tenant on-prem
threat model; a migrating PGP bucket re-encrypts once. To prevent
silent plaintext regressions, any `WALG_PGP_*` env var is a hard error
at startup.

Buckets don't tag objects encrypted-or-not (matches wal-g), so the key
must stay consistently configured per prefix; mismatch fails loudly on
first read.

## Retention & copy

Objects ordered by `(timeline, global_seg_no)` extracted from the
24-hex segment substring, wal-g's `timelineAndSegmentNoLess`. Permanent
backups reserve WAL `[(start_lsn-1)/seg_size, (finish_lsn-1)/seg_size]`
inclusive. `delete` is dry-run by default, `--confirm` executes; the
plan struct is returned so tests assert without parsing logs.
`delete target` BFS-walks the increment graph for dependants.

`copy` reuses source credentials for the destination URI, stream-through
for cross-backend; WAL window `[start_seg, finish_seg]` copied with a
single backup, `--with-history` extends to all WAL ≤ finish_lsn.

## Daemon

Byte-compatible with wal-g's Unix-socket protocol
(`[type][u16 BE len][body]`), so `archive_command` can point at either
tool's daemon-client unchanged. Implemented ops: Check, WalPush,
WalFetch.

## wal-receive

START_REPLICATION CopyBoth consumer. Segments pre-extended to seg_size
(`set_len`) so partial tails carry PG's zero pad; rotation hands off
through `wal::push::handle` so compression/retry/rate-limit/`.ready→.done`
stay consistent with archive_command pushes. Shutdown finalizes the
in-flight segment as `<seg>.partial` locally, never uploaded, matching
`pg_receivewal`. Status updates on a 10 s cadence, immediate on
server-requested-reply keepalives.

## Dependency budget

Recurring theme: prefer hand-rolling small fixed formats over pulling
crates. No `regex` (summary filenames + tablespace prefixes are trivial
decodes), no roaring, no aws-sdk, no quick-xml (string-extraction over
S3 list XML, revisit if AWS changes tag layout). Single crypto stack on
aws-lc-rs (rustls provider + GCS RS256), no transitive ring.
