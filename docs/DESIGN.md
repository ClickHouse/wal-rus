## Goal

Functional parity with wal-g's Postgres surface so an on-prem shop can
swap binaries without touching `archive_command`, sentinels, or bucket
layout. Backups written by either tool restorable by either.

Optimized for https://www.postgresql.org/docs/current/kernel-resources.html#LINUX-MEMORY-OVERCOMMIT

## Runtime

Runtime flavor is picked per command (`Cli::worker_threads`),
overridable via `--threads` / `WALG_THREADS`: 1 builds a current-thread
runtime, >1 multi-thread with that many workers.

Default 1 for most commands. `wal-push` runs once per 16 MB segment as
`archive_command`, so extra worker threads would only add per-thread
malloc arenas; daemon mode stays at 1 (I/O bound). Commands with real
per-task CPU work (compress, encrypt, checksum, TLS) default to
multi-thread capped by the matching concurrency knob: `backup-push`
min(cores, upload concurrency); `backup-fetch` / `wal-prefetch` /
`wal-restore` min(cores, download concurrency). Bounded so arenas +
stacks stay small and postgres keeps its cores.

## Storage trait

```rust
async fn put(&self, key: &str, body: AsyncReader, size_hint: Option<u64>) -> Result<()>;
async fn get(&self, key: &str) -> Result<AsyncReader>;
```

`AsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>`. Compression and
encryption are also `AsyncReader`s, so a push pipeline is
`File → compress → encrypt → storage.put` with nothing materialized.
`size_hint` lets S3 pick single-PUT vs multipart, left unset under
compression/encryption (variable-length output makes the hint lie) so
the unknown-size path takes over.

Pipeline order matches wal-g: push `raw → compress → encrypt → storage`,
fetch inverse. Sentinel / metadata JSON bypass compress+encrypt (wal-g
`UploadDto` behavior), so `backup-list` and `delete` work against an
encrypted bucket without the key.

### S3

Hand-rolled SigV4 instead of `aws-sdk-rust` (multi-MB dependency
footprint) or `object_store` (arrow deps, hides streaming control).
UNSIGNED-PAYLOAD over HTTPS streams bodies without hashing up front, TLS
covers integrity. Multipart parts buffer in memory so a transient retry
replays identical bytes, the safety net since UNSIGNED-PAYLOAD leaves
the body unsigned. Unknown-size bodies buffer up to the single-PUT cap
and skip the multipart create/upload/complete trio when they fit, so a
compressed 16 MiB segment lands in one PUT.

Credentials resolve as a chain (`storage/creds.rs`): static keys
(`AWS_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`),
else IMDSv2 (token PUT then authenticated GET, falling back to
unauthenticated v1 if the token is refused), caching temporary creds and
refetching 5 min before expiry; the lock spans the fetch so concurrent
signers single-flight. `AWS_EC2_METADATA_DISABLED` forces the
static-only path, `AWS_EC2_METADATA_SERVICE_ENDPOINT` overrides the
link-local address. Rotating IMDS keys would break the key-based
server-side-copy identity, so IMDS folds to a constant identity.
Profile/shared-credentials files and STS web-identity
(`AWS_WEB_IDENTITY_TOKEN_FILE`) are not implemented.

### GCS

Service-account JWT (RS256 via aws-lc-rs) exchanged for an OAuth bearer,
cached until 60 s before expiry. Uploads stream via `uploadType=media`.
Resumable uploads and metadata-server auth not implemented.

### Retry classification

`is_transient()` classifies `StorageError::Http { status, body }` +
`Transport`. Reads retry unconditionally on transient. `RetryingStorage`
retries small bounded-size `put`s (sentinels, manifests, history files)
by buffering the body once; larger or unknown-size streams pass through
to S3's own in-place retry, which replays its per-PUT / per-part buffer.
`fs` skips the wrapper.

## Compression

`async_compression` bufread encoders chain as
`File → BufReader → Encoder → put`: no thread per stream, resident
memory stays tiny. `wal-fetch` probes the configured extension first,
then the other codec extensions and bare, handling buckets with
mixed-method writes across a compression-setting migration.

## Replication client

Speaks the PG replication wire protocol directly, no `pg_basebackup`
subprocess, no disk spool. PG14- and PG15+ BASE_BACKUP wire forms both
handled. Auth: trust, cleartext, SCRAM-SHA-256; MD5 rejected. `PGHOST`
starting with `/` dials a Unix socket per libpq convention, skipping TLS.

A tokio task owns the connection and emits `BackupEvent`s over mpsc;
each archive carries an mpsc of `Bytes` chunks wrapped as `ChannelReader`.
Backpressure flows naturally: upload stall → channel fills → pump's send
blocks → TCP window closes. `ChannelReader` loops on empty chunks, since
a real PG 13 stream carries empty CopyData frames mid-stream and an empty
poll-fill would otherwise read as EOF.

### TLS

`sslmode` mirrors libpq exactly: `disable | allow | prefer (default) |
require | verify-ca | verify-full`. `prefer`/`require` encrypt without
authenticating (matches libpq). `verify-ca` delegates to
`WebPkiServerVerifier`, suppressing only `NotValidForName{,Context}`.

Client certificate auth (mTLS): set `PGSSLCERT` and `PGSSLKEY` to a PEM
cert chain and unencrypted private key (PKCS#8 / PKCS#1 / SEC1), presented
in every TLS mode; both required together. Encrypted keys
(`PGSSLPASSWORD`) and libpq's `~/.postgresql/postgresql.{crt,key}` default
location aren't honored, matching the env-only `PGSSLROOTCERT` handling.

## Tar streamer

The BASE_BACKUP path uses `astral-tokio-tar` async archive and builder
APIs. One task per archive re-tars with tablespace path remap, rotates
parts at `WALG_TAR_SIZE_THRESHOLD`, tees `global/pg_control` into its own
part uploaded last, and collects per-file metadata. Part bytes flow
through bounded mpsc chunks into upload workers, overlapping
compression/encryption/storage with re-tarring without a sync bridge
thread.

With a positional `PGDATA`, `backup-push` reads the local data directory
instead of BASE_BACKUP: it brackets the copy with `pg_backup_start` /
`pg_backup_stop`, walks `$PGDATA` plus tablespace symlink targets, and
runs `WALG_UPLOAD_CONCURRENCY` pack workers each streaming one
size-bounded tar part. This is the throughput path for local full and
delta backups; the replication path remains a single source stream
bounded by the BASE_BACKUP protocol. Without `PGDATA` the push is purely
network-driven (`data_dir` from `SHOW data_directory`), so a sidecar host
needs no filesystem access.

`backup-fetch` extracts manually rather than via `Archive::unpack`: the
tar crate's canonicalize guard refuses writes through `pg_tblspc/<oid>`
symlinks that legitimate restores require. `..`-traversal stays blocked.
Tablespace symlinks are created before extraction so the first entry
under `pg_tblspc/<oid>/` can't materialize a real directory there.

BASE_BACKUP uploads drain through `BoundedTasks`, filesystem-source
workers through a `JoinSet`; both bounded by `WALG_UPLOAD_CONCURRENCY`,
bail paths abort in-flight work instead of detaching it.

## Delta backups

Two per-file payload formats, magic-dispatched on apply:

- `wi1`, wal-g's increment format
- PG17 native INCREMENTAL (magic `0xd3ae1f0d`), built from
  `pg_wal/summaries/*.summary` via `--delta-from-wal-summaries`

`IncrementBodyReader` streams header + dirty pages with one BLCKSZ scratch
page, so resident memory is independent of dirty density. Three outcomes
per paged file: incremented, skipped (entry omitted, metadata record
kept), passthrough. Dirty blocks past EOF filtered, apply-side
`read_exact` would underflow otherwise.

Map build fails closed: any WAL-walk error warns, falls back to full, and
leaves `increment_from` unset, so the sentinel never claims a delta the
bucket can't deliver. Fetch walks `increment_from` root→leaf, capped at
64 steps with a visited-set against cyclic sentinels; only the leaf's
tablespace `Spec` is applied (a property of pgdata, not LSN).

The in-memory map is `BTreeMap<RelFileNode, RoaringBitmap>`, matching
wal-g's `map[RelFileNode]*roaring.Bitmap`; roaring keeps dense rewrites
(VACUUM FULL, CREATE INDEX, bulk load) from ballooning resident memory
while staying comparable on sparse OLTP deltas. The on-disk format is a
flat tuple list either way, so it costs nothing in interop.

The sidecar (`<group>_delta`) is never materialized as a struct: a working
file accumulates location tuples append-only across the group's 16
segments, then completion appends the boundary-record tuples, terminator,
and parser seed and streams the file out. Map build folds each sidecar's
tuples back in one at a time, so neither the write nor the read holds a
whole group's locations in memory.

Walparser operates on byte slices (one segment is 16 MiB, already in
memory). WAL-summary and native INCREMENTAL parsing cross-referenced
field-by-field against postgres `src/common/blkreftable.c`,
`src/backend/backup/basebackup.c`, and
`src/bin/pg_combinebackup/reconstruct.c`.

## Encryption

libsodium `crypto_secretstream_xchacha20poly1305` via `dryoc` (pure Rust,
no C toolchain). Key transforms `none | hex | base64` mirror wal-g;
`none` requires ≥ 25 bytes so low-entropy keys can't sneak through.

OpenPGP intentionally unsupported: rPGP pulls dozens of transitives and
its async wrapper buffers whole payloads, breaking the streaming contract;
symmetric AEAD already covers the single-tenant on-prem threat model. Any
`WALG_PGP_*` env var is a hard startup error to prevent silent plaintext
regressions. Buckets don't tag objects encrypted-or-not (matches wal-g),
so the key must stay consistently configured per prefix; a mismatch fails
loudly on first read.

## Retention & copy

Objects ordered by `(timeline, global_seg_no)` from the 24-hex segment
substring (wal-g's `timelineAndSegmentNoLess`). Permanent backups reserve
WAL `[(start_lsn-1)/seg_size, (finish_lsn-1)/seg_size]` inclusive.
`delete` is dry-run by default, `--confirm` executes; the plan struct is
returned so tests assert without parsing logs. `delete target` BFS-walks
the increment graph for dependants.

`copy` reuses source credentials for the destination URI, stream-through
for cross-backend. A single backup's WAL window is `[start_seg,
finish_seg]`; `--with-history` extends to all WAL ≤ finish_lsn.

## Daemon

Byte-compatible with wal-g's Unix-socket protocol
(`[type][u16 BE len][body]`), so `archive_command` can point at either
tool's daemon-client unchanged. Implemented ops: Check, WalPush, WalFetch.

PG's archiver is serial, so a standing `Uploader` keeps a look-ahead pool
saturated across invocations. Foreground `WalPush(N)` acks only once `N`
is durable; `N+1..` pre-upload concurrently
(`lookahead = WALG_UPLOAD_CONCURRENCY - 1`, serial and byte-identical at
1). Replaces wal-g's per-invocation `BgUploader` + on-disk marker dir with
an in-memory inflight/done map.

## wal-receive

START_REPLICATION CopyBoth consumer. Segments pre-extended to seg_size
(`set_len`) so partial tails carry PG's zero pad; rotation hands off
through `wal::push::handle` so compression/retry/rate-limit/`.ready→.done`
stay consistent with archive_command pushes. Shutdown finalizes the
in-flight segment as `<seg>.partial` locally, never uploaded, matching
`pg_receivewal`. Status updates on a 10 s cadence, immediate on
server-requested-reply keepalives.
