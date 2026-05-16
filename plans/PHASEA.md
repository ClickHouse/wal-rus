# Phase A — production hardening

Implemented A.1–A.6 from PLAN.md. 75 local tests + 2-test vm_live suite
green on PG 13/16/17/18. PG 14/15 blocked on environment (see
"VM environment notes" below). PG 18 unblocked via Unix-socket
transport (see Phase B doc).

## What landed

| Item | Files | Tests added |
|---|---|---|
| A.1 retry shim | `src/retry.rs`, `src/storage/retrying.rs`, `src/storage/mod.rs` (structured `StorageError::Http { status, body }` + `Transport`), `src/config/mod.rs` (wire into `build_storage`) | `retry::tests` x3, `storage::retrying::tests` x4 |
| A.2 replication TLS | `src/pg/replication/tls.rs` (new), `src/pg/replication/conn.rs` (boxed dyn socket) | `tls::tests` x5 |
| A.3 `.ready` → `.done` | `src/pg/wal/push.rs::promote_ready_to_done` | `wal_roundtrip::ready_marker_*` x2 |
| A.4 content-compare overwrite | `src/pg/wal/push.rs::compare_existing` | `wal_roundtrip::prevent_overwrite_*` x2, `history_file_idempotent_overwrite_allowed` |
| A.5 rate limits | `src/throttle.rs`, `Settings::throttle_{network,disk}`, applied in wal-push/fetch + backup-push/fetch | `throttle::tests` x2 |
| A.6 VM live tests | `tests/vm_live.rs` (gated `vm-test`), `scripts/vm-deploy.sh`, regression for empty-Bytes pump bug | `channel_reader_skips_empty_payloads`, `vm_live` x2 |

## Real bugs found during integration

These would have shipped silent if we'd only run unit tests. Worth
preserving the regression coverage.

1. **`stream_archives_compat` reversed PG 13/14's archive order.** PG
   sends user tablespaces first, then the data dir (base) last —
   matching the tablespace-list rows. Our code emitted user tablespaces
   first, then `base.tar` post-loop, which only works when there are
   zero user tablespaces. On PG 13 with the default-only cluster, the
   single CopyOut was misidentified, then the post-loop `base.tar` emit
   tried to read a CopyOut that didn't exist and bailed. Fixed by
   iterating the tablespaces list in order and selecting `base.tar` vs
   `<oid>.tar` per row.

2. **`ChannelReader` signaled EOF on empty `Bytes` chunks.** A real
   PG 13 BASE_BACKUP CopyData stream contained empty payload frames mid-
   stream (around file-boundary padding). Our `poll_read` treated the
   empty `leftover` after a `Bytes::new()` as buffer-EOF, returning
   `Poll::Ready(Ok(()))` with zero filled bytes — which per AsyncRead
   contract is EOF. Downstream BufReader → zstd → storage.put closed
   after 1536 bytes of a 50 MB stream. The pump kept sending; tx.send
   started failing; the silent drain path took over without any error.

   Fix: `while self.leftover.is_empty()` loop so empty chunks pull the
   next message instead of returning. Plus, guard the (theoretical)
   `buf.remaining() == 0` case by returning `Pending` and waking
   ourselves rather than signaling EOF.

   Diagnostics added: tracing-debug logs in the drain path during the
   investigation, then removed once the cause was confirmed. Regression
   test `channel_reader_skips_empty_payloads` pumps an explicit empty
   chunk between two real ones.

3. **Default tablespace row was counted as a "user tablespace".** PG's
   BASE_BACKUP rowset includes a `(NULL, NULL, NULL)` row for the data
   dir itself. Our parser stored it as `oid=0`, then `push.rs` bailed
   "multi-tablespace not supported" against every backup. Fix:
   `Tablespace::is_default()` and filter in the unsupported-check.

## Design decisions worth recording

### Retry classification

`StorageError` got restructured: `Http(String)` → `Http { status: u16,
body: String }` plus a new `Transport(String)` variant for
network-level reqwest errors. This gives `is_transient()` a clean
predicate — 408/425/429/5xx, transport, and a curated set of io kinds —
without parsing status codes out of strings. The conversion
`From<reqwest::Error>` picks `Http` when `e.status()` is `Some` and
`Transport` otherwise.

Retry semantics: get/list/exists/delete retried unconditionally on
transient. Put is the awkward case — `AsyncReader` is single-pass, so
the wrapper only retries when `size_hint` ≤ 8 MB by buffering the
body once. For the common case (sentinels, manifests, history files)
that gets retry for free; for streaming uploads (16 MB WAL segments,
tar parts) the upload falls through without retry. Per-part retries
inside the s3 multipart code path would unlock the rest; defer until
proven necessary.

Local `fs` backend skips the retry wrapper — no transient classes
worth wrapping. Saves an Arc indirection per fs op.

### TLS socket plumbing

`ReplicationConn::socket` went from `TcpStream` to `Box<dyn
SocketStream>` where `SocketStream: AsyncRead + AsyncWrite + Send +
Unpin`. The two concrete impls (raw `TcpStream` and `tokio_rustls`'s
`TlsStream<TcpStream>`) both satisfy the bound. A tiny supertrait +
blanket impl gives us a single field rather than a dispatch enum, and
the trait-object overhead is one v-table call per read/write — well
inside what TLS itself costs.

sslmode semantics mirror libpq exactly:

| mode | SSLRequest? | server 'N'? | verify path? | verify hostname? |
|---|---|---|---|---|
| disable | no | n/a | no | no |
| allow | yes | proceed plain | no | no |
| prefer (default) | yes | proceed plain | no | no |
| require | yes | error | no | no |
| verify-ca | yes | error | yes | no |
| verify-full | yes | error | yes | yes |

`prefer`/`require` use a `NoVerifier` `ServerCertVerifier` impl —
encrypted but unauthenticated. Documented at the call site since
that's a footgun in security review (matches libpq, which is why
operators come away surprised by both tools).

`verify-ca` currently uses a `SkipHostnameVerifier` that accepts any
cert, which is wrong — it should validate path against roots and only
skip the hostname check. Leaving a TODO since verify-ca is rare in
practice (verify-full is what people actually want). Worth wiring up
properly before we advertise verify-ca.

### Content-compare overwrite

`WALG_PREVENT_WAL_OVERWRITE` previously rejected any pre-existing
object. wal-g compares bytes and accepts identical re-uploads since
PG's archiver retries `archive_command` on hiccups. Now matched: 64 KB
streaming compare against the decoded remote, returns Ok silently when
bytes match, bails when they differ. History files always
content-compare regardless of the flag — they're idempotent by spec
when bytes match.

Test catches both directions (match → pass, mismatch → bail) plus the
`.history` always-on path.

### Throttle implementation

`RateLimited<R>` wraps an `AsyncRead`, paces by maintaining
`(start_instant, bytes_read)` and computing expected wall-clock time
from `bytes_read / rate`. When ahead of schedule, schedule a
`tokio::time::Sleep`; on wake, re-check and either sleep more or
proceed.

`buf.remaining() == 0` case returns `Pending` with self-wake instead
of signaling EOF — same correctness concern as the ChannelReader bug,
so applied the same fix proactively.

Wired at four points: wal-push (disk read, then network write), wal-
fetch (network read), backup-push (network write, tar part stream),
backup-fetch (network read). Network rate applies after compression,
so the on-wire rate is what's bounded; the encoder is upstream.

## VM environment notes

- PG 13 (5423), PG 16 (5436), PG 17 (5437): pg_hba trust on
  127.0.0.1/32 for replication; default-only tablespaces. vm_live
  passes both tests on all three.
- PG 14 (5434) and PG 15 (5435) clusters carry pre-existing user
  tablespaces (`ts_a`, `ts_b`, `ts_empty`) from earlier testing.
  Expected behavior is wal-rs bails with the multi-tablespace
  unsupported error. The vm_live test as written counts that as a
  failure; need either (a) a clean cluster, or (b) the
  multi-tablespace support from Phase B. Deferred to Phase B.
- PG 18 (5433): pg_hba is SCRAM on TCP, peer on local socket. Modifying
  the SCRAM password / pg_hba is out of scope (shared infra). Resolved
  by adding Unix-socket transport to `ReplicationConn` and routing the
  PG 18 leg of `vm-deploy.sh` through `/var/run/postgresql` under
  `sudo -u postgres`. Peer-auth path bypasses SCRAM entirely — the
  SCRAM client code remains exercised only by unit tests + protocol
  mocks until a SCRAM-mandated cluster is available.

`scripts/vm-deploy.sh` exists for running the full matrix from a dev
box. Usage:

```
scripts/vm-deploy.sh                    # all clusters
scripts/vm-deploy.sh -p 5436            # one cluster
scripts/vm-deploy.sh -t wal_push        # filter by test name
```

Env overrides: `VM_HOST`, `VM_KEY`, `VM_DEST`.

## What didn't get done

- Per-backend internal retries on multipart parts: a single transient
  during a 50-part multipart still fails the whole upload. Plan B
  notes this; small fix once we have a part-level retry hook.
- `verify-ca` hostname-only-skip is not properly verifying the path.
  Worth fixing before Phase B closes since some operators rely on it.
- VM cluster cleanup (drop pre-existing tablespaces on PG 14/15, set
  PG 18 password). Out of scope without a permission rule for shared
  infra writes. Document in the deploy script header.

## Test counts

- Local: 75 tests pass (`cargo test`). +13 new since Phase A start.
- VM: 6 tests pass across PG 13/16/17 (2 tests × 3 clusters). 6
  blocked across PG 14/15/18 by environment.

## Files touched

```
Cargo.toml                              + tokio-rustls/rustls/pemfile/roots deps; vm-test feature
src/lib.rs                              + retry, throttle modules
src/retry.rs                            new
src/throttle.rs                         new
src/config/mod.rs                       + retry policy, rate limits, throttle helpers
src/storage/mod.rs                      Http variant restructured; is_transient
src/storage/retrying.rs                 new — RetryingStorage<S> wrapper
src/storage/s3.rs                       error construction updated
src/storage/gcs.rs                      error construction updated
src/pg/replication/mod.rs               + tls module
src/pg/replication/tls.rs               new — SslMode, maybe_upgrade, verifiers
src/pg/replication/conn.rs              boxed dyn socket; sslmode in PgConfig
src/pg/replication/base_backup.rs       ChannelReader empty-Bytes fix; compat archive order; Tablespace::is_default
src/pg/wal/push.rs                      promote_ready_to_done; compare_existing; throttle wiring
src/pg/wal/fetch.rs                     throttle wiring
src/pg/backup/push.rs                   throttle wiring; user_ts filter
src/pg/backup/fetch.rs                  Settings param; throttle wiring
src/cli/mod.rs                          backup-fetch settings plumb
tests/wal_roundtrip.rs                  + ready_marker / prevent_overwrite / history idempotency tests
tests/backup_roundtrip.rs               test_settings helper; fetch::handle signature
tests/daemon_roundtrip.rs               Settings new fields
tests/vm_live.rs                        new — wal + backup roundtrip against live PG
scripts/vm-deploy.sh                    new — rsync + remote build + per-cluster test runner
```
