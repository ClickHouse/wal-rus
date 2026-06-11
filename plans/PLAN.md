# wal-rs feature-parity plan with wal-g (Postgres)

Baseline: DEVLOG 2026-05-10 snapshot. End-to-end `wal-push`/`wal-fetch`,
`backup-push`/`backup-fetch`/`backup-list`, daemon mode, fs/s3/gcs backends,
zstd/brotli/lz4/lzma compression. 55 tests passing, 3.9 MB RSS at idle WAL push.

Goal: reach functional parity with wal-g's Postgres surface so an on-prem
shop can swap binaries without touching `archive_command`, sentinels,
bucket layout, or operator runbooks. Bidirectional bucket interop is the
North-star: a backup written by either tool restorable by the other.

Out of scope (unchanged from DEVLOG): azure / oss / sh / swift backends;
Windows; mongo/mysql/redis/etcd/fdb/gp/sqlserver wrappers; orioledb;
pgbackrest interop in either direction.

## Status

Closed phases (each with frozen `PHASE<letter>.md` artifact at repo root):

- **Phase A** — production hardening. PHASEA.md
- **Phase B** — backup format parity (multi-tablespace, tar part rotation,
  files_metadata, backup-show/mark, compressed_size). PHASEB.md
- **Phase B.2** — carryover cleanup + Linux PG CI matrix. PHASEB2.md
- **Phase C** — delta-backup foundations: walparser port, page delta map,
  `wi1` increment file format, WALG_DELTA_* env wiring, parent backup
  selector, WAL → delta-map builder. PHASEC.md
- **Phase C.2** — PG17 native incremental: WAL summary file reader
  (`pg_wal/summaries/*.summary`), native INCREMENTAL file format
  (magic 0xd3ae1f0d) read/write, magic-based apply dispatch,
  `--delta-from-wal-summaries` CLI flag, `summarize_wal` server probe.
  PHASEC2.md
- **Phase D** — WAL operations parity (concurrency, wal-prefetch,
  wal-show, wal-verify, wal-restore, wal-receive foundations). PHASED.md
- **Phase E** — retention (`delete` family) and `copy` command, with
  cross-backend stream-through and same-credential cross-bucket support.
  PHASEE.md

**Phase C/C.2 streamer integration is the load-bearing piece still open.**
Both delta paths land their format machinery + CLI surface + parent
selection but stop short of rewiring `tar_streamer.rs` to emit
increment-format per-file payloads during BASE_BACKUP. Today
`WALG_DELTA_MAX_STEPS>0` and `--delta-from-wal-summaries` both run the
pre-flight eagerly (surfacing misconfig) then warn-and-fall-back-to-full,
so the bucket never claims a delta it can't deliver. See the
"Sequencing for the next pass" sections at the foot of PHASEC.md /
PHASEC2.md for the 7-step finish order.

Next phase ready to begin: **Phase D** (WAL operations parity). All
foundational dependencies are in tree (retry, TLS, storage listing,
files_metadata, backup-list). D.1 (concurrency) lives in the caller
layer above the streamer & is recommended *before* the deferred Phase
C/C.2 streamer integration, since paged-file buffering in delta mode
benefits from an already-parallel upload pipeline.

## Wal-g surface to cover

`cmd/pg` ships these subcommands; checkmark = already wired in wal-rs.

| Command | Status | Phase |
|---|---|---|
| wal-push | ✅ | — |
| wal-fetch | ✅ | — |
| backup-push (streaming, multi-tablespace, no delta-emit) | ✅ full backup; delta pre-flight only | B done; C/C.2 streamer integration pending |
| backup-fetch (full backup, no delta-chain) | ✅ full backup; delta apply machinery in tree, chain walk pending | B done; C.4 pending |
| backup-list | ✅ | — |
| backup-show | ✅ | B done |
| backup-mark | ✅ | B done |
| daemon / daemon-client (Check, WalPush, WalFetch) | ✅ | — |
| delete (before / retain / target / everything / garbage) | ✅ | E |
| copy | ✅ | E |
| wal-prefetch | ❌ | D |
| wal-show | ❌ | D |
| wal-verify | ❌ | D |
| wal-restore | ❌ | D |
| wal-receive | ❌ | D |
| catchup-push / -fetch / -send / -receive / -list | ❌ | H |

Cross-cutting wal-g features:

| Feature | Status | Phase |
|---|---|---|
| retry / exponential backoff on Storage | ✅ | A |
| replication TLS (incl. verify-ca path validation) | ✅ | A, A.2 in B.2 |
| `.ready` -> `.done` rename | ✅ | A |
| `WALG_PREVENT_WAL_OVERWRITE` content compare on .history | ✅ | A |
| `WALG_NETWORK_RATE_LIMIT`, `WALG_DISK_RATE_LIMIT` | ✅ | A |
| multi-tablespace (path-rewriting tar streamer) | ✅ | B |
| tar part rotation (`WALG_TAR_SIZE_THRESHOLD`, default 1 GB) | ✅ | B |
| per-file tar metadata (`FilesMetadata=true`) + partial-restore-by-mask | ✅ | B |
| GNU LongLink + pax extended-header pass-through in streamer | ✅ | B.2 |
| delta backups: walparser, page delta map, wi1 increment, parent select | ✅ foundations | C |
| delta backups: streamer rewires paged-file tar entries into wi1 / native | ❌ | C / C.2 finish |
| PG17 native incremental: WAL summary parser, INCREMENTAL format, dispatch | ✅ foundations | C.2 |
| concurrent upload/download (`WALG_UPLOAD_CONCURRENCY`, `…_DOWNLOAD_…`, `…_QUEUE`) | ❌ | D |
| libsodium encryption (XChaCha20-Poly1305) | ❌ | F |
| openpgp encryption + envelope/KMS variants | ❌ | F |
| GCS resumable uploads | ❌ | G |
| GCS workload-identity / metadata-server auth | ❌ | G |
| statsd metrics (`WALG_STATSD_*`) | ❌ | G |
| disk watcher (`WALG_DISK_RATE_LIMIT` + `internal/diskwatcher`) | ❌ | G |

## Phased roadmap

### Phase A — production hardening (blocks any non-toy deploy) — **closed, PHASEA.md**

Order matters: deploy-blocking issues first.

1. **Storage retry shim.** Single retry policy struct (max-attempts,
   base-delay, max-delay, jitter) wraps every `Storage` impl behind a
   `RetryingStorage<S>`. Idempotent reads retried unconditionally;
   writes retried only when transport-layer error or HTTP 5xx (never on
   4xx). Multipart upload aborts on permanent failure, retries per-part
   on transient. Covers `WALG_DOWNLOAD_FILE_RETRIES` env.
2. **TLS for replication.** Wrap the replication TCP socket with rustls
   (aws-lc-rs provider already in tree). Mirror libpq `sslmode`:
   `disable | allow | prefer | require | verify-ca | verify-full`.
   Default `prefer`. `PGSSLROOTCERT` / `PGSSLCERT` / `PGSSLKEY` env vars.
   Without this we ship cleartext passwords on any non-loopback link.
3. **`.ready` → `.done` rename.** After successful `wal-push`, rename
   `<pgdata>/pg_wal/archive_status/<seg>.ready` to `.done`. wal-g does
   this so `archive_command` can be quieter; we should match. Skip if
   `archive_command` is invoked from inside the daemon (PG never wrote
   `.ready` for daemon-side pushes).
4. **`WALG_PREVENT_WAL_OVERWRITE` content compare.** Today we reject any
   pre-existing object. wal-g compares contents on `.history`/`.partial`
   and accepts identical re-uploads (PG retries `archive_command`
   sometimes). Match the semantics so retried archive commands don't
   wedge the cluster.
5. **Rate limits.** Wire `WALG_NETWORK_RATE_LIMIT` (bytes/sec, applied as
   a tokio `Throttle` on the storage put/get reader) and
   `WALG_DISK_RATE_LIMIT` (same, on the file reader). Trivial; required
   for noisy-neighbor environments.
6. **Live PG integration on VM (see "Testing" below)** before phase A
   closes. wal-push/wal-fetch + backup-push/backup-fetch round-trip
   against PG 13/14/15/16/17/18 and a wal-g cross-check.

### Phase B — backup format parity — **closed, PHASEB.md (+ PHASEB2.md carryover)**

Shared work: port `internal/databases/postgres/tarball_streamer.go`. It
re-tars an incoming tar stream with path rewrites and an output-size
budget. That single component unlocks three features:

1. **Multi-tablespace.** Read the tablespace list from the BASE_BACKUP
   response; for each tablespace, re-tar entries under
   `pg_tblspc/<oid>/`. Record `TablespaceSpec` in the sentinel.
   `backup-fetch` recreates the symlinks (or path-relocates per
   `--tablespace-mapping`).
2. **Tar part rotation.** Same streamer; close current part and start
   `part_NNN.tar.<ext>` when the running tar bytes-written exceeds
   `WALG_TAR_SIZE_THRESHOLD` (1 GB default). Tar parts collected and
   uploaded as today.
3. **Per-file tar metadata.** Inspect tar headers as they pass through
   the streamer; collect `BackupFileDescription` (size, mtime, hash,
   IsIncremented). Flip `FilesMetadataDisabled = false`. Enables
   `backup-fetch --restore-spec` partial restore and is required for
   delta backups (Phase C). Encode as wal-g's `files_metadata.json`
   sidecar under the backup directory.
4. **`backup-show`.** Pretty/JSON dump of a single sentinel + (when
   available) files_metadata.json summary. Pure read.
5. **`backup-mark`.** Re-uploads the sentinel with
   `IsPermanent=true|false`. Pure mutation, no protocol work.
6. **`compressed_size` in sentinel.** Today reports raw bytes. Wire the
   storage `put` layer to surface the compressed byte count back so
   `backup-list` shows realistic numbers.

### Phase C — delta backups — **foundations closed, PHASEC.md; streamer integration pending**

1. **walparser port.** ✅ Read XLOG records, classify by RM, extract
   referenced blocks (`RelFileNode + BlockNumber`). On-disk record
   format stable PG 13–18. `src/pg/walparser/`.
2. **Page delta map + `wi1` increment file format.** ✅ in-memory
   `PagedFileDeltaMap` (`src/pg/backup/delta.rs`); on-disk delta-file
   layout matches wal-g; `wi1` reader / writer / apply in
   `src/pg/backup/increment.rs`.
3. **Delta chain configurator.** ✅ `WALG_DELTA_MAX_STEPS`,
   `WALG_DELTA_ORIGIN`, `WALG_DELTA_FROM_NAME`,
   `WALG_DELTA_FROM_USER_DATA` parsed into `DeltaSettings`;
   `configure_delta_parent` picks parent + computes increment_count,
   surfaces misconfig before BASE_BACKUP runs. Streamer integration
   to actually emit `wi1` per-file payloads is the open piece, shared
   with C.2.4 below.
4. **Delta-aware fetch.** ❌ Carryover. Walk chain root → self via
   `IncrementFullName`, apply each per-file payload through
   `apply_increment_in_place` (magic-dispatch already handles both
   wi1 and native), validate page LSN against backup start LSN.

### Phase C.2 — PG17 native incremental — **foundations closed, PHASEC2.md**

Parallel track to Phase C. Both delta paths share the parent selector
+ sentinel fields, differ only on map-build source + per-file payload
format. Mirrors wal-g's `pg-incremental` branch (commit `91d409de`)
which is also not in upstream `master` as of this writing.

1. **WAL summary file reader.** ✅ `pg_wal/summaries/*.summary`
   (`src/pg/wal_summaries.rs`); BlockRefTable format
   (`src/common/blkreftable.c`), CRC32C-Castagnoli verification, LSN
   range selection with hard-error on gaps. Projects MAIN_FORKNUM
   entries into `PagedFileDeltaMap`.
2. **PG17 native INCREMENTAL format.** ✅ Magic 0xd3ae1f0d
   (`src/include/backup/basebackup_incremental.h`), header padding to
   BLCKSZ, magic-byte dispatch between wi1 and native in
   `apply_increment_in_place`.
3. **CLI surface + server probe.** ✅
   `--delta-from-wal-summaries` flag (mutually exclusive with
   `--full`); PG≥17 + `summarize_wal=on` preconditions checked eagerly
   via `SHOW summarize_wal`.
4. **Streamer emits native increments.** ❌ Same blocker as C.4 above;
   one streamer rewrite lands both formats at once via a
   `DeltaContext { map, parent_start_lsn, format: Wi1 | Native }`
   added to `StreamerOpts`.

### Phase D — WAL operations parity

1. **Concurrent upload/download.** `WALG_UPLOAD_CONCURRENCY` (default 16
   in wal-g), `WALG_UPLOAD_QUEUE`, `WALG_DOWNLOAD_CONCURRENCY`. Bound
   parallelism with a `tokio::sync::Semaphore`. Storage trait keeps its
   1-stream contract; concurrency is at the caller layer
   (basebackup tar parts, prefetch). Recommended *before* the Phase
   C/C.2 streamer rewrite — paged-file delta buffering benefits from
   an already-parallel upload pipeline draining the queue.
2. **`wal-prefetch`.** Walk forward N segments from the requested LSN,
   download into `pg_wal/archive_status/<seg>.prefetch` (wal-g uses
   `.prefetched`). `wal-fetch` checks the prefetch dir first; promotes
   via rename. Concurrency from `WALG_DOWNLOAD_CONCURRENCY`.
3. **`wal-show`.** List backups + WAL segment ranges per timeline; flag
   gaps. Output mirrors wal-g's pretty/JSON formats.
4. **`wal-verify`.** Two checks: `integrity` (no missing segment between
   each backup's start LSN and the latest archived segment) and
   `timeline` (HEAD timeline matches the latest backup's timeline).
5. **`wal-restore`.** Inverse of wal-show gaps: fetch the missing
   segments into a local archive directory.
6. **`wal-receive`.** Long-running `START_REPLICATION` consumer that
   archives streamed segments directly (alternative to
   `archive_command`). Shares socket plumbing with `backup-push`.

### Phase E — retention & copy

1. **`delete` family.** Modes: `before` (time / backup-name),
   `retain` (FULL N / FIND_FULL N), `everything`, `target`,
   `garbage`. Permanent backups skipped. Walks the basebackup index
   and the WAL index together, computes the "earliest LSN still
   needed" line, then issues deletes through the Storage trait.
   Reuses wal-g's algorithm in `delete_handler.go` and the PG-specific
   overrides in `databases/postgres/delete.go`. Bucket-side soft-delete
   (S3 versioning, GCS lifecycle) not in scope.
2. **`copy`.** Cross-bucket / cross-prefix copy of a single backup or a
   range. Storage trait needs a `copy_from(src, dst)` for same-backend
   server-side copies (S3 `x-amz-copy-source`, GCS `rewriteTo`); falls
   back to stream-through for cross-backend.

### Phase F — encryption

Both wired between compression and storage (after decompression on read,
before compression on write — wal-g's order). All keys via env / file.

1. **libsodium.** XChaCha20-Poly1305 secretstream; wal-g's
   `internal/crypto/libsodium`. `WALG_LIBSODIUM_KEY` /
   `WALG_LIBSODIUM_KEY_PATH`. Rust impl via `dryoc` or `sodiumoxide`
   (`dryoc` is pure-Rust, no C toolchain — preferred).
2. **openpgp.** `WALG_PGP_KEY` / `_PATH` / `_PASSPHRASE`. Use
   `pgp` crate (rPGP) for ASCII-armored / binary armored. Validate
   against a wal-g-encrypted bucket. Envelope-PGP / YC KMS variants are
   Yandex-specific; defer unless a deployment needs them.

### Phase G — observability + GCS hardening

1. **statsd.** `WALG_STATSD_ADDRESS`, `WALG_STATSD_EXTRA_TAGS`. Counters
   + timers around storage put/get and backup-push lifecycle. Match
   wal-g's metric names so existing dashboards work.
2. **GCS resumable uploads.** Required for tar parts that approach the
   5 GB single-request cap (and to recover from mid-upload disconnects
   on slow links). Switch from `uploadType=media` to `uploadType=resumable`
   once part size > some threshold (e.g. 32 MB).
3. **GCS workload-identity / metadata-server auth.** `metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token`.
   Optional fallback when `GOOGLE_APPLICATION_CREDENTIALS` is unset and
   the host is on GCE/GKE.

### Phase H — catchup operations

1. **`catchup-push`, `catchup-fetch`, `catchup-send`, `catchup-receive`,
   `catchup-list`.** Replica catchup using a delta against a base. Heavy
   reuse of Phase C delta machinery. Lower priority unless a deployment
   requests it.

## Sequencing

Item identifiers are `<phase>.<n>` with `n` resetting at each phase
(so adding C.5 doesn't renumber D). Closed phase docs use whatever
numbering was current when they closed; refer to PHASE\*.md +
git history if a historical reference doesn't line up.

```
A.1 retry ─── A.6 live VM integration  ── lands continuously after every phase
A.2 TLS ─┐
A.3 .ready ─┤
A.4 overwrite cmp ┘
B.1 tarball_streamer ─┬─ B.2 part rotation
                     ├─ B.3 files metadata ───── C.1 walparser ── C.2 delta map ── C.3 delta chain ── C.4 delta fetch
                     └─ B.4/5/6 backup-show/mark/compressed_size
                                              C.2.1 wal-summaries ── C.2.2 native format ── C.2.3 cli/probe ── C.2.4 streamer (shared with C.3/C.4)
D.1 concurrency ── D.2 wal-prefetch
                ├── D.3 wal-show ─── D.4 wal-verify ─── D.5 wal-restore
                └── D.6 wal-receive
E.1 delete ── E.2 copy
F.1 libsodium ─ F.2 openpgp
G.1 statsd · G.2 gcs resumable · G.3 gcs MD-server   (parallel)
H.1 catchup
```

Phases A–C are sequential gates. Phases D–G can interleave once the
streamer (B.1) is in. D.1 lands before the Phase C/C.2 streamer
rewrite so paged-file buffering benefits from a parallel upload queue.
Phase H is optional.

## Phase artifacts

Each phase closes by writing `PHASE<letter>.md` at repo root (see
PHASEA.md, PHASEB.md as exemplars). Audience is future-you or a
reviewer picking up the next phase cold; the docs are intake material
for the phase that follows. Sections, in order:

- **What landed** — table mapping each PLAN item to files touched and
  tests added
- **Real bugs / issues found during integration** — preserved with
  enough context to recover the regression rationale, not just the fix
- **Design decisions worth recording** — choices not derivable from
  the code (algorithm trade-offs, protocol-compat constraints,
  abandoned alternatives, why X over Y)
- **Cross-tool compatibility** — wal-g↔wal-rs roundtrip status if the
  phase touched on-bucket layout; cite the script that gates it
- **VM environment notes** — per-cluster status, environmental blocks,
  workarounds (with reasons)
- **What didn't get done** — deferred items + reason; this is the
  first thing read at the next phase's intake
- **Test counts** — local + VM, delta vs prior phase
- **Files touched** — short list, one line per file

The next phase begins by reading the prior `PHASE<letter>.md`'s
"What didn't get done" and folding any carryovers that compound under
the next phase's scope into the new phase's scope. Carryovers that
stay deferred get re-listed in the new phase's "what didn't get done"
so they don't fall off the floor between phase docs.

Phase docs are append-only by convention: revisions go into the next
phase's doc rather than back-editing a closed one, so each `PHASE*.md`
remains a frozen snapshot of the bar at close-of-phase.

## Testing strategy (remote VM)

Per `~/claude/CLAUDE.md`: VM `admin@3.83.51.154` (m6g.2xlarge, ARM64,
Debian) with PG 13–18 clusters on ports 5423 / 5434 / 5435 / 5436 / 5437
/ 5433. Source layout — wal-g checkout is _not_ on the VM today; we
either install the wal-g `.deb` or build from source for the cross-check.

### Topology

```
local dev (x86_64) ── rsync ──► VM (~/wal-rs)
                                    │
                                    ├── cargo build --release (aarch64)
                                    ├── docker compose up minio fakegcs
                                    └── PG 13–18 clusters already running
```

`cargo test` runs on the VM (the protocol/replication tests need a live
PG socket); unit tests still run locally. CI on local dev pre-deploy:
`cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`
(unit + mock-server tests).

### What runs on the VM, per phase

| Phase | New VM test |
|---|---|
| A | `tests/vm/replication_tls.rs` — TLS handshake against each PG cluster; cross-check `wal-push` -> `wal-fetch` over an `iptables`-induced packet-loss link to exercise retries |
| A | `tests/vm/wal_roundtrip_live.rs` — for each cluster: spin one xlog (`SELECT pg_switch_wal()`), `wal-push` it, `wal-fetch` to /tmp, `cmp` byte-identical |
| B | `tests/vm/basebackup_multi_tbsp.rs` — `CREATE TABLESPACE ts ...`, `backup-push` -> `backup-fetch` to scratch dir, `initdb`-replace, `pg_isready`, sample query |
| B | `tests/vm/walg_cross_read.rs` — write with wal-rs, read with installed wal-g `backup-fetch` (and reverse); covers both `005` key layout and tar metadata |
| C | `tests/vm/delta_chain.rs` — base + 3 deltas, restore must match a parallel non-delta backup of the same DB state (bytewise after `pg_resetwal`) |
| D | `tests/vm/wal_verify.rs` — synthetically drop a WAL segment and assert `wal-verify` flags it |
| E | `tests/vm/retention.rs` — populate 10 backups + WAL, run each `delete` mode, verify only the expected survivors remain |
| F | `tests/vm/encrypt_libsodium.rs` / `_pgp.rs` — round-trip + cross-tool decrypt |

### Backend test matrix

- `fs`: tmpfs under `/tmp/wal-rs-it`. Default for fast loops.
- `s3`: MinIO in docker on the VM, exposed at `localhost:9000`. Tests set
  `AWS_ENDPOINT=http://localhost:9000`, `AWS_S3_FORCE_PATH_STYLE=true`,
  fake `AWS_ACCESS_KEY_ID`/`SECRET`. SigV4 path-style is what we sign
  today.
- `gcs`: `fsouza/fake-gcs-server` in docker. JWT path is skipped (no key
  exchange against fake server); use anonymous mode by setting
  `STORAGE_EMULATOR_HOST` and bypassing token caching when that env is
  set.

### Cross-tool guarantee (bidirectional bucket interop)

Install upstream wal-g `.deb` (or build from source) onto the VM
into a separate `WALG_FILE_PREFIX=/tmp/cross`. After each phase that
touches on-bucket layout (B, C, F):

1. wal-rs writes a backup → wal-g lists/fetches/decrypts/decompresses.
2. wal-g writes a backup → wal-rs lists/fetches/decrypts/decompresses.

Failures here are p0 — they mean the migration story is broken.

### Test runner

`tests/vm/` is `#[cfg(feature = "vm-test")]` gated. Invocation pattern:

```
ssh admin@3.83.51.154 \
  'cd ~/wal-rs && WALG_FILE_PREFIX=/tmp/x \
   PGPORT=5435 cargo test --release --features vm-test'
```

A `scripts/vm-deploy.sh` wraps the rsync + remote build + test loop:
mirrors the `~/claude/parallel-vm-test.sh` shape so the workflow is
familiar. `rsync --delete --exclude='target' --exclude='*.o'` per the VM
CLAUDE.md guidance.

## Risks & open calls

- **Streamer rewrite for delta emit (C.3/C.4/C.2.4) is the largest open
  piece.** Paged files up to 1 GiB must be buffered & scanned page-by-page
  to encode as `wi1` or native increment payloads. ~500 LOC of integration
  through `tar_streamer.rs` + `push.rs` + sentinel population. Risk: a
  half-finished version would regress Phase B's basebackup tests.
  Mitigation: do D.1 first (parallel upload pipeline drains queue while
  encoder buffers next file).
- **Tarball streamer correctness.** Re-tarring an arbitrary tar with
  path rewrites is fiddly (long-name `LongLink` headers, `pax` extended
  headers, sparse-file entries). LongLink + pax covered in B.2;
  sparse-file fixtures still pending (delta-emit will produce sparse
  entries, so this gates the streamer rewrite).
- **Bucket interop drift.** wal-g may change `files_metadata.json`
  format without versioning. Pin a schema snapshot per minor wal-g
  release and CI a cross-read test.
- **lzma alone-format compat (DEVLOG §Open questions).** Async-compression
  uses xz2 LZMA1-alone; wal-g uses `ulikunitz/xz/lzma` LZMA1-alone.
  Should match; cross-validate in Phase B's wal-g cross-read test.
- **MALLOC_ARENA_MAX auto-set.** DEVLOG flags this as undecided. Defer:
  set via systemd `Environment=` in deployment docs, not by the binary.
  Auto-setenv masks issues if a future phase introduces real worker
  threads.
- **Daemon protocol extension.** Today's protocol carries only Check /
  WalPush / WalFetch. wal-g's daemon supports more ops over the same
  wire. As we wire backup operations into the daemon (mainly for
  long-running `backup-push` health checks from a sidecar), stay
  byte-compatible with wal-g's additions rather than inventing a new
  protocol version — bidirectional binary swap stays cheap.

## Acceptance criteria for "feature parity"

A bucket written by `wal-g v3.x` against the wal-g `005` layout can be:

- listed by `wal-rs backup-list`
- fetched by `wal-rs backup-fetch` including delta chain and encryption
- replayed past the last backup via `wal-rs wal-fetch` driven by
  `restore_command`
- verified by `wal-rs wal-verify`
- retained / deleted by `wal-rs delete` (any mode)

…and the reverse: a bucket exclusively written by wal-rs is operable by
wal-g `v3.x` with the same five operations. The VM cross-tool tests in
Phase B/C/F gate this.
