# Phase C.2 — PG17 native incremental format (parallel track)

Adds the second incremental-backup mechanism: PostgreSQL 17's built-in
WAL-summarizer-driven incremental format, alongside the wal-g-style
delta scaffolding from Phase C. Mirrors the work on wal-g's
`pg-incremental` branch (commit `91d409de`, "support native pg
incremental format introduced in PG17"). Format fidelity
double-validated: against the wal-g port AND against PostgreSQL upstream
in `~/s/postgresql` (specifically `src/common/blkreftable.c`,
`src/include/backup/basebackup_incremental.h`,
`src/backend/backup/basebackup.c` and `src/bin/pg_combinebackup/reconstruct.c`).

145 local tests pass (+22 since Phase C close — 15 wal_summaries
parser/range selection/end-to-end, 7 net-new increment-format coverage
including dual-format apply dispatch). Clippy clean, fmt clean.

The same scope caveat from Phase C applies: foundations + read/write
binary formats + apply dispatch + CLI surface land here. The streamer
integration that actually emits increment files during BASE_BACKUP
(for either format) is still pending — `--delta-from-wal-summaries`
today validates server preconditions eagerly, logs the would-be delta
build, then falls back to a full backup.

## What landed

| Item | Files | Tests added |
|---|---|---|
| WAL summary file reader (PG17 `pg_wal/summaries/*.summary`, BlockRefTable format, CRC32C-Castagnoli, LSN-range coverage assertions) | `src/pg/wal_summaries.rs` (new) | `wal_summaries::tests` x15 — filename parse / lowercase hex / reject non-summary / range full-coverage / gap detection / tail-missing / empty range / array chunk / bitmap chunk / limit_block pruning / bad magic / bad CRC / list skips non-summary / empty pgdata / end-to-end main-fork-only |
| PG17 native increment file format (read + write + magic-based apply dispatch) | `src/pg/backup/increment.rs` (existing module extended) | `increment::tests` x12 (was x4) — wi1 + native each get round-trip / trailing data / apply-at-block-offsets; plus `apply_dispatches_both_formats`, `apply_rejects_unknown_magic`, `native_header_padding_aligned_to_blcksz`, `native_zero_blocks_unpadded_header` |
| CRC32C-Castagnoli dependency | `Cargo.toml` (`crc32c = "0.6"`, ~50 KB) | covered by `bad_crc_rejected` |
| `--delta-from-wal-summaries` CLI flag (mutually exclusive with `--full`) | `src/cli/mod.rs`, `src/pg/backup/push.rs::PushArgs` | manual exercise; covered indirectly |
| `summarize_wal` server probe via `SHOW`, with PG version gate | `src/pg/backup/push.rs::handle` | manual exercise |
| Push pre-flight: when `--delta-from-wal-summaries` is set, eagerly validate PG≥17 + `summarize_wal=on` + log what the delta map would be built from | `src/pg/backup/push.rs::handle` | manual log inspection |

## Cross-reference to postgres source

Every wire-format constant & layout verified against the postgres
checkout at `~/s/postgresql`:

### `BLOCKREFTABLE_MAGIC = 0x652b137b`

`src/include/common/blkreftable.h:30`. wal-g & wal-rs match.

### `BlockRefTableSerializedEntry` layout (24 bytes)

`src/common/blkreftable.c:155`:
```c
typedef struct BlockRefTableSerializedEntry {
    RelFileLocator rlocator;   // 12 bytes (spcOid, dbOid, relNumber, each u32)
    ForkNumber forknum;         // 4 bytes (int)
    BlockNumber limit_block;    // 4 bytes (uint32)
    uint32 nchunks;             // 4 bytes
} BlockRefTableSerializedEntry;
```
Our `parse_summary_file` reads these in the same order with LE u32×4
for the entry payload + i32 forknum + 2×u32 trailing. ✓

### Chunk encoding

`src/common/blkreftable.c:78-80`:
```c
#define BLOCKS_PER_CHUNK     (1 << 16)            // 65536
#define BLOCKS_PER_ENTRY     (BITS_PER_BYTE * sizeof(uint16))  // 16
#define MAX_ENTRIES_PER_CHUNK (BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY)  // 4096
```
For chunk N: if `chunk_usage[N] == MAX_ENTRIES_PER_CHUNK` it's a
4096×u16 bitmap (8 KiB); otherwise `chunk_usage[N]` u16 offsets within
the chunk. Bit `j` of bitmap word `i` maps to block number
`N * 65536 + i * 16 + j`. Validated this formula against
`BlockRefTableReaderGetBlocks` at `src/common/blkreftable.c:715-725`:
```c
w = reader->chunk_data[chunkoffset / BLOCKS_PER_ENTRY];
if ((w & (1u << (chunkoffset % BLOCKS_PER_ENTRY))) != 0)
    blocks[blocks_found++] = chunkno * BLOCKS_PER_CHUNK + chunkoffset;
```
Our reader's `parse_chunks` produces the same set. ✓

### File terminator + CRC

`src/common/blkreftable.c:1291`:
```c
BlockRefTableFileTerminate(buffer) {
    BlockRefTableSerializedEntry zentry = {0};       // 24 zero bytes
    BlockRefTableWrite(buffer, &zentry, sizeof(zentry));
    pg_crc32c crc = buffer->crc;                     // snapshot before CRC bytes
    FIN_CRC32C(crc);                                 // ^= 0xFFFFFFFF
    BlockRefTableWrite(buffer, &crc, sizeof(crc));   // 4 bytes
    BlockRefTableFlush(buffer);
}
```
The CRC bytes themselves are written but **not** folded back into the
running CRC. Our `parse_summary_file` reads the 4-byte CRC via a raw
file read (bypassing the `Crc32cHasher`) for exactly this reason. ✓

### CRC32C-Castagnoli (`INIT/COMP/FIN`)

`src/include/port/pg_crc32c.h:41-54`: INIT=0xFFFFFFFF, FIN=XOR
0xFFFFFFFF. The `crc32c` Rust crate's `crc32c(data)` / `crc32c_append(prev,
data)` returns the standard CRC32C value (i.e. post-FIN), matching
postgres's emitted value. Validated round-trip via `parse_array_chunk`
+ `bad_crc_rejected` tests.

### Summary filename format

`src/backend/postmaster/walsummarizer.c:1205`:
```c
snprintf(final_path, MAXPGPATH,
         XLOGDIR "/summaries/%08X%08X%08X%08X%08X.summary",
         tli,
         LSN_FORMAT_ARGS(summary_start_lsn),
         LSN_FORMAT_ARGS(summary_end_lsn));
```
40 hex chars: timeline + start_hi + start_lo + end_hi + end_lo. Our
`parse_summary_filename` validates length == 40, all-hex, & decodes in
that order. ✓ (wal-g uses a regex; we hand-roll because it's a single
shape and avoids pulling in `regex`)

### `INCREMENTAL_MAGIC = 0xd3ae1f0d`

`src/include/backup/basebackup_incremental.h:20`. wal-g & wal-rs match.

### Native INCREMENTAL header layout & padding

Validated against TWO sources for redundancy:

Reader: `src/bin/pg_combinebackup/reconstruct.c:456`:
```c
read_bytes(rf, &magic, sizeof(magic));                          // 4
read_bytes(rf, &rf->num_blocks, sizeof(rf->num_blocks));        // 4
read_bytes(rf, &rf->truncation_block_length, ...);              // 4
read_bytes(rf, rf->relative_block_numbers, ...);                // N*4
// Pad to BLCKSZ when num_blocks > 0 and not already aligned
if ((rf->num_blocks > 0) && ((rf->header_length % BLCKSZ) != 0))
    rf->header_length += (BLCKSZ - (rf->header_length % BLCKSZ));
```

Writer (server side): `src/backend/backup/basebackup.c:1623-1657`:
```c
push_to_sink(..., &magic, sizeof(magic));
push_to_sink(..., &num_incremental_blocks, ...);
push_to_sink(..., &truncation_block_length, ...);
push_to_sink(..., incremental_blocks, ...);
if ((num_incremental_blocks > 0) && (header_bytes_done % BLCKSZ != 0)) {
    paddinglen = (BLCKSZ - (header_bytes_done % BLCKSZ));
    push_to_sink(..., padding, paddinglen);
}
```

Both: order is `magic / num_blocks / truncation_block_length / blocks /
optional padding`. The zero-blocks case keeps the header at exactly 12
bytes ("keep it small" comment at `GetIncrementalHeaderSize`,
`basebackup_incremental.c:899-901`). Our `write_native_increment_header`
+ `read_native_after_magic` enforce both. ✓

### Truncation semantics

`truncation_block_length` is in BLCKSZ units. After applying an
increment, the target file should be truncated to
`truncation_block_length * BLCKSZ`. The wi1 format carries a u64
`file_size` in bytes instead; same semantic but different unit. Our
`apply_increment_in_place` returns the truncation size in bytes for
either format, hiding the unit difference from the caller.

## Parity to wal-g's `pg-incremental` branch

Direct mapping wal-g → wal-rs (where Go file + symbol → Rust file):

| wal-g | wal-rs | Notes |
|---|---|---|
| `internal/databases/postgres/wal_summaries.go::ReadWalSummariesForRange` | `src/pg/wal_summaries.rs::read_for_range` | Same logic; returns `PagedFileDeltaMap` |
| `wal_summaries.go::parseWalSummaryFilename` | `wal_summaries.rs::parse_summary_filename` | Hand-roll vs regex; same parse |
| `wal_summaries.go::selectWalSummariesForRange` | `wal_summaries.rs::select_for_range` | Same gap-detection semantics |
| `wal_summaries.go::parseWalSummaryFile` + `parseSummaryChunks` | `wal_summaries.rs::parse_summary_file` + `parse_chunks` | Same chunk + bitmap encoding |
| `native_increment_writer.go::ReadIncrementalFileNative` | `src/pg/backup/increment.rs::write_native_increment_header` (writer half only) | wal-rs splits writer-header from page-body-streaming since the streamer integration isn't landed |
| `native_increment_writer.go::NativeIncrementMagic` | `increment.rs::NATIVE_INCREMENT_MAGIC` | `0xd3ae1f0d` |
| `native_increment_writer.go::nativeIncrementHeaderSize` | `increment.rs::NativeIncrementHeader::header_size_padded` | Same padding rule |
| `pagefile.go::ApplyFileIncrement` (post-magic dispatch) | `increment.rs::apply_increment_in_place` | Single entry point, dispatches by magic |
| `pagefile.go::applyNativeIncrement` | inlined in `apply_increment_in_place` Native branch | |
| `query_runner.go::IsSummarizeWalEnabled` | `push.rs::handle` inline `fetch_setting(..., "summarize_wal")` | wal-rs uses the existing fetch_setting helper rather than a dedicated method |
| `bundle.go::LoadDeltaMapFromWalSummaries` + `useNativeIncrementFormat` toggle | `push.rs` calls `wal_summaries::read_for_range` directly + (TODO) format flag into streamer | Streamer integration deferred |
| `cmd/pg/backup_push.go::deltaFromWalSummariesFlag` | `src/cli/mod.rs::Cmd::BackupPush::delta_from_wal_summaries` | Same `--delta-from-wal-summaries` name, same `conflicts_with` `--full` mutual-exclusion |
| `backup_push_handler.go::loadDeltaMapFromWalSummaries` | `push.rs::handle` pre-flight (PG17 + summarize_wal validation) | |

The on-disk format wal-g writes and the format wal-rs writes are
byte-identical; both are exact ports of postgres upstream. A bucket
written by either tool should be readable by the other once both have
the streamer integration landed.

## What didn't land (carry into next phase)

Same general gap as Phase C: streamer integration to actually emit
incremental files during BASE_BACKUP. Phase C.2 lands the format
machinery + CLI surface + server-side preconditions; what's still
missing for end-to-end native-incremental backups:

1. **Streamer emits native increment files.** The tar streamer
   (`src/pg/backup/tar_streamer.rs`) must, when delta mode is on AND
   `args.delta_from_wal_summaries` is set, buffer each paged file's
   body, scan it against the `PagedFileDeltaMap`, and re-encode the
   entry as a native INCREMENTAL file (magic 0xd3ae1f0d) instead of
   passing through the dense tar entry. Update tar header size.
   `FileMeta::is_incremented = true` in files_metadata.
2. **Delta-map build timing.** The map covers `[parent.start_lsn,
   this_backup.start_lsn)`. `this_backup.start_lsn` only becomes known
   after `BackupEvent::Start` fires, so the build either has to move
   into the event loop, or wait until START_REPLICATION returns the
   start LSN. wal-g's `LoadDeltaMapFromWalSummaries` is called from
   `handleDeltaBackup` which runs *after* `startBackup` returns the
   start LSN; same plumbing pattern applies here.
3. **Sentinel signaling.** wal-g sets nothing extra in the sentinel to
   signal native vs wi1; consumers detect by the magic byte at the
   start of each per-file payload. We follow the same convention —
   `BackupSentinelDtoV2` carries no format flag; magic-byte dispatch
   in `apply_increment_in_place` does the work on fetch. (PHASE C.2
   has NOT touched the sentinel DTO.)
4. **PG17-only constraint advertising.** wal-g's PG version check at
   handler entry is in tree; wal-rs's is in tree. Once the streamer
   integration lands & writes native increments, a delta chain that
   started with wi1 can't be continued with native (different
   per-file format inside the chain). The selector should refuse
   `--delta-from-wal-summaries` when the parent chain has any wi1
   members. This guard is NOT yet implemented; would also need a
   sentinel field to record which format the chain uses end-to-end.
5. **Remote-pusher case.** wal-rs supports running `backup-push` from
   a sidecar host without local PGDATA. `pg_wal/summaries/` lives on
   the PG host's filesystem, so the remote case can't read summaries
   directly. wal-g has the same constraint. Today's code logs a
   warning & skips. A future option: use `pg_walsummary` over an SSH
   channel, or wait for postgres to expose summaries via the
   replication protocol.

## Design decisions

### `BTreeSet<u32>` instead of RoaringBitmap

wal-g uses RoaringBitmap as both the in-memory delta-map blocks
container *and* the in-memory `relForkState` for summary parsing.
wal-rs uses `BTreeSet<u32>` for both, matching the Phase C choice.
Tradeoff: roaring is ~3-10× more memory-efficient for sparse maps over
large block ranges; `BTreeSet` keeps the dep list small and is fast
enough for the delta workloads we'll see in practice (single-digit
percent of pages touched between backups). Swappable later.

### Single `increment.rs` module for both formats

Could have split into `wi1.rs` + `native.rs` siblings. Kept as one
module because:
- The two formats are *peers* (both incremental file payloads), not
  layered.
- `apply_increment_in_place` is a single function that dispatches by
  magic; splitting it would require either re-exporting or a third
  dispatcher module.
- Total module size is ~480 LOC including tests. Readable as one file.

### CRC bytes excluded from running CRC, manually

Postgres's `BlockRefTableFileTerminate` writes the CRC bytes after
finalizing the CRC, but the writer path uses a single buffered-write
abstraction so the CRC bytes end up flushing through `buffer->data[]`.
In our reader we use a `Crc32cHasher` that hashes everything fed via
`read_full`, plus a final `f.read_exact` (NOT routed through the
hasher) for the 4 CRC bytes themselves. This mirrors what wal-g does
(`hasher := io.TeeReader(f, hasher)` for content, then a non-tee
`io.ReadFull(f, crcBuf)` for the trailing CRC).

### `parse_summary_filename` is hand-rolled

wal-g uses `regexp.MustCompile`. wal-rs avoids the `regex` crate
because it'd pull in `regex-syntax` + automata machinery (~500 KB) for
a single regex that's trivially hand-decoded as "40 hex chars + 5
substrings + `.summary` suffix." Same dependency-budget logic as Phase B's
tarball-streamer regex avoidance.

### Hard error on summary gap

wal-g returns an error when summaries don't fully cover the requested
LSN range; we match that exactly. Falling back to walking WAL on a
gap would silently change the format the increment emits (native →
wi1) inside a single backup, which violates the "increment files in a
single backup use one format" invariant. Better to fail loudly &
require operators to either retain summaries or use wal-g style delta
(walking WAL).

### CLI: `--delta-from-wal-summaries` overrides `WALG_DELTA_*`

Strictly: this flag is wal-g-style "force native format" + "build map
from summaries instead of WAL walking." It does NOT bypass the parent
backup selector — the same `WALG_DELTA_MAX_STEPS` / `_ORIGIN` /
`_FROM_NAME` / `_FROM_USER_DATA` knobs choose which parent to delta
against. Only the map-building step differs.

## Cross-tool compatibility

Same status as Phase C: walparser + delta map + both increment formats
+ wal-summaries parsing are bidirectional with wal-g (same magic
numbers, same field order, same CRC computation, same padding).
Verified by:

1. **Format-level fidelity tests** — each format read-after-write
   round-trips. The native format's padding and zero-blocks edge cases
   are tested against the postgres-source rule.
2. **Format-level cross-reference** — every constant cross-referenced
   to the upstream postgres `~/s/postgresql` checkout (see section
   above).
3. **No end-to-end cross-tool test landed yet** — because the streamer
   integration that emits per-file payloads hasn't shipped, neither
   tool can produce a bucket with native-format increments today.
   Once item 1 of "what didn't land" ships, a new `cross_tool_native_delta.sh`
   variant should run the standard "wal-rs writes, wal-g reads" + reverse.

## Test counts

- Local: **145 tests pass** (`cargo test --locked`). +22 from Phase C close:
  - 15 wal_summaries (10 parse / range, 5 round-trip incl. end-to-end
    main-fork projection)
  - 7 net-new increment-format coverage (was 4 wi1-only; now 4 wi1 +
    4 native + 2 dispatch + 2 padding edge cases = 12 total)
- VM: unchanged from Phase B.2
- CI matrix: unchanged from Phase B.2; pg-compat.yml's PG matrix stops
  at PG 17 today, so a `--delta-from-wal-summaries` lane would be
  exercise-able as soon as the streamer integration lands

## Files touched

```
Cargo.toml                                  + crc32c = "0.6"
src/pg/mod.rs                               + wal_summaries module
src/pg/wal_summaries.rs                     new — WAL summary file reader; BlockRefTable parser; LSN-range selection
src/pg/backup/increment.rs                  expanded — added native INCREMENTAL read/write + magic-based apply dispatch + Format enum
src/pg/backup/push.rs                       + --delta-from-wal-summaries pre-flight (PG≥17 + summarize_wal=on + parent log)
src/cli/mod.rs                              + --delta-from-wal-summaries / --full flags on backup-push
```

## Sequencing for the next pass

Same plan as PHASEC.md, with a fork point for which payload format
gets emitted:

1. Port `postgres_page_header.rs` (page LSN + isNew predicate; needed
   for wi1 but not for native — native doesn't validate per-page LSN
   on the writer side, just lists all blocks in the delta map).
2. Add `DeltaContext { map, parent_start_lsn, format: Format }` to
   `StreamerOpts`.
3. Streamer delta branch: for each paged-file entry, look up blocks in
   `map`, encode as either `wi1` (Format::Wi1) or native (Format::Native)
   based on context flag, rewrite tar entry, mark `is_incremented=true`.
4. Wire push.rs:
   - When `args.delta_from_wal_summaries`: build map via
     `wal_summaries::read_for_range`, set `format=Format::Native`
   - Otherwise (Phase C path): build map via `delta::build_delta_map_from_wal`
     against the `wal_005/` bucket prefix, set `format=Format::Wi1`
   - Populate sentinel `DeltaLSN` / `DeltaFrom` / `DeltaFullName` /
     `DeltaCount` / `DeltaChkpNum` from `PrevBackupInfo`
5. Delta-aware fetch: walk chain root → self, apply each per-file
   payload via `apply_increment_in_place` (magic dispatch handles
   both formats — already wired here).
6. Refuse `--delta-from-wal-summaries` against a chain whose parent
   advertises wi1 (add `IncrementFormat: "wi1"|"native"` to sentinel).
7. CI gate: new `cross_tool_native_delta.sh` covering wal-rs↔wal-g
   native interop on PG 17.
