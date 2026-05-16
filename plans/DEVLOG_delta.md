# DEVLOG: delta-backup integration (post-PHASEC)

Phase-style retrospective for the work that wires `PHASEC.md` §1 (streamer
integration to emit increment files) and §3 (delta-aware fetch with chain
replay) end-to-end. Closes the "logical hole" PHASEC2 called out: a
configured delta parent now actually produces a delta backup, and `fetch`
walks the chain.

## What landed

| Item | File | Tests |
| --- | --- | --- |
| `StreamerOpts.delta_context` + paged-file classification | `src/pg/backup/tar_streamer.rs` | 5 new unit tests under `tar_streamer::tests::delta_*` |
| `IncrementBodyReader` (streaming wi1/native body, no file-sized buffers) | `src/pg/backup/tar_streamer.rs` | exercised by the 5 streamer tests + benchmark |
| Delta-map build at `BackupEvent::Start` (WAL walk + wal-summaries paths) | `src/pg/backup/push.rs` | logic-only; live PG test deferred |
| Sentinel `DeltaFrom` / `DeltaLSN` / `DeltaFullName` / `DeltaCount` wiring + `_D_<parent>` name suffix | `src/pg/backup/push.rs` | `fetch_applies_delta_chain_wi1`, `fetch_walks_three_step_chain` |
| Fetch chain walk via `sentinel.increment_from`, root → leaf | `src/pg/backup/fetch.rs` | `fetch_walks_three_step_chain` (3-step) |
| `apply_increment_in_place` dispatch in unpack | `src/pg/backup/fetch.rs` | `fetch_applies_delta_chain_wi1` |
| Micro-benchmark (encode + apply + wire-size across 4 workloads) | `examples/bench_increment.rs` | run with `cargo run --release --example bench_increment` |

Net diff is contained to four source files and two test additions.
`tar_streamer.rs` grows the streamer body's classification branch; the
rest is purely additive.

## Test status

```
cargo test
  171 lib tests  ok       (5 new delta-mode streamer)
   12 backup_roundtrip ok (2 new chain-replay)
   14 wal_roundtrip ok
   15 daemon_roundtrip ok
    1 retention ok
```

No live-PG (`vm-test`) coverage yet. The push-side delta map build is
exercised only by way of the streamer-with-delta-context unit tests; an
end-to-end VM test that produces a delta backup against a real PG cluster
and round-trips it is the obvious next addition, and is the path that
will catch any divergence from wal-g's on-disk layout.

## Design choices

### Streaming the increment body

The decision: emit increment headers + filtered page bodies through a
custom `Read` impl rather than buffering the whole file into a `Vec<u8>`.

The naive buffered version was tempting (~30 lines fewer code) but its
worst case is `100 % dirty × 1 GiB rel segment = 1 GiB resident per
concurrent paged file`. The streaming `IncrementBodyReader` keeps memory
flat at one BLCKSZ-sized scratch page regardless of dirty density. Cost
is one extra read-from-input pass to skip clean pages — but the input is
a tar entry, which is purely forward anyway, so skipping is just a sized
`read_exact` into the same scratch buffer.

This matches what wal-g's `incremental_page_reader.go` does (postgres
`reconstruct.c` does the same on the apply side). No file-sized buffers
anywhere on the hot path.

### `IsSkipped` vs `IsIncremented` vs absent

Three distinct outcomes for a paged file in delta mode:

- **`is_incremented = true`**: the input file had ≥1 dirty block that
  actually exists in the file (block-no < file_blocks). Body is rewritten
  as wi1 / native; the entry stays in the tar.
- **`is_skipped = true`**: paged file unchanged since parent (`blocks_for`
  returned `None`) OR all dirty blocks fell past EOF. Entry skipped from
  the tar entirely; only the `FilesMetadataDto` record survives, so
  retention / list / show still see the file. Matches wal-g semantics.
- **absent from delta classification** (Passthrough): non-paged files
  (`PG_VERSION`, `pg_filenode.map`, `pg_xact/*`, ...) and files < BLCKSZ.
  Body passes through unchanged.

Filtering dirty blocks past EOF is non-obvious and matters: the apply
side `read_exact` would underflow if we emitted block numbers past the
current file's range. Tested in `delta_filters_blocks_past_eof`.

### Failing closed when the delta map build fails

`build_delta_map_from_wal` can fail mid-flight (a missing WAL segment, a
parse error on a single record, etc). The choice: log a warning and fall
through to a full backup, *and* leave `sentinel.increment_from = None`.
The sentinel must never claim a delta that the bucket can't deliver — a
restore that walks `DeltaFrom` and finds nothing is much worse than a
larger-than-expected upload.

`push.rs` enforces this by tying the sentinel fields to
`delta_context.is_some()`, not to `parent.is_some()`. They diverge
exactly when the map build failed.

### `_D_<parent>` naming, derived at `Start`

wal-g convention: `base_<24hex>_D_<parent_24hex>`. The new start LSN is
only known at `BackupEvent::Start`, so the resolved name is computed
there. `looks_like_backup_name` already accepted this shape (PHASEC was
forward-looking on naming), and the existing tests in
`delete.rs::try_extract_timeline_seg_no` already exercise it — no name
parsing code needed to move.

### Chain walk in fetch

The fetch chain walk is deliberately stupid: walk `increment_from` until
None, push each `(name, sentinel)`, reverse. Cap at 64 steps to guard
against an accidentally-cyclic sentinel (also caught by `HashSet<>` of
visited names). Apply backups root → leaf so the last writer wins per
block.

`files_metadata.json` is fetched once per chain step to know which paths
were incremented. For a 100 k-file cluster this JSON sidecar is ≈ 10 MB
uncompressed — cheap enough to load fully into RAM and walk repeatedly,
much cheaper than re-parsing on every entry.

### What does NOT chain across the leaf

The leaf sentinel owns `Spec` (tablespace mapping). Intermediate
sentinels carry it too but it'd be identical (Spec is a property of
`pgdata`, not LSN). Applying only the leaf's `Spec` saves one tablespace
symlink rebuild per chain step.

## Benchmark

`cargo run --release --example bench_increment` on this host:

```
─── file=4 MiB (512 blocks), dirty=5 pages (1.0% density) ───
  wire-size:   wi1=40996 bytes  native=49152 bytes  diff=8156
  header:      wi1=36 bytes     native_raw=32 bytes  native_padded=8192 bytes
  encode wi1                               per= 365 ns
  encode native                            per= 405 ns
  apply  wi1                               per=  93.3 µs
  apply  native                            per=  75.9 µs

─── file=64 MiB (8192 blocks), dirty=410 pages (5.0% density) ───
  wire-size:   wi1=3360376 bytes  native=3366912 bytes  diff=6536
  apply  wi1                               per=  26.91 ms     119 MiB/s
  apply  native                            per=  26.91 ms     119 MiB/s

─── file=1024 MiB (131072 blocks), dirty=1310 pages (1.0% density) ───
  wire-size:   wi1=10736776  native=10739712  diff=2936
  apply  wi1                               per=  457 ms        22 MiB/s
  apply  native                            per=  467 ms        22 MiB/s

─── file=1024 MiB (131072 blocks), dirty=65536 pages (50.0% density) ───
  wire-size:   wi1=537133072  native=537141248  diff=8176
  apply  wi1                               per=  507 ms      1009 MiB/s
  apply  native                            per=  512 ms      1001 MiB/s
```

### Observations

1. **Wire-size: wi1 wins on sparse deltas, native catches up under load.**
   Native pays a flat ~8 KiB header tax (the 8 KiB boundary-pad), while
   wi1 pays 8 bytes per file for `file_size`. For a sparse delta (5
   dirty pages in a 4 MiB file) that's an 8156-byte overhead. As N
   grows, both formats converge to `N × BLCKSZ + ~constant` and the gap
   becomes irrelevant.

   For a real cluster with thousands of paged files where only ~1 % are
   touched per backup, this rounds up to "native costs an extra
   ≈8 KiB × dirty_file_count bytes." On a 100 k-rel cluster with 1 k
   dirty files, that's 8 MiB of extra padding bytes per backup — a
   round-off error on any sensible cluster size.

2. **Encode throughput is essentially tied.** Both encoders write a
   small fixed header (<1 KiB even for 65 k blocks of `u32` IDs) plus
   the page bodies. Bottleneck is `Vec::extend_from_slice`. wal-rs
   wires both encoders through `IncrementBodyReader` which doesn't
   buffer page bodies; the bench measures the worse case where the
   caller does buffer (matches the apply-side test setup).

3. **Apply throughput is also tied.** Apply is dominated by the
   per-page `seek + write_all` on the target. Both formats decode their
   header in O(N) at start, then page bodies are identical. Native's
   header-padding read costs one extra `read_exact(8192)` for sparse
   deltas — too small to register in the timing.

4. **The 50 %-dirty case is the boundary where deltas stop saving.**
   At 50 % dirty, the increment is 537 MB for a 1 GiB rel. Plus the
   meta overhead, you're within 10 % of just shipping the file. PG and
   wal-g both default to `WALG_DELTA_MAX_STEPS = 7`; this is why — chain
   too long and your last delta is bigger than the next full.

### Conclusion: which format to default to?

For wal-rs's defaults, `wi1` is the safer choice when the upstream is
PG < 17 or `summarize_wal=off`. When PG17 + summarize_wal=on are
available, the `--delta-from-wal-summaries` path is strictly better
because it skips the WAL-walk on the *push side* (which scales with WAL
volume, not with dirty-page count). The on-wire format choice is
basically symmetric — wi1's slightly smaller header isn't worth
trading away interop with `pg_combinebackup` on the apply side.

## What didn't land

1. **VM-test (`tests/vm_live.rs`) for delta round-trip.** The
   PHASEC integration tests stop at `configure_delta_parent`. A real
   end-to-end test (push full → write some data → push delta →
   fetch delta → diff) needs a live PG cluster and a write workload
   between backups. Worth landing before this is called done.

2. **Delta-sidecar writer in `wal-push`.** PHASEC.md §4: building
   `wal_005/<seg>_delta` files as WAL is archived would let
   `build_delta_map_from_wal` be O(touched-relations) instead of
   O(WAL-volume). Today's WAL-walk path re-parses every segment
   between `[parent.start_lsn, this.start_lsn)` on every delta-push.
   On a 50 GB/day WAL volume with hourly deltas, that's 2 GB of
   redundant WAL parsing per push — fine until it isn't.

3. **`is_skipped` files do not get re-asserted in `FilesMetadataDto`
   on subsequent chain steps.** A delta that omits a file but
   doesn't say "skipped" leaves a tiny ambiguity at fetch time:
   "was the file deleted, or was it just unchanged?" wal-g handles
   this by emitting skipped-file entries in the metadata sidecar.
   Our streamer does emit them (see the `is_skipped` branch), but
   if a file is deleted on the source between parent and this
   backup, `BASE_BACKUP` won't emit it at all and the metadata
   won't carry a tombstone. Restore in that case leaves a stale
   file from the parent. Documenting; matches wal-g's behavior.

4. **Header bytes count toward `WALG_TAR_SIZE_THRESHOLD` budget,
   but only for the new entry.** A pathological case: thousands of
   tiny paged files, each emitting a 24-byte wi1 header + 1 dirty
   page = 8.2 KiB on-wire. Threshold of 1 GiB → 130 k files in one
   part. Should be fine, but worth a real-cluster sanity check.

5. **Native `truncation_block_length` always = current
   `file_size / BLCKSZ`.** wal-g and PG can express truncation
   shorter than the body when a file was truncated server-side
   between parent and this backup. Today we always set
   `truncation = file_blocks`. The apply side honors any value
   correctly; the encode side just doesn't generate the smaller
   one. Symptom would be: a relation truncated between full and
   delta restore as the full's size instead of the delta's. Should
   match what `BASE_BACKUP` reports as `entry_size`, which IS the
   post-truncation size, so this is fine in practice.

## Surprises during implementation

- The tar crate's `Builder::append_data` reads from a `Read`-impl
  argument; you can't write the body byte-by-byte after writing the
  header. This forced `IncrementBodyReader` to gate on `header_pos`
  before yielding page bytes. Three phase states (header / current
  page / load next page) handle it cleanly.

- `tar::Entry<R>` discards unread bytes when you advance `Entries::next()`
  — no need to drain after partial reads. This is what makes the
  streaming-skip-then-emit approach work.

- The native format's truncation behavior was the subtle one. From
  `reconstruct.c` it's clear that the apply side truncates to
  `truncation_block_length × BLCKSZ` after all blocks are written —
  *not* to the implicit `max(block_no) + 1`. wal-rs's apply
  (`apply_increment_in_place`) already implemented this correctly;
  worth flagging because the wi1 path uses `file_size` directly and
  doesn't need to know.

## Files touched

```
src/pg/backup/tar_streamer.rs  +DeltaContext, +IncrementBodyReader, +5 tests
src/pg/backup/push.rs          +delta map build at Start, sentinel wiring, _D_<parent> naming
src/pg/backup/fetch.rs         +chain walk, +apply_increment_in_place in unpack
tests/backup_roundtrip.rs      +fetch_applies_delta_chain_wi1, +fetch_walks_three_step_chain
examples/bench_increment.rs    new — encode/apply/wire-size micro-bench
```

## Follow-ups to file

1. VM-test for delta round-trip (highest priority — covers wal-g
   interop).
2. WAL-side delta sidecar writer (PHASEC §4) once cluster size makes
   on-the-fly WAL re-parse painful.
3. Native `truncation_block_length` < file_blocks for server-side
   truncation. Low priority — `BASE_BACKUP` already reports
   post-truncation size, so the symptom is only visible if a
   relation is truncated between `pg_start_backup` and
   `pg_stop_backup`, which is exceedingly rare.
4. Optional: `WALG_DELTA_MAX_TOTAL_SIZE_RATIO` (wal-g has it). If a
   delta is shaping up to be > 50 % of the parent's size, fall back
   to full mid-flight. Today we always commit to delta once the map
   is built.
