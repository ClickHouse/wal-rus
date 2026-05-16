# Phase D — WAL operations parity

Implemented D.1–D.6 from PLAN.md. 158 local tests pass (+13 since
Phase C.2 close: 2 segment-arithmetic, 5 wal-receive frame codec /
accumulator, 2 wal-receive partial-finalize on shutdown, 3 wal-show /
wal-restore / wal-verify integration, 1 prefetch round-trip). Clippy
clean, fmt clean.

The wire-level pieces of wal-receive (START_REPLICATION CopyBoth open,
'w'/'k' frame decode, standby status reply, segment rotation +
hand-off to `wal-push`) all land here, but the live-PG long-running
loop has not yet been exercised against a real server — same
"foundations + format machinery green; live-flow validation pending"
caveat as Phase C/C.2's streamer integration. VM-side test belongs
to the next pass.

## What landed

| Item | Files | Tests added |
|---|---|---|
| D.1 concurrent upload/download (`WALG_UPLOAD_CONCURRENCY`, `WALG_UPLOAD_QUEUE`, `WALG_DOWNLOAD_CONCURRENCY` via tokio `Semaphore` + `JoinSet`) | `src/config/mod.rs` (+`upload_queue`), `src/pg/backup/tar_streamer.rs` (+`queue_depth`), `src/pg/backup/push.rs` (rewritten upload loop) | covered by existing roundtrip tests; concurrent behavior verified by inspection (live tests in next VM pass) |
| D.2 wal-prefetch + prefetch-aware wal-fetch | `src/pg/wal/prefetch.rs` (new), `src/pg/wal/fetch.rs::try_promote_prefetched`, `src/pg/wal/segment.rs::SegmentName::next`, `src/cli/mod.rs` | `prefetch_stages_segments_and_fetch_promotes_by_rename`, `next_segment_increments_seg_no`, `next_segment_rolls_log_id` |
| D.3 wal-show (plain + JSON, per-timeline range/gaps/backups) | `src/pg/wal/show.rs` (new), `src/cli/mod.rs` | `wal_show_groups_segments_and_detects_gaps` |
| D.4 wal-verify (integrity + timeline + all) | `src/pg/wal/verify.rs` (new), `src/cli/mod.rs::WalVerifyOp` | `wal_verify_integrity_detects_gap_after_backup` |
| D.5 wal-restore (gap-fill into local dir, `WALG_DOWNLOAD_CONCURRENCY`-bounded) | `src/pg/wal/restore.rs` (new), `src/cli/mod.rs` | `wal_restore_fills_gap_into_local_dir` |
| D.6 wal-receive foundations: START_REPLICATION wire, 'w'/'k' frame decode, status-update reply, segment accumulator with `wal-push` rotation hand-off | `src/pg/wal/receive.rs` (new), `src/pg/replication/conn.rs::expect_copy_both_open`+`send_copy_data`, `src/cli/mod.rs` | `decode_wal_frame`, `decode_keepalive_frame`, `rejects_short_frames`, `status_update_encoding`, `accumulator_rotates_at_segment_boundary` |

## Design decisions worth recording

### D.1 — Concurrent backup-push uploads

Pre-D the push loop processed parts strictly sequentially: each
`parts_rx.recv` blocked on a single in-flight upload. The streamer's
`parts_tx` channel was hard-coded at buffer 1, so producer was already
single-stream by design. We moved upload work onto a `JoinSet`
bounded by `Arc<Semaphore>(settings.upload_concurrency)` and made the
streamer's `parts_tx` buffer configurable via `StreamerOpts.queue_depth`
(driven by `WALG_UPLOAD_QUEUE`).

Why JoinSet rather than `FuturesUnordered`: on the bail path we want
remaining upload tasks aborted, not detached. JoinSet's `Drop` cancels
in-flight tasks; `FuturesUnordered` over `tokio::spawn` would leak
detached spawn handles instead. With JoinSet the early-error path
stays safe.

The streamer's sync writer thread still serializes tar building (one
PartCtx at a time); D.1 buys parallelism on the
`compression + storage.put` half of the pipeline. Real win on
high-latency S3 buckets where the put dominates; smaller win on fs/
LAN.

### D.2 — Prefetch dir layout

`pg_wal/.wal-g/prefetch/` with `running/<seg>` (in-flight) +
`<seg>` (ready). Matches the wal-g layout exactly so a sidecar can run
either tool against the same pg_wal without coordination. `wal-fetch`'s
promotion check uses the dst path's parent (the PG-supplied %p
points at `<pgdata>/pg_wal/<seg>`), so the prefetch dir is reachable
without any extra wiring.

Prefetch failures are logged + skipped rather than failing the batch.
wal-receive may be filling in the freshest segments while prefetch runs;
a transient "not found" is expected. wal-g treats prefetch errors the
same way.

### D.3 — `BTreeSet<SegmentName>` + `Ord` derive

For gap detection we want sorted iteration + O(log N) membership.
`SegmentName` got a derived `Ord` (with the field order
`timeline, log_id, seg_no` matching PG's natural archive order — a
sorted BTreeSet iteration yields segments in chain order). Same
choice as Phase C's `BTreeSet<u32>` for delta block maps: stdlib,
fast enough at typical density (single-digit-percent missing).

Gap detection walks the sorted set linearly, calling `seg.next()`
to step. Stops on each gap, records `(from, to, missing)`, resumes
from `to`. O(N) over the segment count.

### D.4 — Failure semantics

`wal-verify` returns non-zero exit (`anyhow::Err`) when any check
reports `FAILURE`. `EMPTY` (no backups, no segments) is treated as a
clean Ok — we don't want a fresh bucket to gate CI. Match wal-g.

The `timeline` check compares HEAD timeline (newest archived segment's
timeline) against latest backup's timeline. They diverge when a
promotion happened post-backup but pre-archive-flush — a real
operational signal. We do NOT consult the live PG for this; wal-g's
mode is purely bucket-side.

### D.5 — Per-task error tolerance

Same as prefetch: individual segment-restore failures log + continue.
The pattern across all batch downloaders in this phase
(prefetch, restore) is: spawn into JoinSet, capture `(name, Result)`
in the join return, log on join. Storage backends already raise
`NotFound` for missing segments; we don't want a single missing one
to abort the others.

### D.6 — `CopyBothResponse` handling

postgres-protocol 0.6 doesn't recognize `'W'` (CopyBothResponse), the
exact message START_REPLICATION returns. Calling `Message::parse`
fails with `unknown message tag`. We added
`ReplicationConn::expect_copy_both_open` that peeks the first byte,
drains the 'W' frame manually when present, and falls through to
`recv_message` otherwise (so ErrorResponse and ParameterStatus still
route through the typed path).

Subsequent CopyData frames are recognized fine — the 'W' tag is the
only protocol asymmetry between START_REPLICATION and BASE_BACKUP.

### D.6 — Segment accumulator architecture

Each fully-filled segment is shipped via the existing
`wal::push::handle` so compression + retry + rate-limit + `.ready→.done`
all stay consistent with archive_command-driven pushes. Trade-off:
the rotation+push is synchronous inside the receive loop — the
receiver blocks while the segment uploads. Acceptable for a small
single-host PG and keeps the data model simple (one in-flight segment).
A future enhancement would spawn the push as a JoinSet task and only
backpressure the receiver when the queue grows.

Files are pre-extended to seg_size with `set_len` so partial-tail
writes leave a zero pad — same on-disk shape PG emits. WAL-receive's
status updates point at `write_position` (highest LSN durably
written), which is what the server uses to advance `pg_replication_slots`.

### D.6 — Graceful shutdown via SIGINT / SIGTERM

On Unix, the receive loop selects the recv path against
`tokio::signal::unix::{SignalKind::interrupt, SignalKind::terminate}`
futures; on other platforms, `tokio::signal::ctrl_c()`. When a signal
fires, the loop exits and `SegmentAccumulator::finalize_partial` flushes
the in-flight segment, then renames `<seg>` -> `<seg>.partial` in
`archive_dir`. Matches `pg_receivewal`: partial keeps its full seg_size
zero pad, stays local, and is never uploaded — restart re-requests
WAL from the slot's confirmed LSN, so a stale partial is safe to leave
for forensics. Empty placeholder files (signal arrived before any
write) are removed instead of renamed.

### D.6 — Standby status update cadence

10 second tick, matching wal-g. The receive loop uses
`tokio::time::timeout(STATUS_UPDATE_INTERVAL, recv_message)` so when
the server is quiet we still ping; when traffic is heavy the elapsed
check ensures we don't ping more often than every 10s. The server-
requested-reply bit on keepalive frames triggers an immediate ping
(wal-g does the same).

## Compatibility with wal-g

- Prefetch directory layout: identical (`pg_wal/.wal-g/prefetch/`).
- wal-show output: structurally compatible; field names match
  wal-g's pretty/JSON ouput where overlap exists. The JSON keys
  (`timeline`, `start_segment`, `end_segment`, `segments_count`,
  `gaps`, `backups`, `status`) follow wal-g's naming.
- wal-verify modes (`integrity`, `timeline`, `all`) match
  wal-g's subcommand surface.
- wal-receive on-bucket output is identical to archive_command-driven
  pushes (since it routes through `wal-push`).

No new on-bucket layout was introduced in Phase D, so the cross-tool
gate from Phase B remains unchanged.

## What didn't get done (carry into next phase)

These are deferred but not blocking:

1. **VM live exercise of wal-receive.** The frame codec + accumulator
   are unit-tested; the connection loop is not. A
   `tests/vm_live::wal_receive` test should:
   - bring up an empty PG cluster
   - run `INSERT INTO scratch SELECT generate_series(1, 1_000_000)` to
     produce a few segments' worth of WAL
   - launch wal-receive in the background; assert segments land in
     the storage backend
   - verify byte-identity with `pg_receivewal` output for the same
     LSN range
   This needs PG 17 + WAL streaming enabled (default `wal_level=replica`
   is sufficient). VM matrix already covers PG 13–18.
2. **wal-receive: async segment upload.** Today rotation blocks the
   recv loop until the upload finishes. Real workloads with bursty
   WAL + slow S3 would benefit from a per-segment JoinSet task,
   bounded by `WALG_UPLOAD_CONCURRENCY`. Mechanical to add once D.1
   matures.
3. **wal-prefetch: tying into wal-fetch automatically.** wal-g forks
   a background prefetcher inside wal-fetch — every fetch
   opportunistically pre-stages the next N segments. We expose
   `wal-prefetch` as a separate command so the operator drives it,
   eg from `restore_command`. A follow-up could spawn the prefetcher
   inside wal-fetch with a tunable `WALG_PREFETCH_DIR_COUNT`.
4. **Concurrent backup-fetch.** D.1 only touches push. Parallel
   tar-part downloads on fetch would mirror the same JoinSet pattern;
   pg_control extraction needs to stay sequential at the end.
   Deferred — not a wal-g feature today, but a natural symmetric
   improvement.
5. **Daemon-mode wal-prefetch / wal-show.** The Unix-socket daemon
   protocol only exposes Check / WalPush / WalFetch today. Extending
   to wal-prefetch (so a sidecar can drive it without restart) would
   need a new message type. wal-g's daemon supports more ops over
   the same wire; we should stay byte-compatible as we extend.

## Test counts

- Local: **158 tests pass** (`cargo test --locked`). +13 from Phase C.2:
  - 2 segment arithmetic (next/roll)
  - 5 wal-receive (decode 'w'/'k', rejects, status-update encoding,
    accumulator rotation)
  - 2 wal-receive partial-finalize (rename on shutdown, drop empty)
  - 3 wal-show / wal-restore / wal-verify integration roundtrips
  - 1 prefetch roundtrip
- VM: unchanged from Phase C.2 — Phase D's live-flow tests (wal-receive
  against real PG, prefetch under load) belong to the next pass
- CI matrix: unchanged from Phase B.2

## Files touched

```
src/config/mod.rs                    + WALG_UPLOAD_QUEUE wired through Settings
src/pg/backup/push.rs                upload loop rewritten on JoinSet + Semaphore
src/pg/backup/tar_streamer.rs        + StreamerOpts.queue_depth
src/pg/wal/mod.rs                    + prefetch/show/verify/restore/receive modules
src/pg/wal/segment.rs                + SegmentName::next, Ord+Hash derives
src/pg/wal/fetch.rs                  + try_promote_prefetched
src/pg/wal/prefetch.rs               new — pg_wal/.wal-g/prefetch staging
src/pg/wal/show.rs                   new — per-timeline range + gap enumeration
src/pg/wal/verify.rs                 new — integrity + timeline checks
src/pg/wal/restore.rs                new — gap-fill by parallel download
src/pg/wal/receive.rs                new — START_REPLICATION consumer + accumulator
src/pg/replication/conn.rs           + send_copy_data + expect_copy_both_open
src/cli/mod.rs                       + WalPrefetch / WalShow / WalVerify / WalRestore / WalReceive
tests/wal_roundtrip.rs               + 4 new integration tests
tests/{vm_live,daemon_roundtrip,backup_roundtrip}.rs  Settings literal extended with upload_queue
```

## Sequencing for the next pass

1. Land the **Phase C/C.2 streamer integration** (the delta-emit
   tar-streamer rewrite). D.1's JoinSet upload pipeline is now in
   place to soak up the streamer's increased buffer demand, exactly
   as PLAN.md sequenced.
2. Add **`tests/vm_live::wal_receive`** to exercise the live socket
   flow end-to-end (depends on a PG cluster with `wal_level=replica`
   + replication grants — VM already configured).
3. Wire wal-receive into the daemon protocol so a sidecar host can
   drive it without managing process lifecycle. Pick a message type
   number compatible with whatever wal-g exposes — confirm against
   `internal/daemon/protocol.go`.
4. Extend the cross-tool CI script with a `wal-show` golden-file
   diff against wal-g for an identical bucket — pins JSON-shape
   parity to a CI gate rather than just our test fixtures.
