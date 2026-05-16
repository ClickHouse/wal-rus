# Phase E — retention & copy

Implemented E.1 (`delete` family) and E.2 (`copy`) from PLAN.md, plus
two carry-overs originally deferred to Phase F: `delete retain --after`
and `backup-mark --target-user-data`.
**188 local tests pass** (+30 since Phase D close: 16 delete unit tests +
14 retention/copy integration tests). Clippy clean, fmt clean.

The delete family covers `before` / `retain` / `everything` / `target` /
`garbage` with all wal-g modifiers (`FULL`, `FIND_FULL`, `FORCE`,
`ARCHIVES`, `BACKUPS`). `retain` additionally accepts `--after
<ts|name>`. Default is dry-run; `--confirm` executes. The copy command
supports same-backend cross-prefix and same-scheme cross-bucket
(S3/GCS), plus cross-backend via stream-through fallback. `backup-mark`
accepts either a positional name (or `LATEST`) or `--target-user-data
<json>` as a selector.

VM-side cross-tool wal-g↔wal-rs retention check belongs to the next pass.

## What landed

| Item | Files | Tests added |
|---|---|---|
| E.1 delete: `before` (name / RFC3339 timestamp / `FIND_FULL`) | `src/pg/backup/delete.rs::plan_before` + `resolve_before_target` | `delete_before_name_drops_older_only`, `find_full_walks_chain_root`, `extract_segno_*` |
| E.1 delete: `retain` (`FULL N` / `FIND_FULL N` / plain `N`) | `src/pg/backup/delete.rs::plan_retain` + `resolve_retain_target` | `delete_retain_keeps_n_newest`, `retain_target_walks_backups`, `retain_full_modifier_only_counts_fulls` |
| E.1 delete: `retain --after <ts\|name>` | `src/pg/backup/delete.rs::resolve_retain_after_target` + `resolve_after_target` | `retain_after_time_picks_older_of_two_anchors`, `retain_after_time_falls_back_to_retain_when_no_after_match`, `retain_after_name_latches_on_match`, `retain_after_full_modifier_picks_full_only`, `retain_after_rejects_future_timestamp`, `delete_retain_after_keeps_newer_than_boundary` |
| backup-mark `--target-user-data` selector | `src/pg/backup/show.rs::resolve_by_user_data` + `src/cli/mod.rs::BackupMark` | `backup_mark_target_user_data_flips_sentinel`, `backup_mark_target_user_data_rejects_no_match`, `backup_mark_target_user_data_rejects_ambiguous` |
| E.1 delete: `everything [FORCE]` | `src/pg/backup/delete.rs::plan_everything` | `delete_everything_refuses_with_permanent_unless_force` |
| E.1 delete: `target [FIND_FULL]` (chain-walk vs single-tree) | `src/pg/backup/delete.rs::plan_target` + `find_related_backups` / `find_dependant_backups` | `delete_target_drops_delta_dependants`, `delete_target_find_full_drops_chain_root`, `find_dependants_walks_increment_chain` |
| E.1 delete: `garbage [ARCHIVES\|BACKUPS]` | `src/pg/backup/delete.rs::plan_garbage` | `delete_garbage_scopes_to_wal_archives_only` |
| E.1 permanent-object preservation (backups + reserved WAL window) | `src/pg/backup/delete.rs::permanent_wal_set` + `is_permanent_object` | `delete_permanent_wal_is_preserved`, `permanent_wal_marks_segments_inclusive`, `permanent_object_check_routes_by_prefix` |
| E.1 dry-run vs `--confirm` execution | `src/pg/backup/delete.rs::handle` + `print_plan` | `delete_dry_run_does_not_delete` |
| E.2 copy: single backup or `--all`, optional `--with-history` | `src/pg/backup/copy.rs` | `copy_single_backup_to_other_fs_prefix` |
| E.2 cross-backend / cross-bucket destination URI parsing | `src/config/mod.rs::build_dst_storage` + `storage_from_uri` | covered by copy integration test |
| CLI surface: `wal-rs delete <op>` + `wal-rs copy --to <uri>` | `src/cli/mod.rs` (+`DeleteCli` subcommand) | covered by integration tests |

## Real bugs / issues found during integration

### Copy's blanket-add of `metadata.json` errored on backups that never wrote it

The first cut of `collect_backup_keys` pushed both
`sentinel_key(name)` and `metadata_key(name)` onto the copy plan
unconditionally. For backups produced without the extended metadata
sidecar (e.g. tests seeding only the sentinel + tar parts), the
follow-up `get` failed with `NotFound`. Caught by
`copy_single_backup_to_other_fs_prefix`.

Fix: rely on listing the per-backup prefix
(`basebackups_005/<name>/…`) which transparently picks up
`metadata.json`, `files_metadata.json`, and the
`tar_partitions/part_NNN.tar.*` set. Only the sentinel needs an
explicit push because it lives one level up.

### Same-type `u64 as u64` cast tripped clippy `-D warnings`

Three locations carried over `(0x1_0000_0000u64 / DEFAULT_WAL_SEG_SIZE) as u64`
patterns from older modules. Clippy 1.94 newly rejects these as
`unnecessary_cast`. Replaced with bare arithmetic. Watch this on future
backports.

## Design decisions worth recording

### E.1 — ordering key

Each object is ordered by `(timeline, global_seg_no)` extracted via a
24-hex-char regex match on the object name. Mirrors wal-g's
`timelineAndSegmentNoLess` exactly. The same comparator works for:

- backup sentinel keys (`base_TTTTTTTT…_backup_stop_sentinel.json`)
- per-backup auxiliary objects (`base_TTTTTTTT…/<file>`)
- WAL segments (`TTTTTTTT….<ext>`)
- delta backups (`base_TTT_A…_D_TTT_B…`, where `TTT_A` is the delta's own
  position; the regex finds the leftmost match)

Strict less-than against the resolved target: the target itself
survives. This is wal-g's `DeleteBeforeTargetWhere` semantics.

### E.1 — permanent backup WAL reservation

Every permanent backup reserves the WAL segments containing its
start LSN through finish LSN inclusive. Implemented per wal-g as
`[(start_lsn-1) / seg_size, (finish_lsn-1) / seg_size]` so the
right-edge segment containing the precise finish LSN is preserved
(matches `pg_walfile_name_offset`). The retained set is a
`HashSet<(timeline, global_seg_no)>` and the deletion filter
short-circuits on hit before issuing the storage call.

### E.1 — sentinel-not-in-per-backup-prefix asymmetry

Backup sentinels live at `basebackups_005/<name>_backup_stop_sentinel.json`
(one level above the per-backup directory), while metadata / tar
parts live inside `basebackups_005/<name>/`. wal-g made the same
choice. For `delete target` we walk both the bare-name objects and
the per-prefix objects, classifying each via
`strip_leftmost_backup_name`, which trims the `_backup*` sentinel
suffix to recover the canonical backup name from either layout.

### E.1 — dry-run by default

wal-g uses `--confirm` as an explicit gate before any storage
mutation. We mirror this: `delete` defaults to a logged plan; the
user must add `--confirm` to actually issue `storage.delete` calls.
The plan struct (`DeletePlan`) is returned from `handle` so test
code can assert on it without parsing logs.

### E.1 — `delete before FULL` is unsupported

wal-g explicitly rejects `before FULL …` (only `before` and
`before FIND_FULL` are valid for the `before` mode). We match that.
The CLI argument parser accepts the syntactic form `[FULL] <value>`
uniformly across modifier-extracting functions, but `plan_before`
rejects `FULL` with a clear error. Same for `delete target FULL`.

### E.1 — `retain --after` anchor selection

`retain N --after <ts|name>` keeps `(N newest) ∪ (every backup ≥ boundary)`.
We compute two candidate anchors — the Nth-newest backup (newest→oldest
walk, same as plain `retain`) and the first backup at-or-after the boundary
(oldest→newest walk, latching on name match for the name form) — then
delete-before-target the older of the two. If only one anchor resolves
we use it directly; if neither does we no-op. `FULL` / `FIND_FULL`
restricts the after-anchor to full backups (matches wal-g's
`FindTargetRetainAfter*`). The retain-walk's modifier semantics are
preserved by reusing `resolve_retain_target`.

### backup-mark — `--target-user-data` selector

`resolve_by_user_data` streams every sentinel, parses
`BackupSentinelDtoV2`, and compares `sentinel.UserData` to the JSON-parsed
flag value via `serde_json::Value`'s deep `PartialEq` (matches wal-g's
`reflect.DeepEqual`). Errors on no match or multiple distinct backup
names. Wired into `BackupMark`; the positional `name` is now optional,
mutually exclusive with `--target-user-data`. Same selector hook can be
extended to `delete target` later without restructuring.

### E.1 — chain semantics for `target`

`target <name>` deletes the named backup plus every descendant
delta that has it (or one of its descendants) somewhere in its
increment chain — BFS over the increment graph. `target FIND_FULL
<name>` deletes the chain root plus every backup sharing that root.
Permanent backups within the delete set abort the run (wal-g matches).

### E.2 — destination URI parsing

`Settings::build_dst_storage(uri)` reuses the source's credentials
and retry policy, swapping only the storage settings derived from
the URI. Supported schemes: `file://`, `s3://`, `gs://`, plus a bare
path falling back to `fs`. Cross-scheme is allowed; the stream-through
copy path (`get` → `put`) handles cross-backend without protocol
work. Same-credentials cross-bucket within S3 / GCS works because
the destination Storage instance reuses the source's
`access_key`/`secret_key`/`session_token`/`endpoint`/`force_path_style`.

### E.2 — WAL window inclusion

For single-backup copy: WAL segments within `[start_seg, finish_seg]`
on the backup's timeline are copied alongside the backup objects.
`--with-history` extends the window to include every WAL ≤ finish_lsn
on the timeline (so the destination can restore via PITR back from
the named backup). For `--all`: backup objects only (history off by
default; the user opts in per call). Matches wal-g `cmd/pg/copy.go`.

### E.2 — concurrency

The copy executor uses `tokio::JoinSet` bounded by
`Arc<Semaphore>(settings.upload_concurrency)`. Mirrors D.1's
upload pipeline shape so a future cross-region S3 copy can saturate
the link. Failures are logged per-key and the last error is
propagated up (full-batch failure surfaces rather than silent skips).

## Compatibility with wal-g

- Delete modes mirror wal-g `cmd/pg/delete.go` exactly: `before`,
  `retain`, `everything`, `target`, `garbage` with the same modifier
  vocabulary (`FULL`, `FIND_FULL`, `FORCE`, `ARCHIVES`, `BACKUPS`).
- Permanent backup + WAL-window semantics mirror
  `internal/databases/postgres/delete_util.go::GetPermanentBackupsAndWals`.
- Copy semantics mirror `internal/databases/postgres/copy.go`:
  single-backup default copies `[start_lsn, finish_lsn]` WAL window;
  `--with-history` opens to all WAL ≤ finish_lsn.

No on-bucket layout change in this phase, so the cross-tool gate
from Phase B remains valid. Bucket interop for delete is implicit:
both tools target the same key layout, both compute the same
`(timeline, seg_no)` ordering, both honor `IsPermanent=true`.

## What didn't get done (carry into next phase)

These are deferred but not blocking:

1. **VM live cross-tool retention test.** A
   `tests/vm/retention.rs` should: seed bucket with mixed FULL/DELTA
   + a permanent; run `wal-rs delete retain FULL 2 --confirm`;
   verify with installed wal-g `backup-list` that the same survivors
   remain. And reverse: wal-g writes & deletes, wal-rs lists the
   leftover. Inherits the matrix from Phase B's cross-tool gate.
2. ~~**`delete retain --after` variant.**~~ Landed. `--after <ts|name>`
   on `wal-rs delete retain` resolves an additional anchor and the
   `delete-before-target` core picks the older of the two anchors so
   the surviving set is `(N newest) ∪ (every backup ≥ boundary)`.
   Future-timestamp guard mirrors wal-g.
3. **Server-side copy.** S3 `x-amz-copy-source` and GCS
   `rewriteTo` would let cross-bucket / cross-prefix copy skip the
   stream-through. For multi-GB tar parts the latency win matters.
   Requires a `Storage::copy_within(src_key, dst_key)` extension
   with backend-specific overrides + same-backend detection at the
   copy planner. Stream-through is the correct fallback when keys
   straddle backends, so this is purely an optimization.
4. **Daemon protocol surface for delete/copy.** The Unix-socket
   daemon protocol from Phase D only carries Check / WalPush /
   WalFetch. Long-running batch ops (`delete everything` on a
   million-WAL bucket, `copy --all`) could profit from a daemon
   driver. Stay byte-compatible with wal-g's daemon as we extend.
5. **`--target-user-data` on `delete target` (and `backup-fetch`).**
   `backup-mark --target-user-data` is wired through
   `resolve_by_user_data` (`src/pg/backup/show.rs`). The same selector
   can be hooked into `delete target` and `backup-fetch` without new
   infrastructure — left for the next pass to keep the diff scoped to
   one user-facing surface at a time.
6. **Carryovers from Phase D**: VM live `wal-receive` exercise,
   async segment upload in wal-receive, prefetch-inside-fetch,
   concurrent backup-fetch, daemon-mode wal-prefetch/wal-show. Same
   status as at Phase D close.

## Test counts

- Local: **188 tests pass** (`cargo test --locked`). +30 from Phase D:
  - 16 delete unit tests (segno extraction, leftmost-name strip,
    ordering, retain/before resolver, chain walk, permanent set,
    retain-after time/name/full-modifier/future-guard)
  - 14 retention/copy integration tests (`tests/retention.rs`),
    including `retain --after` end-to-end and three
    `backup-mark --target-user-data` cases
- VM: unchanged from Phase D — Phase E's live-flow cross-tool test
  belongs to the next pass.
- CI matrix: unchanged from Phase B.2.

## Files touched

```
src/pg/backup/mod.rs           + copy & delete module decls; + strip_leftmost_backup_name
src/pg/backup/delete.rs        new — retention handler (before/retain/everything/target/garbage)
                               + resolve_retain_after_target / resolve_after_target
src/pg/backup/copy.rs          new — cross-prefix / cross-backend copy
src/pg/backup/show.rs          + resolve_by_user_data
src/config/mod.rs              + build_dst_storage + storage_from_uri
src/cli/mod.rs                 + Delete / Copy subcommands and routing;
                               + retain `--after`; backup-mark `--target-user-data`
tests/retention.rs             new — 14 integration tests against fs storage
```

## Sequencing for the next pass

1. Add **`tests/vm/retention.rs`** to gate cross-tool delete
   semantics (wal-rs delete, wal-g list — and the reverse).
2. Land the **Phase C/C.2 streamer integration** (still open from
   PHASED): D.1's concurrent upload pipeline + E's retention give
   the surrounding infrastructure room to handle the larger
   paged-file buffers cleanly.
3. Server-side copy optimization (E.2 #3 above) — purely an
   optimization gate, but the LATENCY win on cross-region S3 is
   large enough that real deployments will want it before
   Phase F (encryption) lands and adds yet another stream-through
   stage.
4. Phase F (libsodium + openpgp encryption). Both wired between
   compression and storage; encryption is the last remaining
   wal-g feature that touches on-bucket layout, so the cross-tool
   gate matters there in a way it doesn't for E.
