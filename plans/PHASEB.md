# Phase B — backup format parity

Implemented B.1–B.6 from PLAN.md (originally B.7–B.12; phases now
use per-letter numbering, see PLAN.md "Sequencing"). 85 local tests +
3-test vm_live suite
green on PG 13/14/15/16/17/18. Phase A's PG 18 environmental block
resolved by adding Unix-socket transport to `ReplicationConn` (see
"PG 18 unblock" below). Bidirectional cross-tool compat with upstream
wal-g (`/tmp/wal-g`) verified end-to-end on PG 16.

## What landed

| Item | Files | Tests added |
|---|---|---|
| B.1+B.2 tarball streamer (port of wal-g `TarballStreamer`) with path remap + size-based part rotation | `src/pg/backup/tar_streamer.rs` (new) | `tar_streamer::tests` x5 |
| B.1 multi-tablespace push + fetch (`TablespaceSpec` in sentinel, `pg_tblspc/<oid>/` remap, symlink restore, `--tablespace-mapping`) | `src/pg/backup/mod.rs` (+TablespaceSpec + custom serde), `src/pg/backup/push.rs`, `src/pg/backup/fetch.rs` (manual extraction past tar-crate symlink guard, `FetchArgs::tablespace_mappings`) | `tablespace_spec_roundtrips`, `fetch_recreates_tablespace_symlinks`, `vm_live::backup_with_user_tablespace_against_live_pg` |
| B.3 per-file metadata + `files_metadata.json` sidecar (`FilesMetadataDisabled=false`) | `src/pg/backup/mod.rs` (FilesMetadataDto, FileDescription), wired in push.rs | covered by vm_live + `passthrough_single_part`/`applies_prefix_remap` |
| B.4 `backup-show` (plain/JSON) | `src/pg/backup/show.rs` (new), `src/cli/mod.rs` | `show_round_trip_and_mark_flips_permanent` |
| B.5 `backup-mark --permanent` / `--impermanent` | same file | same test |
| B.6 surface `compressed_size` from put-side counting reader | push.rs (per-part Arc<AtomicU64>, sum across parts + pg_control tee) | vm_live asserts `0 < compressed_size <= uncompressed_size` |
| `--tar-size-threshold` CLI flag wired to `WALG_TAR_SIZE_THRESHOLD` env | cli/mod.rs, push.rs `PushArgs` | covered by `rotates_parts_at_threshold` / `oversize_entry_gets_own_part` |
| Cross-tool test script | `scripts/vm-cross-tool.sh` (new) | manual run; gates Phase B compat claim |

## Design decisions worth recording

### Streamer architecture (`tar_streamer.rs`)

Single `spawn_blocking` task per archive that:

1. Bridges async input → sync `tar::Archive` via `tokio_util::io::SyncIoBridge`.
2. Iterates entries, optionally remaps the header path prefix
   (`pg_tblspc/<oid>/...` for non-default tablespaces), collects
   `(name, mtime)` into a `HashMap<String, FileMeta>` keyed by the
   post-remap name.
3. Pre-emptively rotates the output part when the next entry would push
   the running byte count over `WALG_TAR_SIZE_THRESHOLD` (default 1
   GiB); a single oversize entry gets its own part (matches wal-g).
4. Optionally tees named entries (`global/pg_control` for the data dir
   archive) into an in-memory secondary tar that gets uploaded as
   `pg_control.tar.<ext>` after the loop.

The blocking writer side (`BlockingSender`) coalesces small tar block
writes into 256 KiB chunks before pushing through a tokio mpsc as
`Bytes`. The consumer reads via the existing `ChannelReader`
(`AsyncRead`) → compression encoder → counting reader → storage.put.
Backpressure flows through `blocking_send` parking the writer thread.

Decided against pulling the `regex` crate for path remaps; tablespace
prefixing is just `format!("pg_tblspc/{oid}/{path}")` and wal-g's own
remap regex `^` → prefix degenerates to the same operation.

### `TablespaceSpec` JSON shape

Mirrors wal-g exactly so wal-g can deserialize sentinels we write:

```json
{
  "base_prefix": "/var/lib/pg/16/main",
  "tablespaces": ["16384", "16385"],
  "16384": {"loc": "/srv/ts_a", "link": "pg_tblspc/16384"},
  "16385": {"loc": "/srv/ts_b", "link": "pg_tblspc/16385"}
}
```

The map-keyed-by-name shape doesn't map cleanly to `#[derive(Serialize)]`
on a struct, so the type carries hand-written `Serialize` /
`Deserialize` impls that flatten `locations: HashMap<String, _>` next to
`base_prefix` and `tablespaces`. Tested with `tablespace_spec_roundtrips`.

### Manual tar extraction in `backup-fetch`

The `tar` crate's `Archive::unpack` canonicalizes each entry's parent
and refuses extraction when the result steps outside `dst`. PG restores
legitimately need to write through `pg_tblspc/<oid>` symlinks pointing
to e.g. `/srv/ts_a`, which is outside the data dir on purpose. Replaced
the call with a manual entry walk (`unpack_manual`) that handles
directories, regular files, and symlinks without the path-canonicalize
check. Parent-dir traversal (`..`) is still skipped.

Path validation we retain: skip `Prefix`/`RootDir`/`CurDir` components,
drop `ParentDir` components, only restore file/dir/symlink entry types
(no fifos, devices, hard links — PG's BASE_BACKUP never emits any).

### Symlink restore ordering

Symlinks created BEFORE part extraction starts. Otherwise the first tar
entry under `pg_tblspc/<oid>/` would materialize a real directory at
that path and subsequent extractions would write into the local data
dir instead of the tablespace target. wal-g handles this in
`EnsureSymlinkExist`; we do the same in `restore_tablespace_symlinks`.

Existing symlink at the target is replaced (the typical case after a
re-restore over the same dir). A non-symlink at the target is a hard
error since silently overwriting a real directory would lose data.

### `pg_control` tee

Data dir archive entries get teed when their post-remap name is
`global/pg_control`. The tee buffer is held in memory (8 KB cap in
practice) and uploaded as `pg_control.tar.<ext>` after the main parts.
`backup-fetch` already sorts `pg_control` parts last via the existing
string-contains check, so an interrupted restore can't leave a stale
pg_control behind. Compressed size of the tee tar is included in the
sentinel's CompressedSize.

### Compressed size surfacing (B.6)

Per-part flow:

```
streamer Part.reader → compression::encode → CountingReader<Arc<AtomicU64>> → throttle → storage.put
```

The counter sits BETWEEN compression and storage.put so it captures
on-wire bytes regardless of backend. Summed across all parts including
the pg_control tee. Lives next to (not inside) the storage trait so
backends stay stream-agnostic.

### Settled-in places that needed updating

- `PushArgs` gained `tar_size_threshold: u64` (0 ⇒ default 1 GiB).
  `WALG_TAR_SIZE_THRESHOLD` env propagates via `clap`'s `env` attr.
- `BackupSentinelDto` gained `tablespace_spec: Option<TablespaceSpec>`
  serialized under `"Spec"` (matches wal-g's `BackupSentinelDto.Spec`).
- `FilesMetadataDisabled` flipped from hard-coded `true` to `false`
  since we now collect per-file metadata for every basebackup.
- `backup-fetch` got a `FetchArgs { tablespace_mappings }` so callers
  can redirect tablespace targets at restore time (mirrors wal-g's
  `--restore-tablespace-mapping`). The existing single-arg `handle()`
  delegates with an empty mapping list.

## Cross-tool compatibility (Phase B's North-star)

The acceptance bar for Phase B is "a bucket written by either tool is
operable by the other" for backup list/fetch/show. Verified manually
via `scripts/vm-cross-tool.sh` on PG 16:

**wal-rs → wal-g** (forward):

```
wal-rs backup-push  →  basebackups_005/base_000000010000000000000025/
  ├── _backup_stop_sentinel.json   (Spec: empty, FilesMetadataDisabled: false)
  ├── metadata.json
  ├── files_metadata.json          (1900 entries, 2 tar parts)
  └── tar_partitions/
      ├── part_001.tar.zst         (3.0 MB compressed / 48.6 MB raw)
      └── pg_control.tar.zst       (small tee)
wal-g backup-list                  → lists it
wal-g backup-fetch                 → extracts cleanly, PG_VERSION recovered
```

**wal-g → wal-rs** (reverse):

```
wal-g backup-push                  → 3 parts (data, pg_control, backup_label)
wal-rs backup-list                 → lists it
wal-rs backup-fetch                → extracts cleanly, PG_VERSION recovered
wal-rs backup-show                 → renders sentinel + files_metadata summary
```

The shared format wins:

- Same `basebackups_005/` key layout & `part_NNN.tar.<ext>` naming.
- Same sentinel JSON shape (PascalCase keys, `Spec` for tablespaces,
  `FilesMetadataDisabled` boolean).
- Same `files_metadata.json` schema (`Files`, `TarFileSets`).
- Same `pg_control.tar.<ext>` tee convention.

## Real issues found during integration

1. **`tar` crate refuses symlink-piercing extraction.** Hit by the
   first multi-tablespace test fixture (`fetch_recreates_tablespace_symlinks`).
   The crate's `Archive::unpack` canonicalizes the parent of each entry
   to guard against CVE-2001-1267 style attacks; legitimate PG restores
   break under that guard. Fix: hand-rolled `unpack_manual` that
   preserves the `..`-traversal block but drops the canonicalize step.

2. **PartCtx counter ergonomics.** First draft had three `impl` blocks
   for `PartCtx` because I forgot to consolidate the `bytes_written`
   getter after a clippy lint nudge. Collapsed; counter is now an
   `Arc<AtomicU64>` accessed only through `bytes_written()`.

3. **`tee_buf` move-out vs `tee_builder` borrow.** Initial plan held
   `Option<Vec<u8>>` next to `Option<tar::Builder<&mut Vec<u8>>>`,
   which borrows the Vec mutably through the builder's lifetime and
   blocks the final move-out. Switched to `tar::Builder<Vec<u8>>` so
   the builder owns the Vec; `into_inner()` hands it back at the end.

4. **VM PG14/PG15 default-fetch test crashed on tablespace permission
   denied.** Tests originally called `backup::fetch::handle(... )`
   with no remap. PG 14/15 on the VM carry user tablespaces under
   `/var/lib/postgresql/<v>/...` which are postgres-owned. Resolved by
   making the default-fetch test read the sentinel first and
   auto-remap each tablespace's location into the test temp dir
   (mirroring how a real restore-to-new-host would use
   `--tablespace-mapping`).

## VM environment notes

| Cluster | Port | Result |
|---|---|---|
| PG 13 | 5423 | 3/3 pass (no user tablespaces, basic path) |
| PG 14 | 5434 | 3/3 pass (2 user tablespaces: 35239, 35240) |
| PG 15 | 5435 | 3/3 pass (3 user tablespaces: 35171–35173) |
| PG 16 | 5436 | 3/3 pass (no user tablespaces) + cross-tool roundtrip |
| PG 17 | 5437 | 3/3 pass (no user tablespaces) |
| PG 18 | 5433 | 3/3 pass via Unix socket + peer auth |

PG 14/15 were "deferred to Phase B" at the end of Phase A — they now
pass.

### PG 18 unblock (Unix-socket transport)

PG 18's pg_hba is `scram-sha-256` on TCP, `peer` on local socket;
postgres-superuser password is unset & shared-infra-managed. Rather
than touch pg_hba or credentials, `ReplicationConn::connect` now
detects `PGHOST` values starting with `/` and dials via `UnixStream`
to `<host>/.s.PGSQL.<port>` (libpq convention). TLS negotiation is
skipped on the Unix path, matching libpq's `sslmode=disable`
treatment for Unix sockets.

`vm-deploy.sh` was updated to special-case port 5433: it locates the
prebuilt `vm_live-*` test binary, copies it to `/tmp` 0755, and execs
under `sudo -u postgres` with `PGHOST=/var/run/postgresql`. Test
binary is invoked directly (instead of `cargo test`) because cargo
under sudo would scribble into the postgres user's `$HOME`.

Trade-off: the SCRAM client code (`do_sasl` in `conn.rs`) is now
covered only by the existing protocol-mock unit tests, not by a live
PG server. Acceptable given the alternative (modifying shared infra
credentials).

`scripts/vm-cross-tool.sh` runs the end-to-end roundtrip against any
cluster:

```
scripts/vm-cross-tool.sh                   # PG 16 default
PGPORT=5435 scripts/vm-cross-tool.sh       # PG 15
```

It depends on `/tmp/wal-g` (already installed on the VM, 92 MB statically
linked aarch64 binary). Trust-auth required for the wal-g half too.

## What didn't get done

- **Sparse-file / pax / GNU long-name fixtures.** PLAN.md flagged this
  as a risk for the streamer port. The 2 KB of regression coverage in
  `tar_streamer::tests` only exercises GNU-format headers under 100
  chars. PG's basebackups in the VM range have file names well under
  100 chars, so we haven't tripped the pax-header path in practice.
  When delta backups (Phase C) start producing wider headers we'll
  need fixtures captured from a PG instance with `CREATE TABLE` names
  > 100 chars.
- **`pg_control.tar` size budget against the threshold.** The current
  code uploads the tee tar unconditionally after the main parts; it
  doesn't count against `max_tar_size`. In practice pg_control is 8
  KB, so this is fine. Worth documenting since wal-g splits parts
  more aggressively when oversized files appear.
- **Live SCRAM-SHA-256 server-side exercise.** PG 18 testing now runs
  through the Unix-socket peer-auth path, so the SCRAM client code is
  covered only by unit tests + protocol mocks. Adding a `scram-sha-256`
  entry to one cluster's pg_hba (with a known password) would close
  this gap; out of scope without shared-infra authorization.
- **CRC32 sentinel-content compare on overwrite.** Phase A added
  content-compare for `.history` / `.partial` WAL files, but the
  `WALG_PREVENT_WAL_OVERWRITE` check still does a byte-stream compare
  rather than CRC. For large tar parts this is irrelevant (we never
  re-upload over an existing part), but a CRC32 sidecar would let us
  cheap-validate the part bodies in the future.

## Test counts

- Local: 85 tests pass (`cargo test`). +16 since Phase A start
  (5 tar_streamer + 1 tablespace_spec + 2 backup_roundtrip + 1
  vm_live local-feature-gated + 1 conn Unix-socket dispatch, plus 6
  pre-existing).
- VM: 18 tests pass across PG 13/14/15/16/17/18 (3 tests × 6 clusters).
- Cross-tool: forward (wal-rs → wal-g) and reverse (wal-g → wal-rs)
  basebackup round-trip pass on PG 16. Run via `scripts/vm-cross-tool.sh`.

## Files touched

```
Cargo.toml                                 unchanged (streamer reuses tar already in tree)
src/pg/backup/mod.rs                       + TablespaceSpec (custom serde) + FilesMetadataDto + FileDescription; sentinel got tablespace_spec field
src/pg/backup/tar_streamer.rs              new — streamer, blocking writer, file-meta collector
src/pg/backup/push.rs                      rewritten — drives streamer, sums compressed_size, emits files_metadata, optional tee pg_control
src/pg/backup/fetch.rs                     manual extraction path, FetchArgs::tablespace_mappings, symlink restore
src/pg/backup/show.rs                      new — backup-show / backup-mark
src/cli/mod.rs                             + BackupShow / BackupMark commands; BackupPush --tar-size-threshold flag
src/daemon/mod.rs                          clippy: drop redundant Ok+?
src/storage/gcs.rs                         clippy: drop needless borrows
src/pg/replication/tls.rs                  clippy: rename from_str → parse (avoid trait ambiguity)
src/pg/replication/base_backup.rs          remove unused AsyncReadExt import
src/pg/replication/conn.rs                 + Unix-socket transport when PGHOST starts with `/` (unblocks PG 18 testing)
scripts/vm-deploy.sh                       + PG 18 branch: prebuilt-binary + sudo -u postgres + Unix-socket PGHOST
tests/backup_roundtrip.rs                  + tablespace symlink restore + backup-show/mark tests
tests/vm_live.rs                           + files_metadata + compressed_size + tablespace-mapping assertions; new multi-tablespace test
scripts/vm-cross-tool.sh                   new — bidirectional wal-rs↔wal-g roundtrip
```
