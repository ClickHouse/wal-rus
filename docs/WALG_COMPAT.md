# wal-g compatibility

Bidirectional bucket interop is the project's acceptance bar: a bucket
written by wal-g v3.x is listable / fetchable / replayable / verifiable
/ deletable by wal-rs, and the reverse. CI gates the claim
(`scripts/ci/cross_tool_{forward,reverse}.sh` against a pinned wal-g
version in `.github/workflows/pg-compat.yml`; bumping the pin is a
deliberate one-line change so an interop-breaking wal-g release fails
the bump PR, not master).

## Shared on-bucket format

- Key layout version `005`: `wal_005/<segment>[.<ext>]`,
  `basebackups_005/<name>/tar_partitions/part_NNN.tar.<ext>`,
  `pg_control.tar.<ext>` tee, sentinel at
  `basebackups_005/<name>_backup_stop_sentinel.json` (one level above
  the per-backup dir, same asymmetry as wal-g)
- Sentinel mirrors `BackupSentinelDtoV2` field-for-field, PascalCase
  keys, `Spec` for tablespaces; every Option field tolerant-deserializes
  so sentinels from either tool parse
- `files_metadata.json` schema (`Files`, `TarFileSets`)
- Delta naming `base_<24hex>_D_<parent_24hex>`; chain discovered via
  sentinel `IncrementFrom`, format detected per-file by magic byte, no
  sentinel format flag (wal-g convention)
- `wi1` increment format and PG17 native INCREMENTAL format
  (magic `0xd3ae1f0d`); native layout verified against postgres source
  (`src/common/blkreftable.c`, `src/backend/backup/basebackup.c`,
  `src/bin/pg_combinebackup/reconstruct.c`): header order
  magic / num_blocks / truncation_block_length / blocks / pad-to-BLCKSZ
  when num_blocks > 0, CRC32C-Castagnoli with trailing CRC bytes
  excluded from the running CRC
- libsodium framing: 24-byte secretstream header, 8 KiB plaintext
  chunks, 17-byte per-chunk overhead, explicit FINAL chunk on close;
  a wire-format pin test fails on any drift
- Prefetch dir layout `pg_wal/.wal-g/prefetch/{running/,}` so a sidecar
  can run either tool against the same pg_wal
- Daemon Unix-socket protocol byte format (Check / WalPush / WalFetch)
- `delete` mode + modifier vocabulary (`before` / `retain` /
  `everything` / `target` / `garbage`; `FULL`, `FIND_FULL`, `FORCE`,
  `ARCHIVES`, `BACKUPS`, `--after`), permanent-backup WAL reservation,
  `--confirm` gate
- Env vars follow `WALG_*` / `PG*` / `AWS_*` / `GOOGLE_*` naming

## Deliberate divergences

| Area | wal-rs behavior |
|---|---|
| OpenPGP (`WALG_PGP_*`) | hard error at startup, never silently plaintext; re-encrypt to libsodium when migrating (see DESIGN.md for rationale) |
| Backends | `file://`, `s3://`, `gs://` only; no azure / oss / swift / sh |
| statsd (`WALG_STATSD_*`) | not implemented |
| catchup-push / -fetch / -send / -receive / -list | not implemented |
| Windows | non-goal |
| Other databases (mongo, mysql, redis, …) | non-goal, Postgres only |

## Caveats

- Mixed plaintext/ciphertext buckets unsupported, same as wal-g; keep
  the key configured consistently per prefix
- A delta chain should not mix `wi1` and native formats; the selector
  does not yet refuse `--delta-from-wal-summaries` against a wi1-parent
  chain
- A file deleted on the source between parent and delta backup gets no
  tombstone in metadata, restore leaves the parent's copy; matches
  wal-g
- lzma: async-compression emits LZMA1-alone via xz2, wal-g uses
  ulikunitz/xz/lzma LZMA1-alone; believed identical, not yet
  cross-validated against a wal-g-written bucket
