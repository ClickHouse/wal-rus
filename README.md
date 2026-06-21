Rust port of [wal-g](https://github.com/wal-g/wal-g) for PostgreSQL, tuned for
no-overcommit hosts (streaming I/O, no full-segment buffering).

## Storage backends

- `file://`
- `s3://`
- `gs://`

## Compression

`none`, `zstd`, `brotli`, `lz4`, `lzma`, `gzip`

## Commands

`wal-push`, `wal-fetch`, `wal-prefetch`, `wal-show`, `wal-verify`, `wal-restore`,
`wal-receive`, `backup-push`, `backup-fetch`, `backup-list`, `backup-show`,
`backup-mark`, `delete`, `copy`, `daemon`, `daemon-client`

Configuration follows wal-g env vars (`WALG_*`, `PG*`). See `walrus --help` per
subcommand.

## Docs

- [docs/DESIGN.md](docs/DESIGN.md) — architecture & design decisions
- [docs/WALG_COMPAT.md](docs/WALG_COMPAT.md) — wal-g interop guarantees & divergences
