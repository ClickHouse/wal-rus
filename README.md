# wal-rs

Rust port of [wal-g](https://github.com/wal-g/wal-g) for PostgreSQL, tuned for
no-overcommit hosts (streaming I/O, no full-segment buffering).

## Storage backends

- `file://`
- `s3://`
- `gs://`

## Compression

`none`, `zstd`, `brotli`, `lz4`, `lzma`

## Commands

`wal-push`, `wal-fetch`, `wal-prefetch`, `wal-show`, `wal-verify`, `wal-restore`,
`wal-receive`, `backup-push`, `backup-fetch`, `backup-list`, `backup-show`,
`backup-mark`, `delete`, `copy`, `daemon`, `daemon-client`

Configuration follows wal-g env vars (`WALG_*`, `PG*`). See `wal-rs --help` per
subcommand.

## License

Apache 2.0, derivative work of wal-g. See [LICENSE](LICENSE).
