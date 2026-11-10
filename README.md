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

## Library use & cargo features

Everything is on by default, so the `walrus` binary is the same however it's
built. Library users can turn off `default-features` and enable only what they
need:

| Feature | Enables |
|---|---|
| `cli` | `walrus` binary, `daemon` / `daemon-client`, log init |
| `s3`, `gcs` | storage backends (`file://` is always available) |
| `zstd`, `lz4`, `brotli`, `lzma`, `gzip` | compression codecs |
| `libsodium` | `WALG_LIBSODIUM_KEY` encryption |
| `replication` | streaming replication protocol, `wal-receive` |
| `backup` | `backup-push` / `backup-fetch`, delta backups, WAL summaries (implies `replication`) |

WAL archive/restore and backup metadata commands (`backup-list`/`-show`/`-mark`,
`delete`, `copy`) need no feature. Every compression method name still parses
without its codec feature. Using a codec that wasn't built in, or configuring a
backend or key that wasn't built in, is an error, never a silent fallback.

```toml
walrus = { package = "wal-rus", version = "0.4", default-features = false, features = ["s3", "lz4"] }
```

To parse WAL records only, use
[`pg-walparser`](walparser) (`no_std` + `alloc`; the default `std` feature
adds the `std::io` adapters).

## Docs

- [docs/DESIGN.md](docs/DESIGN.md) — architecture & design decisions
- [docs/WALG_COMPAT.md](docs/WALG_COMPAT.md) — wal-g interop guarantees & divergences
