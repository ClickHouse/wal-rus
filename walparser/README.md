PostgreSQL WAL (XLOG) record parser extracted from
[wal-rus](https://github.com/ClickHouse/wal-rus). A port of wal-g's
`internal/walparser`.

`no_std` + `alloc`. The default `std` feature adds the `std::io` adapters:
`WalParser::save` / `load`, `extract_locations_from_wal_file`, and the delta
location list readers/writers. The slice-based parsers and
`walk_segment_locations` need only `alloc`.

```toml
pg-walparser = { version = "0.1", default-features = false }
```
