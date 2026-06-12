# Testing

Three layers, each catching what the previous can't.

## Unit + hermetic integration

`cargo test --locked`. Inline `#[cfg(test)]` modules cover binary
formats (walparser, increment files, wal summaries, libsodium framing,
SigV4 vectors) and pure logic (segment arithmetic, retention planning,
retry/throttle). `tests/*.rs` run full pipelines against the `fs`
backend in tempdirs: WAL push/fetch roundtrips, backup
list/fetch/delta-chain replay, retention/copy, daemon socket, walsender
protocol over loopback (`walsender_vs_libpq.rs` additionally drives a
real `psql` when present, skipped otherwise).

Mock-server tests in `pg/replication/base_backup.rs` script PG14- and
PG15+ BASE_BACKUP wire responses; several encode real-server quirks
(empty CopyData frames, tablespace row ordering) found only against
live clusters, treat them as load-bearing regressions.

## CI

- `.github/workflows/ci.yml`: `cargo fmt --check`,
  `cargo clippy --all-targets -- -D warnings`, `cargo test --locked`
- `.github/workflows/pg-compat.yml`: release binary × PG version matrix
  × `scripts/ci/*.sh` (full backup + replay, backup-mark, backup-show,
  WAL overwrite semantics, daemon, cross-tool forward/reverse against a
  pinned wal-g). `fs` backend, cluster runs as the runner user,
  failure logs uploaded as artifacts

## Live-cluster tests (`vm-test` feature)

`tests/vm_live.rs` is `#[cfg(feature = "vm-test")]` gated and hits a
real PG cluster:

```
PGHOST=... PGPORT=... WALG_FILE_PREFIX=/tmp/x \
  cargo test --release --features vm-test
```

`scripts/vm-deploy.sh` wraps rsync + remote build + per-cluster test
runs against a test host (`VM_HOST` / `VM_KEY` / `VM_DEST` env);
`scripts/vm-cross-tool.sh` runs the bidirectional wal-g roundtrip
there. Useful cluster spread: PG 13 through 18 covering both
BASE_BACKUP wire forms, at least one cluster with user tablespaces, one
reachable only via Unix socket + peer auth.

For s3/gcs without real buckets: MinIO
(`AWS_ENDPOINT=http://localhost:9000`, `AWS_S3_FORCE_PATH_STYLE=true`)
and `fsouza/fake-gcs-server` (`STORAGE_EMULATOR_HOST`, anonymous mode,
JWT path skipped).
