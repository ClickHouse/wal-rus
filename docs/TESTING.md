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
  `cargo clippy --all-targets --locked -- -D warnings`,
  `cargo test --locked`
- `.github/workflows/pg-compat.yml`: release binary driving
  `ci/*.sh` across lanes:
  - `pg`: PG 13-17 (jammy) + 18 (noble) × full backup + replay,
    backup-mark, backup-show, WAL overwrite, daemon, and cross-tool
    forward / reverse / encryption / retention / lzma against a pinned
    wal-g (`fs` backend)
  - `pg-storage`: s3 + gcs full-backup and copy against MinIO and
    fake-gcs-server emulators
  - `pg-tls-scram`: live TLS handshake, SCRAM-SHA-256 auth, and client
    certificate auth (mutual TLS via `PGSSLCERT`/`PGSSLKEY`) over TCP
  - `pg-codec`: brotli / lz4 / lzma / gzip push→fetch→replay
  - `pg-vm-test`: the `vm-test` live-PG suite (below)
  - `coverage`: `cargo llvm-cov` over the vm-test suite

  Clusters run as the runner user; failure logs upload as artifacts.

## Live-cluster tests (`vm-test` feature)

`tests/vm_live.rs` is `#[cfg(feature = "vm-test")]` gated and hits a
real PG cluster over its unix socket (trust auth, no TLS):

```
PGHOST=... PGPORT=... WALG_FILE_PREFIX=/tmp/x \
  cargo test --release --features vm-test
```

`ci/vm_test_cluster.sh` boots a throwaway replication-enabled
trust cluster, exports its `PG*` env, and runs the passed command
(`ci/vm_test_cluster.sh cargo test --features vm-test`); the
`pg-vm-test` and `coverage` lanes drive it in CI. Useful cluster spread
when pointing at a hand-built host: PG 13 through 18 covering both
BASE_BACKUP wire forms, at least one cluster with user tablespaces, one
reachable only via Unix socket + peer auth.

For s3/gcs without real buckets the `pg-storage` lane runs MinIO
(`AWS_ENDPOINT_URL=http://localhost:9000`,
`WALG_S3_FORCE_PATH_STYLE=true`) and `fsouza/fake-gcs-server`
(`WALG_GS_ENDPOINT` / `STORAGE_EMULATOR_HOST`, anonymous mode, JWT path
skipped); the same emulators work locally.
