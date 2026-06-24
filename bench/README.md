# bench — WAL-archiving benchmark harness

Reproducible single-host benchmark comparing three PostgreSQL 18 WAL archivers on
**throughput** and **memory** under heavy write load:

- **walrus** (this repo, Rust) — look-ahead fan-out daemon (`WALG_UPLOAD_CONCURRENCY`; pre-uploads `concurrency-1` segments, streaming per-upload, no full-segment buffer)
- **wal-g** (Go) — fan-out daemon (same `WALG_UPLOAD_CONCURRENCY`)
- **pgbackrest** (C) — daemonless; PG forks `archive-push`, async `process-max` workers

All three are driven identically: PG `archive_command` → the tool's own client → S3.
Memory/throughput are sampled on the **archiver** process (not Postgres) at 1 Hz.

`run_op.sh` extends the same harness to the rest of walrus's data paths — full
backup send/fetch, delta backups (archived-WAL and PG17 WAL-summary sourced), and
streaming WAL receive — benchmarked cross-tool where an equivalent exists (see
[Operation benchmarks](#operation-benchmarks)).

## Layout

```
config.env.example   copy to config.env; bucket, creds, PG role, sizing
setup.sh             bootstrap THIS host (install PG18, build tools, units)
run.sh               run ONE archive cell (daemon × run-id)
matrix.sh            loop pgbackrest/walg/walrus × repeats
run_op.sh            run ONE operation cell (op × tool × run-id)
op_matrix.sh         loop backup-send/backup-fetch/wal-receive × tools × repeats
scripts/lib.sh       shared driver scaffolding sourced by run.sh + run_op.sh
scripts/make_source_tarball.sh  git archive walrus src for a checkout-less SUT
scripts/sut/         per-host bootstrap steps (00..40, systemd units)
scripts/driver/      pgbench workload: schema, seed, burst (FPI storm)
tools/               Rust crate (standalone): bench-sampler + bench-analyze +
                     bench-compare bins; shared colorscheme/render lib (viz)
```

`bench-sampler` (1 Hz mem/CPU/WAL/backlog sampler; `--daemon walg|walrus|pgbackrest`
picks unit-MainPID vs proc-match), `bench-analyze` (aggregated time-series plots +
self-describing CSV/JSON exports), and `bench-compare` (grouped-bar cross-tool backup
comparison) are built from `tools/` by `setup.sh` and installed to `/usr/local/bin`.
All three draw through one `bench_tools::viz` module, so the dark palette (walrus
yellow, wal-g red, pgbackrest blue) is identical across every chart.

## Prerequisites

- **Debian or Ubuntu + systemd**, x86_64, and `sudo`. The scripts install PostgreSQL
  18 (PGDG apt, codename via `lsb_release -cs`), Go, Rust, `pgbackrest`, and build
  `wal-g` + `walrus` + `walg_archive`. Built/tested on Ubuntu 24.04 (the EC2 AMI) and
  Debian 13; any PGDG-supported release should work.
- An **S3 bucket** and credentials. `walrus` reads credentials from the **environment
  only** (no IMDS, no shared-config profiles), so `config.env` must carry explicit keys.
- The scripts build `walrus` from an **in-repo checkout** by default (`bench/` must
  sit inside the repo). For a fresh SUT with no checkout, package the source on the dev
  box with `bench/scripts/make_source_tarball.sh` (a `git archive`; the commit id rides
  in the tarball so provenance is preserved), get it onto the box, and set
  `WALRUS_SRC_TARBALL` in `config.env` — `03_build_walrus.sh` unpacks and builds from
  it. `terraform/` can do all of this for you (see [Provisioning with terraform](#provisioning-with-terraform)).
- Conventional paths the scripts assume: `PGDATA=/dat/18/data`, daemon env file
  `/etc/postgresql/wal-g.env`, daemon socket `/tmp/wal-g`, PG binaries under
  `/usr/lib/postgresql/18/bin`. A spare NVMe is mounted at `/dat` by
  `scripts/sut/00_mount_nvme.sh` (AWS instance-store oriented) — on other hosts set
  `SKIP_MOUNT=1` and provide `/dat` on a fast disk yourself.

## Provisioning with terraform

`terraform/` stands up the EC2 SUT (VPC, instance-store box, S3 bucket, IAM
instance profile, SSH key). It can also ship the source and bootstrap the box, so a
single `apply` goes from nothing to ready-to-bench:

```sh
t=$(bench/scripts/make_source_tarball.sh)        # git archive of HEAD
terraform -chdir=bench/terraform apply \
  -var my_ip="$(curl -s ifconfig.me)/32" \
  -var walrus_src_tarball="$t" \
  -var run_setup=true \
  -var pg_password='change-me'
```

- `walrus_src_tarball` (alone) uploads + unpacks the self-contained archive (whole repo,
  harness included) to `~/walrus`, keeping the tarball at `~/walrus-src.tar.gz`.
- `run_setup=true` then writes `~/walrus/bench/config.env` (from
  `config.env.tftpl`) and runs `setup.sh` over SSH — installs PG18, builds all three
  tools, starts the units. Needs `pg_password`. S3 creds come from the **instance
  profile via IMDS**, so no AWS keys are written. Re-runs when the source or a config
  knob changes (a password-only change won't retrigger — `terraform taint` to force).
- Omit both to get a bare box and deploy by hand. See `terraform/deploy.tf`.

Workload **sizing** (`SCALE`, `CHURN_*`, `*_SECONDS`) is templated at defaults; tune it
by editing `config.env` on the box (or `config.env.tftpl` before apply). After apply,
`terraform output next_steps` prints the SSH line and what to run.

## Run it

```sh
cd bench
cp config.env.example config.env      # fill BUCKET, AWS keys, PGUSER/PGPASSWORD, sizing

# 1. bootstrap this host (PG18 + build all three tools + systemd units)
sudo ./setup.sh                        # SKIP_MOUNT=1 sudo ./setup.sh  if /dat already exists

# 2. confirm the active daemon archives to S3
bash scripts/sut/40_smoke_test.sh

# 3. seed the bench DB once (shared across cells; large at full scale)
set -a; . ./config.env; set +a
PGHOST=127.0.0.1 ./scripts/driver/pgbench_init.sh

# 4. run one cell, or the whole matrix
./run.sh pgbackrest r1
./matrix.sh                            # pgbackrest, walg, walrus (once each)

# 5. plots + raw CSV/JSON exports (installed by setup.sh)
bench-analyze --run results/walrus-r1 --label walrus --out results/plots
```

`run.sh` and `matrix.sh` run as a normal user (they `sudo` for the root steps); do not
run pgbench as root. Results land under `results/<daemon>-<run_id>/` (gitignored).

## What `run.sh` does (the run contract)

1. **Select the daemon** — write `wal-g.env` with this cell's `UPLOAD_CONCURRENCY`
   (`11_write_walg_env.sh`), start its systemd unit, point `archive_command` at the
   tool's own client (`30_select_daemon.sh`), pre-drain leftover `.ready` backlog. For
   pgbackrest: set `process-max`, (re)create the stanza, set `archive-push`, drain.
2. **Normalize PG state** — force a checkpoint before the measured burst so
   full-page-image WAL is comparable across cells.
3. **Reset** `pg_stat_archiver`, start the 1 Hz sampler into the results dir.
4. **Drive the workload** — `run_workload.sh`: the high-WAL burst (FPI-heavy random
   UPDATEs on a wide indexed table + bulk COPY) — the heavy-load measurement.
5. **Capture** the final S3 inventory and `provenance.txt` (tool versions + binary
   SHA-256, harness git SHA, run parameters).

## Metrics (sampled at 1 Hz, written as CSV per run)

| File | Metric |
|---|---|
| `mem.csv` | archiver `VmRSS` (resident) and `VmPeak` (virtual — the no-overcommit metric) |
| `cpu.csv` | archiver CPU % |
| `wal.csv` | cumulative WAL generated vs archived |
| `archive.csv` | `pg_stat_archiver` counters + `.ready` backlog (does the archiver keep up?) |
| `net.csv` | tx bytes (upload rate) |

`bench-analyze` aggregates replicas of the same variant (median line + min..max band)
and also exports `samples_<stamp>.csv` / `summary_<stamp>.csv` / `summary.json` — every
row carries its run metadata, so the raw data is self-describing for external analysis.

## Operation benchmarks

`run_op.sh OP TOOL RUN_ID` extends the harness past the `archive_command` path to the
rest of walrus's data movement, single-host, reusing the same 1 Hz sampler — here
attached by `--proc-match` on the tool's process name, since these are one-shot CLI
runs, not daemons. `backup-fetch` and `wal-receive` run with the archive daemons
stopped, so the sample is the op process alone. The backup-push ops
(`backup-send`/`backup-delta`/`backup-delta-sidecar`/`backup-delta-summaries`) keep the
tool's own archive daemon live — a base backup's `pg_backup_stop` blocks on `BackupWaitWalArchive` until
its WAL is archived — so for those the sample is the op process plus the mostly-idle
daemon (~27 MB for walrus; wal-g's fan-out daemon adds more baseline).

| OP | walrus / wal-g | pgbackrest | measures |
|---|---|---|---|
| `backup-send` | `backup-push <PGDATA> --full` | `backup --type=full` | full base backup → S3 |
| `backup-fetch` | `backup-fetch <dst> LATEST` | `restore` | restore ← S3 |
| `backup-delta` | `backup-push` (delta, `wi1`) | `backup --type=incr` | delta backup → S3 (map from raw archived WAL) |
| `backup-delta-sidecar` | `backup-push` (delta, `wi1`), `WALG_USE_WAL_DELTA=1` on the daemon | — (no peer) | delta backup → S3 (map folds `<group>_delta` sidecars) |
| `backup-delta-summaries` | `backup-push --delta-from-wal-summaries` | — (walrus-only) | delta from PG17 WAL summaries → S3 |
| `wal-receive` | `wal-receive <dir>` | — (no equivalent) | stream WAL from PG |

walrus's walsender (serving WAL over the replication protocol) has no CLI entry point
yet, so `wal-send` is intentionally absent.

The three delta cells exercise walrus's incremental backup; they differ only in the
**changed-block map source** (the `backup-push` command and `wi1` wire format are
identical, so all stay cross-tool):

- `backup-delta` — walks **archived raw WAL**, reparsed in full. The cold worst case
  (no sidecars).
- `backup-delta-sidecar` — same source, but `WALG_USE_WAL_DELTA=1` is written to the
  daemon env file, so the archiver records `<group>_delta` sidecars during the churn;
  the push then folds whole 16-segment groups and reparses only the trailing partial
  group. A walrus/wal-g daemon feature (both honor the var); pgbackrest has no peer.
- `backup-delta-summaries` — sources the map from `$PGDATA/pg_wal/summaries` (PG17
  `summarize_wal=on`, enabled by `10_init_pg.sh`); wal-g and pgbackrest have no
  WAL-summary delta, so it is walrus-only.

All three first force a checkpoint, then drive a `DELTA_CHURN_SECONDS` burst — with the
archiver live so the churn WAL is in the repo — drain, keep the archiver live, then
time the delta push. They need a parent full, so `backup-send` must precede them.
`DELTA_ORIGIN=LATEST_FULL` keeps each delta cell anchored to the chain root by default;
`DELTA_MAX_STEPS` still caps chain depth.

```sh
# one cell (assumes setup.sh ran; non-fetch ops need the seeded DB)
./run_op.sh backup-send walrus r1
./run_op.sh backup-fetch walrus r1            # fetches LATEST; run a backup-send first
./run_op.sh backup-delta walrus r1            # churn → delta push; needs a parent full
./run_op.sh backup-delta-sidecar walrus r1    # WALG_USE_WAL_DELTA=1 archiver; folds sidecars
./run_op.sh backup-delta-summaries walrus r1  # walrus-only; needs summarize_wal=on
./run_op.sh wal-receive  walrus r1            # streams for WAL_RECEIVE_SECONDS

# whole sweep: send → fetch → delta → delta-sidecar → delta-summaries → wal-receive
./op_matrix.sh
```

Each cell writes the sampler CSVs plus `op_metrics.txt`:

| Field | Meaning |
|---|---|
| `elapsed_s` | wall-clock of the operation |
| `bytes_processed` | backup-send: on-disk cluster size (excl. `pg_wal`); backup-fetch: restored bytes; backup-delta / -sidecar / -summaries: S3-inventory byte growth across the push (the delta's stored size); wal-receive: S3-inventory byte growth while receiver drains |
| `throughput_mb_s` | `bytes_processed / elapsed_s / 1e6` |
| `checkpoint_before_workload` | `1` when cell forced a checkpoint before FPI-sensitive work (backup-send, delta churn, wal-receive); else `0` |
| `delta_origin` | delta parent policy passed as `WALG_DELTA_ORIGIN` for walrus / wal-g delta cells |

Notes:
- `backup-fetch` fetches `LATEST` from the tool's repo. `run_op.sh` scopes walrus /
  wal-g and pgbackrest prefixes by tool and run ID, so `LATEST` and implicit delta
  parents cannot come from another tool or a prior sweep. Op order runs
  `backup-fetch` before the delta cells, so it restores a clean full.
- `backup-send`/`backup-fetch` use `RESTORE_DIR`/`WAL_RECV_DIR` (wiped per run); keep
  them on the fast disk. `wal-receive` and the delta cells drive the burst workload as
  their WAL source (`WAL_RECEIVE_SECONDS` / `DELTA_CHURN_SECONDS`).
- pgbackrest `backup` (full or `incr`) needs live archiving, so those cells point
  `archive_command` at pgbackrest and drain first, as the archive bench does.

### Comparing ops across tools (`bench-compare`)

`bench-compare` renders one grouped-bar chart from the per-op result dirs — bars
grouped by op, one bar per tool/variant (same colors as `bench-analyze`). It pulls
size + elapsed from `op_metrics.txt`, peak CPU from `cpu.csv`, and peak RSS (`VmHWM`)
from `mem.csv`; duration is log-scaled (op timings span ~20 s to ~500 s). Replicas of a
variant on one op are aggregated to the median; absent cells (e.g. walrus-only
`delta-summaries`) just leave a gap.

```sh
# one --run/--label per result dir; variant = label sans -b<N>
bench-compare \
  --run results/backup-send-walrus-v020      --label walrus \
  --run results/backup-send-walg-v020        --label walg \
  --run results/backup-send-pgbackrest-v020  --label pgbackrest \
  --run results/backup-delta-walrus-v020     --label walrus \
  --run results/backup-delta-sidecar-walrus-v020 --label walrus \
  ... \
  --out results/plots --stamp v020
```

Outputs `backup_compare.png` (size / duration / CPU / memory panels), `ops_summary.md`,
and `ops_compare_<stamp>.csv`. Restrict/reorder the x groups with `--ops`
(comma-separated op names; default is the backup family, `wal-receive` excluded).

## Config knobs

See `config.env.example`. Common ones: `UPLOAD_CONCURRENCY` (wal-g concurrency /
pgbackrest `process-max`; also seeds `WALG_DOWNLOAD_CONCURRENCY` so `backup-fetch`
scales with the same knob — set `DOWNLOAD_CONCURRENCY` to decouple), `SCALE`
(pgbench DB size), `CHURN_ROWS`, `BURST_SECONDS`,
`BURST_WORKERS`. `matrix.sh` honors `DAEMONS` (and `RUN_ID`). Operation benchmarks add
`RESTORE_DIR`, `WAL_RECV_DIR`, `WAL_RECEIVE_SECONDS`, `DELTA_CHURN_SECONDS`,
`DELTA_MAX_STEPS`, `DELTA_ORIGIN`; `op_matrix.sh` honors `OPS`, `TOOLS` (and
`RUN_ID`).
