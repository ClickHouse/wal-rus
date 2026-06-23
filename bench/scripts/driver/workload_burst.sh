#!/usr/bin/env bash
#
# workload_burst.sh
#
# High-WAL burst load, driven over the network at the SUT's PostgreSQL.
# Goal: generate WAL >= ~2x the single-daemon archive drain rate so the
# pg_wal *.ready backlog climbs into the hundreds/thousands and we can measure
# how each archiver daemon keeps up (or falls behind).
#
# Strategy:
#   * UPDATE storm  -- random-row UPDATEs on wal_churn that mutate *indexed*
#     columns (k1,k2,k3,tag,updated_at). After each checkpoint, the first touch
#     of every heap + index page emits a full-page image (FPI), so random
#     scatter across a wide, 5-B-tree table maximizes WAL bytes per row.
#   * COPY storm    -- a fraction of workers run large COPY batches into the
#     unindexed wal_bulk table for bursty heap-insert WAL on top of the UPDATEs.
#
# Both are expressed as pgbench custom scripts run by N concurrent workers
# (one pgbench process per worker so COPY \. blocks behave) for DURATION.
#
# Env vars (with defaults):
#   PGHOST      (required) SUT private IP / host
#   PGPORT      5432
#   PGUSER      (required) login role
#   PGPASSWORD  (required) password (or ~/.pgpass)
#   PGDATABASE  walbench
#   WORKERS         <nproc>   number of concurrent burst workers (default vCPUs)
#   COPY_WORKERS    <max(1, WORKERS/4)>  how many of WORKERS run COPY bursts
#   DURATION        600       total burst duration in seconds
#   CHURN_ROWS      2000000   id range to scatter random UPDATEs over
#                             (must match pgbench_init.sh CHURN_ROWS)
#   UPDATE_BATCH    25        UPDATEs per pgbench transaction (update workers)
#   COPY_ROWS       50000     rows per COPY batch (copy workers)
#   PROTOCOL        prepared  pgbench query protocol for the UPDATE workers

set -euo pipefail

PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-walbench}"
DURATION="${DURATION:-600}"
CHURN_ROWS="${CHURN_ROWS:-2000000}"
UPDATE_BATCH="${UPDATE_BATCH:-25}"
COPY_ROWS="${COPY_ROWS:-50000}"
COPY_BLOB_REPEAT="${COPY_BLOB_REPEAT:-8}"   # md5 repeats per blob; raises WAL bytes/row at ~same CPU
PROTOCOL="${PROTOCOL:-prepared}"

host_cpus="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)"
WORKERS="${WORKERS:-${host_cpus}}"
COPY_WORKERS="${COPY_WORKERS:-$(( WORKERS / 4 > 0 ? WORKERS / 4 : 1 ))}"

: "${PGHOST:?Set PGHOST to the SUT private IP}"
: "${PGUSER:?Set PGUSER to the login role}"

if (( COPY_WORKERS > WORKERS )); then
    echo "FATAL: COPY_WORKERS (${COPY_WORKERS}) > WORKERS (${WORKERS})" >&2
    exit 1
fi
UPDATE_WORKERS=$(( WORKERS - COPY_WORKERS ))

export PGPORT

echo "==> Burst load: ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}"
echo "==> workers=${WORKERS} (update=${UPDATE_WORKERS}, copy=${COPY_WORKERS}) duration=${DURATION}s"
echo "==> update_batch=${UPDATE_BATCH} copy_rows=${COPY_ROWS} churn_rows=${CHURN_ROWS}"

# --- temp pgbench script files ---------------------------------------------
WORKDIR="$(mktemp -d)"
cleanup() {
    # Kill any still-running worker pgbench processes, then remove temp scripts.
    if [[ -n "${WORKER_PIDS:-}" ]]; then
        # shellcheck disable=SC2086
        kill ${WORKER_PIDS} 2>/dev/null || true
    fi
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT INT TERM

UPDATE_SQL="${WORKDIR}/update.sql"
COPY_SQL="${WORKDIR}/copy.sql"

# UPDATE worker script: one random id per :rid, repeated UPDATE_BATCH times in a
# single transaction. Every mutated column is indexed so each row dirties the
# heap page plus several index pages -> heavy FPI WAL.
{
    echo "\\set rmax ${CHURN_ROWS}"
    echo "BEGIN;"
    for _ in $(seq 1 "${UPDATE_BATCH}"); do
        cat <<'SQL'
\set rid random(1, :rmax)
UPDATE wal_churn
   SET k1 = (random() * 1e9)::bigint,
       k2 = (random() * 1e9)::bigint,
       k3 = (random() * 1e6)::integer,
       tag = md5(random()::text),
       updated_at = clock_timestamp(),
       counter = counter + 1
 WHERE id = :rid;
SQL
    done
    echo "END;"
} > "${UPDATE_SQL}"

# COPY worker script: build a COPY_ROWS-row batch on the server with
# generate_series feeding an INSERT...SELECT. This is the COPY-equivalent bulk
# heap-insert burst into the unindexed wal_bulk table, fully WAL-logged.
{
    echo "\\set batch random(1, 1000000000)"
    cat <<SQL
INSERT INTO wal_bulk (id, batch, blob)
SELECT g, :batch, repeat(md5((g + :batch)::text), ${COPY_BLOB_REPEAT})
FROM generate_series(1, ${COPY_ROWS}) AS g;
SQL
} > "${COPY_SQL}"

# Reset wal_bulk so the COPY storm does not accumulate across cells/runs and
# fill the data volume — at COPY_ROWS=50000 a 10-min burst adds tens of GB, and
# nothing reclaimed it, so a multi-cell matrix on a finite NVMe eventually hit
# ENOSPC (PG crash + aborted workload). wal_bulk is pure churn (only there to
# emit heap-insert WAL), so truncating loses nothing and each cell starts clean.
echo "==> TRUNCATE wal_bulk (bound data-volume growth across cells/runs)"
psql -X -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE wal_bulk;"

# --- launch workers ---------------------------------------------------------
# One pgbench process per worker (-c 1 -j 1), each looping its script for the
# whole DURATION. Running them as separate processes keeps a clean 1:1 mapping
# between workers and backends and isolates COPY workers from UPDATE workers.
WORKER_PIDS=""

launch_worker() {
    local label="$1" script="$2" proto="$3"
    pgbench \
        -c 1 -j 1 \
        -T "${DURATION}" \
        -M "${proto}" \
        --no-vacuum \
        -f "${script}" \
        "${PGDATABASE}" \
        > "${WORKDIR}/${label}.log" 2>&1 &
    WORKER_PIDS="${WORKER_PIDS} $!"
}

for i in $(seq 1 "${UPDATE_WORKERS}"); do
    launch_worker "update-${i}" "${UPDATE_SQL}" "${PROTOCOL}"
done
for i in $(seq 1 "${COPY_WORKERS}"); do
    # COPY/INSERT-SELECT workers use the simple protocol; no benefit from prepared.
    launch_worker "copy-${i}" "${COPY_SQL}" "simple"
done

echo "==> Launched ${WORKERS} workers; running for ${DURATION}s ..."

# Wait for all workers; tally failures without aborting mid-burst so every log is
# still summarized below.
failed_workers=0
for pid in ${WORKER_PIDS}; do
    if ! wait "${pid}"; then
        failed_workers=$(( failed_workers + 1 ))
    fi
done
WORKER_PIDS=""   # all reaped; nothing for cleanup() to kill

echo "==> Per-worker pgbench summaries:"
for log in "${WORKDIR}"/*.log; do
    [[ -e "${log}" ]] || continue
    echo "---- $(basename "${log}" .log) ----"
    grep -E "tps|number of transactions actually processed|failed" "${log}" || cat "${log}"
done

# pgbench prints "number of failed transactions" only when >0 (deadlock /
# serialization / mid-run disconnect): a worker can exit 0 yet still drop work, so
# a clean exit code alone does not prove a full-strength workload.
failed_txns="$(grep -hoE 'number of failed transactions: [0-9]+' "${WORKDIR}"/*.log 2>/dev/null \
    | awk '{s+=$NF} END{print s+0}' || true)"

echo "==> Burst finished: failed_workers=${failed_workers} failed_txns=${failed_txns}"

# A degraded burst means this cell saw a weaker workload than its peers and is not
# comparable. Exit non-zero; callers record an explicit invalid-run marker.
if (( failed_workers != 0 || failed_txns != 0 )); then
    echo "FATAL: burst degraded (${failed_workers} worker(s) failed, ${failed_txns} failed txn(s))" >&2
    exit 1
fi
