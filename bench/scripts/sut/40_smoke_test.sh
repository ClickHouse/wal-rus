#!/usr/bin/env bash
# Smoke test the currently-active archive daemon: force a few WAL switches,
# insert a tiny table, wait for archiving, then confirm WAL objects landed under
# s3://<BUCKET>/walg-bench/wal_005/. FAIL loudly if nothing appears.
#
# Usage: BUCKET=my-bucket sudo ./40_smoke_test.sh   (or pass BUCKET as $1)
set -euo pipefail

BUCKET="${BUCKET:-${1:-}}"
PGBIN="${PGBIN:-/usr/lib/postgresql/18/bin}"
PSQL="${PGBIN}/psql"
SOCKET="${SOCKET:-/tmp/wal-g}"
AWS_REGION="${AWS_REGION:-us-east-1}"
WAL_SWITCHES="${WAL_SWITCHES:-5}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"

if [[ -z "${BUCKET}" ]]; then
  echo "ERROR: BUCKET is required (env BUCKET=... or first positional arg)." >&2
  exit 1
fi

S3_WAL_PREFIX="s3://${BUCKET}/walg-bench/wal_005/"

run_psql() { sudo -u postgres "${PSQL}" -X -v ON_ERROR_STOP=1 -p 5432 "$@"; }

if [[ ! -S "${SOCKET}" ]]; then
  echo "ERROR: daemon socket ${SOCKET} not present; start a daemon first." >&2
  exit 1
fi

echo "=== Baseline object count under ${S3_WAL_PREFIX} (before this daemon archives) ==="
# The prefix is SHARED by both daemons. Counting >0 would false-pass walrus on
# objects wal-g left earlier (and vice versa). Capture a baseline and require an
# INCREASE so the test proves THIS daemon archived.
before="$(aws s3 ls "${S3_WAL_PREFIX}" --region "${AWS_REGION}" 2>/dev/null | grep -c . || true)"
echo "baseline=${before}"

echo "=== Generating WAL: tiny table + ${WAL_SWITCHES} forced switches ==="
run_psql -d postgres <<SQL
CREATE TABLE IF NOT EXISTS walg_smoke (id bigserial primary key, payload text);
INSERT INTO walg_smoke (payload)
  SELECT repeat('x', 256) FROM generate_series(1, 1000);
SQL

for _ in $(seq 1 "${WAL_SWITCHES}"); do
  run_psql -d postgres -tAc "SELECT pg_switch_wal();" >/dev/null
  run_psql -d postgres -tAc \
    "INSERT INTO walg_smoke (payload) SELECT repeat('y',256) FROM generate_series(1,500);"
done
# Final switch so the last populated segment becomes archivable.
run_psql -d postgres -tAc "SELECT pg_switch_wal();" >/dev/null

echo "=== Waiting up to ${WAIT_SECONDS}s for NEW archived WAL under ${S3_WAL_PREFIX} ==="
deadline=$(( SECONDS + WAIT_SECONDS ))
found=0
count="${before}"
while (( SECONDS < deadline )); do
  count="$(aws s3 ls "${S3_WAL_PREFIX}" --region "${AWS_REGION}" 2>/dev/null | grep -c . || true)"
  if (( count > before )); then
    found=1
    break
  fi
  sleep 3
done

echo "=== Archiver status ==="
run_psql -d postgres -c \
  "SELECT archived_count, failed_count, last_archived_wal FROM pg_stat_archiver;" || true

if (( found == 0 )); then
  echo "FAIL: object count under ${S3_WAL_PREFIX} did not rise past baseline ${before} (still ${count}) after ${WAIT_SECONDS}s — this daemon did not archive." >&2
  echo "Listing parent for diagnostics:" >&2
  aws s3 ls "s3://${BUCKET}/walg-bench/" --region "${AWS_REGION}" >&2 || true
  exit 1
fi

echo "PASS: object count rose ${before} -> ${count} under ${S3_WAL_PREFIX} (this daemon archived $(( count - before )) new):"
aws s3 ls "${S3_WAL_PREFIX}" --region "${AWS_REGION}" | tail -n 10
