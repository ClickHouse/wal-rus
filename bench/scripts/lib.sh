# shellcheck shell=bash
#
# lib.sh, shared benchmark driver helpers
#
# Sourced by run.sh and run_op.sh
#
# Requires driver globals: SCRIPT_DIR, LOG_TAG, PGUSER, PGPASSWORD,
# PGHOST_DRIVER, PGDATA_DIR, PGBIN, RESULT_DIR, SAMPLER, AWS_REGION

# Source config.env with auto-export for child sudo blocks
load_config() {
  set -a
  # shellcheck source=../config.env.example
  . "${ENV_FILE:-${SCRIPT_DIR}/config.env}"
  set +a
}

log() { printf '[%s %s] %s\n' "${LOG_TAG}" "$(date -u +%H:%M:%S)" "$*" >&2; }

# Run stdin script as root
run_root() { sudo bash -s -- "$@"; }

# Abort unless bench DB is seeded
require_seeded() {
  local seeded
  seeded="$(PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST_DRIVER}" -U "${PGUSER}" \
    -d walbench -tAc "SELECT to_regclass('wal_churn') IS NOT NULL" 2>/dev/null || true)"
  if [[ "${seeded}" != "t" ]]; then
    echo "error: bench DB not seeded (wal_churn missing). Seed once, e.g.:" >&2
    echo "  PGHOST=${PGHOST_DRIVER} PGUSER=${PGUSER} PGPASSWORD=*** SCALE=${SCALE:-5000} \\" >&2
    echo "    CHURN_ROWS=${CHURN_ROWS:-2000000} ${SCRIPT_DIR}/scripts/driver/pgbench_init.sh" >&2
    exit 1
  fi
}

# Wait for .ready archive backlog to fall below threshold
# Abort on timeout to avoid sampling prior load
drain_backlog() {
  local threshold="$1" iters="$2"
  run_root "${PGDATA_DIR}" "${threshold}" "${iters}" <<'REMOTE'
set -euo pipefail
PGDATA_DIR="$1"; THRESHOLD="$2"; ITERS="$3"
rb=0
for _ in $(seq 1 "${ITERS}"); do
  rb="$(ls "${PGDATA_DIR}/pg_wal/archive_status/" 2>/dev/null | grep -c '\.ready$' || true)"
  [[ "${rb}" -le "${THRESHOLD}" ]] && break
  sleep 2
done
if [[ "${rb}" -gt "${THRESHOLD}" ]]; then
  echo "error: drain timeout: ready backlog = ${rb} > ${THRESHOLD} (contaminated start; aborting cell)" >&2
  exit 1
fi
echo "drain complete: ready backlog = ${rb}"
REMOTE
}

# Normalize FPI state before bursts
checkpoint_pg() {
  run_root "${PGBIN:-/usr/lib/postgresql/18/bin}" <<'REMOTE'
set -euo pipefail
PGBIN="$1"
sudo -u postgres "${PGBIN}/psql" -p 5432 -d walbench -X -q \
  -c "CHECKPOINT;" >/dev/null
echo "checkpoint complete"
REMOTE
}

# Reset archiver stats and launch sampler
# MODE_FLAG/MODE_VALUE select --daemon or --proc-match
start_sampler() {
  local mode_flag="$1" mode_value="$2"
  log "starting sampler (${mode_flag} ${mode_value}) -> ${RESULT_DIR}"
  run_root "${RESULT_DIR}" "${SAMPLER}" "${mode_flag}" "${mode_value}" "${PGDATA_DIR}" <<'REMOTE'
set -euo pipefail
RESULT_DIR="$1"; SAMPLER="$2"; MODE_FLAG="$3"; MODE_VALUE="$4"; PGDATA="$5"
install -d -o postgres -g postgres "${RESULT_DIR}"
# Sampler runs as postgres, so RESULT_DIR must be postgres-traversable; the
# default results/ under a 0750 home is not. Fail clear, not as a cryptic
# sampler start failure. Relocate via RESULTS_ROOT (e.g. /dat/bench-results).
if ! sudo -u postgres test -w "${RESULT_DIR}"; then
  echo "error: ${RESULT_DIR} unwritable by postgres (sampler runs as postgres)" >&2
  echo "       ancestor likely not traversable (e.g. a 0750 home dir); set" >&2
  echo "       RESULTS_ROOT to a postgres-traversable path, e.g. /dat/bench-results" >&2
  exit 1
fi
sudo -u postgres psql -X -q -c "SELECT pg_stat_reset_shared('archiver');" >/dev/null 2>&1 || true
sudo -u postgres bash -c "
  nohup '${SAMPLER}' ${MODE_FLAG} '${MODE_VALUE}' --pgdata '${PGDATA}' \
    --outdir '${RESULT_DIR}' >'${RESULT_DIR}/sampler.log' 2>&1 &
  echo \$! >'${RESULT_DIR}/sampler.pid'
"
sleep 1
SPID="$(cat "${RESULT_DIR}/sampler.pid")"
if ! kill -0 "${SPID}" 2>/dev/null; then
  echo "error: sampler failed to start; see ${RESULT_DIR}/sampler.log" >&2
  cat "${RESULT_DIR}/sampler.log" >&2 || true
  exit 1
fi
echo "sampler running pid=${SPID}"
REMOTE
}

# Stop sampler, safe from EXIT trap
stop_sampler() {
  log "stopping sampler"
  run_root "${RESULT_DIR}" <<'REMOTE' || true
set -euo pipefail
RESULT_DIR="$1"
if [[ -f "${RESULT_DIR}/sampler.pid" ]]; then
  SPID="$(cat "${RESULT_DIR}/sampler.pid")"
  kill "${SPID}" 2>/dev/null || true
  for _ in $(seq 1 10); do kill -0 "${SPID}" 2>/dev/null || break; sleep 1; done
  kill -9 "${SPID}" 2>/dev/null || true
fi
REMOTE
}

# Mark degraded run so analysis skips it
mark_invalid() {
  log "INVALID run: $*"
  run_root "${RESULT_DIR}" "$*" <<'REMOTE' || true
set -euo pipefail
RESULT_DIR="$1"; REASON="$2"
install -d -o postgres -g postgres "${RESULT_DIR}"
printf 'reason=%s\ncaptured_at=%s\n' "${REASON}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  >"${RESULT_DIR}/INVALID"
chown postgres:postgres "${RESULT_DIR}/INVALID"
REMOTE
}

# Capture S3 inventory and provenance
write_provenance() {
  run_root "$@" <<'REMOTE'
set -euo pipefail
RESULT_DIR="$1"; INV_PREFIX="$2"; AWS_REGION="$3"; shift 3

aws s3 ls --recursive --summarize "${INV_PREFIX}/" --region "${AWS_REGION}" \
  >"${RESULT_DIR}/s3_inventory.txt" 2>&1 || \
  echo "warning: aws s3 ls failed (see file)" >&2

WALG_VER="$(/usr/bin/wal-g --version 2>&1 | head -1 || echo 'unknown')"
WALRUS_VER="$(/usr/local/bin/walrus --version 2>&1 | head -1 || echo 'unknown')"
WALRUS_SHA="$(sha256sum /usr/local/bin/walrus 2>/dev/null | awk '{print $1}' || echo 'unknown')"
WALG_SHA="$(sha256sum /usr/bin/wal-g 2>/dev/null | awk '{print $1}' || echo 'unknown')"
PGBR_VER="$(pgbackrest version 2>&1 | head -1 || echo 'unknown')"
PGBR_SHA="$(sha256sum "$(command -v pgbackrest)" 2>/dev/null | awk '{print $1}' || echo 'unknown')"
{
  for line in "$@"; do printf '%s\n' "${line}"; done
  echo "wal_g_version=${WALG_VER}"
  echo "wal_g_binary_sha256=${WALG_SHA}"
  echo "walrus_version=${WALRUS_VER}"
  echo "walrus_binary_sha256=${WALRUS_SHA}"
  echo "pgbackrest_version=${PGBR_VER}"
  echo "pgbackrest_binary_sha256=${PGBR_SHA}"
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${RESULT_DIR}/provenance.txt"
echo "results persisted to ${RESULT_DIR}"
cat "${RESULT_DIR}/provenance.txt"
REMOTE
}
