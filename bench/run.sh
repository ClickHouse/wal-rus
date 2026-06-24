#!/usr/bin/env bash
#
# run.sh DAEMON RUN_ID
#
#   DAEMON  - walg | walrus | pgbackrest   (which archiver to exercise)
#   RUN_ID  - free-form label, e.g. r1 / 2026-06-22
#
# Drive one archive-command benchmark cell on this host
#
# Select archiver, drain backlog, sample high-WAL burst, capture inventory
#
# Results land under bench/results/<DAEMON>-<RUN_ID>/ (override RESULTS_ROOT).
# Assumes ./setup.sh has run and the bench DB is seeded (pgbench_init.sh).
# Run as normal user, sudo handles root steps
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
LOG_TAG=run
# shellcheck source=scripts/lib.sh
. "${SCRIPT_DIR}/scripts/lib.sh"
load_config

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <walg|walrus|pgbackrest> <run_id>" >&2
  exit 2
fi
DAEMON="$1"
RUN_ID="$2"

case "${DAEMON}" in
  walg|walrus|pgbackrest) ;;
  *) echo "error: DAEMON must be walg|walrus|pgbackrest, got '${DAEMON}'" >&2; exit 2 ;;
esac

: "${BUCKET:?set BUCKET in config.env}"
: "${PGUSER:?set PGUSER in config.env}"
: "${PGPASSWORD:?set PGPASSWORD in config.env}"
: "${UPLOAD_CONCURRENCY:?set UPLOAD_CONCURRENCY in config.env}"

# --- fixed contract constants ------------------------------------------------
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
PG_ENV_FILE="/etc/postgresql/wal-g.env"
AWS_REGION="${AWS_REGION:-us-east-1}"
COMPRESSION="${WALG_COMPRESSION_METHOD:-lz4}"
# Isolate each daemon+run under one bucket
WALG_PREFIX="s3://${BUCKET}/walg-bench/${DAEMON}/${RUN_ID}"
PGBACKREST_REPO_PATH="/pgbackrest-bench/${DAEMON}/${RUN_ID}"
PGBACKREST_STANZA="walbench"
PGDATA_DIR="/dat/18/data"
PGBIN="/usr/lib/postgresql/18/bin"
PGHOST_DRIVER="${PGHOST_DRIVER:-127.0.0.1}"
RESULTS_ROOT="${RESULTS_ROOT:-${SCRIPT_DIR}/results}"
RESULT_DIR="${RESULTS_ROOT}/${DAEMON}-${RUN_ID}"
SAMPLER="/usr/local/bin/bench-sampler"
WORKLOAD="${SCRIPT_DIR}/scripts/driver/run_workload.sh"
if [[ "${DAEMON}" == "pgbackrest" ]]; then
  INV_PREFIX="s3://${BUCKET}${PGBACKREST_REPO_PATH}"
else
  INV_PREFIX="${WALG_PREFIX}"
fi

# --- pre-flight: DB seeded? --------------------------------------------------
require_seeded

log "daemon=${DAEMON} run_id=${RUN_ID} concurrency=${UPLOAD_CONCURRENCY}"

# --- step 1: select + configure the daemon -----------------------------------
if [[ "${DAEMON}" == "pgbackrest" ]]; then
  log "configuring pgbackrest (stanza=${PGBACKREST_STANZA}, process-max=${UPLOAD_CONCURRENCY})"
  run_root "${UPLOAD_CONCURRENCY}" "${PGBACKREST_STANZA}" "${PGDATA_DIR}" "${PGBIN}" \
    "${PGBACKREST_REPO_PATH}" <<'REMOTE'
set -euo pipefail
CONCURRENCY="$1"; STANZA="$2"; PGDATA_DIR="$3"; PGBIN="$4"; REPO_PATH="$5"
CONF="/etc/pgbackrest/pgbackrest.conf"

# Daemonless: stop wal-g/walrus so only pgbackrest archives this cell.
systemctl stop wal-g.service walrus.service 2>/dev/null || true
rm -f /tmp/wal-g

[[ -f "${CONF}" ]] || { echo "error: ${CONF} missing (run 05_install_pgbackrest.sh)" >&2; exit 1; }
sed -i -E "s/^process-max=.*/process-max=${CONCURRENCY}/" "${CONF}"
if grep -qE '^repo1-path=' "${CONF}"; then
  sed -i -E "s#^repo1-path=.*#repo1-path=${REPO_PATH}#" "${CONF}"
else
  printf 'repo1-path=%s\n' "${REPO_PATH}" >>"${CONF}"
fi
echo "process-max -> $(grep -E '^process-max=' "${CONF}")"
echo "repo1-path -> $(grep -E '^repo1-path=' "${CONF}")"

sudo -u postgres pgbackrest --stanza="${STANZA}" stanza-create
ARCHIVE_CMD="pgbackrest --stanza=${STANZA} archive-push %p"
sudo -u postgres "${PGBIN}/psql" -p 5432 -tA \
  -c "ALTER SYSTEM SET archive_library = '';" \
  -c "ALTER SYSTEM SET archive_command = '${ARCHIVE_CMD}';" \
  -c "SELECT pg_reload_conf();" >/dev/null
sleep 2
echo "archive_command set for pgbackrest: ${ARCHIVE_CMD}"

if sudo -u postgres pgbackrest --stanza="${STANZA}" check; then
  echo "pgbackrest check OK"
else
  echo "warning: pgbackrest check non-zero; relying on workload metrics" >&2
fi
REMOTE
  log "pre-drain leftover backlog"
  drain_backlog 10 300
else
  log "writing ${PG_ENV_FILE} + selecting ${DAEMON} (own daemon-client)"
  # Pin ENV_FILE so sudo -E cannot turn config selector into env-file output
  ENV_FILE="${PG_ENV_FILE}" \
    BUCKET="${BUCKET}" UPLOAD_CONCURRENCY="${UPLOAD_CONCURRENCY}" \
    WALG_S3_PREFIX="${WALG_PREFIX}" \
    AWS_REGION="${AWS_REGION}" WALG_COMPRESSION_METHOD="${COMPRESSION}" \
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
    AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    sudo -E bash "${SCRIPT_DIR}/scripts/sut/11_write_walg_env.sh"
  sudo bash "${SCRIPT_DIR}/scripts/sut/30_select_daemon.sh" "${DAEMON}"

  log "pre-drain leftover backlog"
  drain_backlog 10 300
fi

log "checkpoint before measured burst"
checkpoint_pg

# --- step 2: reset archiver stats + start sampler ----------------------------
start_sampler --daemon "${DAEMON}"
trap stop_sampler EXIT

# --- step 3: drive the workload (local) --------------------------------------
log "starting workload against PG ${PGHOST_DRIVER}"
WL_ENV=(PGHOST="${PGHOST_DRIVER}" PGUSER="${PGUSER}" PGPASSWORD="${PGPASSWORD}" RUN_ID="${DAEMON}-${RUN_ID}")
for v in BURST_SECONDS BURST_WORKERS COPY_WORKERS COPY_BLOB_REPEAT CHURN_ROWS; do
  if [[ -n "${!v:-}" ]]; then WL_ENV+=("${v}=${!v}"); fi
done
if env "${WL_ENV[@]}" bash "${WORKLOAD}"; then
  log "workload complete"
else
  mark_invalid "burst degraded (failed workers or failed transactions)"
fi

# --- step 4a: stop sampler ---------------------------------------------------
stop_sampler
trap - EXIT

# --- step 4b: capture S3 inventory + provenance ------------------------------
log "capturing S3 inventory and provenance into ${RESULT_DIR}"
HARNESS_GIT="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo 'no-git')"

write_provenance "${RESULT_DIR}" "${INV_PREFIX}" "${AWS_REGION}" \
  "daemon=${DAEMON}" \
  "run_id=${RUN_ID}" \
  "upload_concurrency=${UPLOAD_CONCURRENCY}" \
  "scale=${SCALE:-unset}" \
  "churn_rows=${CHURN_ROWS:-unset}" \
  "burst_seconds=${BURST_SECONDS:-unset}" \
  "checkpoint_before_burst=1" \
  "harness_git=${HARNESS_GIT}"

log "DONE: ${DAEMON}-${RUN_ID}"
