#!/usr/bin/env bash
#
# run_workload.sh
#
# Driver workload wrapper for one benchmark cell:
#   PGHOST=.. PGUSER=.. PGPASSWORD=.. RUN_ID=.. bash scripts/driver/run_workload.sh
#
# Runs high-WAL burst, assumes pgbench_init.sh already ran
#
# Env (with defaults):
#   PGHOST/PGUSER/PGPASSWORD  (required; passed by run.sh)
#   PGDATABASE       walbench
#   BURST_SECONDS    300    burst-phase duration
#   BURST_WORKERS    <driver nproc>  concurrent burst workers (passed through)
#   CHURN_ROWS       2000000 must match pgbench_init.sh CHURN_ROWS
#   RUN_ID           label, for logs only
#
# Runs next to workload_burst.sh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export PGDATABASE="${PGDATABASE:-walbench}"
BURST_SECONDS="${BURST_SECONDS:-300}"
CHURN_ROWS="${CHURN_ROWS:-2000000}"

: "${PGHOST:?run.sh must pass PGHOST}"
: "${PGUSER:?run.sh must pass PGUSER}"

echo "==> run_workload RUN_ID=${RUN_ID:-?}: burst=${BURST_SECONDS}s db=${PGDATABASE}"

echo "==> high-WAL burst"
if [[ -n "${BURST_WORKERS:-}" ]]; then
  DURATION="${BURST_SECONDS}" CHURN_ROWS="${CHURN_ROWS}" WORKERS="${BURST_WORKERS}" \
    bash "${SCRIPT_DIR}/workload_burst.sh"
else
  DURATION="${BURST_SECONDS}" CHURN_ROWS="${CHURN_ROWS}" \
    bash "${SCRIPT_DIR}/workload_burst.sh"
fi

echo "==> run_workload complete RUN_ID=${RUN_ID:-?}"
