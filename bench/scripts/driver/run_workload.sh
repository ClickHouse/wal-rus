#!/usr/bin/env bash
#
# run_workload.sh
#
# Driver-side orchestrator for ONE benchmark cell. run.sh invokes it locally as:
#   PGHOST=.. PGUSER=.. PGPASSWORD=.. RUN_ID=.. bash scripts/driver/run_workload.sh
#
# Runs the measured workload: the high-WAL burst phase, and BLOCKS until it
# finishes. The burst is the heavy-load phase we care about. Assumes the
# 'walbench' DB is already initialized (pgbench_init.sh ran once during setup) —
# it does NOT re-init.
#
# Env (with defaults):
#   PGHOST/PGUSER/PGPASSWORD  (required; passed by run.sh)
#   PGDATABASE       walbench
#   BURST_SECONDS    300    burst-phase duration
#   BURST_WORKERS    <driver nproc>  concurrent burst workers (passed through)
#   CHURN_ROWS       2000000 must match pgbench_init.sh CHURN_ROWS
#   RUN_ID           label, for logs only
#
# Runs from scripts/driver/, next to the workload_burst.sh phase script.
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
