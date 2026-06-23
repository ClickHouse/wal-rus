#!/usr/bin/env bash
#
# matrix.sh [RUN_ID]
#
#   RUN_ID - label for this sweep's result dirs (default r1).
#
# Sweeps comparison on this host: pgbackrest, wal-g, walrus, once each,
# calling run.sh per cell. Single-host counterpart of the external fleet's
# orchestrate/run_matrix.sh (no SSH, and no GOMEMLIMIT-cap cell — that was a
# GC-policy experiment, not intrinsic footprint).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
RUN_ID="${1:-${RUN_ID:-r1}}"
read -r -a DAEMONS <<< "${DAEMONS:-pgbackrest walg walrus}"

log() { printf '[matrix %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

log "start: run_id=${RUN_ID} daemons='${DAEMONS[*]}'"

for daemon in "${DAEMONS[@]}"; do
  log "=== run ${daemon} ${RUN_ID} ==="
  "${SCRIPT_DIR}/run.sh" "${daemon}" "${RUN_ID}"
done

log "DONE"
