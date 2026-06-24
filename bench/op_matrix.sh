#!/usr/bin/env bash
#
# op_matrix.sh [RUN_ID]
#
#   RUN_ID - label for this sweep's result dirs (default r1).
#
# Sweep data-movement ops across tools that implement them
# backup-send runs first so backup-fetch has something to restore
# Delta cells take their own parent full, avoiding cross-tool WAL gaps
#
# Skipped cells: pgbackrest has no wal-receive equivalent; backup-delta-sidecar
# has no pgbackrest peer (WALG_USE_WAL_DELTA is a walrus/wal-g daemon feature);
# backup-delta-summaries is walrus-only (no wal-g / pgbackrest WAL-summary delta).
# Override OPS / TOOLS via env
#
# backup-delta-chain is opt-in: OPS="backup-delta-chain"
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
RUN_ID="${1:-${RUN_ID:-r1}}"
read -r -a OPS <<< "${OPS:-backup-send backup-fetch backup-delta backup-delta-sidecar backup-delta-summaries wal-receive}"
read -r -a TOOLS <<< "${TOOLS:-pgbackrest walg walrus}"

log() { printf '[op-matrix %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

log "start: run_id=${RUN_ID} ops='${OPS[*]}' tools='${TOOLS[*]}'"

for op in "${OPS[@]}"; do
  for tool in "${TOOLS[@]}"; do
    if [[ "${op}" == "wal-receive" && "${tool}" == "pgbackrest" ]]; then
      log "skip ${op}/${tool} (no equivalent)"
      continue
    fi
    if [[ "${op}" == "backup-delta-summaries" && "${tool}" != "walrus" ]]; then
      log "skip ${op}/${tool} (walrus-only)"
      continue
    fi
    if [[ "${op}" == "backup-delta-sidecar" && "${tool}" == "pgbackrest" ]]; then
      log "skip ${op}/${tool} (no WALG_USE_WAL_DELTA peer)"
      continue
    fi
    log "=== run ${op} ${tool} ${RUN_ID} ==="
    "${SCRIPT_DIR}/run_op.sh" "${op}" "${tool}" "${RUN_ID}"
  done
done

log "DONE"
