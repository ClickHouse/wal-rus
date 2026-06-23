#!/usr/bin/env bash
#
# op_matrix.sh [RUN_ID]
#
#   RUN_ID - label for this sweep's result dirs (default r1).
#
# Sweeps data-movement operation benchmarks (run_op.sh) on this host:
# backup-send, backup-fetch, backup-delta, backup-delta-summaries, then
# wal-receive — each across the tools that implement it, once. Op order matters:
# backup-send runs first so a parent full exists for backup-fetch to restore and
# for the delta cells to extend.
#
# Skipped cells: pgbackrest has no wal-receive equivalent; backup-delta-summaries
# is walrus-only (no wal-g / pgbackrest WAL-summary delta). Override OPS / TOOLS
# via env. Counterpart of matrix.sh (archive path).
#
# backup-delta-chain (DELTA_MAX_STEPS-deep chain + leaf restore) is omitted from
# the default sweep — it churns once per step, so its cost scales with depth. Opt
# in with OPS="backup-send backup-delta-chain" (backup-send must precede it).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
RUN_ID="${1:-${RUN_ID:-r1}}"
read -r -a OPS <<< "${OPS:-backup-send backup-fetch backup-delta backup-delta-summaries wal-receive}"
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
    log "=== run ${op} ${tool} ${RUN_ID} ==="
    "${SCRIPT_DIR}/run_op.sh" "${op}" "${tool}" "${RUN_ID}"
  done
done

log "DONE"
