#!/usr/bin/env bash
#
# run_op.sh OP TOOL RUN_ID
#
#   OP     - backup-send | backup-fetch | backup-delta |
#            backup-delta-summaries | wal-receive       (data-movement operation)
#   TOOL   - walrus | walg | pgbackrest                  (implementation)
#   RUN_ID - free-form label, e.g. r1 / 2026-06-22
#
# Benchmarks ONE data-movement operation with ONE tool, single-host (PG + tool
# local), cross-tool where an equivalent exists. Counterpart of run.sh, which
# benches the archive_command (wal-push) path; this covers the rest of walrus:
#
#   backup-send             base backup -> S3   walrus/wal-g backup-push --full | pgbackrest backup --type=full
#   backup-fetch            restore   <- S3     walrus/wal-g backup-fetch       | pgbackrest restore
#   backup-delta            delta backup -> S3  walrus/wal-g backup-push (wi1)  | pgbackrest backup --type=incr
#   backup-delta-summaries  delta from WAL      walrus backup-push              | (walrus-only)
#                           summaries -> S3      --delta-from-wal-summaries
#   wal-receive             stream WAL from PG  walrus/wal-g wal-receive        | (no pgbackrest peer)
#
# Delta cells need a parent full backup (backup-send must precede them) and a
# churn phase: they configure the tool, checkpoint, drive a DELTA_CHURN_SECONDS
# burst with the archiver live (the default delta map walks archived WAL),
# drain, then time the delta push while archiver stays live. backup-delta-summaries
# instead sources the delta map from $PGDATA/pg_wal/summaries (needs
# summarize_wal=on, set by 10_init_pg.sh) and is walrus-only (no wal-g /
# pgbackrest peer). DELTA_ORIGIN defaults to LATEST_FULL so both delta paths
# anchor to chain root. Delta size is S3-inventory byte growth across the push,
# not on-disk cluster size.
#
# walrus's walsender (serving WAL via the replication protocol) has no CLI entry
# point yet, so wal-send is intentionally absent.
#
# The 1 Hz sampler is reused, here attached with --proc-match <tool comm>: these
# ops are one-shot CLI processes, not systemd units. Both archive daemons are
# stopped first; backup-push ops (NEEDS_ARCHIVE) then start ONLY the tool's own
# daemon and leave it up across the push (pg_backup_stop blocks on WAL archival),
# so for those the sample is the op process plus the mostly-idle daemon (~27 MB
# for walrus). backup-fetch / wal-receive run with no daemon — op process only.
#
# Results: bench/results/<OP>-<TOOL>-<RUN_ID>/ — sampler CSVs, op_metrics.txt
# (elapsed, bytes processed, MB/s), provenance.txt, s3_inventory.txt. Override
# RESULTS_ROOT to relocate.
#
# Assumes ./setup.sh has run. backup-send and wal-receive also assume the bench
# DB is seeded (pgbench_init.sh); backup-fetch assumes a compatible backup-send
# already produced a backup to fetch. Run as a normal user (uses sudo for root
# steps); do not run pgbench as root.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
LOG_TAG=op
# shellcheck source=scripts/lib.sh
. "${SCRIPT_DIR}/scripts/lib.sh"
load_config

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <backup-send|backup-fetch|backup-delta|backup-delta-summaries|wal-receive> <walrus|walg|pgbackrest> <run_id>" >&2
  exit 2
fi
OP="$1"
TOOL="$2"
RUN_ID="$3"

case "${OP}" in
  backup-send|backup-fetch|backup-delta|backup-delta-summaries|wal-receive) ;;
  *) echo "error: OP must be backup-send|backup-fetch|backup-delta|backup-delta-summaries|wal-receive, got '${OP}'" >&2; exit 2 ;;
esac
case "${TOOL}" in
  walrus|walg|pgbackrest) ;;
  *) echo "error: TOOL must be walrus|walg|pgbackrest, got '${TOOL}'" >&2; exit 2 ;;
esac
if [[ "${OP}" == "wal-receive" && "${TOOL}" == "pgbackrest" ]]; then
  echo "error: pgbackrest has no wal-receive equivalent (skip this cell)" >&2
  exit 2
fi
# WAL-summary-sourced delta is a walrus-only path (no wal-g / pgbackrest peer).
if [[ "${OP}" == "backup-delta-summaries" && "${TOOL}" != "walrus" ]]; then
  echo "error: backup-delta-summaries is walrus-only (skip this cell)" >&2
  exit 2
fi

# Delta ops drive a churn phase, then a delta push; group them for branch tests.
IS_DELTA=0
[[ "${OP}" == "backup-delta" || "${OP}" == "backup-delta-summaries" ]] && IS_DELTA=1

# Backup-push ops (full + delta) take a base backup, whose pg_backup_stop blocks
# on BackupWaitWalArchive until the backup's WAL is archived. So the tool's
# archiver MUST stay live across these cells (the sampler then sees the op
# process plus the mostly-idle daemon; for walrus that baseline is ~27 MB).
# backup-fetch (restore) and wal-receive need no archiver.
NEEDS_ARCHIVE=0
case "${OP}" in backup-send|backup-delta|backup-delta-summaries) NEEDS_ARCHIVE=1 ;; esac

: "${BUCKET:?set BUCKET in config.env}"
: "${PGUSER:?set PGUSER in config.env}"
: "${PGPASSWORD:?set PGPASSWORD in config.env}"
: "${UPLOAD_CONCURRENCY:?set UPLOAD_CONCURRENCY in config.env}"

# --- fixed contract constants ------------------------------------------------
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
AWS_REGION="${AWS_REGION:-us-east-1}"
COMPRESSION="${WALG_COMPRESSION_METHOD:-lz4}"
# Scope the prefix per tool+run (same bucket = same destination storage, fair
# comparison) so fetch LATEST / implicit delta-parent resolve only within current
# tool/run backups, never another tool's or a prior sweep's.
WALG_PREFIX="s3://${BUCKET}/walg-bench/${TOOL}/${RUN_ID}"
PGBACKREST_REPO_PATH="/pgbackrest-bench/${TOOL}/${RUN_ID}"
PGBACKREST_STANZA="walbench"
PGDATA_DIR="/dat/18/data"
PGBIN="/usr/lib/postgresql/18/bin"
PGHOST_DRIVER="${PGHOST_DRIVER:-127.0.0.1}"
WALRUS_BIN="/usr/local/bin/walrus"
WALG_BIN="/usr/bin/wal-g"
RESULTS_ROOT="${RESULTS_ROOT:-${SCRIPT_DIR}/results}"
RESULT_DIR="${RESULTS_ROOT}/${OP}-${TOOL}-${RUN_ID}"
SAMPLER="/usr/local/bin/bench-sampler"
# Where backup-fetch restores into and wal-receive assembles segments.
RESTORE_DIR="${RESTORE_DIR:-/dat/restore}"
WAL_RECV_DIR="${WAL_RECV_DIR:-/dat/walrecv}"
WAL_RECEIVE_SECONDS="${WAL_RECEIVE_SECONDS:-300}"
# Delta cells: churn window that dirties pages between the parent full and the
# delta push, and the delta-chain depth handed to walrus/wal-g (WALG_DELTA_MAX_STEPS).
DELTA_CHURN_SECONDS="${DELTA_CHURN_SECONDS:-300}"
DELTA_MAX_STEPS="${DELTA_MAX_STEPS:-7}"
DELTA_ORIGIN="${DELTA_ORIGIN:-LATEST_FULL}"

case "${TOOL}" in
  walrus) COMM="walrus" ;;
  walg)   COMM="wal-g" ;;
  pgbackrest) COMM="pgbackrest" ;;
esac
if [[ "${TOOL}" == "pgbackrest" ]]; then
  INV_PREFIX="s3://${BUCKET}${PGBACKREST_REPO_PATH}"
else
  INV_PREFIX="${WALG_PREFIX}"
fi

# Run a walrus/wal-g command as postgres with the daemon env file sourced
# (WALG_S3_PREFIX, AWS creds, region, compression, PGHOST). Absolute paths, so
# no reliance on the postgres login PATH.
run_tool() {
  sudo -u postgres bash -c '
    set -a
    . /etc/postgresql/wal-g.env
    set +a
    exec "$@"
  ' _ "$@"
}

# Current WAL position as an absolute byte offset (for wal-receive throughput).
lsn_bytes() {
  PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST_DRIVER}" -U "${PGUSER}" -d walbench \
    -tAc "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')"
}

# Total bytes stored under the tool's S3 prefix (delta cells diff before/after
# the push to size the increment). Empty/zero when the prefix has no objects.
inv_size() {
  sudo aws s3 ls --recursive --summarize "${INV_PREFIX}/" --region "${AWS_REGION}" 2>/dev/null \
    | awk '/Total Size:/ {print $3}' | tail -1
}

# --- pre-flight: DB seeded? (backup-send + wal-receive need a populated DB) ---
[[ "${OP}" == "backup-fetch" ]] || require_seeded

log "op=${OP} tool=${TOOL} run_id=${RUN_ID} concurrency=${UPLOAD_CONCURRENCY}"
CHECKPOINT_BEFORE_WORKLOAD=0

# --- step 1: tool config -----------------------------------------------------
# Stop both archive daemons so neither pollutes proc-match (they share the
# 'walrus'/'wal-g' comm with the op process) and so they do not race archiving.
run_root <<'REMOTE'
set -euo pipefail
systemctl stop wal-g.service walrus.service 2>/dev/null || true
rm -f /tmp/wal-g
REMOTE

if [[ "${TOOL}" == "pgbackrest" ]]; then
  log "configuring pgbackrest (stanza=${PGBACKREST_STANZA}, process-max=${UPLOAD_CONCURRENCY})"
  run_root "${UPLOAD_CONCURRENCY}" "${PGBACKREST_STANZA}" "${PGDATA_DIR}" "${PGBIN}" \
    "${OP}" "${PGBACKREST_REPO_PATH}" <<'REMOTE'
set -euo pipefail
CONCURRENCY="$1"; STANZA="$2"; PGDATA_DIR="$3"; PGBIN="$4"; OP="$5"; REPO_PATH="$6"
CONF="/etc/pgbackrest/pgbackrest.conf"
[[ -f "${CONF}" ]] || { echo "error: ${CONF} missing (run 05_install_pgbackrest.sh)" >&2; exit 1; }
sed -i -E "s/^process-max=.*/process-max=${CONCURRENCY}/" "${CONF}"
if grep -qE '^repo1-path=' "${CONF}"; then
  sed -i -E "s#^repo1-path=.*#repo1-path=${REPO_PATH}#" "${CONF}"
else
  printf 'repo1-path=%s\n' "${REPO_PATH}" >>"${CONF}"
fi
echo "process-max -> $(grep -E '^process-max=' "${CONF}")"
echo "repo1-path -> $(grep -E '^repo1-path=' "${CONF}")"
sudo -u postgres pgbackrest --stanza="${STANZA}" stanza-create || true

# backup (full or incr) needs WAL archiving live (pgbackrest blocks on the
# start-WAL archive), so point archive_command at pgbackrest and drain. restore
# reads only the repo. backup-delta (incr) churns + drains in the delta-prep step.
if [[ "${OP}" == "backup-send" || "${OP}" == "backup-delta" ]]; then
  ARCHIVE_CMD="pgbackrest --stanza=${STANZA} archive-push %p"
  sudo -u postgres "${PGBIN}/psql" -p 5432 -tA \
    -c "ALTER SYSTEM SET archive_library = '';" \
    -c "ALTER SYSTEM SET archive_command = '${ARCHIVE_CMD}';" \
    -c "SELECT pg_reload_conf();" >/dev/null
  sleep 2
  sudo -u postgres pgbackrest --stanza="${STANZA}" check || \
    echo "warning: pgbackrest check non-zero" >&2
fi
REMOTE
  [[ "${NEEDS_ARCHIVE}" -eq 1 ]] && { log "pre-drain leftover backlog"; drain_backlog 10 300; }
else
  log "writing /etc/postgresql/wal-g.env for ${TOOL}"
  # Pin ENV_FILE to the daemon env path: 11_write_walg_env.sh reads ENV_FILE as
  # its OUTPUT target, and sudo -E would otherwise leak a caller-set ENV_FILE
  # (our config-file selector) and clobber it.
  ENV_FILE="/etc/postgresql/wal-g.env" \
    BUCKET="${BUCKET}" UPLOAD_CONCURRENCY="${UPLOAD_CONCURRENCY}" \
    WALG_S3_PREFIX="${WALG_PREFIX}" \
    AWS_REGION="${AWS_REGION}" WALG_COMPRESSION_METHOD="${COMPRESSION}" \
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
    AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
    sudo -E bash "${SCRIPT_DIR}/scripts/sut/11_write_walg_env.sh"

  # backup-push ops need a live archiver (see NEEDS_ARCHIVE): start the tool's
  # daemon and point archive_command at its own client, then pre-drain leftover
  # backlog. backup-fetch / wal-receive skip this (no archiving needed).
  if [[ "${NEEDS_ARCHIVE}" -eq 1 ]]; then
    log "starting ${TOOL} archive daemon (backup-push waits on WAL archival at stop)"
    sudo bash "${SCRIPT_DIR}/scripts/sut/30_select_daemon.sh" "${TOOL}"
    drain_backlog 10 300
  fi
fi

if [[ "${OP}" == "backup-send" || "${OP}" == "wal-receive" ]]; then
  log "checkpoint before measured ${OP}"
  checkpoint_pg
  CHECKPOINT_BEFORE_WORKLOAD=1
fi

# --- step 1b: delta prep — churn between the parent full and the delta push ---
# The default delta map walks ARCHIVED WAL, so the churn WAL must reach the repo
# before the push. The tool's archiver is already live (step 1, NEEDS_ARCHIVE)
# and STAYS up through the push: pg_backup_stop blocks on WAL archival, so a push
# without a live archiver hangs. Just churn, then drain so the map is complete.
# (backup-delta-summaries sources the map from local pg_wal/summaries instead,
# but archiving the churn still lets pg_wal recycle and keeps the parent valid.)
if [[ "${IS_DELTA}" -eq 1 ]]; then
  log "delta-prep: checkpoint before churn"
  checkpoint_pg
  CHECKPOINT_BEFORE_WORKLOAD=1
  log "delta-prep: churn ${DELTA_CHURN_SECONDS}s (dirties pages for the delta)"
  CH_ENV=(PGHOST="${PGHOST_DRIVER}" PGUSER="${PGUSER}" PGPASSWORD="${PGPASSWORD}"
    DURATION="${DELTA_CHURN_SECONDS}" CHURN_ROWS="${CHURN_ROWS:-2000000}")
  [[ -n "${BURST_WORKERS:-}" ]] && CH_ENV+=("WORKERS=${BURST_WORKERS}")
  if ! env "${CH_ENV[@]}" bash "${SCRIPT_DIR}/scripts/driver/workload_burst.sh"; then
    mark_invalid "delta-prep churn degraded (weaker dirtying -> non-comparable delta)"
  fi

  log "delta-prep: draining archive backlog so the churn WAL is in the repo"
  drain_backlog 5 600
fi

# --- step 2: start the sampler (proc-match on the tool's comm) ----------------
start_sampler --proc-match "${COMM}"
trap stop_sampler EXIT

# --- step 3: run the operation, timed ----------------------------------------
BYTES=0
START="$(date +%s.%N)"
case "${OP}" in
  backup-send)
    log "base backup -> ${INV_PREFIX} (full)"
    case "${TOOL}" in
      walrus) run_tool "${WALRUS_BIN}" backup-push --full ;;
      walg)   run_tool "${WALG_BIN}" backup-push "${PGDATA_DIR}" --full ;;
      pgbackrest) sudo -u postgres pgbackrest --stanza="${PGBACKREST_STANZA}" backup --type=full ;;
    esac
    # bytes processed = on-disk cluster size, excluding WAL (the backup payload)
    BYTES="$(sudo du -sb --exclude=pg_wal "${PGDATA_DIR}" | awk '{print $1}')"
    ;;
  backup-delta)
    inv_before="$(inv_size)"; inv_before="${inv_before:-0}"
    log "delta backup -> ${INV_PREFIX} (wi1; origin=${DELTA_ORIGIN}; parent inventory ${inv_before} B)"
    case "${TOOL}" in
      walrus) run_tool env WALG_DELTA_MAX_STEPS="${DELTA_MAX_STEPS}" \
                WALG_DELTA_ORIGIN="${DELTA_ORIGIN}" \
                "${WALRUS_BIN}" backup-push --pgdata "${PGDATA_DIR}" ;;
      walg)   run_tool env WALG_DELTA_MAX_STEPS="${DELTA_MAX_STEPS}" \
                WALG_DELTA_ORIGIN="${DELTA_ORIGIN}" \
                "${WALG_BIN}" backup-push "${PGDATA_DIR}" ;;
      pgbackrest) sudo -u postgres pgbackrest --stanza="${PGBACKREST_STANZA}" backup --type=incr ;;
    esac
    # bytes processed = inventory growth = the delta's stored (compressed) size
    inv_after="$(inv_size)"; inv_after="${inv_after:-0}"
    BYTES=$(( inv_after - inv_before )); (( BYTES < 0 )) && BYTES=0
    ;;
  backup-delta-summaries)
    inv_before="$(inv_size)"; inv_before="${inv_before:-0}"
    log "delta-from-wal-summaries backup -> ${INV_PREFIX} (origin=${DELTA_ORIGIN}; parent inventory ${inv_before} B)"
    run_tool env WALG_DELTA_MAX_STEPS="${DELTA_MAX_STEPS}" \
      WALG_DELTA_ORIGIN="${DELTA_ORIGIN}" \
      "${WALRUS_BIN}" backup-push --pgdata "${PGDATA_DIR}" --delta-from-wal-summaries
    inv_after="$(inv_size)"; inv_after="${inv_after:-0}"
    BYTES=$(( inv_after - inv_before )); (( BYTES < 0 )) && BYTES=0
    ;;
  backup-fetch)
    log "restore LATEST -> ${RESTORE_DIR}"
    run_root "${RESTORE_DIR}" <<'REMOTE'
set -euo pipefail
RESTORE_DIR="$1"
rm -rf "${RESTORE_DIR}"
install -d -o postgres -g postgres "${RESTORE_DIR}"
REMOTE
    case "${TOOL}" in
      walrus) run_tool "${WALRUS_BIN}" backup-fetch "${RESTORE_DIR}" LATEST ;;
      walg)   run_tool "${WALG_BIN}" backup-fetch "${RESTORE_DIR}" LATEST ;;
      pgbackrest)
        sudo -u postgres pgbackrest --stanza="${PGBACKREST_STANZA}" \
          --pg1-path="${RESTORE_DIR}" --type=none restore ;;
    esac
    BYTES="$(sudo du -sb "${RESTORE_DIR}" | awk '{print $1}')"
    log "cleaning ${RESTORE_DIR}"
    sudo rm -rf "${RESTORE_DIR}"
    ;;
  wal-receive)
    log "wal-receive for ${WAL_RECEIVE_SECONDS}s while burst generates WAL"
    RECV_LOG="${RESULT_DIR}/wal-receive.log"
    run_root "${WAL_RECV_DIR}" <<'REMOTE'
set -euo pipefail
WAL_RECV_DIR="$1"
rm -rf "${WAL_RECV_DIR}"
install -d -o postgres -g postgres "${WAL_RECV_DIR}"
REMOTE
    if [[ "${TOOL}" == "walrus" ]]; then
      # archive_dir is a rotation buffer: walrus uploads each rotated segment to
      # WALG_S3_PREFIX, the SAME S3 destination wal-g streams to. Both are scored
      # by what lands in storage (below), not where they stage locally.
      recv_cmd=("${WALRUS_BIN}" wal-receive "${WAL_RECV_DIR}")
    else
      recv_cmd=("${WALG_BIN}" wal-receive)
    fi
    # Launch as postgres with the env file sourced; redirect INSIDE sudo so the
    # log lands in the postgres-owned results dir. Background the sudo wrapper.
    sudo -u postgres bash -c '
      set -a; . /etc/postgresql/wal-g.env; set +a
      log="$1"; shift
      exec "$@" >"${log}" 2>&1
    ' _ "${RECV_LOG}" "${recv_cmd[@]}" &
    RECV_PID=$!
    sleep 2
    if ! kill -0 "${RECV_PID}" 2>/dev/null; then
      echo "error: wal-receive exited early; see ${RECV_LOG}" >&2
      sudo cat "${RECV_LOG}" >&2 || true
      exit 1
    fi
    recv_before="$(inv_size)"; recv_before="${recv_before:-0}"
    lsn_start="$(lsn_bytes)"
    log "generating WAL (burst) for ${WAL_RECEIVE_SECONDS}s"
    WL_ENV=(PGHOST="${PGHOST_DRIVER}" PGUSER="${PGUSER}" PGPASSWORD="${PGPASSWORD}"
      DURATION="${WAL_RECEIVE_SECONDS}" CHURN_ROWS="${CHURN_ROWS:-2000000}")
    [[ -n "${BURST_WORKERS:-}" ]] && WL_ENV+=("WORKERS=${BURST_WORKERS}")
    if ! env "${WL_ENV[@]}" bash "${SCRIPT_DIR}/scripts/driver/workload_burst.sh"; then
      mark_invalid "wal-receive burst degraded"
    fi
    lsn_end="$(lsn_bytes)"

    # Throughput = WAL that actually LANDED in the S3 destination, not WAL
    # generated by PG (pg_current_wal_lsn advances regardless of receiver lag).
    # Uploads are async, so keep the receiver alive and poll the inventory until
    # it stops growing before sizing receipt.
    log "draining receiver uploads into ${INV_PREFIX}"
    recv_after="${recv_before}"; prev=""
    for _ in $(seq 1 30); do
      kill -0 "${RECV_PID}" 2>/dev/null || break
      recv_after="$(inv_size)"; recv_after="${recv_after:-0}"
      [[ "${recv_after}" == "${prev}" ]] && break
      prev="${recv_after}"
      sleep 5
    done
    BYTES=$(( recv_after - recv_before )); (( BYTES < 0 )) && BYTES=0

    gen=$(( lsn_end - lsn_start ))
    log "wal-receive: generated=${gen} B (uncompressed) received=${BYTES} B (stored)"
    # WAL generated but nothing stored => measured generation, not receipt.
    if (( gen > 0 && BYTES == 0 )); then
      mark_invalid "wal-receive stored 0 B to ${INV_PREFIX} while ${gen} B WAL generated"
    fi
    log "stopping wal-receive"
    kill "${RECV_PID}" 2>/dev/null || true
    sudo pkill -TERM -x "${COMM}" 2>/dev/null || true
    for _ in $(seq 1 10); do sudo pkill -0 -x "${COMM}" 2>/dev/null || break; sleep 1; done
    sudo pkill -KILL -x "${COMM}" 2>/dev/null || true
    ;;
esac
END="$(date +%s.%N)"

# --- step 4a: stop sampler ---------------------------------------------------
stop_sampler
trap - EXIT

# --- step 4b: metrics + inventory + provenance -------------------------------
ELAPSED="$(awk -v a="${START}" -v b="${END}" 'BEGIN{printf "%.3f", b-a}')"
MBPS="$(awk -v by="${BYTES}" -v s="${ELAPSED}" 'BEGIN{printf "%.2f", (s>0)? by/1e6/s : 0}')"
log "elapsed=${ELAPSED}s bytes=${BYTES} throughput=${MBPS} MB/s"

HARNESS_GIT="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo 'no-git')"

log "writing op metrics into ${RESULT_DIR}"
run_root "${RESULT_DIR}" "${OP}" "${TOOL}" "${RUN_ID}" "${ELAPSED}" "${BYTES}" \
  "${MBPS}" "${UPLOAD_CONCURRENCY}" "${WAL_RECEIVE_SECONDS}" \
  "${CHECKPOINT_BEFORE_WORKLOAD}" "${DELTA_ORIGIN}" <<'REMOTE'
set -euo pipefail
RESULT_DIR="$1"; OP="$2"; TOOL="$3"; RUN_ID="$4"; ELAPSED="$5"; BYTES="$6"
MBPS="$7"; CONCURRENCY="$8"; WAL_RECEIVE_SECONDS="$9"
CHECKPOINT_BEFORE_WORKLOAD="${10}"
DELTA_ORIGIN="${11}"
{
  echo "op=${OP}"
  echo "tool=${TOOL}"
  echo "run_id=${RUN_ID}"
  echo "elapsed_s=${ELAPSED}"
  echo "bytes_processed=${BYTES}"
  echo "throughput_mb_s=${MBPS}"
  echo "upload_concurrency=${CONCURRENCY}"
  echo "wal_receive_seconds=${WAL_RECEIVE_SECONDS}"
  echo "checkpoint_before_workload=${CHECKPOINT_BEFORE_WORKLOAD}"
  echo "delta_origin=${DELTA_ORIGIN:-}"
} >"${RESULT_DIR}/op_metrics.txt"
cat "${RESULT_DIR}/op_metrics.txt"
REMOTE

log "capturing S3 inventory and provenance into ${RESULT_DIR}"
write_provenance "${RESULT_DIR}" "${INV_PREFIX}" "${AWS_REGION}" \
  "op=${OP}" \
  "tool=${TOOL}" \
  "run_id=${RUN_ID}" \
  "checkpoint_before_workload=${CHECKPOINT_BEFORE_WORKLOAD}" \
  "delta_origin=${DELTA_ORIGIN}" \
  "harness_git=${HARNESS_GIT}"

log "DONE: ${OP}-${TOOL}-${RUN_ID}"
