#!/bin/bash
# walrus sync-standby container entrypoint.
#
# Maps the operator's K8s-friendly WALG_* env onto the libpq + walrus vars the
# binary reads, then execs `walrus sync-standby <partial-dir>` as PID 1. The
# receiver streams from one primary as a durable synchronous standby: it retains
# WAL locally and acks the fsync frontier, joining the primary's ANY-1 quorum.
# Adapted from the wal-g entrypoint.
#
# ENV (required): WALG_PRIMARY_HOST, WALG_PRIMARY_USER, WALG_SLOT_NAME
# ENV (optional): WALG_PRIMARY_PORT (5432), WALG_PRIMARY_DB (postgres),
#   WALG_APPLICATION_NAME (-> PGAPPNAME; the CP sets <ubid>_receiver so PG counts
#   it in synchronous_standby_names), WALG_WAL_RECEIVE_PARTIAL_DIR
#   (/var/lib/walg/partials), WALG_WAL_RECEIVE_CONTROL_LISTEN (:8444),
#   WALG_LOG_LEVEL (NORMAL).
# Mounted (ro): /etc/walg/tls/{client.crt,client.key,server-ca.crt} — the mTLS
#   client identity walrus presents to stream from the primary.
# Volume (rw): the partial dir — retained WAL the controller's janitor prunes.

set -euo pipefail

die() { echo "walrus-entrypoint: ERROR: $*" >&2; exit 1; }
log() { echo "walrus-entrypoint: $*"; }

# 1) Required env -------------------------------------------------------------
for var in WALG_PRIMARY_HOST WALG_PRIMARY_USER WALG_SLOT_NAME; do
  [ -n "${!var:-}" ] || die "$var is required"
done

# 2) Required mTLS material for streaming from the primary --------------------
for f in /etc/walg/tls/client.crt /etc/walg/tls/client.key \
         /etc/walg/tls/server-ca.crt; do
  [ -r "$f" ] || die "expected mounted secret at $f (mount it from a K8s Secret)"
done

# 3) Map operator env onto libpq + walrus vars -------------------------------
export PGHOST="$WALG_PRIMARY_HOST"
export PGPORT="${WALG_PRIMARY_PORT:-5432}"
export PGUSER="$WALG_PRIMARY_USER"
export PGDATABASE="${WALG_PRIMARY_DB:-postgres}"
export PGSSLMODE=verify-ca
export PGSSLCERT=/etc/walg/tls/client.crt
export PGSSLKEY=/etc/walg/tls/client.key
export PGSSLROOTCERT=/etc/walg/tls/server-ca.crt
# walrus reads PGAPPNAME (or WALG_APPLICATION_NAME); the CP sets <ubid>_receiver
export PGAPPNAME="${WALG_APPLICATION_NAME:-walrus}"

# walrus sync-standby takes the partial dir positionally; it still builds a
# storage handle (Settings::from_env). sync-standby uploads nothing to primary
# storage, so a harmless local-fs handle at the partial dir satisfies detect_storage.
PARTIAL_DIR="${WALG_WAL_RECEIVE_PARTIAL_DIR:-/var/lib/walg/partials}"
export WALG_FILE_PREFIX="$PARTIAL_DIR"
export WALG_SLOTNAME="$WALG_SLOT_NAME"

# walrus uses WALG_LOG_LEVEL as a tracing EnvFilter directive — NOT wal-g's
# NORMAL/DEVEL words. The operator passes NORMAL, which EnvFilter reads as the
# (unused) target "NORMAL" and silences everything; translate to a real level.
case "${WALG_LOG_LEVEL:-NORMAL}" in
  DEVEL | DEBUG | debug | trace) export WALG_LOG_LEVEL="debug" ;;
  ERROR | error) export WALG_LOG_LEVEL="error" ;;
  *) export WALG_LOG_LEVEL="info" ;;
esac

# The operator emits the control listen as ":8444" (Go bind syntax); Rust's
# TcpListener::bind needs an explicit host.
if [ -n "${WALG_WAL_RECEIVE_CONTROL_LISTEN:-}" ]; then
  case "$WALG_WAL_RECEIVE_CONTROL_LISTEN" in
  :*) export WALG_WAL_RECEIVE_CONTROL_LISTEN="0.0.0.0${WALG_WAL_RECEIVE_CONTROL_LISTEN}" ;;
  esac
fi

# DR-tail S3 (dr-catchup / failover-primary tail flush): the operator emits the
# endpoint + path-style under AWS_* names, but walrus reads AWS_ENDPOINT_URL and
# WALG_S3_FORCE_PATH_STYLE. WALG_S3_PREFIX, AWS_REGION, and the access keys pass
# through unchanged. (The receiver's primary storage stays the local file prefix;
# this S3 handle is built separately for the dr-tail lane.)
if [ -n "${AWS_ENDPOINT:-}" ]; then
  export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-$AWS_ENDPOINT}"
fi
if [ -n "${AWS_S3_FORCE_PATH_STYLE:-}" ]; then
  export WALG_S3_FORCE_PATH_STYLE="${WALG_S3_FORCE_PATH_STYLE:-$AWS_S3_FORCE_PATH_STYLE}"
fi

mkdir -p "$PARTIAL_DIR"

# 4) Hand off to walrus as PID 1 ---------------------------------------------
log "primary=$PGHOST:$PGPORT slot=$WALG_SLOTNAME app=$PGAPPNAME partial_dir=$PARTIAL_DIR"
log "control=${WALG_WAL_RECEIVE_CONTROL_LISTEN:-<disabled>} log=$WALG_LOG_LEVEL"
log "dr_s3=${WALG_WAL_RECEIVE_DR_S3:-<off>} s3_prefix=${WALG_S3_PREFIX:-<none>} s3_endpoint=${AWS_ENDPOINT_URL:-<default>}"

exec /usr/bin/walrus sync-standby "$PARTIAL_DIR"
