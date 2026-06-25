#!/bin/bash
# walrus wal-receive container entrypoint.
#
# Maps the operator's K8s-friendly WALG_* env onto the libpq + walrus vars the
# binary reads, then execs `walrus wal-receive <partial-dir>` as PID 1. The
# receiver streams from one primary as a durable synchronous standby
# (SyncReplica / skip-upload): it retains WAL locally and acks the fsync
# frontier, joining the primary's ANY-1 quorum. Adapted from the wal-g entrypoint.
#
# ENV (required): WALG_PRIMARY_HOST, WALG_PRIMARY_USER, WALG_SLOT_NAME
# ENV (optional): WALG_PRIMARY_PORT (5432), WALG_PRIMARY_DB (postgres),
#   WALG_APPLICATION_NAME (-> PGAPPNAME; the CP sets <ubid>_receiver so PG counts
#   it in synchronous_standby_names), WALG_WAL_RECEIVE_PARTIAL_DIR
#   (/var/lib/walg/partials), WALG_WAL_RECEIVE_SKIP_UPLOAD (true -> SyncReplica),
#   WALG_WAL_RECEIVE_DRAIN_BATCHING (true), WALG_WAL_RECEIVE_CONTROL_LISTEN (:8444),
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

# walrus wal-receive takes the partial dir positionally; it still builds a
# storage handle (Settings::from_env). In skip-upload mode nothing is uploaded,
# so a harmless local-fs handle at the partial dir satisfies detect_storage.
PARTIAL_DIR="${WALG_WAL_RECEIVE_PARTIAL_DIR:-/var/lib/walg/partials}"
export WALG_FILE_PREFIX="$PARTIAL_DIR"
export WALG_SLOTNAME="$WALG_SLOT_NAME"
export WALG_WAL_RECEIVE_SKIP_UPLOAD="${WALG_WAL_RECEIVE_SKIP_UPLOAD:-true}"
export WALG_WAL_RECEIVE_DRAIN_BATCHING="${WALG_WAL_RECEIVE_DRAIN_BATCHING:-true}"
export WALG_LOG_LEVEL="${WALG_LOG_LEVEL:-NORMAL}"

# The operator emits the control listen as ":8444" (Go bind syntax); Rust's
# TcpListener::bind needs an explicit host.
if [ -n "${WALG_WAL_RECEIVE_CONTROL_LISTEN:-}" ]; then
  case "$WALG_WAL_RECEIVE_CONTROL_LISTEN" in
  :*) export WALG_WAL_RECEIVE_CONTROL_LISTEN="0.0.0.0${WALG_WAL_RECEIVE_CONTROL_LISTEN}" ;;
  esac
fi

mkdir -p "$PARTIAL_DIR"

# 4) Hand off to walrus as PID 1 ---------------------------------------------
log "primary=$PGHOST:$PGPORT slot=$WALG_SLOTNAME app=$PGAPPNAME partial_dir=$PARTIAL_DIR"
log "skip_upload=$WALG_WAL_RECEIVE_SKIP_UPLOAD drain_batching=$WALG_WAL_RECEIVE_DRAIN_BATCHING control=${WALG_WAL_RECEIVE_CONTROL_LISTEN:-<disabled>}"

exec /usr/bin/walrus wal-receive "$PARTIAL_DIR"
