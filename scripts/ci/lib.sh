#!/usr/bin/env bash
# Shared helpers for wal-rs CI tests. Sourced by scripts/ci/*.sh.
#
# Adapted from wal-g's docker/pg_tests/scripts/tests/test_functions/. Differs
# in that PG runs under the runner user (no `postgres` OS account, no docker
# isolation), and storage defaults to fs (WALG_FILE_PREFIX) since wal-rs's S3
# matrix isn't on this path yet.

set -euo pipefail

: "${PG_BIN:?PG_BIN must point at the postgresql bindir, e.g. /usr/lib/postgresql/16/bin}"
: "${WALRS_BIN:?WALRS_BIN must point at the wal-rs binary}"

export PATH="$PG_BIN:$PATH"

# Per-test scratch dir; kept on failure for log upload
WORKROOT=$(mktemp -d -t wal-rs-ci-XXXXXX)
export PGDATA="$WORKROOT/pgdata"
export PGHOST="$WORKROOT/run"
export PGUSER="$(id -un)"
export PGDATABASE=postgres
export PGPORT=55435

export WALG_FILE_PREFIX="$WORKROOT/storage"
# wal-g still recognises the legacy WALE_FILE_PREFIX file:// URL; set both so
# either tool reads the same bucket regardless of which storage layer it picks.
export WALE_FILE_PREFIX="file://localhost$WALG_FILE_PREFIX"
export WALG_COMPRESSION_METHOD=zstd

mkdir -p "$PGHOST" "$WALG_FILE_PREFIX"

PG_LOG="$WORKROOT/pg.log"

pg_initdb() {
    initdb \
        --pgdata="$PGDATA" \
        --username="$PGUSER" \
        --auth-local=trust \
        --auth-host=trust \
        --encoding=UTF8 >"$WORKROOT/initdb.log" 2>&1

    cat >>"$PGDATA/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$PGHOST'
port = $PGPORT
fsync = off
synchronous_commit = off
log_min_messages = warning
log_destination = 'stderr'
logging_collector = off
EOF
}

pg_archive_on() {
    # archive_command receives a relative %p from PG; wal-push resolves it
    # against PGDATA's cwd, which matches wal-g's contract.
    cat >>"$PGDATA/postgresql.conf" <<EOF
archive_mode = on
archive_command = 'WALG_FILE_PREFIX=$WALG_FILE_PREFIX WALG_COMPRESSION_METHOD=$WALG_COMPRESSION_METHOD $1 wal-push %p'
archive_timeout = 30
wal_level = replica
max_wal_senders = 4
EOF
}

pg_start() {
    pg_ctl -D "$PGDATA" -l "$PG_LOG" -w -t 60 start
}

pg_stop() {
    pg_ctl -D "$PGDATA" -m fast -w -t 60 stop || true
}

pg_drop() {
    pg_stop
    rm -rf "$PGDATA"
}

# PG 12+ uses recovery.signal + postgresql.conf; older versions use
# recovery.conf. wal-rs supports PG 13+ so the signal-file branch is the only
# one currently exercised, but keep the older branch for forward-compat.
pg_recovery_conf() {
    local restore_cmd=$1
    if [ "${PG_VERSION:-13}" -ge 12 ]; then
        touch "$PGDATA/recovery.signal"
        printf 'restore_command = %s\n' "'$restore_cmd'" >>"$PGDATA/postgresql.conf"
    else
        printf 'restore_command = %s\n' "'$restore_cmd'" >"$PGDATA/recovery.conf"
    fi
}

# Re-export so child processes inherit a clean env. Avoid hard-coding
# WALG_FILE_PREFIX into archive_command in some tests since wal-g's
# `wal-push` reads it from the environment, not the args.
walrs() {
    "$WALRS_BIN" "$@"
}

walg() {
    : "${WALG_BIN:?WALG_BIN must be set for cross-tool tests}"
    "$WALG_BIN" "$@"
}

cleanup() {
    pg_stop || true
}
trap cleanup EXIT

# Drop the bucket between subtests when wal-rs gains `delete everything`.
# For now, just blow away the prefix.
bucket_reset() {
    rm -rf "$WALG_FILE_PREFIX"
    mkdir -p "$WALG_FILE_PREFIX"
}
