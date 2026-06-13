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

# Respect a pre-set compression method (codec matrix) else default zstd
export WALG_COMPRESSION_METHOD="${WALG_COMPRESSION_METHOD:-zstd}"

mkdir -p "$PGHOST"

# Configure the storage backend from WALRS_STORAGE_BACKEND (default fs). Exports
# the WALG_* vars the wal-rs binary reads, plus WALG_ARCHIVE_ENV: the `KEY=VAL`
# prefix pg_archive_on inlines so the archive_command subprocess targets the
# same backend. s3 points at MinIO (path-style), gcs at fake-gcs-server.
storage_init() {
    local backend="${WALRS_STORAGE_BACKEND:-fs}"
    case "$backend" in
    fs)
        export WALG_FILE_PREFIX="$WORKROOT/storage"
        # wal-g still recognises the legacy file:// URL; set both so either
        # tool reads the same bucket regardless of which layer it picks.
        export WALE_FILE_PREFIX="file://localhost$WALG_FILE_PREFIX"
        mkdir -p "$WALG_FILE_PREFIX"
        WALG_ARCHIVE_ENV="WALG_FILE_PREFIX=$WALG_FILE_PREFIX"
        ;;
    s3)
        : "${MINIO_ENDPOINT:?set MINIO_ENDPOINT, e.g. http://127.0.0.1:9000}"
        local bucket="${WALRS_S3_BUCKET:-walrs}"
        export WALG_S3_PREFIX="s3://$bucket/$(basename "$WORKROOT")"
        export AWS_ENDPOINT_URL="$MINIO_ENDPOINT"
        export WALG_S3_FORCE_PATH_STYLE=true
        export AWS_REGION="${AWS_REGION:-us-east-1}"
        export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
        export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
        WALG_ARCHIVE_ENV="WALG_S3_PREFIX=$WALG_S3_PREFIX AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL WALG_S3_FORCE_PATH_STYLE=true AWS_REGION=$AWS_REGION AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
        ;;
    gcs)
        : "${FAKE_GCS_ENDPOINT:?set FAKE_GCS_ENDPOINT, e.g. http://127.0.0.1:4443}"
        local bucket="${WALRS_GS_BUCKET:-walrs}"
        export WALG_GS_PREFIX="gs://$bucket/$(basename "$WORKROOT")"
        export WALG_GS_ENDPOINT="$FAKE_GCS_ENDPOINT"
        WALG_ARCHIVE_ENV="WALG_GS_PREFIX=$WALG_GS_PREFIX WALG_GS_ENDPOINT=$WALG_GS_ENDPOINT"
        ;;
    *)
        echo "unknown WALRS_STORAGE_BACKEND=$backend" >&2
        exit 1
        ;;
    esac
    export WALG_ARCHIVE_ENV
}
storage_init

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
archive_command = '$WALG_ARCHIVE_ENV WALG_COMPRESSION_METHOD=$WALG_COMPRESSION_METHOD $1 wal-push %p'
archive_timeout = 30
wal_level = replica
max_wal_senders = 4
EOF
}

# Enable streaming replication without archiving — for BASE_BACKUP /
# START_REPLICATION exercises (backup-push, wal-receive) that read WAL off the
# wire rather than from the archive. Kept distinct from pg_archive_on so the
# vm-test lane doesn't drag in an archive_command it never uses.
pg_replication_on() {
    # wal_keep_size retains recent segments so START_REPLICATION from the
    # current segment boundary can't race a checkpoint that recycles it
    # ("requested WAL segment ... has already been removed") on an otherwise
    # idle cluster.
    cat >>"$PGDATA/postgresql.conf" <<EOF
wal_level = replica
max_wal_senders = 8
wal_keep_size = 128MB
EOF
}

# initdb's default pg_hba trusts local replication on PG 13+, but make it
# explicit & idempotent so the lane survives template-pg_hba changes.
pg_hba_replication() {
    if ! grep -qE '^[[:space:]]*local[[:space:]]+replication[[:space:]]+all[[:space:]]+trust' "$PGDATA/pg_hba.conf"; then
        printf 'local replication all trust\n' >>"$PGDATA/pg_hba.conf"
    fi
}

# Listen on TCP loopback in addition to the unix socket. Required for the
# TLS + SCRAM lanes: wal-rs skips TLS on unix sockets (mirrors libpq), and the
# handshake tests connect with PGHOST=127.0.0.1.
pg_listen_tcp() {
    cat >>"$PGDATA/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
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

# One bucket-interop roundtrip: $1 writes the backup + WAL, $2 restores and
# replays, dumps compared. Storage/compression/encryption come from the
# exported env so callers vary just those. Leaves the cluster stopped + dropped.
# Used by the new cross_tool_{encryption,lzma} scripts; the original
# forward/reverse scripts predate it and stay inline.
cross_roundtrip() {
    local writer="$1" reader="$2"
    pg_initdb
    pg_archive_on "$writer"
    pg_start
    pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres >/dev/null
    psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
    pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump1.sql"

    if [ "$writer" = "$WALRS_BIN" ]; then walrs backup-push; else walg backup-push "$PGDATA"; fi
    psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
    sleep 3

    pg_drop
    mkdir -p "$PGDATA"
    chmod 700 "$PGDATA"
    # arg order differs: walrs backup-fetch <name> <dst>, walg backup-fetch <dst> <name>
    if [ "$reader" = "$WALRS_BIN" ]; then walrs backup-fetch LATEST "$PGDATA"; else walg backup-fetch "$PGDATA" LATEST; fi

    pg_recovery_conf "$reader wal-fetch %f %p"
    pg_start
    local _i
    for _i in $(seq 1 60); do
        if psql -p "$PGPORT" -h "$PGHOST" -tAc 'SELECT pg_is_in_recovery()' postgres 2>/dev/null | grep -qx f; then
            break
        fi
        sleep 1
    done

    pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump2.sql"
    diff -I '^\\\(restrict\|unrestrict\) ' "$WORKROOT/dump1.sql" "$WORKROOT/dump2.sql"
    pg_drop
}

cleanup() {
    pg_stop || true
}
trap cleanup EXIT

# Drop the bucket between subtests. fs blows away the prefix; object backends
# would need API deletes, so it's an explicit error there (the storage-lane
# scripts use a fresh per-run prefix instead of resetting mid-run).
bucket_reset() {
    case "${WALRS_STORAGE_BACKEND:-fs}" in
    fs)
        rm -rf "$WALG_FILE_PREFIX"
        mkdir -p "$WALG_FILE_PREFIX"
        ;;
    *)
        echo "bucket_reset unsupported for WALRS_STORAGE_BACKEND=${WALRS_STORAGE_BACKEND}" >&2
        return 1
        ;;
    esac
}
