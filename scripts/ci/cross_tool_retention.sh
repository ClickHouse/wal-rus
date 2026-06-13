#!/usr/bin/env bash
# Cross-tool retention agreement: after `delete retain FULL 2`, the OTHER tool
# must see exactly the same 2 survivors. Proves both tools compute the same
# survivor set and neither orphans objects the other still counts. Run once with
# each tool doing the push + delete (full backups only).
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

# Backup names sit in column 1 of both tools' `backup-list` (header lines don't
# start with base_); extract & sort for set comparison.
list_names() { "$1" backup-list 2>/dev/null | awk '/^base_/{print $1}' | sort; }
count_backups() { list_names "$1" | grep -c '^base_' || true; }

push_three() {
    local tool="$1"
    pg_initdb
    pg_archive_on "$tool"
    pg_start
    pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres >/dev/null
    local _i
    for _i in 1 2 3; do
        psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
        if [ "$tool" = "$WALRS_BIN" ]; then walrs backup-push; else walg backup-push "$PGDATA"; fi
        psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
        sleep 1
    done
}

check_agreement() {
    local deleter="$1" other="$2" label="$3"
    local n_deleter n_other
    n_deleter=$(list_names "$deleter")
    n_other=$(list_names "$other")
    [ "$(printf '%s\n' "$n_deleter" | grep -c '^base_' || true)" -eq 2 ] \
        || { echo "FAIL[$label]: expected 2 survivors"; printf '%s\n' "$n_deleter"; exit 1; }
    [ "$n_deleter" = "$n_other" ] || {
        echo "FAIL[$label]: survivor sets disagree"
        echo "-- $deleter --"; printf '%s\n' "$n_deleter"
        echo "-- $other --"; printf '%s\n' "$n_other"
        exit 1
    }
}

# wal-rs pushes 3 fulls, wal-rs deletes to 2; wal-g must agree
push_three "$WALRS_BIN"
[ "$(count_backups "$WALRS_BIN")" -eq 3 ] || { echo "expected 3 backups pre-delete"; exit 1; }
walrs delete retain FULL 2 --confirm
check_agreement "$WALRS_BIN" "$WALG_BIN" "walrs-deletes"
pg_drop
bucket_reset

# wal-g pushes 3 fulls, wal-g deletes to 2; wal-rs must agree
push_three "$WALG_BIN"
walg delete retain FULL 2 --confirm
check_agreement "$WALG_BIN" "$WALRS_BIN" "walg-deletes"
pg_drop

echo "cross_tool_retention OK"
