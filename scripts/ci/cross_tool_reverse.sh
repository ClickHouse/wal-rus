#!/usr/bin/env bash
# Reverse bucket interop: wal-g writes the backup, wal-rs reads it.
# Mirrors the second half of scripts/vm-cross-tool.sh.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_archive_on "$WALG_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump1.sql"

walg backup-push "$PGDATA"
psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
sleep 3

walrs backup-list | tee "$WORKROOT/walrs-list.txt"
grep -E '^base_' "$WORKROOT/walrs-list.txt" || { echo "wal-rs cannot see wal-g backup"; exit 1; }

walrs backup-show LATEST

pg_drop

mkdir -p "$PGDATA"
chmod 700 "$PGDATA"
walrs backup-fetch LATEST "$PGDATA"

pg_recovery_conf "$WALRS_BIN wal-fetch %f %p"
pg_start
for _ in $(seq 1 60); do
    if psql -p "$PGPORT" -h "$PGHOST" -tAc 'SELECT pg_is_in_recovery()' postgres 2>/dev/null | grep -qx f; then
        break
    fi
    sleep 1
done

pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump2.sql"
diff -I '^\\\(restrict\|unrestrict\) ' "$WORKROOT/dump1.sql" "$WORKROOT/dump2.sql"

echo "cross_tool_reverse OK"
