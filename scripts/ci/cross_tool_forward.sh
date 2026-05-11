#!/usr/bin/env bash
# Forward bucket interop: wal-rs writes the backup, wal-g reads it.
# Adapted from scripts/vm-cross-tool.sh; mirrors wal-g's
# pg_wale_compatibility_test pattern of "different tool restores, identical
# PG boots".

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_archive_on "$WALRS_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump1.sql"

walrs backup-push
psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
sleep 3

# wal-g must see the backup written by wal-rs
walg backup-list | tee "$WORKROOT/walg-list.txt"
grep -E '^base_' "$WORKROOT/walg-list.txt" || { echo "wal-g cannot see wal-rs backup"; exit 1; }

pg_drop

mkdir -p "$PGDATA"
chmod 700 "$PGDATA"
# wal-g order: backup-fetch <destination> <name>
walg backup-fetch "$PGDATA" LATEST

# Drive recovery with wal-g wal-fetch so we exercise the WAL side too.
pg_recovery_conf "$WALG_BIN wal-fetch %f %p"
pg_start
for _ in $(seq 1 60); do
    if psql -p "$PGPORT" -h "$PGHOST" -tAc 'SELECT pg_is_in_recovery()' postgres 2>/dev/null | grep -qx f; then
        break
    fi
    sleep 1
done

pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump2.sql"
diff "$WORKROOT/dump1.sql" "$WORKROOT/dump2.sql"

echo "cross_tool_forward OK"
