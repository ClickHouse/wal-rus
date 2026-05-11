#!/usr/bin/env bash
# Mirror of wal-g docker/pg_tests/scripts/tests/test_functions/test_full_backup.sh
# Full archive + backup-push + backup-fetch + WAL replay roundtrip with wal-rs only.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_archive_on "$WALRS_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump1.sql"

# Block until at least one segment is archived; otherwise WAL replay after
# fetch has nothing to consume.
psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
sleep 5

walrs backup-push
walrs backup-list

pg_drop

mkdir -p "$PGDATA"
chmod 700 "$PGDATA"
walrs backup-fetch LATEST "$PGDATA"

pg_recovery_conf "$WALRS_BIN wal-fetch %f %p"
pg_start
# Wait for recovery exit
for _ in $(seq 1 60); do
    if psql -p "$PGPORT" -h "$PGHOST" -tAc 'SELECT pg_is_in_recovery()' postgres 2>/dev/null | grep -qx f; then
        break
    fi
    sleep 1
done

pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump2.sql"
diff "$WORKROOT/dump1.sql" "$WORKROOT/dump2.sql"

echo "full_backup OK"
