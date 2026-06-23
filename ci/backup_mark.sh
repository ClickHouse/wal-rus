#!/usr/bin/env bash
# Adapted from wal-g docker/pg_tests/scripts/tests/backup_mark_permanent_test.sh
# and backup_mark_impermanent_test.sh. walrus doesn't yet implement delete, so
# this verifies only the sentinel mutation (IsPermanent toggle) rather than
# delete-retention interaction.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_archive_on "$WALRUS_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres

walrus backup-push
walrus backup-list

permanent_of() {
    # is_permanent is exposed as snake_case in backup-list --json
    walrus backup-list --json \
        | jq -r 'if .[0].is_permanent then "true" else "false" end'
}

initial=$(permanent_of)
walrus backup-mark LATEST
marked=$(permanent_of)
walrus backup-mark LATEST --impermanent
unmarked=$(permanent_of)

[ "$initial"  = "false" ] || { echo "expected initial=false, got $initial";  exit 1; }
[ "$marked"   = "true"  ] || { echo "expected marked=true, got $marked";     exit 1; }
[ "$unmarked" = "false" ] || { echo "expected unmarked=false, got $unmarked"; exit 1; }

echo "backup_mark OK"
