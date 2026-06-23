#!/usr/bin/env bash
# Adapted from wal-g's backup-show pattern: verifies that backup-show emits a
# parseable sentinel for an existing backup (plain + JSON formats).

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_archive_on "$WALRUS_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
walrus backup-push

walrus backup-show LATEST
walrus backup-show LATEST --json \
    | jq -er '
        (["name","sentinel"] - keys) as $top
        | (.sentinel | ["Version","StartTime","FinishTime","Hostname","IsPermanent","PgVersion"] - keys) as $sub
        | if ($top|length) > 0 then error("missing keys: \($top)")
          elif ($sub|length) > 0 then error("missing sentinel keys: \($sub)")
          else "backup_show OK" end'
