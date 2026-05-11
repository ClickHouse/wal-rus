#!/usr/bin/env bash
# Adapted from wal-g's backup-show pattern: verifies that backup-show emits a
# parseable sentinel for an existing backup (plain + JSON formats).

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_archive_on "$WALRS_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
walrs backup-push

walrs backup-show LATEST
walrs backup-show LATEST --json \
    | python3 -c '
import sys, json
o = json.load(sys.stdin)
assert "name" in o, o
assert "sentinel" in o, o
s = o["sentinel"]
# PascalCase keys mirror wal-g on-disk format
for k in ("Version", "StartTime", "FinishTime", "Hostname", "IsPermanent", "PgVersion"):
    assert k in s, (k, s)
print("backup_show OK")
'
