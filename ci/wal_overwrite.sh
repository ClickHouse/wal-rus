#!/usr/bin/env bash
# Adapted from the test_wal_overwrites() block in wal-g's test_full_backup.sh.
# Covers:
#   - .history files always content-compared (idempotent even with
#     WALG_PREVENT_WAL_OVERWRITE=false)
#   - regular WAL segments only content-compared when
#     WALG_PREVENT_WAL_OVERWRITE=true
#   - mismatched content rejected in both modes for .history

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_start

mkdir -p "$PGDATA/pg_wal"

# Case 1: WALG_PREVENT_WAL_OVERWRITE=false — .history still compares.
export WALG_PREVENT_WAL_OVERWRITE=false

echo test > "$PGDATA/pg_wal/test_file.history"
walrus wal-push "$PGDATA/pg_wal/test_file.history"
# Re-push identical content — must succeed
walrus wal-push "$PGDATA/pg_wal/test_file.history"

echo test1 > "$PGDATA/pg_wal/test_file.history"
if walrus wal-push "$PGDATA/pg_wal/test_file.history"; then
    echo "ERROR: divergent .history push succeeded with overwrite=false"
    exit 1
fi

# Case 2: WALG_PREVENT_WAL_OVERWRITE=true — regular WAL names also compare.
# Use a plausible-looking WAL segment name (24 hex chars) since walrus rejects
# anything that isn't a wal segment or .history.
export WALG_PREVENT_WAL_OVERWRITE=true
SEG="$PGDATA/pg_wal/000000010000000000000099"
dd if=/dev/zero bs=1 count=16 of="$SEG" status=none
walrus wal-push "$SEG"
# identical re-push
walrus wal-push "$SEG"

dd if=/dev/urandom bs=1 count=16 of="$SEG" status=none
if walrus wal-push "$SEG"; then
    echo "ERROR: divergent WAL push succeeded with overwrite=true"
    exit 1
fi

echo "wal_overwrite OK"
