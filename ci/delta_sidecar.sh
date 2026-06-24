#!/usr/bin/env bash
# Delta backup via WAL sidecars (WALG_USE_WAL_DELTA). The archiver records a
# `<group>_delta` sidecar per complete 16-segment group; backup-push's delta map
# then folds the whole group instead of re-parsing its raw WAL. Group 0 never
# finalizes (no preceding segment seeds its boundary head, and segment 0 is never
# written), so the first foldable group is 16 — the run must cross ~32 segments
# for `build_delta_map_from_sidecars` to fold rather than fall back to a raw walk.
#
# walrus-only: parent full, spread real heap changes across a full group, take a
# 1-step delta whose map folds the sidecar, then restore the chain + replay.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

# The archive_command subprocess inherits this from the postmaster's env, so
# wal-push records sidecars (pg_archive_on doesn't inline it).
export WALG_USE_WAL_DELTA=1

pg_initdb
pg_archive_on "$WALRUS_BIN"
pg_start

psql -p "$PGPORT" -h "$PGHOST" -c \
    "CREATE TABLE t (id int primary key, v text) WITH (autovacuum_enabled=false)" postgres
psql -p "$PGPORT" -h "$PGHOST" -c \
    "INSERT INTO t SELECT g, repeat('a', 200) FROM generate_series(1, 2000) g" postgres
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres

# parent full (delta detection off)
export WALG_DELTA_MAX_STEPS=0
walrus backup-push "$PGDATA"

# Spread real heap changes across ~40 segments so the complete group 16
# (segments 16-31) carries changes only its sidecar fold can recover. Each
# pg_switch_wal closes a segment, so the archiver records every one and finalizes
# the group's sidecar on its last segment.
for i in $(seq 1 40); do
    psql -p "$PGPORT" -h "$PGHOST" -qc \
        "UPDATE t SET v = repeat(chr(65 + ($i % 26)), 200) WHERE id % 8 = ($i % 8)" postgres
    psql -p "$PGPORT" -h "$PGHOST" -qtAc "SELECT pg_switch_wal()" postgres >/dev/null
done
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump1.sql"

# Group 16's sidecar uploads only once its last segment (...001F = 31) is
# recorded. Poll for it: its presence proves recording finalized a complete
# group, and is the precondition for the fold below.
sidecar=
for _ in $(seq 1 120); do
    sidecar=$(find "$WALG_FILE_PREFIX" -name '000000010000000000000010_delta*' -print -quit)
    [ -n "$sidecar" ] && break
    sleep 1
done
[ -n "$sidecar" ] || {
    echo "FAIL: group-16 delta sidecar never finalized"
    find "$WALG_FILE_PREFIX" -name '*_delta*' || true
    exit 1
}
echo "sidecar: $sidecar"

# 1-step delta off the parent; its map must fold the sidecar, not raw-walk it
export WALG_DELTA_MAX_STEPS=1
walrus backup-push "$PGDATA" 2>"$WORKROOT/delta.log"
cat "$WORKROOT/delta.log"
unset WALG_DELTA_MAX_STEPS

grep -q "delta map:" "$WORKROOT/delta.log" || { echo "FAIL: no delta map built"; exit 1; }
if grep -q "delta sidecars unusable" "$WORKROOT/delta.log"; then
    echo "FAIL: fell back to full raw-WAL walk instead of folding sidecars"
    exit 1
fi
if grep -q "000000010000000000000010_delta absent" "$WORKROOT/delta.log"; then
    echo "FAIL: group-16 sidecar present but raw-walked instead of folded"
    exit 1
fi

walrus backup-list | tee "$WORKROOT/list.txt"
grep -E '_D_' "$WORKROOT/list.txt" || { echo "FAIL: no delta backup written"; exit 1; }

# Restore the chain (parent full + folded-delta increment) and replay. Changes
# in segments 16-31 are reconstructable only through the folded sidecar, so a
# matching dump proves the fold recovered them.
pg_drop
mkdir -p "$PGDATA"
chmod 700 "$PGDATA"
walrus backup-fetch "$PGDATA" LATEST

pg_recovery_conf "$WALRUS_BIN wal-fetch %f %p"
pg_start
for _ in $(seq 1 60); do
    if psql -p "$PGPORT" -h "$PGHOST" -tAc 'SELECT pg_is_in_recovery()' postgres 2>/dev/null | grep -qx f; then
        break
    fi
    sleep 1
done

pg_dumpall -p "$PGPORT" -h "$PGHOST" -f "$WORKROOT/dump2.sql"
diff -I '^\\\(restrict\|unrestrict\) ' "$WORKROOT/dump1.sql" "$WORKROOT/dump2.sql"
pg_drop

echo "delta_sidecar OK"
