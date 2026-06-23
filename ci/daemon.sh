#!/usr/bin/env bash
# Adapted from wal-g docker/pg_tests/scripts/tests/daemon_test.sh +
# daemon_client_test.sh. walrus's daemon currently exposes Check / WalPush /
# WalFetch over the wal-g binary wire protocol.
#
# Coverage: bare wire CHECK over nc (verifies binary protocol byte for byte),
# CLI daemon-client check, daemon-driven wal-push, then ordinary wal-fetch
# pulls the segment back.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
sleep 2

WAL=""
for f in "$PGDATA"/pg_wal/*; do
    name=$(basename "$f")
    [[ $name =~ ^[0-9A-F]{24}$ ]] && { WAL=$name; break; }
done
[ -n "$WAL" ] || { echo "no WAL segment found"; exit 1; }

SOCKET="$WORKROOT/wal-daemon.sock"
walrus daemon --socket "$SOCKET" >"$WORKROOT/daemon.log" 2>&1 &
DAEMON_PID=$!
trap 'kill $DAEMON_PID 2>/dev/null || true; cleanup' EXIT

for _ in $(seq 1 30); do
    [ -S "$SOCKET" ] && break
    sleep 0.5
done
[ -S "$SOCKET" ] || { echo "daemon socket never appeared"; cat "$WORKROOT/daemon.log"; exit 1; }

# Bare wire-protocol CHECK: 'C' + 2-byte big-endian length (header+body) + "CHECK".
# Daemon answers 'O' 0x00 0x03 ("OO" after tr -d '\0' matches wal-g's test).
RESP=$(printf 'C\x00\x08CHECK' | nc -U -w 5 "$SOCKET" | tr -d '\0')
case "$RESP" in
    O*) echo "daemon wire CHECK OK ($RESP)" ;;
    *) echo "unexpected daemon response: '$RESP'"; exit 1 ;;
esac

# CLI client paths
walrus daemon-client --socket "$SOCKET" check
walrus daemon-client --socket "$SOCKET" wal-push "$PGDATA/pg_wal/$WAL"

# Verify the push actually landed by fetching via direct CLI (not daemon).
DST="$WORKROOT/fetched_wal"
walrus wal-fetch "$WAL" "$DST"
test -s "$DST" || { echo "wal-fetch produced empty file"; exit 1; }

# And via the daemon-client fetch path
DST2="$WORKROOT/fetched_wal_via_daemon"
walrus daemon-client --socket "$SOCKET" wal-fetch "$WAL" "$DST2"
test -s "$DST2" || { echo "daemon-client wal-fetch produced empty file"; exit 1; }

cmp "$DST" "$DST2"

echo "daemon OK"
