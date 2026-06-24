#!/usr/bin/env bash
# Cross-tool streaming BASE_BACKUP interop. Both tools take a full backup over a
# replication connection (no PGDATA arg → wal-g's remote/streaming path, walrus's
# BASE_BACKUP path), the other restores and replays, dumps compared. Asserts the
# streamed tar layout (part_NNN.tar + pg_control.tar tee) is mutually readable.
# Full backups only: delta is unavailable in either tool's streaming mode.
# Forward: walrus streams, wal-g restores. Reverse: wal-g streams, walrus restores.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

# No source arg → cross_roundtrip drives both tools via streaming BASE_BACKUP
cross_roundtrip "$WALRUS_BIN" "$WALG_BIN"
echo "cross_tool_stream forward OK"

bucket_reset
cross_roundtrip "$WALG_BIN" "$WALRUS_BIN"
echo "cross_tool_stream reverse OK"

echo "cross_tool_stream OK"
