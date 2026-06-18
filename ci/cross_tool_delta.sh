#!/usr/bin/env bash
# Cross-tool delta-backup interop: one tool writes a full + a 1-step wi1 delta,
# the other restores the whole chain (parent + increment) and replays. Forward:
# wal-rs writes, wal-g restores. Reverse: wal-g writes, wal-rs restores.
# Exercises the shared wi1 increment format + IncrementFrom chain discovery.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

cross_delta_roundtrip "$WALRS_BIN" "$WALG_BIN"
echo "cross_tool_delta forward OK"

bucket_reset
cross_delta_roundtrip "$WALG_BIN" "$WALRS_BIN"
echo "cross_tool_delta reverse OK"

echo "cross_tool_delta OK"
