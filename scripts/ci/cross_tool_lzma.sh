#!/usr/bin/env bash
# Cross-tool lzma container interop. wal-g uses xz/lzma; this asserts the
# on-disk format is identical in both directions. Forward: walross writes lzma,
# wal-g restores. Reverse: wal-g writes lzma, walross restores.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

# lib.sh respects a pre-set method; the archive_command inlines it too
export WALG_COMPRESSION_METHOD=lzma

cross_roundtrip "$WALROSS_BIN" "$WALG_BIN"
echo "cross_tool_lzma forward OK"

bucket_reset
cross_roundtrip "$WALG_BIN" "$WALROSS_BIN"
echo "cross_tool_lzma reverse OK"

echo "cross_tool_lzma OK"
