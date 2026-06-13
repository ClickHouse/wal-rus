#!/usr/bin/env bash
# Cross-tool libsodium-encrypted bucket interop (full backups; delta is blocked
# on the streamer rewrite). A 32-byte raw key (default transform = none) is read
# by both tools from WALG_LIBSODIUM_KEY. Forward: wal-rs encrypts, wal-g
# restores. Reverse: wal-g encrypts, wal-rs restores.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

# Exported so the archive_command + restore_command subprocesses inherit it too
export WALG_LIBSODIUM_KEY='walrs_test_libsodium_key_32bytes'

cross_roundtrip "$WALRS_BIN" "$WALG_BIN"
echo "cross_tool_encryption forward OK"

bucket_reset
cross_roundtrip "$WALG_BIN" "$WALRS_BIN"
echo "cross_tool_encryption reverse OK"

echo "cross_tool_encryption OK"
