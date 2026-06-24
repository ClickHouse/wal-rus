#!/usr/bin/env bash
# Cross-tool libsodium-encrypted bucket interop (full backups; delta interop is
# covered by cross_tool_delta.sh). A 32-byte raw key (default transform = none)
# is read by both tools from WALG_LIBSODIUM_KEY. Forward: walrus encrypts, wal-g
# restores. Reverse: wal-g encrypts, walrus restores.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

# Exported so the archive_command + restore_command subprocesses inherit it too
export WALG_LIBSODIUM_KEY='walrus_test_libsodium_key_32byte'

cross_roundtrip "$WALRUS_BIN" "$WALG_BIN" "$PGDATA"
echo "cross_tool_encryption forward OK"

bucket_reset
cross_roundtrip "$WALG_BIN" "$WALRUS_BIN" "$PGDATA"
echo "cross_tool_encryption reverse OK"

echo "cross_tool_encryption OK"
