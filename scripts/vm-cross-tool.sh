#!/usr/bin/env bash
#
# Bidirectional cross-tool compat check between wal-rs and wal-g on the
# VM. Requires `/tmp/wal-g` binary to be installed on the VM and a
# trust-auth PG cluster reachable at $PGPORT
#
# Usage:
#   scripts/vm-cross-tool.sh                 # default PG 16 cluster
#   PGPORT=5436 scripts/vm-cross-tool.sh     # explicit port
#
# Env:
#   VM_HOST   default admin@3.83.51.154
#   VM_KEY    default ~/.ssh/id_aws_erpre
#   PGPORT    default 5436 (PG 16 on the standard VM layout)

set -euo pipefail

VM_HOST="${VM_HOST:-admin@3.83.51.154}"
VM_KEY="${VM_KEY:-$HOME/.ssh/id_aws_erpre}"
PGPORT="${PGPORT:-5436}"

# Build + deploy (mirrors vm-deploy.sh shape)
SOURCE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
echo "==> rsync"
rsync -a --delete \
    --exclude='target/' --exclude='.git/' --exclude='*.o' --exclude='*.bc' \
    -e "ssh -i $VM_KEY" \
    "$SOURCE_DIR/" "$VM_HOST:wal-rs/"

echo "==> build release"
ssh -i "$VM_KEY" "$VM_HOST" \
    "PATH=\$HOME/.cargo/bin:\$PATH cargo build --manifest-path wal-rs/Cargo.toml --release --bin wal-rs"

echo "==> cross-tool roundtrip against PG=$PGPORT"
ssh -i "$VM_KEY" "$VM_HOST" \
    "PGPORT=$PGPORT bash -s" <<'EOF'
set -euo pipefail

DIR_FORWARD=$(mktemp -d /tmp/cross-fwd-XXXXX)
DIR_REVERSE=$(mktemp -d /tmp/cross-rev-XXXXX)
sudo chmod 777 "$DIR_REVERSE"
trap 'sudo rm -rf "$DIR_FORWARD" "$DIR_REVERSE"' EXIT

export WALG_COMPRESSION_METHOD=zstd
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres

# Forward: wal-rs writes, wal-g reads
export WALG_FILE_PREFIX=$DIR_FORWARD
echo "-- forward: wal-rs backup-push --"
~/wal-rs/target/release/wal-rs backup-push >/tmp/fwd_push.log 2>&1
echo "-- forward: wal-g backup-list --"
/tmp/wal-g backup-list
echo "-- forward: wal-g backup-fetch --"
mkdir "$DIR_FORWARD/restore"
/tmp/wal-g backup-fetch "$DIR_FORWARD/restore" LATEST >/tmp/fwd_fetch.log 2>&1
test -f "$DIR_FORWARD/restore/PG_VERSION" && echo "forward OK"

# Reverse: wal-g writes, wal-rs reads
export WALG_FILE_PREFIX=$DIR_REVERSE
echo "-- reverse: wal-g backup-push --"
sudo -E -u postgres /tmp/wal-g backup-push "/var/lib/postgresql/$(psql -p $PGPORT -tAc 'SHOW server_version_num' | head -c2)/main" >/tmp/rev_push.log 2>&1 || true
# fall back if version-num probe failed
if ! ls $DIR_REVERSE/basebackups_005/ >/dev/null 2>&1; then
    # try common ports → directories
    case "$PGPORT" in
        5423) PGDATADIR=/var/lib/postgresql/13/main ;;
        5434) PGDATADIR=/var/lib/postgresql/14/main ;;
        5435) PGDATADIR=/var/lib/postgresql/15/main ;;
        5436) PGDATADIR=/var/lib/postgresql/16/main ;;
        5437) PGDATADIR=/var/lib/postgresql/17/main ;;
        5433) PGDATADIR=/var/lib/postgresql/18/main ;;
        *)    echo "unknown PGPORT=$PGPORT — bailing" >&2; exit 1 ;;
    esac
    sudo -E -u postgres /tmp/wal-g backup-push "$PGDATADIR" >/tmp/rev_push.log 2>&1
fi
echo "-- reverse: wal-rs backup-list --"
~/wal-rs/target/release/wal-rs backup-list
echo "-- reverse: wal-rs backup-fetch --"
mkdir -p "$DIR_REVERSE/restore"
~/wal-rs/target/release/wal-rs backup-fetch LATEST "$DIR_REVERSE/restore" >/tmp/rev_fetch.log 2>&1
test -f "$DIR_REVERSE/restore/PG_VERSION" && echo "reverse OK"

echo "-- reverse: wal-rs backup-show --"
~/wal-rs/target/release/wal-rs backup-show LATEST | head -15
EOF
