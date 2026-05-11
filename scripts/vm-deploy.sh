#!/usr/bin/env bash
#
# Deploy wal-rs to the VM and run the live PG integration suite.
#
# Usage:
#   scripts/vm-deploy.sh                    # all clusters
#   scripts/vm-deploy.sh -p 5436            # single cluster
#   scripts/vm-deploy.sh -t wal_push_fetch  # single test by name (filter)
#
# Env:
#   VM_HOST   default admin@3.83.51.154
#   VM_KEY    default ~/.ssh/id_aws_erpre
#   VM_DEST   default ~/wal-rs

set -euo pipefail

VM_HOST="${VM_HOST:-admin@3.83.51.154}"
VM_KEY="${VM_KEY:-$HOME/.ssh/id_aws_erpre}"
VM_DEST="${VM_DEST:-wal-rs}"

# Debian PG cluster ports for each major version
declare -A CLUSTERS=(
    [13]=5423
    [14]=5434
    [15]=5435
    [16]=5436
    [17]=5437
    [18]=5433
)

PORTS=()
FILTER=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p) PORTS+=("$2"); shift 2 ;;
        -t) FILTER="$2"; shift 2 ;;
        -h|--help) sed -n '1,15p' "$0"; exit 0 ;;
        *)  echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ ${#PORTS[@]} -eq 0 ]]; then
    PORTS=(5423 5434 5435 5436 5437 5433)
fi

SOURCE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
echo "==> rsync $SOURCE_DIR -> $VM_HOST:$VM_DEST"
rsync -a --delete \
    --exclude='target/' --exclude='.git/' --exclude='*.o' --exclude='*.bc' \
    -e "ssh -i $VM_KEY" \
    "$SOURCE_DIR/" "$VM_HOST:$VM_DEST/"

echo "==> build (release, vm-test feature)"
ssh -i "$VM_KEY" "$VM_HOST" \
    "PATH=\$HOME/.cargo/bin:\$PATH cargo build --manifest-path $VM_DEST/Cargo.toml --release --tests --features vm-test"

for port in "${PORTS[@]}"; do
    echo "==> run vm_live tests against PG port=$port"
    # PG 18 (5433) pg_hba requires SCRAM on TCP; postgres-superuser password
    # is unset & out-of-scope to modify. Route through the Unix socket where
    # pg_hba grants `peer`, by sudo'ing the prebuilt test binary as postgres
    # (cargo test directly would write into postgres's HOME)
    if [[ $port -eq 5433 ]]; then
        ssh -i "$VM_KEY" "$VM_HOST" "bash -s" <<EOF
set -euo pipefail
TEST_BIN=\$(ls -t $VM_DEST/target/release/deps/vm_live-* 2>/dev/null | grep -v '\.d\$' | head -1)
[ -x "\$TEST_BIN" ] || { echo "vm_live binary not found under $VM_DEST/target/release/deps" >&2; exit 1; }
sudo install -m 0755 "\$TEST_BIN" /tmp/wal-rs-vm_live
sudo -n -u postgres env PGHOST=/var/run/postgresql PGPORT=$port PGUSER=postgres PGDATABASE=postgres \
    /tmp/wal-rs-vm_live ${FILTER:-} --nocapture
EOF
    else
        if [[ -n "$FILTER" ]]; then
            ssh -i "$VM_KEY" "$VM_HOST" \
                "PATH=\$HOME/.cargo/bin:\$PATH PGHOST=127.0.0.1 PGPORT=$port PGUSER=postgres PGDATABASE=postgres \
                 cargo test --manifest-path $VM_DEST/Cargo.toml --release --features vm-test --test vm_live -- $FILTER --nocapture"
        else
            ssh -i "$VM_KEY" "$VM_HOST" \
                "PATH=\$HOME/.cargo/bin:\$PATH PGHOST=127.0.0.1 PGPORT=$port PGUSER=postgres PGDATABASE=postgres \
                 cargo test --manifest-path $VM_DEST/Cargo.toml --release --features vm-test --test vm_live -- --nocapture"
        fi
    fi
done
