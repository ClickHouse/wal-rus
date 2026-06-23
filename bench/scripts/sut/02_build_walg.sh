#!/usr/bin/env bash
# Build wal-g at the pinned commit and install the PG binary + daemon client.
set -euo pipefail

WALG_COMMIT="${WALG_COMMIT:-f81943e64bdf97aa66f6c52fec55114703f97af7}"
WALG_REPO="${WALG_REPO:-https://github.com/wal-g/wal-g.git}"
SRC_DIR="${SRC_DIR:-/opt/walbench/src/wal-g}"
GOBIN_DIR="/usr/bin"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) so installs to /usr/bin succeed." >&2
  exit 1
fi

# Idempotent: skip the rebuild if both binaries are already installed.
if [[ -z "${FORCE_REBUILD:-}" && -x /usr/bin/wal-g && -x /usr/bin/walg-daemon-client ]]; then
  echo "wal-g already installed ($(/usr/bin/wal-g --version 2>/dev/null | head -1)); skipping (FORCE_REBUILD=1 to rebuild)."
  exit 0
fi

export PATH="/usr/local/go/bin:${PATH}"
export GOEXPERIMENT=jsonv2

if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: go not found on PATH (expected /usr/local/go/bin/go)." >&2
  exit 1
fi
echo "Using $(go version)"

echo "=== Fetching wal-g @ ${WALG_COMMIT} ==="
mkdir -p "${SRC_DIR}"
cd "${SRC_DIR}"
if [[ ! -d .git ]]; then
  git init -q
  git remote add origin "${WALG_REPO}"
fi
git remote set-url origin "${WALG_REPO}"
git fetch origin --depth 1 "${WALG_COMMIT}"
git reset --hard FETCH_HEAD

echo "=== make deps ==="
make deps

echo "=== make pg_build ==="
make pg_build

echo "=== make pg_install (GOBIN=${GOBIN_DIR}) ==="
GOBIN="${GOBIN_DIR}" make pg_install

echo "=== make build_client ==="
make build_client
cp bin/walg-daemon-client "${GOBIN_DIR}/walg-daemon-client"
chmod 0755 "${GOBIN_DIR}/walg-daemon-client"

echo "=== Installed ==="
ls -l "${GOBIN_DIR}/wal-g" "${GOBIN_DIR}/walg-daemon-client"
"${GOBIN_DIR}/wal-g" --version
