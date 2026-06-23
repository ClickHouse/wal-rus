#!/usr/bin/env bash
# Build walrus (wal-rs) from the in-repo source and install to
# /usr/local/bin/walrus.
#
# bench/ lives inside the wal-rs repo, so the source is right here: build the
# repo working tree directly (no clone, no uploaded tarball). The git SHA is
# read from the repo when present.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# bench/scripts/sut -> repo root is three levels up.
REPO_ROOT="${WALRUS_REPO:-$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd)}"
INSTALL_BIN="/usr/local/bin/walrus"
SHA_FILE="/opt/walbench/walrus.sha"
# User that owns the rustup/cargo toolchain installed by 01_install_pg18.sh.
# Defaults to the sudo invoker (dev box) then ubuntu (provisioned SUT).
BUILD_USER="${BUILD_USER:-${SUDO_USER:-ubuntu}}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) so installs to /usr/local/bin succeed." >&2
  exit 1
fi
# Idempotent: skip the (slow) cargo build if walrus is already installed.
if [[ -z "${FORCE_REBUILD:-}" && -x "${INSTALL_BIN}" ]]; then
  echo "walrus already installed; skipping build (FORCE_REBUILD=1 to rebuild)."
  exit 0
fi
if [[ ! -f "${REPO_ROOT}/Cargo.toml" ]]; then
  echo "ERROR: wal-rs source not found at ${REPO_ROOT} (no Cargo.toml)." >&2
  exit 1
fi
if ! id "${BUILD_USER}" >/dev/null 2>&1; then
  echo "ERROR: build user '${BUILD_USER}' does not exist." >&2
  exit 1
fi
build_home="$(getent passwd "${BUILD_USER}" | cut -d: -f6)"
cargo_bin="${build_home}/.cargo/bin/cargo"
if [[ ! -x "${cargo_bin}" ]]; then
  echo "ERROR: cargo not found at ${cargo_bin} (run 01_install_pg18.sh first)." >&2
  exit 1
fi

WALRUS_SHA="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
echo "=== Building walrus from ${REPO_ROOT} (SHA ${WALRUS_SHA}) ==="

# cargo runs as BUILD_USER, which must own the tree (target/ is gitignored, so
# building in place is fine). The binary is installed by root afterwards.
echo "=== cargo build --release ==="
sudo -u "${BUILD_USER}" -H bash -c "cd '${REPO_ROOT}' && '${cargo_bin}' build --release"

echo "=== Installing to ${INSTALL_BIN} ==="
install -m 0755 "${REPO_ROOT}/target/release/walrus" "${INSTALL_BIN}"

mkdir -p "$(dirname "${SHA_FILE}")"
printf '%s\n' "${WALRUS_SHA}" > "${SHA_FILE}"
echo "Recorded SHA to ${SHA_FILE}"

echo "=== Installed ==="
ls -l "${INSTALL_BIN}"
"${INSTALL_BIN}" --version
