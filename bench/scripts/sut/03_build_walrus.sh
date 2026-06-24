#!/usr/bin/env bash
# Build walrus from in-repo source, or from a shipped source tarball
# (WALRUS_SRC_TARBALL) when the SUT has no git checkout.
#
# Records git SHA when present
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
INSTALL_BIN="/usr/local/bin/walrus"
SHA_FILE="/opt/walbench/walrus.sha"
# User owning rustup/cargo toolchain
BUILD_USER="${BUILD_USER:-${SUDO_USER:-ubuntu}}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) so installs to /usr/local/bin succeed." >&2
  exit 1
fi
# Skip build when installed, unless FORCE_REBUILD=1
if [[ -z "${FORCE_REBUILD:-}" && -x "${INSTALL_BIN}" ]]; then
  echo "walrus already installed; skipping build (FORCE_REBUILD=1 to rebuild)."
  exit 0
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

# Source: shipped tarball (fresh SUT) or in-repo tree. make_source_tarball.sh
# builds the tarball via git archive; the commit id rides in its pax header and
# is recovered here, so provenance survives a transfer with no .git.
if [[ -n "${WALRUS_SRC_TARBALL:-}" ]]; then
  if [[ ! -f "${WALRUS_SRC_TARBALL}" ]]; then
    echo "ERROR: WALRUS_SRC_TARBALL=${WALRUS_SRC_TARBALL} not found." >&2
    exit 1
  fi
  SRC_DIR="${WALRUS_SRC_DIR:-/opt/walbench/src}"
  echo "=== Unpacking ${WALRUS_SRC_TARBALL} -> ${SRC_DIR} ==="
  rm -rf "${SRC_DIR}"
  mkdir -p "${SRC_DIR}"
  tar -C "${SRC_DIR}" -xzf "${WALRUS_SRC_TARBALL}"
  # --prefix=walrus/ nests the tree; fall back when archived without it
  REPO_ROOT="${SRC_DIR}/walrus"
  [[ -f "${REPO_ROOT}/Cargo.toml" ]] || REPO_ROOT="${SRC_DIR}"
  WALRUS_SHA="$(gzip -dc "${WALRUS_SRC_TARBALL}" | git get-tar-commit-id 2>/dev/null || echo unknown)"
  # Extracted as root; build user needs to write target/
  chown -R "${BUILD_USER}:${BUILD_USER}" "${SRC_DIR}"
else
  # bench/scripts/sut -> repo root
  REPO_ROOT="${WALRUS_REPO:-$(cd -- "${SCRIPT_DIR}/../../.." >/dev/null 2>&1 && pwd)}"
  WALRUS_SHA="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
fi
if [[ ! -f "${REPO_ROOT}/Cargo.toml" ]]; then
  echo "ERROR: wal-rs source not found at ${REPO_ROOT} (no Cargo.toml)." >&2
  exit 1
fi

echo "=== Building walrus from ${REPO_ROOT} (SHA ${WALRUS_SHA}) ==="

# Build as BUILD_USER, install binary as root
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
