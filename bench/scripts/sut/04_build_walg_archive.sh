#!/usr/bin/env bash
# Build and install the walg_archive PostgreSQL archive module for PG18 via PGXS.
set -euo pipefail

ARCHIVE_COMMIT="${ARCHIVE_COMMIT:-ce0d160b8503f98c179646e38cd24b9351ec8c0a}"
ARCHIVE_REPO="${ARCHIVE_REPO:-https://github.com/wal-g/walg_archive}"
SRC_DIR="${SRC_DIR:-/opt/walbench/src/walg_archive}"
PG_CONFIG="${PG_CONFIG:-/usr/lib/postgresql/18/bin/pg_config}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) so 'make install' succeeds." >&2
  exit 1
fi

if [[ ! -x "${PG_CONFIG}" ]]; then
  echo "ERROR: pg_config not found at ${PG_CONFIG} (install postgresql-server-dev-18)." >&2
  exit 1
fi

# Idempotent: skip if the module is already installed for this PG.
_pkglibdir="$("${PG_CONFIG}" --pkglibdir)"
if [[ -z "${FORCE_REBUILD:-}" && -f "${_pkglibdir}/walg_archive.so" ]]; then
  echo "walg_archive.so already installed at ${_pkglibdir}; skipping build (FORCE_REBUILD=1 to rebuild)."
  exit 0
fi

echo "=== Fetching walg_archive @ ${ARCHIVE_COMMIT} ==="
mkdir -p "${SRC_DIR}"
cd "${SRC_DIR}"
if [[ ! -d .git ]]; then
  git init -q
  git remote add origin "${ARCHIVE_REPO}"
fi
git remote set-url origin "${ARCHIVE_REPO}"
git fetch origin --depth 1 "${ARCHIVE_COMMIT}"
git reset --hard FETCH_HEAD

echo "=== Building (PGXS, PG_CONFIG=${PG_CONFIG}) ==="
make clean USE_PGXS=1 PG_CONFIG="${PG_CONFIG}" 2>/dev/null || true
make USE_PGXS=1 PG_CONFIG="${PG_CONFIG}"

echo "=== Installing ==="
make USE_PGXS=1 PG_CONFIG="${PG_CONFIG}" install

pkglibdir="$("${PG_CONFIG}" --pkglibdir)"
echo "=== Installed module ==="
ls -l "${pkglibdir}/walg_archive.so"
