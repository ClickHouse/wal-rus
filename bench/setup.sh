#!/usr/bin/env bash
#
# setup.sh — bootstrap THIS host as a single-box benchmark System-Under-Test.
#
# Runs the numbered scripts/sut steps in order: mount NVMe (optional) -> install
# PG18 + toolchains -> build wal-g, walrus (from this repo), walg_archive -> init
# the PG18 cluster -> install + stanza pgbackrest -> create the bench role ->
# deploy the sampler -> write wal-g.env -> install both systemd units.
#
# Single-host counterpart of the external AWS fleet's setup_sut.sh: no SSH, no
# source upload (walrus builds straight from this repo), driver == this box.
#
# Run as root: sudo ./setup.sh   (config from ./config.env)
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SUT="${SCRIPT_DIR}/scripts/sut"

# Export everything sourced so the sub-scripts inherit it (BUCKET, creds, etc).
set -a
# shellcheck source=config.env.example
. "${ENV_FILE:-${SCRIPT_DIR}/config.env}"
set +a

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo)." >&2
  exit 1
fi

: "${BUCKET:?set BUCKET in config.env}"
: "${PGUSER:?set PGUSER in config.env}"
: "${PGPASSWORD:?set PGPASSWORD in config.env}"
: "${UPLOAD_CONCURRENCY:?set UPLOAD_CONCURRENCY in config.env}"

# Toolchain owner + pg_hba CIDR. Single-host driver is loopback.
export BUILD_USER="${BUILD_USER:-${SUDO_USER:-ubuntu}}"
export DRIVER_CIDR="${DRIVER_CIDR:-127.0.0.1/32}"

log() { printf '[setup %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

cd "${SUT}"
chmod +x ./*.sh

# 00 is AWS instance-store specific: it formats+mounts a spare NVMe at /dat.
# Skip on a box that already has /dat (or set SKIP_MOUNT=1 to point PGDATA at an
# existing fast disk yourself).
if [[ -n "${SKIP_MOUNT:-}" ]]; then
  log "SKIP_MOUNT set — skipping 00_mount_nvme.sh (ensure /dat exists)"
else
  log "00 mount NVMe";              bash ./00_mount_nvme.sh
fi
log "01 install PG18 + toolchains"; bash ./01_install_pg18.sh
log "02 build wal-g";               bash ./02_build_walg.sh
log "03 build walrus (this repo)";  bash ./03_build_walrus.sh
log "04 build walg_archive";        bash ./04_build_walg_archive.sh
log "10 init PG cluster";           bash ./10_init_pg.sh
log "05 install pgbackrest";        bash ./05_install_pgbackrest.sh

log "create bench role '${PGUSER}'"
PSQL="sudo -u postgres /usr/lib/postgresql/18/bin/psql -p 5432"
if [[ "$(${PSQL} -tAc "SELECT 1 FROM pg_roles WHERE rolname='${PGUSER}'")" == "1" ]]; then
  log "role ${PGUSER} exists; updating password"
  ${PSQL} -c "ALTER ROLE \"${PGUSER}\" LOGIN PASSWORD '${PGPASSWORD}' CREATEDB;"
else
  ${PSQL} -c "CREATE ROLE \"${PGUSER}\" LOGIN PASSWORD '${PGPASSWORD}' CREATEDB;"
fi

log "build bench-tools (bench-sampler + bench-analyze) from ${SCRIPT_DIR}/tools"
build_home="$(getent passwd "${BUILD_USER}" | cut -d: -f6)"
cargo_bin="${build_home}/.cargo/bin/cargo"
sudo -u "${BUILD_USER}" -H bash -c "cd '${SCRIPT_DIR}/tools' && '${cargo_bin}' build --release"

log "deploy bench-sampler + bench-analyze to /usr/local/bin"
install -m 0755 "${SCRIPT_DIR}/tools/target/release/bench-sampler" /usr/local/bin/bench-sampler
install -m 0755 "${SCRIPT_DIR}/tools/target/release/bench-analyze" /usr/local/bin/bench-analyze

log "11 write wal-g.env";                          bash ./11_write_walg_env.sh
log "install both systemd units (via 30, starts walg)"; bash ./30_select_daemon.sh walg

log "bootstrap complete"
/usr/bin/wal-g --version || true
/usr/local/bin/walrus --version || true
echo
echo "Next: bash ${SUT}/40_smoke_test.sh   then   ${SCRIPT_DIR}/matrix.sh"
