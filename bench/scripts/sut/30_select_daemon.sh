#!/usr/bin/env bash
# Select exactly one archive daemon (wal-g or walrus). Stops both units,
# removes the shared socket, installs (if needed) and starts the chosen unit,
# waits for the socket to appear, and prints the active PID/cgroup.
#
# Usage: sudo ./30_select_daemon.sh walg|walrus
set -euo pipefail

CHOICE="${1:-}"
SOCKET="${SOCKET:-/tmp/wal-g}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WALG_UNIT="wal-g.service"
WALRUS_UNIT="walrus.service"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) to control systemd." >&2
  exit 1
fi

case "${CHOICE}" in
  walg)    chosen_unit="${WALG_UNIT}";    src_unit="20_walg.service" ;;
  walrus) chosen_unit="${WALRUS_UNIT}"; src_unit="21_walrus.service" ;;
  *)
    echo "Usage: $0 walg|walrus" >&2
    exit 1
    ;;
esac

echo "=== Installing systemd unit files ==="
install -m 0644 "${SCRIPT_DIR}/20_walg.service"    "/etc/systemd/system/${WALG_UNIT}"
install -m 0644 "${SCRIPT_DIR}/21_walrus.service" "/etc/systemd/system/${WALRUS_UNIT}"
systemctl daemon-reload

echo "=== Stopping both daemons ==="
systemctl stop "${WALG_UNIT}" 2>/dev/null || true
systemctl stop "${WALRUS_UNIT}" 2>/dev/null || true

echo "=== Removing stale socket ${SOCKET} ==="
rm -f "${SOCKET}"

echo "=== Starting ${chosen_unit} (from ${src_unit}) ==="
systemctl start "${chosen_unit}"

echo "=== Waiting for socket ${SOCKET} ==="
for _ in $(seq 1 30); do
  if [[ -S "${SOCKET}" ]]; then
    break
  fi
  sleep 0.5
done
if [[ ! -S "${SOCKET}" ]]; then
  echo "ERROR: socket ${SOCKET} did not appear; recent logs:" >&2
  systemctl status "${chosen_unit}" --no-pager || true
  journalctl -u "${chosen_unit}" -n 30 --no-pager || true
  exit 1
fi

# Point archive_command at the chosen daemon's OWN client (walg_archive's
# extension does not interoperate with the walrus daemon; each tool's own
# daemon-client does). walrus needs the absolute WAL path; wal-g uses %f.
# Best-effort here (PG is up by this point in setup); run_one.sh sets it per cell.
PGDATA_DIR="/dat/18/data"
if [[ "${CHOICE}" == "walg" ]]; then
  archive_cmd="/usr/bin/walg-daemon-client ${SOCKET} wal-push %f"
else
  archive_cmd="/usr/local/bin/walrus daemon-client --socket ${SOCKET} wal-push ${PGDATA_DIR}/%p"
fi
echo "=== Setting archive_command for ${CHOICE} (and clearing archive_library) ==="
# Each ALTER SYSTEM in its own -c (cannot run inside a transaction block).
sudo -u postgres /usr/lib/postgresql/18/bin/psql -p 5432 -tA \
  -c "ALTER SYSTEM SET archive_library = '';" \
  -c "ALTER SYSTEM SET archive_command = '${archive_cmd}';" \
  -c "SELECT pg_reload_conf();" >/dev/null 2>&1 || true

main_pid="$(systemctl show -p MainPID --value "${chosen_unit}")"
echo "=== Active daemon: ${chosen_unit} ==="
echo "MainPID: ${main_pid}"
echo "cgroup:  $(systemctl show -p ControlGroup --value "${chosen_unit}")"
ls -l "${SOCKET}"
systemctl is-active "${chosen_unit}"
