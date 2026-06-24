#!/usr/bin/env bash
# Initialize PG18 cluster and benchmark config
set -euo pipefail

PGDATA="${PGDATA:-/dat/18/data}"
PGBIN="${PGBIN:-/usr/lib/postgresql/18/bin}"
# Driver CIDR allowed over network
DRIVER_CIDR="${DRIVER_CIDR:-}"
PG_LOG="${PG_LOG:-/dat/18/pg.log}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo); it drops to 'postgres' internally." >&2
  exit 1
fi

if [[ -z "${DRIVER_CIDR}" ]]; then
  echo "ERROR: DRIVER_CIDR is required (e.g. DRIVER_CIDR=10.0.0.0/24)." >&2
  exit 1
fi

if [[ ! -x "${PGBIN}/initdb" ]]; then
  echo "ERROR: initdb not found at ${PGBIN}/initdb." >&2
  exit 1
fi

# Drop PGDG default cluster so port 5432 is free
if command -v pg_lsclusters >/dev/null 2>&1 \
  && pg_lsclusters -h 2>/dev/null | awk '{print $1"/"$2}' | grep -qx '18/main'; then
  echo "=== Removing PGDG default cluster 18/main (frees port 5432) ==="
  pg_dropcluster --stop 18 main || true
fi

install -d -o postgres -g postgres -m 0750 "$(dirname "${PGDATA}")"

echo "=== initdb (if needed) at ${PGDATA} ==="
if [[ -f "${PGDATA}/PG_VERSION" ]]; then
  echo "Cluster already initialized; skipping initdb."
else
  sudo -u postgres "${PGBIN}/initdb" -D "${PGDATA}" --data-checksums
fi

echo "=== Writing postgresql.conf ==="
sudo -u postgres tee "${PGDATA}/postgresql.conf" >/dev/null <<EOF
# Managed by 10_init_pg.sh
listen_addresses = '*'
port = 5432
# wal-g.env and sampler expect this socket
unix_socket_directories = '/var/run/postgresql'

wal_level = replica
archive_mode = on
archive_timeout = 60
# Required by walrus --delta-from-wal-summaries
summarize_wal = on
# Real archive_command is set per daemon; placeholder fails safe
archive_command = '/bin/false'

wal_compression = lz4
max_wal_size = 5GB
min_wal_size = 80MB
wal_keep_size = 96MB

logging_collector = off
EOF

echo "=== Ensuring pg_hba.conf allows the driver (${DRIVER_CIDR}, scram) ==="
hba="${PGDATA}/pg_hba.conf"
hba_line="host    all    all    ${DRIVER_CIDR}    scram-sha-256"
if ! grep -qF "${DRIVER_CIDR}" "${hba}"; then
  printf '%s\n' "${hba_line}" | sudo -u postgres tee -a "${hba}" >/dev/null
fi

echo "=== (Re)starting cluster ==="
if sudo -u postgres "${PGBIN}/pg_ctl" -D "${PGDATA}" status >/dev/null 2>&1; then
  sudo -u postgres "${PGBIN}/pg_ctl" -D "${PGDATA}" -l "${PG_LOG}" restart -w -m fast
else
  sudo -u postgres "${PGBIN}/pg_ctl" -D "${PGDATA}" -l "${PG_LOG}" start -w
fi

echo "=== Cluster status ==="
sudo -u postgres "${PGBIN}/pg_ctl" -D "${PGDATA}" status
sudo -u postgres "${PGBIN}/psql" -p 5432 -c "SHOW server_version;" -c "SHOW archive_library;"
