#!/usr/bin/env bash
#
# pgbench_init.sh
#
# Initialize benchmark DB over network
#
# Connection comes from libpq env vars
#
# Env vars (with defaults):
#   PGHOST      (required) SUT private IP / host
#   PGPORT      5432
#   PGUSER      (required) login role
#   PGPASSWORD  (required) password for PGUSER (or use ~/.pgpass)
#   PGDATABASE  walbench   target database
#   SCALE       5000       pgbench scaling factor (-s)
#   CHURN_ROWS  2000000    rows seeded into wal_churn by gen_schema.sql
#   PGBENCH_INIT_JOBS  8   parallel client jobs for pgbench load phase (-j)

set -euo pipefail

PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-walbench}"
SCALE="${SCALE:-5000}"
CHURN_ROWS="${CHURN_ROWS:-2000000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_SQL="${SCRIPT_DIR}/gen_schema.sql"

: "${PGHOST:?Set PGHOST to the SUT private IP}"
: "${PGUSER:?Set PGUSER to the login role}"

export PGPORT

if [[ ! -f "${SCHEMA_SQL}" ]]; then
    echo "FATAL: schema file not found: ${SCHEMA_SQL}" >&2
    exit 1
fi

echo "==> Target: ${PGUSER}@${PGHOST}:${PGPORT}, database '${PGDATABASE}', scale ${SCALE}"

# Create database if absent
db_exists="$(psql -d postgres -At -c \
    "SELECT 1 FROM pg_database WHERE datname = '${PGDATABASE}'")"

if [[ "${db_exists}" == "1" ]]; then
    echo "==> Database '${PGDATABASE}' already exists; skipping createdb."
else
    echo "==> Creating database '${PGDATABASE}'."
    createdb "${PGDATABASE}"
fi

# Standard pgbench TPC-B tables
echo "==> pgbench -i -s ${SCALE} (this can take a while at large scale)."
pgbench -i -s "${SCALE}" "${PGDATABASE}"

# WAL-churn workload schema
echo "==> Applying gen_schema.sql with ${CHURN_ROWS} churn rows."
psql -d "${PGDATABASE}" -v ON_ERROR_STOP=1 \
    -v rows="${CHURN_ROWS}" \
    -f "${SCHEMA_SQL}"

echo "==> Initialization complete."
