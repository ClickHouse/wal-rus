#!/usr/bin/env bash
#
# pgbench_init.sh
#
# Initialize the benchmark database on the SUT (driven over the network):
#   1. create the 'walbench' database if it is absent
#   2. pgbench -i -s "$SCALE" to lay down the standard TPC-B tables
#   3. apply gen_schema.sql to add the WAL-churn / bulk-COPY workload tables
#
# All connection parameters come from libpq env vars so no IPs/passwords are
# hardcoded. Set PGHOST to the SUT private IP before running.
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

# 1. Create the database if it does not already exist. Connect to 'postgres'
#    for the existence check / CREATE DATABASE.
db_exists="$(psql -d postgres -At -c \
    "SELECT 1 FROM pg_database WHERE datname = '${PGDATABASE}'")"

if [[ "${db_exists}" == "1" ]]; then
    echo "==> Database '${PGDATABASE}' already exists; skipping createdb."
else
    echo "==> Creating database '${PGDATABASE}'."
    createdb "${PGDATABASE}"
fi

# 2. Standard pgbench TPC-B tables at the requested scale. Init mode is
#    single-threaded — pgbench rejects -j here ("cannot be used in init mode").
echo "==> pgbench -i -s ${SCALE} (this can take a while at large scale)."
pgbench -i -s "${SCALE}" "${PGDATABASE}"

# 3. WAL-churn workload schema. CHURN_ROWS is passed as the :rows variable.
echo "==> Applying gen_schema.sql with ${CHURN_ROWS} churn rows."
psql -d "${PGDATABASE}" -v ON_ERROR_STOP=1 \
    -v rows="${CHURN_ROWS}" \
    -f "${SCHEMA_SQL}"

echo "==> Initialization complete."
