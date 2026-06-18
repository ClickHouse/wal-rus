#!/usr/bin/env bash
# SCRAM-SHA-256 auth against a live cluster. backup-push connects over TCP with
# PGPASSWORD, driving walross's SASL/SCRAM client. Asserts the right password
# authenticates and a wrong one fails with an auth error.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

PW='walross-secret'

pg_initdb
pg_replication_on
pg_listen_tcp
# Hash new passwords with SCRAM, and require SCRAM on TCP while keeping the
# local socket on trust for administration. First-match-wins, so the scram
# host lines must replace (not follow) initdb's default host trust lines.
cat >>"$PGDATA/postgresql.conf" <<EOF
password_encryption = 'scram-sha-256'
EOF
cat >"$PGDATA/pg_hba.conf" <<EOF
local all all trust
host all all 127.0.0.1/32 scram-sha-256
host replication all 127.0.0.1/32 scram-sha-256
EOF
pg_start

# Set the password over the local trust socket (PGHOST is the socket dir)
psql -p "$PGPORT" -h "$PGHOST" -c "ALTER ROLE \"$PGUSER\" PASSWORD '$PW'" postgres

echo "== correct password: must authenticate =="
PGHOST=127.0.0.1 PGPASSWORD="$PW" walross backup-push

echo "== wrong password: must fail with an auth error =="
if PGHOST=127.0.0.1 PGPASSWORD='nope' walross backup-push 2>"$WORKROOT/wrong.err"; then
    echo "FAIL: backup-push succeeded with a wrong password"
    exit 1
fi
grep -qiE 'auth|password|scram|sasl' "$WORKROOT/wrong.err" \
    || { echo "FAIL: expected an auth error, got:"; cat "$WORKROOT/wrong.err"; exit 1; }

echo "scram_auth OK"
