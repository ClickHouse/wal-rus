#!/usr/bin/env bash
# TLS replication handshake against a test-CA server cert. backup-push opens a
# replication connection over TCP, so PGSSLMODE drives wal-rs's maybe_upgrade.
# Asserts: verify-full + verify-ca succeed with the right root; a wrong root
# fails closed; require succeeds without verification.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

CERTS="$WORKROOT/certs"
mkdir -p "$CERTS"

# Test CA + a second (wrong) CA for the negative case
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" -subj '/CN=wal-rs-test-ca'
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$CERTS/wrong.key" -out "$CERTS/wrong.crt" -subj '/CN=wal-rs-wrong-ca'

# Server cert with CN + SAN = 127.0.0.1 (verify-full checks the IP SAN)
openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/server.key" -out "$CERTS/server.csr" -subj '/CN=127.0.0.1'
openssl x509 -req -in "$CERTS/server.csr" \
    -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" -CAcreateserial \
    -out "$CERTS/server.crt" -days 2 \
    -extfile <(printf 'subjectAltName=IP:127.0.0.1\n')
# PG refuses a group/world-readable key
chmod 600 "$CERTS/server.key"

pg_initdb
pg_replication_on
pg_hba_replication
pg_listen_tcp
cat >>"$PGDATA/postgresql.conf" <<EOF
ssl = on
ssl_cert_file = '$CERTS/server.crt'
ssl_key_file = '$CERTS/server.key'
ssl_ca_file = '$CERTS/ca.crt'
EOF
# Replication over TCP loopback with trust (auth is orthogonal to the TLS test)
echo 'host replication all 127.0.0.1/32 trust' >>"$PGDATA/pg_hba.conf"
pg_start

export PGHOST=127.0.0.1

echo "== verify-full with correct root: must succeed =="
PGSSLMODE=verify-full PGSSLROOTCERT="$CERTS/ca.crt" walrs backup-push

echo "== verify-ca with correct root: must succeed =="
PGSSLMODE=verify-ca PGSSLROOTCERT="$CERTS/ca.crt" walrs backup-push

echo "== verify-full with WRONG root: must fail closed =="
if PGSSLMODE=verify-full PGSSLROOTCERT="$CERTS/wrong.crt" walrs backup-push 2>"$WORKROOT/wrong.err"; then
    echo "FAIL: backup-push succeeded against an untrusted server cert"
    exit 1
fi
grep -qiE 'certificate|tls|handshake|verif' "$WORKROOT/wrong.err" \
    || { echo "FAIL: expected a TLS/cert error, got:"; cat "$WORKROOT/wrong.err"; exit 1; }

echo "== require without a root: must succeed (no verification) =="
PGSSLMODE=require walrs backup-push

echo "tls_replication OK"
