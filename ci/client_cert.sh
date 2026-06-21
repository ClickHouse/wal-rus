#!/usr/bin/env bash
# Client certificate auth (mutual TLS) over a replication connection. PG's `cert`
# hba method demands a client cert whose CN maps to the PG user; walrus presents
# it from PGSSLCERT/PGSSLKEY. Asserts: a valid client cert succeeds; omitting it
# fails closed; a cert signed by an untrusted CA is rejected by the server.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

CERTS="$WORKROOT/certs"
mkdir -p "$CERTS"

# One CA signs both the server cert and the trusted client cert; a second CA
# mints an untrusted client cert for the negative case.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" -subj '/CN=walrus-test-ca'
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$CERTS/rogue-ca.key" -out "$CERTS/rogue-ca.crt" -subj '/CN=walrus-rogue-ca'

# Server cert with SAN=127.0.0.1 so verify-full passes against the IP
openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/server.key" -out "$CERTS/server.csr" -subj '/CN=127.0.0.1'
openssl x509 -req -in "$CERTS/server.csr" \
    -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" -CAcreateserial \
    -out "$CERTS/server.crt" -days 2 \
    -extfile <(printf 'subjectAltName=IP:127.0.0.1\n')
chmod 600 "$CERTS/server.key"

# client extensions force an X.509 v3 cert: webpki (rustls client auth) rejects
# v1 certs with UnsupportedCertVersion, and openssl < 3.5 emits v1 from a bare
# `x509 -req` (no default extensions)
CLIENT_EXT='basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth'

# Client cert CN must equal the PG role the `cert` method maps to (our PGUSER)
openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/client.key" -out "$CERTS/client.csr" -subj "/CN=$PGUSER"
openssl x509 -req -in "$CERTS/client.csr" \
    -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" -CAcreateserial \
    -out "$CERTS/client.crt" -days 2 \
    -extfile <(printf '%s\n' "$CLIENT_EXT")
chmod 600 "$CERTS/client.key"

# Rogue client cert: same CN, signed by the untrusted CA
openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/rogue.key" -out "$CERTS/rogue.csr" -subj "/CN=$PGUSER"
openssl x509 -req -in "$CERTS/rogue.csr" \
    -CA "$CERTS/rogue-ca.crt" -CAkey "$CERTS/rogue-ca.key" -CAcreateserial \
    -out "$CERTS/rogue.crt" -days 2 \
    -extfile <(printf '%s\n' "$CLIENT_EXT")
chmod 600 "$CERTS/rogue.key"

pg_initdb
pg_replication_on
pg_listen_tcp
cat >>"$PGDATA/postgresql.conf" <<EOF
ssl = on
ssl_cert_file = '$CERTS/server.crt'
ssl_key_file = '$CERTS/server.key'
ssl_ca_file = '$CERTS/ca.crt'
EOF
# pg_hba is first-match: drop initdb's default `host replication ... trust`
# lines so the cert rule is the only one matching TCP replication. `cert`
# requires a client cert signed by ssl_ca_file whose CN maps to the role.
grep -vE '^[[:space:]]*host(ssl|nossl)?[[:space:]]+replication' \
    "$PGDATA/pg_hba.conf" >"$WORKROOT/pg_hba.conf"
mv "$WORKROOT/pg_hba.conf" "$PGDATA/pg_hba.conf"
echo "hostssl replication all 127.0.0.1/32 cert" >>"$PGDATA/pg_hba.conf"
pg_start

export PGHOST=127.0.0.1
export PGSSLMODE=verify-full
export PGSSLROOTCERT="$CERTS/ca.crt"

echo "== valid client cert: must succeed =="
PGSSLCERT="$CERTS/client.crt" PGSSLKEY="$CERTS/client.key" walrus backup-push

echo "== no client cert: must fail closed (server demands one) =="
if walrus backup-push 2>"$WORKROOT/nocert.err"; then
    echo "FAIL: backup-push succeeded without a client cert"
    exit 1
fi
grep -qiE 'cert|alert|fatal|auth|ssl|tls|handshake|verif' "$WORKROOT/nocert.err" \
    || { echo "FAIL: expected an auth/cert error, got:"; cat "$WORKROOT/nocert.err"; exit 1; }

echo "== client cert from untrusted CA: must fail closed =="
if PGSSLCERT="$CERTS/rogue.crt" PGSSLKEY="$CERTS/rogue.key" walrus backup-push 2>"$WORKROOT/rogue.err"; then
    echo "FAIL: backup-push succeeded with an untrusted client cert"
    exit 1
fi
grep -qiE 'cert|alert|fatal|auth|ssl|tls|handshake|verif' "$WORKROOT/rogue.err" \
    || { echo "FAIL: expected a cert/tls error, got:"; cat "$WORKROOT/rogue.err"; exit 1; }

echo "client_cert OK"
