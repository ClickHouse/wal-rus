#!/usr/bin/env bash
# Install pgBackRest and configure async archive-push
#
# Uses env static keys off-AWS, otherwise pgBackRest IMDS auth
#
# Usage:
#   BUCKET=my-bucket [UPLOAD_CONCURRENCY=4] sudo ./05_install_pgbackrest.sh
#   or: sudo ./05_install_pgbackrest.sh <BUCKET> [UPLOAD_CONCURRENCY]
set -euo pipefail

BUCKET="${BUCKET:-${1:-}}"
UPLOAD_CONCURRENCY="${UPLOAD_CONCURRENCY:-${2:-4}}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STANZA="${PGBACKREST_STANZA:-walbench}"
REPO_PATH="${PGBACKREST_REPO_PATH:-/pgbackrest-bench}"
PGDATA="${PGDATA:-/dat/18/data}"
PGBIN="${PGBIN:-/usr/lib/postgresql/18/bin}"
CONF_DIR="/etc/pgbackrest"
CONF="${CONF_DIR}/pgbackrest.conf"
# Put spool + logs on data NVMe
SPOOL_PATH="${PGBACKREST_SPOOL_PATH:-/dat/pgbackrest/spool}"
LOG_PATH="${PGBACKREST_LOG_PATH:-/dat/pgbackrest/log}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) to apt-install + write ${CONF}." >&2
  exit 1
fi
if [[ -z "${BUCKET}" ]]; then
  echo "ERROR: BUCKET is required (env BUCKET=... or first positional arg)." >&2
  exit 1
fi

echo "=== Installing pgbackrest (PGDG) ==="
# PGDG repo is already configured by 01_install_pg18.sh.
if ! command -v pgbackrest >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y pgbackrest
fi
pgbackrest version

echo "=== Creating spool/log/config dirs (owned by postgres) ==="
install -d -o postgres -g postgres -m 0750 "${CONF_DIR}"
install -d -o postgres -g postgres -m 0750 "${SPOOL_PATH}"
install -d -o postgres -g postgres -m 0750 "${LOG_PATH}"

# Static keys from env off-AWS, otherwise IMDS
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "=== pgbackrest S3 auth: shared (static keys from environment) ==="
  S3_AUTH="repo1-s3-key-type=shared
repo1-s3-key=${AWS_ACCESS_KEY_ID}
repo1-s3-key-secret=${AWS_SECRET_ACCESS_KEY}"
  [[ -n "${AWS_SESSION_TOKEN:-}" ]] && S3_AUTH="${S3_AUTH}
repo1-s3-token=${AWS_SESSION_TOKEN}"
else
  echo "=== pgbackrest S3 auth: auto (EC2 instance-profile via IMDS) ==="
  S3_AUTH="repo1-s3-key-type=auto"
fi

echo "=== Writing ${CONF} (process-max=${UPLOAD_CONCURRENCY}, bucket=${BUCKET}) ==="
# Match process-max to WALG_UPLOAD_CONCURRENCY
# Use lz4 to match WALG_COMPRESSION_METHOD
umask 077
tmp="$(mktemp)"
cat > "${tmp}" <<EOF
[global]
repo1-type=s3
repo1-s3-bucket=${BUCKET}
repo1-s3-endpoint=s3.${AWS_REGION}.amazonaws.com
repo1-s3-region=${AWS_REGION}
${S3_AUTH}
repo1-path=${REPO_PATH}
compress-type=lz4
process-max=${UPLOAD_CONCURRENCY}
archive-async=y
spool-path=${SPOOL_PATH}
log-path=${LOG_PATH}
log-level-console=warn
log-level-file=info
log-level-stderr=warn

[${STANZA}]
pg1-path=${PGDATA}
pg1-port=5432
pg1-socket-path=/var/run/postgresql
EOF
install -o postgres -g postgres -m 0640 "${tmp}" "${CONF}"
rm -f "${tmp}"

echo "=== stanza-create (idempotent) ==="
# Safe to re-run with existing matching stanza
sudo -u postgres pgbackrest --stanza="${STANZA}" stanza-create

echo "=== Installed pgbackrest config ==="
sed -E 's/^(repo1-s3-key|repo1-s3-key-secret|repo1-s3-token)=.*/\1=<redacted>/' "${CONF}"
echo "pgbackrest $(pgbackrest version) ready for stanza '${STANZA}' -> s3://${BUCKET}${REPO_PATH}"
