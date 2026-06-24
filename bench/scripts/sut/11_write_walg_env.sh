#!/usr/bin/env bash
# Write shared daemon environment for wal-g and walrus
#
# Static AWS keys are optional; absent keys mean daemons use IMDS
#
# Usage:
#   BUCKET=my-bucket [UPLOAD_CONCURRENCY=4] [WALG_USE_WAL_DELTA=1] \
#     sudo -E ./11_write_walg_env.sh
#   or: sudo ./11_write_walg_env.sh <BUCKET> [UPLOAD_CONCURRENCY]
set -euo pipefail

BUCKET="${BUCKET:-${1:-}}"
UPLOAD_CONCURRENCY="${UPLOAD_CONCURRENCY:-${2:-4}}"
# Download fan-out defaults to upload fan-out
DOWNLOAD_CONCURRENCY="${DOWNLOAD_CONCURRENCY:-${UPLOAD_CONCURRENCY}}"
ENV_FILE="${ENV_FILE:-/etc/postgresql/wal-g.env}"
AWS_REGION="${AWS_REGION:-us-east-1}"
COMPRESSION_METHOD="${WALG_COMPRESSION_METHOD:-lz4}"
# Pre-record <group>_delta sidecars during wal-push
USE_WAL_DELTA="${WALG_USE_WAL_DELTA:-}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) to write ${ENV_FILE}." >&2
  exit 1
fi

if [[ -z "${BUCKET}" ]]; then
  echo "ERROR: BUCKET is required (env BUCKET=... or first positional arg)." >&2
  exit 1
fi

# Storage prefix, scoped by drivers per tool+run
WALG_S3_PREFIX="${WALG_S3_PREFIX:-s3://${BUCKET}/walg-bench}"

ACCESS_KEY="${AWS_ACCESS_KEY_ID:-}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-}"
SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

if [[ -n "${ACCESS_KEY}" && -n "${SECRET_KEY}" ]]; then
  echo "=== Using static AWS credentials from environment ==="
elif [[ -n "${ACCESS_KEY}" || -n "${SECRET_KEY}" ]]; then
  echo "ERROR: incomplete credentials; set BOTH AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither (IMDS)." >&2
  exit 1
else
  echo "=== No static credentials; daemons resolve EC2 instance-role creds via IMDS ==="
fi

echo "=== Writing ${ENV_FILE} (UPLOAD_CONCURRENCY=${UPLOAD_CONCURRENCY} DOWNLOAD_CONCURRENCY=${DOWNLOAD_CONCURRENCY}) ==="
install -d -o postgres -g postgres -m 0755 "$(dirname "${ENV_FILE}")"
umask 077
tmp="$(mktemp)"
cat > "${tmp}" <<EOF
WALG_S3_PREFIX=${WALG_S3_PREFIX}
AWS_REGION=${AWS_REGION}
WALG_COMPRESSION_METHOD=${COMPRESSION_METHOD}
WALG_UPLOAD_CONCURRENCY=${UPLOAD_CONCURRENCY}
WALG_DOWNLOAD_CONCURRENCY=${DOWNLOAD_CONCURRENCY}
PGHOST=/var/run/postgresql
PGDATA=/dat/18/data
EOF
# Static keys only off-AWS; absent means IMDS
if [[ -n "${ACCESS_KEY}" ]]; then
  printf 'AWS_ACCESS_KEY_ID=%s\n' "${ACCESS_KEY}" >> "${tmp}"
  printf 'AWS_SECRET_ACCESS_KEY=%s\n' "${SECRET_KEY}" >> "${tmp}"
  [[ -n "${SESSION_TOKEN}" ]] && printf 'AWS_SESSION_TOKEN=%s\n' "${SESSION_TOKEN}" >> "${tmp}"
fi
# Omit unset sidecar flag, empty value fails walrus bool parse
if [[ -n "${USE_WAL_DELTA}" ]]; then
  printf 'WALG_USE_WAL_DELTA=%s\n' "${USE_WAL_DELTA}" >> "${tmp}"
fi
install -o postgres -g postgres -m 0600 "${tmp}" "${ENV_FILE}"
rm -f "${tmp}"

echo "Done. ${ENV_FILE}:"
# Redact secret values
sed -E 's/^(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)=.*/\1=<redacted>/' "${ENV_FILE}"
