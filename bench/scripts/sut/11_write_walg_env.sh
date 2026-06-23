#!/usr/bin/env bash
# Write the shared environment file consumed by BOTH daemons (wal-g and walrus).
# wal-rs has no IMDS support, so credentials must live in this file as plain env
# vars.
#
# Credential source, in order:
#   1. AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY already in env (off-AWS / dev /
#      static keys; AWS_SESSION_TOKEN optional) — written verbatim.
#   2. otherwise IMDSv2 (EC2 instance role) — fetched here.
#
# Usage:
#   BUCKET=my-bucket [UPLOAD_CONCURRENCY=4] sudo -E ./11_write_walg_env.sh
#   or: sudo ./11_write_walg_env.sh <BUCKET> [UPLOAD_CONCURRENCY]
set -euo pipefail

BUCKET="${BUCKET:-${1:-}}"
UPLOAD_CONCURRENCY="${UPLOAD_CONCURRENCY:-${2:-16}}"
ENV_FILE="${ENV_FILE:-/etc/postgresql/wal-g.env}"
AWS_REGION="${AWS_REGION:-us-east-1}"
COMPRESSION_METHOD="${WALG_COMPRESSION_METHOD:-lz4}"
IMDS="http://169.254.169.254/latest"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo) to write ${ENV_FILE}." >&2
  exit 1
fi

if [[ -z "${BUCKET}" ]]; then
  echo "ERROR: BUCKET is required (env BUCKET=... or first positional arg)." >&2
  exit 1
fi

# Storage prefix both daemons archive into. run.sh / run_op.sh scope it per
# tool+run for isolation; default keeps the shared bench prefix for setup/smoke.
WALG_S3_PREFIX="${WALG_S3_PREFIX:-s3://${BUCKET}/walg-bench}"

ACCESS_KEY="${AWS_ACCESS_KEY_ID:-}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-}"
SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

if [[ -n "${ACCESS_KEY}" && -n "${SECRET_KEY}" ]]; then
  echo "=== Using AWS credentials from environment ==="
else
  echo "=== Fetching temporary credentials via IMDSv2 ==="
  TOKEN="$(curl -sf -X PUT "${IMDS}/api/token" \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600')"
  if [[ -z "${TOKEN}" ]]; then
    echo "ERROR: no env credentials and failed to obtain IMDSv2 token." >&2
    exit 1
  fi

  ROLE="$(curl -sf -H "X-aws-ec2-metadata-token: ${TOKEN}" \
    "${IMDS}/meta-data/iam/security-credentials/")"
  if [[ -z "${ROLE}" ]]; then
    echo "ERROR: no IAM role attached to this instance." >&2
    exit 1
  fi
  echo "IAM role: ${ROLE}"

  CREDS_JSON="$(curl -sf -H "X-aws-ec2-metadata-token: ${TOKEN}" \
    "${IMDS}/meta-data/iam/security-credentials/${ROLE}")"

  read_field() {
    local key="$1"
    if command -v jq >/dev/null 2>&1; then
      printf '%s' "${CREDS_JSON}" | jq -r ".${key}"
    else
      printf '%s' "${CREDS_JSON}" \
        | python3 -c "import sys,json;print(json.load(sys.stdin)['${key}'])"
    fi
  }

  ACCESS_KEY="$(read_field AccessKeyId)"
  SECRET_KEY="$(read_field SecretAccessKey)"
  SESSION_TOKEN="$(read_field Token)"
  echo "Credentials expire at: $(read_field Expiration)"
fi

if [[ -z "${ACCESS_KEY}" || -z "${SECRET_KEY}" ]]; then
  echo "ERROR: incomplete credentials (need AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY)." >&2
  exit 1
fi

echo "=== Writing ${ENV_FILE} (UPLOAD_CONCURRENCY=${UPLOAD_CONCURRENCY}) ==="
install -d -o postgres -g postgres -m 0755 "$(dirname "${ENV_FILE}")"
umask 077
tmp="$(mktemp)"
cat > "${tmp}" <<EOF
WALG_S3_PREFIX=${WALG_S3_PREFIX}
AWS_REGION=${AWS_REGION}
WALG_COMPRESSION_METHOD=${COMPRESSION_METHOD}
WALG_UPLOAD_CONCURRENCY=${UPLOAD_CONCURRENCY}
PGHOST=/var/run/postgresql
PGDATA=/dat/18/data
AWS_ACCESS_KEY_ID=${ACCESS_KEY}
AWS_SECRET_ACCESS_KEY=${SECRET_KEY}
EOF
# Session token only for temporary (IMDS / STS) credentials.
if [[ -n "${SESSION_TOKEN}" ]]; then
  printf 'AWS_SESSION_TOKEN=%s\n' "${SESSION_TOKEN}" >> "${tmp}"
fi
install -o postgres -g postgres -m 0600 "${tmp}" "${ENV_FILE}"
rm -f "${tmp}"

echo "Done. ${ENV_FILE}:"
# Show keys only, never secret values.
sed -E 's/^(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)=.*/\1=<redacted>/' "${ENV_FILE}"
