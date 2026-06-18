#!/usr/bin/env bash
# Exercise server-side copy_within (S3 CopyObject / GCS rewriteTo) against the
# configured object backend. Pushes a backup, copies it to a sibling prefix in
# the SAME bucket (so copy stays server-side rather than streaming through),
# then verifies the copy is listable. This is the only live copy_within cover.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

case "${WALRS_STORAGE_BACKEND:-fs}" in
s3) DST="s3://${WALRS_S3_BUCKET:-wal-rs}/$(basename "$WORKROOT")-copy" ;;
gcs) DST="gs://${WALRS_GS_BUCKET:-wal-rs}/$(basename "$WORKROOT")-copy" ;;
*)
    echo "storage_copy requires an object backend (WALRS_STORAGE_BACKEND=s3|gcs)" >&2
    exit 1
    ;;
esac

pg_initdb
pg_archive_on "$WALRS_BIN"
pg_start

pgbench -p "$PGPORT" -h "$PGHOST" -i -s 1 postgres
psql -p "$PGPORT" -h "$PGHOST" -c "CHECKPOINT" postgres
wal-rs backup-push
psql -p "$PGPORT" -h "$PGHOST" -c "SELECT pg_switch_wal()" postgres
sleep 3

wal-rs backup-list | grep -E '^base_' || { echo "no source backup"; exit 1; }

# Copy LATEST + its WAL window into the sibling prefix; same bucket ⇒ the copy
# path takes copy_within (CopyObject / rewriteTo).
wal-rs copy --backup-name LATEST --with-history --to "$DST"

# The copy must be listable at the destination prefix. Override only the prefix;
# endpoint + credentials stay in the exported env.
case "${WALRS_STORAGE_BACKEND}" in
s3) WALG_S3_PREFIX="$DST" wal-rs backup-list | tee "$WORKROOT/copy-list.txt" ;;
gcs) WALG_GS_PREFIX="$DST" wal-rs backup-list | tee "$WORKROOT/copy-list.txt" ;;
esac
grep -E '^base_' "$WORKROOT/copy-list.txt" || { echo "copied backup not listable at $DST"; exit 1; }

echo "storage_copy OK"
