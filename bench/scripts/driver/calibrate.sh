#!/usr/bin/env bash
#
# calibrate.sh
#
# Calibration helper: generate ~5 minutes of burst load and report how much WAL
# the SUT produced, so the operator can size burst WORKERS against the measured
# single-daemon drain rate (which the SUT-side sampler records in wal.csv /
# archive.csv at the same time).
#
# It records pg_current_wal_lsn() before and after, then prints WAL bytes,
# WAL MB/s generated, and sizing guidance: to satisfy the "generation >= ~2x
# drain" target, compare generated MB/s here to the drain MB/s the sampler saw.
#
# Env vars (with defaults):
#   PGHOST      (required) SUT private IP / host
#   PGPORT      5432
#   PGUSER      (required) login role
#   PGPASSWORD  (required) password (or ~/.pgpass)
#   PGDATABASE  walbench
#   CAL_DURATION   300    calibration burst duration in seconds (~5 min)
#   WORKERS        <nproc> burst workers to calibrate with (passed through)
#   (any other workload_burst.sh env vars are passed through unchanged)

set -euo pipefail

PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-walbench}"
CAL_DURATION="${CAL_DURATION:-300}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BURST="${SCRIPT_DIR}/workload_burst.sh"

: "${PGHOST:?Set PGHOST to the SUT private IP}"
: "${PGUSER:?Set PGUSER to the login role}"

export PGPORT PGDATABASE

if [[ ! -x "${BURST}" ]]; then
    echo "FATAL: ${BURST} not found or not executable" >&2
    exit 1
fi

# Helper: absolute WAL byte position (LSN distance from origin).
wal_bytes() {
    psql -d "${PGDATABASE}" -At -c \
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')"
}

echo "==> Calibration burst for ${CAL_DURATION}s against ${PGHOST}:${PGPORT}/${PGDATABASE}"
echo "==> Make sure the SUT sampler is running so drain (wal.csv/archive.csv) is captured concurrently."

start_bytes="$(wal_bytes)"
start_epoch="$(date +%s)"

# Run the burst at the calibration duration. WORKERS and other tunables are
# inherited from the environment by workload_burst.sh.
DURATION="${CAL_DURATION}" "${BURST}"

end_bytes="$(wal_bytes)"
end_epoch="$(date +%s)"

elapsed=$(( end_epoch - start_epoch ))
if (( elapsed <= 0 )); then
    elapsed=1
fi
gen_bytes=$(( end_bytes - start_bytes ))

# Report in MB and MB/s using awk for floating point.
awk -v b="${gen_bytes}" -v s="${elapsed}" 'BEGIN {
    mb = b / 1048576.0;
    mbs = mb / s;
    printf "\n===== CALIBRATION RESULT =====\n";
    printf "Elapsed:            %d s\n", s;
    printf "WAL generated:      %.1f MB (%d bytes)\n", mb, b;
    printf "WAL generation rate: %.1f MB/s\n", mbs;
    printf "==============================\n\n";
    printf "Sizing guidance:\n";
    printf "  * Read the single-daemon drain rate (DRAIN MB/s) from the SUT\n";
    printf "    sampler (wal.csv slope, or archive.csv archived_count rate).\n";
    printf "  * Target generation >= 2x drain so *.ready backlog climbs.\n";
    printf "  * If generated %.1f MB/s < 2 * DRAIN, scale WORKERS up by about\n", mbs;
    printf "      ceil( (2 * DRAIN) / (%.1f / current_WORKERS) ).\n", mbs;
    printf "  * If it is already well above 2x drain, you can lower WORKERS to\n";
    printf "    reduce driver-side cost while still backing up the archiver.\n";
}'

echo "==> Calibration complete."
