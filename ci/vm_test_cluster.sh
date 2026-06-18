#!/usr/bin/env bash
# Boot a replication-enabled trust cluster and run the passed command with the
# cluster's PG* env exported. Drives the pg-vm-test CI lane:
#
#   ci/vm_test_cluster.sh cargo test --features vm-test --locked
#
# tests/vm_live.rs connects over the unix socket (PGHOST), trust auth, no TLS.
# lib.sh's EXIT trap stops the cluster; we don't exec so the trap still fires.
set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib.sh"

pg_initdb
pg_replication_on
pg_hba_replication
pg_start

# Already exported by lib.sh; restated since vm_live.rs reads them directly
export PGHOST PGPORT PGUSER PGDATABASE

"$@"
