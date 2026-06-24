#!/usr/bin/env bash
# Package walrus source into a tarball for a fresh SUT that has no git checkout.
#
# git archive embeds the commit id in the tarball's pax header, so
# 03_build_walrus.sh recovers it via `git get-tar-commit-id` and provenance
# survives the transfer even though the extracted tree carries no .git.
#
# Usage:  make_source_tarball.sh [REF]        # REF defaults to HEAD
# Prints the tarball path on stdout (progress on stderr), so it composes:
#   t=$(bench/scripts/make_source_tarball.sh) && scp "$t" sut:/tmp/
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# bench/scripts -> repo root
REPO_ROOT="${WALRUS_REPO:-$(cd -- "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)}"
REF="${1:-${WALRUS_REF:-HEAD}}"

if [[ ! -d "${REPO_ROOT}/.git" ]]; then
  echo "ERROR: ${REPO_ROOT} is not a git checkout; nothing to archive." >&2
  exit 1
fi

SHA="$(git -C "${REPO_ROOT}" rev-parse "${REF}")"
OUT="${WALRUS_SRC_TARBALL:-${REPO_ROOT}/bench/walrus-src-${SHA:0:12}.tar.gz}"

echo "=== git archive ${REF} (${SHA}) -> ${OUT} ===" >&2
git -C "${REPO_ROOT}" archive --format=tar.gz --prefix=walrus/ -o "${OUT}" "${REF}"
echo "=== wrote $(du -h "${OUT}" | cut -f1) ===" >&2

# Path on stdout for scripting
printf '%s\n' "${OUT}"
