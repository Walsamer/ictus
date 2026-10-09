#!/usr/bin/env bash
# Strict verification gate for Fleet integration and promotion.
#
# Fleet runs the registered project verifier as a single, shell-free command
# (``shlex.split`` + exec), so it cannot chain scripts with ``&&``. This script
# is that single entry point: it runs the repository's own quality gates in
# the same order as CI, so a change can never be integrated as "verified" while
# the repository's coverage or mutation gates are failing.
#
#   ./scripts/verify.sh    fmt, clippy (-D warnings), cargo test, pytest
#   ./scripts/coverage.sh  Rust >= 95% lines, Python >= 99%
#   ./scripts/mutants.sh   cargo-mutants: no surviving viable mutant
#
# ``coverage.sh``/``mutants.sh`` require ``cargo-llvm-cov`` and
# ``cargo-mutants``. Mutation testing is the slow step (roughly 10-30 minutes);
# Fleet's verifier wall-clock ceiling defaults to 90 minutes.
#
# Set ``VERIFY_INTEGRATION_SKIP_MUTANTS=1`` to run only verify + coverage (for
# example on a constrained host). That is a deliberate, visible downgrade: it
# must never be used to promote work that CI would reject.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[verify-integration] verify.sh"
./scripts/verify.sh

echo "[verify-integration] coverage.sh"
./scripts/coverage.sh

if [ "${VERIFY_INTEGRATION_SKIP_MUTANTS:-0}" = "1" ]; then
    echo "[verify-integration] mutants.sh SKIPPED (VERIFY_INTEGRATION_SKIP_MUTANTS=1)"
else
    echo "[verify-integration] mutants.sh"
    ./scripts/mutants.sh
fi

echo "[verify-integration] OK"
