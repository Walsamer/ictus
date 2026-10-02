#!/usr/bin/env bash
# Mutation testing for the fast, deterministic crates.
#
# Mutation testing is slow (each mutant rebuilds and re-runs the package's
# tests). It runs on ictus-core / ictus-policy / ictus-adapters; the bridge's
# CLI tests spawn processes and are covered by the normal suite instead.
#
#   cargo install cargo-mutants --locked
#
# Exit code is non-zero if any *viable* mutant survives.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! cargo mutants --version >/dev/null 2>&1; then
    echo "cargo-mutants is not installed. Install it with:" >&2
    echo "    cargo install cargo-mutants --locked" >&2
    exit 1
fi

cargo mutants -p ictus-core -p ictus-policy -p ictus-adapters -j 4 --timeout 90
