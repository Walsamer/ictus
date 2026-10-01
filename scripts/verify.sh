#!/usr/bin/env bash
# Deterministic verification gate for ictus.
#
# Runs the full Rust workspace checks and the Python test suite. Intended to be
# usable both locally and as a registered domain verifier command. It never pushes,
# deploys, or touches any external system.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v cargo >/dev/null 2>&1; then
    echo "verify: cargo not found on PATH" >&2
    exit 1
fi

echo "[verify] cargo fmt --check"
cargo fmt --all -- --check
echo "[verify] cargo clippy -D warnings"
cargo clippy --workspace --all-targets -- -D warnings
echo "[verify] cargo test"
cargo test --workspace

echo "[verify] python tests"
if command -v uv >/dev/null 2>&1; then
    uv sync --frozen >/dev/null
    uv run --frozen pytest -q
else
    python3 -m pytest -q tests/python
fi

echo "[verify] OK"
