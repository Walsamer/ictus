#!/usr/bin/env bash
# Coverage gate.
#
#   Python: >= 99% (configured in pyproject.toml [tool.coverage.report])
#   Rust:   >= 95% of lines (tests/fixtures excluded)
#
# Python coverage is enforced by `pytest` itself (see [tool.pytest.ini_options]
# addopts), so a plain `pytest` run already fails below the threshold. This
# script runs both gates and requires `cargo-llvm-cov` for the Rust side:
#
#   cargo install cargo-llvm-cov --locked
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[coverage] python (>= 99%)"
uv sync --frozen >/dev/null
uv run --frozen pytest -q --cov=ictus_dagster --cov-report=term-missing

echo
echo "[coverage] rust (>= 95% lines)"
if ! cargo llvm-cov --version >/dev/null 2>&1; then
    echo "cargo-llvm-cov is not installed. Install it with:" >&2
    echo "    cargo install cargo-llvm-cov --locked" >&2
    exit 1
fi
cargo llvm-cov --workspace --summary-only \
    --ignore-filename-regex '(tests/|fixtures/)' \
    --fail-under-lines 95

echo
echo "coverage: OK"
