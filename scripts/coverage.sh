#!/usr/bin/env bash
# Coverage gate.
#
#   Python: >= 99%, fixed policy in scripts/coverage.toml (protected scope)
#   Rust:   >= 95% of lines (tests/fixtures excluded)
#
# The Python threshold, source set and exclusions are read from the protected
# ``scripts/coverage.toml`` (via ``--cov-config``) and the minimum is also passed
# explicitly with ``--cov-fail-under``. Lowering ``pyproject.toml``'s
# ``[tool.coverage.report] fail_under`` therefore does not weaken this gate.
#
#   cargo install cargo-llvm-cov --locked
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[coverage] python (>= 99%, fixed policy)"
uv sync --frozen >/dev/null
uv run --frozen pytest -q \
    --cov=ictus_dagster --cov-report=term-missing \
    --cov-config=scripts/coverage.toml --cov-fail-under=99

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
