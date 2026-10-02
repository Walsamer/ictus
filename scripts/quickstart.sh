#!/usr/bin/env bash
# One-command tour of Ictus on a fresh clone.
#
#   1. build the Rust CLI and sync the Python environment
#   2. make a typed decision + policy check (no Dagster needed)
#   3. run the full Rust -> Dagster -> Rust loop
#   4. run the second (data-quality) domain through the same core
#
# Requires Rust (stable) and uv. No domain/testbed checkout is required.
set -euo pipefail
cd "$(dirname "$0")/.."

export DAGSTER_HOME="${DAGSTER_HOME:-$PWD/.dagster_home}"
mkdir -p "$DAGSTER_HOME"
if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then
    printf 'telemetry:\n  enabled: false\n' > "$DAGSTER_HOME/dagster.yaml"
fi

echo "== 1/4 build (cargo + uv) =="
cargo build -q -p ictus-bridge
uv sync --frozen >/dev/null
if [ -x "$PWD/.venv/bin/python" ]; then
    export ICTUS_DAGSTER_BRIDGE_CMD="$PWD/.venv/bin/python -m ictus_dagster.bridge"
else
    export ICTUS_DAGSTER_BRIDGE_CMD="uv run --frozen python -m ictus_dagster.bridge"
fi

echo
echo "== 2/4 typed decision + policy (no Dagster) =="
./target/debug/ictus decide --approve < examples/state-snapshot.worker-timeout.json

echo
echo "== 3/4 Rust -> Dagster -> Rust end to end =="
./scripts/e2e_rust_dagster.sh

echo
echo "== 4/4 second domain (data quality) through the same core =="
./target/debug/ictus flow --approve < examples/state-snapshot.data-quality.json

echo
echo "quickstart: OK"
