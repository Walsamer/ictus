#!/usr/bin/env bash
# M3 — end-to-end Rust -> Dagster -> Rust proof.
#
# 1. `ictus flow` turns a generic StateSnapshot into a proposal, a policy
#    decision and an ExecutionIntent, then executes it through Dagster.
# 2. `ictus execute` sends a raw ExecutionIntent whose execute step is made
#    to fail twice, proving Dagster's retry is observed through the boundary.
#
# stdout from the Rust binary is pure JSON (Dagster logs go to stderr).
set -euo pipefail
cd "$(dirname "$0")/.."

export DAGSTER_HOME="${DAGSTER_HOME:-$PWD/.dagster_home}"
mkdir -p "$DAGSTER_HOME"
if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then
    printf 'telemetry:\n  enabled: false\n' > "$DAGSTER_HOME/dagster.yaml"
fi

echo "[e2e] building ictus"
cargo build -q -p ictus-bridge
BRIDGE="$PWD/target/debug/ictus"

# Point the Rust boundary at this workspace's Python bridge without needing uv
# to resolve anything at call time.
PYTHON="$PWD/.venv/bin/python"
if [ -x "$PYTHON" ]; then
    export ICTUS_DAGSTER_BRIDGE_CMD="$PYTHON -m ictus_dagster.bridge"
else
    export ICTUS_DAGSTER_BRIDGE_CMD="uv run --frozen python -m ictus_dagster.bridge"
fi

echo
echo "== flow: StateSnapshot -> proposal -> policy -> intent -> Dagster =="
"$BRIDGE" flow --approve < examples/state-snapshot.worker-timeout.json | tee /tmp/ictus-flow.json

echo
echo "== execute: intent with deterministic retry through the boundary =="
"$BRIDGE" execute < examples/execution-intent.retry-demo.json | tee /tmp/ictus-execute.json

echo
echo "== types produced by the Rust boundary =="
"$PYTHON" - <<'PY'
import json
for label, path in (("flow", "/tmp/ictus-flow.json"), ("execute", "/tmp/ictus-execute.json")):
    data = json.load(open(path))
    assert data["schema_version"] == 1, label
print("e2e: OK")
PY
