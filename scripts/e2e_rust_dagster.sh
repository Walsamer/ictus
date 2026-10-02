#!/usr/bin/env bash
# M3 — end-to-end Rust -> Dagster -> Rust proof.
#
# 1. `ictus flow` turns a generic StateSnapshot into a proposal, a policy
#    decision and an ExecutionIntent, then executes it through Dagster.
# 2. `ictus execute` sends a raw ExecutionIntent whose execute step is made
#    to fail twice, proving Dagster's retry is observed through the boundary.
# 3. the second, non-software domain (data quality) runs through the same core,
#    proving the core does not depend on a software-engineering domain.
# 4. the third domain (system diagnostics) runs through the same core as well.
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
echo "== flow: second (non-software) domain through the same core =="
"$BRIDGE" flow --approve < examples/state-snapshot.data-quality.json | tee /tmp/ictus-data-quality.json

echo
echo "== flow: third domain (system diagnostics) through the same core =="
"$BRIDGE" flow --approve < examples/state-snapshot.system-diagnostic.json | tee /tmp/ictus-system-diagnostic.json

echo
echo "== types produced by the Rust boundary =="
"$PYTHON" - <<'PY'
import json
for label, path in (
    ("flow", "/tmp/ictus-flow.json"),
    ("execute", "/tmp/ictus-execute.json"),
    ("data-quality", "/tmp/ictus-data-quality.json"),
    ("system-diagnostic", "/tmp/ictus-system-diagnostic.json"),
):
    data = json.load(open(path))
    assert data["schema_version"] == 1, label

flow = json.load(open("/tmp/ictus-flow.json"))
assert flow["execution_result"]["status"] == "succeeded", flow

dq = json.load(open("/tmp/ictus-data-quality.json"))
assert dq["execution_intent"]["capability"] == "data.quality_check", dq
assert dq["execution_result"]["status"] == "succeeded", dq

diag = json.load(open("/tmp/ictus-system-diagnostic.json"))
assert diag["execution_intent"]["capability"] == "system.diagnose", diag
assert diag["execution_result"]["status"] == "succeeded", diag
print("e2e: OK")
PY
