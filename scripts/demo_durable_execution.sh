#!/usr/bin/env bash
# M1 — Dagster OSS durable-execution demo.
#
# Proves, with deterministic local runs and no network:
#   1. an intentional failure is persisted;
#   2. a step retry recovers within the same run;
#   3. re-execution is a fresh durable run;
#   4. run history is inspectable.
set -euo pipefail
cd "$(dirname "$0")/.."

export DAGSTER_HOME="${DAGSTER_HOME:-$PWD/.dagster_home}"
mkdir -p "$DAGSTER_HOME"
if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then
    printf 'telemetry:\n  enabled: false\n' > "$DAGSTER_HOME/dagster.yaml"
fi
echo "DAGSTER_HOME=$DAGSTER_HOME"

run() {
    uv run --frozen dagster job execute \
        -m agentic_dagster.definitions -j capability_execution_job \
        --config "$1"
}

echo
echo "== 1/4 intentional persisted failure (every attempt fails) =="
if run examples/dagster/fail_hard.yaml; then
    echo "UNEXPECTED: fail_hard run succeeded" >&2
    exit 1
else
    echo "expected: run failed and was persisted"
fi

echo
echo "== 2/4 retry recovers within one durable run (fails twice, succeeds 3rd) =="
run examples/dagster/retry_then_succeed.yaml

echo
echo "== 3/4 re-execution is a fresh durable run =="
run examples/dagster/success.yaml

echo
echo "== 4/4 persisted run history =="
uv run --frozen dagster run list

echo
echo "demo: OK"
