"""M3 acceptance: the real Rust binary talks to the real Dagster backend.

This is the only test that crosses the language boundary end to end. It runs the
built ``ictus`` binary, which produces a typed decision, an
``ExecutionIntent`` and an ``ExecutionResult`` through Dagster. It is skipped
when the binary has not been built (``cargo test``/``cargo build`` builds it).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import REPO_ROOT

BRIDGE = REPO_ROOT / "target" / "debug" / "ictus"

pytestmark = pytest.mark.skipif(
    not BRIDGE.exists(),
    reason="ictus not built; run `cargo build -p ictus-bridge` first",
)


def _env(tmp_path: Path) -> dict[str, str]:
    (tmp_path / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    env = os.environ.copy()
    env["DAGSTER_HOME"] = str(tmp_path)
    # Point the Rust boundary at this interpreter's Dagster bridge, avoiding a
    # uv resolution at call time.
    env["ICTUS_DAGSTER_BRIDGE_CMD"] = f"{sys.executable} -m ictus_dagster.bridge"
    return env


def test_full_flow_snapshot_to_result(tmp_path: Path) -> None:
    snapshot = (REPO_ROOT / "examples" / "state-snapshot.worker-timeout.json").read_text()
    proc = subprocess.run(
        [str(BRIDGE), "flow", "--approve"],
        input=snapshot,
        capture_output=True,
        text=True,
        env=_env(tmp_path),
        check=False,
        cwd=str(REPO_ROOT),
    )
    assert proc.returncode == 0, proc.stderr
    trace = json.loads(proc.stdout)

    assert trace["proposal"]["decision"] == "REEXECUTE"
    assert trace["policy_decision"]["decision"] == "ALLOW"
    assert trace["execution_intent"]["capability"] == "demo.verify"
    assert trace["execution_result"]["status"] == "succeeded"
    assert trace["execution_result"]["intent_id"] == trace["execution_intent"]["intent_id"]
    # The result carries a durable Dagster run reference.
    assert any(
        item["kind"] == "dagster_run" for item in trace["execution_result"]["evidence"]
    )


def test_raw_intent_with_dagster_retry_through_the_boundary(tmp_path: Path) -> None:
    intent = (REPO_ROOT / "examples" / "execution-intent.retry-demo.json").read_text()
    proc = subprocess.run(
        [str(BRIDGE), "execute"],
        input=intent,
        capture_output=True,
        text=True,
        env=_env(tmp_path),
        check=False,
        cwd=str(REPO_ROOT),
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout)
    assert result["status"] == "succeeded"
    assert result["observation"]["category"] == "SUCCESS"


def test_failed_execution_is_reported_through_the_boundary(tmp_path: Path) -> None:
    intent = {
        "schema_version": 1,
        "intent_id": "intent:fail-e2e",
        "capability": "demo.verify",
        "target": {"type": "task", "id": "t1"},
        "arguments": {"fail_hard": True},
        "requested_by": {"provider": "rules.v1", "decision_id": "p1"},
    }
    proc = subprocess.run(
        [str(BRIDGE), "execute"],
        input=json.dumps(intent),
        capture_output=True,
        text=True,
        env=_env(tmp_path),
        check=False,
        cwd=str(REPO_ROOT),
    )
    # CLI convention: 4 = the backend executed and reported a non-success result.
    assert proc.returncode == 4, proc.stderr
    result = json.loads(proc.stdout)
    assert result["status"] == "failed"
    assert result["observation"]["category"] == "PROCESS_CRASH"
    assert result["intent_id"] == "intent:fail-e2e"
    assert any(item["kind"] == "dagster_run" for item in result["evidence"])


def test_denied_decision_never_reaches_dagster(tmp_path: Path) -> None:
    # A capability the core does not know is denied by policy; no intent and no
    # Dagster run must be produced.
    snapshot = json.loads(
        (REPO_ROOT / "examples" / "state-snapshot.worker-timeout.json").read_text()
    )
    snapshot["facts"] = [
        fact
        for fact in snapshot["facts"]
        if fact["key"] != "capability.id"
    ] + [{"key": "capability.id", "value": "unknown.capability"}]
    proc = subprocess.run(
        [str(BRIDGE), "decide", "--approve"],
        input=json.dumps(snapshot),
        capture_output=True,
        text=True,
        env=_env(tmp_path),
        check=False,
        cwd=str(REPO_ROOT),
    )
    assert proc.returncode == 3, proc.stderr
    trace = json.loads(proc.stdout)
    assert trace["policy_decision"]["decision"] == "DENY"
    assert trace["execution_intent"] is None
