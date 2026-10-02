"""Bridge behaviour: in-process and as a stdio subprocess."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from dagster import DagsterInstance

from ictus_dagster import bridge
from ictus_dagster.adapters.contracts import ContractError
from ictus_dagster.adapters.intent_config import UnsupportedCapability


def _intent(capability: str = "demo.verify", arguments: dict | None = None) -> dict:
    return {
        "schema_version": 1,
        "intent_id": "intent:bridge-test",
        "capability": capability,
        "target": {"type": "task", "id": "task-bridge"},
        "arguments": arguments or {},
        "requested_by": {"provider": "rules.v1", "decision_id": "proposal-bridge"},
    }


def test_execute_intent_returns_a_contract_result() -> None:
    payload = _intent()
    result = bridge.execute_intent(payload, instance=DagsterInstance.ephemeral())
    assert result["schema_version"] == 1
    assert result["intent_id"] == payload["intent_id"]
    assert result["status"] == "succeeded"
    assert any(item["kind"] == "dagster_run" for item in result["evidence"])


def test_execute_intent_reports_failure_without_raising() -> None:
    result = bridge.execute_intent(
        _intent(arguments={"fail_hard": True}), instance=DagsterInstance.ephemeral()
    )
    assert result["status"] == "failed"
    assert result["observation"]["category"] == "PROCESS_CRASH"


def test_unsupported_capability_fails_closed() -> None:
    with pytest.raises(UnsupportedCapability):
        bridge.execute_intent(
            _intent("software.promote"), instance=DagsterInstance.ephemeral()
        )


def test_bridge_subprocess_writes_pure_json_to_stdout(tmp_path) -> None:
    env = os.environ.copy()
    env["DAGSTER_HOME"] = str(tmp_path)
    (tmp_path / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    proc = subprocess.run(
        [sys.executable, "-m", "ictus_dagster.bridge"],
        input=json.dumps(_intent()),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)  # stdout must be exactly one JSON object
    assert payload["status"] == "succeeded"
    assert proc.stderr.strip(), "diagnostics should go to stderr"


def test_bridge_subprocess_returns_a_failed_result_with_exit_0(tmp_path) -> None:
    # A failed run is a valid result: the bridge must still exit 0 so the Rust
    # boundary parses the ExecutionResult instead of treating it as a crash.
    env = os.environ.copy()
    env["DAGSTER_HOME"] = str(tmp_path)
    (tmp_path / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    proc = subprocess.run(
        [sys.executable, "-m", "ictus_dagster.bridge"],
        input=json.dumps(_intent(arguments={"fail_hard": True})),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["status"] == "failed"
    assert payload["observation"]["category"] == "PROCESS_CRASH"


def test_bridge_rejects_an_invalid_intent() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "ictus_dagster.bridge"],
        input='{"schema_version": 1}',
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    assert proc.stdout.strip() == ""


def test_validate_intent_rejects_wrong_version() -> None:
    payload = _intent()
    payload["schema_version"] = 2
    with pytest.raises(ContractError, match="unsupported schema_version"):
        bridge.execute_intent(payload, instance=DagsterInstance.ephemeral())
