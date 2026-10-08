"""Receipt protocol tests using a small Dagster-instance double.

The production integration tests exercise the real configured instance; these
tests pin the bridge's idempotency and reconciliation decisions without
starting a daemon.
"""

from __future__ import annotations

import io
import json
from types import SimpleNamespace

import pytest

from ictus_dagster import bridge
from ictus_dagster.adapters import workflow_registry
from ictus_dagster import submission


def _intent(**overrides):
    payload = {
        "schema_version": 1,
        "intent_id": "attempt:receipt-test",
        "capability": "demo.verify",
        "target": {"type": "task", "id": "task-1"},
        "arguments": {},
        "requested_by": {"provider": "rules.v1", "decision_id": "decision-1"},
    }
    payload.update(overrides)
    return payload


class _Launcher:
    pass


class _Instance:
    is_ephemeral = False
    run_launcher = _Launcher()

    def __init__(self):
        self.runs = []
        self.submitted = []
        self.logs = []

    def get_runs(self, **_kwargs):
        return list(self.runs)

    def create_run_for_job(self, _job, **kwargs):
        run = SimpleNamespace(run_id="run-1", tags=kwargs["tags"], status="NOT_STARTED")
        self.runs.append(run)
        return run

    def submit_run(self, run_id):
        self.submitted.append(run_id)

    def get_run_by_id(self, run_id):
        return next((run for run in self.runs if run.run_id == run_id), None)

    def all_logs(self, _run_id):
        return list(self.logs)

    def cancel_run(self, run_id):
        self.get_run_by_id(run_id).status = "CANCELED"


class _Reconstructed:
    def get_python_origin(self):
        return object()


@pytest.fixture
def durable(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setattr(submission, "reconstructable", lambda _factory: _Reconstructed())
    return _Instance()


def test_same_identity_and_digest_reconciles_to_the_original_receipt(durable) -> None:
    first = submission.submit_intent(_intent(), instance=durable)
    second = submission.submit_intent(_intent(), instance=durable)
    assert first["receipt_id"] == second["receipt_id"] == "run-1"
    assert first["submitted_at"] == second["submitted_at"]
    assert durable.submitted == ["run-1"]


def test_changed_content_for_an_existing_identity_fails_closed(durable) -> None:
    submission.submit_intent(_intent(), instance=durable)
    with pytest.raises(submission.SubmissionConflict):
        submission.submit_intent(_intent(arguments={"fail_hard": True}), instance=durable)
    assert durable.submitted == ["run-1"]


def test_production_submission_rejects_missing_or_ephemeral_storage(monkeypatch) -> None:
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    with pytest.raises(submission.EphemeralStorageError):
        submission.production_instance()

    monkeypatch.setenv("DAGSTER_HOME", "/tmp/dagster")
    with pytest.raises(submission.EphemeralStorageError):
        submission.production_instance(SimpleNamespace(is_ephemeral=True))

    callable_ephemeral = SimpleNamespace(is_ephemeral=lambda: True)
    with pytest.raises(submission.EphemeralStorageError):
        submission.production_instance(callable_ephemeral)


@pytest.mark.parametrize(
    "launcher",
    [None, type("InMemoryLauncher", (), {})(), type("SyncLauncher", (), {})()],
)
def test_production_submission_requires_an_independent_launcher(
    monkeypatch, launcher
) -> None:
    monkeypatch.setenv("DAGSTER_HOME", "/tmp/dagster")
    with pytest.raises(submission.SubmissionError, match="independent"):
        submission.production_instance(
            SimpleNamespace(is_ephemeral=False, run_launcher=launcher)
        )


def test_production_submission_can_load_the_configured_instance(monkeypatch, durable) -> None:
    monkeypatch.setattr(submission.DagsterInstance, "get", lambda: durable)
    assert submission.production_instance() is durable


def test_unknown_receipt_fails_closed(durable) -> None:
    with pytest.raises(submission.UnknownReceipt):
        submission.query_receipt("missing", instance=durable)

    with pytest.raises(submission.UnknownReceipt, match="non-empty"):
        submission.query_receipt({}, instance=durable)
    with pytest.raises(submission.UnknownReceipt, match="non-empty"):
        submission.query_receipt({"receipt_id": 42}, instance=durable)


def test_foreign_run_is_not_an_ictus_receipt(durable) -> None:
    durable.runs.append(
        SimpleNamespace(run_id="foreign", tags={}, status="NOT_STARTED")
    )
    with pytest.raises(submission.UnknownReceipt):
        submission.query_receipt("foreign", instance=durable)
    with pytest.raises(submission.UnknownReceipt):
        submission.cancel_receipt("foreign", instance=durable)


def test_pending_and_successful_receipts_are_reconciled(durable) -> None:
    receipt = submission.submit_intent(_intent(), instance=durable)
    pending = submission.query_receipt(receipt, instance=durable)
    assert pending["state"] == "pending"
    assert "result" not in pending

    durable.runs[0].status = "SUCCESS"
    terminal = submission.query_receipt(receipt, instance=durable)
    assert terminal["state"] == "terminal"
    assert terminal["result"]["status"] == "succeeded"


def _event(event_type: str, *, step: str | None = None, error: str = ""):
    return SimpleNamespace(
        dagster_event=SimpleNamespace(
            event_type_value=event_type,
            step_key=step,
            event_specific_data=SimpleNamespace(error=SimpleNamespace(cls_name=error)),
        )
    )


def test_failure_query_preserves_step_and_timeout_evidence(durable) -> None:
    receipt = submission.submit_intent(_intent(), instance=durable)
    durable.runs[0].status = "FAILURE"
    durable.logs = [
        _event("STEP_START"),
        _event("STEP_FAILURE", step="execute_op", error="ChildProcessError"),
    ]
    failed = submission.query_receipt(receipt, instance=durable)
    assert failed["result"]["status"] == "failed"
    assert failed["result"]["observation"]["category"] == "PROCESS_CRASH"

    durable.logs = [
        _event("STEP_FAILURE", step="execute_op", error="WorkerTimeoutError")
    ]
    timed_out = submission.query_receipt(receipt, instance=durable)
    assert timed_out["result"]["observation"]["category"] == "WORKER_TIMEOUT"


def test_submit_supports_workspace_and_legacy_launch_signatures() -> None:
    calls = []

    class WorkspaceSubmitter:
        def submit_run(self, run_id, workspace):
            calls.append((run_id, workspace))

    submission._submit_to_dagster(WorkspaceSubmitter(), "workspace-run")
    assert calls == [("workspace-run", None)]

    legacy = SimpleNamespace(launch_run=lambda run_id: calls.append((run_id, "legacy")))
    submission._submit_to_dagster(legacy, "legacy-run")
    assert calls[-1] == ("legacy-run", "legacy")


def test_submission_builds_non_empty_resource_config(durable) -> None:
    receipt = submission.submit_intent(
        _intent(intent_id="attempt:configured", arguments={"fail_hard": True}),
        instance=durable,
    )
    assert receipt["receipt_id"] == "run-1"


def test_workflow_factories_return_the_registered_jobs() -> None:
    assert (
        workflow_registry.capability_execution_job_factory()
        is workflow_registry.capability_execution_job
    )
    assert workflow_registry.data_quality_job_factory() is workflow_registry.data_quality_job
    assert (
        workflow_registry.system_diagnostic_job_factory()
        is workflow_registry.system_diagnostic_job
    )


@pytest.mark.parametrize(
    ("command", "function_name"),
    [("submit", "submit_intent"), ("query", "query_receipt"), ("cancel", "cancel_receipt")],
)
def test_bridge_dispatches_durable_commands(
    monkeypatch, capsys, command, function_name
) -> None:
    expected = {"command": command}
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({"input": command})))
    monkeypatch.setattr(bridge, function_name, lambda payload: expected)
    assert bridge.main([command]) == 0
    assert json.loads(capsys.readouterr().out) == expected


def test_cancellation_acknowledges_runtime_state(durable) -> None:
    receipt = submission.submit_intent(_intent(), instance=durable)
    response = submission.cancel_receipt(receipt, instance=durable)
    assert response["state"] == "terminal"
    assert response["result"]["status"] == "cancelled"


def test_cancellation_rejects_malformed_receipts(durable) -> None:
    with pytest.raises(submission.UnknownReceipt):
        submission.cancel_receipt({}, instance=durable)
