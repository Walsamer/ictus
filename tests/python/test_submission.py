"""Receipt-protocol unit tests using a small Dagster-instance double.

The production integration tests (``test_submission_integration.py``) exercise
the real configured instance; these tests pin the bridge's idempotency,
reconciliation and fail-closed decisions without starting a daemon.  They are
*not* proof of production compatibility: they use doubles, so they must be read
alongside the real-instance tests.
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


class QueuedRunCoordinator:
    """Double whose class name matches Dagster's supported coordinator."""

    def __init__(self, instance: "_Instance | None" = None) -> None:
        self._instance = instance

    def cancel_run(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id) if self._instance else None
        if run is None:
            return False
        run.status = "CANCELED"
        return True


class _Instance:
    is_ephemeral = False
    run_launcher = _Launcher()

    def __init__(self):
        self.runs = []
        self.submitted = []
        self.logs = []
        self.run_coordinator = QueuedRunCoordinator(self)

    def get_runs(self, **_kwargs):
        return list(self.runs)

    def create_run_for_job(self, _job, **kwargs):
        run = SimpleNamespace(
            run_id=f"run-{len(self.runs) + 1}",
            tags=kwargs["tags"],
            status="NOT_STARTED",
            remote_job_origin=kwargs.get("remote_job_origin"),
            job_code_origin=kwargs.get("job_code_origin"),
            create_timestamp=float(len(self.runs)),
        )
        self.runs.append(run)
        return run

    def submit_run(self, run_id):
        self.submitted.append(run_id)
        run = self.get_run_by_id(run_id)
        if run is not None:
            run.status = "QUEUED"

    def get_run_by_id(self, run_id):
        return next((run for run in self.runs if run.run_id == run_id), None)

    def all_logs(self, _run_id):
        return list(self.logs)


class _Reconstructed:
    def get_python_origin(self):
        return object()


@pytest.fixture
def durable(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setenv("ICTUS_DAGSTER_CODE_LOCATION", "ictus")
    monkeypatch.setenv("ICTUS_DAGSTER_REPOSITORY", "ictus_defs")
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


def test_crash_between_record_and_enqueue_is_recovered(durable, monkeypatch) -> None:
    calls = []
    real_submit = submission._submit_to_dagster

    def flaky(instance, run_id):
        calls.append(run_id)
        if len(calls) == 1:
            raise RuntimeError("simulated crash after record, before enqueue")
        return real_submit(instance, run_id)

    monkeypatch.setattr(submission, "_submit_to_dagster", flaky)

    with pytest.raises(RuntimeError, match="simulated crash"):
        submission.submit_intent(_intent(), instance=durable)
    # The run is durably recorded but never handed to the coordinator.
    assert len(durable.runs) == 1
    assert durable.submitted == []

    receipt = submission.submit_intent(_intent(), instance=durable)
    assert receipt["receipt_id"] == durable.runs[0].run_id
    assert len(durable.runs) == 1
    assert durable.submitted == [durable.runs[0].run_id]


def test_recording_without_origins_fails_closed(durable) -> None:
    run = SimpleNamespace(run_id="broken", remote_job_origin=None, job_code_origin=object())
    with pytest.raises(submission.SubmissionConfigurationError, match="remote_job_origin"):
        submission._require_run_origins(run)


def test_duplicate_tagged_runs_reconcile_to_the_oldest_match(durable) -> None:
    tags = {
        submission.IDENTITY_TAG: "attempt:receipt-test",
        submission.DIGEST_TAG: submission.intent_digest(_intent()),
        submission.SUBMISSION_TAG: submission.SUBMISSION_PROTOCOL,
    }
    durable.runs.append(
        SimpleNamespace(run_id="late", tags=tags, status="QUEUED", create_timestamp=2.0)
    )
    durable.runs.append(
        SimpleNamespace(run_id="early", tags=tags, status="QUEUED", create_timestamp=1.0)
    )
    receipt = submission.submit_intent(_intent(), instance=durable)
    assert receipt["receipt_id"] == "early"


def test_duplicate_tagged_runs_with_conflicting_digests_fail_closed(durable) -> None:
    tags = {
        submission.IDENTITY_TAG: "attempt:receipt-test",
        submission.DIGEST_TAG: "0" * 64,
        submission.SUBMISSION_TAG: submission.SUBMISSION_PROTOCOL,
    }
    durable.runs.append(
        SimpleNamespace(run_id="other", tags=tags, status="QUEUED", create_timestamp=1.0)
    )
    with pytest.raises(submission.SubmissionConflict, match="different digest"):
        submission.submit_intent(_intent(), instance=durable)


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
    with pytest.raises(submission.SubmissionConfigurationError, match="independent"):
        submission.production_instance(
            SimpleNamespace(is_ephemeral=False, run_launcher=launcher)
        )


@pytest.mark.parametrize(
    "coordinator",
    [None, type("DefaultRunCoordinator", (), {})()],
)
def test_production_submission_requires_the_queued_coordinator(
    monkeypatch, coordinator
) -> None:
    monkeypatch.setenv("DAGSTER_HOME", "/tmp/dagster")
    with pytest.raises(submission.SubmissionConfigurationError, match="QueuedRunCoordinator"):
        submission.production_instance(
            SimpleNamespace(
                is_ephemeral=False,
                run_launcher=_Launcher(),
                run_coordinator=coordinator,
            )
        )


def test_production_submission_can_load_the_configured_instance(monkeypatch, durable) -> None:
    monkeypatch.setattr(submission.DagsterInstance, "get", lambda: durable)
    assert submission.production_instance() is durable


def test_submission_requires_a_configured_code_location(durable, monkeypatch) -> None:
    monkeypatch.delenv("ICTUS_DAGSTER_CODE_LOCATION", raising=False)
    with pytest.raises(
        submission.SubmissionConfigurationError, match="ICTUS_DAGSTER_CODE_LOCATION"
    ):
        submission.submit_intent(_intent(), instance=durable)


def test_submission_defaults_the_repository_name(durable, monkeypatch) -> None:
    monkeypatch.delenv("ICTUS_DAGSTER_REPOSITORY", raising=False)
    submission.submit_intent(
        _intent(intent_id="attempt:default-repo"), instance=durable
    )
    origin = durable.runs[0].remote_job_origin.repository_origin
    assert origin.repository_name == submission.DEFAULT_REPOSITORY_NAME


def test_run_order_key_handles_missing_or_invalid_timestamps() -> None:
    missing = SimpleNamespace(run_id="a")
    invalid = SimpleNamespace(run_id="b", create_timestamp="not-a-number")
    assert submission._run_order_key(missing) == (0.0, "a")
    assert submission._run_order_key(invalid) == (0.0, "b")


def test_locking_fails_closed_without_fcntl(durable, monkeypatch) -> None:
    monkeypatch.setattr(submission, "fcntl", None)
    with pytest.raises(submission.LockUnavailableError):
        submission.submit_intent(_intent(), instance=durable)


def test_unknown_receipt_fails_closed(durable) -> None:
    with pytest.raises(submission.UnknownReceipt):
        submission.query_receipt("missing", instance=durable)

    with pytest.raises(submission.UnknownReceipt, match="non-empty"):
        submission.query_receipt({}, instance=durable)
    with pytest.raises(submission.UnknownReceipt, match="non-empty"):
        submission.query_receipt({"receipt_id": 42}, instance=durable)
    with pytest.raises(submission.UnknownReceipt, match="non-empty"):
        submission.query_receipt(42, instance=durable)


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
    assert pending["receipt"]["receipt_id"] == "run-1"
    assert pending["receipt"]["status"] == "queued"
    assert "result" not in pending
    assert "cancellation" not in pending

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


def test_submit_rejects_an_instance_without_a_launcher() -> None:
    with pytest.raises(submission.SubmissionConfigurationError, match="submit_run"):
        submission._submit_to_dagster(SimpleNamespace(), "run")


def test_reload_falls_back_when_a_run_disappears() -> None:
    run = SimpleNamespace(run_id="gone")
    instance = SimpleNamespace(get_run_by_id=lambda _run_id: None)
    assert submission._reload(instance, run) is run


def test_submission_builds_non_empty_resource_config(durable) -> None:
    receipt = submission.submit_intent(
        _intent(intent_id="attempt:configured", arguments={"fail_hard": True}),
        instance=durable,
    )
    assert receipt["receipt_id"] == "run-1"


def test_submission_carries_both_required_run_origins(durable) -> None:
    submission.submit_intent(_intent(), instance=durable)
    run = durable.runs[0]
    assert run.remote_job_origin is not None
    assert run.job_code_origin is not None


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
    assert response["cancellation"] == {
        "requested": True,
        "accepted": True,
        "status": "canceled",
    }


def test_cancellation_reports_a_request_that_dagster_declines(durable) -> None:
    receipt = submission.submit_intent(_intent(), instance=durable)
    durable.runs[0].status = "SUCCESS"

    def declining_cancel(_run_id):
        return False

    durable.run_coordinator.cancel_run = declining_cancel
    response = submission.cancel_receipt(receipt, instance=durable)
    assert response["state"] == "terminal"
    assert response["cancellation"]["requested"] is True
    assert response["cancellation"]["accepted"] is False


def test_cancellation_requires_a_cancellable_coordinator(durable) -> None:
    receipt = submission.submit_intent(_intent(), instance=durable)

    class QueuedRunCoordinator:  # same supported name, but no cancel_run
        pass

    durable.run_coordinator = QueuedRunCoordinator()
    with pytest.raises(submission.SubmissionConfigurationError, match="cancel_run"):
        submission.cancel_receipt(receipt, instance=durable)


def test_cancellation_rejects_malformed_receipts(durable) -> None:
    with pytest.raises(submission.UnknownReceipt):
        submission.cancel_receipt({}, instance=durable)


def _schema_validator(schema_name: str):
    """Validate a response against the versioned JSON Schema (source of truth)."""
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource

    from conftest import CONTRACTS_DIR

    schemas = {
        path.name: json.loads(path.read_text())
        for path in CONTRACTS_DIR.glob("*.schema.json")
    }
    registry = Registry().with_resources(
        [(schema["$id"], Resource.from_contents(schema)) for schema in schemas.values()]
    )
    return Draft202012Validator(schemas[schema_name], registry=registry)


def test_api_responses_conform_to_their_versioned_schemas(durable) -> None:
    receipt = submission.submit_intent(_intent(), instance=durable)
    _schema_validator("execution-receipt.schema.json").validate(receipt)

    pending = submission.query_receipt(receipt, instance=durable)
    _schema_validator("execution-status.schema.json").validate(pending)

    durable.runs[0].status = "SUCCESS"
    terminal = submission.query_receipt(receipt, instance=durable)
    _schema_validator("execution-status.schema.json").validate(terminal)


def test_cancel_response_conforms_to_its_versioned_schema(durable) -> None:
    receipt = submission.submit_intent(
        _intent(intent_id="attempt:cancel-schema"), instance=durable
    )
    response = submission.cancel_receipt(receipt, instance=durable)
    _schema_validator("execution-status.schema.json").validate(response)
