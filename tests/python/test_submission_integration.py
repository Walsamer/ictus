"""Real-instance integration tests for the durable submission boundary.

These tests use a **persistent** ``DagsterInstance`` configured exactly as the
supported production deployment requires: ``DAGSTER_HOME`` storage,
``QueuedRunCoordinator`` and ``DefaultRunLauncher``.  The unit tests with
doubles elsewhere pin the decision logic; these tests are the evidence that the
boundary works against real Dagster storage, real queue state, real event logs
and real process restarts.

The ``dagster-daemon`` is *not* started here (it would need a gRPC code server).
Instead the helper :func:`_execute_queued_run` performs the daemon's documented
hand-off in-process: it replays the ``RUN_STARTING`` event the daemon emits and
then executes the already-recorded run against the same instance.  This keeps
the test hermetic while exercising the real run record, events and status.

Not covered here (see ``docs/DURABLE_SUBMISSION.md``): a live daemon + gRPC code
server, and multi-host submission (the local ``fcntl`` lock is single-host only).
"""

from __future__ import annotations

import json
import multiprocessing
import os
import signal

import pytest
from dagster import DagsterInstance, DagsterRunStatus, RunsFilter

from ictus_dagster import submission
from ictus_dagster.adapters.workflow_registry import resolve_workflow

CODE_LOCATION = "ictus"
REPOSITORY = "__repository__"


def _intent(**overrides):
    payload = {
        "schema_version": 1,
        "intent_id": "attempt:integration",
        "capability": "demo.verify",
        "target": {"type": "task", "id": "task-1"},
        "arguments": {},
        "requested_by": {"provider": "rules.v1", "decision_id": "decision-1"},
    }
    payload.update(overrides)
    return payload


def _dagster_home(root, **config):
    lines = ["telemetry:", "  enabled: false"]
    for key, value in config.items():
        lines.append(f"{key}:")
        lines.append(f"  {value}")
    (root / "dagster.yaml").write_text("\n".join(lines) + "\n")
    return root


@pytest.fixture
def persistent_instance(monkeypatch, tmp_path):
    _dagster_home(tmp_path)
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setenv(submission.CODE_LOCATION_ENV, CODE_LOCATION)
    monkeypatch.setenv(submission.REPOSITORY_ENV, REPOSITORY)
    instance = DagsterInstance.get()
    try:
        yield instance
    finally:
        instance.dispose()


def _runs_for(instance, identity):
    return list(
        instance.get_runs(filters=RunsFilter(tags={submission.IDENTITY_TAG: identity}))
    )


def _submit_in_child(home: str, payload: dict, start, send) -> None:
    """Open the real instance in a separate process and submit one intent."""
    os.environ["DAGSTER_HOME"] = home
    os.environ[submission.CODE_LOCATION_ENV] = CODE_LOCATION
    os.environ[submission.REPOSITORY_ENV] = REPOSITORY
    instance = DagsterInstance.get()
    try:
        start.wait(timeout=20)
        send.send({"receipt": submission.submit_intent(payload, instance=instance)})
    except Exception as error:  # pragma: no cover - asserted by the parent
        send.send({"error": type(error).__name__, "message": str(error)})
    finally:
        instance.dispose()


def _record_without_enqueueing_in_child(home: str, payload: dict, send) -> None:
    """Stop at the durable-record boundary for a real process-crash test."""
    os.environ["DAGSTER_HOME"] = home
    os.environ[submission.CODE_LOCATION_ENV] = CODE_LOCATION
    os.environ[submission.REPOSITORY_ENV] = REPOSITORY
    instance = DagsterInstance.get()
    try:
        spec = resolve_workflow(payload["capability"])
        run = submission._create_run(
            instance,
            spec,
            payload,
            payload["intent_id"],
            submission.intent_digest(payload),
        )
        send.send(run.run_id)
        # The parent terminates this process here, before a queue hand-off.
        multiprocessing.Event().wait(60)
    finally:
        instance.dispose()


def _execute_queued_run(instance, run_id):
    """Replay the daemon's hand-off and execute the recorded run in-process."""
    from dagster import reconstructable
    from dagster._core.events import DagsterEvent, DagsterEventType
    from dagster._core.execution.api import execute_run

    run = instance.get_run_by_id(run_id)
    spec = resolve_workflow(run.tags["ictus.capability"])
    instance.report_dagster_event(
        DagsterEvent(
            event_type_value=DagsterEventType.RUN_STARTING.value,
            job_name=run.job_name,
        ),
        run_id=run_id,
    )
    return execute_run(
        reconstructable(spec.job_factory),
        instance.get_run_by_id(run_id),
        instance,
        raise_on_error=False,
    )


def test_submit_persists_both_origins_and_enqueues(persistent_instance) -> None:
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)

    run = persistent_instance.get_run_by_id(receipt["receipt_id"])
    assert run is not None
    # Both origins the launcher needs are persisted and valid.
    assert run.remote_job_origin is not None
    assert run.remote_job_origin.location_name == CODE_LOCATION
    assert run.job_code_origin is not None
    assert run.status == DagsterRunStatus.QUEUED
    assert run.tags["dagster/code_location"] == CODE_LOCATION
    assert run.tags["dagster/max_retries"] == "0"
    # The receipt itself conforms to the versioned contract.
    from ictus_dagster.adapters.contracts import validate_receipt

    assert validate_receipt(receipt) is receipt


def test_same_intent_submitted_twice_creates_one_run(persistent_instance) -> None:
    first = submission.submit_intent(_intent(), instance=persistent_instance)
    second = submission.submit_intent(_intent(), instance=persistent_instance)

    assert first["receipt_id"] == second["receipt_id"]
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1


def test_independent_processes_converge_on_one_persistent_receipt(persistent_instance) -> None:
    """The host lock covers the real SQLite run store, not an instance double."""
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    receivers, processes = [], []
    for _ in range(2):
        receive, send = context.Pipe(duplex=False)
        process = context.Process(
            target=_submit_in_child,
            args=(os.environ["DAGSTER_HOME"], _intent(), start, send),
        )
        process.start()
        send.close()
        receivers.append(receive)
        processes.append(process)

    start.set()
    responses = [receive.recv() for receive in receivers]
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    assert all("error" not in response for response in responses)
    receipt_ids = {response["receipt"]["receipt_id"] for response in responses}
    assert len(receipt_ids) == 1
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1

    receive, send = context.Pipe(duplex=False)
    conflicting = context.Process(
        target=_submit_in_child,
        args=(
            os.environ["DAGSTER_HOME"],
            _intent(arguments={"fail_hard": True}),
            start,
            send,
        ),
    )
    conflicting.start()
    send.close()
    response = receive.recv()
    conflicting.join(timeout=30)
    assert conflicting.exitcode == 0
    assert response["error"] == "SubmissionConflict"
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1


def test_conflicting_payload_fails_closed(persistent_instance) -> None:
    submission.submit_intent(_intent(), instance=persistent_instance)
    with pytest.raises(submission.SubmissionConflict):
        submission.submit_intent(
            _intent(arguments={"fail_hard": True}), instance=persistent_instance
        )
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1


def test_pending_status_has_no_result(persistent_instance) -> None:
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)

    status = submission.query_receipt(receipt, instance=persistent_instance)
    assert status["state"] == "pending"
    assert status["receipt"]["status"] == "queued"
    assert "result" not in status
    assert "cancellation" not in status


def test_successful_run_reconciles_to_a_terminal_result(persistent_instance) -> None:
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)
    assert submission.query_receipt(receipt, instance=persistent_instance)["state"] == "pending"

    result = _execute_queued_run(persistent_instance, receipt["receipt_id"])
    assert result.success

    status = submission.query_receipt(receipt, instance=persistent_instance)
    assert status["state"] == "terminal"
    assert status["receipt"]["status"] == "success"
    assert status["result"]["status"] == "succeeded"
    assert status["result"]["observation"]["category"] == "SUCCESS"
    assert any(item["kind"] == "dagster_run" for item in status["result"]["evidence"])


def test_failed_run_reconciles_to_a_process_crash(persistent_instance) -> None:
    receipt = submission.submit_intent(
        _intent(arguments={"fail_hard": True}), instance=persistent_instance
    )
    result = _execute_queued_run(persistent_instance, receipt["receipt_id"])
    assert not result.success

    status = submission.query_receipt(receipt, instance=persistent_instance)
    assert status["state"] == "terminal"
    assert status["result"]["status"] == "failed"
    assert status["result"]["observation"]["category"] == "PROCESS_CRASH"


def test_cancelling_a_queued_run_is_terminal(persistent_instance) -> None:
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)

    response = submission.cancel_receipt(receipt, instance=persistent_instance)
    assert response["state"] == "terminal"
    assert response["result"]["status"] == "cancelled"
    assert response["cancellation"] == {
        "requested": True,
        "accepted": True,
        "status": "canceled",
    }
    assert (
        persistent_instance.get_run_by_id(receipt["receipt_id"]).status
        == DagsterRunStatus.CANCELED
    )


def test_crash_after_record_before_enqueue_is_recovered(persistent_instance, monkeypatch) -> None:
    real_submit = submission._submit_to_dagster

    def crash(_instance, _run_id):
        raise submission.SubmissionError("simulated crash between record and enqueue")

    monkeypatch.setattr(submission, "_submit_to_dagster", crash)
    with pytest.raises(submission.SubmissionError, match="simulated crash"):
        submission.submit_intent(_intent(), instance=persistent_instance)

    stranded = _runs_for(persistent_instance, "attempt:integration")
    assert len(stranded) == 1
    assert stranded[0].status == DagsterRunStatus.NOT_STARTED

    monkeypatch.setattr(submission, "_submit_to_dagster", real_submit)
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)

    assert receipt["receipt_id"] == stranded[0].run_id
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1
    recovered = persistent_instance.get_run_by_id(receipt["receipt_id"])
    assert recovered.status == DagsterRunStatus.QUEUED

    # The recovered run executes exactly once.
    result = _execute_queued_run(persistent_instance, receipt["receipt_id"])
    assert result.success
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1


def test_process_death_after_record_before_queue_handoff_is_recovered(persistent_instance) -> None:
    """Replay repairs the original durable record after an actual process death."""
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_record_without_enqueueing_in_child,
        args=(os.environ["DAGSTER_HOME"], _intent(), send),
    )
    process.start()
    send.close()
    run_id = receive.recv()
    # A harness that ignores SIGTERM (SIG_IGN) makes spawned children inherit
    # that disposition, and then terminate() is a silent no-op.  Detect it and
    # use SIGKILL so the simulated crash is deterministic in every environment.
    if signal.getsignal(signal.SIGTERM) == signal.SIG_IGN:
        process.kill()
    else:
        process.terminate()
    process.join(timeout=15)
    if process.is_alive():  # pragma: no cover - defensive; SIGKILL cannot be caught
        process.kill()
        process.join(timeout=10)
    assert process.exitcode is not None and process.exitcode != 0

    stranded = persistent_instance.get_run_by_id(run_id)
    assert stranded is not None and stranded.status == DagsterRunStatus.NOT_STARTED
    restart = context.Event()
    restart.set()
    receive, send = context.Pipe(duplex=False)
    replay = context.Process(
        target=_submit_in_child,
        args=(os.environ["DAGSTER_HOME"], _intent(), restart, send),
    )
    replay.start()
    send.close()
    response = receive.recv()
    replay.join(timeout=30)
    assert replay.exitcode == 0
    assert "error" not in response
    receipt = response["receipt"]
    assert receipt["receipt_id"] == run_id
    assert persistent_instance.get_run_by_id(run_id).status == DagsterRunStatus.QUEUED
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1


def test_lost_acknowledgement_after_enqueue_does_not_resubmit(
    persistent_instance, monkeypatch
) -> None:
    first = submission.submit_intent(_intent(), instance=persistent_instance)
    # Dagster 1.13.25 transitions the run NOT_STARTED -> QUEUED inside
    # submit_run; a successful enqueue is therefore durable and observable.
    assert (
        persistent_instance.get_run_by_id(first["receipt_id"]).status
        == DagsterRunStatus.QUEUED
    )

    resubmits = []
    real_submit = submission._submit_to_dagster

    def counting(instance, run_id):
        resubmits.append(run_id)
        return real_submit(instance, run_id)

    monkeypatch.setattr(submission, "_submit_to_dagster", counting)

    # The first acknowledgement is lost; the caller retries from a fresh
    # instance.  Because the run is already QUEUED it must not be re-enqueued.
    restarted = DagsterInstance.get()
    second = submission.submit_intent(_intent(), instance=restarted)

    assert second["receipt_id"] == first["receipt_id"]
    assert resubmits == []
    assert len(_runs_for(persistent_instance, "attempt:integration")) == 1


def test_state_survives_a_process_restart(persistent_instance) -> None:
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)
    assert _execute_queued_run(persistent_instance, receipt["receipt_id"]).success
    before = submission.query_receipt(receipt, instance=persistent_instance)

    # A fresh instance object against the same DAGSTER_HOME is the restart
    # boundary (no in-memory state is carried over).
    restarted = DagsterInstance.get()
    assert restarted is not persistent_instance

    after = submission.query_receipt(receipt, instance=restarted)
    assert after == before
    # A duplicate submission from the restarted process still reconciles.
    assert (
        submission.submit_intent(_intent(), instance=restarted)["receipt_id"]
        == receipt["receipt_id"]
    )


def test_unsupported_coordinator_is_rejected(monkeypatch, tmp_path) -> None:
    _dagster_home(
        tmp_path,
        run_coordinator=(
            "module: dagster._core.run_coordinator\n"
            "  class: DefaultRunCoordinator\n"
            "  config: {}"
        ),
    )
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setenv(submission.CODE_LOCATION_ENV, CODE_LOCATION)
    instance = DagsterInstance.get()
    try:
        with pytest.raises(submission.SubmissionConfigurationError, match="QueuedRunCoordinator"):
            submission.production_instance(instance)
    finally:
        instance.dispose()


def test_query_rejects_a_run_that_is_not_an_ictus_receipt(persistent_instance) -> None:
    from dagster import reconstructable

    from ictus_dagster.adapters.workflow_registry import capability_execution_job_factory

    spec = resolve_workflow("demo.verify")
    run = persistent_instance.create_run_for_job(
        spec.job,
        run_config={"resources": {"settings": {"config": {}}}},
        tags={"unrelated": "tag"},
        job_code_origin=reconstructable(capability_execution_job_factory).get_python_origin(),
    )
    with pytest.raises(submission.UnknownReceipt):
        submission.query_receipt(run.run_id, instance=persistent_instance)


def test_receipt_protocol_tag_is_the_ownership_marker(persistent_instance) -> None:
    receipt = submission.submit_intent(_intent(), instance=persistent_instance)
    run = persistent_instance.get_run_by_id(receipt["receipt_id"])
    assert run.tags[submission.SUBMISSION_TAG] == submission.SUBMISSION_PROTOCOL
    # A receipt round-trips through JSON (the bridge's actual transport).
    assert json.loads(json.dumps(receipt)) == receipt
