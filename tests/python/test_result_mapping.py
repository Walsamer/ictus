"""Dagster run outcome -> generic ExecutionResult and ExecutionObservation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from ictus_dagster import submission
from ictus_dagster.adapters.contracts import ContractError, validate_result
from ictus_dagster.adapters.result_mapping import (
    execution_result_from_run,
    result_to_observation,
)
from conftest import CONTRACTS_DIR


def _schema(name: str) -> Draft202012Validator:
    schemas = {
        path.name: json.loads(path.read_text())
        for path in CONTRACTS_DIR.glob("*.schema.json")
    }
    registry = Registry().with_resources(
        [(s["$id"], Resource.from_contents(s)) for s in schemas.values()]
    )
    return Draft202012Validator(schemas[name], registry=registry)


def _result_schema() -> Draft202012Validator:
    return _schema("execution-result.schema.json")


def test_success_maps_to_success_category() -> None:
    payload = execution_result_from_run(
        execution_id="exec-1",
        intent_id="intent-1",
        success=True,
    )
    assert payload["status"] == "succeeded"
    assert payload["observation"]["category"] == "SUCCESS"
    _result_schema().validate(payload)


def test_failed_execute_step_maps_to_process_crash() -> None:
    payload = execution_result_from_run(
        execution_id="exec-2",
        intent_id="intent-2",
        success=False,
        failed_step="execute_op",
    )
    assert payload["status"] == "failed"
    assert payload["observation"]["category"] == "PROCESS_CRASH"


def test_failed_verify_step_maps_to_verification_failure() -> None:
    payload = execution_result_from_run(
        execution_id="exec-3",
        intent_id="intent-3",
        success=False,
        failed_step="verify_op",
    )
    assert payload["observation"]["category"] == "VERIFICATION_FAILURE"


def test_data_quality_step_categories() -> None:
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="check_op"
        )["observation"]["category"]
        == "VERIFICATION_FAILURE"
    )
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="profile_op"
        )["observation"]["category"]
        == "PROCESS_CRASH"
    )
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="ingest_op"
        )["observation"]["category"]
        == "UNKNOWN"
    )


def test_system_diagnostic_step_categories() -> None:
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="classify_op"
        )["observation"]["category"]
        == "VERIFICATION_FAILURE"
    )
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="inspect_op"
        )["observation"]["category"]
        == "PROCESS_CRASH"
    )
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="collect_op"
        )["observation"]["category"]
        == "UNKNOWN"
    )


def test_unknown_failed_step_and_missing_step_map_to_unknown() -> None:
    assert (
        execution_result_from_run(
            execution_id="e", intent_id="i", success=False, failed_step="mystery_op"
        )["observation"]["category"]
        == "UNKNOWN"
    )
    assert (
        execution_result_from_run(execution_id="e", intent_id="i", success=False)[
            "observation"
        ]["category"]
        == "UNKNOWN"
    )


def test_explicit_failure_category_wins() -> None:
    payload = execution_result_from_run(
        execution_id="exec-4",
        intent_id="intent-4",
        success=False,
        failed_step="execute_op",
        failure_category="WORKER_TIMEOUT",
    )
    assert payload["observation"]["category"] == "WORKER_TIMEOUT"


def test_known_terminal_runtime_statuses_preserve_timeout_and_cancellation() -> None:
    timed_out = execution_result_from_run(
        execution_id="timeout", intent_id="intent", success=False, run_status="TIMED_OUT"
    )
    cancelled = execution_result_from_run(
        execution_id="cancel", intent_id="intent", success=False, run_status="CANCELED"
    )
    assert timed_out["status"] == "timed_out"
    assert timed_out["observation"]["category"] == "WORKER_TIMEOUT"
    assert cancelled["status"] == "cancelled"
    assert cancelled["observation"]["category"] == "UNKNOWN"


def test_result_always_carries_intent_id_and_schema_version() -> None:
    payload = execution_result_from_run(
        execution_id="exec-5", intent_id="intent-5", success=True
    )
    assert payload["intent_id"] == "intent-5"
    assert payload["schema_version"] == 1


def test_invalid_category_is_rejected() -> None:
    with pytest.raises(ContractError):
        validate_result(
            {
                "schema_version": 1,
                "execution_id": "e",
                "intent_id": "i",
                "status": "succeeded",
                "observation": {"category": "BOGUS"},
            }
        )


# --- ExecutionResult -> ExecutionObservation -------------------------------

FINISHED_AT = "2024-05-01T12:00:00Z"

OBSERVATION_RESULT_CASES = [
    pytest.param(
        dict(
            execution_id="exec-ok",
            intent_id="intent-ok",
            success=True,
            evidence=[{"kind": "dagster_run", "uri": "dagster://runs/exec-ok"}],
        ),
        id="success",
    ),
    pytest.param(
        dict(
            execution_id="exec-crash",
            intent_id="intent-crash",
            success=False,
            failed_step="execute_op",
            evidence=[
                {"kind": "dagster_event_log", "uri": "dagster://runs/exec-crash/events"}
            ],
        ),
        id="process-failure",
    ),
    pytest.param(
        dict(
            execution_id="exec-cancel",
            intent_id="intent-cancel",
            success=False,
            run_status="CANCELED",
        ),
        id="cancellation",
    ),
    pytest.param(
        dict(
            execution_id="exec-empty",
            intent_id="intent-empty",
            success=True,
            evidence=[],
        ),
        id="empty-evidence",
    ),
]


@pytest.mark.parametrize("case", OBSERVATION_RESULT_CASES)
def test_result_to_observation_validates_against_the_observation_schema(case) -> None:
    result = execution_result_from_run(finished_at=FINISHED_AT, **case)
    observation = result_to_observation(result)

    _schema("observation.schema.json").validate(observation)
    assert observation["schema_version"] == 1
    assert observation["observation_id"] == f"ictus-result:{result['execution_id']}"
    assert observation["execution_id"] == result["execution_id"]
    assert observation["intent_id"] == result["intent_id"]
    assert observation["category"] == result["observation"]["category"]
    assert observation["evidence"] == result["evidence"]
    assert observation["observed_at"] == FINISHED_AT


def test_result_to_observation_accepts_explicit_identity_and_timestamp() -> None:
    result = execution_result_from_run(
        execution_id="exec-explicit", intent_id="intent-explicit", success=True
    )
    observation = result_to_observation(
        result, observation_id="observation:1", observed_at=FINISHED_AT
    )
    assert observation["observation_id"] == "observation:1"
    assert observation["observed_at"] == FINISHED_AT
    assert observation["evidence"] == []


def test_result_to_observation_fails_closed_without_a_timestamp() -> None:
    result = execution_result_from_run(
        execution_id="exec-untimed", intent_id="intent-untimed", success=True
    )
    with pytest.raises(ContractError, match="observed_at"):
        result_to_observation(result)


def test_result_to_observation_rejects_an_invalid_result() -> None:
    with pytest.raises(ContractError):
        result_to_observation(
            {
                "schema_version": 1,
                "execution_id": "e",
                "intent_id": "i",
                "status": "succeeded",
                "observation": {"category": "BOGUS"},
                "finished_at": FINISHED_AT,
            }
        )


# --- durable query path ----------------------------------------------------


class QueuedRunCoordinator:
    """Double whose class name matches Dagster's supported coordinator."""


class _QueryInstance:
    """Minimal durable-instance double: only what the query path reads."""

    is_ephemeral = False
    run_launcher = object()
    run_coordinator = QueuedRunCoordinator()

    def __init__(self, run, records):
        self._run = run
        self._records = records

    def get_run_by_id(self, _run_id):
        return self._run

    def all_logs(self, _run_id):
        return []

    def get_run_records(self, **_kwargs):
        return list(self._records)


def _terminal_run() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="run-times",
        status="SUCCESS",
        tags={
            submission.IDENTITY_TAG: "attempt:times",
            submission.DIGEST_TAG: "0" * 64,
            submission.SUBMISSION_TAG: submission.SUBMISSION_PROTOCOL,
            submission.SUBMITTED_AT_TAG: "2024-05-01T10:00:00Z",
        },
        create_timestamp=0.0,
    )


def _record(start, end) -> SimpleNamespace:
    return SimpleNamespace(
        start_time=None if start is None else start.timestamp(),
        end_time=None if end is None else end.timestamp(),
    )


def test_durable_query_reports_the_run_times(monkeypatch) -> None:
    monkeypatch.setenv("DAGSTER_HOME", "/dagster-home")
    instance = _QueryInstance(
        _terminal_run(),
        [
            _record(
                datetime(2024, 5, 1, 11, 0, tzinfo=timezone.utc),
                datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc),
            )
        ],
    )
    status = submission.query_receipt("run-times", instance=instance)

    assert status["result"]["started_at"] == "2024-05-01T11:00:00Z"
    assert status["result"]["finished_at"] == "2024-05-01T12:00:00Z"
    # observed_at derives from the run's own end time, never from wall clock.
    observation = result_to_observation(status["result"])
    assert observation["observed_at"] == status["result"]["finished_at"]
    _schema("observation.schema.json").validate(observation)


def test_durable_query_manufactures_no_timestamp_the_run_does_not_report(
    monkeypatch,
) -> None:
    monkeypatch.setenv("DAGSTER_HOME", "/dagster-home")
    instance = _QueryInstance(_terminal_run(), [_record(None, None)])
    status = submission.query_receipt("run-times", instance=instance)

    assert "started_at" not in status["result"]
    assert "finished_at" not in status["result"]
    with pytest.raises(ContractError, match="observed_at"):
        result_to_observation(status["result"])


def test_run_times_are_absent_without_a_run_record() -> None:
    instance = SimpleNamespace(get_run_records=lambda **_kwargs: [])
    run = SimpleNamespace(run_id="gone")
    assert submission._run_record_times(instance, run) == (None, None)
