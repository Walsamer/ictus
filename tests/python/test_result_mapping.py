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
    seam_evidence_items,
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


@pytest.mark.parametrize(
    ("failed_step", "category"),
    [
        ("software_agent_prepare_op", "INTEGRATION_CONFLICT"),
        ("software_agent_execute_op", "PROCESS_CRASH"),
        ("software_agent_verify_op", "VERIFICATION_FAILURE"),
        ("software_agent_finalize_op", "UNKNOWN"),
        ("software_publish_prepare_op", "INTEGRATION_CONFLICT"),
        ("software_publish_execute_op", "PROCESS_CRASH"),
        ("software_publish_verify_op", "VERIFICATION_FAILURE"),
        ("software_publish_finalize_op", "UNKNOWN"),
    ],
)
def test_software_seam_step_categories(failed_step: str, category: str) -> None:
    payload = execution_result_from_run(
        execution_id="exec-seam",
        intent_id="intent-seam",
        success=False,
        failed_step=failed_step,
    )
    assert payload["status"] == "failed"
    assert payload["observation"]["category"] == category
    _result_schema().validate(payload)


def _step_failure_event(*, reported: str | None) -> SimpleNamespace:
    """A ``STEP_FAILURE`` event shaped the way the durable query path reads it."""
    metadata = (
        {}
        if reported is None
        else {"ictus.failure_category": SimpleNamespace(text=reported)}
    )
    return SimpleNamespace(
        event_type_value="STEP_FAILURE",
        step_key="software_agent_prepare_op",
        event_specific_data=SimpleNamespace(
            error=None, user_failure_data=SimpleNamespace(metadata=metadata)
        ),
    )


def test_a_reported_failure_category_wins_over_the_step_default() -> None:
    reported = submission._reported_failure_category(
        _step_failure_event(reported="WORKER_TIMEOUT")
    )
    assert reported == "WORKER_TIMEOUT"
    # The prepare step's own default is INTEGRATION_CONFLICT; the typed category
    # the step reported is what the result carries.
    payload = execution_result_from_run(
        execution_id="e",
        intent_id="i",
        success=False,
        failed_step="software_agent_prepare_op",
        failure_category=reported,
    )
    assert payload["observation"]["category"] == "WORKER_TIMEOUT"


@pytest.mark.parametrize(
    "reported",
    [
        pytest.param(None, id="no-metadata"),
        pytest.param("NOT_A_CATEGORY", id="unknown"),
    ],
)
def test_an_unknown_reported_category_falls_back_to_the_step_default(
    reported: str | None,
) -> None:
    event = _step_failure_event(reported=reported)
    assert submission._reported_failure_category(event) is None
    payload = execution_result_from_run(
        execution_id="e",
        intent_id="i",
        success=False,
        failed_step="software_agent_prepare_op",
        failure_category=None,
    )
    assert payload["observation"]["category"] == "INTEGRATION_CONFLICT"


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


# --- runtime seam evidence -------------------------------------------------

#: A seam summary as ``seam_runtime.finalize`` emits it.
SEAM_SUMMARY: dict = {
    "capability": "software.implement",
    "job": "software_agent_job",
    "verified": True,
    "outcome": "succeeded",
    "changed_paths": [
        "python/ictus_dagster/seam/port.py",
        "python/ictus_dagster/seam/runtime.py",
    ],
    "patch_digest": "sha256:" + "b" * 64,
    "patch_bytes": 4096,
    "steps_used": 7,
    "duration_seconds": 12.5,
    "exit_status": {"code": 0, "signal": None},
    "termination": {"requested": False, "signals": [], "confirmed": False},
    "verification": {"verified": True, "detail": "tests passed"},
    "runtime": {
        "executable": "stub",
        "version_token": "1",
        "probe": {"ok": True, "detail": ""},
    },
}

SEAM_PATCH_NOTE = (
    "changed_paths=python/ictus_dagster/seam/port.py,"
    "python/ictus_dagster/seam/runtime.py; patch_bytes=4096"
)
SEAM_EXIT_NOTE = (
    "outcome=succeeded; exit_code=0; signal=None; termination_confirmed=False; "
    "steps_used=7; duration_seconds=12.5"
)


def test_seam_evidence_is_empty_without_a_configured_seam() -> None:
    assert seam_evidence_items(None) == []
    assert seam_evidence_items({}) == []
    for seam_evidence in (None, {}):
        payload = execution_result_from_run(
            execution_id="exec-no-seam",
            intent_id="intent-no-seam",
            success=False,
            failed_step="software_agent_prepare_op",
            seam_evidence=seam_evidence,
        )
        assert payload["evidence"] == []
        _result_schema().validate(payload)


def test_seam_evidence_restates_the_runtime_facts() -> None:
    assert seam_evidence_items(SEAM_SUMMARY) == [
        {
            "kind": "runtime_patch",
            "uri": "seam://patch",
            "note": SEAM_PATCH_NOTE,
            # The contract carries the digest without the algorithm prefix.
            "sha256": "b" * 64,
        },
        {
            "kind": "runtime_exit",
            "uri": "seam://exit",
            "note": SEAM_EXIT_NOTE,
        },
    ]

    payload = execution_result_from_run(
        execution_id="exec-seam",
        intent_id="intent-seam",
        success=True,
        evidence=[{"kind": "dagster_run", "uri": "dagster://runs/exec-seam"}],
        seam_evidence=SEAM_SUMMARY,
    )
    _result_schema().validate(payload)
    assert payload["evidence"][0] == {
        "kind": "dagster_run",
        "uri": "dagster://runs/exec-seam",
    }
    assert payload["evidence"][1:] == seam_evidence_items(SEAM_SUMMARY)


def test_seam_evidence_reports_an_empty_attempt_faithfully() -> None:
    items = seam_evidence_items(
        {**SEAM_SUMMARY, "changed_paths": [], "patch_digest": None, "patch_bytes": 0}
    )
    assert items[0] == {
        "kind": "runtime_patch",
        "uri": "seam://patch",
        "note": "changed_paths=none; patch_bytes=0",
    }
    assert items[1]["note"] == SEAM_EXIT_NOTE


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
