"""Fail-closed routing for the software implementation/publication capabilities.

These tests pin the operator-approved routing option A: both capabilities get
explicit workflow names, but the jobs are placeholders that always fail closed
and can never report success.  The generic capability mapper keeps rejecting
them, and the durable submission contract is unchanged.
"""

from __future__ import annotations

import pytest
from dagster import (
    DagsterInstance,
    DagsterInvalidConfigError,
    DagsterInvariantViolationError,
    RunsFilter,
    reconstructable,
)
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.execution.api import execute_run

import tactus_dummy_location as dummy
from ictus_dagster import bridge, submission
from ictus_dagster.adapters.contracts import validate_result
from ictus_dagster.adapters.intent_config import (
    CAPABILITY_WORKFLOW_CAPABILITIES,
    KNOWN_UNSUPPORTED,
    SUPPORTED_CAPABILITIES,
    UnsupportedCapability,
    settings_from_intent,
    software_agent_settings,
    software_agent_settings_from_intent,
    software_publish_settings,
    software_publish_settings_from_intent,
)
from ictus_dagster.adapters.result_mapping import execution_result_from_run
from ictus_dagster.adapters.workflow_registry import resolve_workflow

CODE_LOCATION = "ictus"
REPOSITORY = "__repository__"

SOFTWARE_ROUTES = [
    pytest.param(
        "software.implement",
        "software_agent_job",
        "software_agent_settings",
        "software_agent_placeholder_op",
        dummy.software_agent_job_factory,
        id="implement",
    ),
    pytest.param(
        "software.promote",
        "software_publish_job",
        "software_publish_settings",
        "software_publish_placeholder_op",
        dummy.software_publish_job_factory,
        id="promote",
    ),
]


def _intent(capability: str, *, intent_id: str = "attempt:software", arguments=None) -> dict:
    return {
        "schema_version": 1,
        "intent_id": intent_id,
        "capability": capability,
        "target": {"type": "task", "id": "task-software"},
        "arguments": arguments or {},
        "requested_by": {"provider": "rules.v1", "decision_id": "decision-software"},
    }


@pytest.fixture
def persistent_instance(monkeypatch, tmp_path):
    (tmp_path / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setenv(submission.CODE_LOCATION_ENV, CODE_LOCATION)
    monkeypatch.setenv(submission.REPOSITORY_ENV, REPOSITORY)
    instance = DagsterInstance.get()
    try:
        yield instance
    finally:
        instance.dispose()


def _execute_recorded_run(instance, run_id: str, factory):
    run = instance.get_run_by_id(run_id)
    instance.report_dagster_event(
        DagsterEvent(
            event_type_value=DagsterEventType.RUN_STARTING.value,
            job_name=run.job_name,
        ),
        run_id=run_id,
    )
    return execute_run(
        reconstructable(factory),
        instance.get_run_by_id(run_id),
        instance,
        raise_on_error=False,
    )


def _failure_metadata(instance, run_id: str) -> dict:
    for record in instance.all_logs(run_id):
        event = getattr(record, "dagster_event", None)
        if getattr(event, "event_type_value", None) != "STEP_FAILURE":
            continue
        failure = getattr(event.event_specific_data, "user_failure_data", None)
        if failure is not None:
            return failure.metadata
    raise AssertionError(f"run {run_id} has no user Failure metadata")


@pytest.mark.parametrize(
    ("capability", "job_name", "resource_key", "_op_name", "_factory"),
    SOFTWARE_ROUTES,
)
def test_software_capabilities_route_to_distinct_explicit_jobs(
    capability, job_name, resource_key, _op_name, _factory
) -> None:
    spec = resolve_workflow(capability)
    assert spec.job.name == job_name
    assert spec.resource_key == resource_key
    assert capability in SUPPORTED_CAPABILITIES
    assert capability not in CAPABILITY_WORKFLOW_CAPABILITIES
    # The capability-specific mapper has a known, explicit config shape.
    assert spec.settings_from_arguments({}) == {
        "capability": capability,
        "placeholder": True,
    }


def test_the_two_software_routes_are_distinct() -> None:
    implement = resolve_workflow("software.implement")
    promote = resolve_workflow("software.promote")
    assert implement.job.name != promote.job.name
    assert implement.resource_key != promote.resource_key
    assert implement.job.name == "software_agent_job"
    assert promote.job.name == "software_publish_job"


def test_generic_settings_mapper_still_rejects_software_capabilities() -> None:
    for capability in ("software.implement", "software.promote"):
        assert capability in KNOWN_UNSUPPORTED
        with pytest.raises(UnsupportedCapability, match="not owned"):
            settings_from_intent(_intent(capability))


def test_software_specific_mappers_are_explicit_and_fail_closed_elsewhere() -> None:
    assert software_agent_settings({"ignored": "value"}) == {
        "capability": "software.implement",
        "placeholder": True,
    }
    assert software_publish_settings({"ignored": "value"}) == {
        "capability": "software.promote",
        "placeholder": True,
    }
    assert software_agent_settings_from_intent(_intent("software.implement")) == {
        "capability": "software.implement",
        "placeholder": True,
    }
    assert software_publish_settings_from_intent(_intent("software.promote")) == {
        "capability": "software.promote",
        "placeholder": True,
    }
    with pytest.raises(UnsupportedCapability, match="not a software-agent capability"):
        software_agent_settings_from_intent(_intent("software.promote"))
    with pytest.raises(UnsupportedCapability, match="not a software-publish capability"):
        software_publish_settings_from_intent(_intent("software.implement"))


@pytest.mark.parametrize(
    ("capability", "_job_name", "resource_key", "op_name", "_factory"),
    SOFTWARE_ROUTES,
)
def test_placeholder_jobs_fail_closed_with_a_typed_non_retryable_failure(
    capability, _job_name, resource_key, op_name, _factory
) -> None:
    spec = resolve_workflow(capability)
    config = spec.settings_from_arguments({})
    instance = DagsterInstance.ephemeral()
    try:
        result = spec.job.execute_in_process(
            resources={resource_key: spec.resource_cls(**config)},
            instance=instance,
            raise_on_error=False,
        )
        assert result.success is False
        assert result.get_failed_step_keys() == {op_name}

        metadata = _failure_metadata(instance, result.run_id)
        assert metadata["ictus.capability"].text == capability
        assert metadata["ictus.failure_category"].text == "INTEGRATION_CONFLICT"
        assert metadata["ictus.placeholder"].value is True
        assert metadata["ictus.retryable"].value is False
        assert metadata["ictus.job"].text == spec.job.name

        mapped = execution_result_from_run(
            execution_id=result.run_id,
            intent_id="attempt:software",
            success=result.success,
            failed_step=op_name,
        )
        assert validate_result(mapped) is mapped
        assert mapped["status"] == "failed"
        assert mapped["observation"]["category"] == "INTEGRATION_CONFLICT"
    finally:
        instance.dispose()


@pytest.mark.parametrize(
    ("capability", "_job_name", "_resource_key", "_op_name", "_factory"),
    SOFTWARE_ROUTES,
)
def test_bridge_executes_software_route_as_a_failed_result(
    capability, _job_name, _resource_key, _op_name, _factory
) -> None:
    instance = DagsterInstance.ephemeral()
    try:
        result = bridge.execute_intent(_intent(capability), instance=instance)
    finally:
        instance.dispose()
    assert result["status"] == "failed"
    assert result["observation"]["category"] == "INTEGRATION_CONFLICT"


@pytest.mark.parametrize(
    ("capability", "job_name", "resource_key", "_op_name", "factory"),
    SOFTWARE_ROUTES,
)
def test_dummy_tactus_like_location_executes_the_recorded_run(
    persistent_instance, capability, job_name, resource_key, _op_name, factory
) -> None:
    intent = _intent(capability)
    receipt = submission.submit_intent(intent, instance=persistent_instance)
    run = persistent_instance.get_run_by_id(receipt["receipt_id"])
    assert run.job_name == job_name
    assert set(run.run_config["resources"]) == {resource_key}
    # The independent location registers the same routed job name.
    assert dummy.defs.get_job_def(job_name).name == job_name

    result = _execute_recorded_run(persistent_instance, receipt["receipt_id"], factory)
    assert result.success is False

    status = submission.query_receipt(receipt, instance=persistent_instance)
    assert status["state"] == "terminal"
    assert status["result"]["status"] == "failed"
    assert status["result"]["observation"]["category"] == "INTEGRATION_CONFLICT"


def test_dummy_location_mismatch_fails_closed() -> None:
    # A location that does not register the routed job name cannot resolve it.
    with pytest.raises(DagsterInvariantViolationError):
        dummy.mismatched_defs.get_job_def("software_publish_job")

    # A location that reuses the name with a different resource shape rejects
    # the unchanged submission run config before executing any step.
    spec = resolve_workflow("software.implement")
    run_config = submission._run_config(spec, _intent("software.implement"))
    with pytest.raises(DagsterInvalidConfigError):
        dummy.mismatched_defs.get_job_def("software_agent_job").execute_in_process(
            run_config=run_config,
            raise_on_error=False,
        )


def test_unknown_capability_fails_before_a_run_is_recorded(persistent_instance) -> None:
    with pytest.raises(UnsupportedCapability, match="not owned by this execution backend"):
        submission.submit_intent(
            _intent("software.unknown", intent_id="attempt:unknown"),
            instance=persistent_instance,
        )
    runs = persistent_instance.get_runs(
        filters=RunsFilter(tags={submission.IDENTITY_TAG: "attempt:unknown"})
    )
    assert list(runs) == []
