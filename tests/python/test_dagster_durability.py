"""M1 acceptance: Dagster owns durable execution, retries and run state."""

from __future__ import annotations

from dagster import DagsterInstance

from agentic_dagster.jobs.capability_job import capability_execution_job
from agentic_dagster.resources.execution_settings import ExecutionSettings


def _run(**settings):
    instance = DagsterInstance.ephemeral()
    result = capability_execution_job.execute_in_process(
        resources={"settings": ExecutionSettings(**settings)},
        instance=instance,
        raise_on_error=False,
    )
    return result, instance


def test_clean_run_succeeds_on_first_attempt() -> None:
    result, _ = _run()
    assert result.success
    assert result.output_for_node("finalize_op")["execute_attempts"] == 1


def test_retry_recovers_within_the_same_run() -> None:
    result, _ = _run(fail_until_attempt=2)
    assert result.success
    assert result.output_for_node("finalize_op")["execute_attempts"] == 3


def test_hard_failure_is_persisted_and_inspectable() -> None:
    result, instance = _run(fail_hard=True)
    assert not result.success
    assert "execute_op" in result.get_failed_step_keys()
    # The failed run is a durable record, not a transient exception.
    assert instance.get_run_record_by_id(result.run_id) is not None
    assert result.get_run_failure_event() is not None


def test_verification_failure_is_a_distinct_failed_step() -> None:
    result, _ = _run(fail_verify=True)
    assert not result.success
    assert "verify_op" in result.get_failed_step_keys()


def test_re_execution_is_a_new_durable_run() -> None:
    first, first_instance = _run()
    second, second_instance = _run()
    assert first.success and second.success
    assert first.run_id != second.run_id
    assert first_instance.get_run_record_by_id(first.run_id) is not None
    assert second_instance.get_run_record_by_id(second.run_id) is not None


def test_job_graph_is_generic_and_ordered() -> None:
    # The generic graph must stay prepare -> execute -> verify -> finalize.
    assert set(capability_execution_job.graph.node_dict) == {
        "prepare_op",
        "execute_op",
        "verify_op",
        "finalize_op",
    }
