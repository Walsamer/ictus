"""Durable execution of the second (non-software) domain workflow."""

from __future__ import annotations

from dagster import DagsterInstance

from ictus_dagster.jobs.data_quality_job import data_quality_job
from ictus_dagster.resources.data_quality_settings import DataQualitySettings


def _run(**settings):
    instance = DagsterInstance.ephemeral()
    result = data_quality_job.execute_in_process(
        resources={"dq_settings": DataQualitySettings(**settings)},
        instance=instance,
        raise_on_error=False,
    )
    return result, instance


def test_clean_run_publishes_a_summary() -> None:
    result, _ = _run(dataset="synthetic.orders", rows=42)
    assert result.success
    summary = result.output_for_node("publish_op")
    assert summary == {
        "dataset": "synthetic.orders",
        "rows": 42,
        "passed": True,
        "profile_attempts": 1,
        "null_ratio": 0.01,
    }


def test_profile_retry_recovers_within_the_run() -> None:
    result, _ = _run(fail_until_attempt=2)
    assert result.success
    assert result.output_for_node("publish_op")["profile_attempts"] == 3


def test_check_failure_is_a_distinct_failed_step() -> None:
    result, instance = _run(fail_on_check=True)
    assert not result.success
    assert "check_op" in result.get_failed_step_keys()
    assert instance.get_run_record_by_id(result.run_id) is not None


def test_min_rows_failure_fails_the_check() -> None:
    result, _ = _run(rows=0, min_rows=5)
    assert not result.success
    assert "check_op" in result.get_failed_step_keys()


def test_ingest_failure_fails_the_first_step() -> None:
    result, _ = _run(fail_ingest=True)
    assert not result.success
    assert "ingest_op" in result.get_failed_step_keys()


def test_negative_rows_fail_ingest() -> None:
    result, _ = _run(rows=-1)
    assert not result.success
    assert "ingest_op" in result.get_failed_step_keys()


def test_re_execution_is_a_new_durable_run() -> None:
    first, _ = _run()
    second, _ = _run()
    assert first.run_id != second.run_id


def test_graph_shape_is_the_data_quality_pipeline() -> None:
    assert set(data_quality_job.graph.node_dict) == {
        "ingest_op",
        "profile_op",
        "check_op",
        "publish_op",
    }
