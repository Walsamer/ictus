"""Durable execution of the third (system-diagnostics) domain workflow."""

from __future__ import annotations

from dagster import DagsterInstance

from ictus_dagster.jobs.system_diagnostic_job import system_diagnostic_job
from ictus_dagster.resources.diagnostic_settings import DiagnosticSettings


def _run(**settings):
    instance = DagsterInstance.ephemeral()
    result = system_diagnostic_job.execute_in_process(
        resources={"diag_settings": DiagnosticSettings(**settings)},
        instance=instance,
        raise_on_error=False,
    )
    return result, instance


def test_clean_run_reports_healthy_condition() -> None:
    result, _ = _run(target="node-1", samples=3)
    assert result.success
    report = result.output_for_node("report_op")
    assert report["target"] == "node-1"
    assert report["sample_count"] == 3
    assert report["mean_latency"] == 110.0
    assert report["condition"] == "healthy"


def test_more_samples_cross_into_degraded() -> None:
    result, _ = _run(samples=5)
    assert result.success
    assert result.output_for_node("report_op")["condition"] == "degraded"


def test_inspect_retry_recovers_within_the_run() -> None:
    result, _ = _run(fail_until_attempt=2)
    assert result.success
    assert result.output_for_node("report_op")["inspect_attempts"] == 3


def test_collect_failure_fails_the_first_step() -> None:
    result, instance = _run(fail_collect=True)
    assert not result.success
    assert "collect_op" in result.get_failed_step_keys()
    assert instance.get_run_record_by_id(result.run_id) is not None


def test_classify_failure_is_a_distinct_failed_step() -> None:
    result, _ = _run(fail_classify=True)
    assert not result.success
    assert "classify_op" in result.get_failed_step_keys()


def test_samples_must_be_positive() -> None:
    result, _ = _run(samples=0)
    assert not result.success
    assert "collect_op" in result.get_failed_step_keys()


def test_re_execution_is_a_new_durable_run() -> None:
    first, _ = _run()
    second, _ = _run()
    assert first.run_id != second.run_id


def test_graph_shape_is_the_diagnostics_pipeline() -> None:
    assert set(system_diagnostic_job.graph.node_dict) == {
        "collect_op",
        "inspect_op",
        "classify_op",
        "report_op",
    }
