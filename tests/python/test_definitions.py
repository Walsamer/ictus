"""The Dagster code location must load and expose the generic job(s)."""

from __future__ import annotations

from ictus_dagster.definitions import defs


def test_code_location_loads_with_expected_jobs() -> None:
    definitions = defs
    job_names = {job.name for job in definitions.jobs}
    assert {"capability_execution_job", "data_quality_job"} <= job_names
    assert {"settings", "dq_settings"} <= set(definitions.resources)


def test_capability_job_graph_is_generic() -> None:
    job = next(j for j in defs.jobs if j.name == "capability_execution_job")
    assert set(job.graph.node_dict) == {
        "prepare_op",
        "execute_op",
        "verify_op",
        "finalize_op",
    }


def test_data_quality_job_graph() -> None:
    job = next(j for j in defs.jobs if j.name == "data_quality_job")
    assert set(job.graph.node_dict) == {
        "ingest_op",
        "profile_op",
        "check_op",
        "publish_op",
    }
