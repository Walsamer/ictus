"""Data-driven capability -> workflow registry."""

from __future__ import annotations

import pytest

from ictus_dagster.adapters.errors import UnsupportedCapability
from ictus_dagster.adapters.intent_config import (
    CAPABILITY_WORKFLOW_CAPABILITIES,
    DATA_QUALITY_CAPABILITIES,
    SUPPORTED_CAPABILITIES,
    SYSTEM_DIAGNOSTIC_CAPABILITIES,
)
from ictus_dagster.adapters.workflow_registry import (
    JOBS,
    RESOURCES,
    SETTINGS_MAPPERS,
    SUPPORTED_CAPABILITIES as REGISTRY_CAPABILITIES,
    WORKFLOW_REGISTRY_VERSION,
    WORKFLOWS,
    WORKFLOW_SPECS,
    WorkflowRegistryError,
    capabilities_for_job,
    load_workflows,
    resolve_workflow,
)


# -- the shipped registry ---------------------------------------------------


def test_registry_is_the_source_of_truth_for_capabilities() -> None:
    assert REGISTRY_CAPABILITIES == SUPPORTED_CAPABILITIES
    assert set(WORKFLOWS) == SUPPORTED_CAPABILITIES


def test_specs_and_map_agree() -> None:
    assert len(WORKFLOW_SPECS) == len(WORKFLOWS)


def test_capability_workflow_routes_to_the_capability_job() -> None:
    spec = resolve_workflow("demo.verify")
    assert spec.job.name == "capability_execution_job"
    assert spec.resource_key == "settings"


def test_data_quality_capabilities_route_to_the_data_quality_job() -> None:
    spec = resolve_workflow("data.quality_check")
    assert spec.job.name == "data_quality_job"
    assert spec.resource_key == "dq_settings"
    for capability in DATA_QUALITY_CAPABILITIES:
        assert resolve_workflow(capability).job.name == "data_quality_job"


def test_system_diagnostic_capabilities_route_to_the_diagnostic_job() -> None:
    spec = resolve_workflow("system.diagnose")
    assert spec.job.name == "system_diagnostic_job"
    assert spec.resource_key == "diag_settings"
    for capability in SYSTEM_DIAGNOSTIC_CAPABILITIES:
        assert resolve_workflow(capability).job.name == "system_diagnostic_job"


def test_all_capability_workflow_capabilities_share_one_job() -> None:
    for capability in CAPABILITY_WORKFLOW_CAPABILITIES:
        assert resolve_workflow(capability).job.name == "capability_execution_job"


def test_capabilities_for_job_groups_consistently() -> None:
    assert capabilities_for_job("capability_execution_job") == CAPABILITY_WORKFLOW_CAPABILITIES
    assert capabilities_for_job("data_quality_job") == DATA_QUALITY_CAPABILITIES
    assert capabilities_for_job("system_diagnostic_job") == SYSTEM_DIAGNOSTIC_CAPABILITIES
    assert capabilities_for_job("no_such_job") == frozenset()


def test_unknown_capability_fails_closed() -> None:
    with pytest.raises(UnsupportedCapability, match="not owned by this execution backend"):
        resolve_workflow("does.not.exist")


# -- loader validation (fail closed) ----------------------------------------


def _entry(**overrides) -> dict:
    entry = {
        "capability": "x.cap",
        "job": "capability_execution_job",
        "resource": "settings",
        "settings": "capability",
    }
    entry.update(overrides)
    return entry


def _raw(*entries: dict, version: int = WORKFLOW_REGISTRY_VERSION) -> dict:
    return {"version": version, "workflow": list(entries)}


def test_loader_rejects_a_wrong_version() -> None:
    with pytest.raises(WorkflowRegistryError, match="unsupported workflow registry version"):
        load_workflows(_raw(_entry(), version=999))


def test_loader_rejects_missing_or_empty_entries() -> None:
    with pytest.raises(WorkflowRegistryError, match="no \\[\\[workflow\\]\\] entries"):
        load_workflows({"version": WORKFLOW_REGISTRY_VERSION})
    with pytest.raises(WorkflowRegistryError, match="no \\[\\[workflow\\]\\] entries"):
        load_workflows(_raw())


def test_loader_rejects_unknown_job_resource_and_settings() -> None:
    with pytest.raises(WorkflowRegistryError, match="unknown job 'nope'"):
        load_workflows(_raw(_entry(job="nope")))
    with pytest.raises(WorkflowRegistryError, match="unknown resource 'nope'"):
        load_workflows(_raw(_entry(resource="nope")))
    with pytest.raises(WorkflowRegistryError, match="unknown settings mapper 'nope'"):
        load_workflows(_raw(_entry(settings="nope")))


def test_loader_rejects_a_missing_key() -> None:
    entry = _entry()
    del entry["job"]
    with pytest.raises(WorkflowRegistryError, match="missing key 'job'"):
        load_workflows(_raw(entry))


def test_loader_rejects_duplicate_capabilities() -> None:
    with pytest.raises(WorkflowRegistryError, match="duplicate capability 'x.cap'"):
        load_workflows(_raw(_entry(), _entry()))


def test_loader_rejects_a_non_table_entry() -> None:
    with pytest.raises(WorkflowRegistryError, match="must be a table"):
        load_workflows(_raw("not-a-table"))


def test_loader_accepts_a_valid_custom_mapping() -> None:
    workflows = load_workflows(_raw(_entry(capability="custom.cap")))
    assert set(workflows) == {"custom.cap"}
    assert workflows["custom.cap"].job.name == "capability_execution_job"


def test_code_tables_cover_every_referenced_name() -> None:
    # Every name referenced by the shipped registry must exist in the tables.
    assert set(JOBS) >= {spec.job.name for spec in WORKFLOWS.values()}
    assert set(RESOURCES) >= {spec.resource_key for spec in WORKFLOWS.values()}
    assert SETTINGS_MAPPERS  # non-empty
