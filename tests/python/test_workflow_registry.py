"""Capability -> workflow routing."""

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
    WORKFLOWS,
    WORKFLOW_SPECS,
    resolve_workflow,
)


def test_every_supported_capability_is_routable() -> None:
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


def test_all_capability_workflow_capabilities_share_one_job() -> None:
    for capability in CAPABILITY_WORKFLOW_CAPABILITIES:
        assert resolve_workflow(capability).job.name == "capability_execution_job"


def test_system_diagnostic_capabilities_route_to_the_diagnostic_job() -> None:
    spec = resolve_workflow("system.diagnose")
    assert spec.job.name == "system_diagnostic_job"
    assert spec.resource_key == "diag_settings"
    for capability in SYSTEM_DIAGNOSTIC_CAPABILITIES:
        assert resolve_workflow(capability).job.name == "system_diagnostic_job"


def test_unknown_capability_fails_closed() -> None:
    with pytest.raises(UnsupportedCapability, match="not owned by this execution backend"):
        resolve_workflow("does.not.exist")
