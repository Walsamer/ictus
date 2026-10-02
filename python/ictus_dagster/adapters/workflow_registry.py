"""Capability -> workflow routing for the execution backend.

Routing a *validated* capability to a workflow graph is an execution concern,
not a policy decision: the Rust policy layer has already authorized the
capability. Adding a workflow here never adds authorization logic.

The registry is data, so the same bridge can drive different workflow shapes
without changing the Rust boundary or the versioned contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from dagster import JobDefinition

from ictus_dagster.adapters.errors import UnsupportedCapability
from ictus_dagster.adapters.intent_config import (
    capability_settings,
    data_quality_settings,
)
from ictus_dagster.jobs.capability_job import capability_execution_job
from ictus_dagster.jobs.data_quality_job import data_quality_job
from ictus_dagster.resources.data_quality_settings import DataQualitySettings
from ictus_dagster.resources.execution_settings import ExecutionSettings

SettingsMapper = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class WorkflowSpec:
    """A capability, the job that executes it, and how to build its resource."""

    capability: str
    job: JobDefinition
    resource_key: str
    resource_cls: type
    settings_from_arguments: SettingsMapper


WORKFLOW_SPECS: tuple[WorkflowSpec, ...] = (
    WorkflowSpec(
        "demo.verify",
        capability_execution_job,
        "settings",
        ExecutionSettings,
        capability_settings,
    ),
    WorkflowSpec(
        "software.verify",
        capability_execution_job,
        "settings",
        ExecutionSettings,
        capability_settings,
    ),
    WorkflowSpec(
        "data.quality_check",
        data_quality_job,
        "dq_settings",
        DataQualitySettings,
        data_quality_settings,
    ),
)

WORKFLOWS: dict[str, WorkflowSpec] = {spec.capability: spec for spec in WORKFLOW_SPECS}


def resolve_workflow(capability: str) -> WorkflowSpec:
    """Return the workflow that owns ``capability``, or fail closed."""
    try:
        return WORKFLOWS[capability]
    except KeyError:
        raise UnsupportedCapability(
            f"capability '{capability}' is not owned by this execution backend"
        ) from None
