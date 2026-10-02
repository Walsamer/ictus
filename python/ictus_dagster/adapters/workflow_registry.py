"""Capability -> workflow routing for the execution backend, as data.

The routing table lives in `ictus_dagster/workflows.toml`. This module loads it,
resolves the named job/resource/settings-mapper against code-side tables, and
fails closed on any unknown or inconsistent entry.

Routing a *validated* capability to a workflow graph is an execution concern,
not a policy decision: the Rust policy layer has already authorized the
capability. Adding a workflow here never adds authorization logic.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from importlib import resources
from typing import Any, Callable

from dagster import JobDefinition

from ictus_dagster.adapters.errors import UnsupportedCapability
from ictus_dagster.adapters.settings import (
    capability_settings,
    data_quality_settings,
    diagnostic_settings,
)
from ictus_dagster.jobs.capability_job import capability_execution_job
from ictus_dagster.jobs.data_quality_job import data_quality_job
from ictus_dagster.jobs.system_diagnostic_job import system_diagnostic_job
from ictus_dagster.resources.data_quality_settings import DataQualitySettings
from ictus_dagster.resources.diagnostic_settings import DiagnosticSettings
from ictus_dagster.resources.execution_settings import ExecutionSettings

# The registry file format version.
WORKFLOW_REGISTRY_VERSION = 1

SettingsMapper = Callable[[dict[str, Any]], dict[str, Any]]


class WorkflowRegistryError(ValueError):
    """Raised when the workflow registry is unknown, malformed or inconsistent."""


@dataclass(frozen=True)
class WorkflowSpec:
    """A capability, the job that executes it, and how to build its resource."""

    capability: str
    job: JobDefinition
    resource_key: str
    resource_cls: type
    settings_from_arguments: SettingsMapper


# Code-side tables referenced by name from workflows.toml. Adding a new
# workflow means adding an entry to each relevant table *and* to the TOML.
JOBS: dict[str, JobDefinition] = {
    "capability_execution_job": capability_execution_job,
    "data_quality_job": data_quality_job,
    "system_diagnostic_job": system_diagnostic_job,
}

RESOURCES: dict[str, type] = {
    "settings": ExecutionSettings,
    "dq_settings": DataQualitySettings,
    "diag_settings": DiagnosticSettings,
}

SETTINGS_MAPPERS: dict[str, SettingsMapper] = {
    "capability": capability_settings,
    "data_quality": data_quality_settings,
    "diagnostic": diagnostic_settings,
}


def _load_toml() -> dict[str, Any]:
    raw = resources.files("ictus_dagster").joinpath("workflows.toml").read_bytes()
    return tomllib.loads(raw.decode("utf-8"))


def _resolve(name: str, table: dict[str, Any], kind: str, capability: str) -> Any:
    try:
        return table[name]
    except KeyError:
        raise WorkflowRegistryError(
            f"workflow for capability '{capability}' references unknown {kind} '{name}'"
        ) from None


def load_workflows(raw: dict[str, Any] | None = None) -> dict[str, WorkflowSpec]:
    """Build the workflow map from the TOML file (or an injected mapping)."""
    data = raw if raw is not None else _load_toml()

    version = data.get("version")
    if version != WORKFLOW_REGISTRY_VERSION:
        raise WorkflowRegistryError(
            f"unsupported workflow registry version {version!r}; "
            f"expected {WORKFLOW_REGISTRY_VERSION}"
        )

    entries = data.get("workflow")
    if not isinstance(entries, list) or not entries:
        raise WorkflowRegistryError("workflow registry has no [[workflow]] entries")

    workflows: dict[str, WorkflowSpec] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise WorkflowRegistryError("each [[workflow]] entry must be a table")
        try:
            capability = entry["capability"]
            job_name = entry["job"]
            resource_name = entry["resource"]
            settings_name = entry["settings"]
        except KeyError as exc:
            raise WorkflowRegistryError(f"workflow entry missing key {exc}") from None

        if capability in workflows:
            raise WorkflowRegistryError(f"duplicate capability '{capability}'")

        workflows[capability] = WorkflowSpec(
            capability=capability,
            job=_resolve(job_name, JOBS, "job", capability),
            resource_key=resource_name,
            resource_cls=_resolve(resource_name, RESOURCES, "resource", capability),
            settings_from_arguments=_resolve(
                settings_name, SETTINGS_MAPPERS, "settings mapper", capability
            ),
        )
    return workflows


WORKFLOWS: dict[str, WorkflowSpec] = load_workflows()

# Ordered view of the registry (declaration order is preserved by dict).
WORKFLOW_SPECS: tuple[WorkflowSpec, ...] = tuple(WORKFLOWS.values())

# The registry is the source of truth for which capabilities this backend owns.
SUPPORTED_CAPABILITIES: frozenset[str] = frozenset(WORKFLOWS)


def capabilities_for_job(job_name: str) -> frozenset[str]:
    """Capabilities routed to a given job."""
    return frozenset(
        capability for capability, spec in WORKFLOWS.items() if spec.job.name == job_name
    )


def resolve_workflow(capability: str) -> WorkflowSpec:
    """Return the workflow that owns ``capability``, or fail closed."""
    try:
        return WORKFLOWS[capability]
    except KeyError:
        raise UnsupportedCapability(
            f"capability '{capability}' is not owned by this execution backend"
        ) from None
