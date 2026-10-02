"""Capability catalog and per-workflow intent validators.

The capability sets are derived from the workflow registry
(`ictus_dagster/workflows.toml`), so the routing table is the single source of
truth for which capabilities this execution backend owns.

The pure settings mappers live in `adapters/settings.py` (Dagster-free); this
module only adds capability validation on top of them.
"""

from __future__ import annotations

from typing import Any

from ictus_dagster.adapters.errors import UnsupportedCapability
from ictus_dagster.adapters.settings import (
    capability_settings,
    data_quality_settings,
    diagnostic_settings,
)
from ictus_dagster.adapters.workflow_registry import (
    SUPPORTED_CAPABILITIES,
    capabilities_for_job,
)

__all__ = [
    "UnsupportedCapability",
    "CAPABILITY_WORKFLOW_CAPABILITIES",
    "DATA_QUALITY_CAPABILITIES",
    "SYSTEM_DIAGNOSTIC_CAPABILITIES",
    "SUPPORTED_CAPABILITIES",
    "KNOWN_UNSUPPORTED",
    "capability_settings",
    "data_quality_settings",
    "diagnostic_settings",
    "settings_from_intent",
    "data_quality_settings_from_intent",
    "diagnostic_settings_from_intent",
]

# Capabilities executed by each workflow job (derived from the registry).
CAPABILITY_WORKFLOW_CAPABILITIES = capabilities_for_job("capability_execution_job")
DATA_QUALITY_CAPABILITIES = capabilities_for_job("data_quality_job")
SYSTEM_DIAGNOSTIC_CAPABILITIES = capabilities_for_job("system_diagnostic_job")

# Capabilities that are recognised but must be executed by a different backend.
KNOWN_UNSUPPORTED = {
    "software.implement",
    "software.promote",
    "data.materialize",
}


def _arguments(payload: dict[str, Any]) -> dict[str, Any]:
    arguments = payload.get("arguments") or {}
    if not isinstance(arguments, dict):
        raise UnsupportedCapability("intent arguments must be a JSON object")
    return arguments


def settings_from_intent(payload: dict[str, Any]) -> dict[str, Any]:
    """Resource config for a capability-workflow intent (validates capability)."""
    capability = str(payload.get("capability", ""))
    if capability in KNOWN_UNSUPPORTED:
        raise UnsupportedCapability(
            f"capability '{capability}' is not owned by this execution backend"
        )
    if capability not in CAPABILITY_WORKFLOW_CAPABILITIES:
        raise UnsupportedCapability(
            f"capability '{capability}' is not a capability-workflow capability"
        )
    settings = capability_settings(_arguments(payload))
    settings["capability"] = capability
    return settings


def data_quality_settings_from_intent(payload: dict[str, Any]) -> dict[str, Any]:
    """Resource config for a data-quality intent (validates capability)."""
    capability = str(payload.get("capability", ""))
    if capability not in DATA_QUALITY_CAPABILITIES:
        raise UnsupportedCapability(
            f"capability '{capability}' is not a data-quality capability"
        )
    return data_quality_settings(_arguments(payload))


def diagnostic_settings_from_intent(payload: dict[str, Any]) -> dict[str, Any]:
    """Resource config for a system-diagnostics intent (validates capability)."""
    capability = str(payload.get("capability", ""))
    if capability not in SYSTEM_DIAGNOSTIC_CAPABILITIES:
        raise UnsupportedCapability(
            f"capability '{capability}' is not a system-diagnostics capability"
        )
    return diagnostic_settings(_arguments(payload))
