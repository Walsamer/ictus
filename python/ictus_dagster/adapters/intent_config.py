"""Interpret a generic ExecutionIntent's ``arguments`` as execution settings.

No Dagster import, so this is unit-testable in isolation. The intent's
``arguments`` object is opaque to the core and is interpreted here by the
adapter that owns the capability.
"""

from __future__ import annotations

from typing import Any

from ictus_dagster.adapters.errors import UnsupportedCapability

__all__ = [
    "UnsupportedCapability",
    "CAPABILITY_WORKFLOW_CAPABILITIES",
    "DATA_QUALITY_CAPABILITIES",
    "SUPPORTED_CAPABILITIES",
    "KNOWN_UNSUPPORTED",
    "capability_settings",
    "data_quality_settings",
    "settings_from_intent",
    "data_quality_settings_from_intent",
]

# Capabilities executed by the generic `capability_execution_job`
# (prepare -> execute -> verify -> finalize).
CAPABILITY_WORKFLOW_CAPABILITIES = {
    "demo.verify",
    "software.verify",
}

# Capabilities executed by the `data_quality_job`
# (ingest -> profile -> check -> publish). A deliberately non-software domain,
# to demonstrate that the generic core is domain-independent.
DATA_QUALITY_CAPABILITIES = {
    "data.quality_check",
}

SUPPORTED_CAPABILITIES = CAPABILITY_WORKFLOW_CAPABILITIES | DATA_QUALITY_CAPABILITIES

# Capabilities that are recognised but must be executed by a different backend.
KNOWN_UNSUPPORTED = {
    "software.implement",
    "software.promote",
    "data.materialize",
    "system.diagnose",
}


def _arguments(payload: dict[str, Any]) -> dict[str, Any]:
    arguments = payload.get("arguments") or {}
    if not isinstance(arguments, dict):
        raise UnsupportedCapability("intent arguments must be a JSON object")
    return arguments


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    if value is None:
        return default
    return bool(value)


def capability_settings(arguments: dict[str, Any]) -> dict[str, Any]:
    """Settings for the generic capability workflow."""
    return {
        # Deterministic failure injection. `fail_until_attempt=N` makes the
        # execute step fail on attempts 1..N (proving retry), `fail_hard=true`
        # makes it fail always (proving persisted failure).
        "fail_until_attempt": _as_int(arguments.get("fail_until_attempt"), 0),
        "fail_hard": _as_bool(arguments.get("fail_hard"), False),
        "fail_verify": _as_bool(arguments.get("fail_verify"), False),
    }


def data_quality_settings(arguments: dict[str, Any]) -> dict[str, Any]:
    """Settings for the data-quality workflow."""
    return {
        "dataset": str(arguments.get("dataset", "synthetic.orders")),
        "rows": _as_int(arguments.get("rows"), 100),
        "min_rows": _as_int(arguments.get("min_rows"), 1),
        "fail_ingest": _as_bool(arguments.get("fail_ingest"), False),
        "fail_until_attempt": _as_int(arguments.get("fail_until_attempt"), 0),
        "fail_on_check": _as_bool(arguments.get("fail_on_check"), False),
    }


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
