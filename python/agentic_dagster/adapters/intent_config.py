"""Map a generic ExecutionIntent onto execution settings (no Dagster import).

The mapping is intentionally declarative: the intent's ``arguments`` object is
opaque to the core and interpreted here. A real deployment would register
capability -> workflow mappings rather than reading ad-hoc keys.
"""

from __future__ import annotations

from typing import Any

# Capabilities this generic backend knows how to execute. Deliberately generic;
# no domain system (e.g. the testbed) is referenced here.
SUPPORTED_CAPABILITIES = {
    "demo.verify",
    "software.verify",
}

# Capabilities that are recognised but must be executed by a different backend.
KNOWN_UNSUPPORTED = {
    "software.implement",
    "software.promote",
    "data.materialize",
    "system.diagnose",
}


class UnsupportedCapability(ValueError):
    """Raised when the backend is asked to execute a capability it does not own."""


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


def settings_from_intent(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the resource config derived from a validated intent."""
    capability = str(payload.get("capability", ""))
    if capability in KNOWN_UNSUPPORTED:
        raise UnsupportedCapability(
            f"capability '{capability}' is not owned by this execution backend"
        )
    if capability not in SUPPORTED_CAPABILITIES:
        raise UnsupportedCapability(f"unknown capability '{capability}'")

    arguments = payload.get("arguments") or {}
    if not isinstance(arguments, dict):
        raise UnsupportedCapability("intent arguments must be a JSON object")

    return {
        "capability": capability,
        # Deterministic failure injection. `fail_until_attempt=N` makes the
        # execute step fail on attempts 1..N (proving retry), `fail_hard=true`
        # makes it fail always (proving persisted failure).
        "fail_until_attempt": _as_int(arguments.get("fail_until_attempt"), 0),
        "fail_hard": _as_bool(arguments.get("fail_hard"), False),
        "fail_verify": _as_bool(arguments.get("fail_verify"), False),
    }
