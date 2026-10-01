"""Pure-Python contract helpers (no Dagster import).

Keeping the contracts importable without Dagster lets the contract tests and the
intent/result mapping run in a minimal environment. Dagster-specific code lives
in ``ops``, ``jobs``, ``resources`` and ``definitions``.
"""

from __future__ import annotations

from typing import Any

SCHEMA_VERSION = 1

OBSERVATION_CATEGORIES = {
    "SUCCESS",
    "WORKER_TIMEOUT",
    "PROCESS_CRASH",
    "VERIFICATION_FAILURE",
    "INTEGRATION_CONFLICT",
    "RESOURCE_EXHAUSTED",
    "PROVIDER_UNAVAILABLE",
    "UNKNOWN",
}

EXECUTION_STATUSES = {"succeeded", "failed", "timed_out", "cancelled"}


class ContractError(ValueError):
    """Raised when a payload does not satisfy the versioned contract."""


def _require(payload: dict[str, Any], field: str, expected: type) -> Any:
    if field not in payload:
        raise ContractError(f"missing required field: {field}")
    value = payload[field]
    if not isinstance(value, expected):
        raise ContractError(
            f"field {field} must be {expected.__name__}, got {type(value).__name__}"
        )
    return value


def validate_intent(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate an ExecutionIntent payload and return it unchanged."""
    if not isinstance(payload, dict):
        raise ContractError("ExecutionIntent must be a JSON object")
    version = _require(payload, "schema_version", int)
    if version != SCHEMA_VERSION:
        raise ContractError(
            f"unsupported schema_version {version}; supported is {SCHEMA_VERSION}"
        )
    _require(payload, "intent_id", str)
    capability = _require(payload, "capability", str)
    if not capability.strip():
        raise ContractError("capability must be non-empty")
    target = _require(payload, "target", dict)
    _require(target, "type", str)
    _require(target, "id", str)
    requested_by = _require(payload, "requested_by", dict)
    _require(requested_by, "provider", str)
    _require(requested_by, "decision_id", str)
    return payload


def validate_result(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate an ExecutionResult payload and return it unchanged."""
    if not isinstance(payload, dict):
        raise ContractError("ExecutionResult must be a JSON object")
    version = _require(payload, "schema_version", int)
    if version != SCHEMA_VERSION:
        raise ContractError(
            f"unsupported schema_version {version}; supported is {SCHEMA_VERSION}"
        )
    _require(payload, "execution_id", str)
    _require(payload, "intent_id", str)
    status = _require(payload, "status", str)
    if status not in EXECUTION_STATUSES:
        raise ContractError(f"status must be one of {sorted(EXECUTION_STATUSES)}")
    observation = _require(payload, "observation", dict)
    category = _require(observation, "category", str)
    if category not in OBSERVATION_CATEGORIES:
        raise ContractError(f"unknown observation category: {category}")
    return payload
