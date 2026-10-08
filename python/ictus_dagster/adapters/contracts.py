"""Pure-Python contract helpers (no Dagster import).

Keeping the contracts importable without Dagster lets the contract tests and the
intent/result mapping run in a minimal environment. Dagster-specific code lives
in ``ops``, ``jobs``, ``resources`` and ``definitions``.
"""

from __future__ import annotations

import re
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
    selected_route = payload.get("selected_route")
    if selected_route is not None:
        if not isinstance(selected_route, dict):
            raise ContractError("selected_route must be a JSON object")
        route = _require(selected_route, "route", dict)
        for field in ("backend", "provider", "runtime", "model"):
            value = _require(route, field, str)
            if not value.strip():
                raise ContractError(f"selected_route.route.{field} must be non-empty")
        for name in ("descriptor", "health", "quota", "disablement"):
            fact = _require(selected_route, name, dict)
            fact_id = _require(fact, "fact_id", str)
            revision = _require(fact, "revision", int)
            if not fact_id.strip() or revision < 0:
                raise ContractError(f"selected_route.{name} must bind a non-empty fact_id and non-negative revision")
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


def validate_receipt(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate the receipt fields required for durable reconciliation."""
    if not isinstance(payload, dict):
        raise ContractError("ExecutionReceipt must be a JSON object")
    version = _require(payload, "schema_version", int)
    if version != SCHEMA_VERSION:
        raise ContractError(
            f"unsupported schema_version {version}; supported is {SCHEMA_VERSION}"
        )
    for field in ("receipt_id", "execution_id", "intent_id", "status", "submitted_at"):
        value = _require(payload, field, str)
        if not value.strip():
            raise ContractError(f"{field} must be non-empty")
    digest = _require(payload, "intent_digest", str)
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ContractError("intent_digest must be a SHA-256 hex digest")
    evidence = _require(payload, "evidence", list)
    if not evidence:
        raise ContractError("receipt must carry durable evidence")
    return payload
