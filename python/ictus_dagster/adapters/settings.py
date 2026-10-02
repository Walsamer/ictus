"""Pure intent-argument -> resource-settings mappers (no Dagster import).

These are the named settings mappers referenced by the workflow registry. They
are pure functions so they can be unit-tested in isolation.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "capability_settings",
    "data_quality_settings",
    "diagnostic_settings",
]


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


def diagnostic_settings(arguments: dict[str, Any]) -> dict[str, Any]:
    """Settings for the system-diagnostics workflow."""
    return {
        "target": str(arguments.get("target", "local")),
        "samples": _as_int(arguments.get("samples"), 3),
        "fail_collect": _as_bool(arguments.get("fail_collect"), False),
        "fail_until_attempt": _as_int(arguments.get("fail_until_attempt"), 0),
        "fail_classify": _as_bool(arguments.get("fail_classify"), False),
    }
