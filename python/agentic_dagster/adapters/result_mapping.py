"""Map a Dagster run outcome onto the generic ExecutionResult contract.

This is an adapter: it translates execution facts into the generic result shape.
It must never introduce policy — it reports facts only.
"""

from __future__ import annotations

from typing import Any

from agentic_dagster.adapters.contracts import SCHEMA_VERSION, validate_result

# Generic, execution-side mapping from a failed step to an observation category.
# Domain-specific failure taxonomies live in domain adapters, never here.
FAILED_STEP_CATEGORY = {
    "prepare_op": "UNKNOWN",
    "execute_op": "PROCESS_CRASH",
    "verify_op": "VERIFICATION_FAILURE",
    "finalize_op": "UNKNOWN",
    "prepare": "UNKNOWN",
    "execute": "PROCESS_CRASH",
    "verify": "VERIFICATION_FAILURE",
    "finalize": "UNKNOWN",
}


def _category_for_failed_step(step: str | None) -> str:
    if not step:
        return "UNKNOWN"
    return FAILED_STEP_CATEGORY.get(step, "UNKNOWN")


def execution_result_from_run(
    *,
    execution_id: str,
    intent_id: str,
    success: bool,
    failed_step: str | None = None,
    failure_category: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    evidence: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a validated ExecutionResult payload from raw run facts."""
    status = "succeeded" if success else "failed"
    if success:
        category = "SUCCESS"
    else:
        category = failure_category or _category_for_failed_step(failed_step)

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "execution_id": execution_id,
        "intent_id": intent_id,
        "status": status,
        "observation": {"category": category},
        "evidence": list(evidence or []),
    }
    if started_at:
        payload["started_at"] = started_at
    if finished_at:
        payload["finished_at"] = finished_at
    return validate_result(payload)
