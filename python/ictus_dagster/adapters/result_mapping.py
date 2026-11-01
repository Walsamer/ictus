"""Map a Dagster run outcome onto the generic ExecutionResult contract.

This is an adapter: it translates execution facts into the generic result shape.
It must never introduce policy — it reports facts only.
"""

from __future__ import annotations

from typing import Any

from ictus_dagster.adapters.contracts import (
    SCHEMA_VERSION,
    ContractError,
    validate_observation,
    validate_result,
)

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
    # data-quality workflow
    "ingest_op": "UNKNOWN",
    "profile_op": "PROCESS_CRASH",
    "check_op": "VERIFICATION_FAILURE",
    "publish_op": "UNKNOWN",
    # system-diagnostics workflow
    "collect_op": "UNKNOWN",
    "inspect_op": "PROCESS_CRASH",
    "classify_op": "VERIFICATION_FAILURE",
    "report_op": "UNKNOWN",
    # fail-closed software routing placeholders (pre-seam step names)
    "software_agent_placeholder_op": "INTEGRATION_CONFLICT",
    "software_publish_placeholder_op": "INTEGRATION_CONFLICT",
    # software.implement / software.promote seam skeleton
    "software_agent_prepare_op": "INTEGRATION_CONFLICT",
    "software_agent_execute_op": "PROCESS_CRASH",
    "software_agent_verify_op": "VERIFICATION_FAILURE",
    "software_agent_finalize_op": "UNKNOWN",
    "software_publish_prepare_op": "INTEGRATION_CONFLICT",
    "software_publish_execute_op": "PROCESS_CRASH",
    "software_publish_verify_op": "VERIFICATION_FAILURE",
    "software_publish_finalize_op": "UNKNOWN",
}


def _category_for_failed_step(step: str | None) -> str:
    if not step:
        return "UNKNOWN"
    return FAILED_STEP_CATEGORY.get(step, "UNKNOWN")


def seam_evidence_items(seam_evidence: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Translate the runtime seam's summary into contract evidence entries.

    The seam already reported structured facts (changed paths, patch digest and
    size, exit/termination status) rather than a log tail; this only restates
    them in the ``ExecutionResult`` evidence shape, and reports nothing when no
    seam was configured.
    """
    if not seam_evidence:
        return []
    changed_paths = seam_evidence.get("changed_paths") or []
    patch: dict[str, Any] = {
        "kind": "runtime_patch",
        "uri": "seam://patch",
        "note": (
            f"changed_paths={','.join(str(path) for path in changed_paths) or 'none'}; "
            f"patch_bytes={seam_evidence.get('patch_bytes', 0)}"
        ),
    }
    digest = seam_evidence.get("patch_digest")
    if isinstance(digest, str) and digest.startswith("sha256:"):
        patch["sha256"] = digest[len("sha256:") :]
    termination = seam_evidence.get("termination") or {}
    exit_status = seam_evidence.get("exit_status") or {}
    return [
        patch,
        {
            "kind": "runtime_exit",
            "uri": "seam://exit",
            "note": (
                f"outcome={seam_evidence.get('outcome', 'unknown')}; "
                f"exit_code={exit_status.get('code')}; "
                f"signal={exit_status.get('signal')}; "
                f"termination_confirmed={termination.get('confirmed')}; "
                f"steps_used={seam_evidence.get('steps_used', 0)}; "
                f"duration_seconds={seam_evidence.get('duration_seconds', 0)}"
            ),
        },
    ]


def execution_result_from_run(
    *,
    execution_id: str,
    intent_id: str,
    success: bool,
    run_status: str | None = None,
    failed_step: str | None = None,
    failure_category: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    evidence: list[dict[str, Any]] | None = None,
    seam_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a validated ExecutionResult payload from raw run facts."""
    normalized_status = (run_status or "").upper()
    if success or normalized_status == "SUCCESS":
        status = "succeeded"
        category = "SUCCESS"
    elif normalized_status in {"CANCELED", "CANCELING"}:
        # Cancellation is a known terminal *status*.  The generic observation
        # vocabulary has no cancellation category, so do not invent one.
        status = "cancelled"
        category = "UNKNOWN"
    elif normalized_status == "TIMED_OUT":
        status = "timed_out"
        category = failure_category or "WORKER_TIMEOUT"
    else:
        status = "failed"
        category = failure_category or _category_for_failed_step(failed_step)

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "execution_id": execution_id,
        "intent_id": intent_id,
        "status": status,
        "observation": {"category": category},
        "evidence": [
            *list(evidence or []),
            *seam_evidence_items(seam_evidence),
        ],
    }
    if started_at:
        payload["started_at"] = started_at
    if finished_at:
        payload["finished_at"] = finished_at
    return validate_result(payload)


def result_to_observation(
    result: dict[str, Any],
    *,
    observation_id: str | None = None,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Derive the ExecutionObservation envelope from an ExecutionResult.

    This is owner-side conversion only: the identity, category and evidence are
    copied verbatim from an already validated ``ExecutionResult``, so the
    observation reports the same facts and introduces no policy.  The two
    derived values are its own identity and its timestamp.

    ``observation_id`` defaults to ``ictus-result:<execution_id>``.  ``observed_at``
    defaults to the result's ``finished_at`` and never to the wall clock: a
    result that carries no ``finished_at`` and no explicit ``observed_at`` fails
    closed with :class:`ContractError`, because a fabricated timestamp would
    silently reorder the fact against the decisions that read it.
    """
    validate_result(result)

    resolved_observed_at = (
        observed_at if observed_at is not None else result.get("finished_at")
    )
    if not resolved_observed_at:
        raise ContractError(
            "ExecutionObservation requires observed_at: the result reports no "
            "finished_at and none was supplied"
        )
    execution_id = result["execution_id"]
    return validate_observation(
        {
            "schema_version": SCHEMA_VERSION,
            "observation_id": (
                observation_id
                if observation_id is not None
                else f"ictus-result:{execution_id}"
            ),
            "execution_id": execution_id,
            "intent_id": result["intent_id"],
            "category": result["observation"]["category"],
            "observed_at": resolved_observed_at,
            "evidence": list(result.get("evidence") or []),
        }
    )
