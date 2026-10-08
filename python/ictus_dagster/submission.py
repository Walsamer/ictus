"""Durable, idempotent Dagster submission and receipt reconciliation.

This module is the production side of the execution boundary.  It deliberately
does not execute a job in the caller: it records a Dagster run and hands it to
the instance's configured launcher.  The run record is the durable receipt;
the tags below are the small bridge-owned deduplication protocol.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from dagster import DagsterInstance, RunsFilter, reconstructable

from ictus_dagster.adapters.contracts import validate_intent, validate_receipt
from ictus_dagster.adapters.result_mapping import execution_result_from_run
from ictus_dagster.adapters.workflow_registry import resolve_workflow

IDENTITY_TAG = "ictus.intent_id"
DIGEST_TAG = "ictus.intent_digest"
SUBMISSION_TAG = "ictus.submission_protocol"
SUBMITTED_AT_TAG = "ictus.submitted_at"
SUBMISSION_PROTOCOL = "v1"


class SubmissionError(RuntimeError):
    """A durable submission could not be accepted safely."""


class EphemeralStorageError(SubmissionError):
    """Production submission was pointed at an in-memory Dagster instance."""


class SubmissionConflict(SubmissionError):
    """An identity was already submitted with different immutable content."""


class UnknownReceipt(SubmissionError):
    """A receipt does not identify a run in this Dagster instance."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def intent_digest(payload: dict[str, Any]) -> str:
    """Return the stable digest used to bind an identity to immutable intent."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _instance_is_ephemeral(instance: DagsterInstance) -> bool:
    value = getattr(instance, "is_ephemeral", False)
    return bool(value() if callable(value) else value)


def production_instance(instance: DagsterInstance | None = None) -> DagsterInstance:
    """Get an instance suitable for cross-process, restart-safe submission."""
    if not os.environ.get("DAGSTER_HOME"):
        raise EphemeralStorageError("production submission requires DAGSTER_HOME")
    selected = instance or DagsterInstance.get()
    if _instance_is_ephemeral(selected):
        raise EphemeralStorageError("production submission rejects ephemeral Dagster storage")

    launcher = getattr(selected, "run_launcher", None)
    name = type(launcher).__name__ if launcher is not None else ""
    if launcher is None or "InMemory" in name or "Sync" in name:
        raise SubmissionError("production submission requires an independent Dagster run launcher")
    return selected


@contextmanager
def _identity_lock(intent_id: str) -> Iterator[None]:
    """Serialize local submitters without introducing a second state store.

    Dagster remains the record of ownership.  The lock only closes the interval
    between looking up the tagged run and creating it, including concurrent
    bridge processes on one persistent ``DAGSTER_HOME``.
    """
    home = Path(os.environ["DAGSTER_HOME"])
    directory = home / ".ictus-submission-locks"
    directory.mkdir(mode=0o700, exist_ok=True)
    filename = hashlib.sha256(intent_id.encode("utf-8")).hexdigest() + ".lock"
    with (directory / filename).open("a+") as handle:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except NameError:  # pragma: no cover - platforms without fcntl
                pass


def _runs_for_identity(instance: DagsterInstance, identity: str) -> list[Any]:
    return list(instance.get_runs(filters=RunsFilter(tags={IDENTITY_TAG: identity})))


def _receipt(run: Any, digest: str | None = None) -> dict[str, Any]:
    tags = dict(getattr(run, "tags", {}) or {})
    return validate_receipt({
        "schema_version": 1,
        "receipt_id": run.run_id,
        "execution_id": run.run_id,
        "intent_id": tags.get(IDENTITY_TAG, ""),
        "intent_digest": digest or tags.get(DIGEST_TAG, ""),
        "status": str(getattr(run, "status", "NOT_STARTED")).rsplit(".", 1)[-1].lower(),
        "submitted_at": tags.get(SUBMITTED_AT_TAG, _now()),
        "evidence": [{"kind": "dagster_run", "uri": f"dagster://runs/{run.run_id}"}],
    })


def _run_config(spec: Any, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "resources": {
            spec.resource_key: {"config": spec.settings_from_arguments(payload.get("arguments") or {})}
        }
    }


def _submit_to_dagster(instance: DagsterInstance, run_id: str) -> None:
    """Hand work to Dagster's queue/coordinator, never ``execute_in_process``."""
    submit = getattr(instance, "submit_run", None) or instance.launch_run
    # Dagster's public API has used both ``...(run_id)`` and
    # ``...(run_id, workspace)``.  DefaultRunLauncher does not require a
    # workspace object, so ``None`` is the correct value for the latter.
    parameters = inspect.signature(submit).parameters
    if "workspace" in parameters:
        submit(run_id, None)
    else:
        submit(run_id)


def submit_intent(
    payload: dict[str, Any], *, instance: DagsterInstance | None = None
) -> dict[str, Any]:
    """Submit immutable intent once and return its durable receipt.

    A repeat request with the same identity and digest returns the original
    receipt.  A different digest fails closed.  Existing runs are never
    launched again: this makes a lost acknowledgement a reconciliation case,
    rather than a duplicate-effect case.
    """
    validate_intent(payload)
    dagster_instance = production_instance(instance)
    digest = intent_digest(payload)
    identity = payload["intent_id"]

    with _identity_lock(identity):
        existing = _runs_for_identity(dagster_instance, identity)
        if existing:
            run = existing[0]
            existing_digest = (getattr(run, "tags", {}) or {}).get(DIGEST_TAG)
            if existing_digest != digest:
                raise SubmissionConflict(
                    f"submission identity '{identity}' is already bound to a different digest"
                )
            return _receipt(run, digest)

        spec = resolve_workflow(payload["capability"])
        reconstructed = reconstructable(spec.job_factory)
        tags = {
            IDENTITY_TAG: identity,
            DIGEST_TAG: digest,
            SUBMISSION_TAG: SUBMISSION_PROTOCOL,
            SUBMITTED_AT_TAG: _now(),
            "ictus.capability": payload["capability"],
            "ictus.schema_version": str(payload["schema_version"]),
            # One semantic attempt maps to one Dagster run.  Step retry policy
            # remains on the job; Dagster must not create automatic new runs.
            "dagster/max_retries": "0",
        }
        run = dagster_instance.create_run_for_job(
            spec.job,
            run_config=_run_config(spec, payload),
            tags=tags,
            job_code_origin=reconstructed.get_python_origin(),
        )
        # This is intentionally after durable record creation.  If the caller
        # loses the acknowledgement, the next caller finds this same record;
        # it never manufactures a second run for the semantic attempt.
        _submit_to_dagster(dagster_instance, run.run_id)
        return _receipt(run, digest)


def _failed_step(instance: DagsterInstance, run_id: str) -> tuple[str | None, str | None]:
    """Extract raw failure facts without assigning policy meaning to them."""
    try:
        events = instance.all_logs(run_id)
    except Exception:  # pragma: no cover - unavailable event storage is evidence itself
        return None, None
    for event_record in events:
        event = getattr(event_record, "dagster_event", None)
        if getattr(event, "event_type_value", None) != "STEP_FAILURE":
            continue
        error = getattr(getattr(event, "event_specific_data", None), "error", None)
        class_name = str(getattr(error, "cls_name", ""))
        if "Timeout" in class_name:
            return getattr(event, "step_key", None), "WORKER_TIMEOUT"
        return getattr(event, "step_key", None), None
    return None, None


def query_receipt(
    receipt: dict[str, Any] | str, *, instance: DagsterInstance | None = None
) -> dict[str, Any]:
    """Return current durable status and a correlated terminal result, if any."""
    dagster_instance = production_instance(instance)
    run_id = receipt if isinstance(receipt, str) else receipt.get("receipt_id", "")
    if not isinstance(run_id, str) or not run_id:
        raise UnknownReceipt("receipt must include a non-empty receipt_id")
    run = dagster_instance.get_run_by_id(run_id)
    if run is None or (getattr(run, "tags", {}) or {}).get(SUBMISSION_TAG) != SUBMISSION_PROTOCOL:
        raise UnknownReceipt(f"unknown receipt '{run_id}'")

    response = _receipt(run)
    status = getattr(run, "status", None)
    status_name = str(status).rsplit(".", 1)[-1].upper()
    terminal = {"SUCCESS", "FAILURE", "CANCELED", "TIMED_OUT"}
    if status_name not in terminal:
        response["state"] = "pending"
        return response

    failed_step, failure_category = _failed_step(dagster_instance, run_id)
    response["state"] = "terminal"
    response["result"] = execution_result_from_run(
        execution_id=run_id,
        intent_id=response["intent_id"],
        success=status_name == "SUCCESS",
        run_status=status_name,
        failed_step=failed_step,
        failure_category=failure_category,
        evidence=[
            {"kind": "dagster_run", "uri": f"dagster://runs/{run_id}"},
            {"kind": "dagster_event_log", "uri": f"dagster://runs/{run_id}/events"},
        ],
    )
    return response


def cancel_receipt(
    receipt: dict[str, Any] | str, *, instance: DagsterInstance | None = None
) -> dict[str, Any]:
    """Request cancellation from Dagster and return the reconciled receipt."""
    dagster_instance = production_instance(instance)
    run_id = receipt if isinstance(receipt, str) else receipt.get("receipt_id", "")
    run = dagster_instance.get_run_by_id(run_id) if isinstance(run_id, str) and run_id else None
    if run is None or (getattr(run, "tags", {}) or {}).get(SUBMISSION_TAG) != SUBMISSION_PROTOCOL:
        raise UnknownReceipt(f"unknown receipt '{run_id}'")
    dagster_instance.cancel_run(run_id)
    return query_receipt(run_id, instance=dagster_instance)
