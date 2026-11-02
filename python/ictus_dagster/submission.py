"""Durable, idempotent Dagster submission and receipt reconciliation.

This module is the production side of the execution boundary.  It deliberately
does not execute a job in the caller: it records a Dagster run and hands it to
the instance's configured queued coordinator.  The run record is the durable
receipt; the tags below are the small bridge-owned deduplication protocol.

Supported deployment (Dagster 1.13.x)
-------------------------------------
Durable submission requires a *queued* deployment, because the bridge process
has no workspace and therefore must not launch a run itself:

* ``DAGSTER_HOME`` points at persistent (non-ephemeral) storage;
* the instance uses ``QueuedRunCoordinator`` so ``submit_run`` only enqueues;
* the instance uses an independent run launcher (``DefaultRunLauncher``);
* an independent ``dagster-daemon`` is running, with a ``workspace.yaml`` that
  registers the code location named by ``ICTUS_DAGSTER_CODE_LOCATION``;
* the daemon dequeues, attaches its real workspace and launches the run.

``submit_intent`` creates the run with both required origins persisted
(``remote_job_origin`` so the daemon can find the code location, and
``job_code_origin`` so the run is reconstructable).  Configurations outside the
supported set are rejected explicitly rather than silently degraded.

Concurrency scope
-----------------
Submission is serialized only among processes *on one host* that share this
``DAGSTER_HOME`` (see :func:`_identity_lock`).  Concurrent writers on different
hosts are **not supported**: two hosts can each create a run for the same
identity, because Dagster does not enforce uniqueness on the bridge-owned tags.
Deterministic reconciliation (below) makes reading and repair deterministic; it
does **not** make cross-host submission unique.  A distributed coordination
mechanism would be required before multi-host writers could be enabled.

The submit acknowledgement is a flat ``ExecutionReceipt`` (the Rust
``ictus_ports::ExecutionReceipt`` deserializes that exact shape).  Reconciliation
responses are a separate ``ExecutionStatus`` read model that wraps a receipt and
adds the current state, the correlated ``ExecutionResult`` once terminal, and
(for cancellation) the cancellation outcome.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from dagster import DagsterInstance, RunsFilter, reconstructable

from ictus_dagster.adapters.contracts import (
    OBSERVATION_CATEGORIES,
    validate_execution_status,
    validate_intent,
    validate_receipt,
)
from ictus_dagster.adapters.result_mapping import execution_result_from_run
from ictus_dagster.adapters.workflow_registry import resolve_workflow

try:  # POSIX advisory locking; absent on Windows.
    import fcntl
except ImportError:  # pragma: no cover - exercised only off POSIX
    fcntl = None  # type: ignore[assignment]

IDENTITY_TAG = "ictus.intent_id"
DIGEST_TAG = "ictus.intent_digest"
SUBMISSION_TAG = "ictus.submission_protocol"
SUBMITTED_AT_TAG = "ictus.submitted_at"
SUBMISSION_PROTOCOL = "v1"
DIGEST_SCHEMA_VERSION = 1

#: Name of the Dagster code location (in the daemon's workspace) that owns the
#: jobs this bridge submits.  Required for durable submission.
CODE_LOCATION_ENV = "ICTUS_DAGSTER_CODE_LOCATION"
#: Repository name inside that code location.  A ``Definitions`` object is
#: exposed as the default ``__repository__`` unless the code location names it.
REPOSITORY_ENV = "ICTUS_DAGSTER_REPOSITORY"
DEFAULT_REPOSITORY_NAME = "__repository__"

TERMINAL_RUN_STATUSES = frozenset({"SUCCESS", "FAILURE", "CANCELED", "TIMED_OUT"})
#: A run that is still NOT_STARTED was recorded but never handed to the
#: coordinator (the only status ``submit_run`` acts on).
_UNSUBMITTED_STATUS = "NOT_STARTED"


class SubmissionError(RuntimeError):
    """A durable submission could not be accepted safely."""


class EphemeralStorageError(SubmissionError):
    """Production submission was pointed at an in-memory Dagster instance."""


class SubmissionConfigurationError(SubmissionError):
    """The Dagster instance is not configured for durable, independent execution."""


class SubmissionConflict(SubmissionError):
    """An identity was already submitted with different immutable content."""


class UnknownReceipt(SubmissionError):
    """A receipt does not identify a run in this Dagster instance."""


class LockUnavailableError(SubmissionError):
    """The host cannot provide the advisory lock that serializes submitters."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _iso(timestamp: Any) -> str | None:
    """Format a Dagster epoch-seconds timestamp as an ISO-8601 UTC instant."""
    if not isinstance(timestamp, (int, float)):
        return None
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


class DigestDomainError(ValueError):
    """A value cannot be reproduced in the identity-digest JSON domain."""

    def __init__(self, path: str, reason: str, literal: str | None = None) -> None:
        self.path = path
        self.reason = reason
        self.literal = literal
        detail = f"{reason} at {path}"
        if literal is not None:
            detail = f"{detail} (literal {literal})"
        super().__init__(detail)


class _RefusedLiteral:
    """A number literal the ingress refused, kept as a marker so its path can be
    named after the document has been parsed."""

    __slots__ = ("literal", "reason")

    def __init__(self, literal: str, reason: str) -> None:
        self.literal = literal
        self.reason = reason


def require_digest_schema_version(schema_version: int) -> None:
    """Fail closed unless the consumer's digest protocol is supported."""
    if type(schema_version) is not int or schema_version != DIGEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported digest schema_version: {schema_version!r}")


def _validate_digest_value(value: Any, path: str = "$") -> None:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return
    if isinstance(value, int):
        if not -(2**63) <= value <= 2**64 - 1:
            raise DigestDomainError(path, "number-out-of-domain")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise DigestDomainError(path, "non-finite")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _validate_digest_value(item, f"{path}[{index}]")
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DigestDomainError(path, "unsupported-value")
            _validate_digest_value(item, f"{path}.{key}")
    else:
        raise DigestDomainError(path, "unsupported-value")


def intent_digest(payload: Any) -> str:
    """Return the stable digest, rejecting values outside the shared JSON domain.

    This guard sees a *parsed* payload, so it can still refuse a value the domain
    cannot carry (an integer outside it, a non-finite float) but not a literal
    that only rounded to its value. Text therefore enters through
    :func:`parse_digestable_document`, which applies the literal rule.
    """
    _validate_digest_value(payload)
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _find_refused(value: Any, path: str = "$") -> tuple[_RefusedLiteral, str] | None:
    """The first refused literal in `value`, with the JSON path it sits at."""
    if isinstance(value, _RefusedLiteral):
        return value, path
    if isinstance(value, list):
        for index, item in enumerate(value):
            found = _find_refused(item, f"{path}[{index}]")
            if found is not None:
                return found
    elif isinstance(value, dict):
        for key, item in value.items():
            found = _find_refused(item, f"{path}.{key}")
            if found is not None:
                return found
    return None


def parse_digestable_document(document: str) -> Any:
    """Parse a JSON document for digesting, refusing what the protocol cannot carry.

    A parser has already lost a literal's spelling, so the domain is enforced on
    the text: an integer outside the domain, a non-finite number (including the
    ``NaN``, ``Infinity`` and ``-Infinity`` literals ``json.loads`` accepts by
    default), and a literal that only *rounds* into the domain are all refused
    with a :class:`DigestDomainError` naming the JSON path. A literal denotes its
    value exactly when it equals the shortest round-trip form of the double it
    parses to, compared as numbers: ``0.10`` and ``1e-05`` are their values and
    pass, while 25 significant digits for a double that holds 17 do not.
    """
    refused: list[_RefusedLiteral] = []

    def refuse(literal: str, reason: str) -> _RefusedLiteral:
        marker = _RefusedLiteral(literal, reason)
        refused.append(marker)
        return marker

    def constant(literal: str) -> Any:
        return refuse(literal, "non-finite")

    def integer(literal: str) -> Any:
        value = int(literal)
        if not -(2**63) <= value <= 2**64 - 1:
            return refuse(literal, "number-out-of-domain")
        return value

    def number(literal: str) -> Any:
        value = float(literal)
        if not math.isfinite(value):
            return refuse(literal, "non-finite")
        if Decimal(literal) != Decimal(repr(value)):
            return refuse(literal, "number-would-round")
        return value

    payload = json.loads(
        document, parse_constant=constant, parse_int=integer, parse_float=number
    )
    found = _find_refused(payload)
    if found is not None:
        marker, path = found
        raise DigestDomainError(path, marker.reason, marker.literal)
    if refused:
        # A duplicate key hid the marker: the parsed object keeps the last value,
        # so the path cannot be named. The document still carries a literal
        # outside the domain and is refused as a whole rather than accepted.
        raise DigestDomainError("$", refused[0].reason, refused[0].literal)
    return payload


def _instance_is_ephemeral(instance: DagsterInstance) -> bool:
    value = getattr(instance, "is_ephemeral", False)
    return bool(value() if callable(value) else value)


def _type_name(value: Any) -> str:
    return type(value).__name__ if value is not None else ""


def production_instance(instance: DagsterInstance | None = None) -> DagsterInstance:
    """Get an instance suitable for cross-process, restart-safe submission.

    Rejects configurations the durable boundary cannot honor: missing
    ``DAGSTER_HOME``, ephemeral storage, an in-process launcher, or a run
    coordinator that would launch in the caller instead of independently.
    """
    if not os.environ.get("DAGSTER_HOME"):
        raise EphemeralStorageError("production submission requires DAGSTER_HOME")
    selected = instance or DagsterInstance.get()
    if _instance_is_ephemeral(selected):
        raise EphemeralStorageError("production submission rejects ephemeral Dagster storage")

    launcher = getattr(selected, "run_launcher", None)
    launcher_name = _type_name(launcher)
    if launcher is None or "InMemory" in launcher_name or "Sync" in launcher_name:
        raise SubmissionConfigurationError(
            "production submission requires an independent Dagster run launcher; "
            f"configured launcher is '{launcher_name or 'none'}'"
        )

    coordinator = getattr(selected, "run_coordinator", None)
    coordinator_name = _type_name(coordinator)
    if coordinator is None or coordinator_name != "QueuedRunCoordinator":
        raise SubmissionConfigurationError(
            "production submission requires Dagster's QueuedRunCoordinator so an "
            "independent dagster-daemon performs the launch; configured coordinator "
            f"is '{coordinator_name or 'none'}'"
        )
    return selected


@contextmanager
def _identity_lock(intent_id: str) -> Iterator[None]:
    """Serialize local submitters without introducing a second state store.

    Dagster remains the record of ownership.  The lock only closes the interval
    between looking up the tagged run and creating it, including concurrent
    bridge processes on one persistent ``DAGSTER_HOME``.

    **Single-host only.**  ``fcntl`` advisory locks do not coordinate across
    hosts, and multi-host concurrent writers are unsupported: each host could
    create its own run for the same identity.  Dagster's record plus the
    deterministic reconciliation in :func:`submit_intent` detect and reconcile
    that after the fact, but they do not prevent the duplicate creation.  A host
    without ``fcntl`` fails closed rather than proceeding unserialized.
    """
    if fcntl is None:  # pragma: no cover - exercised only off POSIX
        raise LockUnavailableError(
            "durable submission requires POSIX advisory locking (fcntl); "
            "this host does not provide it"
        )
    home = Path(os.environ["DAGSTER_HOME"])
    directory = home / ".ictus-submission-locks"
    directory.mkdir(mode=0o700, exist_ok=True)
    filename = hashlib.sha256(intent_id.encode("utf-8")).hexdigest() + ".lock"
    with (directory / filename).open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _runs_for_identity(instance: DagsterInstance, identity: str) -> list[Any]:
    return list(
        instance.get_runs(filters=RunsFilter(tags={IDENTITY_TAG: identity}), ascending=True)
    )


def _run_order_key(run: Any) -> tuple[float, str]:
    """Deterministic canonical order: oldest creation time, then run id."""
    timestamp = getattr(run, "create_timestamp", None)
    if not isinstance(timestamp, (int, float)):
        timestamp = 0.0
    return (float(timestamp), str(getattr(run, "run_id", "")))


def _reconcile_existing(instance: DagsterInstance, identity: str, digest: str) -> Any | None:
    """Return the canonical run for an identity, or ``None`` if unseen.

    Fails closed if any run carrying this identity binds a different digest:
    one identity may never silently mean two different immutable payloads.
    """
    runs = _runs_for_identity(instance, identity)
    if not runs:
        return None
    ordered = sorted(runs, key=_run_order_key)
    mismatched = [
        str(getattr(run, "run_id", "?"))
        for run in ordered
        if (getattr(run, "tags", {}) or {}).get(DIGEST_TAG) != digest
    ]
    if mismatched:
        raise SubmissionConflict(
            f"submission identity '{identity}' is already bound to a different digest "
            f"(run(s): {', '.join(mismatched)})"
        )
    return ordered[0]


def _status_name(run: Any) -> str:
    return str(getattr(run, "status", _UNSUBMITTED_STATUS)).rsplit(".", 1)[-1].upper()


def _receipt(run: Any, digest: str | None = None) -> dict[str, Any]:
    tags = dict(getattr(run, "tags", {}) or {})
    return validate_receipt(
        {
            "schema_version": 1,
            "receipt_id": run.run_id,
            "execution_id": run.run_id,
            "intent_id": tags.get(IDENTITY_TAG, ""),
            "intent_digest": digest or tags.get(DIGEST_TAG, ""),
            "status": _status_name(run).lower(),
            "submitted_at": tags.get(SUBMITTED_AT_TAG, _now()),
            "evidence": [{"kind": "dagster_run", "uri": f"dagster://runs/{run.run_id}"}],
        }
    )


def _run_config(spec: Any, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "resources": {
            spec.resource_key: {"config": spec.settings_from_arguments(payload.get("arguments") or {})}
        }
    }


def _remote_job_origin(spec: Any) -> Any:
    """Build the remote origin the daemon resolves against its workspace.

    Only ``location_name`` is consulted by the daemon/launcher, so a
    ``RegisteredCodeLocationOrigin`` (managed externally by the workspace) is
    the correct lightweight origin for a caller that has no workspace here.
    """
    from dagster._core.remote_origin import (
        RegisteredCodeLocationOrigin,
        RemoteJobOrigin,
        RemoteRepositoryOrigin,
    )

    location_name = os.environ.get(CODE_LOCATION_ENV, "").strip()
    if not location_name:
        raise SubmissionConfigurationError(
            f"durable submission requires {CODE_LOCATION_ENV} to name the Dagster code "
            "location registered in the running daemon's workspace"
        )
    repository_name = os.environ.get(REPOSITORY_ENV, "").strip() or DEFAULT_REPOSITORY_NAME
    repository_origin = RemoteRepositoryOrigin(
        code_location_origin=RegisteredCodeLocationOrigin(location_name=location_name),
        repository_name=repository_name,
    )
    return RemoteJobOrigin(repository_origin=repository_origin, job_name=spec.job.name)


def _require_run_origins(run: Any) -> None:
    """Verify both origins the launcher needs were actually persisted."""
    missing = [
        name
        for name in ("remote_job_origin", "job_code_origin")
        if getattr(run, name, None) is None
    ]
    if missing:
        raise SubmissionConfigurationError(
            f"Dagster run '{getattr(run, 'run_id', '?')}' is missing required origin(s) "
            f"{', '.join(missing)}; the configured launcher cannot reconstruct it. "
            "This usually means it was recorded by an unsupported client."
        )


def _submit_to_dagster(instance: DagsterInstance, run_id: str) -> None:
    """Hand a durably-recorded run to Dagster's queued coordinator.

    ``workspace=None`` is safe only because :func:`production_instance` requires
    ``QueuedRunCoordinator``: its ``submit_run`` only enqueues.  The independent
    daemon attaches the real workspace when it dequeues and launches.  Never
    ``execute_in_process``.
    """
    submit = getattr(instance, "submit_run", None) or getattr(instance, "launch_run", None)
    if submit is None:
        raise SubmissionConfigurationError(
            "Dagster instance exposes neither submit_run nor launch_run"
        )
    # Dagster's public API has used both ``...(run_id)`` and
    # ``...(run_id, workspace)``.
    parameters = inspect.signature(submit).parameters
    if "workspace" in parameters:
        submit(run_id, None)
    else:
        submit(run_id)


def _create_run(
    instance: DagsterInstance,
    spec: Any,
    payload: dict[str, Any],
    identity: str,
    digest: str,
) -> Any:
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
    run = instance.create_run_for_job(
        spec.job,
        run_config=_run_config(spec, payload),
        tags=tags,
        remote_job_origin=_remote_job_origin(spec),
        job_code_origin=reconstructed.get_python_origin(),
    )
    _require_run_origins(run)
    return run


def _ensure_submitted(instance: DagsterInstance, run: Any) -> None:
    """Close the crash window between recording a run and enqueueing it.

    A run that exists but is still ``NOT_STARTED`` was recorded before the
    submitter died; it is safe (and idempotent) to enqueue it now.  Any other
    status means Dagster already accepted it, so it must never be resubmitted.
    """
    if _status_name(run) != _UNSUBMITTED_STATUS:
        return
    _require_run_origins(run)
    _submit_to_dagster(instance, run.run_id)


def _reload(instance: DagsterInstance, run: Any) -> Any:
    return instance.get_run_by_id(run.run_id) or run


def submit_intent(
    payload: dict[str, Any], *, instance: DagsterInstance | None = None
) -> dict[str, Any]:
    """Submit immutable intent once and return its durable receipt.

    A repeat request with the same identity and digest reconciles to the
    original receipt and never manufactures a second run.  A different digest
    fails closed.  If an earlier attempt recorded a run but crashed before
    enqueueing it, this call recovers that run instead of creating another.

    Recovery is idempotent but *caller-driven*: a stranded run is repaired only
    when the same identity is submitted again.  The durable control plane (for
    example Tactus) owns the retry; Ictus runs no background reconciler.
    """
    validate_intent(payload)
    dagster_instance = production_instance(instance)
    digest = intent_digest(payload)
    identity = payload["intent_id"]

    with _identity_lock(identity):
        existing = _reconcile_existing(dagster_instance, identity, digest)
        if existing is not None:
            _ensure_submitted(dagster_instance, existing)
            return _receipt(_reload(dagster_instance, existing), digest)

        spec = resolve_workflow(payload["capability"])
        run = _create_run(dagster_instance, spec, payload, identity, digest)
        # Intentionally after durable record creation: if the caller loses the
        # acknowledgement, the next caller finds this same record and recovers
        # it via ``_ensure_submitted``; it never manufactures a second run.
        _submit_to_dagster(dagster_instance, run.run_id)
        return _receipt(_reload(dagster_instance, run), digest)


def _run_events(instance: DagsterInstance, run_id: str) -> list[Any]:
    """A run's recorded events, or none when the instance cannot supply them."""
    try:
        return instance.all_logs(run_id)
    except Exception:  # pragma: no cover - unavailable event storage is evidence itself
        return []


def _failed_step_from_events(events: list[Any]) -> tuple[str | None, str | None]:
    for event_record in events:
        event = getattr(event_record, "dagster_event", None)
        if getattr(event, "event_type_value", None) != "STEP_FAILURE":
            continue
        error = getattr(getattr(event, "event_specific_data", None), "error", None)
        class_name = str(getattr(error, "cls_name", ""))
        if "Timeout" in class_name:
            return getattr(event, "step_key", None), "WORKER_TIMEOUT"
        return getattr(event, "step_key", None), _reported_failure_category(event)
    return None, None


def failed_step_from_run(
    instance: DagsterInstance, run_id: str
) -> tuple[str | None, str | None]:
    """Report a run's failed step and the typed category that step reported.

    Raw failure facts only: the step key Dagster failed, and the
    ``ictus.failure_category`` that step put on its ``Failure`` (the runtime seam
    classifies its own failures).  A step that reported nothing usable yields
    ``None`` for the category, which lets the caller fall back to its generic
    per-step default instead of inventing a cause.
    """
    return _failed_step_from_events(_run_events(instance, run_id))


def _reported_failure_category(event: Any) -> str | None:
    """The typed category a step reported, when it reported a known one.

    A step that classifies its own failure (the runtime seam does) knows better
    than the generic per-step default, so its ``ictus.failure_category`` is
    reported verbatim.  An unknown or malformed value falls back to the
    per-step category instead of inventing one.
    """
    failure = getattr(event.event_specific_data, "user_failure_data", None)
    metadata = getattr(failure, "metadata", None)
    if not isinstance(metadata, dict):
        return None
    reported = getattr(metadata.get("ictus.failure_category"), "text", None)
    if reported in OBSERVATION_CATEGORIES:
        return reported
    return None


#: Step metadata key the runtime seam's finalize step records its summary under.
SEAM_EVIDENCE_KEY = "ictus.seam_evidence"


def _seam_evidence_from_events(events: list[Any]) -> dict[str, Any] | None:
    for event_record in events:
        event = getattr(event_record, "dagster_event", None)
        metadata = getattr(
            getattr(event, "event_specific_data", None), "metadata", None
        )
        if not isinstance(metadata, dict):
            continue
        recorded = metadata.get(SEAM_EVIDENCE_KEY)
        payload = getattr(recorded, "value", None)
        if isinstance(payload, dict):
            return payload
    return None


def seam_evidence_from_run(
    instance: DagsterInstance, run_id: str
) -> dict[str, Any] | None:
    """Read the runtime seam's evidence document from a run's step record.

    The seam's finalize step records the structured summary (changed paths,
    patch digest and size, exit/termination status) as step metadata, so what
    the runtime reported survives the run instead of living in a log tail.
    Returns ``None`` when the run carried no seam (the fail-closed routes) or
    when no event storage is available.
    """
    return _seam_evidence_from_events(_run_events(instance, run_id))


def _receipt_id(receipt: dict[str, Any] | str) -> str:
    if isinstance(receipt, str):
        run_id = receipt
    elif isinstance(receipt, dict):
        run_id = receipt.get("receipt_id", "")
    else:
        run_id = ""
    if not isinstance(run_id, str) or not run_id:
        raise UnknownReceipt("receipt must include a non-empty receipt_id")
    return run_id


def _require_known_run(instance: DagsterInstance, run_id: str) -> Any:
    run = instance.get_run_by_id(run_id)
    if run is None or (getattr(run, "tags", {}) or {}).get(SUBMISSION_TAG) != SUBMISSION_PROTOCOL:
        raise UnknownReceipt(f"unknown receipt '{run_id}'")
    return run


def _run_record_times(instance: DagsterInstance, run: Any) -> tuple[str | None, str | None]:
    """Return the run's real start/finish times as ISO-8601, when Dagster has them.

    ``DagsterRun`` carries no timestamps (Dagster 1.13.x); the instance's run
    record does, filled from the ``RUN_START`` and terminal events.  A run the
    backend has no record for, or a run that never started, yields ``None`` for
    the missing side rather than a manufactured wall-clock time.
    """
    records = instance.get_run_records(filters=RunsFilter(run_ids=[run.run_id]), limit=1)
    if not records:
        return None, None
    record = records[0]
    return _iso(getattr(record, "start_time", None)), _iso(
        getattr(record, "end_time", None)
    )


def _status_envelope(
    instance: DagsterInstance,
    run: Any,
    *,
    cancellation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the versioned ExecutionStatus read model for a run."""
    response: dict[str, Any] = {
        "schema_version": 1,
        "receipt": _receipt(run),
        "state": "pending",
    }
    status_name = _status_name(run)
    if status_name in TERMINAL_RUN_STATUSES:
        # One event-log read supplies both facts the result needs.
        events = _run_events(instance, run.run_id)
        failed_step, failure_category = _failed_step_from_events(events)
        started_at, finished_at = _run_record_times(instance, run)
        response["state"] = "terminal"
        response["result"] = execution_result_from_run(
            execution_id=run.run_id,
            intent_id=response["receipt"]["intent_id"],
            success=status_name == "SUCCESS",
            run_status=status_name,
            failed_step=failed_step,
            failure_category=failure_category,
            started_at=started_at,
            finished_at=finished_at,
            evidence=[
                {"kind": "dagster_run", "uri": f"dagster://runs/{run.run_id}"},
                {"kind": "dagster_event_log", "uri": f"dagster://runs/{run.run_id}/events"},
            ],
            seam_evidence=_seam_evidence_from_events(events),
        )
    if cancellation is not None:
        response["cancellation"] = cancellation
    return validate_execution_status(response)


def query_receipt(
    receipt: dict[str, Any] | str, *, instance: DagsterInstance | None = None
) -> dict[str, Any]:
    """Return current durable status and a correlated terminal result, if any."""
    dagster_instance = production_instance(instance)
    run = _require_known_run(dagster_instance, _receipt_id(receipt))
    return _status_envelope(dagster_instance, run)


def cancel_receipt(
    receipt: dict[str, Any] | str, *, instance: DagsterInstance | None = None
) -> dict[str, Any]:
    """Request cancellation from Dagster and return the reconciled status.

    Cancellation *requested* is distinct from cancellation *completed*: the
    response records whether the coordinator accepted the request, and its
    ``state`` reconciles to terminal only once the run itself is terminal
    (which may require a second :func:`query_receipt` if it is CANCELING).
    """
    dagster_instance = production_instance(instance)
    run_id = _receipt_id(receipt)
    run = _require_known_run(dagster_instance, run_id)

    coordinator = getattr(dagster_instance, "run_coordinator", None)
    cancel = getattr(coordinator, "cancel_run", None)
    if cancel is None:
        raise SubmissionConfigurationError(
            "Dagster run coordinator exposes no cancel_run; cannot request cancellation"
        )
    accepted = bool(cancel(run_id))
    run = _reload(dagster_instance, run)
    cancellation = {
        "requested": True,
        "accepted": accepted,
        "status": _status_name(run).lower(),
    }
    return _status_envelope(dagster_instance, run, cancellation=cancellation)
