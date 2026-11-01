"""Structured attempt evidence and the typed failure kinds the seam reports.

A runtime reports what happened as one validated document — changed paths, the
patch digest and size, the exit/termination status, a typed failure kind — not
as a log tail.  This module is pure Python: it carries no Dagster import, no
control-plane import, no domain import and no runtime dependency.
``python/ictus_dagster/`` must never import a control plane, a domain adapter or
a specific agent runtime; the runtime is named in an operator-owned config file
and reached only through the seam's port (``ictus_dagster.seam.port``).
"""

from __future__ import annotations

import math
import re
from typing import Any

#: Version of the evidence document a backend returns from ``execute``.
EVIDENCE_SCHEMA_VERSION = 1

#: Terminal outcomes an attempt may report back.
OUTCOMES = frozenset({"succeeded", "failed", "cancelled", "timed_out"})

#: Typed failure kinds.  A runtime classifies *its own* failure; Ictus never
#: guesses a cause from a log line.
FAILURE_KINDS = frozenset(
    {
        # The seam (config, backend reference, reported document) was unusable.
        "CONFIGURATION",
        # The runtime's own workspace probe reported the workspace unusable.
        "WORKSPACE_PROBE_FAILED",
        # The backend raised, crashed or returned something unusable.
        "BACKEND_ERROR",
        # The attempt wrote outside the declared write scope.
        "SCOPE_VIOLATION",
        # The attempt exceeded its step or wall-clock budget.
        "STEP_BUDGET_EXCEEDED",
        "WALL_CLOCK_EXCEEDED",
        # The backend returned while a process group it started was still live.
        "SUPERVISION_VIOLATION",
        # A termination was requested but could not be confirmed.
        "TERMINATION_UNCONFIRMED",
        # The attempt's process exited unsuccessfully.
        "PROCESS_EXIT",
        # Cancellation was requested and completed.
        "CANCEL_REQUESTED",
        # The runtime's own post-condition check failed.
        "RUNTIME_VERIFICATION_FAILED",
    }
)

#: Generic observation category for each typed failure kind.  This is a
#: back-end-side *report* of a failure fact; the policy layer stays the
#: authority over what a category means for a decision.
FAILURE_CATEGORIES: dict[str, str] = {
    "CONFIGURATION": "INTEGRATION_CONFLICT",
    "WORKSPACE_PROBE_FAILED": "INTEGRATION_CONFLICT",
    "BACKEND_ERROR": "PROCESS_CRASH",
    "SCOPE_VIOLATION": "VERIFICATION_FAILURE",
    "STEP_BUDGET_EXCEEDED": "RESOURCE_EXHAUSTED",
    "WALL_CLOCK_EXCEEDED": "WORKER_TIMEOUT",
    "SUPERVISION_VIOLATION": "PROCESS_CRASH",
    "TERMINATION_UNCONFIRMED": "PROCESS_CRASH",
    "PROCESS_EXIT": "PROCESS_CRASH",
    "CANCEL_REQUESTED": "UNKNOWN",
    "RUNTIME_VERIFICATION_FAILED": "VERIFICATION_FAILURE",
}

#: Every field an evidence document must carry.
REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "outcome",
        "failure_kind",
        "changed_paths",
        "patch_digest",
        "patch_bytes",
        "steps_used",
        "exit_status",
        "termination",
        "duration_seconds",
    }
)

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


class EvidenceError(ValueError):
    """Raised when an attempt evidence document does not satisfy the contract."""


def category_for_failure_kind(kind: str) -> str:
    """Return the generic observation category for a typed failure kind."""
    try:
        return FAILURE_CATEGORIES[kind]
    except KeyError:
        raise EvidenceError(f"unknown failure kind '{kind}'") from None


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_failure_kind(kind: Any, outcome: str) -> None:
    if outcome == "succeeded":
        if kind is not None:
            raise EvidenceError("a succeeded attempt must not report a failure kind")
        return
    if kind not in FAILURE_KINDS:
        raise EvidenceError(
            f"outcome '{outcome}' requires a typed failure kind from "
            f"{sorted(FAILURE_KINDS)}"
        )


def _validate_exit_status(value: Any) -> None:
    if not isinstance(value, dict):
        raise EvidenceError("exit_status must be a JSON object")
    missing = {"code", "signal"} - set(value)
    if missing:
        raise EvidenceError(
            f"exit_status is missing field(s): {', '.join(sorted(missing))}"
        )
    if value["code"] is not None and not _is_int(value["code"]):
        raise EvidenceError("exit_status.code must be an integer or null")
    if value["signal"] is not None and not isinstance(value["signal"], str):
        raise EvidenceError("exit_status.signal must be a string or null")


def _validate_termination(value: Any) -> None:
    if not isinstance(value, dict):
        raise EvidenceError("termination must be a JSON object")
    missing = {"requested", "signals", "confirmed"} - set(value)
    if missing:
        raise EvidenceError(
            f"termination is missing field(s): {', '.join(sorted(missing))}"
        )
    for name in ("requested", "confirmed"):
        if not isinstance(value[name], bool):
            raise EvidenceError(f"termination.{name} must be a boolean")
    signals = value["signals"]
    if not isinstance(signals, list) or not all(isinstance(item, str) for item in signals):
        raise EvidenceError("termination.signals must be a list of strings")


def validate_evidence(document: Any) -> dict[str, Any]:
    """Validate an attempt evidence document and return it unchanged.

    Validation is fail-closed on shape only: it never reinterprets what the
    runtime reported.  Unknown fields are preserved (a runtime may add its own
    diagnostics); a missing, mistyped or inconsistent required field is
    rejected.
    """
    if not isinstance(document, dict):
        raise EvidenceError("attempt evidence must be a JSON object")
    missing = REQUIRED_FIELDS - set(document)
    if missing:
        raise EvidenceError(
            f"attempt evidence is missing field(s): {', '.join(sorted(missing))}"
        )
    version = document["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise EvidenceError(
            f"evidence schema_version must be an integer, not {version!r}"
        )
    if version != EVIDENCE_SCHEMA_VERSION:
        raise EvidenceError(
            f"unsupported evidence schema_version {version!r}; "
            f"expected {EVIDENCE_SCHEMA_VERSION}"
        )
    outcome = document["outcome"]
    if outcome not in OUTCOMES:
        raise EvidenceError(
            f"unsupported outcome {outcome!r}; expected one of {sorted(OUTCOMES)}"
        )
    _validate_failure_kind(document["failure_kind"], outcome)

    changed_paths = document["changed_paths"]
    if not isinstance(changed_paths, list) or not all(
        isinstance(path, str) and path for path in changed_paths
    ):
        raise EvidenceError("changed_paths must be a list of non-empty strings")

    digest = document["patch_digest"]
    if digest is not None and (
        not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
    ):
        raise EvidenceError("patch_digest must be null or 'sha256:<64 lowercase hex>'")

    for name in ("patch_bytes", "steps_used"):
        value = document[name]
        if not _is_int(value) or value < 0:
            raise EvidenceError(f"{name} must be a non-negative integer")

    duration = document["duration_seconds"]
    if not isinstance(duration, (int, float)) or isinstance(duration, bool):
        raise EvidenceError("duration_seconds must be a number")
    if not math.isfinite(duration) or duration < 0:
        raise EvidenceError("duration_seconds must be a finite, non-negative number")

    _validate_exit_status(document["exit_status"])
    _validate_termination(document["termination"])
    return document


def normalize_scope_path(path: str) -> str:
    """Normalize a scope/patch path for the syntactic containment check."""
    while path.startswith("./"):
        path = path[2:]
    return path.rstrip("/")


def out_of_scope(changed_paths: list[str], write_scope: tuple[str, ...]) -> list[str]:
    """Return the changed paths outside the declared write scope.

    Containment is deliberately syntactic and default-deny: a changed path is
    in scope when it equals a scope entry or continues it with ``/``.  Ictus
    never resolves symlinks, never opens the workspace and never interprets the
    runtime's view of it; a runtime that reports absolute paths must be given
    absolute scope entries, and a mismatch fails the attempt closed.  A root
    entry ``/`` therefore contains absolute paths only, and a relative path
    needs a relative entry.
    """
    scopes = tuple(normalize_scope_path(scope) for scope in write_scope)
    outside = []
    for changed_path in changed_paths:
        normalized = normalize_scope_path(changed_path)
        if not any(
            normalized == scope or normalized.startswith(f"{scope}/")
            for scope in scopes
        ):
            outside.append(changed_path)
    return outside
