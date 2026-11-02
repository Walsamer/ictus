"""Typed, non-retryable failures raised by the runtime seam's steps.

Every seam failure is a Dagster ``Failure`` with ``allow_retries=False`` and an
``ictus.failure_category`` in its metadata, so a control plane reads a typed
fact rather than inferring one from a log line.  The unconfigured case keeps
exactly the placeholder failure Ictus raised before the seam existed, message
included, so an unconfigured deployment cannot accidentally succeed and its
category does not change.

This module carries no control-plane, domain or runtime dependency.
"""

from __future__ import annotations

import json
from typing import Any

from dagster import Failure

from ictus_dagster.seam.evidence import category_for_failure_kind


def placeholder_failure(*, job_name: str, capability: str) -> Failure:
    """The fail-closed default: no seam is configured, so nothing may run."""
    return Failure(
        description=(
            f"{job_name} is a fail-closed placeholder: "
            f"'{capability}' has no implementation in "
            "this Ictus code location"
        ),
        metadata={
            "ictus.capability": capability,
            "ictus.job": job_name,
            "ictus.failure_category": "INTEGRATION_CONFLICT",
            "ictus.placeholder": True,
            "ictus.retryable": False,
        },
        allow_retries=False,
    )


def seam_failure(
    *,
    job_name: str,
    capability: str,
    failure_kind: str,
    description: str,
    details: dict[str, Any] | None = None,
) -> Failure:
    """A typed seam failure: the runtime's own kind, mapped to a category."""
    metadata: dict[str, Any] = {
        "ictus.capability": capability,
        "ictus.job": job_name,
        "ictus.failure_category": category_for_failure_kind(failure_kind),
        "ictus.failure_kind": failure_kind,
        "ictus.placeholder": False,
        "ictus.retryable": False,
    }
    if details is not None:
        metadata["ictus.evidence"] = json.dumps(
            details, sort_keys=True, default=str
        )
    return Failure(
        description=description,
        metadata=metadata,
        allow_retries=False,
    )
