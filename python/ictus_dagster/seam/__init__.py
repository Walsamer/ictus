"""The runtime seam: Ictus owns the steps, a configured backend owns the runtime.

The boundary this package keeps: ``python/ictus_dagster/`` imports no control
plane, no domain adapter and no specific agent runtime, vendor, binary or model.
A runtime is *named in configuration* (``contracts/runtime-seam.schema.json``,
version 1) as a ``module:attribute`` reference, resolved lazily at run time, and
reached only through :mod:`ictus_dagster.seam.port`.  With no configuration the
seam is closed: the software jobs keep their placeholder behaviour and fail with
the same typed ``INTEGRATION_CONFLICT`` failure they always did.
"""

from __future__ import annotations

from ictus_dagster.seam.config import (
    SEAM_CONFIG_VERSION,
    SeamConfig,
    SeamConfigError,
    load_seam_config,
    parse_seam_config,
)
from ictus_dagster.seam.control import (
    AttemptAlreadyCancelled,
    AttemptHalted,
    ExecutionControl,
    StepBudgetExceeded,
    WallClockExceeded,
)
from ictus_dagster.seam.evidence import (
    EVIDENCE_SCHEMA_VERSION,
    FAILURE_CATEGORIES,
    FAILURE_KINDS,
    OUTCOMES,
    EvidenceError,
    category_for_failure_kind,
    out_of_scope,
    validate_evidence,
)
from ictus_dagster.seam.failures import placeholder_failure, seam_failure
from ictus_dagster.seam.port import AttemptRequest, RuntimeBackend, WorkspaceRef
from ictus_dagster.seam.process import ProcessSupervisor, TerminationError

__all__ = [
    "AttemptAlreadyCancelled",
    "AttemptHalted",
    "AttemptRequest",
    "EVIDENCE_SCHEMA_VERSION",
    "EvidenceError",
    "ExecutionControl",
    "FAILURE_CATEGORIES",
    "FAILURE_KINDS",
    "OUTCOMES",
    "ProcessSupervisor",
    "RuntimeBackend",
    "SEAM_CONFIG_VERSION",
    "SeamConfig",
    "SeamConfigError",
    "StepBudgetExceeded",
    "TerminationError",
    "WallClockExceeded",
    "WorkspaceRef",
    "category_for_failure_kind",
    "load_seam_config",
    "out_of_scope",
    "parse_seam_config",
    "placeholder_failure",
    "seam_failure",
    "validate_evidence",
]
