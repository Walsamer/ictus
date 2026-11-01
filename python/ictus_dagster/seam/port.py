"""The runtime seam's port: what Ictus asks of a runtime, as data.

Ictus owns the step skeleton (``prepare -> execute -> verify -> finalize``) and
never owns a runtime.  The runtime arrives through the seam: its module is named
in an operator-owned config file, resolved by name at run time, and reached
only through this protocol.  No control plane, domain adapter, vendor, binary or
model is imported by anything in ``python/ictus_dagster/``.

A backend implements three methods:

``describe_workspace``
    Report the workspace's runtime identity (executable, version token and the
    result of its own probe).  Ictus records the report back as evidence; it
    never reads the descriptor file and never trusts the probe silently.

``execute``
    Run one bounded attempt against the request and return a validated evidence
    document (see :mod:`ictus_dagster.seam.evidence`).  Everything it starts
    goes through :class:`ictus_dagster.seam.control.ExecutionControl`.

``verify``
    Report the runtime's own post-condition result as
    ``{"verified": bool, "detail": str}``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ictus_dagster.seam.control import ExecutionControl


@dataclass(frozen=True)
class WorkspaceRef:
    """A workspace, held by reference: the path to the owner's descriptor.

    Ictus never opens, parses or validates this file.  Whatever the descriptor
    declares (an executable, a version token, a probe) is the runtime's and its
    operator's business; the runtime reports what it found back through
    ``describe_workspace``.
    """

    descriptor_path: str

    def to_json(self) -> dict[str, str]:
        return {"descriptor_path": self.descriptor_path}


@dataclass(frozen=True)
class AttemptRequest:
    """Everything one bounded attempt is allowed to know, and no more."""

    capability: str
    write_scope: tuple[str, ...]
    wall_clock_seconds: float
    step_budget: int
    workspace: WorkspaceRef


@runtime_checkable
class RuntimeBackend(Protocol):
    """The narrow port between the Ictus step skeleton and a runtime."""

    def describe_workspace(self, workspace: WorkspaceRef) -> dict[str, Any]:
        """Report the workspace runtime's identity and probe result."""
        ...

    def execute(
        self, request: AttemptRequest, control: ExecutionControl
    ) -> dict[str, Any]:
        """Run one bounded attempt and return an evidence document."""
        ...

    def verify(self, evidence: dict[str, Any]) -> dict[str, Any]:
        """Check the runtime's own post-conditions on the returned evidence."""
        ...
