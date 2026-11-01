"""The runtime seam's four steps, shared by the software execution jobs.

``prepare -> execute -> verify -> finalize`` is the graph shape Ictus owns.  Each
step re-resolves the seam from the same operator-owned config path (Dagster
re-resolves resources per step too) and then delegates through
:mod:`ictus_dagster.seam.port`:

* ``prepare`` holds the workspace by reference and records the runtime's own
  version/probe report back as evidence;
* ``execute`` bounds one attempt (write scope, wall clock, step budget) and
  validates the structured evidence the runtime returns;
* ``verify`` re-checks that evidence against the operator's bounds and asks the
  runtime to verify its own post-conditions;
* ``finalize`` emits the summary other code builds a result from.

This module carries no control-plane, domain or vendor dependency: the runtime
backend is named in configuration, resolved by name at run time and reached only
through the port.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

from dagster import OpExecutionContext

from ictus_dagster.seam.config import SeamConfig, SeamConfigError, load_seam_config
from ictus_dagster.seam.control import AttemptHalted, ExecutionControl
from ictus_dagster.seam.evidence import EvidenceError, out_of_scope, validate_evidence
from ictus_dagster.seam.failures import placeholder_failure, seam_failure
from ictus_dagster.seam.port import AttemptRequest, RuntimeBackend, WorkspaceRef

PROBE_REPORT_FIELDS = frozenset({"executable", "version_token", "probe"})
PROBE_FIELDS = frozenset({"ok", "detail"})
VERIFICATION_FIELDS = frozenset({"verified", "detail"})


@dataclass(frozen=True)
class Seam:
    """One configured seam: the operator's config plus the resolved backend."""

    config: SeamConfig
    backend: RuntimeBackend


def resolve_backend(reference: str) -> RuntimeBackend:
    """Resolve the backend named in configuration, without importing it here.

    The named attribute is called with no arguments and must return an object
    implementing the port.  Resolution is lazy and by name: this module never
    imports the runtime, and a backend that cannot be resolved fails closed.
    """
    module_name, _, attribute = reference.partition(":")
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise SeamConfigError(
            f"runtime seam backend '{reference}' cannot be imported: {exc}"
        ) from None
    try:
        factory = getattr(module, attribute)
    except AttributeError:
        raise SeamConfigError(
            f"runtime seam backend '{module_name}' has no attribute '{attribute}'"
        ) from None
    backend = factory()
    if not isinstance(backend, RuntimeBackend):
        raise SeamConfigError(
            f"runtime seam backend '{reference}' does not implement the seam port"
        )
    return backend


def open_seam(
    *, capability: str, seam_config_path: str | None
) -> Seam | None:
    """Return the configured seam, or ``None`` when nothing is configured.

    ``capability`` is accepted for log context and reserved for future
    per-capability bindings; the config file currently names one backend.
    """
    config = load_seam_config(seam_config_path)
    if config is None:
        return None
    return Seam(config=config, backend=resolve_backend(config.backend))


def _require_seam(
    context: OpExecutionContext,
    *,
    job_name: str,
    capability: str,
    seam_config_path: str | None,
) -> Seam:
    """Open the seam, or fail closed exactly as the placeholder always has."""
    try:
        seam = open_seam(capability=capability, seam_config_path=seam_config_path)
    except SeamConfigError as exc:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="CONFIGURATION",
            description=f"{job_name}: runtime seam is misconfigured: {exc}",
        ) from None
    if seam is None:
        context.log.error(
            "%s: refusing unimplemented capability=%s", job_name, capability
        )
        raise placeholder_failure(job_name=job_name, capability=capability)
    return seam


def _validate_probe_report(
    report: Any, *, job_name: str, capability: str
) -> dict[str, Any]:
    if not isinstance(report, dict):
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="CONFIGURATION",
            description="runtime backend returned no workspace probe report",
        )
    missing = PROBE_REPORT_FIELDS - set(report)
    probe = report.get("probe")
    if missing or not isinstance(probe, dict) or PROBE_FIELDS - set(probe):
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="CONFIGURATION",
            description=(
                "runtime backend probe report must carry executable, "
                "version_token and probe{ok, detail}"
            ),
            details={"report": report},
        )
    if not isinstance(probe["ok"], bool) or not isinstance(probe["detail"], str):
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="CONFIGURATION",
            description="runtime backend probe report is mistyped",
            details={"report": report},
        )
    return report


def prepare(
    context: OpExecutionContext,
    *,
    job_name: str,
    capability: str,
    seam_config_path: str | None,
) -> dict[str, Any]:
    """Hold the workspace by reference and record the runtime's probe report."""
    seam = _require_seam(
        context,
        job_name=job_name,
        capability=capability,
        seam_config_path=seam_config_path,
    )
    workspace = WorkspaceRef(descriptor_path=seam.config.workspace_descriptor)
    try:
        report = seam.backend.describe_workspace(workspace)
    except Exception as exc:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="BACKEND_ERROR",
            description=f"runtime backend failed to describe the workspace: {exc}",
        ) from None
    report = _validate_probe_report(report, job_name=job_name, capability=capability)
    if not report["probe"]["ok"]:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="WORKSPACE_PROBE_FAILED",
            description=f"workspace probe failed: {report['probe']['detail']}",
            details=report,
        )
    context.log.info(
        "%s: prepared capability=%s backend=%s workspace=%s",
        job_name,
        capability,
        seam.config.backend,
        workspace.descriptor_path,
    )
    return {
        "capability": capability,
        "job": job_name,
        "seam_config_path": seam.config.path,
        "prepared": True,
        "workspace": workspace.to_json(),
        "seam": seam.config.to_json(),
        "runtime": report,
    }


def _halted_failure(
    control: ExecutionControl,
    exc: AttemptHalted,
    *,
    job_name: str,
    capability: str,
) -> Any:
    details = control.termination
    if details is None:
        details = control.cancel(f"halted:{exc.kind.lower()}")
    return seam_failure(
        job_name=job_name,
        capability=capability,
        failure_kind=exc.kind,
        description=str(exc),
        details=details,
    )


def execute(context: OpExecutionContext, prepared: dict[str, Any]) -> dict[str, Any]:
    """Run one bounded attempt and validate the evidence it reports back."""
    job_name = prepared["job"]
    capability = prepared["capability"]
    seam = _require_seam(
        context,
        job_name=job_name,
        capability=capability,
        seam_config_path=prepared["seam_config_path"],
    )
    control = ExecutionControl(
        wall_clock_seconds=seam.config.wall_clock_seconds,
        step_budget=seam.config.step_budget,
    )
    request = AttemptRequest(
        capability=capability,
        write_scope=seam.config.write_scope,
        wall_clock_seconds=seam.config.wall_clock_seconds,
        step_budget=seam.config.step_budget,
        workspace=WorkspaceRef(prepared["workspace"]["descriptor_path"]),
    )
    try:
        document = seam.backend.execute(request, control)
    except AttemptHalted as exc:
        raise _halted_failure(
            control, exc, job_name=job_name, capability=capability
        ) from None
    except Exception as exc:
        control.cancel("backend_exception")
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="BACKEND_ERROR",
            description=f"runtime backend failed during execution: {exc}",
            details=control.termination,
        ) from None
    leftover = control.finish()
    if leftover is not None:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="SUPERVISION_VIOLATION",
            description=(
                "runtime backend returned while a process group it started was "
                "still running; it was terminated"
            ),
            details=leftover,
        )
    try:
        evidence = validate_evidence(document)
    except EvidenceError as exc:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="CONFIGURATION",
            description=f"runtime backend returned invalid evidence: {exc}",
        ) from None
    termination = evidence["termination"]
    if termination["requested"] and not termination["confirmed"]:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="TERMINATION_UNCONFIRMED",
            description=(
                "runtime reported a termination it could not confirm; the "
                "attempt must not be treated as stopped"
            ),
            details=evidence,
        )
    if evidence["outcome"] != "succeeded":
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind=evidence["failure_kind"],
            description=(
                f"attempt ended '{evidence['outcome']}' "
                f"({evidence['failure_kind']})"
            ),
            details=evidence,
        )
    context.log.info(
        "%s: executed capability=%s steps=%d patch_bytes=%d",
        job_name,
        capability,
        evidence["steps_used"],
        evidence["patch_bytes"],
    )
    return {**prepared, "executed": True, "evidence": evidence}


def _validate_verification(
    verification: Any, *, job_name: str, capability: str
) -> dict[str, Any]:
    if (
        not isinstance(verification, dict)
        or VERIFICATION_FIELDS - set(verification)
        or not isinstance(verification["verified"], bool)
        or not isinstance(verification["detail"], str)
    ):
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="CONFIGURATION",
            description="runtime backend verification must carry verified{bool} and detail{str}",
            details={"verification": verification},
        )
    return verification


def verify(context: OpExecutionContext, executed: dict[str, Any]) -> dict[str, Any]:
    """Re-check the attempt's evidence against the operator's bounds."""
    job_name = executed["job"]
    capability = executed["capability"]
    seam = _require_seam(
        context,
        job_name=job_name,
        capability=capability,
        seam_config_path=executed["seam_config_path"],
    )
    evidence = executed["evidence"]
    outside = out_of_scope(evidence["changed_paths"], seam.config.write_scope)
    if outside:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="SCOPE_VIOLATION",
            description=(
                "attempt changed path(s) outside the declared write scope: "
                + ", ".join(outside)
            ),
            details=evidence,
        )
    if evidence["steps_used"] > seam.config.step_budget:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="STEP_BUDGET_EXCEEDED",
            description=(
                f"attempt reported {evidence['steps_used']} steps against a "
                f"budget of {seam.config.step_budget}"
            ),
            details=evidence,
        )
    if evidence["duration_seconds"] > seam.config.wall_clock_seconds:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="WALL_CLOCK_EXCEEDED",
            description=(
                f"attempt reported {evidence['duration_seconds']}s against a "
                f"wall clock of {seam.config.wall_clock_seconds}s"
            ),
            details=evidence,
        )
    try:
        verification = seam.backend.verify(evidence)
    except Exception as exc:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="BACKEND_ERROR",
            description=f"runtime backend failed to verify the attempt: {exc}",
        ) from None
    verification = _validate_verification(
        verification, job_name=job_name, capability=capability
    )
    if not verification["verified"]:
        raise seam_failure(
            job_name=job_name,
            capability=capability,
            failure_kind="RUNTIME_VERIFICATION_FAILED",
            description=(
                "runtime verification failed"
                + (f": {verification['detail']}" if verification["detail"] else "")
            ),
            details=evidence,
        )
    context.log.info(
        "%s: verified capability=%s outcome=%s",
        job_name,
        capability,
        evidence["outcome"],
    )
    return {**executed, "verified": True, "verification": verification}


def finalize(context: OpExecutionContext, verified: dict[str, Any]) -> dict[str, Any]:
    """Emit the summary other code builds an execution result from."""
    evidence = verified["evidence"]
    summary = {
        "capability": verified["capability"],
        "job": verified["job"],
        "verified": True,
        "outcome": evidence["outcome"],
        "changed_paths": list(evidence["changed_paths"]),
        "patch_digest": evidence["patch_digest"],
        "patch_bytes": evidence["patch_bytes"],
        "steps_used": evidence["steps_used"],
        "duration_seconds": evidence["duration_seconds"],
        "exit_status": evidence["exit_status"],
        "termination": evidence["termination"],
        "verification": verified["verification"],
        "runtime": verified["runtime"],
    }
    context.log.info("%s: finalize summary=%s", verified["job"], summary)
    return summary
