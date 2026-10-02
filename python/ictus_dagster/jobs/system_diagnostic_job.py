"""The generic system-diagnostics job.

``collect -> inspect -> classify -> report``

A third domain workflow with yet another shape. It exists to demonstrate that
the generic core, contracts and bridge do not depend on a particular domain.
"""

from __future__ import annotations

from dagster import job

from ictus_dagster.ops.system_diagnostic import (
    classify_op,
    collect_op,
    inspect_op,
    report_op,
)
from ictus_dagster.resources.diagnostic_settings import DiagnosticSettings


@job(
    name="system_diagnostic_job",
    resource_defs={"diag_settings": DiagnosticSettings()},
    description=(
        "Generic system diagnostics: collect -> inspect -> classify -> report. "
        "Carries no policy semantics; receives a validated ExecutionIntent."
    ),
)
def system_diagnostic_job() -> None:
    collected = collect_op()
    inspected = inspect_op(collected)
    classified = classify_op(inspected)
    report_op(classified)
