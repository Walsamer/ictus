"""The generic data-quality job.

``ingest -> profile -> check -> publish``

A second domain workflow with a different shape from the capability workflow.
It exists to demonstrate that the generic core, contracts and bridge do not
depend on a software-engineering domain.
"""

from __future__ import annotations

from dagster import job

from ictus_dagster.ops.data_quality import check_op, ingest_op, profile_op, publish_op
from ictus_dagster.resources.data_quality_settings import DataQualitySettings


@job(
    name="data_quality_job",
    resource_defs={"dq_settings": DataQualitySettings()},
    description=(
        "Generic data-quality execution: ingest -> profile -> check -> publish. "
        "Carries no policy semantics; receives a validated ExecutionIntent."
    ),
)
def data_quality_job() -> None:
    ingested = ingest_op()
    profiled = profile_op(ingested)
    checked = check_op(profiled)
    publish_op(checked)
