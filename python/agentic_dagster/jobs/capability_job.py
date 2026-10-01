"""The generic capability execution job.

``prepare -> execute -> verify -> finalize``

The graph is intentionally generic and contains no domain-specific step. Both
Dagster and the decision core depend only on the versioned contracts.
"""

from __future__ import annotations

from dagster import job

from agentic_dagster.ops.execute import execute_op
from agentic_dagster.ops.finalize import finalize_op
from agentic_dagster.ops.prepare import prepare_op
from agentic_dagster.ops.verify import verify_op
from agentic_dagster.resources.execution_settings import ExecutionSettings


@job(
    name="capability_execution_job",
    resource_defs={"settings": ExecutionSettings()},
    description=(
        "Generic durable capability execution: prepare -> execute -> verify -> finalize. "
        "Carries no domain/policy semantics; receives a validated ExecutionIntent."
    ),
)
def capability_execution_job() -> None:
    prepared = prepare_op()
    executed = execute_op(prepared)
    verified = verify_op(executed)
    finalize_op(verified)
