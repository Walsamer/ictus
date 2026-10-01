"""``finalize`` step: emit the bounded execution summary."""

from __future__ import annotations

from dagster import In, Out, op

from agentic_dagster.resources.execution_settings import ExecutionSettings


@op(ins={"verified": In(dict)}, out=Out(dict))
def finalize_op(
    context,
    verified: dict,
    settings: ExecutionSettings,
) -> dict:
    """Produce the execution summary consumed by the result adapter."""
    summary = {
        "capability": settings.capability,
        "verified": bool(verified.get("verified")),
        "execute_attempts": verified.get("execute_attempts", 0),
    }
    context.log.info("finalize: summary=%s", summary)
    return summary
