"""``prepare`` step: validate the capability the intent targets."""

from __future__ import annotations

from dagster import Out, op

from ictus_dagster.resources.execution_settings import ExecutionSettings


@op(out=Out(dict))
def prepare_op(context, settings: ExecutionSettings) -> dict:
    """Prepare a bounded capability execution.

    This step performs no policy work: the intent was already validated by the
    Rust decision/policy core before it reached this backend.
    """
    context.log.info("prepare: capability=%s", settings.capability)
    return {"capability": settings.capability, "prepared": True}
