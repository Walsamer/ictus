"""``verify`` step: bounded check on the execution output."""

from __future__ import annotations

from dagster import Failure, In, Out, op

from ictus_dagster.resources.execution_settings import ExecutionSettings


@op(ins={"executed": In(dict)}, out=Out(dict))
def verify_op(
    context,
    executed: dict,
    settings: ExecutionSettings,
) -> dict:
    """Verify the execution produced a usable result.

    ``fail_verify=true`` deterministically produces a verification failure so the
    decision layer can observe a ``VERIFICATION_FAILURE`` fact.
    """
    if settings.fail_verify:
        raise Failure(description="intentional verification failure")
    if not executed.get("executed"):
        raise Failure(description="nothing was executed to verify")
    context.log.info(
        "verify: capability=%s execute_attempts=%s",
        settings.capability,
        executed.get("execute_attempts"),
    )
    return {**executed, "verified": True}
