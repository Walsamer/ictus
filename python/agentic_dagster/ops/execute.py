"""``execute`` step: the single step with deterministic failure injection.

A ``RetryPolicy`` makes Dagster — not the policy layer — own retry mechanics.
The current attempt comes from Dagster's ``context.retry_number`` (0 for the
initial attempt), so no attempt state is kept in application code.
"""

from __future__ import annotations

from dagster import Failure, In, Out, RetryPolicy, op

from agentic_dagster.resources.execution_settings import ExecutionSettings

# Retries are an execution concern and are declared on the step, never in the
# decision/policy core.
EXECUTE_RETRY_POLICY = RetryPolicy(max_retries=3, delay=0)


@op(
    ins={"prepared": In(dict)},
    out=Out(dict),
    retry_policy=EXECUTE_RETRY_POLICY,
)
def execute_op(context, prepared: dict, settings: ExecutionSettings) -> dict:
    """Execute the bounded capability.

    Deterministic failure injection (for the durable-execution demo):
    - ``fail_until_attempt=N`` fails attempts 1..N, then succeeds (proves retry);
    - ``fail_hard=true`` fails every attempt (proves persisted failure).
    """
    attempt = context.retry_number + 1
    context.log.info(
        "execute: attempt=%d capability=%s fail_until_attempt=%d fail_hard=%s",
        attempt,
        settings.capability,
        settings.fail_until_attempt,
        settings.fail_hard,
    )
    if settings.fail_hard or attempt <= settings.fail_until_attempt:
        raise Failure(
            description=f"intentional execution failure on attempt {attempt}"
        )
    return {**prepared, "execute_attempts": attempt, "executed": True}
