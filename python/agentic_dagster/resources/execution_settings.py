"""Execution settings resource.

Holds the per-run configuration derived from an ExecutionIntent. It carries no
retry state: the current attempt is read from Dagster's own
``OpExecutionContext.retry_number``, so retry mechanics stay entirely owned by
the execution backend.
"""

from __future__ import annotations

from dagster import ConfigurableResource


class ExecutionSettings(ConfigurableResource):
    """Per-run settings for the generic capability execution job."""

    capability: str = "demo.verify"
    fail_until_attempt: int = 0
    fail_hard: bool = False
    fail_verify: bool = False
