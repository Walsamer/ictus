"""Dagster code location.

Load with:

    uv run dagster dev -m agentic_dagster.definitions
    uv run dagster job execute -m agentic_dagster.definitions -j capability_execution_job
"""

from __future__ import annotations

from dagster import Definitions

from agentic_dagster.jobs.capability_job import capability_execution_job
from agentic_dagster.resources.execution_settings import ExecutionSettings

defs = Definitions(
    jobs=[capability_execution_job],
    resources={"settings": ExecutionSettings()},
)
