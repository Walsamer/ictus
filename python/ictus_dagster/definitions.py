"""Dagster code location.

Load with:

    uv run dagster dev -m ictus_dagster.definitions
    uv run dagster job execute -m ictus_dagster.definitions -j capability_execution_job
"""

from __future__ import annotations

from dagster import Definitions

from ictus_dagster.jobs.capability_job import capability_execution_job
from ictus_dagster.resources.execution_settings import ExecutionSettings

defs = Definitions(
    jobs=[capability_execution_job],
    resources={"settings": ExecutionSettings()},
)
