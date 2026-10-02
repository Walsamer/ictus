"""Dagster code location.

Load with:

    uv run dagster dev -m ictus_dagster.definitions
    uv run dagster job execute -m ictus_dagster.definitions -j capability_execution_job
    uv run dagster job execute -m ictus_dagster.definitions -j data_quality_job
    uv run dagster job execute -m ictus_dagster.definitions -j system_diagnostic_job
"""

from __future__ import annotations

from dagster import Definitions

from ictus_dagster.jobs.capability_job import capability_execution_job
from ictus_dagster.jobs.data_quality_job import data_quality_job
from ictus_dagster.jobs.system_diagnostic_job import system_diagnostic_job
from ictus_dagster.resources.data_quality_settings import DataQualitySettings
from ictus_dagster.resources.diagnostic_settings import DiagnosticSettings
from ictus_dagster.resources.execution_settings import ExecutionSettings

defs = Definitions(
    jobs=[capability_execution_job, data_quality_job, system_diagnostic_job],
    resources={
        "settings": ExecutionSettings(),
        "dq_settings": DataQualitySettings(),
        "diag_settings": DiagnosticSettings(),
    },
)
