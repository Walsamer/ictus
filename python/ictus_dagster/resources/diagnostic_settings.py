"""Per-run settings for the system-diagnostics workflow."""

from __future__ import annotations

from dagster import ConfigurableResource


class DiagnosticSettings(ConfigurableResource):
    """Settings derived from a `system.diagnose` intent."""

    target: str = "local"
    samples: int = 3
    fail_collect: bool = False
    fail_until_attempt: int = 0
    fail_classify: bool = False
