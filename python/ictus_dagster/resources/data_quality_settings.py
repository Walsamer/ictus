"""Per-run settings for the data-quality workflow."""

from __future__ import annotations

from dagster import ConfigurableResource


class DataQualitySettings(ConfigurableResource):
    """Settings derived from a `data.quality_check` intent."""

    dataset: str = "synthetic.orders"
    rows: int = 100
    min_rows: int = 1
    fail_ingest: bool = False
    fail_until_attempt: int = 0
    fail_on_check: bool = False
