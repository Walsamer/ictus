"""Data-quality workflow ops: ingest -> profile -> check -> publish.

This is a second, deliberately non-software domain. It uses the same contracts,
the same bridge and the same Rust policy core, which demonstrates that the core
is domain-independent. The graph shape differs from the capability workflow, so
the execution backend (not the core) chooses the workflow for a capability.
"""

from __future__ import annotations

from dagster import Failure, In, Out, RetryPolicy, op

from ictus_dagster.resources.data_quality_settings import DataQualitySettings

PROFILE_RETRY_POLICY = RetryPolicy(max_retries=3, delay=0)


@op(out=Out(dict))
def ingest_op(context, dq_settings: DataQualitySettings) -> dict:
    """Load the dataset (synthetic and deterministic)."""
    context.log.info(
        "data-quality ingest: dataset=%s rows=%d", dq_settings.dataset, dq_settings.rows
    )
    if dq_settings.fail_ingest:
        raise Failure(description="intentional ingest failure")
    if dq_settings.rows < 0:
        raise Failure(description="row count must be non-negative")
    return {"dataset": dq_settings.dataset, "rows": dq_settings.rows, "ingested": True}


@op(
    ins={"ingested": In(dict)},
    out=Out(dict),
    retry_policy=PROFILE_RETRY_POLICY,
)
def profile_op(context, ingested: dict, dq_settings: DataQualitySettings) -> dict:
    """Profile the dataset. Fails deterministically on the first N attempts."""
    attempt = context.retry_number + 1
    context.log.info(
        "data-quality profile: attempt=%d fail_until_attempt=%d",
        attempt,
        dq_settings.fail_until_attempt,
    )
    if attempt <= dq_settings.fail_until_attempt:
        raise Failure(description=f"intentional profile failure on attempt {attempt}")
    return {
        **ingested,
        "profile_attempts": attempt,
        "null_ratio": 0.01,
        "profiled": True,
    }


@op(ins={"profiled": In(dict)}, out=Out(dict))
def check_op(context, profiled: dict, dq_settings: DataQualitySettings) -> dict:
    """Run the quality check. A failure here is a data-quality failure fact."""
    if dq_settings.fail_on_check:
        raise Failure(description="intentional data-quality check failure")
    if profiled["rows"] < dq_settings.min_rows:
        raise Failure(
            description=f"rows {profiled['rows']} below min_rows {dq_settings.min_rows}"
        )
    return {**profiled, "passed": True}


@op(ins={"checked": In(dict)}, out=Out(dict))
def publish_op(context, checked: dict, dq_settings: DataQualitySettings) -> dict:
    """Publish the quality summary."""
    summary = {
        "dataset": checked["dataset"],
        "rows": checked["rows"],
        "passed": bool(checked.get("passed")),
        "profile_attempts": checked.get("profile_attempts", 0),
        "null_ratio": checked.get("null_ratio"),
    }
    context.log.info("data-quality publish: %s", summary)
    return summary
