"""System-diagnostics workflow ops: collect -> inspect -> classify -> report.

A third domain workflow, with a different shape again. It uses the same
contracts, the same Rust policy core and the same bridge, so it further
demonstrates that the core is domain-independent.
"""

from __future__ import annotations

from dagster import Failure, In, Out, RetryPolicy, op

from ictus_dagster.resources.diagnostic_settings import DiagnosticSettings

INSPECT_RETRY_POLICY = RetryPolicy(max_retries=3, delay=0)


@op(out=Out(dict))
def collect_op(context, diag_settings: DiagnosticSettings) -> dict:
    """Collect synthetic, deterministic samples for the target."""
    context.log.info(
        "diagnose collect: target=%s samples=%d", diag_settings.target, diag_settings.samples
    )
    if diag_settings.fail_collect:
        raise Failure(description="intentional collection failure")
    if diag_settings.samples < 1:
        raise Failure(description="samples must be >= 1")
    # Deterministic synthetic signal: an increasing latency sequence.
    samples = [100 + index * 10 for index in range(diag_settings.samples)]
    return {"target": diag_settings.target, "samples": samples, "collected": True}


@op(
    ins={"collected": In(dict)},
    out=Out(dict),
    retry_policy=INSPECT_RETRY_POLICY,
)
def inspect_op(context, collected: dict, diag_settings: DiagnosticSettings) -> dict:
    """Inspect the samples. Fails deterministically on the first N attempts."""
    attempt = context.retry_number + 1
    context.log.info(
        "diagnose inspect: attempt=%d fail_until_attempt=%d",
        attempt,
        diag_settings.fail_until_attempt,
    )
    if attempt <= diag_settings.fail_until_attempt:
        raise Failure(description=f"intentional inspect failure on attempt {attempt}")
    samples = collected["samples"]
    return {
        **collected,
        "inspect_attempts": attempt,
        "sample_count": len(samples),
        "mean_latency": sum(samples) / len(samples),
        "inspected": True,
    }


@op(ins={"inspected": In(dict)}, out=Out(dict))
def classify_op(context, inspected: dict, diag_settings: DiagnosticSettings) -> dict:
    """Classify the inspected signal into a condition."""
    if diag_settings.fail_classify:
        raise Failure(description="intentional classification failure")
    condition = "degraded" if inspected["mean_latency"] >= 120 else "healthy"
    return {**inspected, "condition": condition, "classified": True}


@op(ins={"classified": In(dict)}, out=Out(dict))
def report_op(context, classified: dict, diag_settings: DiagnosticSettings) -> dict:
    """Produce the diagnostic report."""
    report = {
        "target": classified["target"],
        "condition": classified["condition"],
        "sample_count": classified["sample_count"],
        "mean_latency": classified["mean_latency"],
        "inspect_attempts": classified.get("inspect_attempts", 0),
    }
    context.log.info("diagnose report: %s", report)
    return report
