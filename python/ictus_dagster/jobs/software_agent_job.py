"""The ``software.implement`` job: ``prepare -> execute -> verify -> finalize``.

This graph is the step skeleton Ictus owns.  Dagster job graphs are static per
code location, so a control plane cannot substitute its own prepare/execute/
verify ops; instead the runtime is named *in configuration* and reached through
the narrow seam port (``ictus_dagster.seam``).  Nothing here imports a control
plane, a domain adapter or a specific runtime, vendor, binary or model.

With no seam configured the route stays fail-closed: ``prepare`` raises the same
typed, explicitly non-retryable ``INTEGRATION_CONFLICT`` failure the placeholder
always raised, so an unconfigured deployment cannot accidentally succeed.
"""

from __future__ import annotations

from dagster import ConfigurableResource, In, MetadataValue, Out, Output, job, op

from ictus_dagster.seam import runtime as seam_runtime

JOB_NAME = "software_agent_job"


class SoftwareAgentSettings(ConfigurableResource):
    """Resource shape for the ``software.implement`` route.

    ``runtime_seam`` is the path to an operator-owned runtime seam
    configuration; an orchestrating deployment sets it per run or in its own
    code location.  It is unset by default and an unset seam keeps the route
    fail-closed.  ``placeholder`` is the legacy route marker: it is accepted so
    existing run configs keep validating and no longer selects behaviour.
    """

    capability: str = "software.implement"
    placeholder: bool = True
    runtime_seam: str | None = None


@op(name="software_agent_prepare_op", out=Out(dict))
def software_agent_prepare_op(
    context,
    software_agent_settings: SoftwareAgentSettings,
) -> dict:
    """Hold the workspace by reference and record the runtime's probe report."""
    return seam_runtime.prepare(
        context,
        job_name=JOB_NAME,
        capability=software_agent_settings.capability,
        seam_config_path=software_agent_settings.runtime_seam,
    )


@op(name="software_agent_execute_op", ins={"prepared": In(dict)}, out=Out(dict))
def software_agent_execute_op(context, prepared: dict) -> dict:
    """Run one bounded attempt through the configured runtime backend."""
    return seam_runtime.execute(context, prepared)


@op(name="software_agent_verify_op", ins={"executed": In(dict)}, out=Out(dict))
def software_agent_verify_op(context, executed: dict) -> dict:
    """Verify the attempt's structured evidence against the operator's bounds."""
    return seam_runtime.verify(context, executed)


@op(name="software_agent_finalize_op", ins={"verified": In(dict)}, out=Out(dict))
def software_agent_finalize_op(context, verified: dict) -> Output[dict]:
    """Emit the execution summary, with the seam evidence in the step record."""
    summary = seam_runtime.finalize(context, verified)
    return Output(summary, metadata={"ictus.seam_evidence": MetadataValue.json(summary)})


@job(
    name=JOB_NAME,
    resource_defs={"software_agent_settings": SoftwareAgentSettings()},
    description=(
        "Durable software implementation: prepare -> execute -> verify -> finalize. "
        "The runtime is named in configuration through the seam port; with no "
        "configuration the job fails closed."
    ),
)
def software_agent_job() -> None:
    prepared = software_agent_prepare_op()
    executed = software_agent_execute_op(prepared)
    verified = software_agent_verify_op(executed)
    software_agent_finalize_op(verified)
