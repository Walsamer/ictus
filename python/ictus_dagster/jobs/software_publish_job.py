"""The ``software.promote`` job: ``prepare -> execute -> verify -> finalize``.

Publication gets the same skeleton and the same seam as ``software.implement``:
the publisher is a runtime the configuration names, reached only through the
narrow port, and Ictus keeps the steps so a control plane does not have to
substitute its own graph.

With no seam configured the route stays fail-closed: ``prepare`` raises the same
typed, explicitly non-retryable ``INTEGRATION_CONFLICT`` failure the placeholder
always raised, so an unconfigured deployment cannot accidentally publish.
"""

from __future__ import annotations

from dagster import ConfigurableResource, In, MetadataValue, Out, Output, job, op

from ictus_dagster.seam import runtime as seam_runtime

JOB_NAME = "software_publish_job"


class SoftwarePublishSettings(ConfigurableResource):
    """Resource shape for the ``software.promote`` route.

    ``runtime_seam`` is the path to an operator-owned runtime seam
    configuration.  It is **deployment configuration**: the code location that
    serves this job sets it (its resource default, or the run config that
    deployment writes), and no intent argument can reach it.  Unset by default,
    which keeps the route fail-closed.  ``placeholder`` is the legacy route
    marker: it is accepted so existing run configs keep validating and no
    longer selects behaviour.
    """

    capability: str = "software.promote"
    placeholder: bool = True
    runtime_seam: str | None = None


@op(name="software_publish_prepare_op", out=Out(dict))
def software_publish_prepare_op(
    context,
    software_publish_settings: SoftwarePublishSettings,
) -> dict:
    """Hold the workspace by reference and record the runtime's probe report."""
    return seam_runtime.prepare(
        context,
        job_name=JOB_NAME,
        capability=software_publish_settings.capability,
        seam_config_path=software_publish_settings.runtime_seam,
    )


@op(name="software_publish_execute_op", ins={"prepared": In(dict)}, out=Out(dict))
def software_publish_execute_op(context, prepared: dict) -> dict:
    """Run one bounded publication attempt through the runtime backend."""
    return seam_runtime.execute(context, prepared)


@op(name="software_publish_verify_op", ins={"executed": In(dict)}, out=Out(dict))
def software_publish_verify_op(context, executed: dict) -> dict:
    """Verify the attempt's structured evidence against the operator's bounds."""
    return seam_runtime.verify(context, executed)


@op(name="software_publish_finalize_op", ins={"verified": In(dict)}, out=Out(dict))
def software_publish_finalize_op(context, verified: dict) -> Output[dict]:
    """Emit the publication summary, with the seam evidence in the step record."""
    summary = seam_runtime.finalize(context, verified)
    return Output(summary, metadata={"ictus.seam_evidence": MetadataValue.json(summary)})


@job(
    name=JOB_NAME,
    resource_defs={"software_publish_settings": SoftwarePublishSettings()},
    description=(
        "Durable software publication: prepare -> execute -> verify -> finalize. "
        "The publisher is named in configuration through the seam port; with no "
        "configuration the job fails closed."
    ),
)
def software_publish_job() -> None:
    prepared = software_publish_prepare_op()
    executed = software_publish_execute_op(prepared)
    verified = software_publish_verify_op(executed)
    software_publish_finalize_op(verified)
