"""Fail-closed placeholder workflow for ``software.implement``.

The capability is routed here so routing is explicit and testable, but this
code location owns no implementation backend.  The single step therefore raises
a typed, explicitly non-retryable failure instead of ever reporting success.
"""

from __future__ import annotations

from dagster import ConfigurableResource, Failure, job, op


class SoftwareAgentSettings(ConfigurableResource):
    """Known resource shape for the fail-closed ``software.implement`` route.

    The shape is deliberately inert: it carries only the routed capability and
    a placeholder marker, so no intent argument can be mistaken for a real
    implementation request.
    """

    capability: str = "software.implement"
    placeholder: bool = True


@op
def software_agent_placeholder_op(
    context,
    software_agent_settings: SoftwareAgentSettings,
) -> None:
    """Report that implementation is not implemented by this backend."""
    context.log.error(
        "software_agent_job: refusing unimplemented capability=%s",
        software_agent_settings.capability,
    )
    raise Failure(
        description=(
            "software_agent_job is a fail-closed placeholder: "
            f"'{software_agent_settings.capability}' has no implementation in "
            "this Ictus code location"
        ),
        metadata={
            "ictus.capability": software_agent_settings.capability,
            "ictus.job": "software_agent_job",
            "ictus.failure_category": "INTEGRATION_CONFLICT",
            "ictus.placeholder": True,
            "ictus.retryable": False,
        },
        allow_retries=False,
    )


@job(
    name="software_agent_job",
    resource_defs={"software_agent_settings": SoftwareAgentSettings()},
    description=(
        "Fail-closed placeholder for software.implement. It never reports "
        "success; a real implementation must be registered by a capable backend."
    ),
)
def software_agent_job() -> None:
    software_agent_placeholder_op()
