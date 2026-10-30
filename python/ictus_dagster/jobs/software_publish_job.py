"""Fail-closed placeholder workflow for ``software.promote``.

Like the implementation placeholder, this job makes the route explicit without
pretending that publication happened.  It raises a typed, explicitly
non-retryable failure and therefore never produces a SUCCESS result.
"""

from __future__ import annotations

from dagster import ConfigurableResource, Failure, job, op


class SoftwarePublishSettings(ConfigurableResource):
    """Known resource shape for the fail-closed ``software.promote`` route.

    The shape is deliberately inert: it carries only the routed capability and
    a placeholder marker, so no intent argument can be mistaken for a real
    publication request.
    """

    capability: str = "software.promote"
    placeholder: bool = True


@op
def software_publish_placeholder_op(
    context,
    software_publish_settings: SoftwarePublishSettings,
) -> None:
    """Report that publication is not implemented by this backend."""
    context.log.error(
        "software_publish_job: refusing unimplemented capability=%s",
        software_publish_settings.capability,
    )
    raise Failure(
        description=(
            "software_publish_job is a fail-closed placeholder: "
            f"'{software_publish_settings.capability}' has no implementation in "
            "this Ictus code location"
        ),
        metadata={
            "ictus.capability": software_publish_settings.capability,
            "ictus.job": "software_publish_job",
            "ictus.failure_category": "INTEGRATION_CONFLICT",
            "ictus.placeholder": True,
            "ictus.retryable": False,
        },
        allow_retries=False,
    )


@job(
    name="software_publish_job",
    resource_defs={"software_publish_settings": SoftwarePublishSettings()},
    description=(
        "Fail-closed placeholder for software.promote. It never reports "
        "success; a real publisher must be registered by a capable backend."
    ),
)
def software_publish_job() -> None:
    software_publish_placeholder_op()
