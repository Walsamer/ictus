"""A dummy, independently registered Tactus-like Dagster code location.

It deliberately does not import ``ictus_dagster``.  Its only job is to prove
that Ictus' submission contract addresses software jobs by *name* and resource
key, so a separately registered code location with matching names can execute
the recorded run.  A location with mismatched names/keys must fail closed.

This is test evidence only: it is not shipped in the Ictus package.
"""

from __future__ import annotations

from dagster import (
    ConfigurableResource,
    Definitions,
    Failure,
    in_process_executor,
    job,
    op,
)


class DummySoftwareSettings(ConfigurableResource):
    capability: str = "software.implement"
    placeholder: bool = True


class MismatchedSoftwareSettings(ConfigurableResource):
    unrelated: str = "mismatch"


@op
def software_agent_placeholder_op(
    context,
    software_agent_settings: DummySoftwareSettings,
) -> None:
    raise Failure(
        description=f"dummy software_agent_job refused {software_agent_settings.capability}",
        metadata={
            "ictus.capability": software_agent_settings.capability,
            "ictus.failure_category": "INTEGRATION_CONFLICT",
            "ictus.placeholder": True,
            "ictus.retryable": False,
        },
        allow_retries=False,
    )


@op
def software_publish_placeholder_op(
    context,
    software_publish_settings: DummySoftwareSettings,
) -> None:
    raise Failure(
        description=f"dummy software_publish_job refused {software_publish_settings.capability}",
        metadata={
            "ictus.capability": software_publish_settings.capability,
            "ictus.failure_category": "INTEGRATION_CONFLICT",
            "ictus.placeholder": True,
            "ictus.retryable": False,
        },
        allow_retries=False,
    )


@op
def mismatched_agent_op(context, wrong_settings: MismatchedSoftwareSettings) -> None:
    raise Failure(description="mismatched placeholder must never run", allow_retries=False)


@job(
    name="software_agent_job",
    resource_defs={"software_agent_settings": DummySoftwareSettings()},
    executor_def=in_process_executor,
)
def dummy_software_agent_job() -> None:
    software_agent_placeholder_op()


@job(
    name="software_publish_job",
    resource_defs={"software_publish_settings": DummySoftwareSettings()},
    executor_def=in_process_executor,
)
def dummy_software_publish_job() -> None:
    software_publish_placeholder_op()


@job(
    name="software_agent_job",
    resource_defs={"wrong_settings": MismatchedSoftwareSettings()},
    executor_def=in_process_executor,
)
def mismatched_software_agent_job() -> None:
    mismatched_agent_op()


@job(
    name="wrong_software_agent_job",
    resource_defs={"software_agent_settings": DummySoftwareSettings()},
    executor_def=in_process_executor,
)
def wrong_name_software_agent_job() -> None:
    software_agent_placeholder_op()


def software_agent_job_factory():
    return dummy_software_agent_job


def software_publish_job_factory():
    return dummy_software_publish_job


def wrong_name_job_factory():
    return wrong_name_software_agent_job


defs = Definitions(
    jobs=[dummy_software_agent_job, dummy_software_publish_job],
    resources={
        "software_agent_settings": DummySoftwareSettings(),
        "software_publish_settings": DummySoftwareSettings(),
    },
)

mismatched_defs = Definitions(
    jobs=[mismatched_software_agent_job, wrong_name_software_agent_job],
    resources={"wrong_settings": MismatchedSoftwareSettings()},
)
