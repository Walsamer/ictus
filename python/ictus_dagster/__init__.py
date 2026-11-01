"""Dagster OSS execution backend for ictus.

This package owns durable execution: run/step state, retries, re-execution,
persistence and execution observability. It makes no policy decisions.

It must not import, read or mutate any domain system (e.g. a domain system). The only
objects crossing into it are validated `ExecutionIntent` payloads.

The owner-side conversions a control plane needs are exported here:
:func:`result_to_observation` derives the ExecutionObservation facts from a
terminal ``ExecutionResult``, and :func:`validate_observation` enforces the
observation contract on whatever the caller holds. Both are pure Python and
need no Dagster instance.
"""

from ictus_dagster.adapters.contracts import validate_observation
from ictus_dagster.adapters.result_mapping import result_to_observation

__version__ = "0.1.0"

__all__ = ["result_to_observation", "validate_observation", "__version__"]
