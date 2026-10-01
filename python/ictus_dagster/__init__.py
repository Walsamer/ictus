"""Dagster OSS execution backend for ictus.

This package owns durable execution: run/step state, retries, re-execution,
persistence and execution observability. It makes no policy decisions.

It must not import, read or mutate any domain system (e.g. a domain system). The only
objects crossing into it are validated `ExecutionIntent` payloads.
"""

__version__ = "0.1.0"
