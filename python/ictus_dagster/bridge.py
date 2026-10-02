"""JSON-over-stdio bridge: the Dagster side of the Rust <-> Dagster boundary.

Reads one validated ``ExecutionIntent`` JSON object on stdin, executes it
durably through Dagster, and writes exactly one ``ExecutionResult`` JSON object
on stdout. All logging and diagnostics go to stderr so stdout stays a pure
contract.

Transport choice and its rationale are documented in ``docs/ARCHITECTURE.md``.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from typing import Any

from dagster import DagsterInstance

from ictus_dagster.adapters.contracts import ContractError, validate_intent
from ictus_dagster.adapters.intent_config import UnsupportedCapability
from ictus_dagster.adapters.result_mapping import execution_result_from_run
from ictus_dagster.adapters.workflow_registry import resolve_workflow


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _instance() -> DagsterInstance:
    """Use the durable instance when ``DAGSTER_HOME`` is configured, else an
    ephemeral one. Durable execution is only observable with a real instance."""
    if os.environ.get("DAGSTER_HOME"):
        return DagsterInstance.get()
    return DagsterInstance.ephemeral()


def execute_intent(
    payload: dict[str, Any],
    *,
    instance: DagsterInstance | None = None,
) -> dict[str, Any]:
    """Execute one validated intent and return an ExecutionResult payload."""
    validate_intent(payload)
    # Routing a validated capability to a workflow graph is an execution
    # concern; the Rust policy layer already authorized the capability.
    spec = resolve_workflow(payload["capability"])
    config = spec.settings_from_arguments(payload.get("arguments") or {})
    resource = spec.resource_cls(**config)
    dagster_instance = instance or _instance()

    started_at = _now()
    # Keep stdout a pure contract: Dagster's console logging is redirected to
    # stderr for the duration of the run.
    with redirect_stdout(sys.stderr):
        result = spec.job.execute_in_process(
            resources={spec.resource_key: resource},
            instance=dagster_instance,
            raise_on_error=False,
            tags={
                "ictus.intent_id": payload["intent_id"],
                "ictus.capability": payload["capability"],
                "ictus.schema_version": str(payload["schema_version"]),
            },
        )
    finished_at = _now()

    failed_step_keys = result.get_failed_step_keys() or set()
    failed_step = None
    if failed_step_keys:
        first = sorted(str(key) for key in failed_step_keys)[0]
        failed_step = first

    evidence = [
        {
            "kind": "dagster_run",
            "uri": f"dagster://runs/{result.run_id}",
            "note": "durable execution record (Dagster run id)",
        }
    ]

    return execution_result_from_run(
        execution_id=result.run_id,
        intent_id=payload["intent_id"],
        success=bool(result.success),
        failed_step=failed_step,
        started_at=started_at,
        finished_at=finished_at,
        evidence=evidence,
    )


def main(argv: list[str] | None = None) -> int:  # noqa: ARG001 - CLI signature
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw)
        result = execute_intent(payload)
    except (ContractError, UnsupportedCapability, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - a backend crash must be reported
        print(f"error: backend failure: {exc}", file=sys.stderr)
        return 1

    # A failed execution is a *valid result*, not a transport error: failure is
    # carried by `status` / `observation.category` and the decision layer must
    # be able to observe it. Exit non-zero only when no well-formed result could
    # be produced (contract error -> 2, unexpected backend crash -> 1).
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
