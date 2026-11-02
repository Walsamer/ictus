# The runtime seam

`software.implement` and `software.promote` execute through real Dagster job
graphs in this code location:

```
software_agent_job:    software_agent_prepare_op   -> software_agent_execute_op   -> software_agent_verify_op   -> software_agent_finalize_op
software_publish_job:  software_publish_prepare_op -> software_publish_execute_op -> software_publish_verify_op -> software_publish_finalize_op
```

The graphs are the same shape as `capability_execution_job`. Dagster job graphs
are static per code location, so a control plane cannot substitute its own
prepare/execute/verify steps; instead **the runtime is named in configuration**
and reached through a narrow port. This document is the contract for whoever
supplies that runtime.

Code: [`python/ictus_dagster/seam/`](../python/ictus_dagster/seam/) ·
Schemas: [`contracts/runtime-seam.schema.json`](../contracts/runtime-seam.schema.json),
[`contracts/runtime-evidence.schema.json`](../contracts/runtime-evidence.schema.json)

## Boundaries

- `python/ictus_dagster/` imports no control plane, no domain adapter and no
  specific runtime, vendor, binary or model. The backend reference is imported
  lazily, by name, at run time.
- Ictus owns the steps, the bounds and the termination. The backend owns the
  runtime.
- Ictus never reads the workspace descriptor. It holds the path and passes it
  through; the runtime reports its version and probe result back.
- The seam adds no routing, retry or recovery policy: one intent is still one
  Dagster run, step retries stay inside that attempt, and `capability_validation`,
  decision and result semantics are untouched.

## Selecting a runtime

Two configuration surfaces name the seam, and both hold only a **path**:

| Surface | Who sets it | Meaning |
| --- | --- | --- |
| resource setting `runtime_seam` | the code location's resource default, or `submit_intent`/`bridge.execute_intent` run config derived from the intent argument `runtime_seam` | path to the operator-owned seam configuration |
| intent argument `runtime_seam` | the submitting control plane | same path, stripped; a non-string or blank value is ignored |

Unset, the seam is closed: `prepare` raises the placeholder failure described
below. A path that is set but unusable fails closed too — never open.

### The seam configuration (version 1)

```json
{
  "schema_version": 1,
  "backend": "their_package.runtime:create_backend",
  "workspace": {"descriptor": "config/agent-runtime.json"},
  "bounds": {"write_scope": ["src", "tests"], "wall_clock_seconds": 600, "step_budget": 200}
}
```

- `schema_version` — `1`. Any other value, an unknown field or a missing field is
  rejected: pin it and fail closed on drift.
- `backend` — `module:attribute`. Ictus imports the module lazily, calls the
  attribute with no arguments and requires an object implementing the port
  (`describe_workspace`, `execute`, `verify`). Nothing about the runtime is
  compiled into Ictus.
- `workspace.descriptor` — a path. A relative path resolves against the seam
  configuration file's directory. Ictus never opens it.
- `bounds` — passed to the backend as data and re-checked against the evidence it
  returns: `write_scope` (non-empty, syntactic, default-deny containment),
  `wall_clock_seconds` (`0 < s <= 86400`) and `step_budget` (`0 < n <= 10000`).

### The workspace descriptor (the runtime's own file)

Whatever the runtime needs to find itself: an executable, a version token and a
probe. Ictus does not read, parse or validate it. The runtime's
`describe_workspace` report is recorded back as step evidence
(`executable`, `version_token`, `probe{ok, detail}`) and a probe reporting
`ok: false` fails the attempt closed with `WORKSPACE_PROBE_FAILED`, so a
version/probe result is never trusted silently.

## The port

```python
class RuntimeBackend(Protocol):
    def describe_workspace(self, workspace: WorkspaceRef) -> dict: ...
    def execute(self, request: AttemptRequest, control: ExecutionControl) -> dict: ...
    def verify(self, evidence: dict) -> dict: ...
```

`describe_workspace` must return
`{"executable": str, "version_token": str, "probe": {"ok": bool, "detail": str}}`.
`verify` must return `{"verified": bool, "detail": str}`.

`execute` receives the bounded request (`capability`, `write_scope`,
`wall_clock_seconds`, `step_budget`, `workspace`) and an `ExecutionControl`.
Everything the attempt starts goes through the control:

| Control call | Contract |
| --- | --- |
| `control.spawn(argv, cwd=None, env=None)` | starts the process in its own process group and tracks it; refuses once the attempt was cancelled |
| `control.step()` | counts one semantic step; past the budget it raises `StepBudgetExceeded` (`STEP_BUDGET_EXCEEDED`) |
| `control.wait(process)` | waits within the remaining wall clock; past it the process group is terminated and confirmed, then `WallClockExceeded` (`WALL_CLOCK_EXCEEDED`) is raised |
| `control.cancel(reason)` | terminates every tracked group and returns the termination evidence |
| `control.finish()` | called by Ictus after `execute` returns; a backend that returns with a live process group is a `SUPERVISION_VIOLATION` |

### Cancellation really terminates

`control.cancel` (and every budget overrun, and every backend exception) sends
`SIGTERM` to the whole process group, waits the supervisor's grace, sends
`SIGKILL` to survivors, reaps them and **confirms** the group is gone. The
confirmation is reported as data:

```json
{"requested": true, "reason": "control_plane_request", "signals": ["SIGTERM", "SIGKILL"], "process_groups": [4242], "confirmed": true}
```

An attempt whose evidence reports `termination.requested && !confirmed` fails
with `TERMINATION_UNCONFIRMED`: Ictus will not report a stopped writer it could
not confirm, so a supervisor holding writer exclusion can rely on the flag.
There is no cooperative-flag path.

### The evidence an attempt returns

`execute` returns one document, validated against
[`contracts/runtime-evidence.schema.json`](../contracts/runtime-evidence.schema.json):

```json
{
  "schema_version": 1,
  "outcome": "succeeded",
  "failure_kind": null,
  "changed_paths": ["src/thing.py"],
  "patch_digest": "sha256:…",
  "patch_bytes": 1234,
  "steps_used": 7,
  "exit_status": {"code": 0, "signal": null},
  "termination": {"requested": false, "signals": [], "confirmed": true},
  "duration_seconds": 12.5
}
```

`outcome` is `succeeded`, `failed`, `cancelled` or `timed_out`; `failure_kind` is
`null` exactly when the outcome succeeded, otherwise one of:
`CONFIGURATION`, `WORKSPACE_PROBE_FAILED`, `BACKEND_ERROR`, `SCOPE_VIOLATION`,
`STEP_BUDGET_EXCEEDED`, `WALL_CLOCK_EXCEEDED`, `SUPERVISION_VIOLATION`,
`TERMINATION_UNCONFIRMED`, `PROCESS_EXIT`, `CANCEL_REQUESTED`,
`RUNTIME_VERIFICATION_FAILED`. Unknown extra fields are preserved for the
runtime's own diagnostics.

Ictus re-checks the evidence against the operator's bounds before accepting it:
a changed path outside `write_scope`, `steps_used` above the step budget or
`duration_seconds` above the wall clock fail the attempt closed. On success the
facts are recorded as the finalize step's evidence and restated in the
`ExecutionResult` as `runtime_patch` (`sha256`, `changed_paths`, `patch_bytes`)
and `runtime_exit` (outcome, exit code/signal, termination confirmation, steps,
duration) — not as a log tail.

## Serving these routes from another code location

A control plane that serves `software.implement` / `software.promote` from its
own Dagster code location registers **these jobs** — importing
`ictus_dagster.jobs.software_agent_job` / `software_publish_job` — and
configures the seam through the job's resource. It does not substitute a graph
of its own under the same job name: Dagster pins the recording location's step
keys on the run, so a location whose graph differs cannot execute that run
record at all. That is exactly why the skeleton has to exist here, and why a
configured backend is the only extension point.

## Failure facts a control plane reads

Every failure is a Dagster `Failure` with `allow_retries=False` and typed
metadata: `ictus.capability`, `ictus.job`, `ictus.failure_category`,
`ictus.failure_kind`, `ictus.placeholder`, `ictus.retryable`, plus
`ictus.evidence` (JSON) when the seam has evidence. `ictus.failure_category` maps
each failure kind onto the generic observation vocabulary:

| Kind | Category |
| --- | --- |
| `CONFIGURATION`, `WORKSPACE_PROBE_FAILED` | `INTEGRATION_CONFLICT` |
| `PROCESS_EXIT`, `BACKEND_ERROR`, `SUPERVISION_VIOLATION`, `TERMINATION_UNCONFIRMED` | `PROCESS_CRASH` |
| `SCOPE_VIOLATION`, `RUNTIME_VERIFICATION_FAILED` | `VERIFICATION_FAILURE` |
| `STEP_BUDGET_EXCEEDED` | `RESOURCE_EXHAUSTED` |
| `WALL_CLOCK_EXCEEDED` | `WORKER_TIMEOUT` |
| `CANCEL_REQUESTED` | `UNKNOWN` |

The durable query path reports that typed category instead of guessing one from
the step name.

## The unconfigured default

With no `runtime_seam` set, `prepare` raises exactly what the old placeholder
raised — same description, same metadata, same non-retryable flag:

```
software_agent_job is a fail-closed placeholder: 'software.implement' has no implementation in this Ictus code location
```

with `ictus.failure_category = "INTEGRATION_CONFLICT"`, `ictus.placeholder = true`
and `allow_retries = False`. An unconfigured deployment cannot report success.

## Trying it without a runtime

`tests/python/stub_runtime_backend.py` is a committed stub backend (test support,
not shipped) whose behaviour is chosen by its own descriptor file; it is
`backend: "stub_runtime_backend:create_backend"` in a test seam configuration.
`tests/python/test_software_seam_jobs.py` drives both jobs through it end to end.
