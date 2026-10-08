# Implementation status — Baseline v1 inspection

Inspected main: `833175d88adeed59f8e62c14c5e5577580cfdf33`, 2026-10-06.
Historical M0–M3 completion referred to repository foundation, Dagster demos,
typed core and stdio bridge. Those numbers are not the new shared milestones.

## Implemented and retained

- Rust generic core, ports, adapters, deterministic rules/policy and JSON schemas.
- Software-engineering, data-quality and system-diagnostic example workflows.
- Python Dagster bridge, capability/job mapping, normalized result observations.
- Persistent DAGSTER_HOME mode and durability/re-execution demo; ephemeral fallback.
- Format/lint/unit/property tests, Python coverage gate, Rust coverage and mutation CI.

## Integration-only candidate

Commit `5ee42ea5602703b8aafd8cab9b88e3feb8347087` on a local Fleet integration ref
adds generic decision vocabulary, DecisionProposal v2 and RETRY v1 compatibility.
It is not in inspected GitHub main. Preserve and review it through a dedicated
implementation PR; architecture documentation does not promote that commit.

## Missing for the complete Tactus path

- Versioned initial/recovery context profile and aligned semantic-budget facts.
- Validated decision/intent binding to subject, revision, policy and real grants.
- Semantic recovery policy using facts.

## Routing policy

Ictus now owns generic compatibility and deterministic route selection. Versioned
descriptor/health/quota/disablement facts are normalized into candidates; the
selector rejects disabled, incompatible, unavailable, excluded and
policy-forbidden routes, applies explicit stale/unknown handling, and resolves
ties by full route identity. A selected `ExecutionIntent` carries every source
fact identity/revision so Tactus can reject stale admission without reranking.
Runtime adapters must execute that exact route or report a fact; they cannot
silently substitute a provider or model. Compatibility callers should migrate
from `BackendRegistry.compatible()` to `LocalPolicyComposition::validate_with_route_selection`.
- Persistent asynchronous submission, receipt/reconciliation and result delivery.
- Cross-repository behavioral fixtures and the five-scenario vertical slice.

The synchronous `execute_in_process` bridge has useful history/correlation but
does not establish queued, independently launched restart-safe execution.
Rules read retry.attempt/retry.budget while Tactus emits different semantic facts;
missing values must not silently become an exhausted 0/0 budget. ABORT-on-SUCCESS
cannot be wired to domain retirement. Python/Rust validation parity needs review.

See [reconciliation](architecture/BASELINE_V1_RECONCILIATION.md) for evidence and
[Issues](https://github.com/Walsamer/ictus/issues) for implementation intent.
No current Fleet runtime migration is authorized merely by these status entries.
