# Implementation status — Baseline v1 inspection

Inspected main: `833175d88adeed59f8e62c14c5e5577580cfdf33`, 2026-10-06.
Historical M0–M3 completion referred to repository foundation, Dagster demos,
typed core and stdio bridge. Those numbers are not the new shared milestones.

The Tactus-facing surface was re-inspected at main `c140af3`, 2026-11-01: its
contracts and its durable submission boundary exist and are tested, but they are
unwired at the binary and Python-bridge boundary. See
"Missing for the complete Tactus path".

## Implemented and retained

- Rust generic core, ports, adapters, deterministic rules/policy and JSON schemas.
- Software-engineering, data-quality and system-diagnostic example workflows.
- Python Dagster bridge, capability/job mapping, normalized result observations.
- Persistent DAGSTER_HOME mode and durability/re-execution demo; ephemeral fallback.
- Format/lint/unit/property tests, Python coverage gate, Rust coverage and mutation CI.

## Reconciled Fleet integration candidate

Commit `5ee42ea5602703b8aafd8cab9b88e3feb8347087` added the generic decision
vocabulary, DecisionProposal v2 and RETRY v1 compatibility. It is not an
ancestor of `main` here: it is reachable only from the preserved
`agent/agentic-control/*` and `archive/*` refs. Only its position in history was
inspected, not its contents.

Its vocabulary is in `main` under a different commit, `248ea92` (Fleet FIX-006):
`contracts/proposal.schema.json` accepts `schema_version` 1 and 2 with legacy
`RETRY` normalized to `REEXECUTE`, `crates/core/src/decision.rs` carries the
`RETRY` serde alias on the `Reexecute` variant, and
`tests/python/test_contracts_schemas.py` and
`crates/core/tests/contracts_api.rs` exercise both versions. That `248ea92` is
this candidate re-landed is **not** proven: the trees differ, and `5ee42ea`
holds content `248ea92` does not. Nothing here is left to review; the entry is
closed as history.

## Missing for the complete Tactus path

The contracts are not the gap. `contracts/decision-context.schema.json`,
`contracts/decision-envelope.schema.json`, `examples/decision-envelope.*.json`,
`crates/core/src/envelope.rs` and `crates/policy/src/composition.rs` implement
initial/recovery contexts and validated envelopes bound to subject, revision,
policy and grants, and `crates/core/tests/contracts_json.rs`,
`crates/core/tests/contracts_api.rs` and
`tests/python/test_contracts_schemas.py` cover them. What is missing is the wire
between those contracts and Tactus:

- **No binary emits a `DecisionEnvelope`.** `crates/bridge/src/main.rs` offers
  `decide`, `execute`, `submit` and `flow`; `decide` reads a `StateSnapshot` and
  prints `schema_version`, `proposal`, `policy_decision` and `execution_intent`.
  Tactus therefore admits that legacy triple on structural and binding checks
  only (`src/tactus/application/execution_cycle.py`,
  `parse_execution_decision`) and translates its canonical attempt facts onto
  the legacy `retry.*` keys itself
  (`src/tactus/integrations/ictus/state_snapshot.py`,
  `to_ictus_legacy_boundary_payload`).
- **`ExecutionResult` → `ExecutionObservation` is not exposed in Python.**
  `crates/core/src/execution.rs` (`to_observation`) holds the conversion;
  `python/ictus_dagster/` never calls it, so Tactus builds the v1 observation
  envelope itself in `scripts/validate_execution_slice.py`
  (`_observation_from_result`), documented there as a shim.
- **No shared cross-repository fixtures are held here.** The five-scenario slice
  is proven by the Tactus repository (`Walsamer/tactus#12`), not by fixtures in
  this repository that assert the Rust, Python and Tactus contracts against each
  other.
- **The runtime and publication capability backends do not exist here.**
  `software.implement` and `software.promote` are explicit fail-closed routes
  (`python/ictus_dagster/workflows.toml`,
  `python/ictus_dagster/jobs/software_agent_job.py`); Tactus issues #22 and #23
  remain open.

## Semantic recovery policy

`crates/policy/src/recovery.rs` provides the generic bounded recovery provider.
It turns a `RECOVERY` context and its canonical semantic accounting into a
validated `RecoveryDecision`: success is an explicit `NO_RECOVERY_WORK` (never
`ABORT`), re-executable terminal failures re-execute only within the semantic
budget (`attempts.semantic` / `attempts.max_semantic`, legacy `retry.*` only as
a complete pair), persistent backend failures may `ROUTE` under normalized role
constraints, semantic failures require a typed bounded `DECOMPOSE` plan plus
approval, and ambiguous or exhausted cases escalate (or abort only under the
explicit `abort_on_exhaustion` constraint). It has no loop, scheduler, durable
state mutation or direct runtime call. `docs/DECISIONS.md` DEC-015 records the
boundary; `crates/policy/tests/recovery.rs` holds the decision tables.

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

The synchronous `execute_in_process` bridge remains the demo path; the queued,
independently launched restart-safe path is `python/ictus_dagster/submission.py`.
Rules read the canonical semantic accounting (`attempts.semantic` /
`attempts.max_semantic`) and escalate on missing/ambiguous values instead of
silently exhausting a 0/0 budget. Success is tagged `recovery_outcome =
"NO_RECOVERY_WORK"` so `ABORT`-on-success cannot be wired to domain retirement.
Python/Rust validation parity needs review.

See [reconciliation](architecture/BASELINE_V1_RECONCILIATION.md) for evidence and
[Issues](https://github.com/Walsamer/ictus/issues) for implementation intent.
No current Fleet runtime migration is authorized merely by these status entries.
