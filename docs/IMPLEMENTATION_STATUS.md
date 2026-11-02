# Implementation status — Baseline v1 inspection

Inspected main: `833175d88adeed59f8e62c14c5e5577580cfdf33`, 2026-10-06.
Historical M0–M3 completion referred to repository foundation, Dagster demos,
typed core and stdio bridge. Those numbers are not the new shared milestones.

The Tactus-facing surface was re-inspected at main `c140af3`, 2026-11-01 and
wired on this side the same day: `ictus decide --context` emits the validated
`DecisionEnvelope` and `ictus_dagster.result_to_observation` derives the
observation. What remains is listed below.

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

The contracts exist, the Rust and schema tests cover them, and this repository
emits both of them now: the envelope from `ictus decide --context`
(`crates/bridge/src/main.rs`) and the observation from
`ictus_dagster.result_to_observation`
(`python/ictus_dagster/adapters/result_mapping.py`). The envelope carries two
versions: `schema_version: 1` is the frozen capability-bearing shape, and
`schema_version: 2` carries `capability_validation` only when the decision
carries a capability, so an `ESCALATE`, `ABORT` or `DECOMPOSE` crosses it with
outcome `NO_EXECUTION` and `decide --context` exits 3 with that envelope instead
of printing nothing (ADR 0003). What remains:

- **The control plane has not switched.** Tactus still admits the legacy
  `{schema_version, proposal, policy_decision, execution_intent}` triple on
  structural and binding checks only
  (`src/tactus/application/execution_cycle.py`, `parse_execution_decision`) and
  still assembles the observation in a harness script
  (`scripts/validate_execution_slice.py`, `_observation_from_result`). Its side
  of the change is tracked in that repository; this one cannot make it.
- **No shared cross-repository fixtures are held here.** The five-scenario slice
  is proven by the Tactus repository (`Walsamer/tactus#12`), not by fixtures in
  this repository that assert the Rust, Python and Tactus contracts against each
  other.
- **The runtime and publication capability backends do not exist here.**
  `software.implement` and `software.promote` now run the real
  `prepare -> execute -> verify -> finalize` skeleton
  (`python/ictus_dagster/jobs/software_agent_job.py`,
  `software_publish_job.py`) and reach a runtime through the configuration-selected
  seam port ([`docs/RUNTIME_SEAM.md`](RUNTIME_SEAM.md)); no runtime is shipped
  here, and with no seam configured both routes keep their typed
  `INTEGRATION_CONFLICT` placeholder failure. Tactus issues #22 and #23 remain
  open for their backends.

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
