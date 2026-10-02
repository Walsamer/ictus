# Implementation status (M0–M3)

Scope of this document: what actually exists in the repository today, mapped to
the roadmap in [`ROADMAP.md`](ROADMAP.md) and
[`MIGRATION_PLAN.md`](MIGRATION_PLAN.md). The supplied architecture documents are
the source of truth for intent; this file is the honest status of the code.

## Milestones

| Milestone | Status | Evidence |
| --- | --- | --- |
| **M0 — repository foundation** | done | Rust workspace, Python package, `contracts/`, CI, `scripts/verify.sh`, docs |
| **M1 — Dagster OSS durable execution demo** | done | `python/ictus_dagster/` (two workflows), `scripts/demo_durable_execution.sh`, `tests/python/test_dagster_durability.py` |
| **M2 — typed Rust core** | done | `crates/core`, `crates/policy`, `crates/ports`; deterministic rules and policy |
| **M3 — generic Rust ↔ Dagster bridge** | done | `crates/bridge`, `python/ictus_dagster/bridge.py`, `scripts/e2e_rust_dagster.sh`, `tests/python/test_rust_bridge_e2e.py` |
| **M4 — domain adapter discovery** | not started (tracking only) | — |
| **M5 — domain verification shadow mode** | not started (tracking only) | — |

## Architecture as implemented

```text
StateSnapshot ──> DecisionProvider ──> DecisionProposal
                                          │
                                          v
                               DefaultPolicyEvaluator
                                          │
                             PolicyDecision (+ ExecutionIntent)
                                          │
                        JsonStdioBackend (crates/bridge)
                                          │  JSON / stdin-stdout
                                          v
                       ictus_dagster.bridge (Dagster OSS)
                                          │
                     resolve_workflow(capability)  [execution-side]
                    ┌─────────────────────┴─────────────────────┐
        capability_execution_job                  data_quality_job
   prepare→execute→verify→finalize          ingest→profile→check→publish
                                          │
                                          v
                                  ExecutionResult
```

Capability → workflow routing is **execution-side** (see
[`DECISIONS.md`](DECISIONS.md) DEC-008): the Rust policy layer has already
authorized the capability, and adding a workflow never adds authorization code.

### Domain workflows shipped

| Capability | Workflow | Domain |
| --- | --- | --- |
| `demo.verify`, `software.verify` | `capability_execution_job` (`prepare→execute→verify→finalize`) | software-engineering |
| `data.quality_check` | `data_quality_job` (`ingest→profile→check→publish`) | data quality |
| `system.diagnose` | `system_diagnostic_job` (`collect→inspect→classify→report`) | system diagnostics |

The second and third workflows are deliberate evidence that the generic core,
contracts and bridge are domain-independent (`examples/data-quality-demo/`,
`examples/system-diagnostic-demo/`, and both legs of
`scripts/e2e_rust_dagster.sh`).

## State ownership

| Concern | Owner | Where |
| --- | --- | --- |
| Typed contracts, decisions, capability metadata, intents | Rust core | `crates/core` |
| Policy, capability validation, approval requirement | Rust policy | `crates/policy` |
| Abstract ports | Rust ports | `crates/ports` |
| Transport/adapter to the execution backend | Rust bridge | `crates/bridge` |
| Run state, step state, retries, re-execution, run persistence, event history | Dagster | `python/ictus_dagster` |
| Domain semantics (domain work items, promotion, domain state) | domain adapter — **not present** | — |

The core owns **no** workflow state. Rust never implements retries, queues,
scheduling, step sequencing or crash recovery.

## Contract versions

All externally serialized payloads are `schema_version = 1`:

| Contract | Schema | Rust type |
| --- | --- | --- |
| StateSnapshot | `contracts/state-snapshot.schema.json` | `ictus_core::StateSnapshot` |
| ExecutionObservation | `contracts/observation.schema.json` | `ictus_core::ExecutionObservation` |
| DecisionProposal | `contracts/proposal.schema.json` | `ictus_core::DecisionProposal` |
| PolicyDecision | `contracts/policy-decision.schema.json` | `ictus_core::PolicyDecision` |
| ExecutionIntent | `contracts/execution-intent.schema.json` | `ictus_core::ExecutionIntent` |
| ExecutionResult | `contracts/execution-result.schema.json` | `ictus_core::ExecutionResult` |

## Boundary and responsibilities

- **Transport:** versioned JSON over stdin/stdout (see [`DECISIONS.md`](DECISIONS.md) DEC-001).
- **Rust owns:** typed decisions, policy, capability validation, approval
  requirements, execution-intent generation, deterministic decision providers.
- **Dagster owns:** durable execution, step state, retries, re-execution,
  dependencies, run persistence, execution event history, observability.
- **Rust must never:** become a workflow engine, scheduler, retry engine, queue
  or durable-state machine.
- **Dagster must never:** decide retry-vs-decompose-vs-escalate, authorize
  capabilities, require approvals or own domain state transitions. It reports
  facts only.

## Deliberately absent (by design)

- No domain-system, agent-harness, specialized-model, LLM, Kubernetes, Dagster+ or enterprise concepts.
- No domain-specific failure taxonomy in the core (adapters map onto the
  generic observation categories).
- No decomposition decision (introduced only after simpler decisions are proven).
- No runtime integration with a domain adapter — that is M4+ and requires governance.

## Known technical debt / open items

- **Capability registry** is in-memory / file-based (a built-in demo set or
  `--capabilities <file>`), not a service.
- **Process supervision** for long-running capabilities is not implemented; the
  demo capabilities are synchronous and fast.
- **Timeouts** are represented by a `timeout_class` string only; no timeout
  enforcement in the demo backend yet.
- **Evidence store** is a file-backed JSONL adapter (`crates/adapters`); it is
  not yet wired into the CLI/bridge by default.
- **Approvals** are file-backed (`JsonFileApprovalProvider`) or `--approve`;
  there is no interactive/live approval service.
- The **Rust↔Dagster E2E test** and full pytest suite take ~1 minute (Dagster
  process startup per run).
- `policy-decision.schema.json` `allOf` guard is validated only where a schema
  validator is used (Python tests); Rust enforces the same rule in
  `PolicyDecision::validate`.
- No mutation testing / property-based testing yet (see `scripts/mutants.sh`).

## Next steps (not started)

1. M4 discovery: document the domain adapter's verification entrypoint, inputs, outputs,
   side effects and failure classes — in the domain adapter, not here.
2. Governance: an ADR and the external-integration checklist before
   any testbed ↔ Ictus runtime integration.
