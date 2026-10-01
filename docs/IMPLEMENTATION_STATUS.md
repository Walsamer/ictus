# Implementation status (M0–M3)

Scope of this document: what actually exists in the repository today, mapped to
the roadmap in [`ROADMAP.md`](ROADMAP.md) and
[`MIGRATION_PLAN.md`](MIGRATION_PLAN.md). The supplied architecture documents are
the source of truth for intent; this file is the honest status of the code.

## Milestones

| Milestone | Status | Evidence |
| --- | --- | --- |
| **M0 — repository foundation** | done | Rust workspace, Python package, `contracts/`, CI, `scripts/verify.sh`, docs |
| **M1 — Dagster OSS durable execution demo** | done | `python/agentic_dagster/`, `scripts/demo_durable_execution.sh`, `tests/python/test_dagster_durability.py` |
| **M2 — typed Rust core** | done | `crates/core`, `crates/policy`, `crates/ports`; deterministic rules and policy |
| **M3 — generic Rust ↔ Dagster bridge** | done | `crates/bridge`, `python/agentic_dagster/bridge.py`, `scripts/e2e_rust_dagster.sh`, `tests/python/test_rust_bridge_e2e.py` |
| **M4 — the testbed adapter discovery** | not started (tracking only) | — |
| **M5 — the testbed verification shadow mode** | not started (tracking only) | — |

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
                       agentic_dagster.bridge (Dagster OSS)
                                          │
                             capability_execution_job
                             prepare -> execute -> verify -> finalize
                                          │
                                          v
                                  ExecutionResult
```

## State ownership

| Concern | Owner | Where |
| --- | --- | --- |
| Typed contracts, decisions, capability metadata, intents | Rust core | `crates/core` |
| Policy, capability validation, approval requirement | Rust policy | `crates/policy` |
| Abstract ports | Rust ports | `crates/ports` |
| Transport/adapter to the execution backend | Rust bridge | `crates/bridge` |
| Run state, step state, retries, re-execution, run persistence, event history | Dagster | `python/agentic_dagster` |
| Domain semantics (Work Orders, promotion, the testbed state) | domain adapter — **not present** | — |

The core owns **no** workflow state. Rust never implements retries, queues,
scheduling, step sequencing or crash recovery.

## Contract versions

All externally serialized payloads are `schema_version = 1`:

| Contract | Schema | Rust type |
| --- | --- | --- |
| StateSnapshot | `contracts/state-snapshot.schema.json` | `agentic_core::StateSnapshot` |
| ExecutionObservation | `contracts/observation.schema.json` | `agentic_core::ExecutionObservation` |
| DecisionProposal | `contracts/proposal.schema.json` | `agentic_core::DecisionProposal` |
| PolicyDecision | `contracts/policy-decision.schema.json` | `agentic_core::PolicyDecision` |
| ExecutionIntent | `contracts/execution-intent.schema.json` | `agentic_core::ExecutionIntent` |
| ExecutionResult | `contracts/execution-result.schema.json` | `agentic_core::ExecutionResult` |

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

- No the testbed, agent harness, agent harness, a specialized model, LLM, Kubernetes, Dagster+ or enterprise concepts.
- No domain-specific failure taxonomy in the core (adapters map onto the
  generic observation categories).
- No decomposition decision (introduced only after simpler decisions are proven).
- No runtime integration with the testbed — that is M4+ and requires governance.

## Known technical debt / open items

- **License** not chosen; Cargo workspace is `publish = false`.
- **Capability registry** is in-memory / file-based, not a service.
- **Process supervision** for long-running capabilities is not implemented; the
  demo capabilities are synchronous and fast.
- **Timeouts** are represented by a `timeout_class` string only; no timeout
  enforcement in the demo backend yet.
- **Approval provider** is a boolean port with an in-memory implementation;
  there is no persistent approval store.
- The **Rust↔Dagster E2E test** and full pytest suite take ~1 minute (Dagster
  process startup per run).
- `policy-decision.schema.json` `allOf` guard is validated only where a schema
  validator is used (Python tests); Rust enforces the same rule in
  `PolicyDecision::validate`.

## Next steps (not started)

1. M4 discovery: document the testbed's verification entrypoint, inputs, outputs,
   side effects and failure classes — in the testbed, not here.
2. Governance: an ADR and the external-integration checklist in the testbed before
   any the testbed ↔ agentic-control runtime integration.
