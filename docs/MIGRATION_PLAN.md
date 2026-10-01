# Migration Plan

## Migration philosophy

The migration is responsibility-driven, not rewrite-driven.

For every capability:

1. identify current owner
2. define target owner
3. introduce an adapter
4. run shadow mode where possible
5. compare behavior
6. move authority
7. delete redundant code
8. keep rollback available until stable

No large-bang rewrite.

---

# Milestone 0 — repository foundation

## Goal

Create a clean, publishable repository with no the testbed dependency.

## Deliverables

- repository skeleton
- architecture docs
- typed core models
- initial JSON schemas
- Dagster OSS local environment
- toy durable execution demo
- CI
- formatting/linting/test setup

## Acceptance criteria

- repo runs without the testbed
- Dagster demo can fail and be re-executed
- no domain-specific types appear in the core
- no Kubernetes dependency
- no LLM dependency

---

# Milestone 1 — contracts and typed core

## Goal

Define the smallest stable architecture contracts.

## Core types

Suggested first types:

```text
StateSnapshot
ExecutionObservation
DecisionProposal
PolicyDecision
Capability
ExecutionIntent
ExecutionResult
EvidenceRef
ApprovalRequirement
```

## Decision types

Start small:

```text
Retry
Abort
Escalate
ExecuteCapability
```

Do not introduce decomposition until needed.

## Acceptance criteria

- all contracts versioned
- invalid proposals rejected deterministically
- core can run fully without Dagster
- decision-provider interface is model-agnostic

---

# Milestone 2 — Dagster execution adapter

## Goal

Connect typed ExecutionIntent to Dagster OSS.

## Flow

```text
ExecutionIntent
→ Dagster adapter
→ Dagster run
→ ExecutionResult
```

## Demo capability

Use a generic demo such as:

```text
demo.verify
```

with steps:

```text
prepare
→ check_a
→ check_b
→ finalize
```

## Acceptance criteria

- failed step persisted
- retry/re-execution demonstrated
- execution result mapped back to generic schema
- Dagster-specific objects do not leak into Rust domain types

---

# Milestone 3 — the testbed verification shadow integration

## Goal

Connect the private the testbed repository as the first real adapter.

This work primarily happens in the testbed, not in the public core repository.

## Flow

```text
the testbed run
   ├── native verifier → authoritative
   └── Dagster verifier → shadow
```

## Data captured

- the testbed run ID
- Dagster run ID
- native result
- Dagster result
- failure class
- duration
- mismatch reason
- evidence references

## Acceptance criteria

- no Dagster result can mutate the testbed state
- Dagster unavailable does not break native the testbed verification
- comparison report exists
- rollback requires configuration only

---

# Milestone 4 — Dagster-authoritative verification

## Goal

Move execution authority for verification to Dagster.

the testbed remains authoritative over the testbed state.

## Flow

```text
the testbed requests verification
→ Dagster executes
→ structured result
→ the testbed validates
→ the testbed updates domain state
```

## Acceptance criteria

- native path still available as fallback
- the testbed state transitions remain domain-owned
- no duplicate completion possible
- run/result correlation is explicit

---

# Milestone 5 — Rust decision shadowing

## Goal

Use real the testbed observations to test the new decision layer.

## Example observations

- WORKER_TIMEOUT
- VERIFICATION_FAILURE
- PROVIDER_FAILURE
- PROCESS_CRASH
- RESOURCE_EXHAUSTED

## Flow

```text
Observation
├── the testbed legacy decision
└── Rust decision provider
      ↓
compare
```

Start with deterministic rules only.

## Acceptance criteria

- decisions are reproducible
- mismatches are recorded
- no new decision affects production behavior yet
- policy validation is separate from proposal generation

---

# Milestone 6 — selective Rust decision authority

## Goal

Move one narrow recovery class at a time.

Suggested order:

1. simple retry eligibility
2. abort/stop conditions
3. human escalation
4. provider routing
5. decomposition
6. more complex autonomous recovery

## Acceptance criteria

- each migrated decision class has shadow evidence
- fallback to legacy decision path exists
- every decision is auditable
- decision provider identity is recorded

---

# Milestone 7 — full execution-attempt migration

## Goal

Move a complete selected the testbed execution lifecycle under Dagster.

Possible graph:

```text
prepare_workspace
→ launch_worker
→ collect_result
→ verify
→ integrate
→ produce_handoff
```

## Important rule

Dagster owns execution state.

domain adapter owns domain state.

## Acceptance criteria

- execution can resume/retry at step level
- the testbed does not duplicate step lifecycle state
- generic execution code in the testbed can begin to be deleted

---

# Milestone 8 — scheduler decomposition

## Goal

Separate two concerns that are currently mixed:

### Admission / eligibility

Owned by policy layer.

Examples:

- may this task run?
- budget available?
- project conflict?
- approval required?
- dependency satisfied?

### Execution scheduling

Owned by Dagster.

Examples:

- when to launch
- what step is next
- retry timing
- persisted execution lifecycle

## Acceptance criteria

- no duplicate scheduler authority
- the testbed's current scheduler can be reduced safely
- policy decisions remain explicit

---

# Milestone 9 — generic the testbed execution removal

## Goal

Delete infrastructure code that Dagster now replaces.

Candidate categories:

- generic retry loops
- generic step sequencing
- generic run-step persistence
- some crash-recovery machinery
- some scheduler mechanics
- generic execution event logging

Do not delete domain semantics.

---

# Milestone 10 — Kubernetes runtime

Only after local architecture is stable.

## Goal

Introduce runtime portability.

```text
Dagster
→ Kubernetes
→ sandbox/runtime
→ capability
```

Possible later options:

- normal hardened pods
- gVisor
- Kata
- OpenShell
- agent-sandbox
- other runtime adapters

Runtime isolation is orthogonal to decision policy.

---

# Milestone 11 — enterprise portability validation

## Goal

Prove that the core can map to a non-the testbed environment.

Create a mock enterprise adapter rather than coupling to real enterprise infrastructure.

Demonstrate replaceability of:

- state provider
- policy provider
- model provider
- Dagster backend
- runtime
- identity
- capability registry

The project should remain useful even if enterprise never adopts it.
