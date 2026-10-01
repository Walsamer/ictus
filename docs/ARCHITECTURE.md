# Architecture

## 1. Target architecture

```text
                    ┌────────────────────────────┐
                    │   Event / State Sources    │
                    │                            │
                    │ testbed / data / diagnostics │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Normalized State Snapshot  │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │     Decision Provider      │
                    │                            │
                    │ rules / a specialized model / LLM / human │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │      Typed Proposal        │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │ Rust Decision/Policy Core  │
                    │                            │
                    │ schema validation          │
                    │ capability validation      │
                    │ policy/admission           │
                    │ approval requirements      │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │      Execution Intent      │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │          Dagster           │
                    │                            │
                    │ durable execution          │
                    │ retries                    │
                    │ dependencies               │
                    │ persistence                │
                    │ events / observability     │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │     Runtime / Adapters     │
                    │                            │
                    │ local process initially    │
                    │ Kubernetes later           │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │      Capabilities          │
                    │                            │
                    │ verify / implement / test  │
                    │ diagnose / integrate       │
                    │ promote / data jobs / etc. │
                    └────────────────────────────┘
```

## 2. Core ownership rule

Each responsibility must have exactly one authoritative owner.

### Rust core owns

- typed proposal schema
- proposal validation
- capability admission
- policy validation
- approval requirements
- decision-provider interface
- execution-intent schema
- decision/evidence contracts

### Dagster owns

- execution run state
- step state
- retries
- re-execution
- execution dependencies
- execution persistence
- execution event history
- execution timing
- execution observability
- workflow schedules/sensors where appropriate

### Domain adapter owns

- domain semantics
- domain-specific state
- domain-specific validation
- authoritative domain transitions
- domain-specific capability implementations

### Runtime owns

- process/container/pod execution
- environment setup
- sandboxing/isolation
- runtime resource allocation

## 3. Critical non-overlap rules

### Rule A — Rust must not become a workflow engine

Rust must not own:

- RUNNING/RETRYING/WAITING workflow state
- durable step state
- step retry loops
- workflow persistence
- step sequencing
- queue scheduling
- crash recovery mechanics

Those belong to Dagster.

### Rule B — Dagster must not become the policy engine

Dagster must not decide:

- whether a failure means retry vs decompose vs escalate
- whether a proposal is authorized
- whether a capability is allowed
- whether a human approval is required
- domain-specific eligibility rules

Dagster executes approved intents and reports facts.

### Rule C — Decision providers must never execute directly

A rule engine, LLM, specialized model, or human may propose an action.

They may not:

- call execution backends directly
- bypass policy validation
- mutate domain state directly
- bypass capability validation

## 4. Canonical flow

```text
Observation
   ↓
DecisionProvider
   ↓
DecisionProposal
   ↓
Validation
   ↓
PolicyDecision
   ↓
ExecutionIntent
   ↓
DagsterRun
   ↓
ExecutionResult
   ↓
DomainAdapter
   ↓
DomainStateTransition
```

## 5. Failure/recovery flow

```text
Dagster step fails
      ↓
ExecutionObservation
      ↓
Decision Provider
      ↓
Typed Recovery Proposal
      ↓
Policy Validation
      ↓
Execution Intent
      ↓
Dagster next action
```

Example:

```text
Observation:
WORKER_TIMEOUT

Possible proposals:
- RETRY
- DECOMPOSE
- ROUTE_DIFFERENTLY
- ESCALATE
- ABORT

Dagster itself does not choose among them.
```

## 6. Capability abstraction

The core should never reason about implementation details like:

```text
run the domain verifier entrypoint
```

It should reason about capabilities such as:

```text
software.verify
software.implement
software.integrate
software.promote
system.diagnose
data.materialize
```

Each capability should expose metadata such as:

```text
id
version
input schema
output schema
risk class
side effects
idempotency
required approvals
timeout class
execution backend
```

## 7. Ports

The Rust core should expose abstract ports such as:

```text
StateProvider
DecisionProvider
PolicyEvaluator
CapabilityRegistry
ExecutionBackend
EvidenceStore
ApprovalProvider
```

Adapters implement those ports.

## 8. Example adapters

Personal:

```text
TestbedStateProvider
RuleDecisionProvider
SpecializedModelDecisionProvider
LlmDecisionProvider
HumanDecisionProvider
DagsterOssExecutionBackend
LocalRuntimeAdapter
```

Possible enterprise adapters:

```text
EnterpriseStateProvider
EnterprisePolicyAdapter
ApprovedModelDecisionProvider
DagsterPlusExecutionBackend
KubernetesRuntimeAdapter
GatewayToolAdapter
GatewayModelAdapter
```

## 9. Rust ↔ Python boundary

Keep the boundary simple and versioned.

Preferred options:

- HTTP
- Unix domain socket
- gRPC
- JSON over stdin/stdout

Avoid deep language embedding.

The contract must be understandable independently of either implementation.

## 10. Suggested repository layout

```text
ictus/
├── Cargo.toml
├── crates/
│   ├── core/
│   ├── policy/
│   └── ports/
├── python/
│   └── ictus_dagster/
│       ├── definitions.py
│       ├── jobs/
│       ├── ops/
│       ├── resources/
│       └── adapters/
├── contracts/
├── examples/
├── tests/
└── docs/
```
