# Agentic Control

A small, typed decision-and-policy layer designed to sit above a durable execution engine such as Dagster.

The project exists to separate **agentic decision-making** from **reliable execution**.

The intended split is:

```text
Events / System State
        ↓
Normalized State Snapshot
        ↓
Decision Provider
(rules / specialized model / LLM / human)
        ↓
Typed Proposal
        ↓
Policy + Capability Validation
        ↓
Execution Intent
        ↓
Dagster
        ↓
Runtime
(local processes first, Kubernetes later)
        ↓
Domain workloads
(the testbed first, others later)
```

## Why this project exists

The current personal the testbed system combines several concerns:

- work-order semantics
- planning and decomposition
- worker/provider routing
- retries and recovery
- execution state
- scheduling
- subprocess lifecycle
- verification
- integration
- promotion
- observability
- human escalation

That has been useful for rapid iteration, but reliability-critical orchestration mechanics are increasingly expensive to maintain as custom code.

The goal of this project is to **gradually move generic execution mechanics to Dagster** while keeping a much smaller custom layer responsible for:

- typed decisions
- admission policy
- capability validation
- domain-independent recovery proposals
- model-agnostic decision providers

the testbed remains operational throughout the migration and acts as the first real-world adapter and testbed.

## Design principles

1. **Dagster owns durable execution mechanics.**
2. **The control layer owns typed decisions and policy.**
3. **Domain systems remain authoritative over domain-specific state.**
4. **Decision providers never execute actions directly.**
5. **The core must not depend on the testbed, a specialized model, a specific LLM, Kubernetes, or enterprise infrastructure.**
6. **All integrations happen through explicit, versioned contracts and adapters.**
7. **Migration is incremental and reversible.**
8. **No second orchestration engine is built in Rust.**

## Intended users

Initially:

- the private personal the testbed environment
- local Dagster OSS
- software-engineering workloads

Potentially later:

- enterprise Dagster environments
- data/ML workflows
- operational automation
- agentic recovery and diagnostics
- Kubernetes-backed execution

## Repository role

This repository should contain only the reusable layer:

- typed domain contracts
- policy validation
- capability model
- decision-provider interfaces
- Dagster adapter/integration
- examples
- reference documentation

domain-specific code should remain in the private the testbed repository behind an adapter.

## Suggested high-level repository layout

```text
agentic-control/
├── README.md
├── docs/
│   ├── PROJECT_DESCRIPTION.md
│   ├── ARCHITECTURE.md
│   ├── STATES.md
│   ├── MIGRATION_PLAN.md
│   ├── CONTRACTS_AND_BOUNDARIES.md
│   └── ROADMAP.md
├── crates/
│   ├── core/
│   ├── policy/
│   └── ports/
├── python/
│   └── agentic_dagster/
├── contracts/
│   ├── observation.schema.json
│   ├── proposal.schema.json
│   ├── execution-intent.schema.json
│   └── execution-result.schema.json
├── examples/
│   ├── local-recovery-demo/
│   └── dagster-verification-demo/
└── tests/
```

## First milestone

The first milestone is deliberately narrow:

> Prove that a generic typed decision can be validated, translated into an execution intent, executed durably through Dagster OSS, and reported back as a structured result — without domain-specific knowledge in the core.

The first real the testbed migration target should be **verification in shadow mode**.

---

## Repository layout (as implemented)

```text
agentic-control/
├── Cargo.toml                 # Rust workspace
├── pyproject.toml             # uv-managed Python package (3.12)
├── crates/
│   ├── core/                  # typed, versioned domain contracts
│   ├── policy/                # deterministic policy + capability validation
│   ├── ports/                 # abstract ports (StateProvider, DecisionProvider, ...)
│   └── bridge/                # JSON-over-stdio Dagster bridge + `ac-bridge` binary
├── python/agentic_dagster/    # Dagster OSS execution backend (durable execution)
├── contracts/                 # versioned JSON schemas (schema_version = 1)
├── examples/                  # payloads, run configs, demos
├── scripts/                   # verify.sh, demo_durable_execution.sh, e2e_rust_dagster.sh
├── tests/python/              # pytest suite (contracts, Dagster, bridge, E2E)
└── docs/                      # architecture (source of truth) + status/decisions
```

## Getting started

Prerequisites: Rust (stable) and [`uv`](https://docs.astral.sh/uv/) with Python
3.12. Nothing else, and no the testbed checkout is required.

```bash
# Rust: format, lint, test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Python / Dagster
uv sync --frozen
uv run --frozen pytest -q

# Everything at once
./scripts/verify.sh
```

Demonstrations:

```bash
./scripts/demo_durable_execution.sh   # M1: persisted failure, retry, re-execution, inspection
./scripts/e2e_rust_dagster.sh         # M3: StateSnapshot -> Rust policy -> Dagster -> ExecutionResult
```

## Rust ↔ Dagster boundary

The boundary is **versioned JSON over stdin/stdout**: one process invocation per
`ExecutionIntent`, one JSON `ExecutionResult` back. The contracts are
transport-independent, so HTTP or gRPC can replace the transport later without
changing the domain contracts. Rationale: [`docs/DECISIONS.md`](docs/DECISIONS.md).

```bash
# The Rust side reads any StateSnapshot on stdin:
./target/debug/ac-bridge decide --approve < examples/state-snapshot.worker-timeout.json

# The Python side is a pure stdio adapter:
echo '<ExecutionIntent JSON>' | uv run --frozen python -m agentic_dagster.bridge
```

## Status

M0–M3 are implemented; M4 (the testbed adapter discovery) and M5 (verification shadow
mode) are deliberately **not** started. See
[`docs/IMPLEMENTATION_STATUS.md`](docs/IMPLEMENTATION_STATUS.md) for what exists,
state ownership and open items.

This repository builds, tests and runs **without** `the control repository`; the testbed
integration is intentionally deferred to a later, separately-governed phase.
