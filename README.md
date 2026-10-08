# Ictus

> **Ictus:** The exact "click" or rebound point in a conductor's gesture where the pulse actually occurs.

Ictus is a typed decision and policy layer for reliable agentic systems, using
Dagster as the durable execution substrate.

Ictus is the point at which observed state becomes an explicit, validated and
executable decision:

```text
State / Events
      ↓
Decision Provider        (rules / specialized model / LLM / human)
      ↓
Typed Proposal
      ↓
Ictus Policy + Capability Validation
      ↓
Execution Intent
      ↓
Dagster                  (durable execution)
      ↓
Runtime
      ↓
Domain Capability
```

The responsibility split is deliberate and load-bearing:

```text
Ictus   = decisions + policy + capability validation
Dagster = durable execution (including execution-level retry)
```

## Why this project exists

Reliability-critical orchestration — durable run/step state, retries,
re-execution, dependency progression, persistence and execution observability —
is expensive to build and maintain correctly. It is also largely generic.

The decision logic above it is different: what bounded action is being
proposed, whether it is valid, whether the requested capability exists and is
permitted, whether human approval is required, and what execution intent should
be emitted. That layer is small, typed and domain-sensitive.

Ictus separates the two:

- **Dagster owns durable execution mechanics.**
- **Ictus owns typed decisions, policy and capability validation.**
- **Domain systems remain authoritative over their own domain state.**

A proposal is not yet an action. Every proposal passes through typed validation,
capability validation, policy validation and (when required) approval before it
becomes an `ExecutionIntent`. Only a validated intent may reach Dagster.

The architecture was developed against a private software-engineering automation
testbed, which is expected to become the first domain adapter. No testbed code,
database or configuration is required to build, test or run Ictus.

## Design principles

1. **Dagster owns durable execution mechanics.**
2. **Ictus owns typed decisions and policy.**
3. **Domain systems remain authoritative over domain-specific state.**
4. **Decision providers never execute actions directly.**
5. **The core must not depend on any particular domain, model provider, runtime or infrastructure.**
6. **All integrations happen through explicit, versioned contracts and adapters.**
7. **Migration is incremental and reversible.**
8. **No second orchestration engine is built in Rust.**

## Ownership boundary

| Concern | Owner |
| --- | --- |
| Typed contracts, decisions, capability metadata, execution intents | `ictus-core` / `ictus-policy` |
| Abstract ports (state, decision, policy, capability, execution, approval) | `ictus-ports` |
| Transport adapter to the execution backend | `ictus-bridge` |
| Capability → workflow routing (which graph executes a validated capability) | `ictus_dagster` (execution-side) |
| Run state, step state, retries, re-execution, dependencies, persistence, event history | Dagster (`ictus_dagster`) |
| Domain semantics (domain objects, domain state transitions) | the domain adapter |

**Rust must never** become a workflow engine, scheduler, retry engine, queue or
durable-state machine. **Dagster must never** decide semantic retry-vs-decompose-vs-escalate,
authorize capabilities, require approvals, or own domain state transitions — it
reports facts.

## Intended users

Initially:

- local Dagster OSS
- software-engineering automation workloads
- private domain adapters

Potentially later:

- data/ML workflows
- operational automation
- agentic recovery and diagnostics
- enterprise state providers and model/tool gateways behind adapters
- container/Kubernetes-backed runtimes

## Repository layout (as implemented)

```text
ictus/
├── Cargo.toml                 # Rust workspace
├── pyproject.toml             # uv-managed Python package (3.12)
├── LICENSE, NOTICE            # Apache-2.0
├── crates/
│   ├── core/                  # typed, versioned domain contracts
│   ├── policy/                # deterministic policy + capability validation
│   ├── ports/                 # abstract ports (StateProvider, DecisionProvider, ...)
│   └── bridge/                # JSON-over-stdio Dagster bridge + `ictus` binary
├── python/ictus_dagster/      # Dagster OSS execution backend (durable execution)
├── contracts/                 # versioned JSON schemas (schema_version = 1)
├── examples/                  # payloads, run configs, demos
├── scripts/                   # verify.sh, demo_durable_execution.sh, e2e_rust_dagster.sh
├── tests/python/              # pytest suite (contracts, Dagster, bridge, E2E)
└── docs/                      # architecture, decisions, implementation status
```

## Getting started

Prerequisites: Rust (stable) and [`uv`](https://docs.astral.sh/uv/) with Python
3.12. Nothing else is required.

```bash
# Rust: format, lint, test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Python / Dagster (enforces >= 99% coverage)
uv sync --frozen
uv run --frozen pytest -q

# Everything at once
./scripts/verify.sh
```

Demonstrations:

```bash
./scripts/quickstart.sh               # one-command tour: build, decide, Rust<->Dagster, second domain
./scripts/demo_durable_execution.sh   # persisted failure, retry, re-execution, run inspection
./scripts/e2e_rust_dagster.sh         # StateSnapshot -> policy -> Dagster -> ExecutionResult
```

## Rust ↔ Dagster boundary

The boundary is **versioned JSON over stdin/stdout**: one process invocation per
`ExecutionIntent`, one JSON `ExecutionResult` back. The contracts are
transport-independent, so HTTP or gRPC can replace the transport later without
changing the domain contracts. Rationale: [`docs/DECISIONS.md`](docs/DECISIONS.md).

```bash
# The Rust side reads any StateSnapshot on stdin:
./target/debug/ictus decide --approve < examples/state-snapshot.worker-timeout.json

# The Python side is a pure stdio adapter:
echo '<ExecutionIntent JSON>' | uv run --frozen python -m ictus_dagster.bridge
```

### Durable submission

The no-argument bridge command remains a synchronous demo.  Production uses a
persistent Dagster instance, an independently started daemon and the
receipt/query boundary.  Copy
[`examples/dagster/dagster.yaml`](examples/dagster/dagster.yaml) into
`$DAGSTER_HOME` (it configures SQLite persistence and a concurrency limit of
two), then start `uv run --frozen dagster-daemon run` in another process.

```bash
echo '<ExecutionIntent JSON>' | uv run --frozen python -m ictus_dagster.bridge submit
echo '<receipt JSON>' | uv run --frozen python -m ictus_dagster.bridge query
```

Repeated submission of the same `intent_id` and immutable content returns its
original receipt.  A changed payload for that identity fails closed; a caller
that missed the first acknowledgement reconciles by submitting or querying the
same identity rather than creating another run.

## Architecture Baseline v1 and status

Read [Ictus scope](docs/architecture/BASELINE_V1.md) and the
[implementation reconciliation](docs/architecture/BASELINE_V1_RECONCILIATION.md).
The generic foundation and three example domains are implemented. Persistent
Dagster demos exist; the complete Tactus context/authorization contract and a
queued, restart-safe execution bridge remain work.

The shared plan is [GitHub Issues](https://github.com/Walsamer/ictus/issues), grouped
by the [M0–M5 roadmap](docs/ROADMAP.md). Humans and Fleet use the same intent:
Issue → human branch OR derived WorkOrder + branch → PR → review → main.
Only explicit fleet:ready issues may be ingested after the documented intake gates.
Old session-plan M-numbers are historical, not current milestone completion.

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
