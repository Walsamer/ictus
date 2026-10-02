# Decisions

Durable, cross-cutting decisions made while implementing M0–M3. These are the
project's own record; they do not modify the supplied architecture documents in
`docs/ARCHITECTURE.md` etc., which remain the architectural source of truth.

## DEC-001 — Rust ↔ Dagster transport is versioned JSON over stdin/stdout

- **Decision:** the initial boundary is one process invocation per
  `ExecutionIntent`, JSON on stdin and one JSON `ExecutionResult` on stdout.
- **Why:** no socket/port/auth/lifecycle to manage; the payload is an
  independently testable text contract; stdout stays a pure contract while
  diagnostics go to stderr.
- **Portability:** the contracts (`contracts/*.schema.json`) are
  transport-independent. Moving to HTTP/gRPC later changes only the adapter
  (`ictus-bridge`) and the Python `bridge.py`, never the domain contracts.
- **Enforced by:** `crates/bridge` (`JsonStdioBackend`),
  `python/ictus_dagster/bridge.py`, and `tests/python/test_bridge.py`.

## DEC-002 — Python is pinned to 3.12 and managed by `uv`

- **Decision:** `.python-version = 3.12`, `requires-python = ">=3.12,<3.13"`,
  `uv.lock` committed, and the environment lives in the repo-local `.venv`.
- **Why:** Dagster OSS does not support the system Python 3.14; the environment
  must stay independent of the machine default.
- **Enforced by:** `pyproject.toml`, `uv.lock`, `.python-version`.

## DEC-003 — an extra `crates/bridge` crate

- **Decision:** the supplied layout (`core`, `policy`, `ports`) is kept, plus a
  fourth crate `crates/bridge` holding the Dagster transport adapter and the
  `ictus` binary.
- **Why:** `ports` stays a pure trait crate (no transport, no process spawn) and
  `core`/`policy` stay free of adapters. This preserves the dependency
  direction `core <- policy <- adapters` instead of putting a concrete adapter
  next to the abstract ports.

## DEC-004 — retry attempt comes from Dagster, not application state

- **Decision:** `execute_op` reads `context.retry_number` (0 for the first
  attempt) rather than keeping an attempt counter in the resource.
- **Why:** resource state did not survive retries when the resource was
  configured from run config, and keeping retry state in application code would
  duplicate a concern Dagster owns. `context.retry_number` is authoritative.
- **Enforced by:** `python/ictus_dagster/ops/execute.py`,
  `tests/python/test_dagster_durability.py`.

## DEC-005 — serialization conventions follow the documented examples

- **Decision:** observation categories use `SCREAMING_SNAKE_CASE`; the
  `ExecutionResult.status` enum uses lowercase `snake_case`; `subject.type` /
  `target.type` are the serialized names (Rust field is `kind`).
- **Why:** `docs/CONTRACTS_AND_BOUNDARIES.md` shows exactly these shapes.
- **Enforced by:** `crates/core/tests/contracts_json.rs`,
  `tests/python/test_contracts_schemas.py`.

## DEC-006 — the project is named Ictus and licensed Apache-2.0

- **Decision:** the project/repository name is **Ictus** (`ictus`); Rust crates
  are `ictus-core` / `ictus-policy` / `ictus-ports` / `ictus-bridge`, the Python
  package is `ictus_dagster`, and the CLI/binary is `ictus`. The project is
  licensed under Apache License 2.0 (`LICENSE`, `NOTICE`, SPDX
  `Apache-2.0`).
- **Why:** the previous working name (`agentic-control`) was a
  placeholder; `Ictus` names the exact point at which observed state becomes a
  validated, executable decision. Contract semantics and `schema_version` are
  unchanged: a project rename is not a wire-format change.
- **Explicitly not renamed:** the generic contract types (`StateSnapshot`,
  `ExecutionObservation`, `DecisionProposal`, `PolicyDecision`, `Capability`,
  `ExecutionIntent`, `ExecutionResult`) and `schema_version`.

## DEC-007 — a failed execution is a valid ExecutionResult, not a transport error

- **Decision:** the execution backend must exit `0` whenever it emitted a
  well-formed `ExecutionResult`, **including a failed run**. Failure is carried
  by `status` / `observation.category`. The Rust `JsonStdioBackend` treats a
  non-zero child exit as “no usable result” and fails closed. The `ictus` CLI
  still distinguishes outcomes: `0` success, `3` not executable (policy denied
  or approval required), `4` executed but reported non-success.
- **Why:** the decision layer must be able to *observe* a failure as a fact
  (`PROCESS_CRASH`, `VERIFICATION_FAILURE`, …). If a failed run were a
  transport error, the Rust boundary would discard the result and no recovery
  decision could ever be made.
- **Found by:** end-to-end testing during the publication milestone; regression
  covered by `tests/python/test_rust_bridge_e2e.py`
  (`test_failed_execution_is_reported_through_the_boundary`) and
  `tests/python/test_bridge.py`.

## DEC-008 — capability → workflow routing lives in the execution backend

- **Decision:** the Dagster adapter maps a *validated* capability to a workflow
  graph (`ictus_dagster.adapters.workflow_registry`). Routing is data, not
  policy: the Rust policy layer has already authorized the capability, so adding
  a workflow never adds authorization logic, and the Rust boundary stays
  workflow-agnostic.
- **Why:** different capabilities legitimately need different graph shapes
  (`prepare→execute→verify→finalize` vs `ingest→profile→check→publish`).
- **Enforced by:** `tests/python/test_workflow_registry.py`; an unknown
  capability fails closed with `UnsupportedCapability`.

## DEC-009 — a second, non-software domain proves genericity

- **Decision:** ship a second domain workflow (data quality) end to end.
- **Why:** the core claims to be domain-independent; the strongest evidence is a
  non-software domain running through the same versioned contracts, the same Rust
  policy core and the same bridge, with no core change.
- **Evidence:** `python/ictus_dagster/jobs/data_quality_job.py`,
  `examples/state-snapshot.data-quality.json`,
  `examples/data-quality-demo/`, and the data-quality leg of
  `scripts/e2e_rust_dagster.sh`.

## DEC-010 — a third domain workflow (system diagnostics)

- **Decision:** add `system_diagnose` (`collect→inspect→classify→report`) so
  capability routing is genuinely multi-way, not a single special case.
- **Why:** two workflows can be dismissed as a one-off; three distinct shapes
  (software capability, data quality, system diagnostics) make the routing
  registry and the domain-independence claim structural.
- **Evidence:** `python/ictus_dagster/jobs/system_diagnostic_job.py`,
  `tests/python/test_system_diagnostic_job.py`,
  `tests/python/test_workflow_registry.py`, `examples/system-diagnostic-demo/`,
  and the system-diagnostics leg of `scripts/e2e_rust_dagster.sh`.

## DEC-011 — the approval requirement comes from the capability, not the proposal

- **Decision:** `ApprovalProvider::is_approved(&[String])` takes the required
  token set; the evaluator derives it from the capability (`capability.required_approvals`,
  or an implicit `human` gate for a high-risk capability with none).
- **Why (correctness fix):** the previous signature read `required_approvals`
  from the *proposal*, which never carries it, so an empty token provider
  silently approved a gated capability. Regression tests added in
  `crates/policy/src/approval.rs` and `crates/policy/src/evaluator.rs`.
- **Also adds:** persistent approvals — `JsonFileApprovalProvider`
  (`crates/adapters`) and `ictus decide/flow --approvals <file>` — so approvals
  can be granted out-of-band instead of via `--approve`.

## DEC-012 — workflows are routed by data, and evidence can be persisted

- **Decision:** capability → workflow routing lives in
  `python/ictus_dagster/workflows.toml`. Names are resolved against code-side
  tables (`JOBS`, `RESOURCES`, `SETTINGS_MAPPERS`) and any unknown or
  inconsistent entry fails closed. The registry is the source of truth for
  `SUPPORTED_CAPABILITIES`.
- **Why:** adding a capability that reuses an existing workflow becomes a
  data-only change, and the routing table is inspectable and testable.
- **Decision:** `JsonlEvidenceStore` (`crates/adapters`) appends `EvidenceRef`s
  as JSONL, giving the `EvidenceStore` port a real, non-Dagster implementation.

## DEC-013 — the M3→M4 gate is opened for design only; M5 stays gated

- **Decision (operator):** the hard M3→M4 migration gate is opened for **M4
discovery/design only**. Fleet Work Order
`FLEET-V3-WO-193-m4-fleet-adapter-discovery` (project `fleet`) carries the
deliverable: a draft ADR for the Fleet ↔ Ictus boundary, the
external-integration twelve-question checklist, the adapter interface, and the
explicit Fleet-state-ownership list.
- **Still gated:** M5 (verification shadow mode) is the first step that may touch
Fleet runtime. It requires the M4 ADR to be **accepted** by the operator and an
explicit sign-off. Opening M4 does **not** open M5.
- **Boundary unchanged:** the core owns no workflow state, Dagster owns durable
execution, and Fleet owns Fleet state. M4 adds **no** runtime coupling.

## Open decisions (not yet made)

- **License holder scope.** Apache-2.0 is adopted with the copyright holder
  `Samuel Eder` (an individual, not a company). Revisit if ownership changes.
- **Capability registry persistence.** Capabilities are currently supplied
  programmatically / from a JSON file (`--capabilities`), not served by a
  registry service.
- **Domain-adapter integration boundary.** Deliberately out of scope for the
  current milestone; see `docs/IMPLEMENTATION_STATUS.md`.
- **Crates.io publication.** Crates publish `license`/`repository` metadata but
  have never been published; inter-crate dependencies would need versions and a
  registry strategy first.
