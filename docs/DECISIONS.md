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
