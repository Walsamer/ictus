# Versioned contracts

Every externally serialized payload in this directory carries
`schema_version`. `schema_version` is per-contract: the base contracts are at
`1`, the `DecisionProposal` contract is at `2` (see
`docs/adr/0001-generic-decision-vocabulary.md`), and the initial/recovery
`DecisionContext` and validated `DecisionEnvelope` contracts
(`docs/adr/0002-initial-recovery-context-and-decision-envelope.md`) are at `1`.
Breaking changes require a new version; adapters may support multiple versions
during a migration, and new wire versions change only when required.

| Contract | Schema | Rust type | Python |
| --- | --- | --- | --- |
| StateSnapshot | `state-snapshot.schema.json` | `ictus_core::StateSnapshot` | bridge input |
| DecisionContext | `decision-context.schema.json` | `ictus_core::DecisionContext` | — (initial/recovery input) |
| ExecutionObservation | `observation.schema.json` | `ictus_core::ExecutionObservation` | result mapping |
| DecisionProposal | `proposal.schema.json` | `ictus_core::DecisionProposal` | — (v2; v1 input accepted) |
| PolicyDecision | `policy-decision.schema.json` | `ictus_core::PolicyDecision` | — |
| DecisionEnvelope | `decision-envelope.schema.json` | `ictus_core::DecisionEnvelope` | — (validated output) |
| ExecutionIntent | `execution-intent.schema.json` | `ictus_core::ExecutionIntent` | bridge input |
| ExecutionResult | `execution-result.schema.json` | `ictus_core::ExecutionResult` | bridge output |

## Conventions

- Enums that describe *observations* (a category) use `SCREAMING_SNAKE_CASE`.
- Enum `status` on `ExecutionResult` uses lowercase `snake_case` (matching the
  documented example in `docs/CONTRACTS_AND_BOUNDARIES.md`).
- `subject.type` / `target.type` are the serialized names; the Rust field is
  `kind` because `type` is a reserved word.
- Contracts are transport-independent. The current Rust↔Dagster transport is
  JSON over stdin/stdout (see `docs/ARCHITECTURE.md`); replacing it with HTTP or
  gRPC must not change these schemas.

## Compatibility

The Rust integration test `crates/core/tests/contracts_json.rs` deserializes the
exact example payloads documented in `docs/CONTRACTS_AND_BOUNDARIES.md`.
The Python test `tests/python/test_contracts_schemas.py` validates the same
examples against these schemas with `jsonschema`.

### DecisionContext and DecisionEnvelope (v1)

`decision-context.schema.json` and `decision-envelope.schema.json` are the
generic serialization a consumer such as Tactus exchanges. Both start at
`schema_version: 1` and are versioned independently of the base contracts.

- A `DecisionContext` is an **initial** or **recovery** input built from a
  `StateSnapshot`, a subject revision, a semantic attempt budget, facts and
grants. An `INITIAL` context must not carry an observation and must not bind a
recovery; a `RECOVERY` context must bind the failed attempt and its snapshot.
  `semantic_attempts` counts accepted attempts including the failed one,
  `max_semantic_attempts` is the total bound and `step_retries` (execution-
  backend step retries) is separate. Missing or invalid values fail closed.
- A `DecisionEnvelope` binds the subject, snapshot digest/revision, proposal and
  policy identity/version/verdict, capability validation, route, approvals, the
  resolved `outcome` and an expiry. Only `EXECUTABLE` may carry an
  `ExecutionIntent`; `DENIED`, `PENDING`, `NO_ROUTE` and `INVALID` carry none.

The Rust and Python halves agree: `crates/core/tests/contracts_json.rs` parses
the exact example files that `tests/python/test_contracts_schemas.py` validates
the new schemas against.

### DecisionProposal v1 -> v2

The generic semantic vocabulary bumped `proposal.schema.json` to
`schema_version: 2`: `RETRY` became `REEXECUTE`, and `ROUTE` / `DECOMPOSE` were
added. Readers accept `schema_version: 1` proposals and normalize `RETRY` to
`REEXECUTE`; v1 payloads may not use the v2-only `ROUTE` / `DECOMPOSE` tokens.
`examples/decision-proposal.retry.json` is the legacy v1 example.
The other contracts are unchanged at `schema_version: 1`.
