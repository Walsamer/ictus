# Versioned contracts

Every externally serialized payload in this directory carries
`schema_version` (currently `1`). Breaking changes require a new version;
adapters may support multiple versions during a migration.

| Contract | Schema | Rust type | Python |
| --- | --- | --- | --- |
| StateSnapshot | `state-snapshot.schema.json` | `agentic_core::StateSnapshot` | bridge input |
| ExecutionObservation | `observation.schema.json` | `agentic_core::ExecutionObservation` | result mapping |
| DecisionProposal | `proposal.schema.json` | `agentic_core::DecisionProposal` | — |
| PolicyDecision | `policy-decision.schema.json` | `agentic_core::PolicyDecision` | — |
| ExecutionIntent | `execution-intent.schema.json` | `agentic_core::ExecutionIntent` | bridge input |
| ExecutionResult | `execution-result.schema.json` | `agentic_core::ExecutionResult` | bridge output |

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
