# Versioned contracts

Every externally serialized payload in this directory carries
`schema_version`. The base contracts are at `1`; the `DecisionProposal`
contract is at `2` (see `docs/adr/0001-generic-decision-vocabulary.md`).
`schema_version` is per-contract. Breaking changes require a new version;
adapters may support multiple versions during a migration.

| Contract | Schema | Rust type | Python |
| --- | --- | --- | --- |
| StateSnapshot | `state-snapshot.schema.json` | `ictus_core::StateSnapshot` | bridge input |
| ExecutionObservation | `observation.schema.json` | `ictus_core::ExecutionObservation` | result mapping |
| DecisionProposal | `proposal.schema.json` | `ictus_core::DecisionProposal` | — (v2; v1 input accepted) |
| PolicyDecision | `policy-decision.schema.json` | `ictus_core::PolicyDecision` | — |
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

### DecisionProposal v1 -> v2

The generic semantic vocabulary bumped `proposal.schema.json` to
`schema_version: 2`: `RETRY` became `REEXECUTE`, and `ROUTE` / `DECOMPOSE` were
added. Readers accept `schema_version: 1` proposals and normalize `RETRY` to
`REEXECUTE`; v1 payloads may not use the v2-only `ROUTE` / `DECOMPOSE` tokens.
`examples/decision-proposal.retry.json` is the legacy v1 example.
The other contracts are unchanged at `schema_version: 1`.
