# Versioned contracts

Every externally serialized payload in this directory carries
`schema_version`. `schema_version` is per-contract: the base contracts are at
`1`, the `DecisionProposal` contract is at `2` (see
`docs/adr/0001-generic-decision-vocabulary.md`), the initial/recovery
`DecisionContext` contract is at `1`
(`docs/adr/0002-initial-recovery-context-and-decision-envelope.md`), and the
validated `DecisionEnvelope` contract is at `2` with the frozen `1` still
accepted and still emitted for a capability-bearing decision
(`docs/adr/0003-decision-envelope-v2-conditional-capability.md`).
Breaking changes require a new version; adapters may support multiple versions
during a migration, and new wire versions change only when required.

| Contract | Schema | Rust type | Python |
| --- | --- | --- | --- |
| StateSnapshot | `state-snapshot.schema.json` | `ictus_core::StateSnapshot` | bridge input |
| DecisionContext | `decision-context.schema.json` | `ictus_core::DecisionContext` | — (initial/recovery input) |
| ExecutionObservation | `observation.schema.json` | `ictus_core::ExecutionObservation` | `result_mapping.result_to_observation` |
| DecisionProposal | `proposal.schema.json` | `ictus_core::DecisionProposal` | — (v2; v1 input accepted) |
| PolicyDecision | `policy-decision.schema.json` | `ictus_core::PolicyDecision` | — |
| DecisionEnvelope | `decision-envelope.schema.json` | `ictus_core::DecisionEnvelope` | — (validated output of `decide --context`; v2, with frozen v1 still emitted for a capability-bearing decision) |
| ExecutionIntent | `execution-intent.schema.json` | `ictus_core::ExecutionIntent` | bridge input |
| ExecutionReceipt | `execution-receipt.schema.json` | `ictus_ports::ExecutionReceipt` | durable submit acknowledgement |
| ExecutionStatus | `execution-status.schema.json` | — (read model) | durable query/cancel reconciliation |
| ExecutionResult | `execution-result.schema.json` | `ictus_core::ExecutionResult` | bridge output |
| RuntimeSeamConfig | `runtime-seam.schema.json` | — (execution backend only) | `ictus_dagster.seam.config.load_seam_config` |
| RuntimeEvidence | `runtime-evidence.schema.json` | — (execution backend only) | `ictus_dagster.seam.evidence.validate_evidence` |
| IdentityDigestVectors | `digest-vectors.schema.json` + `digest-vectors.json` | `ictus_core::digest` | `ictus_dagster.submission.intent_digest` |

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

### DecisionContext and DecisionEnvelope (v1 and v2)

`decision-context.schema.json` and `decision-envelope.schema.json` are the
generic serialization a consumer such as Tactus exchanges. Both are versioned
independently of the base contracts: the context is at `schema_version: 1`, and
the envelope is at `2` with its frozen `1` still accepted and still emitted.

- A `DecisionContext` is an **initial** or **recovery** input built from a
  `StateSnapshot`, a subject revision, a semantic attempt budget, facts and
grants. An `INITIAL` context must not carry an observation and must not bind a
recovery; a `RECOVERY` context must bind the failed attempt and its snapshot.
  `semantic_attempts` counts accepted attempts including the failed one,
  `max_semantic_attempts` is the total bound and `step_retries` (execution-
  backend step retries) is separate. Missing or invalid values fail closed.
- A `DecisionEnvelope` binds the subject, snapshot digest/revision, proposal and
  policy identity/version/verdict, capability validation, route, approvals, the
  resolved `outcome` and an expiry. `schema_version: 1` is the frozen
  capability-bearing shape and always carries `capability_validation`;
  `schema_version: 2` carries it exactly when the decision carries a capability,
  so `EXECUTE_CAPABILITY`, `REEXECUTE` and `ROUTE` carry one and `ESCALATE`,
  `ABORT` and `DECOMPOSE` carry none. A decision that carries none resolves to
  `NO_EXECUTION` and binds no approvals, grants, route or intent; a capability
  claim on it is a contract error, not a tolerated extra field. Only
  `EXECUTABLE` may carry an `ExecutionIntent`; `DENIED`, `PENDING`, `NO_ROUTE`,
  `INVALID` and `NO_EXECUTION` carry none. Unknown versions fail closed.

The Rust and Python halves agree: `crates/core/tests/contracts_json.rs` parses
the exact example files that `tests/python/test_contracts_schemas.py` validates
the new schemas against.

### IdentityDigestVectors (v1)

`digest-vectors.json` is not a payload exchanged at runtime: it is the **pinned
identity-digest protocol** that Ictus produces and a consumer such as Tactus
implements independently. `docs/IDENTITY_DIGEST.md` defines the canonical form,
the digested field set of each subject (snapshot content, proposal, intent
payload), the number domain and the versioning policy; this file is the
artefact a consumer pins, so a change to the algorithm, the field set or the
vectors needs a new `schema_version`.

The file is asserted **whole**, not case by case: the digested `cases`, the
`accepts` documents an ingress must let through, and the `rejections` it must
refuse — each with the `reason` it must report and the `path` it must name —
because an implementation that computes the digests and still digests what the
protocol refuses fails exactly where a proposal binding is verified.

Three digests cross the boundary and all three use that one algorithm:
`snapshot.digest` (StateSnapshot content), `validation.proposal_digest`
(repeated in `intent.policy_context.proposal_digest`) and the submission
identity digest `ictus.intent_digest`. Ictus asserts the vectors from Rust
(`crates/core/tests/digest_vectors.rs`) and Python
(`tests/python/test_digest_vectors.py`) so the file and the implementations
cannot drift apart.

### ExecutionReceipt vs ExecutionStatus

`ExecutionReceipt` is the durable acknowledgement that one immutable intent was
recorded exactly once: `submit` returns it flat, and the Rust
`ictus_ports::ExecutionReceipt` deserializes that same shape. `ExecutionStatus`
is the *query/cancel read model*: it wraps a receipt and adds the current
`state` (`pending` or `terminal`), the correlated `ExecutionResult` once
terminal, and (for cancel) the cancellation outcome. The two are deliberately
different contracts so a strict `ExecutionReceipt` reader never has to tolerate
reconciliation-only fields.

The Rust port models this read model as
`ictus_ports::ExecutionQuery::{Pending, Terminal(ExecutionResult)}`, which maps
directly onto `state` + `result`; `DurableExecutionBackend::cancel` returns `()`
and therefore ignores the optional `cancellation` detail. No adapter implements
that port over the JSON transport yet, so adding `ExecutionStatus` breaks no
consumer.

### RuntimeSeamConfig and RuntimeEvidence (v1)

The two runtime-seam contracts are execution-side and versioned independently at
`schema_version: 1`. They are how a runtime is bound to the software execution
jobs without Ictus importing it:

- `RuntimeSeamConfig` is an **operator-owned** document. Ictus receives its
  *path* (a per-run resource setting or an intent argument) and validates it
  against this schema, failing closed on an unknown `schema_version`, an unknown
  field or an out-of-bounds budget. It names the backend as `module:attribute`,
  holds the workspace by reference (a descriptor path Ictus never reads) and
  bounds the attempt (write scope, wall clock, step budget).
- `RuntimeEvidence` is what that backend returns from one attempt: the changed
  paths, patch digest and size, step count, exit/termination status and a typed
  failure kind. Ictus validates the shape and the bounds, requires a requested
  termination to be confirmed, and restates the facts as `ExecutionResult`
  evidence (`runtime_patch`, `runtime_exit`).

Unconfigured, both software routes keep their fail-closed placeholder failure
(`INTEGRATION_CONFLICT`). The Python validators in `ictus_dagster/seam/` mirror
these schemas without a runtime dependency, and
`tests/python/test_runtime_seam_contracts.py` checks that the schema and the
validators agree on the same corpus of documents.

### DecisionProposal v1 -> v2

The generic semantic vocabulary bumped `proposal.schema.json` to
`schema_version: 2`: `RETRY` became `REEXECUTE`, and `ROUTE` / `DECOMPOSE` were
added. Readers accept `schema_version: 1` proposals and normalize `RETRY` to
`REEXECUTE`; v1 payloads may not use the v2-only `ROUTE` / `DECOMPOSE` tokens.
`examples/decision-proposal.retry.json` is the legacy v1 example.
The other contracts are unchanged at `schema_version: 1`.
