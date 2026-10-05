# ADR 0001 — Generic semantic decision vocabulary and its versioning

- **Status:** Accepted (frozen) — 2026-10-05
- **Supersedes:** none
- **Related:** `docs/DECISIONS.md` DEC-014, DEC-004, DEC-005;
  `docs/CONTRACTS_AND_BOUNDARIES.md` §1 `DecisionProposal`.

## Context

Ictus is the generic, domain-neutral decision core. Its initial decision
vocabulary was the v1 set:

```text
RETRY
ABORT
ESCALATE
EXECUTE_CAPABILITY
```

That set is too small for the decisions the core is asked to express, and the
name `RETRY` collides with durable-execution *step* retries (for example
Dagster's `RetryPolicy`). A richer vocabulary is needed so a Fleet/Tactus
integration can exchange recovery decisions without importing any Tactus domain
noun into the generic core.

## Decision

Freeze the generic semantic vocabulary to exactly these kinds:

| Kind | Generic semantics | Requires capability | Produces `ExecutionIntent` |
| --- | --- | --- | --- |
| `REEXECUTE` | request another **semantic** execution attempt of the same capability | yes | yes |
| `EXECUTE_CAPABILITY` | execute a named capability | yes | yes |
| `ROUTE` | execute a capability subject to generic route constraints | yes | yes |
| `DECOMPOSE` | decide the subject should be decomposed into smaller units | no | no |
| `ESCALATE` | hand to a human | no | no |
| `ABORT` | stop; no further action | no | no |

Binding semantics:

- **`REEXECUTE` is a semantic re-execution request.** It yields a new
  `ExecutionIntent` and is *never* a Dagster step retry. Dagster's own
  `RetryPolicy` remains an execution-backend concern (see DEC-004); the two must
  not be conflated.
- **`ROUTE` carries generic route constraints:** `exclude_backend`,
  `preferred_backend`, `required_provider`, `required_runtime`. These name
  *roles*, never concrete vendors, transports or domain nouns. They are carried
  on the resulting `ExecutionIntent.policy_context` for the execution backend to
  honour; the core performs no routing itself.
- **`DECOMPOSE` describes the decision to decompose.** Ictus itself creates no
  child work items and no domain lifecycle objects. Translation into any
  domain's decomposition is an adapter concern.
- `ESCALATE` and `ABORT` never reach the execution backend.

## Versioning

The vocabulary change is breaking for the `DecisionProposal` wire contract: the
token set changes and the previous token is no longer emitted. Therefore the
`DecisionProposal` contract is bumped to `schema_version: 2`
(`DECISION_SCHEMA_VERSION`).

The other contracts (`StateSnapshot`, `ExecutionObservation`, `ExecutionIntent`,
`ExecutionResult`, `PolicyDecision`) did not change and remain
`schema_version: 1` (`SCHEMA_VERSION`). `schema_version` is per-contract.

## v1 compatibility (deliberate)

- v2 writers emit `schema_version: 2`.
- Readers accept `schema_version: 1` proposals and normalize the legacy `RETRY`
  token to `REEXECUTE` (a serde alias on `DecisionKind::Reexecute`).
- v1 proposals may use only the legacy vocabulary
  (`RETRY`/`REEXECUTE`, `ABORT`, `ESCALATE`, `EXECUTE_CAPABILITY`). The v2-only
  tokens `ROUTE` and `DECOMPOSE` are rejected under `schema_version: 1`.
- `route` constraints are valid only for a `ROUTE` decision.
- Unknown proposal versions fail closed.

## Non-goals / explicit exclusions

- **No Tactus lifecycle nouns** (`REQUEUE_READY`, `BLOCK`, `RETIRE`) in generic
  Ictus contracts.
- No conflation of semantic re-execution with Dagster step retry.
- No Tactus child `WorkOrder` creation.
- No vendor, domain, transport or infrastructure concepts in the core.

## Consequences

- `DecisionKind` is exhaustive; matching must not use a catch-all that hides a
  future variant.
- The rule provider emits `REEXECUTE` where it previously emitted `RETRY`; the
  semantic re-execution budget is still enforced by policy and Dagster still owns
  its own step retries.
- Existing v1 proposal payloads remain readable during migration; consumers
  that relied on the literal `RETRY` token must accept `REEXECUTE` (or keep
  reading v1).
