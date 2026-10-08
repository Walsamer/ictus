# ADR 0002 — Versioned initial/recovery context and validated decision envelope

- **Status:** Accepted — 2026-10-07
- **Supersedes:** none
- **Related:** `docs/adr/0001-generic-decision-vocabulary.md`;
  `docs/CONTRACTS_AND_BOUNDARIES.md` §1 `DecisionContext` / `DecisionEnvelope`;
  `docs/DECISIONS.md` DEC-004.

## Context

A consumer such as Tactus must be able to hand Ictus a situation and receive a
decision back **without losing policy or subject binding**. Two gaps made that
unsafe:

1. There was no versioned description of the situation. A caller could imply a
   recovery without binding *which* attempt failed or *which* snapshot the
   failure was observed against, and there was nothing preventing a fresh
   (initial) decision from carrying a fabricated observation. Attempt budgets
   were also ambiguous: a semantic attempt budget and an execution backend's own
   step-retry counter were easy to conflate.
2. `PolicyDecision` was the only record of validation. It carries a proposal id
   and a verdict, but does not bind the *subject*, the snapshot digest/revision,
   the policy identity/version, the capability admission, the route or the
   approvals. A downstream consumer had no portable object proving that an
   `ExecutionIntent` was legitimate, and a denial could be adjacent to a
   still-executable intent.

These problems are generic. Solving them must not import any Tactus domain
entity into the core.

## Decision

Add two generic, versioned contracts, both starting at `schema_version: 1`
(independent of the base contracts and of the `DecisionProposal` v2 bump):

### `DecisionContext` — initial or recovery input

- `kind` is `INITIAL` or `RECOVERY`.
- An `INITIAL` context carries **no fabrication**: it must not bind a recovery
  and must not contain an `observation.category` fact. A recovery is expressed
  by a `RECOVERY` context, not by smuggling an observation into initial state.
- A `RECOVERY` context **must** bind the failed attempt
  (`failed_attempt_id`, `failed_intent_id`, `failed_snapshot`) together with the
  `ExecutionObservation` produced by the failure. The failed snapshot is pinned
  by id, revision and digest.
- `attempts.semantic_attempts` counts accepted semantic attempts **including the
  failed attempt**. `attempts.max_semantic_attempts` is the total bound.
  `attempts.step_retries` is the execution backend's own step-retry counter and
  is deliberately a *separate* field: a step retry never consumes a semantic
  attempt and never changes the semantic count.
- Missing or invalid values fail closed: unknown `kind`, blank ids, non-integer
  counts (a JSON `true` is not `1`), and `semantic_attempts` greater than
  `max_semantic_attempts` are all rejected.
- Facts are generic and dotted (`subject.revision`, `attempts.semantic`, ...).
  The core understands no domain noun; a domain adapter supplies the *fact
  profile* the consumer expects (`profile`, e.g. `tactus.generic.v1`).

### `DecisionEnvelope` — validated output

The envelope binds everything needed to reconstruct an authorized decision:

```text
subject
snapshot: { snapshot_id, revision, digest }
proposal: { proposal_id, schema_version, decision }
policy:   { policy_decision_id, policy_version, verdict }
capability_validation: { capability, admitted, permitted, reason }
route
approvals[]
outcome: EXECUTABLE | DENIED | PENDING | NO_ROUTE | INVALID
expiry
intent?
```

Binding rules:

- Only `EXECUTABLE` may carry an `ExecutionIntent`. `DENIED`, `PENDING`,
  `NO_ROUTE` and `INVALID` **must not** carry an intent, so a denial, a pending
  approval or a missing route can never ship an executable command.
- An `EXECUTABLE` envelope requires an admitted **and** permitted capability and
  an intent that references the same capability and the same subject as the
  snapshot; the intent is revalidated when the envelope is validated.
- A `MODIFY` policy verdict revalidates its modified intent
  (`PolicyDecision::revalidate_modified`) before anything executes; the modified
  intent must itself pass full validation.

## Versioning

- `DecisionContext` and `DecisionEnvelope` are `schema_version: 1`
  (`CONTEXT_SCHEMA_VERSION`, `ENVELOPE_SCHEMA_VERSION`).
- `schema_version` stays per-contract. The base contracts remain at `1`; the
  `DecisionProposal` contract remains at `2` from ADR 0001.
- A new wire version is introduced only when a token or shape genuinely changes.

## Compatibility (deliberate, preserved)

- The six frozen decision kinds from ADR 0001 are reused unchanged: `REEXECUTE`,
  `ROUTE`, `DECOMPOSE`, `ESCALATE`, `ABORT`, `EXECUTE_CAPABILITY`.
- Nothing here removes or changes the legacy `RETRY` (`schema_version: 1`)
  proposal compatibility; `examples/decision-proposal.retry.json` and its
  fixtures still pass.
- Existing `PolicyDecision` behavior is preserved: `REQUIRE_APPROVAL` may still
  carry a non-executable `modified_intent`. The "no executable intent" rule is
  enforced by the new envelope `outcome`, not by weakening `PolicyDecision`.
- Rust and Python fixtures are shared: the Rust JSON tests parse the *same*
  example files the Python schema tests validate, so the two halves cannot
  silently drift on unknown versions/kinds, bool-vs-integer counts or missing
  bindings.

## Non-goals / explicit exclusions

- No `WorkOrder` state machine, database, runtime dispatch, backend-selection
  algorithm or automatic integration-branch promotion.
- No Tactus domain entities or lifecycle nouns in the generic core.
- No vendor/transport concepts in route constraints (`ROUTE` names roles only).

## Consequences

- Callers have a portable, versioned way to describe initial and recovery
  situations and to prove a decision was validated.
- Attempt accounting is unambiguous: semantic attempts and step retries are
  distinct, and the semantic bound is total (including the failed attempt).
- `DENIED` / `PENDING` / `NO_ROUTE` / `INVALID` are structurally incapable of
  carrying an intent.
- Any future change to these shapes requires a versioned migration rather than a
  silent wire change.
