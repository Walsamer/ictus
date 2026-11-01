# ADR 0003 — `DecisionEnvelope` v2: capability validation only for capability-bearing decisions

- **Status:** Accepted — 2026-11-01
- **Amends:** `docs/adr/0002-initial-recovery-context-and-decision-envelope.md`
- **Related:** `docs/adr/0001-generic-decision-vocabulary.md`;
  `contracts/decision-envelope.schema.json`;
  `docs/CONTRACTS_AND_BOUNDARIES.md` §1 `DecisionEnvelope`; DEC-016.

## Context

The validated `DecisionEnvelope` (ADR 0002) required
`capability_validation.capability` to be present and non-empty whenever an
envelope was emitted. `EXECUTE_CAPABILITY`, `REEXECUTE` and `ROUTE` all carry a
capability, so they crossed it. `ESCALATE` and `ABORT` do not: they are the
decisions that hand work to a human or stop it. The trusted composition
therefore refused them, and `ictus decide --context` printed nothing
(exit 2, `missing required field: capability_validation.capability`) — reproduced
with a recovery context whose semantic budget was exhausted (2 of 2) and with an
`abort_on_exhaustion` context.

That left the system in the inverse of the intended shape: routine execution
carried a portable proof that Ictus authorized it, while the one decision where
a person authorizes something had to stay on the legacy unvalidated
`{proposal, policy_decision, execution_intent}` triple.

Three options were recorded. Inventing a capability to satisfy the field was
rejected outright: it is the self-asserted `ALLOW` the architecture forbids.
Leaving escalation on the triple was rejected because the hole sits exactly
where a human gate is. **Versioning the envelope** is the decision of record.

## Decision

### 1. `capability_validation` is conditional on the decision kind

`capability_validation` is required exactly when the decision carries a
capability — `EXECUTE_CAPABILITY`, `REEXECUTE`, `ROUTE` — and **must be absent**
for `ESCALATE`, `ABORT` and `DECOMPOSE`. A decision that carries no capability
has nothing to admit or permit, so a capability claim on one is a contract
error rather than a tolerated extra field: it is the fabricated `ALLOW` the
contract exists to prevent.

The condition is expressed in the artifact itself
(`contracts/decision-envelope.schema.json`, `allOf[0]`), so a consumer validates
against the schema rather than against prose. `DECOMPOSE` is in the
non-capability group because the proposal contract already forbids it a
capability (`DecisionProposal::requires_capability` is false for it, and
`RecoveryDecision::validate` rejects a `DECOMPOSE` carrying one). It had the
same defect and is closed by the same rule.

### 2. A new outcome token: `NO_EXECUTION`

The existing outcomes could not describe an escalation. `DENIED`, `PENDING`,
`NO_ROUTE` and `INVALID` are refusals, and a consumer must fail closed on them —
which would have re-opened the hole one layer up. `EXECUTABLE` requires an
intent by the ADR 0002 rule.

`NO_EXECUTION` says: the decision was validated and permitted, it carries no
capability, and nothing reaches an execution backend. The consumer acts on
`proposal.decision` (hand to a human, stop, decompose). It is only valid for a
decision that carries no capability, and it never carries an intent — the
"only `EXECUTABLE` may carry an intent" rule is unchanged.

A non-capability envelope binds no approvals, no grants and no route: each of
those binds a capability, and a satisfied approval on an escalation would be
readable as a human gate nobody passed. `validation.capability_version` carries
`not-applicable` (the field stays required and non-empty in both versions).

Everything else an execution envelope binds, an escalation envelope binds
identically: subject, snapshot id/revision and Ictus-computed digest, proposal
identity/version/decision, policy identity/version/verdict, the SHA-256 digest
of the canonical proposal bytes, the validation timestamp and the expiry. An
escalation is as verifiable as an execution.

### 3. Version policy

| | v1 | v2 |
| --- | --- | --- |
| Shape | frozen (ADR 0002) | conditional `capability_validation` |
| `capability_validation` | always present | present iff the decision carries a capability |
| `NO_EXECUTION` | not reachable | the outcome for `ESCALATE` / `ABORT` / `DECOMPOSE` |
| Emitted by this build for | a capability-bearing decision | a decision that carries none |
| Accepted on input for | a capability-bearing decision | either kind |

- **Emitted:** `2` for a decision that carries no capability, `1` for one that
  does (`DecisionEnvelope::schema_version_for`). v1 output for a
  capability-bearing decision is **byte-identical** to the previous build: no
  field was added, removed, reordered or reinterpreted, and the CLI's stdout for
  the initial, recovery, pending and denied examples is unchanged.
- **Accepted:** both versions. v2 is the conditional contract, not a
  non-executing-only format, so a capability-bearing decision under v2 is valid
  and must still carry its capability — a writer can move to v2 everywhere
  without another contract change. v1 cannot describe a decision that carries no
  capability, because v1 always carries `capability_validation`.
- Unknown versions fail closed.

### 4. Not changed

Decision semantics are untouched: no new proposal kinds, no policy weakening,
no change to when escalation, abort or decomposition is chosen, and no change to
the `PROPOSAL`/`PolicyDecision` contracts. The bridge's exit-code map is
unchanged (0 executable, 3 non-executable envelope on stdout, 2 usage or
contract error with empty stdout); an escalation now exits **3 with an
envelope** instead of 2 with nothing.

## Consequences

- The moment a human authorizes something now crosses the validated envelope
  with the same binding and digest chain as routine execution. Nothing has to
  stay on the legacy triple for that reason.
- A consumer that pinned `schema_version == 1` must accept version `2` and the
  `NO_EXECUTION` token, and take its action from `proposal.decision` for such an
  envelope. A consumer that enumerates outcomes must add the token and keep
  rejecting unknown ones.
- The wire vocabulary grew by one outcome token. This is a contract change
  carried by a version, not a silent reinterpretation of v1.
- Any future change to the conditional shape needs another versioned migration.
