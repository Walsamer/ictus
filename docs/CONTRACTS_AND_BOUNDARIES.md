> Implemented main-wire reference at `833175d`. Baseline v1 integration requirements
> and missing authorization/context semantics are tracked in
> [reconciliation](architecture/BASELINE_V1_RECONCILIATION.md). Do not treat a valid
> wire shape as proof of policy authorization.

# Contracts and Boundaries

## 1. Core contracts

The contracts are the real architecture.

Implementations may change.

Contracts should remain small, versioned, and stable.

---

## StateSnapshot

Represents the normalized state visible to a decision provider.

Example conceptual shape:

```json
{
  "schema_version": 1,
  "snapshot_id": "uuid",
  "timestamp": "2026-10-01T18:00:00Z",
  "domain": "software",
  "subject": {
    "type": "task",
    "id": "task-123"
  },
  "facts": [],
  "capabilities": [],
  "constraints": []
}
```

The core must not require domain-specific fields.

---

## ExecutionObservation

Represents a fact produced by execution.

Examples:

```text
SUCCESS
WORKER_TIMEOUT
PROCESS_CRASH
VERIFICATION_FAILURE
INTEGRATION_CONFLICT
RESOURCE_EXHAUSTED
PROVIDER_UNAVAILABLE
```

Observations are facts, not decisions.

---

## DecisionProposal

Produced by any DecisionProvider.

The frozen generic semantic vocabulary
(`docs/adr/0001-generic-decision-vocabulary.md`) is:

```text
REEXECUTE            # another semantic execution attempt; never a Dagster step retry
ROUTE                # execute a capability with generic route constraints
DECOMPOSE            # decompose the subject; Ictus creates no child work items
ESCALATE
ABORT
EXECUTE_CAPABILITY
```

A `ROUTE` may carry generic constraints (`exclude_backend`,
`preferred_backend`, `required_provider`, `required_runtime`) that name roles,
never vendors. Constraints are requirements, not a selected route: Ictus
normalizes versioned descriptor, health, quota and disablement facts, applies
compatibility/preference policy and emits either a selected route, bounded
no-route result, or approval requirement. The `DecisionProposal` contract is `schema_version: 2`; v1
payloads (legacy `RETRY`) are accepted and normalized to `REEXECUTE`, and
v2-only tokens are rejected under v1.

A proposal is untrusted until validated.

---

## PolicyDecision

Result of validation.

Examples:

```text
ALLOW
DENY
REQUIRE_APPROVAL
MODIFY
```

A denied proposal must never reach Dagster.

---

## ExecutionIntent

The only object allowed to cross into the execution backend.

Conceptual shape:

```json
{
  "schema_version": 1,
  "intent_id": "uuid",
  "capability": "software.verify",
  "target": {
    "type": "task",
    "id": "task-123"
  },
  "arguments": {},
  "policy_context": {},
  "selected_route": {
    "route": { "backend": "...", "provider": "...", "runtime": "...", "model": "..." },
    "descriptor": { "fact_id": "...", "revision": 1 },
    "health": { "fact_id": "...", "revision": 1 },
    "quota": { "fact_id": "...", "revision": 1 },
    "disablement": { "fact_id": "...", "revision": 1 }
  },
  "requested_by": {
    "provider": "rules",
    "decision_id": "uuid"
  }
}
```

`selected_route` is present only when a `ROUTE` decision is executable. The
fact references let Tactus reject stale admission without reranking; an
execution adapter must not replace any identity component with a fallback.

---

## ExecutionResult

Returned by execution backend.

Conceptual shape:

```json
{
  "schema_version": 1,
  "execution_id": "uuid",
  "intent_id": "uuid",
  "status": "failed",
  "observation": {
    "category": "VERIFICATION_FAILURE"
  },
  "evidence": [],
  "started_at": "...",
  "finished_at": "..."
}
```

---

## DecisionContext

Binds the situation a decision provider is asked about. It is either a fresh
`INITIAL` context or a `RECOVERY` context that re-enters a failed attempt.

Conceptual shape:

```json
{
  "schema_version": 1,
  "context_id": "uuid",
  "kind": "INITIAL",
  "profile": "tactus.generic.v1",
  "snapshot": { "...": "a StateSnapshot" },
  "revision": 3,
  "attempts": {
    "semantic_attempts": 0,
    "max_semantic_attempts": 2,
    "step_retries": 0
  },
  "grants": [],
  "facts": []
}
```

Rules:

- An `INITIAL` context carries no fabrication: it must not bind a recovery and
  must not contain an `observation.category` fact. A recovery situation is a
  `RECOVERY` context.
- A `RECOVERY` context must bind the failed attempt: `failed_attempt_id`,
  `failed_intent_id`, the `failed_snapshot` (id + revision + digest) and the
  `ExecutionObservation` produced by the failure.
- `semantic_attempts` counts accepted semantic attempts **including** the failed
  attempt; `max_semantic_attempts` is the total bound. `step_retries` is the
  execution backend's own step-retry counter and is deliberately separate. It
  is not a semantic attempt budget and never consumes one.
- Missing or invalid values (blank ids, non-integer counts, `semantic_attempts`
  greater than the bound) fail closed.

The core stores generic `profile`/facts keyed with dotted names, e.g.
`subject.revision` and `attempts.semantic`; it knows no domain entity. A domain
adapter supplies the profile facts Tactus expects.

---

## DecisionEnvelope

A validated, portable record of the decision. It is the object a consumer may
act on; it binds every fact needed to reconstruct *why* an intent is
legitimate.

Conceptual shape:

```json
{
  "schema_version": 1,
  "envelope_id": "uuid",
  "subject": { "type": "task", "id": "task-123" },
  "snapshot": { "snapshot_id": "uuid", "revision": 3, "digest": "sha256" },
  "proposal": { "proposal_id": "uuid", "schema_version": 2, "decision": "REEXECUTE" },
  "policy": { "policy_decision_id": "uuid", "policy_version": "0.1.0", "verdict": "ALLOW" },
  "capability_validation": { "capability": "demo.verify", "admitted": true, "permitted": true, "reason": "..." },
  "route": null,
  "approvals": [],
  "grants": [],
  "validation": { "context_id": "uuid", "proposal_digest": "sha256", "capability_version": "1", "validated_at": "..." },
  "outcome": "EXECUTABLE",
  "expiry": "2026-10-01T19:00:00Z",
  "intent": { "...": "an ExecutionIntent" }
}
```

The `outcome` is the resolved gate: `EXECUTABLE`, `DENIED`, `PENDING`,
`NO_ROUTE` or `INVALID`. It is derived from the policy verdict and route
resolution, and is never inferred from the presence of an intent.

Binding rules:

- The envelope binds the subject, the snapshot digest/revision, the proposal
  identity and version, the policy identity and version, the verdict, the
  capability validation, the route, the approvals and an expiry.
- Only `EXECUTABLE` may carry an `ExecutionIntent`. `DENIED`, `PENDING`,
  `NO_ROUTE` and `INVALID` carry no intent, so a denial, a pending approval or a
  missing route can never accidentally ship an executable command.
- An `EXECUTABLE` intent must reference the same capability the envelope
  validated and the same subject the snapshot was taken for; the intent is
  revalidated when the envelope is validated.
- A `MODIFY` proposal is revalidated (`PolicyDecision::revalidate_modified`)
  before its modified intent is executed: the modified intent must itself pass
  full validation.
- Production approval evidence is a bounded `ApprovalGrant`: it names one
  approval, subject, subject revision, capability, policy version, issue and
  expiry timestamps, revocation state, and at least one opaque domain evidence
  reference. Wildcard/demo tokens are not grants. A satisfied approval in an
  executable envelope must have a matching non-revoked bound grant.
- `validation.proposal_digest` binds the exact serialized candidate payload and
  must also be present in the executable intent's policy context. A changed
  payload therefore requires a fresh validation result.
- The `TrustedDecision` wrapper is the in-process authority boundary. Its JSON
  envelope is portable audit data; parsing or manually constructing an `ALLOW`
  envelope does not recreate the wrapper accepted by local execution code.

The vocabulary in the envelope stays generic. The decision kinds carried by
`proposal.decision` are exactly the six frozen tokens (including `REEXECUTE`,
`ROUTE`, `DECOMPOSE`, `ESCALATE`, `ABORT`, `EXECUTE_CAPABILITY`); a new wire
version is introduced only when a token or shape genuinely changes.

---

# 2. Interface boundaries

## StateProvider

```text
domain system
→ normalized snapshot
```

Must not expose internal database models directly.

## DecisionProvider

```text
StateSnapshot
→ DecisionProposal
```

Implementations:

- deterministic rules
- specialized decision model
- LLM
- human

## PolicyEvaluator

```text
StateSnapshot + DecisionProposal
→ PolicyDecision
```

Must be deterministic wherever possible.

## CapabilityRegistry

Answers:

- does capability exist?
- version?
- input/output schema?
- risk?
- approval requirements?
- side effects?
- idempotency?

## ExecutionBackend

```text
ExecutionIntent
→ ExecutionHandle
→ ExecutionResult
```

Initial implementation:

```text
Dagster OSS
```

Possible future implementation:

```text
Dagster+
```

The domain core must not care.

---

# 3. State ownership

## Core

Owns no workflow state.

It may store:

- decision records
- policy decisions
- audit references

It must not own:

- step execution lifecycle
- durable workflow progress
- domain object lifecycle

## Dagster

Authoritative for:

- Dagster run state
- Dagster step state
- retries
- re-execution
- workflow events

## Domain adapter

Authoritative for:

- domain-specific entities
- domain state
- domain-specific completion semantics

Example:

Dagster may report:

```text
verification workflow succeeded
```

The testbed may still decide:

```text
WO is not complete because integration evidence is missing
```

---

# 4. Adapter rule

Adapters translate.

They must not silently introduce policy.

Bad:

```text
Dagster adapter sees timeout
→ automatically decomposes task
```

Good:

```text
Dagster adapter sees timeout
→ emits WORKER_TIMEOUT observation
→ decision provider proposes action
→ policy validates
→ new execution intent emitted
```

---

# 5. Model independence

The core must not know whether a proposal came from:

- rules
- a specialized model
- Claude
- GPT
- Gemini
- human UI

Only typed proposal + metadata matter.

Required metadata should include:

```text
provider_type
provider_id
provider_version
confidence if meaningful
reason/evidence references
timestamp
```

---

# 6. Versioning

Every externally serialized contract must include:

```text
schema_version
```

`schema_version` is per-contract. The base contracts and the initial/recovery
`DecisionContext` and validated `DecisionEnvelope` contracts are at `1`; the
`DecisionProposal` contract is at `2`. Breaking changes require a new version;
new wire versions change only when required. Adapters may support multiple
versions during migration, and old inputs (e.g. legacy `RETRY` proposals) have
explicit compatibility fixtures.

---

# 7. Security boundary

The decision provider is not trusted to execute.

The policy layer is the trust boundary.

The execution backend only accepts validated ExecutionIntent objects.

Secrets must never appear in:

- model prompts unnecessarily
- decision proposals
- stored generic state snapshots
- public evidence records

Secrets belong to runtime-specific secret handling.

---

# 8. Domain-specific mapping examples

A private domain adapter may translate:

```text
VALIDATION_VERIFIER_FAILURE
→ VERIFICATION_FAILURE
```

or:

```text
domain work item status READY
→ domain fact task.runnable_candidate=true
```

These mappings belong in the domain adapter, not in the generic core.

---

# 9. Enterprise portability rule

Do not add concepts to the core solely because one enterprise uses them.

For example, the core should not contain:

```text
tool gateway
model gateway
a data warehouse
enterprise state platform
Dagster+
enterprise namespace names
```

Those are adapter/infrastructure concerns.

The core should contain generic equivalents:

```text
ToolGateway
ModelGateway
StateProvider
ExecutionBackend
IdentityContext
Capability
Policy
```
