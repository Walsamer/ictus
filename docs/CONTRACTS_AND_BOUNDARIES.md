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

Examples:

```text
Retry
Abort
Escalate
ExecuteCapability
Route
Decompose
```

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
  "requested_by": {
    "provider": "rules",
    "decision_id": "uuid"
  }
}
```

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
- specialized specialized model
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

the testbed may still decide:

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

Breaking changes require a new version.

Adapters may support multiple versions during migration.

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

# 8. domain-specific mapping examples

Private the testbed adapter may translate:

```text
VALIDATION_VERIFIER_FAILURE
→ VERIFICATION_FAILURE
```

or:

```text
WO status READY
→ domain fact task.runnable_candidate=true
```

These mappings belong in the testbed, not in the generic core.

---

# 9. enterprise portability rule

Do not add concepts to the core solely because enterprise uses them.

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
