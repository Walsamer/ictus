# Initial, Transitional, and End State

## 1. Initial state

The current the testbed environment is the source system and remains fully operational.

Conceptually:

```text
the testbed
├── work-order semantics
├── scheduler
├── atomic claims
├── dependency rules
├── worker routing
├── provider selection
├── subprocess execution
├── retries
├── stale-run recovery
├── verification
├── integration
├── promotion
├── recovery policy
├── decomposition
├── observability
├── handoffs
└── escalation
```

Dagster is not yet authoritative for any real the testbed behavior.

The new repository exists beside the testbed.

```text
the control repository          agentic-control
     │                        │
     │                        ├── Rust core
     │                        ├── Dagster OSS integration
     │                        └── contracts/examples
     │
     └──────── future adapter ─────────┘
```

### Initial-state rule

No new component may mutate the testbed state unless explicitly introduced in a later migration stage.

---

## 2. Early transitional state

First migration target:

```text
the testbed native verification
        ↓
authoritative result

Dagster verification
        ↓
shadow result
```

The system compares:

```text
native result
dagster result
match / mismatch
```

but only the testbed's native path affects behavior.

The new system gains real-world data without operational authority.

---

## 3. Transitional state: selective Dagster authority

After shadow validation:

```text
the testbed
  ↓
claims work
  ↓
creates the testbed run
  ↓
emits execution request
  ↓
Dagster
  ↓
runs verification / selected workflow
  ↓
structured execution result
  ↓
the testbed validates result
  ↓
the testbed performs authoritative domain transition
```

At this stage:

### the testbed still owns

- WO state
- claims
- domain eligibility
- project/resource conflicts
- decomposition
- recovery policy
- provider/worker policy
- escalation
- domain transitions

### Dagster owns

- selected execution workflows
- step state
- retries
- re-execution
- execution history
- workflow persistence

---

## 4. Transitional state: Rust decision shadowing

The Rust core is introduced only after Dagster executes at least one real workflow.

Flow:

```text
the testbed failure observation
      ├──────────────→ legacy the testbed decision
      │
      └──────────────→ Rust decision path
                         ↓
                    proposed decision

compare:
legacy decision vs new decision
```

Rust remains non-authoritative initially.

Examples to shadow:

- worker timeout
- verification failure
- provider failure
- task too complex
- repeated failure
- escalation threshold reached

---

## 5. Transitional state: Rust authority for narrow decisions

Rust becomes authoritative for selected bounded decisions.

Example:

```text
ExecutionObservation:
WORKER_TIMEOUT

Rust:
validate state
→ produce RETRY / DECOMPOSE / ESCALATE

Dagster:
execute resulting intent
```

the testbed still supplies domain-specific capabilities.

---

## 6. Late transitional state

Generic the testbed orchestration code is progressively removed.

Conceptually:

```text
Rust
= typed decision + policy

Dagster
= execution

the testbed
= software-engineering domain
```

the testbed capabilities may include:

```text
software.plan
software.implement
software.verify
software.integrate
software.promote
software.inspect_repo
software.manage_worktree
```

the testbed no longer needs to own generic retry/execution infrastructure.

---

## 7. End state

```text
                    ┌───────────────────────┐
                    │  State/Event Sources  │
                    └───────────┬───────────┘
                                ↓
                    ┌───────────────────────┐
                    │ Rust Decision/Policy  │
                    └───────────┬───────────┘
                                ↓
                    ┌───────────────────────┐
                    │      Dagster          │
                    └───────────┬───────────┘
                                ↓
                    ┌───────────────────────┐
                    │ Runtime / Sandboxing  │
                    └───────────┬───────────┘
                                ↓
                    ┌───────────────────────┐
                    │ Domain Capabilities   │
                    └───────────────────────┘
```

the testbed becomes one domain adapter:

```text
domains/
└── software_engineering/
    ├── state_adapter
    ├── capabilities
    ├── evidence_mapping
    └── domain_policy
```

The generic core is reusable without the testbed.

---

## 8. Enterprise/enterprise mapping

Personal environment:

```text
the testbed
→ Rust core
→ Dagster OSS
→ local processes
```

Potential enterprise environment:

```text
enterprise state platform
→ same Rust core
→ Dagster+
→ Kubernetes
→ enterprise capabilities
```

Only adapters and policies change.

The core contracts stay stable.
