# Roadmap and Supervised Session Plan

This project should be developed in supervised sessions rather than a large autonomous migration.

Each session should:

1. inspect current state
2. make one bounded architectural change
3. run tests
4. compare behavior
5. stop for review
6. avoid automatically continuing into the next phase

---

# Session 1 — create the new repository

## Goal

Create a clean public-style repository.

## Work

- initialize repo
- add Rust workspace
- add Python package for Dagster integration
- add contracts directory
- add docs
- add CI
- add formatting/linting
- add minimal tests

## Do not

- connect to the testbed yet
- add Kubernetes
- add LLMs
- add a specialized model
- add deployment machinery

## Done when

- repo builds
- Rust tests run
- Python tests run
- Dagster local dev environment starts

---

# Session 2 — durable execution demo

## Goal

Prove Dagster is providing real value.

## Demo

```text
prepare
→ execute
→ verify
→ finalize
```

Introduce a deterministic failure.

Demonstrate:

- failed run persistence
- retry
- re-execution
- event history
- successful completion

## Done when

Dagster clearly handles execution state more cleanly than equivalent custom code would.

---

# Session 3 — typed contracts

## Goal

Implement the smallest viable Rust domain model.

Suggested types:

```text
StateSnapshot
ExecutionObservation
DecisionProposal
PolicyDecision
Capability
ExecutionIntent
ExecutionResult
```

Start with deterministic rules.

## Done when

A fake observation can produce:

```text
typed proposal
→ validated policy decision
→ execution intent
```

without Dagster or domain-specific knowledge.

---

# Session 4 — connect Rust to Dagster

## Goal

Connect generic ExecutionIntent to Dagster OSS.

## Done when

```text
Rust proposal
→ validated intent
→ Dagster execution
→ structured result
→ Rust-side result parsing
```

works locally.

---

# Session 5 — the testbed adapter discovery

This session happens primarily in the private the testbed repository.

## Goal

Inspect existing verification flow and define the smallest adapter.

Questions:

- exact verifier entrypoint?
- inputs?
- outputs?
- side effects?
- evidence?
- failure classes?
- state mutations?

Do not implement until these are explicit.

---

# Session 6 — the testbed verification shadow mode

## Goal

Run native and Dagster verification side by side.

## Rules

- native remains authoritative
- Dagster cannot mutate the testbed state
- mismatches are recorded
- Dagster failure must not break native path

## Output

Comparison report per run.

---

# Session 7 — verification authority transfer

Only after shadow evidence is good.

## Goal

Dagster becomes authoritative for executing verification.

the testbed remains authoritative for interpreting the result and changing the testbed state.

---

# Session 8 — recovery decision shadowing

## Goal

Feed real the testbed observations into the Rust decision layer.

Start with one simple class:

```text
WORKER_TIMEOUT
```

Compare:

```text
legacy the testbed decision
vs
new typed decision
```

No authority change yet.

---

# Session 9 — first Rust-authoritative policy

Pick one bounded decision class.

Suggested:

```text
simple retry eligibility
```

or:

```text
human escalation after exhausted retry budget
```

Avoid decomposition initially.

---

# Session 10 — full Dagster execution attempt

Move one complete attempt:

```text
prepare workspace
→ worker execution
→ verification
→ result collection
```

Leave integration/promotion for later if needed.

---

# Session 11 — integration/promotion capabilities

Model them as capabilities rather than hard-coded control flow.

Example:

```text
software.integrate
software.promote
```

Dagster executes.

Domain policy decides whether allowed.

---

# Session 12 — decomposition migration

Only after the simpler layers are stable.

Move:

```text
TASK_TOO_COMPLEX
→ Decompose proposal
```

into the generic decision model.

the testbed may remain the implementation of child-task creation initially.

---

# Session 13 — reduce the testbed scheduler responsibilities

Separate:

```text
admission/eligibility
```

from:

```text
temporal execution scheduling
```

Move the latter toward Dagster.

Do not duplicate authority.

---

# Session 14 — remove redundant the testbed execution code

Delete only code proven redundant.

Candidate areas:

- generic retry loops
- generic step sequencing
- generic execution persistence
- generic crash recovery
- duplicate execution event history

Preserve domain behavior.

---

# Session 15 — Kubernetes runtime experiment

Only after local architecture is stable.

Test one execution backend on Kubernetes.

Do not redesign the core.

---

# Session 16 — portability demonstration

Create a second non-the testbed example domain.

For example:

```text
data quality workflow
system diagnostic workflow
batch file processing
```

The purpose is to prove the project is genuinely generic.

---

# Release strategy

## v0.1

- contracts
- Rust core
- deterministic rules
- Dagster OSS adapter
- local demo

## v0.2

- first real the testbed adapter proven privately
- verification workflow stable
- stronger audit/evidence contracts

## v0.3

- recovery decision model
- multiple decision providers
- human approval interface

## v0.4

- capability registry
- richer Dagster orchestration patterns
- second generic domain example

## v1.0

Only after:

- core boundaries are stable
- no domain-specific leakage
- Dagster execution contract is stable
- at least two distinct domains demonstrated
- migration/adapter story documented
- operational semantics are clear
