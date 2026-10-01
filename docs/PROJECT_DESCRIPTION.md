# Project Description

## Problem

The current the testbed system is an effective autonomous software-engineering orchestration environment, but it currently owns too many reliability-critical concerns itself.

Examples include:

- scheduling
- work claiming
- retries
- crash recovery
- execution state
- subprocess lifecycle
- dependency progression
- run history
- verification sequencing
- recovery classification
- integration flow
- promotion flow
- human escalation

Some of those concerns are domain-specific and worth keeping. Others are generic durable-orchestration problems already solved by mature systems such as Dagster.

The long-term goal is therefore not to rewrite the testbed. It is to **separate policy and domain logic from durable execution mechanics**.

## Project goal

Build a reusable, publishable project that provides a small **typed agentic decision and policy layer** above Dagster.

The new project should:

- connect to Dagster OSS locally
- allow Dagster+ to be used later without changing the core architecture
- remain independent of the testbed
- allow the testbed to become its first private adapter
- progressively take over reliability-critical execution responsibilities from the testbed
- remain useful outside software engineering
- be portable enough that enterprise could potentially adopt the architecture and core code with different adapters

## Non-goals

This project is **not**:

- a replacement for Dagster
- a new workflow engine
- a new queue
- a custom durable execution engine
- a Kubernetes control plane
- a replacement for the testbed's domain semantics
- an LLM framework
- a hard dependency on a specialized model
- a the testbed rewrite
- a vendor-specific product

## Core idea

Separate three categories of responsibility:

### 1. Decision and policy

Owned by the new core.

Questions:

- What bounded action is being proposed?
- Is that action valid?
- Is the requested capability available?
- Is the action permitted under policy?
- Is human approval required?
- Is the proposal structurally valid?
- What execution intent should be emitted?

### 2. Durable execution

Owned by Dagster.

Questions:

- What steps must run?
- Which dependencies are satisfied?
- What failed?
- What can be retried?
- What can be re-executed?
- What is the persisted run state?
- What events occurred?
- What is the execution history?

### 3. Domain semantics

Owned by domain adapters such as the testbed.

Questions:

- What is a Work Order?
- What project does it belong to?
- What counts as a valid software verification?
- What does promotion mean?
- Which Git/worktree rules apply?
- Which worker/provider should be available?
- Which domain state transition is authoritative?

## Decision-provider model

The architecture must be model-agnostic.

Any of the following may produce the same typed proposal:

```text
Deterministic Rules
Specialized Model / specialized Model
LLM
Human
```

The control layer must treat them uniformly.

A proposal is not yet an action.

Every proposal must pass through:

```text
typed validation
→ capability validation
→ policy validation
→ optional approval
→ execution intent
```

Only then may Dagster execute it.

## Why Rust

Rust is used only for the small, stable core where stronger compile-time guarantees are valuable.

Appropriate uses:

- typed domain models
- exhaustive enums
- versioned contracts
- policy evaluation
- capability validation
- state invariants
- deterministic decision plumbing

Inappropriate uses:

- rebuilding Dagster
- custom workflow execution
- custom retry engines
- custom distributed scheduling
- custom queues
- process supervision that Dagster already handles

The Rust core should remain deliberately small.

## Why Dagster

Dagster is expected to own the reliability-critical execution substrate:

- durable runs
- step execution
- retries
- re-execution
- dependency graphs
- schedules and sensors where appropriate
- execution event history
- observability
- run persistence

Locally, this project uses Dagster OSS.

In a enterprise environment, the same concepts could map to Dagster+ while keeping the decision/policy layer unchanged.

## Why the testbed remains important

the testbed is not discarded.

the testbed provides:

- the first real domain adapter
- real failure modes
- real recovery scenarios
- worker execution
- Git/worktree behavior
- verification
- promotion
- provider logic
- decomposition behavior
- human escalation

That makes the testbed an excellent pressure-test environment for the new architecture.

The migration target is:

```text
the testbed today:
custom orchestration + policy + agents + recovery + execution

Target:
Rust = typed decisions and policy
Dagster = durable execution
the testbed = software-engineering domain/capability adapter
```

## Long-term portability

The core should be reusable by replacing adapters.

Personal environment:

```text
the testbed adapter
Local Dagster OSS
Local process runtime
agent harness / agent harness workers
Local policies
```

Possible enterprise environment:

```text
enterprise/data-platform state adapter
Dagster+
Kubernetes runtime
tool and model gateway integrations
enterprise policies
approved model providers
domain-specific capabilities
```

The reusable part should remain the same:

- Observation
- DecisionProposal
- PolicyDecision
- Capability
- ExecutionIntent
- ExecutionResult
- Evidence
