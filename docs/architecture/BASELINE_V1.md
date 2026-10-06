# Ictus within Architecture Baseline v1

Normative Ictus scope, 2026-10-06. The [system baseline](https://github.com/Walsamer/tactus/blob/architecture/baseline-v1/docs/architecture/SYSTEM_ARCHITECTURE.md)
and [ownership table](https://github.com/Walsamer/tactus/blob/architecture/baseline-v1/docs/architecture/OWNERSHIP_BOUNDARIES.md) are maintained in Tactus.
These branch links make the paired baseline reviewable before merge; move links
to accepted main as part of adoption. Do not maintain another system ownership table.

## Keep the generic core

Ictus transforms observed state/context → typed candidate → policy validation →
capability validation → authorized route where applicable → validated decision or
ExecutionIntent. Candidate generation may be nondeterministic; validation is
explicit, bounded and fail-closed. Backend compatibility, model/provider selection,
approval requirements and semantic recovery belong here.

Retain core, policy, ports, bridge, domain-independent subjects/facts/capabilities,
and the software/data-quality/system-diagnostic examples. No WorkOrder class,
Tactus lifecycle enum, durable domain state, queue or retry loop belongs in core.
Tactus owns state, human grant records, fenced admission and application of
validated effects. Dagster owns temporal execution; the Python bridge may contain
execution transport and job mechanics but must not contain semantic policy.

## Decisions versus execution

Use the generic vocabulary already present on the reviewed integration candidate:
REEXECUTE, ROUTE, DECOMPOSE, ESCALATE, ABORT, EXECUTE_CAPABILITY. Preserve deliberate
RETRY v1 compatibility. RecoveryDecision describes a validated semantic result,
not a new domain-specific core state machine. A proposal alone is not authority.
The [contract specification](https://github.com/Walsamer/tactus/blob/architecture/baseline-v1/docs/architecture/CROSS_SYSTEM_CONTRACTS.md) requires subject,
snapshot/revision, policy/capability verdict, approvals and effect binding.

Ictus produces approval requirements; the domain records/resolves grants and
Ictus checks their sufficiency. No permissive `--approve` demo shortcut may become
a production approval grant. Denied/pending/invalid decisions have no executable
intent. A no-route result is typed and bounded; adapters cannot choose a fallback.

REEXECUTE is semantic authorization for another domain attempt. Dagster step retry
is an execution mechanism within an existing attempt. Ictus does not persist a
retry engine. Success is applied by Tactus directly; the existing rule-provider
ABORT-on-SUCCESS behavior is not permission to retire completed domain work.

## Bridge requirements

Retain stdio examples and persistent DAGSTER_HOME support. Add a separately usable
durable submission/receipt/query/result boundary for independently launched Dagster
runs. Persistence of in-process history alone does not prove queueing or restart
safety. Capability-to-job mapping is mechanical execution routing; selecting an
allowed backend/model remains Ictus policy.

M2 uses one run per semantic attempt with safe step retries, a simple local runtime
and run-level retries disabled. Shared fixtures must exercise Tactus's initial
snapshot and semantic budgets, policy denial, route facts, approval binding and
late/duplicate results. No new Tactus-specific database or global orchestrator.

## Source of implementation intent

[GitHub Issues](https://github.com/Walsamer/ictus/issues) replace the old session
plan. Fleet WorkOrders derive from those Issues and preserve source revisions.
Only `fleet:ready` is an intake authorization label; baseline review, upgraded
source freshness checks and correct project mapping precede enablement.
See [reconciliation](BASELINE_V1_RECONCILIATION.md) and [roadmap](../ROADMAP.md).
