# Migration Plan

Architecture Baseline v1 supersedes the earlier system/session specification.
See [Ictus scope](architecture/BASELINE_V1.md), the
[canonical system specification](https://github.com/Walsamer/tactus/blob/main/docs/architecture/SYSTEM_ARCHITECTURE.md),
[migration](https://github.com/Walsamer/tactus/blob/main/docs/architecture/MIGRATION_FROM_FLEET.md), and [GitHub roadmap](ROADMAP.md).

The earlier document is preserved in Git history at
`833175d88adeed59f8e62c14c5e5577580cfdf33`; it is historical design evidence.
Current wire schemas remain under `contracts/`, with examples in
[contracts and boundaries](CONTRACTS_AND_BOUNDARIES.md).

## Tactus compatibility-caller migration

Tactus callers must stop using `BackendRegistry.compatible()` (or any local
ranking/fallback) as an authorization decision. They send versioned route
descriptor, health, quota and disablement facts, task capability requirements,
policy and prior-route exclusions to Ictus's
`LocalPolicyComposition::validate_with_route_selection` path.

The caller may admit only an `EXECUTABLE` envelope with an intent containing
`selected_route`. Before admission it compares the selected descriptor, health,
quota and disablement fact ids/revisions with its current observed facts. A
mismatch is rejected as stale; it is not reranked locally. `NO_ROUTE` and
`PENDING` envelopes carry no intent, and the Dagster adapter must execute the
selected backend/provider/runtime/model exactly or fail with a fact. It must
never substitute a compatible-looking route.
