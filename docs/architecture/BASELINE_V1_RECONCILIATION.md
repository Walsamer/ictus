# Ictus Baseline v1 reconciliation

Inspected GitHub/local main `833175d88adeed59f8e62c14c5e5577580cfdf33` and local
Fleet integration `5ee42ea5602703b8aafd8cab9b88e3feb8347087` on 2026-10-06.
There were no existing GitHub Issues, PRs or milestones. Reviewed source, tests,
schemas, examples, CI, README, status, architecture and migration/session plans.

| Existing behavior | Classification / conflict | Correct owner / action | Affected code/docs | Risk |
|---|---|---|---|---|
| Generic typed core, ports and three example domains | KEEP | Ictus; preserve other domains in regression fixtures | crates/core, ports, policy, adapters; Python jobs | Low. |
| Generic vocabulary ADR/Proposal v2 on integration only | KEEP + MODIFY | Ictus; review/promote candidate with bound validated result contracts | integration 5ee42ea, docs/adr/0001, proposal schema | Medium: main does not yet contain it. |
| Versioned wire JSON and stdio bridge | KEEP + MODIFY | Ictus; keep transport-independent schema; add initial/recovery profile and subject/revision/authorization binding | contracts/, bridge, core | High: shape is not authorization. |
| retry.attempt/budget defaults do not match Tactus facts | KEEP + MODIFY | Ictus semantic policy; explicit count/limit with no missing-value default | policy/rules.rs, evaluator.rs; Tactus snapshot | High: premature escalation. |
| Rule provider returns ABORT on SUCCESS | KEEP + MODIFY | Tactus handles success directly; Ictus makes generic no-action semantics explicit | rules.rs; cross-system fixtures | High: wrong domain retirement if naively wired. |
| ROUTE constraints carried but no actual selector | NEW GAP | Ictus compatibility/routing policy | core/decision, policy, intent construction | Medium: enum support is not routing. |
| Approval examples use local token/file/--approve providers | KEEP examples; MODIFY integration | Ictus validates bound domain grants; Tactus stores grant/intervention state | policy/approval.rs, adapters, bridge CLI | High: global demo approval is not production authorization. |
| Persistent DAGSTER_HOME plus synchronous execute_in_process | KEEP + MODIFY | Dagster integration; independently launched durable submission + receipt/query/result | python/ictus_dagster/bridge.py; execution port | High: persistence is not safe replay. |
| Coarse result categories and partial Python validators | KEEP + MODIFY | Execution bridge facts and shared contract conformance | adapters/result_mapping.py; validators; schemas/tests | Medium: wrong facts/schema divergence. |
| Roadmap/session plans use incompatible milestone sets | SUPERSEDE | GitHub Issues canonical intent; baseline canonical specification | ROADMAP, MIGRATION_PLAN, STATES, PROJECT_DESCRIPTION | Low; old versions preserved in Git. |
| Status says property/mutation tests absent despite existing gates | REMOVE stale claim | Documentation normalization | IMPLEMENTATION_STATUS, scripts/, CI | Low. |
| Domain WorkOrders treated as next-step roadmap | SUPERSEDE | Issue-derived execution units with provenance | README, contribution/status/migration docs | Medium: duplicate implementation effort. |

See the [system reconciliation](https://github.com/Walsamer/tactus/blob/architecture/baseline-v1/docs/architecture/BASELINE_V1_RECONCILIATION.md)
for Tactus issues #1–12, PRs #13–18, local/GitHub tree comparison and audit
06/07/08 findings. No runtime code is removed or changed by this architecture PR.
The new Ictus backlog covers contracts, compatibility/routing, capability/approval
validation, semantic recovery and the durable execution bridge. Implementation
can reuse the integration candidate but must not treat it as merged authority.
