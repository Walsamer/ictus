# Ictus roadmap — Architecture Baseline v1

The [shared roadmap](https://github.com/Walsamer/tactus/blob/main/docs/ROADMAP.md)
links all M0–M5 objectives. GitHub Issues are shared intent; this table is a
2026-10-06 navigation snapshot. Native GitHub dependencies match the issue bodies.

M0 baseline adoption is complete through [PR #1](https://github.com/Walsamer/ictus/pull/1).
M0 intake enablement is tracked in [Tactus #20](https://github.com/Walsamer/tactus/issues/20).

## M1 — Control / Decision Boundary

| Issue | Title | Priority | Fleet status | Implementation dependencies |
|---|---|---|---|---|
| [ictus#2](https://github.com/Walsamer/ictus/issues/2) | Version initial/recovery context and validated decision contracts for Tactus | p0 | fleet:blocked | None |
| [ictus#3](https://github.com/Walsamer/ictus/issues/3) | Validate capability and approval grants before emitting executable intents | p0 | fleet:blocked | [ictus#2](https://github.com/Walsamer/ictus/issues/2) |
| [ictus#4](https://github.com/Walsamer/ictus/issues/4) | Select compatible backend and model routes from observed facts | p0 | fleet:blocked | [ictus#2](https://github.com/Walsamer/ictus/issues/2), [ictus#3](https://github.com/Walsamer/ictus/issues/3), [tactus#5](https://github.com/Walsamer/tactus/issues/5) |
| [ictus#5](https://github.com/Walsamer/ictus/issues/5) | Produce bounded semantic RecoveryDecisions from normalized observations | p0 | fleet:blocked | [ictus#2](https://github.com/Walsamer/ictus/issues/2), [ictus#3](https://github.com/Walsamer/ictus/issues/3), [ictus#4](https://github.com/Walsamer/ictus/issues/4) |

## M2 — Minimal Vertical Slice

| Issue | Title | Priority | Fleet status | Implementation dependencies |
|---|---|---|---|---|
| [ictus#6](https://github.com/Walsamer/ictus/issues/6) | Add durable Dagster submission, receipt reconciliation and result delivery | p0 | fleet:blocked | [ictus#2](https://github.com/Walsamer/ictus/issues/2), [ictus#3](https://github.com/Walsamer/ictus/issues/3) |

## Later cross-system milestones

M3 runtime/Stax/decomposition lives in Tactus #22/#23/#10. M4 authority transfer is
Tactus #24. M5 hardening is Tactus #25. These objectives may yield separately
scoped PRs in either repository; do not create duplicate roadmap issues for the
same objective. All six milestone names are available in both repositories.

## Initial Fleet intake

No fleet:ready issues yet. The current Fleet adapter's label, project mapping and
source-revision gates must be corrected in Tactus #20 before enablement. Once that
rollout is verified, Ictus #2 is the first proposed Ictus intake item; it can run
alongside Tactus #3. Humans may take dependency-satisfied issues by explicit
assignment. Preserve Issue revision/provenance in every derived WorkOrder.
