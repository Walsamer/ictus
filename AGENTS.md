# Ictus — project agent entry point

This repository is **Ictus** (Fleet project id `agentic-control`): the Rust
execution core, the Python `ictus_dagster` bridge, and the durable Dagster
submission boundary.

## Boundaries — keep the systems separate

- **Ictus** (this repo, `Walsamer/ictus`) owns execution, the Dagster bridge, and
  Dagster run / receipt / result state.
- **Tactus** (`Walsamer/tactus`, Fleet project `tactus`) is the control plane
  (validated intents, decisions, StateSnapshots, recovery vocabulary). Do not
  implement Tactus policy here, and do not edit the Tactus repository from an
  Ictus Work Order.
- **Fleet** (`../dev-ops-fleet`) is the controller. Its
  [`AGENTS.md`](../dev-ops-fleet/AGENTS.md) is authoritative for the controller;
  this file is authoritative for this repository.

## Read first

- `docs/DURABLE_SUBMISSION.md` — submission / receipt / result contract and recovery semantics.
- `docs/DURABLE_SUBMISSION_REVIEW.md` — acceptance review (criteria 1–3 PASS).
- `docs/DURABLE_SUBMISSION_VERIFICATION.md` — exact-head verification evidence.
- `docs/CONTRACTS_AND_BOUNDARIES.md`, `docs/DECISIONS.md`.
- `docs/architecture/BASELINE_V1.md`.

## Hard rules

- The production submission path uses a real persistent `DagsterInstance`
  (`DAGSTER_HOME`, `QueuedRunCoordinator`, independent launcher). It rejects
  ephemeral storage and `InMemory`/`Sync` launchers and never falls back to
  `execute_in_process`.
- One semantic intent = one Dagster run: `run_retries.enabled: false`,
  `max_retries: 0`; safe step retries stay inside that attempt.
- Same identity + digest returns the same receipt; a changed digest fails closed;
  lost acknowledgement reconciles without a duplicate effect.
- Result mapping preserves raw evidence and distinguishes timeout, cancellation
  and process failure; the bridge carries no semantic reroute/escalation policy.
- Keep the three-domain synchronous / stdin-stdout demos working; no Tactus
  dependency in the core.
- Run `./scripts/verify-integration.sh` with every gate (verify, coverage, e2e,
  property, mutation). Never use `VERIFY_INTEGRATION_SKIP_MUTANTS=1` to promote
  work CI would reject. Do not modify `scripts/verify*.sh`, `scripts/coverage.*`
  or `scripts/mutants.sh` unless a Work Order explicitly authorizes it.
- Never push, deploy, publish, or edit another repository from here. Fleet owns
  integration and promotion.
- **Publication is a PR by default.** A request to "push <branch>" means open a
  pull request for review, not a direct push. Direct pushes to `main` require an
  explicit operator override in the same request; force-push is never allowed.

## State

The published revision is whatever `main` points at (check `git log -1`); do not
assume a SHA. Fleet integrates Work Orders into
`fleet/agentic-control-integration` and promotes to `main` separately.
