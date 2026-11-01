# .ultimate — Ultimate's setup for Ictus

This folder is Ultimate's project configuration, committed with the repository, so
everyone who starts `ultimate` in this checkout gets the same working setup. It
adds no product behaviour; the Rust crates, the Python bridge and the scripts
stay the source of truth.

## What is configured

`settings.json`:

- **`isolates.worktree.setup`** — every coordinator, worktree subagent and the
  lander works in a fresh `git worktree`, which has none of the ignored files.
  `uv sync --frozen` (about 6 s, from the uv cache) and `cargo fetch` (about 1 s)
  give it its dependencies before the first tool call. Nothing else is needed:
  the pytest suite creates its own Dagster home per test and touches no
  developer state.
- **`lander.checks`** — every landing runs these four, in this order, and a
  failure names its gate: `./scripts/verify.sh` (fmt, clippy `-D warnings`,
  `cargo test --workspace`, `uv sync --frozen`, `pytest` — about 70 s),
  `./scripts/coverage.sh` (Python ≥ 99%, Rust ≥ 95% lines — about 2 min),
  `./scripts/e2e_rust_dagster.sh`, and the property suite with
  `PROPTEST_CASES=1024`. `cargo-llvm-cov` must be installed on the machine.
  A failing landing is reported to the branch that submitted it.
- **Mutation testing is the branch's job, not the lander's.** `./scripts/mutants.sh`
  is silent for roughly two hours (494 mutants on this workspace), and the
  lander stops any check that prints nothing for 120 s — a limit a check's
  `timeout` can only lower, since `extensions/workspaces/lander.ts` has
  `stall = min(120, timeout)` and no per-check stall setting. So the branch runs
  it, to completion, and reports the numbers (caught / unviable / missed /
  timeouts) in its commit and its submission. Mutants are never skipped to
  promote work CI would reject; `VERIFY_INTEGRATION_SKIP_MUTANTS=1` stays
  forbidden for promotion.
- **No push.** The lander merges into local `main` only. Publication is a pull
  request and Fleet owns integration and promotion, so `lander.push` is unset on
  purpose.

## Working with Ultimate here

Start it with `ultimate` in this folder. Onboarding asks once whether to trust
this setup; after that:

- **@ultimate** is the one conversation: tell it what you want and it hands the
  work to a coordinator branch. This repository's `AGENTS.md` is the boundary
  document — Ictus owns execution, Tactus owns the control plane, Fleet owns
  integration.
- A coordinator works in its own `git worktree` and submits through the lander.
- `main` in this checkout is integrated locally; the published revision is
  whatever `git log -1` says after Fleet promotes it.

Secrets belong to the user, never to this folder: add them in ⌘K → Secrets.
