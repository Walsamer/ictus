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
- **`lander.checks`** — every landing runs the repository's own promotion gate,
  as `./scripts/verify-integration.sh` runs it, but as separate steps so a
  failure names its gate: `./scripts/verify.sh` (fmt, clippy `-D warnings`,
  `cargo test --workspace`, `uv sync --frozen`, `pytest` — about 70 s),
  `./scripts/coverage.sh` (Python ≥ 99%, Rust ≥ 95% lines — about 2 min),
  `./scripts/e2e_rust_dagster.sh`, the property suite with `PROPTEST_CASES=1024`,
  and `./scripts/mutants.sh` (`cargo-mutants` over `ictus-core`, `ictus-policy`,
  `ictus-adapters`: about 500 mutants, the slow step at 10–30 min).
  `cargo-llvm-cov` and `cargo-mutants` must be installed on the machine.
  A failing landing is reported to the branch that submitted it; mutants are
  never skipped to promote work CI would reject.
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
