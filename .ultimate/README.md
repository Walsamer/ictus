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
- **`lander.checks`** — `./scripts/verify.sh` (fmt, clippy `-D warnings`,
  `cargo test --workspace`, `uv sync --frozen`, `pytest` with its 99% coverage
  gate) runs on every landing, about 2 minutes from a warm worktree. No push: the
  lander merges into local `main` only. Publication stays a pull request, and
  Fleet owns integration and promotion — `./scripts/verify-integration.sh` (the
  full gate, including `coverage.sh`, `e2e_rust_dagster.sh`, the 1024-case
  property suite and `cargo-mutants`) is the promotion gate before a PR merges,
  not a step on every local landing.

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
