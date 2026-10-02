# Contributing to Ictus

Thanks for your interest. Ictus is small on purpose: the value comes from a
strict separation of responsibilities, so most review feedback is about
boundaries, not features.

## Architecture boundaries (binding)

```text
Ictus   = decisions + policy + capability validation
Dagster = durable execution
```

- **`ictus-core`** — typed, versioned contracts. No I/O, no domain, no provider,
  no transport.
- **`ictus-ports`** — abstract traits. No concrete adapter, no transport.
- **`ictus-policy`** — deterministic validation and policy. Never executes.
- **`ictus-bridge`** — the adapter to the execution backend.
- **`ictus_dagster`** — Dagster OSS execution backend.

Rules:

1. **No workflow engine in Rust.** Rust must never own durable run/step state,
   retries, re-execution, queues, scheduling, step sequencing or crash recovery.
2. **No policy decisions hidden in adapters.** The Dagster adapter reports
   facts only. It must never decide retry-vs-decompose-vs-escalate, authorize a
   capability, require an approval, or change domain state.
3. **Decision providers never execute.** A proposal is untrusted until it
   passes validation; only a validated `ExecutionIntent` may reach the backend.
4. **A `DENY` never reaches execution.** A denied proposal must not carry an
   execution intent.
5. **No domain knowledge in the core.** No domain system, model provider,
   runtime or infrastructure concept may leak into `ictus-core` or
   `ictus-policy`. Domain specifics belong in adapters.
6. **Routing is not authorization.** Mapping a validated capability to a
   workflow graph (`ictus_dagster.adapters.workflow_registry`) is an execution
   concern. Adding a workflow must never add a policy decision.

### Adding a workflow

1. add the ops/job under `python/ictus_dagster/{ops,jobs}/`;
2. add a `WorkflowSpec` to `adapters/workflow_registry.py` (capability → job,
   resource key/class, settings mapper);
3. add a step→observation mapping in `adapters/result_mapping.py`;
4. register the job in `definitions.py`; and
5. add a capability to the Rust CLI's builtin registry only if the example needs
   it — a real deployment loads capabilities from a registry file.

## Contracts require versioning discipline

- Every externally serialized payload carries `schema_version`.
- **A breaking wire-format change requires a new `schema_version`.** A rename,
  refactor or branding change does **not**.
- The documented examples in `docs/CONTRACTS_AND_BOUNDARIES.md` are a
  compatibility surface: `crates/core/tests/contracts_json.rs` and
  `tests/python/test_contracts_schemas.py` assert that the schemas and the
  examples still match.
- Adapters may support multiple versions during a migration; the core fails
  closed on unknown versions.

## Tests are required

Every change must keep the deterministic gate green:

```bash
./scripts/verify.sh
```

which runs:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `uv sync --frozen && uv run --frozen pytest -q` (also enforces Python coverage)

Coverage thresholds are enforced, not aspirational:

- **Python ≥ 99%** (configured in `pyproject.toml`; `pytest` fails below it).
- **Rust ≥ 95% of lines** (tests/fixtures excluded). Run the full gate with
  `./scripts/coverage.sh` (requires `cargo install cargo-llvm-cov --locked`).

Mutation testing guards against tests that execute lines but do not assert:

- `./scripts/mutants.sh` runs `cargo-mutants` on the fast crates
  (`ictus-core`, `ictus-policy`, `ictus-adapters`); it fails on any surviving
  viable mutant. Equivalent mutants are excluded with a documented reason in
  `.cargo/mutants.toml`.

New behaviour needs a deterministic test. Prefer synthetic fixtures; never
commit real secrets, private data or machine-specific paths.

## Pull requests

- Keep changes focused; do not mix a rename with a behaviour change.
- Explain the boundary you are touching and why.
- Update `docs/` and the contract schemas in the same change when a contract
  changes.
- Do not add new external integrations without a documented decision
  (see `docs/DECISIONS.md`).

## Development setup

Requires Rust (stable) and [`uv`](https://docs.astral.sh/uv/) with Python 3.12.

```bash
cargo build --workspace
uv sync --frozen
./scripts/verify.sh
```

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
