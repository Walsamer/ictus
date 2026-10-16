# Durable submission boundary — integration verification evidence (AC-WO-007-B)

This is an **evidence record** for the complete Ictus integration verification of
the durable Dagster submission boundary at its published revision. It attaches
the exact published SHAs, the verification intent identity, the verifier
command, the mutation result and the residual live-daemon smoke note. It makes
no code, contract or policy change and it does not re-publish the boundary.

It complements the acceptance review in
[`DURABLE_SUBMISSION_REVIEW.md`](DURABLE_SUBMISSION_REVIEW.md) and the operator
contract in [`DURABLE_SUBMISSION.md`](DURABLE_SUBMISSION.md).

## Published revision

| Item | Value |
| --- | --- |
| Repository | `Walsamer/ictus` |
| Canonical published SHA (`main`) | `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` |
| PR #16 head SHA | `90d427de29ab9630ec03d0801efdc986ce92abc2` |
| PR #16 merge commit | `50c5af213a7635f3a73620ab67b603ccde677403` (merged 2026-10-11T12:00:40Z) |
| PR #17 head SHA | `b182912073354161b1d0b3df4db48ef9c781474f` |
| PR #17 merge commit = canonical SHA | `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` (merged 2026-10-11T14:23:29Z) |
| Local hardening SHA | `c7087b5` — `local_hardening_tree_matches_canonical: true` |
| `submission.py` blob @ `f4581f9` | `9b82601dd1c2a775703e362af426fde7d15590cc` |
| `submission.py` blob @ current worktree HEAD (`dadec5a`) | `9b82601dd1c2a775703e362af426fde7d15590cc` (identical) |

The reviewed worktree contains the later AC-WO-GH-5 / AC-WO-GH-15 integrations;
`git diff f4581f9..HEAD` shows **no** change to `python/ictus_dagster/submission.py`.
The published boundary is therefore the exact artifact that was verified.

## Verifier command

The registered merge gate is a single shell-free entry point:

```
./scripts/verify-integration.sh
```

It runs the repository's own quality gates in CI order and fails fast
(`set -euo pipefail`). No gate is optional and none may be weakened:

1. `./scripts/verify.sh` — `cargo fmt --check`, `clippy -D warnings`, `cargo test`, `pytest`
2. `./scripts/coverage.sh` — Rust line coverage ≥ 95%, Python coverage ≥ 99% (fixed policy)
3. `./scripts/e2e_rust_dagster.sh` — Rust ↔ Dagster boundary end-to-end proof
4. `PROPTEST_CASES=1024 cargo test -p ictus-core --test properties` — property tests
5. `./scripts/mutants.sh` — `cargo-mutants`, no surviving viable mutant

`VERIFY_INTEGRATION_SKIP_MUTANTS=1` is a visible downgrade for constrained hosts
and **must never** be used to promote work CI would reject. It was not used for
the recorded pass below.

## Exact-head verification result

The authoritative exact-head verification was executed by Fleet and recorded in
`runtime/artifacts/operator-ictus-recovery-20261016/publication-evidence.json`:

| Field | Value |
| --- | --- |
| Intent id | `iverify-86e2929943b44c4f8b143015fa50f397` |
| Status | **PASSED** |
| Command | `./scripts/verify-integration.sh` |
| Started | 2026-10-12T00:01:37Z |
| Finished | 2026-10-12T01:21:45Z |
| Artifact dir | `runtime/artifacts/integration-sync/isync-cb3c1d47518049e7ba8ad6615e20534b/integration-verifier` |
| Revision | `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` |

### Mutation result

`./scripts/mutants.sh` reported:

| Metric | Count |
| --- | --- |
| Mutants tested | 418 |
| Mutants caught | 354 |
| Unviable mutants | 64 |
| Surviving viable mutants | 0 |

A surviving viable count of zero is the gate's pass condition.

This work order did **not** re-run `./scripts/verify-integration.sh`. It reuses
the authoritative preserved pass above, and Fleet independently re-runs the
complete registered verifier on the changes. Re-running the full mutation suite
here is explicitly out of the worker budget and is redundant with Fleet's gate.

## Residual live-daemon smoke note

The recorded intent carries one explicit limitation:

> Optional live-daemon smoke was skipped by the hermetic suite; this pass does
> not alone prove the independent-launcher acceptance criterion.

This is a deliberate design of the gate, not a hidden skip:

- The live smoke test `tests/python/test_submission_daemon_smoke.py` is guarded
  by `ICTUS_DAEMON_SMOKE=1` and is documented as isolated from the hermetic
  suite (it depends on daemon scheduling, subprocess startup and wall-clock
  timing). It is **not** part of the coverage gate's required evidence.
- `scripts/dagster_submission_smoke.py --evidence <path>` is the bounded,
  disposable live-daemon scenario runner. Like the smoke test, it is an
  operational check and is not part of the registered verifier.

Supplemental recovery-run evidence exists for the independent-launcher
criterion and is consistent with the published revision
(`main_sha: f4581f94906d07decd3aca6ad3e83ae42e6cf9ae`):

| Evidence | Location | Observation |
| --- | --- | --- |
| Main-process acceptance | `runtime/artifacts/operator-ictus-recovery-20261016/main-process-acceptance/result.json` (`passed: true`) | 2 concurrent submitter processes converge on **1** run/receipt; changed digest rejected; crash before queue hand-off recovered (`NOT_STARTED` → `QUEUED`, no duplicate); lost acknowledgement does not resubmit |
| Live-daemon smoke | `runtime/artifacts/operator-ictus-recovery-20261016/main-daemon-smoke/` | A real `dagster-daemon` dequeues, launches and reaches `RUN_SUCCESS` for a queued submission under a persistent instance |
| Recovery runs | `20261016T111050Z-run-AC-WO-GH-15-a7`, `20261016T134401Z-run-AC-WO-GH-5-a6` | Source runs that produced the preserved recovery evidence |

These supplemental checks are **not** part of the exact-head verifier pass and
must not be presented as if they were. The durable boundary remains reviewed
within its documented single-writer-host scope; multi-host writers are an
explicitly unsupported deployment assumption (see
[`DURABLE_SUBMISSION.md`](DURABLE_SUBMISSION.md) → "Deployment assumption: a
single writer host" and "Known limitations"), not a defect hidden by this pass.

## Claims and non-claims

- **Claimed:** the registered integration verifier (`verify` + coverage + e2e +
  property + mutation) passed at the published revision `f4581f9` under intent
  `iverify-86e2929943b44c4f8b143015fa50f397`.
- **Not claimed:** the hermetic verifier pass alone proves the
  independent-launcher live path; that is covered by the supplemental
  live-daemon evidence above and by Fleet's independent re-run.
- No gate was skipped, weakened or downgraded in producing this record.

## Provenance

- Preserved publication evidence:
  `runtime/artifacts/operator-ictus-recovery-20261016/publication-evidence.json`
- Recovery manifest:
  `runtime/artifacts/operator-ictus-recovery-20261016/manifest.json`
- Source review: [`DURABLE_SUBMISSION_REVIEW.md`](DURABLE_SUBMISSION_REVIEW.md)
- Work order: `AC-WO-007-review-and-publish-durable-submission-boundaryB`
  (parent `AC-WO-007-review-and-publish-durable-submission-boundary`)
