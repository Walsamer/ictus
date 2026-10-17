# Durable submission boundary — publication evidence (AC-WO-007-C)

This is a **publication evidence record** for the durable Dagster submission
boundary. It pins the exact reviewed commit SHA, the pull requests and merge
commits that published the boundary to `Walsamer/ictus`, and the exact-head
integration verification result for that revision. The document makes no code,
contract, schema-semantic or policy change, and it does not re-publish, push or
mutate any branch.

It complements the acceptance review in
[`DURABLE_SUBMISSION_REVIEW.md`](DURABLE_SUBMISSION_REVIEW.md), the verification
record in [`DURABLE_SUBMISSION_VERIFICATION.md`](DURABLE_SUBMISSION_VERIFICATION.md)
and the operator contract in [`DURABLE_SUBMISSION.md`](DURABLE_SUBMISSION.md).

## Exact reviewed SHA

| Item | Value |
| --- | --- |
| Repository | `Walsamer/ictus` |
| Exact reviewed commit SHA | `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` |
| Role | Canonical published `main` revision of the durable submission boundary |
| `submission.py` blob @ reviewed SHA | `9b82601dd1c2a775703e362af426fde7d15590cc` |
| `submission.py` blob @ current worktree HEAD | `9b82601dd1c2a775703e362af426fde7d15590cc` (identical) |
| Boundary source | [`python/ictus_dagster/submission.py`](../python/ictus_dagster/submission.py) |

The reviewed revision is the artifact that was published and verified. The
current worktree carries later, additive AC-WO-GH-5 / AC-WO-GH-15 integrations;
`git diff f4581f94906d07decd3aca6ad3e83ae42e6cf9ae..HEAD -- python/ictus_dagster/submission.py`
is empty, so no post-publication change has touched the boundary source.

## Pull-request references and merge commits

The durable submission boundary was published to `Walsamer/ictus` through two
merged pull requests. Both are visible in the repository history of the
reviewed worktree (`git log --oneline` contains `50c5af2` and `f4581f9`).

| PR | URL | Head SHA | Merge commit | Merged at |
| --- | --- | --- | --- | --- |
| PR #16 — "Harden the durable Dagster submission boundary (Phases 1–4)" | <https://github.com/Walsamer/ictus/pull/16> | `90d427de29ab9630ec03d0801efdc986ce92abc2` | `50c5af213a7635f3a73620ab67b603ccde677403` | 2026-10-11T12:00:40Z |
| PR #17 — "docs(submission): tighten deployment examples and recovery semantics" | <https://github.com/Walsamer/ictus/pull/17> | `b182912073354161b1d0b3df4db48ef9c781474f` | `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` | 2026-10-11T14:23:29Z |

PR #16 introduced the hardened boundary; PR #17 `docs` merge commit is the
canonical published `main` SHA (`f4581f9…`) and therefore the exact reviewed
revision. A local hardening branch carried SHA `c7087b5`; the preserved
publication evidence records `local_hardening_tree_matches_canonical: true`,
so the local tree and the canonical published tree are identical.

## Exact-head verification result

The authoritative exact-head integration verification was executed by Fleet and
recorded in
`runtime/artifacts/operator-ictus-recovery-20261016/publication-evidence.json`:

| Field | Value |
| --- | --- |
| Intent id | `iverify-86e2929943b44c4f8b143015fa50f397` |
| Status | **PASSED** |
| Revision | `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` |
| Started | 2026-10-12T00:01:37Z |
| Finished | 2026-10-12T01:21:45Z |
| Artifact dir | `runtime/artifacts/integration-sync/isync-cb3c1d47518049e7ba8ad6615e20534b/integration-verifier` |

### Verifier command

The registered merge gate is the single shell-free entry point
[`scripts/verify-integration.sh`](../scripts/verify-integration.sh):

```
./scripts/verify-integration.sh
```

It is `set -euo pipefail`, runs the repository's quality gates in CI order and
fails fast. No gate is optional and none may be weakened:

1. `./scripts/verify.sh` — `cargo fmt --check`, `clippy -D warnings`, `cargo test`, `pytest`
2. `./scripts/coverage.sh` — Rust line coverage ≥ 95%, Python coverage ≥ 99% (fixed policy)
3. `./scripts/e2e_rust_dagster.sh` — Rust ↔ Dagster boundary end-to-end proof
4. `PROPTEST_CASES=1024 cargo test -p ictus-core --test properties` — property tests
5. `./scripts/mutants.sh` — `cargo-mutants`, no surviving viable mutant

`VERIFY_INTEGRATION_SKIP_MUTANTS=1` is a visible downgrade for constrained hosts
and **must never** be used to promote work CI would reject. It was not used for
the recorded pass.

### Mutation result

`./scripts/mutants.sh` reported:

| Metric | Count |
| --- | --- |
| Mutants tested | 418 |
| Mutants caught | 354 |
| Unviable mutants | 64 |
| Surviving viable mutants | 0 |

A surviving viable count of zero satisfies the mutation gate.

This work order did **not** re-run `./scripts/verify-integration.sh`; re-running
the full mutation suite exceeds the worker budget and would duplicate Fleet's
independent re-run of the complete registered verifier on these changes.

## Residual live-daemon smoke limitation

The published revision is verified by the hermetic registered verifier. That
record carries one explicit limitation from the preserved evidence:

> Optional live-daemon smoke was skipped by the hermetic suite; this pass does
> not alone prove the independent-launcher acceptance criterion.

This is a deliberate property of the gate, not a hidden skip. The live smoke
test `tests/python/test_submission_daemon_smoke.py` is guarded by
`ICTUS_DAEMON_SMOKE=1` and is isolated from the hermetic suite because it
depends on daemon scheduling, subprocess startup and wall-clock timing.
`scripts/dagster_submission_smoke.py --evidence <path>` is the bounded,
disposable live-daemon scenario runner; it is an operational check and is not
part of the registered verifier.

Supplemental recovery-run evidence for the independent-launcher criterion is
consistent with the published revision (`main_sha: f4581f9…`):

| Evidence | Location | Observation |
| --- | --- | --- |
| Main-process acceptance | `runtime/artifacts/operator-ictus-recovery-20261016/main-process-acceptance/result.json` (`passed: true`) | Concurrent submitter processes converge on one run/receipt; changed digest rejected; crash before queue hand-off recovered without duplicate |
| Live-daemon smoke | `runtime/artifacts/operator-ictus-recovery-20261016/main-daemon-smoke/` | A real `dagster-daemon` dequeues, launches and reaches `RUN_SUCCESS` for a queued submission |
| Recovery runs | `20261016T111050Z-run-AC-WO-GH-15-a7`, `20261016T134401Z-run-AC-WO-GH-5-a6` | Source runs that produced the preserved recovery evidence |

These supplemental checks are **not** part of the exact-head verifier pass and
are not presented as if they were. The durable boundary remains published within
its documented single-writer-host scope; multi-host writers are an explicitly
unsupported deployment assumption (see [`DURABLE_SUBMISSION.md`](DURABLE_SUBMISSION.md)
→ "Deployment assumption: a single writer host" and "Known limitations"), not a
defect concealed by this record.

## Claims and non-claims

- **Claimed:** the durable submission boundary at the exact reviewed SHA
  `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae` was published to `Walsamer/ictus`
  via PR #16 (merge `50c5af21…`) and PR #17 (merge `f4581f9…`), and passed the
  registered integration verifier under intent
  `iverify-86e2929943b44c4f8b143015fa50f397`.
- **Not claimed:** the hermetic verifier pass alone proves the
  independent-launcher live path; that is covered by the supplemental
  live-daemon evidence above and by Fleet's independent re-run.
- No gate was skipped, weakened or downgraded in producing this record, and no
  branch, commit or pull request was created or modified by it.

## Provenance

- Preserved publication evidence:
  `runtime/artifacts/operator-ictus-recovery-20261016/publication-evidence.json`
- Recovery manifest:
  `runtime/artifacts/operator-ictus-recovery-20261016/manifest.json`
- Acceptance review: [`DURABLE_SUBMISSION_REVIEW.md`](DURABLE_SUBMISSION_REVIEW.md)
- Exact-head verification record: [`DURABLE_SUBMISSION_VERIFICATION.md`](DURABLE_SUBMISSION_VERIFICATION.md)
- Operator contract: [`DURABLE_SUBMISSION.md`](DURABLE_SUBMISSION.md)
- Work order: `AC-WO-007-review-and-publish-durable-submission-boundaryC`
  (parent `AC-WO-007-review-and-publish-durable-submission-boundary`)
