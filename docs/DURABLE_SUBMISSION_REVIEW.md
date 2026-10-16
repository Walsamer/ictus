# Durable submission boundary — acceptance review (AC-WO-007)

This is a **review and evidence** artifact for the durable Dagster submission
boundary published in `main`. It records the result of checking the published
revision against acceptance criteria 1–3. It makes no code, policy or
schema-semantic change, and it does not re-publish the boundary. The
implementation itself lives in
[`python/ictus_dagster/submission.py`](../python/ictus_dagster/submission.py);
the operator contract lives in [`DURABLE_SUBMISSION.md`](DURABLE_SUBMISSION.md).

## Reviewed revision

| Item | Value |
| --- | --- |
| Boundary pull requests | PR #16 (merge `50c5af213a7635f3a73620ab67b603ccde677403`), PR #17 (merge `f4581f94906d07decd3aca6ad3e83ae42e6cf9ae`) |
| Reviewed worktree HEAD | `ec698751e55a9b47e96146220f324204d885c842` |
| `submission.py` blob @ `f4581f9` | `9b82601dd1c2a775703e362af426fde7d15590cc` |
| `submission.py` blob @ HEAD | `9b82601dd1c2a775703e362af426fde7d15590cc` (identical) |
| Dagster | 1.13.25 (pinned `dagster>=1.8,<2`) |

The worktree HEAD contains the later AC-WO-GH-5 / AC-WO-GH-15 integrations.
`git diff f4581f9..HEAD` shows **no** change to `submission.py`; the only
boundary-adjacent additions after `f4581f9` are additive tests and docs in
`tests/python/test_submission_integration.py`, `docs/DURABLE_SUBMISSION.md` and
`examples/dagster/dagster.yaml`. The published boundary is therefore reviewed
as-is.

## Method

1. Static inspection of `submission.py`, `bridge.py` and the `submit`/`query`/
   `cancel` CLI routing.
2. Focused execution of the submission test modules and the Rust↔Dagster
   end-to-end test against Dagster 1.13.25 (results below).
3. Diff of the reviewed revision to confirm it matches what was published.

## Acceptance criterion 1 — real persistent instance, no in-process fallback

**Result: PASS.**

- `production_instance()` fails closed unless the instance is durable and
  independently launched: it requires `DAGSTER_HOME`, rejects ephemeral
  storage (`EphemeralStorageError`), rejects `InMemory`/`Sync` launchers, and
  requires `QueuedRunCoordinator` (`SubmissionConfigurationError`).
- `submit_intent()` performs `create_run_for_job` → `submit_run(run_id, None)`
  (`launch_run` only as a signature fallback). `_submit_to_dagster` never
  executes the job in the caller.
- `execute_in_process` does **not** appear in the production submission path.
  The only occurrence of the token in `submission.py` is the docstring at
  line 308 explaining that it must never be used (`submission.py:308`).
- The legacy synchronous bridge command (`execute_intent` in `bridge.py:69`,
  reachable only via the no-argument / `execute-demo` route) still uses
  `execute_in_process`; it is a separate, documented path. The durable commands
  (`submit`/`query`/`cancel`) never route through it (`bridge.py:111-116`).
- `tests/python/test_submission_integration.py` exercises a **real persistent**
  `DagsterInstance` (SQLite under `DAGSTER_HOME`, `QueuedRunCoordinator`,
  `DefaultRunLauncher`) and asserts both required run origins are persisted,
  the run is `QUEUED`, and `dagster/max_retries == "0"`.

## Acceptance criterion 2 — restart/replay and concurrent submitters reconcile to one receipt/effect; changed digest rejected

**Result: PASS (within the documented single-writer-host scope).**

- Same identity **and** digest reconciles to the original run/receipt and never
  creates a second run (`_reconcile_existing`, `_ensure_submitted`).
- A different digest for the same identity fails closed with
  `SubmissionConflict`; any run carrying the identity with a mismatched digest
  blocks the whole identity.
- Crash after durable record but before enqueue is repaired on the next
  submission of the same identity (`_ensure_submitted` re-enqueues only a
  `NOT_STARTED` run and returns the original receipt).
- A successful enqueue (`QUEUED`) is never resubmitted, so a lost
  acknowledgement cannot duplicate the effect.
- Concurrent submitters on one host are serialized by the `fcntl` advisory lock
  in `_identity_lock` under `$DAGSTER_HOME/.ictus-submission-locks`; a host
  without `fcntl` fails closed with `LockUnavailableError`.
- Proof: `test_same_intent_submitted_twice_creates_one_run`,
  `test_conflicting_payload_fails_closed`,
  `test_independent_processes_converge_on_one_persistent_receipt` (two real
  spawn processes → one receipt, then a changed-digest child → one
  `SubmissionConflict`, still one run),
  `test_crash_after_record_before_enqueue_is_recovered`,
  `test_process_death_after_record_before_queue_handoff_is_recovered`,
  `test_lost_acknowledgement_after_enqueue_does_not_resubmit` and
  `test_state_survives_a_process_restart`.
- Scope note: deterministic reconciliation detects and repairs duplicates, but
  Dagster does not enforce uniqueness on the bridge tags, so **multi-host
  writers remain unsupported**. This is an explicit, documented operator
  contract (`DURABLE_SUBMISSION.md` → "Deployment assumption: a single writer
  host", "Known limitations"), not an implementation defect in the submitted
  single-host scope.

## Acceptance criterion 3 — canonical v2 `REEXECUTE` vocabulary asserted end-to-end

**Result: PASS.**

- The end-to-end Rust → Dagster → Rust flow asserts the canonical v2 vocabulary:
  `tests/python/test_rust_bridge_e2e.py::test_full_flow_snapshot_to_result`
  requires `proposal.schema_version == 2` and `proposal.decision == "REEXECUTE"`
  (never the legacy v1 `RETRY`).
- The Rust layer enforces the same vocabulary: `crates/core/tests/contracts_api.rs`
  (`v1_retry_token_is_normalized_to_reexecute`, `v1_payload_rejects_v2_only_decisions`)
  and `crates/bridge/tests/cli.rs` (`decide_reexecute_within_budget_is_executable`
  plus the `flow`/`execute` assertions) pin `REEXECUTE`.
- Versioned JSON schemas (`contracts/proposal.schema.json`,
  `contracts/decision-envelope.schema.json`) restrict the v2 decision enum to the
  frozen six tokens including `REEXECUTE`.
- Semantic boundary note (not a gap): the durable `submit` command consumes an
  already-authorized `ExecutionIntent` and does not re-emit the decision token;
  `REEXECUTE` is a policy-layer output upstream of submission. The
  `DURABLE_SUBMISSION.md` "Submission retry vs semantic re-execution" section
  states that `submit` implements only transport retry (A) and never turns it
  into a semantic `REEXECUTE` (B). Criterion 3 is about the vocabulary being
  asserted end-to-end, which the Rust↔Dagster tests do.

## Gap analysis

No genuine code or contract gap was found within the durable submission
boundary, so no semantic change was made. The limitations that a reader could
mistake for gaps are deliberate and already documented:

| Observation | Status |
| --- | --- |
| Multi-host writers can create duplicate runs | Documented unsupported; needs external coordination. Not in the AC's single-host scope. |
| Stranded `NOT_STARTED` runs are repaired only on caller retry | Documented `Who owns recovery` contract; Ictus runs no background reconciler by design. |
| Legacy `execute-demo`/`execute` path uses `execute_in_process` | Separate documented synchronous path; production `submit`/`query`/`cancel` never use it. |
| `REEXECUTE` is not carried on the submission receipt | By design: the token is upstream policy output; the receipt binds intent identity + digest. |

## Verification evidence

Environment: Dagster 1.13.25, Python 3.12, Rust bridge built from this worktree
(`target/debug/ictus`, SHA-256 `20c7d5aa224dff2974471e620100c758cf838c3fbcd62dec5a16d295de43cc20`).
All commands were run from the reviewed worktree root.

| Command | Result |
| --- | --- |
| `pytest tests/python/test_submission.py` (required) | **36 passed** |
| `pytest tests/python/test_submission_integration.py` | **15 passed** |
| `pytest tests/python/test_rust_bridge_e2e.py` | **4 passed** |
| `cargo test -p ictus-core --test contracts_api` | **56 passed** |
| `cargo test -p ictus-bridge --test cli` | **21 passed** |

The registered integration verifier (`./scripts/verify-integration.sh`) remains
the merge gate and was not modified by this review.
