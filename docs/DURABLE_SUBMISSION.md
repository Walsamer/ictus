# Durable Dagster submission

This document is the supported-deployment contract for the Ictus → Dagster
submission boundary in [`python/ictus_dagster/submission.py`](../python/ictus_dagster/submission.py).
It is verified against **Dagster 1.13.25** (the pinned `dagster>=1.8,<2`).

## What the boundary does

| Command | Input | Output contract | Effect |
| --- | --- | --- | --- |
| `submit` | `ExecutionIntent` | `ExecutionReceipt` (flat) | record one run, enqueue it |
| `query` | receipt (or `execution_id`) | `ExecutionStatus` | read current state + result |
| `cancel` | receipt | `ExecutionStatus` (+ `cancellation`) | request cancellation, report it |

`submit` never executes work in the caller; it records a Dagster run and hands
it to the instance's queued coordinator. `ExecutionReceipt` and
`ExecutionStatus` are deliberately **separate** contracts (see
[`contracts/README.md`](../contracts/README.md)) so a strict receipt reader
never has to tolerate reconciliation-only fields.

## Supported deployment

Durable submission is only meaningful on a queued, independently executed
deployment. Requirements fall into two classes with different enforcement:

**Checked synchronously, fail closed.** `production_instance()` rejects these
immediately with `EphemeralStorageError` or `SubmissionConfigurationError`:

1. **Persistent storage.** `DAGSTER_HOME` is set to an absolute path and the
   instance is not ephemeral.
2. **`QueuedRunCoordinator`.** The bridge process has no workspace, so it must
   not launch the run itself; `submit_run` only enqueues.
3. **Independent launcher.** `DefaultRunLauncher` (not the InMemory/Sync
   launcher used by ephemeral/test instances).

`_create_run` additionally requires `ICTUS_DAGSTER_CODE_LOCATION` and verifies
that both the `remote_job_origin` and `job_code_origin` were persisted.

**Operational prerequisites, not checked synchronously:**

4. **A running `dagster-daemon`.** The daemon attaches its real workspace,
   dequeues the run and launches it. `production_instance()` cannot observe
   whether a daemon is live; without one the run simply stays `QUEUED`.
5. **A registered, loadable code location.** The daemon's `workspace.yaml` must
   register the location named by `ICTUS_DAGSTER_CODE_LOCATION` and be able to
   load it. This is validated only when the daemon resolves and launches the
   run; the bridge has no workspace to check against.

There is no silent fallback to in-process execution.

### Environment variables

| Variable | Required | Meaning |
| --- | --- | --- |
| `DAGSTER_HOME` | yes | Absolute path to the persistent Dagster instance home. |
| `ICTUS_DAGSTER_CODE_LOCATION` | yes | Code-location name the daemon resolves (must match `workspace.yaml`). |
| `ICTUS_DAGSTER_REPOSITORY` | no | Repository name inside that location; defaults to `__repository__` (the name Dagster gives a `Definitions` object). |

### Example configuration

Copy [`examples/dagster/dagster.yaml`](../examples/dagster/dagster.yaml) and
[`examples/dagster/workspace.yaml`](../examples/dagster/workspace.yaml) into
`$DAGSTER_HOME`:

```
$DAGSTER_HOME/
├── dagster.yaml      # QueuedRunCoordinator + DefaultRunLauncher
└── workspace.yaml    # registers location_name: ictus
```

Dagster keeps run/event state under `$DAGSTER_HOME/storage` by default; keep it
there. Do **not** configure a cwd-relative `storage.sqlite.base_dir` such as
`./storage`: the bridge and the daemon are separate processes that may run from
different working directories, so a relative path gives them different run
stores and the daemon never sees the bridge's queued runs.

The bundled example uses local SQLite. Run the bridge and the daemon on the
**same host** so they share that file. Split them across hosts only after
independently validating that the configured storage backend is safe for
concurrent multi-host access.

Initialize `$DAGSTER_HOME` **once** before the first concurrent use (start the
daemon, or run `dagster instance migrate`). SQLite migrations are not safe
against two processes initializing a brand-new store at the same time, so
do not point the bridge and the daemon at an uninitialized store simultaneously.

Then, in a separate process:

```bash
export DAGSTER_HOME=/var/lib/ictus/dagster_home
export ICTUS_DAGSTER_CODE_LOCATION=ictus
uv run --frozen dagster-daemon run
```

If any requirement is unmet, submission fails closed with
`EphemeralStorageError` or `SubmissionConfigurationError` and a message naming
the offending configuration. There is no silent fallback to in-process
execution.

## Lifecycle and state

Dagster owns the run record, queue and event log; Ictus keeps **no** second
execution-state database.

1. `create_run_for_job` durably records the run with both origins:
   - `remote_job_origin` (`RegisteredCodeLocationOrigin(location_name)`) so the
     daemon can resolve the code location;
   - `job_code_origin` (the reconstructable job) so the run is rebuildable.
2. `submit_run(run_id, None)` enqueues it. Passing `None` for the workspace is
   safe **only** because the queued coordinator ignores it; the daemon supplies
   the real workspace at dequeue time.
3. `query` reconciles the run status. Pending runs return `state: "pending"`
   with no result; terminal runs return `state: "terminal"` and the correlated
   `ExecutionResult` in `result`.

### Cancellation

`cancel` calls `instance.run_coordinator.cancel_run(run_id)`. The response
distinguishes:

- **requested** – Ictus asked Dagster to cancel;
- **accepted** – the coordinator accepted the request (`accepted: true`);
- **status** – the run status observed immediately afterwards.

A queued run cancels synchronously and reconciles to a terminal `cancelled`
result. A running run may report `canceling` (`state: "pending"`) and must be
reconciled again with `query` before it is terminal.

## Idempotency, crash recovery, duplicates

One immutable intent maps to **one** Dagster run:

- Submission is keyed by `ictus.intent_id`; the immutable content is bound by
  `ictus.intent_digest` (SHA-256 of the canonical JSON).
- A repeat with the same identity and digest reconciles to the original run and
  returns its original receipt; it never creates a second run.
- A different digest for the same identity fails closed
  (`SubmissionConflict`).
- **Crash window.** If the process dies *after* `create_run_for_job` but
  *before* `submit_run`, the run is still `NOT_STARTED`. The next submission of
  the same identity finds it and re-enqueues it (`_ensure_submitted`), rather
  than creating another run.
- **Duplicate tagged runs.** If more than one run somehow carries the identity,
  reconciliation is deterministic (oldest `create_timestamp`, then `run_id`).
  Any run with a conflicting digest makes the whole identity fail closed. This
  is **after-the-fact detection and repair**, not prevention: it makes reads
  deterministic and lets a retry pick a canonical run, but it cannot stop two
  hosts from each creating a run in the first place.

### Local locking

`_identity_lock` serializes concurrent submitters on one host with an
`fcntl` advisory lock under `$DAGSTER_HOME/.ictus-submission-locks`. This closes
the look-up/create race between local processes. On a host without `fcntl`
(e.g. Windows) submission fails closed with `LockUnavailableError` rather than
proceeding unserialized.

### Deployment assumption: a single writer host

This is the distinction between **supported multi-process, single-host
execution** and **unsupported multi-host writers**. The boundary assumes **one
writer host per `DAGSTER_HOME`**:

- multiple bridge processes on **one host** are supported; they are serialized
  by the `fcntl` lock;
- a single `dagster-daemon` serves the store (it may run on another host);
- no second host opens the same store as a writer.

Dagster does not enforce uniqueness on the bridge-owned identity/digest tags, so
the boundary cannot prevent two hosts from creating two runs for one identity.
Enabling multi-host writers requires coordination that Dagster's run history does
not provide — for example a unique constraint on the identity tag in shared
storage (rejecting the second insert), or an external lease / leader election in
front of submission. Until such a mechanism exists, the single-writer assumption
is an explicit operator contract, not an oversight.

## Submission retry vs semantic re-execution

Two different things look like "trying again" and must not be conflated:

| | Transport / submission retry (A) | Semantic `REEXECUTE` (B) |
| --- | --- | --- |
| Identity | same `intent_id` **and** same `intent_digest` | a new attempt identity |
| Meaning | the same semantic attempt, not yet acknowledged | a new authorized semantic attempt |
| Mechanism | recover/reconcile the existing Dagster run or its terminal result | Ictus emits a fresh intent after policy authorization |
| Duplicate execution | must never happen | is the point |
| Accounting | none; the same durable submission row is recovered | Tactus owns attempt + semantic-budget accounting |

`submit` implements only (A). Re-running work is (B), owned by Ictus policy and
Tactus attempt accounting; the submission boundary never turns a transport retry
into a new run.

### Durable replay in the Tactus integration

The replay trigger is durable, not in-memory. Tactus persists every submission
with a nullable Dagster receipt and exposes unreceipted rows for send or
reconciliation after restart (`pending_submissions()` for active attempts,
`reconciliation_submissions()` for closed ones, `attach_receipt()` to bind a
late receipt). A crash after send but before acknowledgement replays the *same*
submission identity and attaches the receipt; it cannot synthesize a replacement
run. This is specified in Tactus
[`EXECUTION_AND_RECOVERY.md`](https://github.com/Walsamer/tactus/blob/main/docs/architecture/EXECUTION_AND_RECOVERY.md)
("Crash safety and duplicate prevention") and covered by its process-crash tests.
Ictus therefore adds no reconciler of its own.

## Who owns recovery

Recovery responsibility is split deliberately, and Ictus adds no background
reconciler:

- **Ictus owns idempotent repair on retry.** If a submitter dies after recording
  a run but before enqueueing it, the run is left `NOT_STARTED`. The next
  submission of the *same identity* re-enqueues it and returns the original
  receipt; it never creates a second run.
- **The durable control plane owns the retry.** Recovery happens only when
  something re-submits the identity. In the Ictus/Tactus architecture that is
  Tactus, which owns WorkOrder lifecycle and semantic-attempt budgets and
  re-submits the same identity after a lost or failed attempt.
- **Dagster owns execution scheduling and run state**, but it does not retry
  submission of a run the bridge never enqueued.

Consequence: Ictus deliberately does **not** sweep stranded `NOT_STARTED` runs in
the background. A sweep would be a second reconciler with its own lifecycle
state, which the ownership boundary forbids. If no upstream component ever
retries the identity, the stranded run stays stranded; that is the documented
trade-off, and automatic reconciliation would require an explicit owner and
lifecycle.

## Known limitations

- **Multi-host writers are unsupported.** The `fcntl` lock is local to one host
  and one `DAGSTER_HOME`; Dagster does not enforce uniqueness on the bridge
  tags. See
  [Deployment assumption: a single writer host](#deployment-assumption-a-single-writer-host).
- **Stranded-run recovery is caller-driven.** Ictus repairs a `NOT_STARTED` run
  only when the same identity is submitted again; it runs no background sweep.
  See [Who owns recovery](#who-owns-recovery).
- **No bundled daemon supervision.** Ictus does not start or supervise the
  `dagster-daemon`; that is the operator's deployment responsibility.
- **The `ICTUS_DAGSTER_CODE_LOCATION` name must match the daemon's workspace.**
  A mismatch is rejected only when the daemon fails to resolve the run, not at
  submit time (the bridge has no workspace to check against).
- **In packaged installs the JSON schemas live in the repository `contracts/`
  directory** and are enforced by tests and the pure-Python validators; the
  bridge does not load schema files at runtime.

## Tests

- `tests/python/test_submission.py` — decision logic against a Dagster double
  (idempotency, conflict, crash recovery, duplicate ordering, fail-closed
  configuration). These are not proof of production compatibility.
- `tests/python/test_submission_integration.py` — a **real persistent**
  `DagsterInstance` (queued coordinator + default launcher). It exercises
  enqueueing, origin persistence, pending/terminal reconciliation, failure
  mapping, cancellation and process restart. The daemon hand-off is replayed
  in-process (see the module docstring).
- `tests/python/test_submission_daemon_smoke.py` — opt-in (`ICTUS_DAEMON_SMOKE=1`)
  live `dagster-daemon` + managed gRPC code-location smoke covering submission
  through a terminal result. Isolated from the hermetic suite.
- `tests/python/test_contracts_schemas.py` — validates example payloads and
  runtime validation against the versioned JSON schemas.
