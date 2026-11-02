"""End-to-end evidence for the software routes' runtime seam, through real runs.

The contract this file pins, one clause per group of tests:

* **Fail-closed default.**  With no ``runtime_seam`` configured both software
  jobs fail in ``prepare`` with the *same* typed, non-retryable
  ``INTEGRATION_CONFLICT`` placeholder failure Ictus raised before the seam
  existed — compared as an exact document, so an unconfigured deployment can
  neither succeed nor quietly change category.
* **Configured happy path.**  With a backend named in operator-owned
  configuration, the ``prepare -> execute -> verify -> finalize`` skeleton Ictus
  owns really runs, the runtime is reached by name, and the structured evidence
  it returns is recorded on the finalize step — so it survives the run and
  reaches a validated ``ExecutionResult`` and the durable receipt.
* **Typed failures.**  Every failure kind the seam can raise is produced by a
  real run and carries its own typed category all the way to the run's consumer
  (``submission``), which is the fact a control plane reads instead of guessing
  from a log line.
* **Real termination.**  An expired wall clock and a leftover process group are
  terminated for real (``SIGTERM``, then ``SIGKILL`` for a group that ignores
  ``SIGTERM``), the recorded evidence says so, and the pids are gone.
* **Misconfiguration.**  A seam that cannot be used fails closed as
  ``CONFIGURATION`` / ``INTEGRATION_CONFLICT``.  The seam is deployment
  configuration: an intent that names a seam path -- nonsense or a valid file --
  changes nothing, and the route stays on its fail-closed default.

The runtime here is ``tests/python/stub_runtime_backend.py``: it is reached only
through configuration, so nothing in this file weakens the boundary that
``python/ictus_dagster/`` imports no runtime, domain adapter or control plane.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

import pytest
from dagster import DagsterInstance, ExecuteInProcessResult, reconstructable
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.execution.api import execute_run

import stub_runtime_backend as stub
from ictus_dagster import submission
from ictus_dagster.adapters.contracts import validate_execution_status
from ictus_dagster.adapters.result_mapping import execution_result_from_run
from ictus_dagster.adapters.workflow_registry import resolve_workflow
from ictus_dagster.seam.control import ExecutionControl, WallClockExceeded
from ictus_dagster.seam.failures import placeholder_failure, seam_failure
from ictus_dagster.seam.process import ProcessSupervisor

#: The two capabilities routed through the seam skeleton.
ROUTES = [
    pytest.param(
        "software.implement",
        "software_agent_job",
        "software_agent_settings",
        id="implement",
    ),
    pytest.param(
        "software.promote",
        "software_publish_job",
        "software_publish_settings",
        id="promote",
    ),
]

#: Both routes share one step skeleton (``ictus_dagster.seam.runtime``); the
#: stage-level tests below (typed failures, termination) run one route only,
#: while the fail-closed, happy-path and durable tests cover both.
IMPLEMENT = "software.implement"
IMPLEMENT_JOB = "software_agent_job"

BACKEND = "stub_runtime_backend:create_backend"
EXECUTABLE = "/usr/local/bin/stub-runtime"
VERSION_TOKEN = "stub-1.0.0"
PROBE_DETAIL = "stub probe ok"
DEFAULT_PROBE = {"ok": True, "detail": PROBE_DETAIL}

CODE_LOCATION = "ictus"
REPOSITORY = "__repository__"

#: Every failure kind the seam can raise, as a real run produces it:
#: behaviour, typed kind, typed category, failed stage, seam keeps evidence.
TYPED_FAILURES = [
    pytest.param(
        "scope_violation",
        "SCOPE_VIOLATION",
        "VERIFICATION_FAILURE",
        "verify",
        True,
        id="scope-violation",
    ),
    pytest.param(
        "budget",
        "STEP_BUDGET_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "verify",
        True,
        id="step-budget-exceeded",
    ),
    pytest.param(
        "duration_overrun",
        "WALL_CLOCK_EXCEEDED",
        "WORKER_TIMEOUT",
        "verify",
        True,
        id="reported-wall-clock",
    ),
    pytest.param(
        "verify_failed",
        "RUNTIME_VERIFICATION_FAILED",
        "VERIFICATION_FAILURE",
        "verify",
        True,
        id="runtime-verification-failed",
    ),
    pytest.param(
        "verify_raises",
        "BACKEND_ERROR",
        "PROCESS_CRASH",
        "verify",
        False,
        id="verify-raises",
    ),
    pytest.param(
        "fail", "PROCESS_EXIT", "PROCESS_CRASH", "execute", True, id="process-exit"
    ),
    pytest.param(
        "bad_evidence",
        "CONFIGURATION",
        "INTEGRATION_CONFLICT",
        "execute",
        False,
        id="invalid-evidence",
    ),
    pytest.param(
        "undefined_termination",
        "TERMINATION_UNCONFIRMED",
        "PROCESS_CRASH",
        "execute",
        True,
        id="termination-unconfirmed",
    ),
    pytest.param(
        "describe_raises",
        "BACKEND_ERROR",
        "PROCESS_CRASH",
        "prepare",
        False,
        id="describe-raises",
    ),
    pytest.param(
        "describe_mistyped",
        "CONFIGURATION",
        "INTEGRATION_CONFLICT",
        "prepare",
        True,
        id="describe-mistyped",
    ),
    pytest.param(
        "describe_nondict",
        "CONFIGURATION",
        "INTEGRATION_CONFLICT",
        "prepare",
        False,
        id="describe-nondict",
    ),
    pytest.param(
        "describe_probe_mistyped",
        "CONFIGURATION",
        "INTEGRATION_CONFLICT",
        "prepare",
        True,
        id="describe-probe-mistyped",
    ),
    pytest.param(
        "verify_mistyped",
        "CONFIGURATION",
        "INTEGRATION_CONFLICT",
        "verify",
        True,
        id="verify-mistyped",
    ),
    # No stub behaviour by this name: the backend raises, which is the generic
    # backend-exception branch of ``execute`` (it cancels, then reports).
    pytest.param(
        "unimplemented_behavior",
        "BACKEND_ERROR",
        "PROCESS_CRASH",
        "execute",
        True,
        id="backend-raises",
    ),
    pytest.param(
        "cancel", "CANCEL_REQUESTED", "UNKNOWN", "execute", True, id="cancel-requested"
    ),
]

#: Ways an operator-owned seam configuration can be unusable, each with the
#: phrase the resulting ``CONFIGURATION`` failure must carry.
CONFIG_BREAKAGES = [
    pytest.param("missing-file", "cannot be read", id="missing-file"),
    pytest.param("unknown-field", "unknown field(s): unexpected", id="unknown-field"),
    pytest.param(
        "schema-version",
        "unsupported runtime seam config schema_version",
        id="schema-version",
    ),
    pytest.param("backend-module", "cannot be imported", id="backend-module"),
    pytest.param("backend-attribute", "has no attribute", id="backend-attribute"),
    pytest.param("backend-not-callable", "cannot be constructed", id="backend-not-callable"),
    pytest.param(
        "backend-not-a-port", "does not implement the seam port", id="backend-not-a-port"
    ),
]

#: Values an intent may put in ``runtime_seam`` that must change nothing.
NON_STRING_SEAMS = [
    pytest.param(None, id="null"),
    pytest.param(42, id="number"),
    pytest.param("", id="empty"),
    pytest.param("   ", id="blank"),
    pytest.param(["seam.json"], id="list"),
    pytest.param({"path": "seam.json"}, id="object"),
]


def _prefix(job_name: str) -> str:
    """``software_agent_job`` -> ``software_agent`` (the step-key prefix)."""
    return job_name.removesuffix("_job")


def _descriptor(**overrides: Any) -> dict[str, Any]:
    document = {
        "executable": EXECUTABLE,
        "version_token": VERSION_TOKEN,
        "probe": dict(DEFAULT_PROBE),
        "behavior": "success",
    }
    document.update(overrides)
    return document


def _write_config(tmp_path: Path, *, document: dict[str, Any]) -> Path:
    path = tmp_path / "runtime-seam.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _config_document(
    descriptor: Path,
    *,
    backend: str = BACKEND,
    write_scope: tuple[str, ...] = ("src",),
    wall_clock_seconds: float = 5.0,
    step_budget: int = 5,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "backend": backend,
        "workspace": {"descriptor": str(descriptor)},
        "bounds": {
            "write_scope": list(write_scope),
            "wall_clock_seconds": wall_clock_seconds,
            "step_budget": step_budget,
        },
    }


@pytest.fixture
def seam(tmp_path: Path) -> Callable[..., Path]:
    """Write a workspace descriptor plus the seam config that names the stub."""

    def _seam(
        behavior: str = "success",
        *,
        backend: str = BACKEND,
        write_scope: tuple[str, ...] = ("src",),
        wall_clock_seconds: float = 5.0,
        step_budget: int = 5,
        descriptor: dict[str, Any] | None = None,
    ) -> Path:
        descriptor_path = tmp_path / "workspace-descriptor.json"
        document = _descriptor(behavior=behavior)
        if descriptor is not None:
            document.update(descriptor)
        descriptor_path.write_text(json.dumps(document), encoding="utf-8")
        return _write_config(
            tmp_path,
            document=_config_document(
                descriptor_path,
                backend=backend,
                write_scope=write_scope,
                wall_clock_seconds=wall_clock_seconds,
                step_budget=step_budget,
            ),
        )

    return _seam


@pytest.fixture
def instance() -> Any:
    ephemeral = DagsterInstance.ephemeral()
    try:
        yield ephemeral
    finally:
        ephemeral.dispose()


def _run(
    instance: DagsterInstance,
    capability: str,
    *,
    seam_config: Path | None = None,
    arguments: dict[str, Any] | None = None,
) -> ExecuteInProcessResult:
    """Run the routed job the way a configured code location would.

    The seam is deployment configuration: it arrives as the resource's own
    ``runtime_seam`` (what a code location sets), never through intent
    arguments.
    """
    spec = resolve_workflow(capability)
    settings = spec.settings_from_arguments(dict(arguments or {}))
    if seam_config is not None:
        settings = {**settings, "runtime_seam": str(seam_config)}
    return spec.job.execute_in_process(
        resources={spec.resource_key: spec.resource_cls(**settings)},
        instance=instance,
        raise_on_error=False,
    )


def _intent(capability: str, *, intent_id: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "intent_id": intent_id,
        "capability": capability,
        "target": {"type": "task", "id": "task-seam"},
        "arguments": {},
        "requested_by": {"provider": "rules.v1", "decision_id": "decision-seam"},
    }


def _failure(instance: DagsterInstance, run_id: str) -> Any:
    """The user ``Failure`` data of the run's failed step."""
    for record in instance.all_logs(run_id):
        event = getattr(record, "dagster_event", None)
        if getattr(event, "event_type_value", None) != "STEP_FAILURE":
            continue
        failure = getattr(event.event_specific_data, "user_failure_data", None)
        if failure is not None:
            return failure
    raise AssertionError(f"run {run_id} reports no user Failure metadata")


def _metadata(document: Any) -> dict[str, Any]:
    """Compare Dagster metadata as a plain document, exactly."""
    return {key: getattr(value, "value", value) for key, value in document.items()}


def _retry_events(instance: DagsterInstance, run_id: str) -> list[str]:
    return [
        str(getattr(getattr(record, "dagster_event", None), "event_type_value", None))
        for record in instance.all_logs(run_id)
        if getattr(getattr(record, "dagster_event", None), "event_type_value", None)
        == "STEP_UP_FOR_RETRY"
    ]


def _expected_digest() -> str:
    return "sha256:" + hashlib.sha256(stub.PATCH_TEXT.encode("utf-8")).hexdigest()


def _assert_pids_gone(pids: list[int]) -> None:
    assert pids, "a terminated attempt must report the process groups it stopped"
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


@pytest.fixture
def persistent_instance(monkeypatch: Any, tmp_path: Path) -> Any:
    """The durable-submission instance pattern used by ``test_software_routing``."""
    (tmp_path / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setenv(submission.CODE_LOCATION_ENV, CODE_LOCATION)
    monkeypatch.setenv(submission.REPOSITORY_ENV, REPOSITORY)
    durable = DagsterInstance.get()
    try:
        yield durable
    finally:
        durable.dispose()


def _execute_recorded_run(instance: DagsterInstance, run_id: str, factory: Any) -> Any:
    run = instance.get_run_by_id(run_id)
    instance.report_dagster_event(
        DagsterEvent(
            event_type_value=DagsterEventType.RUN_STARTING.value,
            job_name=run.job_name,
        ),
        run_id=run_id,
    )
    return execute_run(
        reconstructable(factory),
        instance.get_run_by_id(run_id),
        instance,
        raise_on_error=False,
    )


@pytest.mark.parametrize(("capability", "job_name", "_resource_key"), ROUTES)
def test_unconfigured_route_fails_closed_with_the_placeholder_failure(
    instance: DagsterInstance,
    capability: str,
    job_name: str,
    _resource_key: str,
) -> None:
    """No seam configured: the exact, unchanged, non-retryable failure."""
    result = _run(instance, capability)
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(job_name)}_prepare_op"}

    failure = _failure(instance, result.run_id)
    assert failure.description == (
        f"{job_name} is a fail-closed placeholder: "
        f"'{capability}' has no implementation in this Ictus code location"
    )
    # Exact document equality: no extra key may appear, ``ictus.failure_kind``
    # included, and the category/placeholder/retryable facts may not drift.
    assert _metadata(failure.metadata) == {
        "ictus.capability": capability,
        "ictus.job": job_name,
        "ictus.failure_category": "INTEGRATION_CONFLICT",
        "ictus.placeholder": True,
        "ictus.retryable": False,
    }
    assert _retry_events(instance, result.run_id) == []
    assert submission.seam_evidence_from_run(instance, result.run_id) is None


def test_seam_failures_are_non_retryable_by_construction() -> None:
    """``allow_retries=False`` is on the raised ``Failure`` itself.

    Dagster's persisted ``UserFailureData`` drops the flag, so the run-level
    tests assert the persisted ``ictus.retryable`` fact and the absence of retry
    events instead; here the flag the retry policy would read is pinned.
    """
    assert (
        placeholder_failure(job_name="job", capability="capability").allow_retries
        is False
    )
    assert (
        seam_failure(
            job_name="job",
            capability="capability",
            failure_kind="CONFIGURATION",
            description="description",
        ).allow_retries
        is False
    )


@pytest.mark.parametrize(("capability", "job_name", "resource_key"), ROUTES)
def test_configured_route_records_the_structured_evidence(
    instance: DagsterInstance,
    seam: Callable[..., Path],
    capability: str,
    job_name: str,
    resource_key: str,
) -> None:
    """A configured run really executes through the stub and records the facts."""
    result = _run(instance, capability, seam_config=seam("success"))
    assert result.success is True
    assert result.get_failed_step_keys() == set()

    summary = submission.seam_evidence_from_run(instance, result.run_id)
    assert summary is not None
    # What the finalize step recorded is what the step emitted.
    assert summary == result.output_for_node(f"{_prefix(job_name)}_finalize_op")

    assert summary["capability"] == capability
    assert summary["job"] == job_name
    assert summary["verified"] is True
    assert summary["outcome"] == "succeeded"
    assert summary["changed_paths"] == ["src/stub.py"]
    assert summary["patch_digest"] == _expected_digest()
    assert summary["patch_bytes"] == len(stub.PATCH_TEXT.encode("utf-8"))
    assert summary["patch_bytes"] > 0
    assert summary["steps_used"] == 2
    assert summary["exit_status"] == {"code": 0, "signal": None}
    assert summary["termination"] == {"requested": False, "signals": [], "confirmed": True}
    assert summary["runtime"] == {
        "executable": EXECUTABLE,
        "version_token": VERSION_TOKEN,
        "probe": dict(DEFAULT_PROBE),
    }
    assert summary["verification"] == {
        "verified": True,
        "detail": "stub post-conditions hold",
    }
    # The route is still addressed by the same resource key as the registry.
    assert resolve_workflow(capability).resource_key == resource_key


@pytest.mark.parametrize(
    ("behavior", "failure_kind", "category", "stage", "keeps_evidence"), TYPED_FAILURES
)
def test_every_typed_failure_is_produced_by_a_real_run(
    instance: DagsterInstance,
    seam: Callable[..., Path],
    behavior: str,
    failure_kind: str,
    category: str,
    stage: str,
    keeps_evidence: bool,
) -> None:
    """One real run per typed kind: step, kind, category and evidence survive."""
    result = _run(instance, IMPLEMENT, seam_config=seam(behavior))
    run_id = result.run_id
    assert result.success is False

    failure = _failure(instance, run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_kind"] == failure_kind
    assert metadata["ictus.failure_category"] == category
    assert metadata["ictus.capability"] == IMPLEMENT
    assert metadata["ictus.job"] == IMPLEMENT_JOB
    assert metadata["ictus.placeholder"] is False
    assert metadata["ictus.retryable"] is False
    assert _retry_events(instance, run_id) == []
    if keeps_evidence:
        assert isinstance(json.loads(metadata["ictus.evidence"]), dict)
    else:
        assert "ictus.evidence" not in metadata
    assert submission.seam_evidence_from_run(instance, run_id) is None

    # The consumer of a terminal run (``submission``) reports the typed facts:
    # the typed category, not the generic per-step default.
    failed_step, reported_category = submission.failed_step_from_run(instance, run_id)
    assert failed_step == f"{_prefix(IMPLEMENT_JOB)}_{stage}_op"
    assert reported_category == category
    mapped = execution_result_from_run(
        execution_id=run_id,
        intent_id="attempt:seam",
        success=False,
        run_status="FAILURE",
        failed_step=failed_step,
        failure_category=reported_category,
    )
    assert mapped["status"] == "failed"
    assert mapped["observation"]["category"] == category
    # The failure did not reach ``finalize``, so the result carries no seam
    # patch/exit evidence: a failed attempt must not look like it produced one.
    assert [item for item in mapped["evidence"] if item["kind"].startswith("runtime_")] == []


def test_a_workspace_probe_that_says_no_fails_closed(
    instance: DagsterInstance, seam: Callable[..., Path]
) -> None:
    """The runtime's own probe rejects the workspace, so ``prepare`` refuses."""
    config = seam(
        "success",
        descriptor={"probe": {"ok": False, "detail": "stub probe rejected it"}},
    )
    result = _run(instance, IMPLEMENT, seam_config=config)
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_prepare_op"}

    failure = _failure(instance, result.run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_kind"] == "WORKSPACE_PROBE_FAILED"
    assert metadata["ictus.failure_category"] == "INTEGRATION_CONFLICT"
    assert metadata["ictus.retryable"] is False
    assert failure.description == "workspace probe failed: stub probe rejected it"
    report = json.loads(metadata["ictus.evidence"])
    assert report["probe"] == {"ok": False, "detail": "stub probe rejected it"}


def test_a_halted_attempt_is_terminated_and_confirmed(
    instance: DagsterInstance, seam: Callable[..., Path]
) -> None:
    """A control halt inside ``execute`` still records a confirmed termination.

    ``steps_overrun`` spends more steps than the budget allows, so the control
    raises before the backend started anything: the seam must still ask the
    control to stop the attempt and record the confirmation, otherwise a caller
    cannot tell a halted attempt from a silently abandoned one.
    """
    result = _run(
        instance, IMPLEMENT, seam_config=seam("steps_overrun", step_budget=2)
    )
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_execute_op"}

    failure = _failure(instance, result.run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_kind"] == "STEP_BUDGET_EXCEEDED"
    assert metadata["ictus.failure_category"] == "RESOURCE_EXHAUSTED"
    termination = json.loads(metadata["ictus.evidence"])
    assert termination["requested"] is True
    assert termination["confirmed"] is True
    assert termination["reason"] == "halted:step_budget_exceeded"
    assert termination["signals"] == []
    assert termination["process_groups"] == []


def test_an_expired_wall_clock_terminates_the_process_group(
    instance: DagsterInstance, seam: Callable[..., Path]
) -> None:
    """A bounded attempt that outlives its wall clock is stopped for real."""
    config = seam("sleep", wall_clock_seconds=0.3)
    result = _run(instance, IMPLEMENT, seam_config=config)
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_execute_op"}

    failure = _failure(instance, result.run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_kind"] == "WALL_CLOCK_EXCEEDED"
    assert metadata["ictus.failure_category"] == "WORKER_TIMEOUT"
    termination = json.loads(metadata["ictus.evidence"])
    assert termination["requested"] is True
    assert termination["confirmed"] is True
    assert termination["reason"] == "wall_clock"
    assert termination["signals"]
    assert "SIGTERM" in termination["signals"]
    _assert_pids_gone(termination["process_groups"])


def test_a_leftover_process_group_is_terminated(
    instance: DagsterInstance, seam: Callable[..., Path]
) -> None:
    """A backend that returns with a live group broke supervision; it is stopped."""
    result = _run(instance, IMPLEMENT, seam_config=seam("leftover"))
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_execute_op"}

    failure = _failure(instance, result.run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_kind"] == "SUPERVISION_VIOLATION"
    assert metadata["ictus.failure_category"] == "PROCESS_CRASH"
    termination = json.loads(metadata["ictus.evidence"])
    assert termination["requested"] is True
    assert termination["confirmed"] is True
    assert termination["reason"] == "leftover_process"
    assert termination["signals"]
    _assert_pids_gone(termination["process_groups"])


def test_an_interpreter_abort_still_terminates_the_process_group(
    instance: DagsterInstance, seam: Callable[..., Path], tmp_path: Path
) -> None:
    """A ``BaseException`` from the backend must not leave a live writer.

    ``KeyboardInterrupt`` is not an ``Exception``: the seam cancels the attempt
    before letting it propagate, so the process group the backend started is
    gone and the run records a failure.
    """
    config = seam("interrupt")
    result = _run(instance, IMPLEMENT, seam_config=config)
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_execute_op"}

    pid_file = tmp_path / "workspace-descriptor.pid"
    assert pid_file.exists(), "the stub never started its process"
    pid = int(pid_file.read_text(encoding="utf-8"))
    _assert_pids_gone([pid])


def test_a_process_that_ignores_sigterm_is_killed_after_the_grace() -> None:
    """``SIGTERM`` then ``SIGKILL``, confirmed — exercised at the control level.

    Asserted here rather than through a full run on purpose: the supervisor's
    default grace is 5s, so a run whose process ignores ``SIGTERM`` would cost
    5+ seconds of wall clock for the same evidence.  A shortened grace keeps the
    behaviour identical (the group ignores ``SIGTERM``, gets ``SIGKILL``) and
    the test fast.
    """
    supervisor = ProcessSupervisor(grace_seconds=0.1)
    control = ExecutionControl(
        wall_clock_seconds=0.2, step_budget=5, supervisor=supervisor
    )
    process = control.spawn(
        [
            sys.executable,
            "-c",
            "import signal, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "time.sleep(300)",
        ]
    )
    with pytest.raises(WallClockExceeded, match="wall clock"):
        control.wait(process)

    termination = control.termination
    assert termination == {
        "requested": True,
        "reason": "wall_clock",
        "signals": ["SIGTERM", "SIGKILL"],
        "process_groups": [process.pid],
        "confirmed": True,
    }
    _assert_pids_gone(termination["process_groups"])


@pytest.mark.parametrize(("breakage", "phrase"), CONFIG_BREAKAGES)
def test_a_misconfigured_seam_fails_closed(
    instance: DagsterInstance,
    tmp_path: Path,
    seam: Callable[..., Path],
    breakage: str,
    phrase: str,
) -> None:
    """An unusable seam configuration cannot run and cannot succeed."""
    if breakage == "missing-file":
        config = tmp_path / "absent-runtime-seam.json"
    else:
        config = seam("success")
        document = json.loads(config.read_text(encoding="utf-8"))
        if breakage == "unknown-field":
            document["unexpected"] = True
        elif breakage == "schema-version":
            document["schema_version"] = 2
        elif breakage == "backend-module":
            document["backend"] = "no_such_backend_module:create_backend"
        elif breakage == "backend-attribute":
            document["backend"] = f"{stub.__name__}:no_such_backend"
        elif breakage == "backend-not-callable":
            document["backend"] = f"{stub.__name__}:PATCH_TEXT"
        elif breakage == "backend-not-a-port":
            document["backend"] = f"{stub.__name__}:not_a_port"
        else:
            raise AssertionError(f"unknown breakage {breakage}")
        config = _write_config(tmp_path, document=document)

    result = _run(instance, IMPLEMENT, seam_config=config)
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_prepare_op"}

    failure = _failure(instance, result.run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_kind"] == "CONFIGURATION"
    assert metadata["ictus.failure_category"] == "INTEGRATION_CONFLICT"
    assert metadata["ictus.placeholder"] is False
    assert metadata["ictus.retryable"] is False
    assert phrase in failure.description
    assert submission.seam_evidence_from_run(instance, result.run_id) is None


@pytest.mark.parametrize("value", NON_STRING_SEAMS)
def test_an_intent_argument_never_selects_the_seam(
    instance: DagsterInstance, value: Any
) -> None:
    """The seam is deployment configuration; intent data cannot choose it.

    An intent that names a seam path -- nonsense or a real file -- changes
    nothing: the mapper keeps its legacy shape, and the run stays on the
    fail-closed default unless the deployment configured the resource.
    """
    spec = resolve_workflow(IMPLEMENT)
    assert spec.settings_from_arguments({"runtime_seam": value}) == {
        "capability": IMPLEMENT,
        "placeholder": True,
    }

    result = _run(instance, IMPLEMENT, arguments={"runtime_seam": value})
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_prepare_op"}
    failure = _failure(instance, result.run_id)
    metadata = _metadata(failure.metadata)
    assert metadata["ictus.failure_category"] == "INTEGRATION_CONFLICT"
    assert metadata["ictus.placeholder"] is True
    assert "ictus.failure_kind" not in metadata


def test_an_intent_cannot_point_at_a_real_seam_config(
    instance: DagsterInstance, seam: Callable[..., Path]
) -> None:
    """A perfectly valid seam path in an intent is still ignored."""
    config = seam("success")
    spec = resolve_workflow(IMPLEMENT)
    assert spec.settings_from_arguments({"runtime_seam": str(config)}) == {
        "capability": IMPLEMENT,
        "placeholder": True,
    }
    result = _run(instance, IMPLEMENT, arguments={"runtime_seam": str(config)})
    assert result.success is False
    assert result.get_failed_step_keys() == {f"{_prefix(IMPLEMENT_JOB)}_prepare_op"}
    assert submission.seam_evidence_from_run(instance, result.run_id) is None


@pytest.mark.parametrize(
    ("capability", "job_name", "resource_key"),
    [ROUTES[0]],
)
def test_durable_submission_carries_the_seam(
    persistent_instance: DagsterInstance,
    seam: Callable[..., Path],
    capability: str,
    job_name: str,
    resource_key: str,
) -> None:
    """A submitted intent cannot configure the seam; a deployment still can.

    Two runs on one persistent instance, because the duration of the test would
    double otherwise and both capabilities address the same submission path:

    * an intent submitted through ``submit_intent`` carries no seam, so its
      recorded run stays fail-closed even when the intent names a valid seam
      path in its arguments;
    * a run recorded by the *deployment* -- the same job, identity tags and
      resource key, with the seam in its resource config -- executes through the
      backend and its evidence reaches the durable receipt.
    """
    config = seam("success")
    intent = _intent(capability, intent_id="attempt:durable-seam")
    intent["arguments"] = {"runtime_seam": str(config)}
    receipt = submission.submit_intent(intent, instance=persistent_instance)

    run = persistent_instance.get_run_by_id(receipt["receipt_id"])
    assert run.job_name == job_name
    assert "runtime_seam" not in run.run_config["resources"][resource_key]["config"]

    spec = resolve_workflow(capability)
    refused = _execute_recorded_run(
        persistent_instance, receipt["receipt_id"], spec.job_factory
    )
    assert refused.success is False
    refused_status = submission.query_receipt(receipt, instance=persistent_instance)
    assert refused_status["state"] == "terminal"
    assert (
        refused_status["result"]["observation"]["category"] == "INTEGRATION_CONFLICT"
    )
    assert submission.seam_evidence_from_run(
        persistent_instance, receipt["receipt_id"]
    ) is None

    deployed = persistent_instance.create_run_for_job(
        spec.job,
        run_config={
            "resources": {
                resource_key: {
                    "config": {
                        **spec.settings_from_arguments({}),
                        "runtime_seam": str(config),
                    }
                }
            }
        },
        tags=dict(run.tags),
    )
    executed = _execute_recorded_run(
        persistent_instance, deployed.run_id, spec.job_factory
    )
    assert executed.success is True
    summary = submission.seam_evidence_from_run(persistent_instance, deployed.run_id)
    assert summary is not None
    assert summary["changed_paths"] == ["src/stub.py"]
    assert summary["patch_digest"] == _expected_digest()

    status = submission.query_receipt(deployed.run_id, instance=persistent_instance)
    assert validate_execution_status(status) is status
    assert status["state"] == "terminal"
    result = status["result"]
    assert result["status"] == "succeeded"
    assert result["observation"]["category"] == "SUCCESS"
    evidence = {item["kind"]: item for item in result["evidence"]}
    assert {"dagster_run", "dagster_event_log", "runtime_patch", "runtime_exit"} <= set(
        evidence
    )
    assert evidence["runtime_patch"]["sha256"] == _expected_digest().removeprefix(
        "sha256:"
    )
    assert evidence["runtime_exit"]["note"].startswith("outcome=succeeded; exit_code=0;")
