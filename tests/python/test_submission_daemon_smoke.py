"""Optional live-daemon smoke test, isolated from the hermetic suite.

It starts a real ``dagster-daemon`` with a managed gRPC code location, submits
one intent through the durable boundary, and waits for the daemon to launch the
run to a terminal result.  It is skipped unless ``ICTUS_DAEMON_SMOKE=1`` so the
hermetic suite never depends on daemon scheduling, subprocess startup or
wall-clock timing.

    ICTUS_DAEMON_SMOKE=1 uv run --frozen pytest tests/python/test_submission_daemon_smoke.py

This is deliberately *not* part of the coverage gate's required evidence; use it
as a manual deployment smoke check.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from dagster import DagsterInstance, DagsterRunStatus

from conftest import REPO_ROOT
from ictus_dagster import submission

pytestmark = pytest.mark.skipif(
    os.environ.get("ICTUS_DAEMON_SMOKE") != "1",
    reason="live daemon smoke test; set ICTUS_DAEMON_SMOKE=1 to enable",
)

CODE_LOCATION = "ictus"
REPOSITORY = "__repository__"
DEADLINE_SECONDS = 240
POLL_SECONDS = 3


def _write_home(home: Path) -> None:
    # No storage override: DAGSTER_HOME-local storage keeps this test isolated
    # from the repo and from other tests (a cwd-relative base_dir would not).
    (home / "dagster.yaml").write_text(
        "run_launcher:\n"
        "  module: dagster\n"
        "  class: DefaultRunLauncher\n"
        "run_coordinator:\n"
        "  module: dagster.core.run_coordinator\n"
        "  class: QueuedRunCoordinator\n"
        "  config:\n"
        "    max_concurrent_runs: 1\n"
        "telemetry:\n"
        "  enabled: false\n"
    )
    # The live smoke uses absolute paths so the daemon's managed gRPC code
    # server loads this checkout regardless of its working directory.
    (home / "workspace.yaml").write_text(
        "load_from:\n"
        "  - python_module:\n"
        "      module_name: ictus_dagster.definitions\n"
        f"      location_name: {CODE_LOCATION}\n"
        f"      working_directory: {REPO_ROOT}\n"
        f"      executable_path: {sys.executable}\n"
    )


def _wait_for_daemon_ready(log_path: Path, timeout: float = 30.0) -> None:
    """Best-effort wait for the daemon to finish starting before we enqueue.

    Reduces (does not eliminate) the race where a freshly started daemon
    dequeues a run before its managed gRPC code location is ready.  The
    terminal assertion still fails loudly if the run does not succeed.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log_path.exists() and "Instance is configured with the following daemons" in (
            log_path.read_text(errors="replace")
        ):
            return
        time.sleep(0.5)


def _payload() -> dict:
    return {
        "schema_version": 1,
        "intent_id": "attempt:daemon-smoke",
        "capability": "demo.verify",
        "target": {"type": "task", "id": "task-1"},
        "arguments": {},
        "requested_by": {"provider": "rules.v1", "decision_id": "decision-1"},
    }


def test_daemon_launches_a_queued_intent_to_terminal(tmp_path, monkeypatch) -> None:
    _write_home(tmp_path)
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setenv(submission.CODE_LOCATION_ENV, CODE_LOCATION)
    monkeypatch.setenv(submission.REPOSITORY_ENV, REPOSITORY)

    # Initialize the instance/storage before the daemon starts.  SQLite
    # migrations are not safe against two processes initializing a brand-new
    # store at once, and both the test and the daemon open this DAGSTER_HOME.
    instance = DagsterInstance.get()
    instance.get_runs(limit=1)

    daemon = Path(sys.executable).parent / "dagster-daemon"
    assert daemon.exists(), f"dagster-daemon not found at {daemon}"
    log_path = tmp_path / "daemon.log"
    log = log_path.open("w")
    process = subprocess.Popen(
        [
            str(daemon),
            "run",
            "--log-level",
            "info",
            "-w",
            str(tmp_path / "workspace.yaml"),
        ],
        cwd=str(REPO_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_for_daemon_ready(log_path)
        receipt = submission.submit_intent(_payload(), instance=instance)
        run = instance.get_run_by_id(receipt["receipt_id"])
        log.flush()
        assert run is not None and run.status == DagsterRunStatus.QUEUED, (
            f"expected QUEUED, saw {getattr(run, 'status', None)}\n"
            + log_path.read_text()[-4000:]
        )

        deadline = time.time() + DEADLINE_SECONDS
        status = submission.query_receipt(receipt, instance=instance)
        while status["state"] != "terminal" and time.time() < deadline:
            time.sleep(POLL_SECONDS)
            status = submission.query_receipt(receipt, instance=instance)

        log.flush()
        assert status["state"] == "terminal", (
            "daemon did not reach a terminal run state\n"
            + log_path.read_text()[-4000:]
        )
        assert status["result"]["status"] == "succeeded", (
            json.dumps(status) + "\n" + log_path.read_text()[-4000:]
        )
        assert status["result"]["observation"]["category"] == "SUCCESS"
    finally:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive cleanup
            process.kill()
        log.close()
        instance.dispose()
