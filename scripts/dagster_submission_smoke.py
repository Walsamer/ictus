#!/usr/bin/env python3
"""Disposable, live-daemon evidence for Ictus durable Dagster submission.

The runner owns its temporary DAGSTER_HOME and daemon process.  It exercises a
real code location and DefaultRunLauncher without using ``execute_in_process``
on the submission path, then writes the precise versions, receipt IDs and
terminal results as JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from dagster import DagsterInstance

from ictus_dagster import submission


ROOT = Path(__file__).resolve().parent.parent
CODE_LOCATION = "ictus"
REPOSITORY = "__repository__"


def _intent(intent_id: str, *, hold_seconds: float = 0.0) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "intent_id": intent_id,
        "capability": "demo.verify",
        "target": {"type": "task", "id": intent_id},
        "arguments": {"hold_seconds": hold_seconds} if hold_seconds else {},
        "requested_by": {"provider": "smoke.v1", "decision_id": intent_id},
    }


def _write_home(home: Path) -> None:
    (home / "dagster.yaml").write_text(
        "run_launcher:\n"
        "  module: dagster\n"
        "  class: DefaultRunLauncher\n"
        "run_coordinator:\n"
        "  module: dagster.core.run_coordinator\n"
        "  class: QueuedRunCoordinator\n"
        "  config:\n"
        "    max_concurrent_runs: 1\n"
        "    max_user_code_failure_retries: 0\n"
        "run_retries:\n"
        "  enabled: false\n"
        "  max_retries: 0\n"
        "telemetry:\n"
        "  enabled: false\n"
    )
    (home / "workspace.yaml").write_text(
        "load_from:\n"
        "  - python_module:\n"
        "      module_name: ictus_dagster.definitions\n"
        f"      location_name: {CODE_LOCATION}\n"
        f"      working_directory: {ROOT}\n"
        f"      executable_path: {sys.executable}\n"
    )


def _status(instance: DagsterInstance, run_id: str) -> str:
    run = instance.get_run_by_id(run_id)
    return submission._status_name(run) if run is not None else "MISSING"


def _wait_for_status(
    instance: DagsterInstance, run_id: str, expected: set[str], deadline: float
) -> str:
    observed = "MISSING"
    while time.monotonic() < deadline:
        observed = _status(instance, run_id)
        if observed in expected:
            return observed
        time.sleep(0.2)
    raise TimeoutError(
        f"run {run_id} did not reach one of {sorted(expected)}; last saw {observed}"
    )


def _wait_for_queued_while_active(
    instance: DagsterInstance,
    active_run_id: str,
    queued_run_id: str,
    deadline: float,
) -> str:
    """Prove the queued run is held back by the configured one-run limit.

    Returns the active run's status observed at the same instant the second run
    is QUEUED.  If the active run reaches a terminal state before that, the
    limit was not what kept the second run queued, so fail loudly instead of
    recording a misleading success.
    """
    active = "MISSING"
    queued = "MISSING"
    while time.monotonic() < deadline:
        active = _status(instance, active_run_id)
        queued = _status(instance, queued_run_id)
        if active in {"STARTING", "STARTED"} and queued == "QUEUED":
            return active
        if active in {"SUCCESS", "FAILURE", "CANCELED"}:
            raise RuntimeError(
                f"active run {active_run_id} finished ({active}) before "
                f"{queued_run_id} was observed QUEUED (saw {queued})"
            )
        time.sleep(0.2)
    raise TimeoutError(
        f"{queued_run_id} was not observed QUEUED while {active_run_id} was active "
        f"(active={active}, queued={queued})"
    )


def _terminal(
    instance: DagsterInstance, receipt: dict[str, Any], deadline: float
) -> dict[str, Any]:
    while time.monotonic() < deadline:
        status = submission.query_receipt(receipt, instance=instance)
        if status["state"] == "terminal":
            return status
        time.sleep(0.25)
    raise TimeoutError(f"receipt {receipt['receipt_id']} did not reach a terminal result")


def _daemon_command() -> list[str]:
    daemon = Path(sys.executable).with_name("dagster-daemon")
    if not daemon.exists():
        raise FileNotFoundError(f"dagster-daemon was not installed next to {sys.executable}")
    return [str(daemon)]


def _grpc_temp_root() -> str:
    """Pick a short base directory for the daemon's gRPC unix socket.

    gRPC rejects unix socket paths longer than ~103 characters, and the harness
    may point ``TMPDIR`` at a deep artifact/evidence directory.  Prefer ``/tmp``
    when it is short and usable, otherwise fall back to the platform temp dir.
    """
    candidates: list[str] = []
    if os.name != "nt":
        candidates.append("/tmp")
    candidates.append(tempfile.gettempdir())
    for candidate in candidates:
        if candidate and Path(candidate).is_dir() and len(candidate) <= 48:
            return candidate
    return tempfile.gettempdir()


def _session_kwargs() -> dict[str, Any]:
    """Run the daemon in its own session so cleanup is bounded to its tree."""
    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        return {}
    return {"start_new_session": True}


def _terminate_process_tree(process: subprocess.Popen) -> None:
    """Terminate the daemon and every descendant it spawned, with a bounded wait.

    ``dagster-daemon`` spawns the managed gRPC code server and run workers as
    ordinary children in its process group.  Signalling the whole group avoids
    leaking those descendants (and the SQLite store they hold) after the smoke.
    """
    if os.name == "nt":  # pragma: no cover - exercised on Windows only
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=30)
        return

    def _signal_group(sig: int) -> None:
        # The daemon is a session leader, so its pid is also its process-group id.
        try:
            os.killpg(process.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    _signal_group(signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        _signal_group(signal.SIGKILL)
        process.wait(timeout=30)
    # A daemon that exits early can still leave a child in the group.
    _signal_group(signal.SIGKILL)


def run(timeout: float) -> dict[str, Any]:
    import dagster

    with (
        tempfile.TemporaryDirectory(prefix="ictus-dagster-smoke-") as raw_home,
        tempfile.TemporaryDirectory(
            prefix="ictus-grpc-", dir=_grpc_temp_root()
        ) as raw_socket_home,
    ):
        home = Path(raw_home)
        socket_home = Path(raw_socket_home)
        _write_home(home)
        managed_names = (
            "DAGSTER_HOME",
            "TMPDIR",
            submission.CODE_LOCATION_ENV,
            submission.REPOSITORY_ENV,
        )
        previous = {name: os.environ.get(name) for name in managed_names}
        environment = {
            **os.environ,
            "DAGSTER_HOME": str(home),
            # The managed gRPC code server creates its unix socket under TMPDIR;
            # give it a short path so gRPC accepts it regardless of the harness.
            "TMPDIR": str(socket_home),
            submission.CODE_LOCATION_ENV: CODE_LOCATION,
            submission.REPOSITORY_ENV: REPOSITORY,
        }
        os.environ.update(environment)
        instance = DagsterInstance.get()
        # Force migrations before the daemon opens the brand-new SQLite store.
        instance.get_runs(limit=1)
        log_path = home / "daemon.log"
        log = log_path.open("w")
        daemon = subprocess.Popen(
            [
                *_daemon_command(),
                "run",
                "--log-level",
                "warning",
                "-w",
                str(home / "workspace.yaml"),
            ],
            cwd=ROOT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            **_session_kwargs(),
        )
        deadline = time.monotonic() + timeout
        try:
            # A first normal run proves the live workspace resolves the stored
            # remote origin through DefaultRunLauncher.
            first = submission.submit_intent(_intent("smoke:normal"), instance=instance)
            normal = _terminal(instance, first, deadline)
            if normal["result"]["status"] != "succeeded":
                raise RuntimeError(f"normal run failed: {json.dumps(normal)}")

            # The first queued run stays active (holds in its op) long enough
            # to prove the configured one-run limit keeps the second distinct
            # intent QUEUED while the first is still STARTED.
            limited_first = submission.submit_intent(
                _intent("smoke:limit:first", hold_seconds=8), instance=instance
            )
            _wait_for_status(
                instance,
                limited_first["receipt_id"],
                {"STARTING", "STARTED"},
                deadline,
            )
            limited_second = submission.submit_intent(
                _intent("smoke:limit:second", hold_seconds=0.1), instance=instance
            )
            active_status = _wait_for_queued_while_active(
                instance,
                limited_first["receipt_id"],
                limited_second["receipt_id"],
                deadline,
            )
            limited_results = [
                _terminal(instance, limited_first, deadline),
                _terminal(instance, limited_second, deadline),
            ]
            if any(result["result"]["status"] != "succeeded" for result in limited_results):
                raise RuntimeError(f"limited runs failed: {json.dumps(limited_results)}")

            # Hold another run active so cancellation is unambiguously a queue
            # request, then preserve the accepted acknowledgement and terminal
            # correlation separately in the evidence.
            blocker = submission.submit_intent(
                _intent("smoke:cancel:blocker", hold_seconds=8), instance=instance
            )
            _wait_for_status(instance, blocker["receipt_id"], {"STARTING", "STARTED"}, deadline)
            cancellable = submission.submit_intent(_intent("smoke:cancel:queued"), instance=instance)
            _wait_for_queued_while_active(
                instance, blocker["receipt_id"], cancellable["receipt_id"], deadline
            )
            cancellation = submission.cancel_receipt(cancellable, instance=instance)
            if not cancellation["cancellation"]["accepted"] or cancellation["state"] != "terminal":
                raise RuntimeError(f"cancellation was rejected: {json.dumps(cancellation)}")
            _terminal(instance, blocker, deadline)

            return {
                "schema_version": 1,
                "dagster_version": dagster.__version__,
                "python_version": sys.version,
                "code_location": CODE_LOCATION,
                "run_retries": "disabled",
                "normal": normal,
                "concurrency": {
                    "max_concurrent_runs": 1,
                    "second_status_while_first_active": "queued",
                    "first_status_when_second_queued": active_status.lower(),
                    "results": limited_results,
                },
                "cancellation": cancellation,
            }
        except Exception as error:
            log.flush()
            tail = log_path.read_text(errors="replace")[-12000:]
            raise RuntimeError(f"live daemon smoke failed: {error}\n{tail}") from error
        finally:
            _terminate_process_tree(daemon)
            log.close()
            instance.dispose()
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    evidence = run(args.timeout)
    encoded = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(encoded)
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
