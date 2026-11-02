"""Termination and budget control, asserted against real process groups.

Nothing here trusts a "please stop" flag: every termination case inspects the
reported evidence *and* asks the operating system whether the process group is
really gone (``os.kill(pid, 0)``).  Supervisors are built with a short grace
period so the ``SIGTERM`` -> ``SIGKILL`` escalation stays fast.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

import pytest

from ictus_dagster.seam.control import (
    AttemptAlreadyCancelled,
    AttemptHalted,
    ExecutionControl,
    StepBudgetExceeded,
    WallClockExceeded,
)
from ictus_dagster.seam.process import ProcessSupervisor, TerminationError, copy_report

#: Long enough for a cooperative child to die on SIGTERM, short enough that the
#: SIGKILL path still finishes the suite in a fraction of a second.
GRACE_SECONDS = 0.2

#: A child that honours the default SIGTERM action.
SLEEPER = "import time; time.sleep(300)"

#: How long a case may wait for a child to reach a known state.
READY_TIMEOUT_SECONDS = 10.0


def _supervisor(**kwargs: Any) -> ProcessSupervisor:
    return ProcessSupervisor(grace_seconds=GRACE_SECONDS, **kwargs)


def _deaf_sleeper(ready: Path) -> str:
    """A child that ignores SIGTERM and says so, so the test cannot race it."""
    return (
        "import pathlib, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"pathlib.Path({str(ready)!r}).write_text('ready')\n"
        "time.sleep(300)\n"
    )


def _await(predicate: Callable[[], bool], *, what: str) -> None:
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"waited {READY_TIMEOUT_SECONDS}s for {what}")


def _pid_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def _assert_pid_gone(pid: int) -> None:
    # The strongest available proof: the kernel no longer knows the pid.
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert _pid_gone(pid)


class _Clock:
    """A settable clock, so the wall-clock deadline can be crossed on purpose."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def _control(**kwargs: Any) -> ExecutionControl:
    kwargs.setdefault("wall_clock_seconds", 30.0)
    kwargs.setdefault("step_budget", 5)
    kwargs.setdefault("supervisor", _supervisor())
    return ExecutionControl(**kwargs)


# -- the supervisor: rejection and the empty case --------------------------


def test_spawn_rejects_a_bare_string() -> None:
    with pytest.raises(TerminationError, match="non-empty sequence"):
        _supervisor().spawn(f"{sys.executable} -c {SLEEPER!r}")


def test_spawn_rejects_an_empty_argv() -> None:
    with pytest.raises(TerminationError, match="non-empty sequence"):
        _supervisor().spawn([])


def test_terminate_without_children_reports_a_confirmed_empty_termination() -> None:
    report = _supervisor().terminate("operator")
    assert report == {
        "requested": True,
        "reason": "operator",
        "signals": [],
        "process_groups": [],
        "confirmed": True,
    }


# -- the supervisor: real termination ---------------------------------------


def test_a_child_honouring_sigterm_is_reported_and_confirmed() -> None:
    supervisor = _supervisor()
    process = supervisor.spawn([sys.executable, "-c", SLEEPER])
    _await(lambda: supervisor.live_groups() == [process.pid], what="the child to appear")

    report = supervisor.terminate("operator")

    assert report["signals"] == ["SIGTERM"]
    assert report["process_groups"] == [process.pid]
    assert report["confirmed"] is True
    assert supervisor.live_groups() == []
    _assert_pid_gone(process.pid)


def test_a_child_ignoring_sigterm_is_killed_and_confirmed(tmp_path: Path) -> None:
    supervisor = _supervisor()
    ready = tmp_path / "ready"
    process = supervisor.spawn([sys.executable, "-c", _deaf_sleeper(ready)])
    _await(ready.exists, what="the child to install its SIGTERM handler")

    report = supervisor.terminate("wall_clock")

    assert report["signals"] == ["SIGTERM", "SIGKILL"]
    assert report["confirmed"] is True
    assert supervisor.live_groups() == []
    # The group really died: the pid is gone, not merely unreaped.
    _assert_pid_gone(process.pid)


def test_terminate_is_idempotent() -> None:
    supervisor = _supervisor()
    process = supervisor.spawn([sys.executable, "-c", SLEEPER])
    _await(lambda: supervisor.live_groups() == [process.pid], what="the child to appear")

    first = supervisor.terminate("first")
    second = supervisor.terminate("second")

    assert second == first
    assert second is not first
    assert second["reason"] == "first"
    # The report is a shallow copy: rewriting a top-level key cannot reach the
    # stored confirmation ...
    second["reason"] = "rewritten"
    assert supervisor.terminate("third")["reason"] == "first"
    _assert_pid_gone(process.pid)


def test_every_termination_report_is_caller_owned() -> None:
    # ``copy_report`` hands out lists of their own, so a caller cannot rewrite
    # the one authoritative confirmation by mutating what it was given.
    supervisor = _supervisor()
    process = supervisor.spawn([sys.executable, "-c", SLEEPER])
    _await(lambda: supervisor.live_groups() == [process.pid], what="the child to appear")

    report = supervisor.terminate("first")
    report["signals"].append("SIGKILL")
    report["process_groups"].clear()

    stored = supervisor.terminate("second")
    assert stored["signals"] == ["SIGTERM"]
    assert stored["process_groups"] == [process.pid]
    _assert_pid_gone(process.pid)


def test_copy_report_hands_out_its_own_lists() -> None:
    original = {
        "requested": True,
        "reason": "operator",
        "signals": ["SIGTERM"],
        "process_groups": [1234],
        "confirmed": True,
    }

    copied = copy_report(original)

    assert copied == original
    assert copied is not original
    for name in ("signals", "process_groups"):
        assert copied[name] is not original[name]
    copied["signals"].append("SIGKILL")
    copied["process_groups"].append(5678)
    assert original["signals"] == ["SIGTERM"]
    assert original["process_groups"] == [1234]


def test_a_group_that_outlives_its_direct_child_is_still_terminated(
    tmp_path: Path,
) -> None:
    # A backend that returns after forking a worker keeps the group live even
    # though the tracked child exited: live_groups must still see it.
    supervisor = _supervisor()
    pid_file = tmp_path / "grandchild.pid"
    script = (
        "import pathlib, subprocess, sys\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(child.pid))\n"
    )
    process = supervisor.spawn([sys.executable, "-c", script])
    _await(pid_file.exists, what="the grandchild pid")
    process.wait(timeout=READY_TIMEOUT_SECONDS)
    assert process.poll() is not None
    grandchild = int(pid_file.read_text())

    assert supervisor.live_groups() == [process.pid]

    report = supervisor.terminate("leftover_process")

    assert report["signals"] == ["SIGTERM"]
    assert report["confirmed"] is True
    assert supervisor.live_groups() == []
    _assert_pid_gone(grandchild)


def test_spawn_applies_cwd_and_env(tmp_path: Path) -> None:
    supervisor = _supervisor()
    script = (
        "import os, pathlib\n"
        "pathlib.Path('marker.txt').write_text(os.environ['ICTUS_MARKER'])\n"
    )
    process = supervisor.spawn(
        [sys.executable, "-c", script],
        cwd=str(tmp_path),
        env={**os.environ, "ICTUS_MARKER": "seam"},
    )
    assert process.wait(timeout=READY_TIMEOUT_SECONDS) == 0
    assert (tmp_path / "marker.txt").read_text() == "seam"


# -- the control: typed halts ------------------------------------------------


@pytest.mark.parametrize(
    ("halt", "kind"),
    [
        (AttemptHalted, "BACKEND_ERROR"),
        (AttemptAlreadyCancelled, "CANCEL_REQUESTED"),
        (StepBudgetExceeded, "STEP_BUDGET_EXCEEDED"),
        (WallClockExceeded, "WALL_CLOCK_EXCEEDED"),
    ],
    ids=["backend-error", "cancel-requested", "step-budget", "wall-clock"],
)
def test_every_halt_carries_its_typed_kind(halt: type[AttemptHalted], kind: str) -> None:
    assert issubclass(halt, AttemptHalted)
    assert halt.kind == kind


def test_a_control_owns_a_supervisor_when_none_is_given() -> None:
    control = ExecutionControl(wall_clock_seconds=5.0, step_budget=2)
    assert control.steps_used == 0
    assert control.cancelled is False
    assert control.termination is None

    report = control.cancel("operator")

    assert report["requested"] is True
    assert report["signals"] == []
    assert report["confirmed"] is True
    assert control.cancelled is True
    assert control.termination == report


def test_step_budget_is_enforced_past_the_budget() -> None:
    control = _control(step_budget=2)
    assert control.step() == 1
    assert control.step() == 2
    with pytest.raises(StepBudgetExceeded) as raised:
        control.step()
    assert raised.value.kind == "STEP_BUDGET_EXCEEDED"
    assert isinstance(raised.value, AttemptHalted)
    # The over-budget step is still counted as used evidence.
    assert control.steps_used == 3


def test_spawn_after_cancel_raises_attempt_already_cancelled() -> None:
    control = _control()
    control.cancel("operator")
    with pytest.raises(AttemptAlreadyCancelled) as raised:
        control.spawn([sys.executable, "-c", SLEEPER])
    assert raised.value.kind == "CANCEL_REQUESTED"
    assert control.cancelled is True


def test_cancel_terminates_the_running_child() -> None:
    supervisor = _supervisor()
    control = _control(supervisor=supervisor)
    process = control.spawn([sys.executable, "-c", SLEEPER])
    _await(lambda: supervisor.live_groups() == [process.pid], what="the child to appear")

    report = control.cancel("operator")

    assert report["signals"] == ["SIGTERM"]
    assert report["confirmed"] is True
    assert supervisor.live_groups() == []
    _assert_pid_gone(process.pid)


def test_cancel_is_idempotent_and_reports_a_caller_owned_copy() -> None:
    control = _control()
    first = control.cancel("operator")
    second = control.cancel("something_else")

    assert second == first
    assert second["reason"] == "operator"
    reported = control.termination
    assert reported is not None
    assert control.termination is not reported
    reported["reason"] = "rewritten"
    reported["signals"].append("SIGKILL")
    assert control.termination == first
    assert control.cancel("third")["signals"] == []


# -- the control: waiting and the wall clock ---------------------------------


def test_wait_returns_the_exit_code_of_a_quick_child() -> None:
    control = _control()
    process = control.spawn([sys.executable, "-c", "raise SystemExit(3)"])
    assert control.wait(process) == 3
    assert control.termination is None


def test_wait_past_an_already_expired_deadline_terminates_first() -> None:
    clock = _Clock()
    supervisor = _supervisor()
    control = _control(clock=clock, wall_clock_seconds=10.0, supervisor=supervisor)
    process = control.spawn([sys.executable, "-c", SLEEPER])
    _await(lambda: supervisor.live_groups() == [process.pid], what="the child to appear")
    clock.now = 11.0  # the wall clock elapsed before wait() was called

    with pytest.raises(WallClockExceeded) as raised:
        control.wait(process)

    assert raised.value.kind == "WALL_CLOCK_EXCEEDED"
    termination = control.termination
    assert termination is not None
    assert termination["requested"] is True
    assert termination["confirmed"] is True
    assert termination["signals"] == ["SIGTERM"]
    assert supervisor.live_groups() == []
    _assert_pid_gone(process.pid)


def test_wait_that_times_out_terminates_first() -> None:
    supervisor = _supervisor()
    control = _control(wall_clock_seconds=0.25, supervisor=supervisor)
    process = control.spawn([sys.executable, "-c", SLEEPER])

    with pytest.raises(WallClockExceeded) as raised:
        control.wait(process)

    assert raised.value.kind == "WALL_CLOCK_EXCEEDED"
    termination = control.termination
    assert termination is not None
    assert termination["requested"] is True
    assert termination["confirmed"] is True
    assert termination["signals"] == ["SIGTERM"]
    assert supervisor.live_groups() == []
    _assert_pid_gone(process.pid)


# -- the control: finishing -----------------------------------------------


def test_finish_without_live_children_is_none() -> None:
    control = _control()
    assert control.finish() is None
    assert control.termination is None
    assert control.cancelled is False


def test_finish_terminates_a_child_the_backend_left_running() -> None:
    supervisor = _supervisor()
    control = _control(supervisor=supervisor)
    process = control.spawn([sys.executable, "-c", SLEEPER])
    _await(lambda: supervisor.live_groups() == [process.pid], what="the child to appear")

    report = control.finish()

    assert report is not None
    assert report["requested"] is True
    assert report["reason"] == "leftover_process"
    assert report["confirmed"] is True
    assert control.termination == report
    assert control.cancelled is True
    assert supervisor.live_groups() == []
    _assert_pid_gone(process.pid)
    # Nothing is left to clean up a second time.
    assert control.finish() is None


class _StuckChild:
    """A child that survives both signals: what an unkillable group looks like.

    ``SIGKILL`` cannot be ignored, so this only happens to a process stuck in
    uninterruptible sleep.  It exercises the two defensive branches around that:
    the bounded reap and the unconfirmed report.
    """

    pid = 2**22 + 7  # never used as a real process group: signalling is faked

    def __init__(self) -> None:
        self.waits = 0

    def poll(self) -> None:
        return None

    def wait(self, timeout: float | None = None) -> int:
        import subprocess

        self.waits += 1
        raise subprocess.TimeoutExpired(cmd="stuck", timeout=timeout or 0)


class _FakeSignals(ProcessSupervisor):
    """A supervisor that records signals instead of sending them."""

    def __init__(self, child: _StuckChild) -> None:
        super().__init__(grace_seconds=0)
        self._stuck = child
        self._children = [child]  # type: ignore[list-item]
        self.sent: list[int] = []

    def _signal_live_groups(self, signal_number: int) -> None:
        self.sent.append(signal_number)


def test_a_group_that_survives_sigkill_is_reported_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import signal

    from ictus_dagster.seam import process as process_module

    # The kill-confirmation window is what makes this test fast; the behaviour
    # (bounded reap, unconfirmed report) is the production one.
    monkeypatch.setattr(process_module, "KILL_CONFIRM_TIMEOUT_SECONDS", 0)
    child = _StuckChild()
    supervisor = _FakeSignals(child)

    report = supervisor.terminate("stuck")

    assert report["signals"] == ["SIGTERM", "SIGKILL"]
    assert supervisor.sent == [signal.SIGTERM, signal.SIGKILL]
    assert report["confirmed"] is False
    assert child.waits == 1  # the reap was bounded, not a blocking wait
