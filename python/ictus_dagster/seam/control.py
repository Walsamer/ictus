"""The bounded execution control a runtime backend is handed.

A backend does three things through this object, and only these: it starts
processes (which are tracked in their own process group), it counts semantic
steps against the operator's budget, and it waits for a process under the
operator's wall-clock budget.  The control owns termination: when a budget runs
out it stops the process group for real and reports whether the group's death was
confirmed.

It carries no Dagster, control-plane, domain or runtime dependency.
"""

from __future__ import annotations

import subprocess
import time
from typing import Any, Callable, Mapping, Sequence

from ictus_dagster.seam.process import ProcessSupervisor


class AttemptHalted(RuntimeError):
    """A control primitive stopped the attempt; ``kind`` is its typed reason."""

    kind = "BACKEND_ERROR"


class AttemptAlreadyCancelled(AttemptHalted):
    """The attempt was cancelled before the backend asked for another process."""

    kind = "CANCEL_REQUESTED"


class StepBudgetExceeded(AttemptHalted):
    """The attempt asked for more steps than the operator's budget allows."""

    kind = "STEP_BUDGET_EXCEEDED"


class WallClockExceeded(AttemptHalted):
    """The attempt ran past the operator's wall-clock budget."""

    kind = "WALL_CLOCK_EXCEEDED"


class ExecutionControl:
    """Bounded, terminating control over one attempt's processes and steps."""

    def __init__(
        self,
        *,
        wall_clock_seconds: float,
        step_budget: int,
        supervisor: ProcessSupervisor | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._supervisor = ProcessSupervisor() if supervisor is None else supervisor
        self._wall_clock_seconds = wall_clock_seconds
        self._step_budget = step_budget
        self._clock = clock
        self._deadline = clock() + wall_clock_seconds
        self._steps = 0
        self._cancelled_reason: str | None = None
        self._termination: dict[str, Any] | None = None

    @property
    def steps_used(self) -> int:
        return self._steps

    @property
    def cancelled(self) -> bool:
        return self._cancelled_reason is not None

    @property
    def termination(self) -> dict[str, Any] | None:
        """The termination evidence document, once termination was requested."""
        return None if self._termination is None else dict(self._termination)

    def spawn(
        self,
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start a process in its own group, or refuse once cancelled."""
        if self._cancelled_reason is not None:
            raise AttemptAlreadyCancelled(
                f"attempt already cancelled: {self._cancelled_reason}"
            )
        return self._supervisor.spawn(argv, cwd=cwd, env=env)

    def step(self) -> int:
        """Count one semantic step against the operator's budget."""
        self._steps += 1
        if self._steps > self._step_budget:
            raise StepBudgetExceeded(
                f"attempt used more than its {self._step_budget} step budget"
            )
        return self._steps

    def wait(self, process: subprocess.Popen[bytes]) -> int:
        """Wait for ``process`` within the remaining wall clock.

        Past the deadline the process group is terminated and confirmed before
        :class:`WallClockExceeded` is raised, so the caller never has to guess
        whether the writer is still running.
        """
        remaining = self._deadline - self._clock()
        if remaining <= 0:
            self.cancel("wall_clock")
            raise WallClockExceeded(
                f"attempt exceeded its {self._wall_clock_seconds}s wall clock"
            )
        try:
            return process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            self.cancel("wall_clock")
            raise WallClockExceeded(
                f"attempt exceeded its {self._wall_clock_seconds}s wall clock"
            ) from None

    def cancel(self, reason: str) -> dict[str, Any]:
        """Terminate every tracked process group and return the confirmation."""
        if self._termination is None:
            self._cancelled_reason = reason
            self._termination = self._supervisor.terminate(reason)
        return dict(self._termination)

    def finish(self) -> dict[str, Any] | None:
        """End the attempt's remaining processes; report if any were left live.

        A backend that returns while a process group it started is still alive
        has broken the supervision contract, so the leftover group is
        terminated and the caller fails the attempt closed.
        """
        if not self._supervisor.live_groups():
            return None
        return self.cancel("leftover_process")
