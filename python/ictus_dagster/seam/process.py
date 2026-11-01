"""Real process-group termination for the runtime seam.

A supervisor that holds writer exclusion on a workspace refuses to release it
until the writer's death is *confirmed*, so a cooperative "please stop" flag is
not enough.  :class:`ProcessSupervisor` starts every runtime process in its own
process group, terminates the whole group (``SIGTERM``, then ``SIGKILL`` for
survivors), reaps it and confirms the group is gone.

This module is POSIX-oriented (``start_new_session`` + ``killpg``) on purpose:
the same host-scoped locking assumptions as ``submission.py``.  It carries no
Dagster, control-plane, domain or runtime dependency.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from typing import Any, Callable, Mapping, Sequence

#: How long a group gets to honour ``SIGTERM`` before it is killed.
DEFAULT_GRACE_SECONDS = 5.0
#: How long to wait for a group to disappear after ``SIGKILL``.  ``SIGKILL``
#: cannot be ignored, so this only bounds how long we poll before reporting an
#: unconfirmed termination.
KILL_CONFIRM_TIMEOUT_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.05


class TerminationError(RuntimeError):
    """Raised when a process group cannot be started or terminated safely."""


def _group_alive(pgid: int) -> bool:
    """Whether any process is still left in ``pgid``."""
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - a foreign, live group
        return True
    return True


class ProcessSupervisor:
    """Owns every process group one attempt started and can end all of them."""

    def __init__(
        self,
        *,
        grace_seconds: float = DEFAULT_GRACE_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._grace_seconds = grace_seconds
        self._clock = clock
        self._sleep = sleep
        self._children: list[subprocess.Popen[bytes]] = []
        self._report: dict[str, Any] | None = None

    def spawn(
        self,
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start one process in its own group and track it."""
        if isinstance(argv, str) or not argv:
            raise TerminationError("runtime argv must be a non-empty sequence of strings")
        process = subprocess.Popen(
            list(argv),
            cwd=cwd,
            env=None if env is None else dict(env),
            start_new_session=True,
        )
        self._children.append(process)
        return process

    def live_groups(self) -> list[int]:
        """Process groups of tracked children that still have a member alive."""
        return [
            process.pid
            for process in self._children
            if process.poll() is None or _group_alive(process.pid)
        ]

    def _signal_live_groups(self, signal_number: int) -> None:
        for pgid in self.live_groups():
            try:
                os.killpg(pgid, signal_number)
            except ProcessLookupError:  # pragma: no cover - raced with its own exit
                continue

    def _await_groups_gone(self, timeout_seconds: float) -> bool:
        deadline = self._clock() + timeout_seconds
        while self.live_groups() and self._clock() < deadline:
            self._sleep(POLL_INTERVAL_SECONDS)
        return not self.live_groups()

    def _reap(self) -> None:
        for process in self._children:
            process.wait()

    def terminate(self, reason: str) -> dict[str, Any]:
        """Stop every tracked group and confirm it is gone.

        Returns the termination evidence document (``requested``, ``reason``,
        ``signals``, ``process_groups``, ``confirmed``).  It is idempotent: a
        second request reports what the first one did, so a caller can always
        read the one authoritative confirmation.
        """
        if self._report is not None:
            return dict(self._report)
        groups = self.live_groups()
        signals: list[str] = []
        if groups:
            self._signal_live_groups(signal.SIGTERM)
            signals.append("SIGTERM")
            if not self._await_groups_gone(self._grace_seconds):
                self._signal_live_groups(signal.SIGKILL)
                signals.append("SIGKILL")
                self._await_groups_gone(KILL_CONFIRM_TIMEOUT_SECONDS)
        self._reap()
        self._report = {
            "requested": True,
            "reason": reason,
            "signals": signals,
            "process_groups": groups,
            "confirmed": not self.live_groups(),
        }
        return dict(self._report)
