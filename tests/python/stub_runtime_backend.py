"""A tiny stub runtime backend for the seam tests (test evidence only).

It is not shipped in the Ictus package.  It exists to prove three things the
seam promises: a runtime is reached by *name* from configuration, the workspace
descriptor is the runtime's (only this file reads it), and everything the
backend starts goes through the seam's control so termination is real.

The workspace descriptor is a small JSON document this stub owns::

    {"executable": "/usr/local/bin/stub-runtime",
     "version_token": "stub-1.0.0",
     "probe": {"ok": true, "detail": "stub probe ok"},
     "behavior": "success"}
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

from ictus_dagster.seam.control import ExecutionControl
from ictus_dagster.seam.port import AttemptRequest, WorkspaceRef

PATCH_TEXT = "diff --git a/src/stub.py b/src/stub.py\n+stub\n"


def _descriptor(workspace: WorkspaceRef) -> dict[str, Any]:
    return json.loads(Path(workspace.descriptor_path).read_text(encoding="utf-8"))


def _evidence(
    *,
    outcome: str = "succeeded",
    failure_kind: str | None = None,
    changed_paths: list[str] | None = None,
    steps_used: int = 2,
    exit_code: int | None = 0,
    exit_signal: str | None = None,
    termination: dict[str, Any] | None = None,
    duration_seconds: float = 0.01,
) -> dict[str, Any]:
    patch = changed_paths or []
    return {
        "schema_version": 1,
        "outcome": outcome,
        "failure_kind": failure_kind,
        "changed_paths": patch,
        "patch_digest": (
            "sha256:" + hashlib.sha256(PATCH_TEXT.encode("utf-8")).hexdigest()
            if patch
            else None
        ),
        "patch_bytes": len(PATCH_TEXT.encode("utf-8")) if patch else 0,
        "steps_used": steps_used,
        "exit_status": {"code": exit_code, "signal": exit_signal},
        "termination": termination
        or {"requested": False, "signals": [], "confirmed": True},
        "duration_seconds": duration_seconds,
    }


class StubRuntimeBackend:
    """A deterministic backend whose behaviour is chosen by its descriptor."""

    def describe_workspace(self, workspace: WorkspaceRef) -> dict[str, Any]:
        descriptor = _descriptor(workspace)
        if descriptor.get("behavior") == "describe_raises":
            raise RuntimeError("stub probe crashed")
        if descriptor.get("behavior") == "describe_nondict":
            return ["not a report"]  # type: ignore[return-value]
        if descriptor.get("behavior") == "describe_mistyped":
            return {"executable": descriptor["executable"]}
        if descriptor.get("behavior") == "describe_probe_mistyped":
            return {
                "executable": descriptor["executable"],
                "version_token": descriptor["version_token"],
                "probe": {"ok": "yes", "detail": 1},
            }
        return {
            "executable": descriptor["executable"],
            "version_token": descriptor["version_token"],
            "probe": dict(descriptor.get("probe", {"ok": True, "detail": "stub"})),
        }

    def execute(
        self, request: AttemptRequest, control: ExecutionControl
    ) -> dict[str, Any]:
        behavior = _descriptor(request.workspace).get("behavior", "success")
        started = time.monotonic()
        if behavior == "success":
            control.step()
            process = control.spawn([sys.executable, "-c", "print('stub step')"])
            control.wait(process)
            control.step()
            return _evidence(
                changed_paths=["src/stub.py"],
                steps_used=control.steps_used,
                duration_seconds=time.monotonic() - started,
            )
        if behavior == "sleep":
            process = control.spawn([sys.executable, "-c", "import time; time.sleep(300)"])
            control.wait(process)
        if behavior == "stubborn":
            process = control.spawn(
                [
                    sys.executable,
                    "-c",
                    "import signal, time; "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                    "time.sleep(300)",
                ]
            )
            control.wait(process)
        if behavior == "leftover":
            control.spawn([sys.executable, "-c", "import time; time.sleep(300)"])
            return _evidence(changed_paths=["src/stub.py"])
        if behavior == "fail":
            return _evidence(
                outcome="failed",
                failure_kind="PROCESS_EXIT",
                exit_code=1,
                steps_used=1,
            )
        if behavior == "scope_violation":
            return _evidence(changed_paths=["outside/escape.py"])
        if behavior == "steps_overrun":
            # Ask for more steps than the operator's budget allows before
            # starting anything, so the control halts the attempt while it has
            # no termination to report yet.
            for _ in range(5):
                control.step()
            return _evidence(changed_paths=["src/stub.py"], steps_used=control.steps_used)
        if behavior == "duration_overrun":
            # Report an attempt whose own duration is past the operator's wall
            # clock without the control timing out: only ``verify`` can catch a
            # runtime that misreports how long it ran.
            return _evidence(changed_paths=["src/stub.py"], duration_seconds=99.0)
        if behavior == "budget":
            return _evidence(changed_paths=["src/stub.py"], steps_used=99)
        if behavior == "bad_evidence":
            return {"schema_version": 1}
        if behavior == "verify_failed":
            document = _evidence(changed_paths=["src/stub.py"])
            document["stub_verification"] = {
                "verified": False,
                "detail": "stub post-condition failed",
            }
            return document
        if behavior == "verify_mistyped":
            document = _evidence(changed_paths=["src/stub.py"])
            document["stub_verification"] = {"verified": "yes"}
            return document
        if behavior == "verify_raises":
            document = _evidence(changed_paths=["src/stub.py"])
            document["stub_verification"] = "raise"
            return document
        if behavior == "undefined_termination":
            return _evidence(
                changed_paths=["src/stub.py"],
                termination={"requested": True, "signals": ["SIGKILL"], "confirmed": False},
            )
        if behavior == "cancel":
            control.spawn([sys.executable, "-c", "import time; time.sleep(300)"])
            termination = control.cancel("control_plane_request")
            return _evidence(
                outcome="cancelled",
                failure_kind="CANCEL_REQUESTED",
                termination=termination,
                steps_used=1,
            )
        raise RuntimeError(f"stub crashed on behavior '{behavior}'")

    def verify(self, evidence: dict[str, Any]) -> dict[str, Any]:
        reported = evidence.get("stub_verification")
        if reported == "raise":
            raise RuntimeError("stub verification crashed")
        if isinstance(reported, dict):
            return reported
        return {"verified": True, "detail": "stub post-conditions hold"}


def create_backend() -> StubRuntimeBackend:
    """The factory the seam configuration names (``module:create_backend``)."""
    return StubRuntimeBackend()


def not_a_port() -> dict[str, Any]:
    """A resolvable, callable backend reference that is not a port implementation.

    The seam calls any attribute named in configuration and then checks the
    result against the port, so this covers the fail-closed "named backend does
    not implement the seam port" branch with a reference that *can* be
    constructed (``PATCH_TEXT`` cannot be called at all).
    """
    return {"backend": "not a port"}
