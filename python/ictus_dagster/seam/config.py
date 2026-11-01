"""Loading and validating the operator-owned runtime seam configuration.

The seam is selected by a *path* to an operator-owned JSON document, never by
an import: Ictus holds the path, validates the document against the published
schema (``contracts/runtime-seam.schema.json``, version 1) and resolves the
backend reference named inside it at run time.  Every deviation — unreadable
file, unknown field, unsupported version, out-of-bounds budget — fails closed.

Nothing here imports a control plane, a domain adapter or a runtime, and
nothing here reads the workspace descriptor: it is passed through by reference.

Shape (schema_version 1)::

    {
      "schema_version": 1,
      "backend": "python.module:attribute",
      "workspace": {"descriptor": "config/agent-runtime.json"},
      "bounds": {"write_scope": ["src"], "wall_clock_seconds": 600,
                 "step_budget": 200}
    }
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Version of the seam configuration document this build accepts.
SEAM_CONFIG_VERSION = 1

CONFIG_FIELDS = frozenset({"schema_version", "backend", "workspace", "bounds"})
WORKSPACE_FIELDS = frozenset({"descriptor"})
BOUNDS_FIELDS = frozenset({"write_scope", "wall_clock_seconds", "step_budget"})

#: A backend is named ``module:attribute``; it is imported lazily, by name.
BACKEND_REFERENCE = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*"
    r":[A-Za-z_][A-Za-z0-9_]*"
)

#: Bounds are operator-set, so they must be explicit and bounded themselves.
MAX_WALL_CLOCK_SECONDS = 86_400.0
MAX_STEP_BUDGET = 10_000


class SeamConfigError(ValueError):
    """Raised when the runtime seam configuration is unusable (fail closed)."""


@dataclass(frozen=True)
class SeamConfig:
    """A validated seam configuration, with the descriptor resolved by path."""

    backend: str
    workspace_descriptor: str
    write_scope: tuple[str, ...]
    wall_clock_seconds: float
    step_budget: int
    path: str

    def to_json(self) -> dict[str, Any]:
        """Report the resolved configuration (used as step evidence)."""
        return {
            "schema_version": SEAM_CONFIG_VERSION,
            "backend": self.backend,
            "workspace": {"descriptor": self.workspace_descriptor},
            "bounds": {
                "write_scope": list(self.write_scope),
                "wall_clock_seconds": self.wall_clock_seconds,
                "step_budget": self.step_budget,
            },
            "source": self.path,
        }


def _require_mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SeamConfigError(f"{where} must be a JSON object")
    return value


def _require_exact_fields(
    document: dict[str, Any], allowed: frozenset[str], where: str
) -> None:
    missing = allowed - set(document)
    unknown = set(document) - allowed
    if missing:
        raise SeamConfigError(f"{where} is missing field(s): {', '.join(sorted(missing))}")
    if unknown:
        raise SeamConfigError(
            f"{where} carries unknown field(s): {', '.join(sorted(unknown))}"
        )


def _require_backend(document: dict[str, Any]) -> str:
    backend = document["backend"]
    if not isinstance(backend, str) or not BACKEND_REFERENCE.fullmatch(backend):
        raise SeamConfigError(
            "backend must be a 'module:attribute' reference to the runtime seam "
            "backend selected by this configuration"
        )
    return backend


def _require_descriptor(workspace: dict[str, Any], source: Path) -> str:
    descriptor = workspace["descriptor"]
    if not isinstance(descriptor, str) or not descriptor.strip():
        raise SeamConfigError("workspace.descriptor must be a non-empty path")
    candidate = Path(descriptor)
    # A relative descriptor is resolved against the config file that named it,
    # so an operator-owned directory keeps working from any working directory.
    resolved = candidate if candidate.is_absolute() else source.parent / candidate
    return str(resolved)


def _require_scope(bounds: dict[str, Any]) -> tuple[str, ...]:
    write_scope = bounds["write_scope"]
    if (
        not isinstance(write_scope, list)
        or not write_scope
        or not all(isinstance(entry, str) and entry.strip() for entry in write_scope)
    ):
        raise SeamConfigError(
            "bounds.write_scope must be a non-empty list of non-empty paths"
        )
    return tuple(write_scope)


def _require_wall_clock(bounds: dict[str, Any]) -> float:
    value = bounds["wall_clock_seconds"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SeamConfigError("bounds.wall_clock_seconds must be a number")
    seconds = float(value)
    if not math.isfinite(seconds) or not 0 < seconds <= MAX_WALL_CLOCK_SECONDS:
        raise SeamConfigError(
            "bounds.wall_clock_seconds must be finite and greater than 0 and at "
            f"most {MAX_WALL_CLOCK_SECONDS:g}"
        )
    return seconds


def _require_step_budget(bounds: dict[str, Any]) -> int:
    value = bounds["step_budget"]
    if isinstance(value, bool) or not isinstance(value, int):
        raise SeamConfigError("bounds.step_budget must be an integer")
    if not 0 < value <= MAX_STEP_BUDGET:
        raise SeamConfigError(
            f"bounds.step_budget must be greater than 0 and at most {MAX_STEP_BUDGET}"
        )
    return value


def parse_seam_config(document: Any, *, source: str) -> SeamConfig:
    """Validate a seam configuration document and return it parsed."""
    parsed = _require_mapping(document, "runtime seam config")
    _require_exact_fields(parsed, CONFIG_FIELDS, "runtime seam config")
    version = parsed["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise SeamConfigError(
            f"runtime seam config schema_version must be an integer, not {version!r}"
        )
    if version != SEAM_CONFIG_VERSION:
        raise SeamConfigError(
            f"unsupported runtime seam config schema_version "
            f"{version!r}; expected {SEAM_CONFIG_VERSION}"
        )
    workspace = _require_mapping(parsed["workspace"], "workspace")
    _require_exact_fields(workspace, WORKSPACE_FIELDS, "workspace")
    bounds = _require_mapping(parsed["bounds"], "bounds")
    _require_exact_fields(bounds, BOUNDS_FIELDS, "bounds")
    return SeamConfig(
        backend=_require_backend(parsed),
        workspace_descriptor=_require_descriptor(workspace, Path(source)),
        write_scope=_require_scope(bounds),
        wall_clock_seconds=_require_wall_clock(bounds),
        step_budget=_require_step_budget(bounds),
        path=source,
    )


def load_seam_config(path: str | None) -> SeamConfig | None:
    """Load the seam configuration named by ``path``.

    Returns ``None`` when no seam is configured, which is the fail-closed
    default: the job then keeps its placeholder behaviour.  An unreadable or
    malformed document raises :class:`SeamConfigError` rather than falling back
    to that default.
    """
    if path is None or not str(path).strip():
        return None
    source = Path(path)
    try:
        raw = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise SeamConfigError(
            f"runtime seam config '{path}' cannot be read: {exc}"
        ) from None
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SeamConfigError(
            f"runtime seam config '{path}' is not valid JSON: {exc}"
        ) from None
    return parse_seam_config(document, source=str(source))
