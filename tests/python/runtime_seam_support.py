"""Backend factories that cannot become a seam backend (test support only).

``stub_runtime_backend`` is the working case; these two exist so the resolver's
fail-closed branches can be exercised by name — a factory that raises while
being constructed and a factory that returns something which does not implement
the seam port.  Nothing here is shipped in the Ictus package.
"""

from __future__ import annotations

from typing import Any


def broken_factory() -> Any:
    """A factory that cannot construct anything."""
    raise RuntimeError("broken factory")


def not_a_backend() -> Any:
    """A factory that returns an object without the seam port."""
    return object()
