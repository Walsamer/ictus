"""Adapter-level errors (pure Python, no Dagster import)."""

from __future__ import annotations


class UnsupportedCapability(ValueError):
    """Raised when the backend is asked to execute a capability it does not own."""
