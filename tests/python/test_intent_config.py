"""Intent -> Dagster settings mapping (no Dagster runtime required)."""

from __future__ import annotations

import pytest

from agentic_dagster.adapters.intent_config import (
    UnsupportedCapability,
    settings_from_intent,
)


def _intent(capability: str = "demo.verify", arguments: dict | None = None) -> dict:
    return {
        "schema_version": 1,
        "intent_id": "intent:x",
        "capability": capability,
        "target": {"type": "task", "id": "t1"},
        "arguments": arguments or {},
        "requested_by": {"provider": "rules.v1", "decision_id": "p1"},
    }


def test_supported_capability_defaults_are_clean() -> None:
    config = settings_from_intent(_intent())
    assert config["capability"] == "demo.verify"
    assert config["fail_until_attempt"] == 0
    assert config["fail_hard"] is False
    assert config["fail_verify"] is False


def test_failure_injection_arguments_are_parsed() -> None:
    config = settings_from_intent(
        _intent(arguments={"fail_until_attempt": "2", "fail_hard": "true"})
    )
    assert config["fail_until_attempt"] == 2
    assert config["fail_hard"] is True


def test_known_unsupported_capability_is_rejected() -> None:
    with pytest.raises(UnsupportedCapability, match="not owned"):
        settings_from_intent(_intent("software.promote"))


def test_unknown_capability_is_rejected() -> None:
    with pytest.raises(UnsupportedCapability, match="unknown capability"):
        settings_from_intent(_intent("something.else"))


def test_non_object_arguments_are_rejected() -> None:
    intent = _intent()
    intent["arguments"] = "not-an-object"
    with pytest.raises(UnsupportedCapability, match="JSON object"):
        settings_from_intent(intent)
