"""Intent -> execution settings mapping (no Dagster runtime required)."""

from __future__ import annotations

import pytest

from ictus_dagster.adapters.intent_config import (
    CAPABILITY_WORKFLOW_CAPABILITIES,
    DATA_QUALITY_CAPABILITIES,
    KNOWN_UNSUPPORTED,
    SUPPORTED_CAPABILITIES,
    UnsupportedCapability,
    capability_settings,
    data_quality_settings,
    data_quality_settings_from_intent,
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


def test_capability_sets_are_disjoint_and_consistent() -> None:
    assert CAPABILITY_WORKFLOW_CAPABILITIES.isdisjoint(DATA_QUALITY_CAPABILITIES)
    assert SUPPORTED_CAPABILITIES == CAPABILITY_WORKFLOW_CAPABILITIES | DATA_QUALITY_CAPABILITIES
    assert "software.promote" in KNOWN_UNSUPPORTED


# -- capability workflow ----------------------------------------------------


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


def test_capability_settings_coerces_and_defaults() -> None:
    config = capability_settings(
        {"fail_until_attempt": None, "fail_hard": "yes", "fail_verify": "false"}
    )
    assert config == {"fail_until_attempt": 0, "fail_hard": True, "fail_verify": False}


def test_known_unsupported_capability_is_rejected() -> None:
    with pytest.raises(UnsupportedCapability, match="not owned"):
        settings_from_intent(_intent("software.promote"))


def test_unknown_capability_is_rejected() -> None:
    with pytest.raises(UnsupportedCapability, match="not a capability-workflow capability"):
        settings_from_intent(_intent("something.else"))


def test_data_quality_capability_is_not_a_capability_workflow_capability() -> None:
    with pytest.raises(UnsupportedCapability, match="not a capability-workflow capability"):
        settings_from_intent(_intent("data.quality_check"))


def test_non_object_arguments_are_rejected() -> None:
    intent = _intent()
    intent["arguments"] = "not-an-object"
    with pytest.raises(UnsupportedCapability, match="JSON object"):
        settings_from_intent(intent)


# -- data quality workflow --------------------------------------------------


def test_data_quality_settings_parse_and_default() -> None:
    config = data_quality_settings_from_intent(
        _intent(
            "data.quality_check",
            {"dataset": "synthetic.events", "rows": "7", "fail_on_check": "true"},
        )
    )
    assert config == {
        "dataset": "synthetic.events",
        "rows": 7,
        "min_rows": 1,
        "fail_ingest": False,
        "fail_until_attempt": 0,
        "fail_on_check": True,
    }


def test_data_quality_settings_defaults() -> None:
    config = data_quality_settings({})
    assert config["dataset"] == "synthetic.orders"
    assert config["rows"] == 100
    assert config["min_rows"] == 1
    assert config["fail_on_check"] is False


def test_data_quality_rejects_other_capabilities() -> None:
    with pytest.raises(UnsupportedCapability, match="not a data-quality capability"):
        data_quality_settings_from_intent(_intent("demo.verify"))
