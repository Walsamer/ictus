"""Direct unit tests for the pure-Python contract validators.

The schema tests validate examples against JSON Schema; these tests cover the
runtime validators in `ictus_dagster.adapters.contracts` (used by the bridge)
including every error path.
"""

from __future__ import annotations

import pytest

from ictus_dagster.adapters.contracts import (
    EXECUTION_STATUSES,
    OBSERVATION_CATEGORIES,
    SCHEMA_VERSION,
    ContractError,
    validate_intent,
    validate_result,
)


def _intent() -> dict:
    return {
        "schema_version": 1,
        "intent_id": "intent:x",
        "capability": "demo.verify",
        "target": {"type": "task", "id": "t1"},
        "requested_by": {"provider": "rules.v1", "decision_id": "p1"},
    }


def _result() -> dict:
    return {
        "schema_version": 1,
        "execution_id": "e1",
        "intent_id": "i1",
        "status": "succeeded",
        "observation": {"category": "SUCCESS"},
    }


def test_constants_are_stable() -> None:
    assert SCHEMA_VERSION == 1
    assert "SUCCESS" in OBSERVATION_CATEGORIES
    assert "succeeded" in EXECUTION_STATUSES


def test_valid_payloads_pass_through_unchanged() -> None:
    intent = _intent()
    assert validate_intent(intent) is intent
    result = _result()
    assert validate_result(result) is result


def test_selected_route_requires_identity_and_fact_revisions() -> None:
    intent = _intent()
    intent["selected_route"] = {
        "route": {
            "backend": "dagster",
            "provider": "provider.a",
            "runtime": "container",
            "model": "model.a",
        },
        "descriptor": {"fact_id": "descriptor-1", "revision": 3},
        "health": {"fact_id": "health-1", "revision": 4},
        "quota": {"fact_id": "quota-1", "revision": 5},
        "disablement": {"fact_id": "disablement-1", "revision": 6},
    }
    assert validate_intent(intent) is intent
    intent["selected_route"]["health"]["revision"] = -1
    with pytest.raises(ContractError, match="selected_route.health"):
        validate_intent(intent)


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda d: d.pop("schema_version"), "missing required field: schema_version"),
        (lambda d: d.update(schema_version="1"), "schema_version must be int"),
        (lambda d: d.update(schema_version=2), "unsupported schema_version"),
        (lambda d: d.pop("intent_id"), "missing required field: intent_id"),
        (lambda d: d.pop("capability"), "missing required field: capability"),
        (lambda d: d.update(capability="  "), "capability must be non-empty"),
        (lambda d: d.update(target="not-an-object"), "target must be dict"),
        (lambda d: d.update(target={}), "missing required field: type"),
        (lambda d: d.update(target={"type": "task"}), "missing required field: id"),
        (lambda d: d.pop("requested_by"), "missing required field: requested_by"),
        (lambda d: d.update(requested_by={"provider": "rules.v1"}), "missing required field: decision_id"),
    ],
)
def test_validate_intent_rejects_bad_payloads(mutate, match) -> None:
    payload = _intent()
    mutate(payload)
    with pytest.raises(ContractError, match=match):
        validate_intent(payload)


def test_validate_intent_rejects_non_object() -> None:
    with pytest.raises(ContractError, match="must be a JSON object"):
        validate_intent(["not", "an", "object"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda d: d.pop("execution_id"), "missing required field: execution_id"),
        (lambda d: d.pop("intent_id"), "missing required field: intent_id"),
        (lambda d: d.update(schema_version="1"), "schema_version must be int"),
        (lambda d: d.update(schema_version=2), "unsupported schema_version"),
        (lambda d: d.update(status="bogus"), "status must be one of"),
        (lambda d: d.pop("status"), "missing required field: status"),
        (lambda d: d.pop("observation"), "missing required field: observation"),
        (lambda d: d.update(observation={}), "missing required field: category"),
        (lambda d: d.update(observation={"category": "NOPE"}), "unknown observation category"),
    ],
)
def test_validate_result_rejects_bad_payloads(mutate, match) -> None:
    payload = _result()
    mutate(payload)
    with pytest.raises(ContractError, match=match):
        validate_result(payload)


def test_validate_result_rejects_non_object() -> None:
    with pytest.raises(ContractError, match="must be a JSON object"):
        validate_result("nope")  # type: ignore[arg-type]
