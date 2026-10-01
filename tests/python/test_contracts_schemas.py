"""Validate the documented example payloads against the versioned JSON schemas.

This is the Python half of the contract-compatibility guarantee; the Rust half
is `crates/core/tests/contracts_json.rs`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource

from conftest import CONTRACTS_DIR, EXAMPLES_DIR

SCHEMA_EXAMPLE_PAIRS = [
    ("state-snapshot.schema.json", "state-snapshot.worker-timeout.json"),
    ("observation.schema.json", "observation.worker-timeout.json"),
    ("proposal.schema.json", "decision-proposal.retry.json"),
    ("policy-decision.schema.json", "policy-decision.allow.json"),
    ("execution-intent.schema.json", "execution-intent.demo-verify.json"),
    ("execution-result.schema.json", "execution-result.demo-verify.json"),
]


def _schemas() -> dict[str, dict]:
    return {
        path.name: json.loads(path.read_text())
        for path in sorted(CONTRACTS_DIR.glob("*.schema.json"))
    }


def _registry() -> Registry:
    return Registry().with_resources(
        [
            (schema["$id"], Resource.from_contents(schema))
            for schema in _schemas().values()
        ]
    )


def _validator(schema_name: str) -> Draft202012Validator:
    schema = _schemas()[schema_name]
    return Draft202012Validator(schema, registry=_registry())


@pytest.mark.parametrize("schema_name,example_name", SCHEMA_EXAMPLE_PAIRS)
def test_documented_example_matches_schema(schema_name: str, example_name: str) -> None:
    payload = json.loads((EXAMPLES_DIR / example_name).read_text())
    _validator(schema_name).validate(payload)


def test_all_contracts_are_versioned() -> None:
    for name, schema in _schemas().items():
        properties = schema.get("properties", {})
        assert "schema_version" in properties, f"{name} lacks schema_version"
        assert properties["schema_version"] == {"const": 1}, name


def test_deny_policy_decision_with_intent_is_rejected() -> None:
    payload = {
        "schema_version": 1,
        "policy_decision_id": "policy:x",
        "proposal_id": "proposal:x",
        "decision": "DENY",
        "reasons": ["unknown capability"],
        "decided_at": "2026-10-01T00:00:00Z",
        "modified_intent": json.loads(
            (EXAMPLES_DIR / "execution-intent.demo-verify.json").read_text()
        ),
    }
    with pytest.raises(ValidationError):
        _validator("policy-decision.schema.json").validate(payload)


def test_unknown_observation_category_is_rejected() -> None:
    payload = json.loads((EXAMPLES_DIR / "observation.worker-timeout.json").read_text())
    payload["category"] = "NOT_A_REAL_CATEGORY"
    with pytest.raises(ValidationError):
        _validator("observation.schema.json").validate(payload)
