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
    ("decision-context.schema.json", "decision-context.initial.json"),
    ("decision-context.schema.json", "decision-context.recovery.json"),
    ("decision-envelope.schema.json", "decision-envelope.executable.json"),
    ("decision-envelope.schema.json", "decision-envelope.denied.json"),
    ("decision-envelope.schema.json", "decision-envelope.pending.json"),
]

# Only the DecisionProposal contract was bumped (v2, generic semantic
# vocabulary); its v1 payloads remain accepted. Every other contract stays v1.
DECISION_PROPOSAL_VERSIONS = [1, 2]


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
        if name == "proposal.schema.json":
            assert properties["schema_version"]["enum"] == DECISION_PROPOSAL_VERSIONS, name
        else:
            assert properties["schema_version"] == {"const": 1}, name


def _proposal_base(version: int, decision: str) -> dict:
    return {
        "schema_version": version,
        "proposal_id": "proposal:x",
        "decision": decision,
        "subject": {"type": "task", "id": "t1"},
        "provider": {"provider_type": "RULES", "provider_id": "rules.v1"},
        "proposed_at": "2026-10-01T00:00:00Z",
    }


def test_v2_semantic_vocabulary_is_accepted() -> None:
    validator = _validator("proposal.schema.json")
    for decision in ("REEXECUTE", "ROUTE", "DECOMPOSE", "ESCALATE", "ABORT", "EXECUTE_CAPABILITY"):
        validator.validate(_proposal_base(2, decision))
    # The legacy token is v1-only; it must not be smuggled into a v2 payload.
    with pytest.raises(ValidationError):
        validator.validate(_proposal_base(2, "RETRY"))


def test_v2_route_carries_generic_constraints() -> None:
    payload = _proposal_base(2, "ROUTE")
    payload["capability"] = "demo.verify"
    payload["route"] = {
        "exclude_backend": "backend.a",
        "preferred_backend": "backend.b",
        "required_provider": "provider.c",
        "required_runtime": "wasm",
    }
    _validator("proposal.schema.json").validate(payload)


def test_route_constraints_are_rejected_on_non_route_decisions() -> None:
    payload = _proposal_base(2, "EXECUTE_CAPABILITY")
    payload["capability"] = "demo.verify"
    payload["route"] = {"preferred_backend": "backend.b"}
    with pytest.raises(ValidationError):
        _validator("proposal.schema.json").validate(payload)


def test_v1_legacy_vocabulary_is_accepted_but_v2_tokens_are_not() -> None:
    validator = _validator("proposal.schema.json")
    # Legacy RETRY is accepted under v1...
    validator.validate(_proposal_base(1, "RETRY"))
    # ...but the v2-only tokens are not.
    for decision in ("ROUTE", "DECOMPOSE"):
        with pytest.raises(ValidationError):
            validator.validate(_proposal_base(1, decision))


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


# -- initial/recovery context ------------------------------------------------


def _example(name: str) -> dict:
    return json.loads((EXAMPLES_DIR / name).read_text())


def test_initial_context_forbids_a_fabricated_observation() -> None:
    payload = _example("decision-context.initial.json")
    payload["snapshot"]["facts"].append(
        {"key": "observation.category", "value": "WORKER_TIMEOUT"}
    )
    with pytest.raises(ValidationError):
        _validator("decision-context.schema.json").validate(payload)


def test_initial_context_forbids_a_recovery_binding() -> None:
    payload = _example("decision-context.initial.json")
    payload["recovery"] = _example("decision-context.recovery.json")["recovery"]
    with pytest.raises(ValidationError):
        _validator("decision-context.schema.json").validate(payload)


def test_recovery_context_requires_a_binding() -> None:
    payload = _example("decision-context.recovery.json")
    del payload["recovery"]
    with pytest.raises(ValidationError):
        _validator("decision-context.schema.json").validate(payload)


def test_attempt_counts_must_be_integers_not_booleans() -> None:
    payload = _example("decision-context.initial.json")
    payload["attempts"]["semantic_attempts"] = True
    with pytest.raises(ValidationError):
        _validator("decision-context.schema.json").validate(payload)


def test_missing_attempt_fields_fail_closed() -> None:
    payload = _example("decision-context.initial.json")
    del payload["attempts"]["max_semantic_attempts"]
    with pytest.raises(ValidationError):
        _validator("decision-context.schema.json").validate(payload)


def test_unknown_context_kind_is_rejected() -> None:
    payload = _example("decision-context.initial.json")
    payload["kind"] = "FRESH"
    with pytest.raises(ValidationError):
        _validator("decision-context.schema.json").validate(payload)


def test_step_retries_are_separate_from_the_semantic_budget() -> None:
    # The step-retry counter is independent: a high value is valid even when the
    # semantic bound is already reached.
    payload = _example("decision-context.recovery.json")
    payload["attempts"] = {
        "semantic_attempts": 1,
        "max_semantic_attempts": 1,
        "step_retries": 42,
    }
    _validator("decision-context.schema.json").validate(payload)


# -- validated decision envelope ---------------------------------------------


def test_non_executable_envelope_cannot_carry_an_intent() -> None:
    intent = _example("decision-envelope.executable.json")["intent"]
    for name in ("decision-envelope.denied.json", "decision-envelope.pending.json"):
        payload = _example(name)
        payload["intent"] = intent
        with pytest.raises(ValidationError):
            _validator("decision-envelope.schema.json").validate(payload)


def test_executable_envelope_requires_an_intent() -> None:
    payload = _example("decision-envelope.executable.json")
    del payload["intent"]
    with pytest.raises(ValidationError):
        _validator("decision-envelope.schema.json").validate(payload)


def test_executable_envelope_requires_a_permitted_capability() -> None:
    payload = _example("decision-envelope.executable.json")
    payload["capability_validation"]["permitted"] = False
    with pytest.raises(ValidationError):
        _validator("decision-envelope.schema.json").validate(payload)


def test_unknown_envelope_outcome_is_rejected() -> None:
    payload = _example("decision-envelope.denied.json")
    payload["outcome"] = "MAYBE"
    with pytest.raises(ValidationError):
        _validator("decision-envelope.schema.json").validate(payload)


def test_envelope_binds_policy_identity_and_version() -> None:
    payload = _example("decision-envelope.executable.json")
    del payload["policy"]["policy_version"]
    with pytest.raises(ValidationError):
        _validator("decision-envelope.schema.json").validate(payload)


def test_envelope_binds_snapshot_digest_and_revision() -> None:
    payload = _example("decision-envelope.executable.json")
    del payload["snapshot"]["digest"]
    with pytest.raises(ValidationError):
        _validator("decision-envelope.schema.json").validate(payload)


def test_no_route_envelope_uses_generic_route_constraints() -> None:
    payload = _example("decision-envelope.executable.json")
    del payload["intent"]
    payload["outcome"] = "NO_ROUTE"
    payload["route"] = {"exclude_backend": "backend.a"}
    _validator("decision-envelope.schema.json").validate(payload)

    payload["route"] = {"preferred_backend": ""}
    with pytest.raises(ValidationError):
        _validator("decision-envelope.schema.json").validate(payload)
