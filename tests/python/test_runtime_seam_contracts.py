"""The published runtime-seam schemas and the Python validators must agree.

``contracts/README.md`` names this file as the check that the two execution-side
contracts (``RuntimeSeamConfig`` and ``RuntimeEvidence``, both ``schema_version:
1``) say what ``ictus_dagster.seam`` enforces: the same required fields, the same
version, the same failure-kind vocabulary and the same answers on a corpus of
accepted and rejected documents.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, ValidationError

from ictus_dagster.seam.config import (
    BOUNDS_FIELDS,
    CONFIG_FIELDS,
    MAX_STEP_BUDGET,
    MAX_WALL_CLOCK_SECONDS,
    SEAM_CONFIG_VERSION,
    SeamConfigError,
    WORKSPACE_FIELDS,
    parse_seam_config,
)
from ictus_dagster.seam.evidence import (
    EVIDENCE_SCHEMA_VERSION,
    FAILURE_KINDS,
    OUTCOMES,
    EvidenceError,
    REQUIRED_FIELDS,
    validate_evidence,
)

SEAM_SCHEMA = "runtime-seam.schema.json"
EVIDENCE_SCHEMA = "runtime-evidence.schema.json"

DIGEST = "sha256:" + "0123456789abcdef" * 4


def _schema(contracts_dir: Path, name: str) -> dict[str, Any]:
    return json.loads((contracts_dir / name).read_text(encoding="utf-8"))


def _validator(contracts_dir: Path, name: str) -> Draft202012Validator:
    return Draft202012Validator(_schema(contracts_dir, name))


def _seam_config_document(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": SEAM_CONFIG_VERSION,
        "backend": "tests.python.stub_runtime_backend:create_backend",
        "workspace": {"descriptor": "runtime/workspace.json"},
        "bounds": {
            "write_scope": ["src"],
            "wall_clock_seconds": 600,
            "step_budget": 200,
        },
    }
    document.update(overrides)
    return document


def _evidence_document(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "outcome": "succeeded",
        "failure_kind": None,
        "changed_paths": ["src/a.py"],
        "patch_digest": DIGEST,
        "patch_bytes": 42,
        "steps_used": 2,
        "exit_status": {"code": 0, "signal": None},
        "termination": {"requested": True, "signals": ["SIGTERM"], "confirmed": True},
        "duration_seconds": 0.5,
    }
    document.update(overrides)
    return document


def _failure_kind_enum(schema: dict[str, Any]) -> frozenset[str]:
    for branch in schema["properties"]["failure_kind"]["oneOf"]:
        if "enum" in branch:
            return frozenset(branch["enum"])
    raise AssertionError("failure_kind carries no enum branch")


# -- the schemas' vocabulary equals the module constants ---------------------


def test_the_seam_config_schema_matches_the_loader(contracts_dir: Path) -> None:
    schema = _schema(contracts_dir, SEAM_SCHEMA)
    assert frozenset(schema["required"]) == CONFIG_FIELDS
    assert frozenset(schema["properties"]) == CONFIG_FIELDS
    assert schema["additionalProperties"] is False
    assert schema["properties"]["schema_version"]["const"] == SEAM_CONFIG_VERSION

    workspace = schema["properties"]["workspace"]
    assert frozenset(workspace["required"]) == WORKSPACE_FIELDS
    assert frozenset(workspace["properties"]) == WORKSPACE_FIELDS
    assert workspace["additionalProperties"] is False

    bounds = schema["properties"]["bounds"]
    assert frozenset(bounds["required"]) == BOUNDS_FIELDS
    assert frozenset(bounds["properties"]) == BOUNDS_FIELDS
    assert bounds["additionalProperties"] is False


def test_the_schema_bounds_are_the_loader_bounds(contracts_dir: Path) -> None:
    """The published bounds are the enforced bounds, not a second opinion."""
    bounds = _schema(contracts_dir, SEAM_SCHEMA)["properties"]["bounds"]["properties"]
    assert bounds["wall_clock_seconds"]["maximum"] == MAX_WALL_CLOCK_SECONDS
    assert bounds["step_budget"]["maximum"] == MAX_STEP_BUDGET
    assert bounds["wall_clock_seconds"]["exclusiveMinimum"] == 0
    assert bounds["step_budget"]["exclusiveMinimum"] == 0


def test_the_evidence_schema_matches_the_validator(contracts_dir: Path) -> None:
    schema = _schema(contracts_dir, EVIDENCE_SCHEMA)
    assert frozenset(schema["required"]) == REQUIRED_FIELDS
    assert schema["properties"]["schema_version"]["const"] == EVIDENCE_SCHEMA_VERSION
    assert frozenset(schema["properties"]["outcome"]["enum"]) == OUTCOMES
    assert _failure_kind_enum(schema) == FAILURE_KINDS


def test_the_schemas_are_versioned_at_one(contracts_dir: Path) -> None:
    for name in (SEAM_SCHEMA, EVIDENCE_SCHEMA):
        schema = _schema(contracts_dir, name)
        assert "$id" in schema, name
        assert schema["properties"]["schema_version"]["const"] == 1, name


# -- the accepted corpus ----------------------------------------------------


@pytest.mark.parametrize(
    "document",
    [
        pytest.param(_seam_config_document(), id="relative-descriptor"),
        pytest.param(
            _seam_config_document(workspace={"descriptor": "/opt/runtime/workspace.json"}),
            id="absolute-descriptor",
        ),
        pytest.param(
            _seam_config_document(
                bounds={
                    "write_scope": ["src", "tests/"],
                    "wall_clock_seconds": 0.5,
                    "step_budget": 1,
                }
            ),
            id="small-bounds",
        ),
        pytest.param(
            _seam_config_document(
                bounds={"write_scope": ["src"], "wall_clock_seconds": 86400, "step_budget": 10000}
            ),
            id="maximum-bounds",
        ),
    ],
)
def test_accepted_seam_configs_pass_the_schema_and_the_loader(
    contracts_dir: Path, document: dict[str, Any]
) -> None:
    _validator(contracts_dir, SEAM_SCHEMA).validate(document)

    config = parse_seam_config(document, source="/srv/seam/seam.json")
    descriptor = document["workspace"]["descriptor"]
    expected_descriptor = descriptor if descriptor.startswith("/") else f"/srv/seam/{descriptor}"
    assert config.workspace_descriptor == expected_descriptor
    assert config.backend == document["backend"]
    assert config.write_scope == tuple(document["bounds"]["write_scope"])
    assert config.wall_clock_seconds == float(document["bounds"]["wall_clock_seconds"])
    assert config.step_budget == document["bounds"]["step_budget"]


@pytest.mark.parametrize("kind", sorted(FAILURE_KINDS))
@pytest.mark.parametrize("outcome", sorted(OUTCOMES - {"succeeded"}))
def test_accepted_evidence_passes_the_schema_and_the_validator(
    contracts_dir: Path, outcome: str, kind: str
) -> None:
    document = _evidence_document(outcome=outcome, failure_kind=kind)
    _validator(contracts_dir, EVIDENCE_SCHEMA).validate(document)
    assert validate_evidence(document) is document


def test_a_succeeded_evidence_document_passes_both(contracts_dir: Path) -> None:
    document = _evidence_document()
    _validator(contracts_dir, EVIDENCE_SCHEMA).validate(document)
    assert validate_evidence(document) is document


def test_a_relative_workspace_descriptor_is_valid_and_resolves(contracts_dir: Path) -> None:
    # The schema only requires a non-empty string, so a relative descriptor is
    # schema-valid *and* validator-valid: the loader resolves it against the
    # config file's directory instead of rejecting it.
    document = _seam_config_document(workspace={"descriptor": "runtime/workspace.json"})
    _validator(contracts_dir, SEAM_SCHEMA).validate(document)
    config = parse_seam_config(document, source="/srv/seam/config/seam.json")
    assert config.workspace_descriptor == "/srv/seam/config/runtime/workspace.json"


# -- the rejected corpus: cases both must refuse ----------------------------


REJECTED_SEAM_CONFIGS = [
    pytest.param(_seam_config_document(extra="x"), id="unknown-field"),
    pytest.param(_seam_config_document(schema_version=2), id="wrong-version"),
    # A JSON ``true`` is not the integer 1, so the schema's ``const: 1`` and the
    # loader's integer check both refuse it.
    pytest.param(_seam_config_document(schema_version=True), id="boolean-version"),
    pytest.param(_seam_config_document(schema_version="1"), id="string-version"),
    pytest.param(
        _seam_config_document(workspace={"descriptor": "   "}), id="blank-descriptor"
    ),
    pytest.param(
        _seam_config_document(
            bounds={
                "write_scope": ["src/../etc"],
                "wall_clock_seconds": 600,
                "step_budget": 200,
            }
        ),
        id="escaping-scope",
    ),
    pytest.param(
        _seam_config_document(
            bounds={"write_scope": ["."], "wall_clock_seconds": 600, "step_budget": 200}
        ),
        id="scope-names-no-path",
    ),
]

REJECTED_EVIDENCE = [
    pytest.param(
        {key: value for key, value in _evidence_document().items() if key != "patch_digest"},
        id="missing-required-field",
    ),
    pytest.param(
        _evidence_document(outcome="failed", failure_kind="NOT_A_KIND"),
        id="invalid-failure-kind",
    ),
    pytest.param(_evidence_document(patch_digest="sha256:abc"), id="malformed-digest"),
    pytest.param(_evidence_document(schema_version=2), id="wrong-version"),
    pytest.param(_evidence_document(schema_version=True), id="boolean-version"),
    pytest.param(_evidence_document(schema_version="1"), id="string-version"),
]


@pytest.mark.parametrize("document", REJECTED_SEAM_CONFIGS)
def test_a_rejected_seam_config_fails_both_the_schema_and_the_loader(
    contracts_dir: Path, document: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        _validator(contracts_dir, SEAM_SCHEMA).validate(document)
    with pytest.raises(SeamConfigError):
        parse_seam_config(document, source="/srv/seam/seam.json")


@pytest.mark.parametrize("document", REJECTED_EVIDENCE)
def test_rejected_evidence_fails_both_sides(
    contracts_dir: Path, document: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        _validator(contracts_dir, EVIDENCE_SCHEMA).validate(document)
    with pytest.raises(EvidenceError):
        validate_evidence(document)


# -- where the schema is deliberately weaker than the validator --------------


def test_the_failure_kind_coupling_is_only_enforced_by_the_validator(
    contracts_dir: Path,
) -> None:
    # The schema lets ``failure_kind`` be null or any kind independently of
    # ``outcome``; the validator enforces the coupling ("null exactly when the
    # outcome is succeeded").  Both statements are pinned here so the gap is
    # visible instead of implied.
    inconsistent = _evidence_document(outcome="succeeded", failure_kind="BACKEND_ERROR")
    _validator(contracts_dir, EVIDENCE_SCHEMA).validate(inconsistent)
    with pytest.raises(EvidenceError, match="must not report a failure kind"):
        validate_evidence(inconsistent)

    missing_kind = _evidence_document(outcome="failed", failure_kind=None)
    _validator(contracts_dir, EVIDENCE_SCHEMA).validate(missing_kind)
    with pytest.raises(EvidenceError, match="requires a typed failure kind"):
        validate_evidence(missing_kind)


def test_a_boolean_schema_version_is_rejected_by_both_sides(contracts_dir: Path) -> None:
    # The drift case that used to split the two sides: ``True == 1`` in Python
    # let a JSON ``true`` through the validators while the schemas' ``const: 1``
    # rejected it.  Both now refuse it, and they agree on the reason.
    seam_document = _seam_config_document(schema_version=True)
    with pytest.raises(ValidationError):
        _validator(contracts_dir, SEAM_SCHEMA).validate(seam_document)
    with pytest.raises(SeamConfigError, match="must be an integer"):
        parse_seam_config(seam_document, source="/srv/seam/seam.json")

    evidence_document = _evidence_document(schema_version=True)
    with pytest.raises(ValidationError):
        _validator(contracts_dir, EVIDENCE_SCHEMA).validate(evidence_document)
    with pytest.raises(EvidenceError, match="must be an integer"):
        validate_evidence(evidence_document)
