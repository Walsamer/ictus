"""Pin the production Python digest to the independently published protocol vectors."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from ictus_dagster.submission import (
    DIGEST_SCHEMA_VERSION,
    DigestDomainError,
    intent_digest,
    parse_digestable_document,
    require_digest_schema_version,
)
from conftest import CONTRACTS_DIR, EXAMPLES_DIR

VECTORS = json.loads((CONTRACTS_DIR / "digest-vectors.json").read_text())


def test_digest_vectors_match_schema_and_protocol() -> None:
    schema = json.loads((CONTRACTS_DIR / "digest-vectors.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(VECTORS)
    assert VECTORS["schema_version"] == DIGEST_SCHEMA_VERSION == 1
    assert VECTORS["algorithm"] == "sorted-keys-compact-ascii-escaped-cpython-number"
    assert VECTORS["hash"] == "sha256"
    assert VECTORS["encoding"] == "lowercase-hex"
    require_digest_schema_version(VECTORS["schema_version"])


@pytest.mark.parametrize("version", [2, True, 1.0, "1", None])
def test_unknown_digest_schema_version_fails_closed(version: Any) -> None:
    with pytest.raises(ValueError, match="unsupported digest schema_version"):
        require_digest_schema_version(version)


@pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda case: case["name"])
def test_published_digest_vector(case: dict[str, Any]) -> None:
    assert intent_digest(case["payload"]) == case["sha256"]


@pytest.mark.parametrize("case", VECTORS["accepts"], ids=lambda case: case["name"])
def test_published_digest_acceptance(case: dict[str, Any]) -> None:
    # The ingress must let the document through, and what it carries must digest
    # to the published value: a guard that refuses one of these cuts off text
    # the protocol carries.
    payload = parse_digestable_document(case["document"])
    assert intent_digest(payload) == case["sha256"]


@pytest.mark.parametrize(
    "case", VECTORS["rejections"], ids=lambda case: case["name"]
)
def test_published_digest_rejection(case: dict[str, Any]) -> None:
    # Asserted from the file, not from a copy: the document must be refused with
    # the rule the file names, at the path it names.
    assert case["expect"] == "refusal"
    with pytest.raises(DigestDomainError) as caught:
        parse_digestable_document(case["document"])
    error = caught.value
    assert error.reason == case["reason"]
    assert error.path == case["path"]
    assert error.path in str(error)


def test_spelling_is_compared_by_value_not_by_text() -> None:
    # Extra zeros, a capital exponent and a moved point all denote the value they
    # parse to, so none of them is a refusal.
    payload = parse_digestable_document(
        '{"a":0.10,"b":1e-05,"c":1E5,"d":-0.0,"e":"0.1000000000000000055511151"}'
    )
    assert payload == {
        "a": 0.1,
        "b": 1e-05,
        "c": 100000.0,
        "d": -0.0,
        "e": "0.1000000000000000055511151",
    }


def test_a_duplicate_key_cannot_hide_a_refused_literal() -> None:
    # The parsed object keeps the last value, so the marker is gone and the path
    # cannot be named; the document is still refused rather than accepted.
    with pytest.raises(DigestDomainError) as caught:
        parse_digestable_document('{"a":0.1000000000000000055511151,"a":1}')
    assert caught.value.reason == "number-would-round"
    assert caught.value.path == "$"


@pytest.mark.parametrize(
    "document,reason,path",
    [
        ('{"a":[1,{"b":NaN}]}', "non-finite", "$.a[1].b"),
        ('{"a":[1,{"b":Infinity}]}', "non-finite", "$.a[1].b"),
        ('{"a":[1,{"b":-Infinity}]}', "non-finite", "$.a[1].b"),
        ('{"a":18446744073709551616}', "number-out-of-domain", "$.a"),
        ('{"a":-9223372036854775809}', "number-out-of-domain", "$.a"),
        ('{"a":1e400}', "non-finite", "$.a"),
        ('{"a":-1e400}', "non-finite", "$.a"),
        ('{"a":0.1000000000000000055511151}', "number-would-round", "$.a"),
        ('{"a":9007199254740993.0}', "number-would-round", "$.a"),
    ],
)
def test_the_ingress_names_the_rule_and_the_path(
    document: str, reason: str, path: str
) -> None:
    with pytest.raises(DigestDomainError) as caught:
        parse_digestable_document(document)
    assert caught.value.reason == reason
    assert caught.value.path == path
    assert caught.value.path in str(caught.value)


@pytest.mark.parametrize(
    "value,reason",
    [
        (-(2**63) - 1, "number-out-of-domain"),
        (2**64, "number-out-of-domain"),
        (float("nan"), "non-finite"),
        (float("inf"), "non-finite"),
        (float("-inf"), "non-finite"),
        (b"bytes", "unsupported-value"),
        (Decimal("1.5"), "unsupported-value"),
        ({1}, "unsupported-value"),
        ((1, 2), "unsupported-value"),
        (datetime(2026, 10, 1, tzinfo=timezone.utc), "unsupported-value"),
    ],
)
def test_out_of_domain_values_report_json_path(value: Any, reason: str) -> None:
    with pytest.raises(DigestDomainError) as caught:
        intent_digest({"facts": [{"value": value}]})
    assert caught.value.path == "$.facts[0].value"
    assert caught.value.reason == reason
    assert caught.value.path in str(caught.value)


def test_root_rejection_reports_root_path() -> None:
    with pytest.raises(DigestDomainError) as caught:
        intent_digest(2**64)
    assert caught.value.path == "$"
    assert caught.value.reason == "number-out-of-domain"
    assert "$" in str(caught.value)


@pytest.mark.parametrize("key", [1, False, None, ("tuple",)])
def test_non_string_keys_report_containing_object_path(key: Any) -> None:
    with pytest.raises(DigestDomainError) as caught:
        intent_digest({"facts": [{"value": {key: "not a JSON key"}}]})
    assert caught.value.path == "$.facts[0].value"
    assert caught.value.reason == "unsupported-value"
    assert caught.value.path in str(caught.value)


@pytest.mark.parametrize(
    "envelope_name,proposal_name",
    [
        ("decision-envelope.aborted.json", "decision-proposal.abort.json"),
        ("decision-envelope.escalated.json", "decision-proposal.escalate.json"),
    ],
)
def test_example_envelope_binds_proposal(envelope_name: str, proposal_name: str) -> None:
    envelope = json.loads((EXAMPLES_DIR / envelope_name).read_text())
    proposal = json.loads((EXAMPLES_DIR / proposal_name).read_text())
    assert envelope["validation"]["proposal_digest"] == intent_digest(proposal)
    assert envelope["proposal"] == {
        key: proposal[key] for key in ("proposal_id", "schema_version", "decision")
    }


def test_existing_demo_intent_digest_is_unchanged() -> None:
    payload = json.loads((EXAMPLES_DIR / "execution-intent.demo-verify.json").read_text())
    assert intent_digest(payload) == (
        "33e54b25448f8ddf9414c12cc6f9e5a5e7188447361bebe03ee79fb36fd54299"
    )
