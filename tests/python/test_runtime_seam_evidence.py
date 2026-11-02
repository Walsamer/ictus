"""Fail-closed tests for the attempt evidence contract and scope containment.

Evidence validation is shape-only: it never reinterprets what a runtime
reported, it preserves unknown fields for the runtime's own diagnostics, and it
rejects a missing, mistyped or self-contradictory field.  ``out_of_scope`` is the
syntactic, default-deny containment check the write scope is re-checked with.
"""

from __future__ import annotations

from typing import Any

import pytest

from ictus_dagster.seam.evidence import (
    EVIDENCE_SCHEMA_VERSION,
    FAILURE_CATEGORIES,
    FAILURE_KINDS,
    OUTCOMES,
    REQUIRED_FIELDS,
    EvidenceError,
    category_for_failure_kind,
    normalize_scope_path,
    out_of_scope,
    validate_evidence,
)

DIGEST = "sha256:" + "0123456789abcdef" * 4


def _evidence(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "outcome": "succeeded",
        "failure_kind": None,
        "changed_paths": ["src/a.py"],
        "patch_digest": DIGEST,
        "patch_bytes": 42,
        "steps_used": 2,
        "exit_status": {"code": 0, "signal": None},
        "termination": {"requested": False, "signals": [], "confirmed": False},
        "duration_seconds": 0.5,
    }
    document.update(overrides)
    return document


def _exit_status(**overrides: Any) -> dict[str, Any]:
    status: dict[str, Any] = {"code": 0, "signal": None}
    status.update(overrides)
    return status


def _termination(**overrides: Any) -> dict[str, Any]:
    termination: dict[str, Any] = {"requested": False, "signals": [], "confirmed": False}
    termination.update(overrides)
    return termination


def _failed(**overrides: Any) -> dict[str, Any]:
    document = _evidence(outcome="failed", failure_kind="PROCESS_EXIT")
    document.update(overrides)
    return document


# -- the accepted document ---------------------------------------------------


def test_a_complete_evidence_document_is_returned_unchanged() -> None:
    document = _evidence()
    assert validate_evidence(document) is document


def test_unknown_fields_are_preserved_for_the_runtime_diagnostics() -> None:
    diagnostics = {"vendor": "runtime-x", "timings": {"probe": 0.01}}
    document = _evidence(diagnostics=diagnostics)
    validated = validate_evidence(document)
    assert validated["diagnostics"] == diagnostics


@pytest.mark.parametrize("outcome", sorted(OUTCOMES))
def test_every_terminal_outcome_is_accepted(outcome: str) -> None:
    kind = None if outcome == "succeeded" else "PROCESS_EXIT"
    document = _evidence(outcome=outcome, failure_kind=kind)
    assert validate_evidence(document) is document


@pytest.mark.parametrize("kind", sorted(FAILURE_KINDS))
def test_every_typed_failure_kind_is_accepted(kind: str) -> None:
    assert validate_evidence(_failed(failure_kind=kind)) is not None


def test_an_empty_changed_paths_and_null_digest_are_valid() -> None:
    document = _evidence(changed_paths=[], patch_digest=None, patch_bytes=0)
    assert validate_evidence(document) is document


# -- document shape ----------------------------------------------------------


@pytest.mark.parametrize("field", sorted(REQUIRED_FIELDS))
def test_a_missing_required_field_fails_closed(field: str) -> None:
    document = _evidence()
    del document[field]
    with pytest.raises(EvidenceError, match=f"missing field\\(s\\): {field}"):
        validate_evidence(document)


@pytest.mark.parametrize(
    "document", [[], "evidence", 1, None], ids=["list", "string", "int", "null"]
)
def test_a_non_object_document_fails_closed(document: Any) -> None:
    with pytest.raises(EvidenceError, match="attempt evidence must be a JSON object"):
        validate_evidence(document)


@pytest.mark.parametrize(
    "version",
    [True, False, "1", 1.5, None],
    ids=["true", "false", "string", "float", "null"],
)
def test_a_non_integer_schema_version_fails_closed(version: Any) -> None:
    with pytest.raises(EvidenceError, match="evidence schema_version must be an integer"):
        validate_evidence(_evidence(schema_version=version))


@pytest.mark.parametrize("version", [2, 0, -1], ids=["two", "zero", "negative"])
def test_an_unsupported_schema_version_fails_closed(version: int) -> None:
    with pytest.raises(EvidenceError, match="unsupported evidence schema_version"):
        validate_evidence(_evidence(schema_version=version))


@pytest.mark.parametrize("outcome", ["success", "SUCCEEDED", "completed", None, 1])
def test_an_unknown_outcome_fails_closed(outcome: Any) -> None:
    with pytest.raises(EvidenceError, match="unsupported outcome"):
        validate_evidence(_evidence(outcome=outcome, failure_kind=None))


# -- failure kind / outcome coupling -----------------------------------------


def test_a_succeeded_attempt_must_not_report_a_failure_kind() -> None:
    with pytest.raises(EvidenceError, match="must not report a failure kind"):
        validate_evidence(_evidence(failure_kind="BACKEND_ERROR"))


@pytest.mark.parametrize("kind", [None, "NOT_A_KIND", "", 7])
def test_a_non_succeeded_attempt_requires_a_typed_failure_kind(kind: Any) -> None:
    with pytest.raises(EvidenceError, match="requires a typed failure kind"):
        validate_evidence(_evidence(outcome="failed", failure_kind=kind))


# -- changed paths, digest and sizes -----------------------------------------


@pytest.mark.parametrize(
    "changed_paths",
    ["src/a.py", ["src/a.py", 7], [""], [None], {"src/a.py"}],
    ids=["string", "non-string-entry", "empty-entry", "null-entry", "set"],
)
def test_malformed_changed_paths_fail_closed(changed_paths: Any) -> None:
    with pytest.raises(EvidenceError, match="changed_paths must be a list of non-empty strings"):
        validate_evidence(_evidence(changed_paths=changed_paths))


@pytest.mark.parametrize(
    "digest",
    [
        "sha256:" + "a" * 63,  # too short
        "sha256:" + "a" * 65,  # too long
        "sha256:" + "A" * 64,  # uppercase hex
        "md5:" + "a" * 32,  # wrong algorithm
        "a" * 64,  # no prefix
        "sha256:",
        7,
        ["sha256:" + "a" * 64],
    ],
    ids=["short", "long", "uppercase", "wrong-prefix", "no-prefix", "prefix-only", "int", "list"],
)
def test_a_malformed_patch_digest_fails_closed(digest: Any) -> None:
    with pytest.raises(EvidenceError, match="patch_digest must be null or"):
        validate_evidence(_evidence(patch_digest=digest))


@pytest.mark.parametrize("name", ["patch_bytes", "steps_used"])
@pytest.mark.parametrize(
    "value",
    [True, False, -1, 2.5, "3", None],
    ids=["true", "false", "negative", "float", "string", "null"],
)
def test_malformed_sizes_fail_closed(name: str, value: Any) -> None:
    with pytest.raises(EvidenceError, match=f"{name} must be a non-negative integer"):
        validate_evidence(_evidence(**{name: value}))


@pytest.mark.parametrize(
    ("name", "value"), [("patch_bytes", 0), ("steps_used", 0), ("steps_used", 3)]
)
def test_non_negative_integer_sizes_are_accepted(name: str, value: int) -> None:
    assert validate_evidence(_evidence(**{name: value})) is not None


@pytest.mark.parametrize(
    "duration",
    ["1.0", True, False, None, "nan", float("nan"), float("inf"), float("-inf"), -1, -0.5],
    ids=[
        "string",
        "true",
        "false",
        "null",
        "numeric-string",
        "nan",
        "inf",
        "-inf",
        "negative",
        "negative-float",
    ],
)
def test_a_malformed_duration_fails_closed(duration: Any) -> None:
    with pytest.raises(EvidenceError, match="duration_seconds must be a"):
        validate_evidence(_evidence(duration_seconds=duration))


@pytest.mark.parametrize(
    "duration", [0, 0.0, 1.5, 86_400], ids=["zero-int", "zero-float", "float", "large-int"]
)
def test_a_finite_non_negative_duration_is_accepted(duration: float) -> None:
    assert validate_evidence(_evidence(duration_seconds=duration)) is not None


# -- exit status and termination ---------------------------------------------


@pytest.mark.parametrize("exit_status", ["0", [], None, 0], ids=["string", "list", "null", "int"])
def test_a_non_object_exit_status_fails_closed(exit_status: Any) -> None:
    with pytest.raises(EvidenceError, match="exit_status must be a JSON object"):
        validate_evidence(_evidence(exit_status=exit_status))


@pytest.mark.parametrize("field", ["code", "signal"])
def test_an_exit_status_missing_a_field_fails_closed(field: str) -> None:
    status = _exit_status()
    del status[field]
    with pytest.raises(EvidenceError, match=f"exit_status is missing field\\(s\\): {field}"):
        validate_evidence(_evidence(exit_status=status))


@pytest.mark.parametrize("code", ["0", 1.5, True, [0]], ids=["string", "float", "bool", "list"])
def test_a_mistyped_exit_code_fails_closed(code: Any) -> None:
    with pytest.raises(EvidenceError, match="exit_status.code must be an integer or null"):
        validate_evidence(_evidence(exit_status=_exit_status(code=code)))


@pytest.mark.parametrize(
    "signal_name",
    [9, 1.5, ["SIGTERM"], {"name": "SIGTERM"}],
    ids=["int", "float", "list", "object"],
)
def test_a_mistyped_exit_signal_fails_closed(signal_name: Any) -> None:
    with pytest.raises(EvidenceError, match="exit_status.signal must be a string or null"):
        validate_evidence(_evidence(exit_status=_exit_status(signal=signal_name)))


@pytest.mark.parametrize(
    "status",
    [_exit_status(code=None, signal="SIGKILL"), _exit_status(code=137, signal=None)],
    ids=["signalled", "exited"],
)
def test_exit_status_null_code_or_signal_is_valid(status: dict[str, Any]) -> None:
    assert validate_evidence(_evidence(exit_status=status)) is not None


@pytest.mark.parametrize(
    "termination", ["none", [], None, True], ids=["string", "list", "null", "bool"]
)
def test_a_non_object_termination_fails_closed(termination: Any) -> None:
    with pytest.raises(EvidenceError, match="termination must be a JSON object"):
        validate_evidence(_evidence(termination=termination))


@pytest.mark.parametrize("field", ["requested", "signals", "confirmed"])
def test_a_termination_missing_a_field_fails_closed(field: str) -> None:
    termination = _termination()
    del termination[field]
    with pytest.raises(EvidenceError, match=f"termination is missing field\\(s\\): {field}"):
        validate_evidence(_evidence(termination=termination))


@pytest.mark.parametrize("name", ["requested", "confirmed"])
@pytest.mark.parametrize("value", ["true", 1, None], ids=["string", "int", "null"])
def test_a_non_boolean_termination_flag_fails_closed(name: str, value: Any) -> None:
    with pytest.raises(EvidenceError, match=f"termination.{name} must be a boolean"):
        validate_evidence(_evidence(termination=_termination(**{name: value})))


@pytest.mark.parametrize(
    "signals",
    ["SIGTERM", [9], {"signals": ["SIGTERM"]}],
    ids=["string", "non-string-entry", "object"],
)
def test_malformed_termination_signals_fail_closed(signals: Any) -> None:
    with pytest.raises(EvidenceError, match="termination.signals must be a list of strings"):
        validate_evidence(_evidence(termination=_termination(signals=signals)))


def test_a_confirmed_termination_is_accepted() -> None:
    termination = _termination(requested=True, signals=["SIGTERM", "SIGKILL"], confirmed=True)
    assert validate_evidence(_evidence(termination=termination)) is not None


# -- failure categories ------------------------------------------------------


def test_every_failure_kind_has_exactly_one_category() -> None:
    assert set(FAILURE_CATEGORIES) == FAILURE_KINDS


@pytest.mark.parametrize("kind", sorted(FAILURE_KINDS))
def test_every_failure_kind_maps_to_a_generic_category(kind: str) -> None:
    category = category_for_failure_kind(kind)
    assert category == FAILURE_CATEGORIES[kind]
    assert category and category == category.upper()


@pytest.mark.parametrize("kind", ["NOT_A_KIND", "", "process_exit"])
def test_an_unknown_failure_kind_has_no_category(kind: str) -> None:
    with pytest.raises(EvidenceError, match="unknown failure kind"):
        category_for_failure_kind(kind)


# -- scope containment -------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("./src/a.py", "src/a.py"),
        ("././src/a.py", "src/a.py"),
        ("src/a.py/", "src/a.py"),
        ("src/", "src"),
        # A trailing-slash scope collapses to the empty prefix: the filesystem
        # root contains every absolute path (see the containment cases below).
        ("/", ""),
        ("//", ""),
        ("./", ""),
        ("src/a.py", "src/a.py"),
    ],
)
def test_normalize_scope_path_strips_dot_prefixes_and_trailing_slashes(
    path: str, expected: str
) -> None:
    assert normalize_scope_path(path) == expected


@pytest.mark.parametrize(
    ("changed_paths", "write_scope", "outside"),
    [
        # Exact match and nested containment.
        (["src/a.py"], ("src",), []),
        (["src/deep/a.py"], ("src",), []),
        # The changed path may carry "./" and a trailing slash.
        (["./src/a.py"], ("src",), []),
        (["src/a.py"], ("./src",), []),
        (["src/a.py"], ("src/",), []),
        (["src/"], ("src",), []),
        # A scope entry matches as a path segment, never as a string prefix.
        (["srcx/a.py"], ("src",), ["srcx/a.py"]),
        (["docs/a.md"], ("src",), ["docs/a.md"]),
        # Default deny: a path outside every entry is reported, unchanged.
        (["src/a.py", "Cargo.toml"], ("src",), ["Cargo.toml"]),
        (["src/a.py", "tests/b.py"], ("src", "tests"), []),
        # Root scope normalizes to the empty prefix, so it contains every
        # absolute path and no relative one.
        (["/"], ("/",), []),
        (["/src/a.py"], ("/",), []),
        (["src/a.py"], ("/",), ["src/a.py"]),
        # No changed paths means nothing is out of scope.
        ([], ("src",), []),
    ],
)
def test_out_of_scope_is_syntactic_and_default_deny(
    changed_paths: list[str], write_scope: tuple[str, ...], outside: list[str]
) -> None:
    assert out_of_scope(changed_paths, write_scope) == outside
