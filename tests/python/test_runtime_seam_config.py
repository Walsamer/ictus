"""Fail-closed tests for the runtime seam configuration loader.

Every case here is a way an operator-owned document can be unusable: an
unreadable file, a document that is not an object, a missing or unknown field, a
wrong or non-integer version, a backend that is not a ``module:attribute``
reference, an empty write scope and a budget that is missing, mistyped or out of
bounds.  The loader must reject each of them with :class:`SeamConfigError` rather
than fall back to the unconfigured default, and it must resolve a relative
workspace descriptor against the config file's own directory.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ictus_dagster.seam.config import (
    BACKEND_REFERENCE,
    BOUNDS_FIELDS,
    CONFIG_FIELDS,
    MAX_STEP_BUDGET,
    MAX_WALL_CLOCK_SECONDS,
    SEAM_CONFIG_VERSION,
    WORKSPACE_FIELDS,
    SeamConfig,
    SeamConfigError,
    load_seam_config,
    parse_seam_config,
)
from ictus_dagster.seam.port import RuntimeBackend, WorkspaceRef
from ictus_dagster.seam.runtime import resolve_backend

BACKEND = "stub_runtime_backend:create_backend"


def _bounds(**overrides: Any) -> dict[str, Any]:
    bounds: dict[str, Any] = {
        "write_scope": ["src"],
        "wall_clock_seconds": 600,
        "step_budget": 200,
    }
    bounds.update(overrides)
    return bounds


def _document(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": SEAM_CONFIG_VERSION,
        "backend": BACKEND,
        "workspace": {"descriptor": "runtime/workspace.json"},
        "bounds": _bounds(),
    }
    document.update(overrides)
    return document


def _without(document: dict[str, Any], *fields: str) -> dict[str, Any]:
    trimmed = dict(document)
    for field in fields:
        del trimmed[field]
    return trimmed


def _write_config(path: Path, document: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


# -- the unconfigured default ------------------------------------------------


@pytest.mark.parametrize("path", [None, "", "   ", "\n\t "])
def test_an_absent_configuration_keeps_the_seam_closed(path: str | None) -> None:
    assert load_seam_config(path) is None


@pytest.mark.parametrize(
    ("contents", "match"),
    [
        (None, "cannot be read"),
        ("{", "is not valid JSON"),
        ("[]", "runtime seam config must be a JSON object"),
        ('"seam"', "runtime seam config must be a JSON object"),
        ("null", "runtime seam config must be a JSON object"),
        (json.dumps({"schema_version": 1}), "missing field"),
    ],
    ids=["missing-file", "invalid-json", "list", "string", "null", "missing-fields"],
)
def test_an_unusable_config_file_fails_closed(
    tmp_path: Path, contents: str | None, match: str
) -> None:
    path = tmp_path / "config" / "runtime-seam.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if contents is not None:
        path.write_text(contents, encoding="utf-8")
    with pytest.raises(SeamConfigError, match=match):
        load_seam_config(str(path))


def test_a_directory_is_not_a_readable_configuration(tmp_path: Path) -> None:
    with pytest.raises(SeamConfigError, match="cannot be read"):
        load_seam_config(str(tmp_path))


# -- document shape ----------------------------------------------------------


@pytest.mark.parametrize(
    ("document", "match"),
    [
        pytest.param(
            _without(_document(), "backend"), "missing field\\(s\\): backend", id="missing-backend"
        ),
        pytest.param(
            _without(_document(), "workspace", "bounds"),
            "missing field\\(s\\): bounds, workspace",
            id="missing-workspace-and-bounds",
        ),
        pytest.param(
            _document(extra="x"),
            "unknown field\\(s\\): extra",
            id="unknown-top-level-field",
        ),
        pytest.param(
            _document(schema_version=2),
            "unsupported runtime seam config schema_version 2",
            id="wrong-version",
        ),
        pytest.param(
            _document(schema_version=True),
            "schema_version must be an integer, not True",
            id="boolean-version",
        ),
        pytest.param(
            _document(schema_version="1"),
            "schema_version must be an integer, not '1'",
            id="string-version",
        ),
        pytest.param(
            _document(schema_version=1.0),
            "schema_version must be an integer, not 1.0",
            id="float-version",
        ),
        pytest.param(
            _document(schema_version=None),
            "schema_version must be an integer, not None",
            id="null-version",
        ),
        pytest.param(
            _document(workspace=[]), "workspace must be a JSON object", id="workspace-list"
        ),
        pytest.param(
            _document(workspace="p"), "workspace must be a JSON object", id="workspace-string"
        ),
        pytest.param(
            _document(workspace={}),
            "workspace is missing field\\(s\\): descriptor",
            id="workspace-missing-descriptor",
        ),
        pytest.param(
            _document(workspace={"descriptor": "p", "probe": "x"}),
            "workspace carries unknown field\\(s\\): probe",
            id="workspace-unknown-field",
        ),
        pytest.param(_document(bounds=[]), "bounds must be a JSON object", id="bounds-list"),
        pytest.param(
            _document(bounds={"write_scope": ["src"]}),
            "bounds is missing field\\(s\\): step_budget, wall_clock_seconds",
            id="bounds-missing-fields",
        ),
        pytest.param(
            _document(bounds=_bounds(retries=3)),
            "bounds carries unknown field\\(s\\): retries",
            id="bounds-unknown-field",
        ),
    ],
)
def test_a_malformed_document_fails_closed(document: dict[str, Any], match: str) -> None:
    with pytest.raises(SeamConfigError, match=match):
        parse_seam_config(document, source="runtime-seam.json")


@pytest.mark.parametrize("document", [[], "seam", 1, None], ids=["list", "string", "int", "null"])
def test_a_non_object_document_fails_closed(document: Any) -> None:
    with pytest.raises(SeamConfigError, match="runtime seam config must be a JSON object"):
        parse_seam_config(document, source="runtime-seam.json")


# -- the backend reference ---------------------------------------------------


@pytest.mark.parametrize(
    "backend",
    [
        "stub_runtime_backend",  # no colon
        "stub_runtime_backend:create_backend:extra",  # two colons
        " stub_runtime_backend:create_backend",  # leading space
        "stub_runtime_backend : create_backend",  # spaces around the colon
        ":create_backend",  # empty module
        "stub_runtime_backend:",  # empty attribute
        "1stub:create_backend",  # module identifier starts with a digit
        "stub:create-backend",  # attribute is not an identifier
        "stub..other:create_backend",  # empty dotted segment
        "stub:create_backend\n",  # trailing newline is not part of the reference
        42,
        None,
        ["stub:create_backend"],
    ],
    ids=[
        "no-colon",
        "two-colons",
        "leading-space",
        "spaces-around-colon",
        "empty-module",
        "empty-attribute",
        "digit-module",
        "dashed-attribute",
        "dotted-empty-segment",
        "trailing-newline",
        "int",
        "null",
        "list",
    ],
)
def test_an_invalid_backend_reference_fails_closed(backend: Any) -> None:
    with pytest.raises(SeamConfigError, match="module:attribute"):
        parse_seam_config(_document(backend=backend), source="runtime-seam.json")


@pytest.mark.parametrize(
    "backend",
    ["stub:create_backend", "tests.python.stub_runtime_backend:create_backend", "_a._b:c_d"],
)
def test_a_valid_backend_reference_is_accepted(backend: str) -> None:
    config = parse_seam_config(_document(backend=backend), source="runtime-seam.json")
    assert config.backend == backend
    # The loader names the backend; it never imports it.
    assert BACKEND_REFERENCE.fullmatch(config.backend) is not None


# -- resolving the named backend --------------------------------------------
# The reference is checked by the loader; resolving it is what actually imports
# and constructs the runtime, so every way it can fail must fail closed too.

SUPPORT_MODULE = "runtime_seam_support"


@pytest.mark.parametrize(
    ("reference", "match"),
    [
        pytest.param(
            "ictus_dagster_no_such_backend_module:create_backend",
            "cannot be imported",
            id="missing-module",
        ),
        pytest.param(
            "stub_runtime_backend:no_such_factory",
            "has no attribute 'no_such_factory'",
            id="missing-attribute",
        ),
        pytest.param(
            "stub_runtime_backend:PATCH_TEXT",
            "cannot be constructed",
            id="attribute-is-not-callable",
        ),
        pytest.param(
            f"{SUPPORT_MODULE}:broken_factory", "cannot be constructed", id="factory-raises"
        ),
        pytest.param(
            f"{SUPPORT_MODULE}:not_a_backend",
            "does not implement the seam port",
            id="not-a-backend",
        ),
    ],
)
def test_an_unresolvable_backend_fails_closed(reference: str, match: str) -> None:
    with pytest.raises(SeamConfigError, match=match):
        resolve_backend(reference)


def test_a_working_reference_resolves_to_a_usable_backend(tmp_path: Path) -> None:
    descriptor = tmp_path / "workspace.json"
    descriptor.write_text(
        json.dumps(
            {
                "executable": "/usr/local/bin/stub-runtime",
                "version_token": "stub-1.0.0",
                "probe": {"ok": True, "detail": "stub probe ok"},
            }
        ),
        encoding="utf-8",
    )

    backend = resolve_backend("stub_runtime_backend:create_backend")

    assert isinstance(backend, RuntimeBackend)
    report = backend.describe_workspace(WorkspaceRef(descriptor_path=str(descriptor)))
    assert report["version_token"] == "stub-1.0.0"
    assert report["probe"]["ok"] is True


# -- the workspace descriptor ------------------------------------------------


@pytest.mark.parametrize(
    "descriptor", ["", "   ", 7, None, ["runtime/workspace.json"]],
    ids=["empty", "blank", "int", "null", "list"],
)
def test_an_unusable_workspace_descriptor_fails_closed(descriptor: Any) -> None:
    document = _document(workspace={"descriptor": descriptor})
    with pytest.raises(SeamConfigError, match="workspace.descriptor must be a non-empty path"):
        parse_seam_config(document, source="runtime-seam.json")


def test_a_relative_descriptor_resolves_against_the_config_directory() -> None:
    document = _document(workspace={"descriptor": "runtime/workspace.json"})
    config = parse_seam_config(document, source="/srv/seam/config/seam.json")
    assert config.workspace_descriptor == "/srv/seam/config/runtime/workspace.json"
    assert Path(config.workspace_descriptor).is_absolute()


def test_an_absolute_descriptor_is_passed_through_unchanged() -> None:
    document = _document(workspace={"descriptor": "/opt/runtime/workspace.json"})
    config = parse_seam_config(document, source="/srv/seam/seam.json")
    assert config.workspace_descriptor == "/opt/runtime/workspace.json"


def test_the_descriptor_is_never_read(tmp_path: Path) -> None:
    # The loader passes the path through; a descriptor that does not exist is
    # still a valid configuration (only the runtime may read the file).
    path = _write_config(
        tmp_path / "seam.json", _document(workspace={"descriptor": "absent.json"})
    )
    config = load_seam_config(str(path))
    assert config is not None
    assert config.workspace_descriptor == str(tmp_path / "absent.json")
    assert not Path(config.workspace_descriptor).exists()


# -- bounds ------------------------------------------------------------------


@pytest.mark.parametrize(
    "write_scope",
    [[], "src", ["src", 3], ["src", ""], ["src", "   "], [""], ["src", None], None],
    ids=[
        "empty-list",
        "not-a-list",
        "non-string-entry",
        "empty-string-entry",
        "blank-entry",
        "only-empty-string",
        "null-entry",
        "null",
    ],
)
def test_an_unusable_write_scope_fails_closed(write_scope: Any) -> None:
    document = _document(bounds=_bounds(write_scope=write_scope))
    with pytest.raises(SeamConfigError, match="write_scope must be a non-empty list"):
        parse_seam_config(document, source="runtime-seam.json")


@pytest.mark.parametrize(
    "seconds",
    [
        True,
        False,
        "600",
        None,
        0,
        0.0,
        -1,
        -0.5,
        float("nan"),
        float("inf"),
        float("-inf"),
        MAX_WALL_CLOCK_SECONDS + 1,
    ],
    ids=[
        "true",
        "false",
        "string",
        "null",
        "zero",
        "zero-float",
        "negative-int",
        "negative-float",
        "nan",
        "inf",
        "-inf",
        "above-maximum",
    ],
)
def test_an_unsupported_wall_clock_fails_closed(seconds: Any) -> None:
    document = _document(bounds=_bounds(wall_clock_seconds=seconds))
    with pytest.raises(SeamConfigError, match="bounds.wall_clock_seconds must be"):
        parse_seam_config(document, source="runtime-seam.json")


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(600, 600.0), (600.5, 600.5), (0.05, 0.05), (MAX_WALL_CLOCK_SECONDS, MAX_WALL_CLOCK_SECONDS)],
    ids=["int", "float", "sub-second", "maximum"],
)
def test_a_supported_wall_clock_is_accepted_and_normalized(
    seconds: int | float, expected: float
) -> None:
    config = parse_seam_config(
        _document(bounds=_bounds(wall_clock_seconds=seconds)), source="runtime-seam.json"
    )
    assert config.wall_clock_seconds == expected
    assert isinstance(config.wall_clock_seconds, float)


@pytest.mark.parametrize(
    "budget",
    [True, False, 2.0, "200", None, 0, -1, MAX_STEP_BUDGET + 1],
    ids=[
        "true",
        "false",
        "float",
        "string",
        "null",
        "zero",
        "negative",
        "above-maximum",
    ],
)
def test_an_unsupported_step_budget_fails_closed(budget: Any) -> None:
    document = _document(bounds=_bounds(step_budget=budget))
    with pytest.raises(SeamConfigError, match="bounds.step_budget must be"):
        parse_seam_config(document, source="runtime-seam.json")


@pytest.mark.parametrize("budget", [1, 200, MAX_STEP_BUDGET])
def test_a_supported_step_budget_is_accepted(budget: int) -> None:
    config = parse_seam_config(
        _document(bounds=_bounds(step_budget=budget)), source="runtime-seam.json"
    )
    assert config.step_budget == budget


def test_a_valid_configuration_is_parsed_field_by_field() -> None:
    document = _document(
        workspace={"descriptor": "runtime/workspace.json"},
        bounds=_bounds(write_scope=["src", "tests/"], wall_clock_seconds=600, step_budget=200),
    )
    config = parse_seam_config(document, source="/srv/seam/seam.json")
    assert config == SeamConfig(
        backend=BACKEND,
        workspace_descriptor="/srv/seam/runtime/workspace.json",
        write_scope=("src", "tests/"),
        wall_clock_seconds=600.0,
        step_budget=200,
        path="/srv/seam/seam.json",
    )


# -- the resolved document ---------------------------------------------------


def test_to_json_reports_the_resolved_configuration(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "seam.json",
        _document(
            workspace={"descriptor": "runtime/workspace.json"},
            bounds=_bounds(write_scope=["src", "tests"], wall_clock_seconds=600.5, step_budget=7),
        ),
    )
    config = load_seam_config(str(path))
    assert config is not None
    assert config.to_json() == {
        "schema_version": SEAM_CONFIG_VERSION,
        "backend": BACKEND,
        "workspace": {"descriptor": str(tmp_path / "runtime" / "workspace.json")},
        "bounds": {
            "write_scope": ["src", "tests"],
            "wall_clock_seconds": 600.5,
            "step_budget": 7,
        },
        "source": str(path),
    }


def test_to_json_round_trips_through_the_parser(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "seam.json", _document())
    config = load_seam_config(str(path))
    assert config is not None
    reported = config.to_json()
    # "source" is the loader's own provenance note, not an operator field.
    assert set(reported) == CONFIG_FIELDS | {"source"}
    operator_document = {name: value for name, value in reported.items() if name != "source"}
    reparsed = parse_seam_config(operator_document, source=str(config.workspace_descriptor))
    assert reparsed.backend == config.backend
    assert reparsed.workspace_descriptor == config.workspace_descriptor
    assert reparsed.write_scope == config.write_scope
    assert reparsed.wall_clock_seconds == config.wall_clock_seconds
    assert reparsed.step_budget == config.step_budget


def test_the_published_field_sets_are_the_module_constants() -> None:
    # test_runtime_seam_contracts.py compares these with the published schema;
    # this pins the constants themselves so a drift shows up in one place.
    assert CONFIG_FIELDS == frozenset({"schema_version", "backend", "workspace", "bounds"})
    assert WORKSPACE_FIELDS == frozenset({"descriptor"})
    assert BOUNDS_FIELDS == frozenset({"write_scope", "wall_clock_seconds", "step_budget"})
    assert SEAM_CONFIG_VERSION == 1
