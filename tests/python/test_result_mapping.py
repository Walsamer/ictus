"""Dagster run outcome -> generic ExecutionResult mapping."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from ictus_dagster.adapters.contracts import ContractError, validate_result
from ictus_dagster.adapters.result_mapping import execution_result_from_run
from conftest import CONTRACTS_DIR


def _result_schema() -> Draft202012Validator:
    schemas = {
        path.name: json.loads(path.read_text())
        for path in CONTRACTS_DIR.glob("*.schema.json")
    }
    registry = Registry().with_resources(
        [(s["$id"], Resource.from_contents(s)) for s in schemas.values()]
    )
    return Draft202012Validator(
        schemas["execution-result.schema.json"], registry=registry
    )


def test_success_maps_to_success_category() -> None:
    payload = execution_result_from_run(
        execution_id="exec-1",
        intent_id="intent-1",
        success=True,
    )
    assert payload["status"] == "succeeded"
    assert payload["observation"]["category"] == "SUCCESS"
    _result_schema().validate(payload)


def test_failed_execute_step_maps_to_process_crash() -> None:
    payload = execution_result_from_run(
        execution_id="exec-2",
        intent_id="intent-2",
        success=False,
        failed_step="execute_op",
    )
    assert payload["status"] == "failed"
    assert payload["observation"]["category"] == "PROCESS_CRASH"


def test_failed_verify_step_maps_to_verification_failure() -> None:
    payload = execution_result_from_run(
        execution_id="exec-3",
        intent_id="intent-3",
        success=False,
        failed_step="verify_op",
    )
    assert payload["observation"]["category"] == "VERIFICATION_FAILURE"


def test_explicit_failure_category_wins() -> None:
    payload = execution_result_from_run(
        execution_id="exec-4",
        intent_id="intent-4",
        success=False,
        failed_step="execute_op",
        failure_category="WORKER_TIMEOUT",
    )
    assert payload["observation"]["category"] == "WORKER_TIMEOUT"


def test_result_always_carries_intent_id_and_schema_version() -> None:
    payload = execution_result_from_run(
        execution_id="exec-5", intent_id="intent-5", success=True
    )
    assert payload["intent_id"] == "intent-5"
    assert payload["schema_version"] == 1


def test_invalid_category_is_rejected() -> None:
    with pytest.raises(ContractError):
        validate_result(
            {
                "schema_version": 1,
                "execution_id": "e",
                "intent_id": "i",
                "status": "succeeded",
                "observation": {"category": "BOGUS"},
            }
        )
