"""Tests for standard inter-brick contracts."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from brix.bricks.contracts import (
    contract_names,
    get_contract,
    is_dict_like_contract,
    is_list_like_contract,
    list_contracts,
)
from brix.bricks.types import _get_type_compatibility, is_compatible
from brix.models import Pipeline, Step
from brix.validator import PipelineValidator


def test_standard_contract_catalog_contains_core_domains() -> None:
    names = set(contract_names())

    assert {
        "file_blob",
        "remote_download_result",
        "document_extract_input",
        "document_extraction_result",
        "db_query_result",
        "db_mutation_result",
        "classification_result",
    }.issubset(names)
    assert [contract.name for contract in list_contracts()] == sorted(names)


def test_contracts_are_lookupable_and_classified() -> None:
    document_result = get_contract("Document_Extraction_Result")
    assert document_result is not None
    assert document_result.required_fields == ("normalized",)

    assert is_dict_like_contract("classification_result") is True
    assert is_dict_like_contract("db_mutation_result") is True
    assert is_list_like_contract("db_query_result") is True
    assert is_list_like_contract("document_extraction_result") is False


def test_standard_contract_type_compatibility() -> None:
    assert is_compatible("remote_download_result", "document_extract_input") is True
    assert is_compatible("file_blob", "document_extract_input") is True
    assert is_compatible("document_extraction_result", "dict") is True
    assert is_compatible("classification_result", "object") is True
    assert is_compatible("db_query_result", "list[dict]") is True
    assert is_compatible("db_mutation_result", "list[dict]") is False


def test_db_type_compatibility_extends_builtin_contracts(monkeypatch) -> None:
    db = MagicMock()
    db.type_compatibility_count.return_value = 1
    db.type_compatibility_as_dict.return_value = {"custom_payload": ["document_extract_input"]}

    monkeypatch.setattr("brix.db.BrixDB", lambda: db)
    table = _get_type_compatibility()

    assert "document_extract_input" in table["remote_download_result"]
    assert table["custom_payload"] == ["document_extract_input"]


def test_validator_treats_domain_contracts_as_container_shapes() -> None:
    steps = [
        Step(id="read", type="db.query"),
        Step(id="persist", type="db.upsert", params={"data": "{{ read.output }}"}),
        Step(id="extract", type="flow.set"),
        Step(
            id="lookup",
            type="db.query",
            config={"connection": "main", "query": "SELECT * FROM t WHERE c = :c", "params": "{{ extract.output }}"},
        ),
    ]

    def _get(name: str, _seen=None):
        brick = MagicMock()
        brick.output_type = {
            "db.query": "db_query_result",
            "flow.set": "document_extraction_result",
            "db.upsert": "db_mutation_result",
        }.get(name, "")
        return brick if brick.output_type else None

    with (
        patch("brix.validator.PipelineValidator._check_sub_pipeline_existence", lambda self, ctx, result: None),
        patch("brix.validator.PipelineValidator._check_connection_existence", lambda self, ctx, result: None),
        patch("brix.validator.PipelineValidator._check_brick_config_schema", lambda self, ctx, result: None),
        patch("brix.validator.PipelineValidator._check_jinja_ast", lambda self, ctx, result: None),
        patch("brix.bricks.registry.BrickRegistry.get", side_effect=_get),
    ):
        result = PipelineValidator().validate(Pipeline(name="contracts", steps=steps), level="standard")

    assert [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning] == []
