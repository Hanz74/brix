from __future__ import annotations

from brix.db import BrixDB
from brix.hmk_refactor import rewrite_hmk_save_results_to_persistence_brick
from brix.validator import PipelineValidator
from brix.pipeline_store import PipelineStore


def _hmk_single_pipeline() -> dict:
    return {
        "name": "buddy-hmk-extract-single",
        "version": "1.6.4",
        "description": "HMK single-doc pipeline fixture",
        "project": "buddy",
        "tags": ["extraction", "hmk", "brick-first"],
        "input": {
            "item": {"type": "object", "default": None, "description": "document row"},
            "language": {"type": "str", "default": "de", "description": "language"},
        },
        "output": {
            "doc_id": "{{ input.item.id }}",
            "status": "{{ 'ok' if save_results.output is defined else 'marked_only' }}",
        },
        "steps": [
            {
                "id": "get_download_url",
                "type": "mcp.call",
                "config": {"server": "m365", "tool": "download-onedrive-file-content", "params": {"driveId": "me"}},
                "params": {"driveId": "me"},
            },
            {
                "id": "download_save",
                "type": "script.python",
                "config": {
                    "helper": "att_onedrive_save",
                    "params": {"file_name": "{{ input.item.file_name | default('') }}"},
                },
                "params": {"file_name": "{{ input.item.file_name | default('') }}"},
            },
            {
                "id": "extract",
                "type": "flow.pipeline",
                "config": {
                    "pipeline": "buddy-daigestr-extract",
                    "params": {"base64_data": "{{ read_file_b64.output.base64 | default('') }}"},
                },
                "params": {"base64_data": "{{ read_file_b64.output.base64 | default('') }}"},
                "when": "{{ read_file_b64.output is defined }}",
            },
            {
                "id": "save_results",
                "type": "db.exec",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "UPDATE documents SET raw_structured = '{{ extract.output | tojson | "
                        "replace(\"'\", \"''\") }}'::jsonb, doc_type = "
                        "COALESCE(NULLIF('{{ extract.output.document_type | default(\"\") }}', ''), doc_type), "
                        "content_hash = COALESCE(NULLIF('{{ download_save.output.content_hash | default(\"\") }}', ''), "
                        "content_hash), file_path = COALESCE(NULLIF('{{ download_save.output.file_bytes_path | default(\"\") }}', ''), "
                        "file_path) WHERE id = {{ input.item.id }}::int RETURNING id"
                    ),
                    "params": [],
                },
                "params": [],
                "when": "{{ extract.output is defined }}",
            },
            {
                "id": "mark_processed",
                "type": "db.exec",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "UPDATE documents SET extraction_specialists = "
                        "array_append(COALESCE(extraction_specialists, ARRAY[]::text[]), 'hmk_extracted') "
                        "WHERE id = {{ input.item.id }}::int"
                    ),
                    "params": [],
                },
                "params": [],
                "when": "{{ save_results.output is not defined }}",
            },
        ],
    }


def test_rewrite_hmk_save_results_to_persistence_brick(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    updated = rewrite_hmk_save_results_to_persistence_brick(db=db)

    steps = updated["steps"]
    save_results = next(step for step in steps if step["id"] == "save_results")
    mark_processed = next(step for step in steps if step["id"] == "mark_processed")

    assert save_results["type"] == "document.persist_extraction_result"
    assert save_results["config"] == {
        "connection": "buddy-db",
        "document_id": "{{ input.item.id }}",
        "extraction_result": "{{ extract.output | tojson }}",
        "content_hash": "{{ download_save.output.content_hash | default('') }}",
        "file_path": "{{ download_save.output.file_bytes_path | default('') }}",
    }
    assert save_results["params"] == save_results["config"]
    assert "replace(\"'\", \"''\")" not in str(save_results.get("config", {}).get("query", ""))
    assert mark_processed["type"] == "db.exec"


def test_rewrite_hmk_save_results_is_idempotent(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    rewrite_hmk_save_results_to_persistence_brick(db=db)
    updated = rewrite_hmk_save_results_to_persistence_brick(db=db)

    save_results = next(step for step in updated["steps"] if step["id"] == "save_results")
    assert save_results["type"] == "document.persist_extraction_result"
    assert save_results["params"] == save_results["config"]


def test_rewrite_hmk_save_results_validates_without_object_type_warning(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    rewrite_hmk_save_results_to_persistence_brick(db=db)
    pipeline = store.load("buddy-hmk-extract-single")
    result = PipelineValidator().validate(pipeline)

    assert not any(
        finding.code == "SCHEMA_TYPE_MISMATCH"
        and finding.step_id == "save_results"
        and finding.field == "extraction_result"
        for finding in result.findings
    )
