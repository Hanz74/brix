from __future__ import annotations

from brix.db import BrixDB
from brix.hmk_refactor import (
    promote_hmk_to_document_persistence_bricks,
    register_hmk_prior_case_metadata,
    standardize_hmk_extract_flow,
    standardize_hmk_download_flow,
    rewrite_hmk_mark_processed_to_specialist_brick,
    rewrite_hmk_save_results_to_persistence_brick,
)
from brix.validator import PipelineValidator
from brix.pipeline_store import PipelineStore
from brix.semantic_retrieval import semantic_search


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
                "id": "read_file_b64",
                "type": "file.read_base64",
                "config": {"path": "{{ download_save.output.file_bytes_path }}"},
                "params": {"path": "{{ download_save.output.file_bytes_path }}"},
                "when": "{{ download_save.output.extractable | default(false) }}",
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


def test_rewrite_hmk_mark_processed_to_specialist_brick(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    updated = rewrite_hmk_mark_processed_to_specialist_brick(db=db)

    mark_processed = next(step for step in updated["steps"] if step["id"] == "mark_processed")
    assert mark_processed["type"] == "document.mark_specialist_processed"
    assert mark_processed["config"] == {
        "connection": "buddy-db",
        "document_id": "{{ input.item.id }}",
        "specialist_name": "hmk_extracted",
    }
    assert mark_processed["params"] == mark_processed["config"]


def test_rewrite_hmk_mark_processed_is_idempotent(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    rewrite_hmk_mark_processed_to_specialist_brick(db=db)
    updated = rewrite_hmk_mark_processed_to_specialist_brick(db=db)

    mark_processed = next(step for step in updated["steps"] if step["id"] == "mark_processed")
    assert mark_processed["type"] == "document.mark_specialist_processed"


def test_promote_hmk_to_document_persistence_bricks(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    updated = promote_hmk_to_document_persistence_bricks(db=db)

    save_results = next(step for step in updated["steps"] if step["id"] == "save_results")
    mark_processed = next(step for step in updated["steps"] if step["id"] == "mark_processed")

    assert save_results["type"] == "document.persist_extraction_result"
    assert mark_processed["type"] == "document.mark_specialist_processed"
    assert not any(
        step["type"] == "db.exec" and step["id"] in {"save_results", "mark_processed"}
        for step in updated["steps"]
    )


def test_promote_hmk_preserves_existing_connection_name(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    pipeline = _hmk_single_pipeline()
    for step in pipeline["steps"]:
        if step["id"] in {"save_results", "mark_processed"}:
            step["config"]["connection"] = "custom-docs-db"
    store.save(pipeline)

    updated = promote_hmk_to_document_persistence_bricks(db=db)

    save_results = next(step for step in updated["steps"] if step["id"] == "save_results")
    mark_processed = next(step for step in updated["steps"] if step["id"] == "mark_processed")
    assert save_results["config"]["connection"] == "custom-docs-db"
    assert mark_processed["config"]["connection"] == "custom-docs-db"


def test_standardize_hmk_extract_flow(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    updated = standardize_hmk_extract_flow(db=db)

    prepare = next(step for step in updated["steps"] if step["id"] == "prepare_extractable")
    extract = next(step for step in updated["steps"] if step["id"] == "extract")

    assert prepare["type"] == "document.prepare_extractable_payload"
    assert prepare["config"]["include_base64"] is True
    assert extract["type"] == "extract.document_with_daigestr"
    assert extract["config"]["file_bytes_path"] == "{{ prepare_extractable.output.file_bytes_path }}"
    assert extract["when"] == "{{ prepare_extractable.output is defined }}"
    assert not any(step["id"] == "read_file_b64" for step in updated["steps"])


def test_standardize_hmk_extract_flow_is_idempotent(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    standardize_hmk_extract_flow(db=db)
    updated = standardize_hmk_extract_flow(db=db)

    prepare = next(step for step in updated["steps"] if step["id"] == "prepare_extractable")
    extract = next(step for step in updated["steps"] if step["id"] == "extract")
    assert prepare["type"] == "document.prepare_extractable_payload"
    assert extract["type"] == "extract.document_with_daigestr"


def test_standardize_hmk_download_flow(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    updated = standardize_hmk_download_flow(db=db)

    download_save = next(step for step in updated["steps"] if step["id"] == "download_save")
    assert download_save["type"] == "source.download_to_file"
    assert download_save["config"] == {
        "url": "{{ get_download_url.output['@microsoft.graph.downloadUrl'] | default('') }}",
        "filename": "{{ input.item.file_name | default('') }}",
    }
    assert download_save["params"] == download_save["config"]
    assert download_save.get("helper") is None


def test_standardize_hmk_download_flow_is_idempotent(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    standardize_hmk_download_flow(db=db)
    updated = standardize_hmk_download_flow(db=db)

    download_save = next(step for step in updated["steps"] if step["id"] == "download_save")
    assert download_save["type"] == "source.download_to_file"


def test_fully_standardized_hmk_has_no_non_info_findings_on_standardized_steps(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    standardize_hmk_download_flow(db=db)
    standardize_hmk_extract_flow(db=db)
    promote_hmk_to_document_persistence_bricks(db=db)

    pipeline = store.load("buddy-hmk-extract-single")
    result = PipelineValidator().validate(pipeline)

    assert not any(
        finding.step_id in {"download_save", "prepare_extractable", "extract"}
        and finding.severity != "info"
        for finding in result.findings
    )


def test_register_hmk_prior_case_metadata_links_pipeline_bricks_and_semantics(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    standardize_hmk_download_flow(db=db)
    standardize_hmk_extract_flow(db=db)
    promote_hmk_to_document_persistence_bricks(db=db)

    result = register_hmk_prior_case_metadata(db=db)

    assert result["entry"]["name"] == "hmk-anchor-refactor-prior-case"
    assert result["entry"]["entity_type"] == "decision"
    assert result["entry"]["lifecycle_stage"] == "active"
    assert result["semantic_document"]["entity_type"] == "decision"
    assert result["semantic_document"]["document_type"] == "knowledge"
    related = db.knowledge_context("decision", result["entry"]["id"])["related"]
    related_pairs = {(item["relation_type"], item["entity_type"], item["entity"]["name"]) for item in related}
    assert ("documents", "pipeline", "buddy-hmk-extract-single") in related_pairs
    assert ("candidate_for_reuse", "brick", "source.download_to_file") in related_pairs
    assert ("candidate_for_reuse", "brick", "document.persist_extraction_result") in related_pairs

    search = semantic_search("HMK brick first orchestration prior case", db=db, project="buddy", limit=5)
    assert any(match["entity_id"] == result["entry"]["id"] for match in search["matches"])


def test_register_hmk_prior_case_metadata_is_idempotent(tmp_path):
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = PipelineStore(pipelines_dir=tmp_path / "pipelines", db=db)
    store.save(_hmk_single_pipeline())

    standardize_hmk_download_flow(db=db)
    standardize_hmk_extract_flow(db=db)
    promote_hmk_to_document_persistence_bricks(db=db)

    first = register_hmk_prior_case_metadata(db=db)
    second = register_hmk_prior_case_metadata(db=db)

    assert first["entry"]["id"] == second["entry"]["id"]
    assert len(db.knowledge_entity_list(entity_type="decision", project="buddy")) == 1
    related = db.knowledge_context("decision", first["entry"]["id"])["related"]
    assert len([item for item in related if item["relation_type"] == "documents"]) == 1
    assert len([item for item in related if item["relation_type"] == "candidate_for_reuse"]) == 5
