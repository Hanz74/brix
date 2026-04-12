"""Targeted DB-first refactors for HMK pipelines."""
from __future__ import annotations

import json
from typing import Any

from brix.config import BrixConfig
from brix.db import BrixDB
from brix.pipeline_store import PipelineStore
from brix.semantic_retrieval import sync_semantic_index


def _step_by_id(steps: list[dict[str, Any]], step_id: str) -> dict[str, Any]:
    for step in steps:
        if isinstance(step, dict) and step.get("id") == step_id:
            return step
    raise ValueError(f"Step '{step_id}' not found")


def _preserve_connection(config: dict[str, Any], *, fallback: str | None = None) -> str:
    connection = config.get("connection")
    if isinstance(connection, str) and connection.strip():
        return connection.strip()
    return fallback or BrixConfig.reload().DEFAULT_DOCUMENT_CONNECTION


def rewrite_hmk_save_results_to_persistence_brick(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Replace HMK inline extraction persistence with the reusable document brick."""

    active_db = db if db is not None else BrixDB()
    store = PipelineStore(db=active_db)
    raw = store.load_raw(pipeline_name)
    steps = raw.get("steps")
    if not isinstance(steps, list):
        raise ValueError(f"Pipeline '{pipeline_name}' has no step list")

    save_results = _step_by_id(steps, "save_results")
    config = save_results.get("config")
    if not isinstance(config, dict):
        raise ValueError("HMK 'save_results' step must carry dict config")

    current_query = str(config.get("query") or "")
    if save_results.get("type") == "document.persist_extraction_result":
        return raw
    if "replace(\"'\", \"''\")" not in current_query:
        raise ValueError("HMK 'save_results' no longer matches interpolated SQL workaround shape")

    connection_name = _preserve_connection(config)
    save_results["type"] = "document.persist_extraction_result"
    save_results["config"] = {
        "connection": connection_name,
        "document_id": "{{ input.item.id }}",
        "extraction_result": "{{ extract.output | tojson }}",
        "content_hash": "{{ download_save.output.content_hash | default('') }}",
        "file_path": "{{ download_save.output.file_bytes_path | default('') }}",
    }
    save_results["params"] = {}
    raw["steps"] = steps
    store.save(raw, name=pipeline_name)
    return store.load_raw(pipeline_name)


def rewrite_hmk_mark_processed_to_specialist_brick(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Replace HMK inline specialist state mutation with the reusable document brick."""

    active_db = db if db is not None else BrixDB()
    store = PipelineStore(db=active_db)
    raw = store.load_raw(pipeline_name)
    steps = raw.get("steps")
    if not isinstance(steps, list):
        raise ValueError(f"Pipeline '{pipeline_name}' has no step list")

    mark_processed = _step_by_id(steps, "mark_processed")
    config = mark_processed.get("config")
    if not isinstance(config, dict):
        raise ValueError("HMK 'mark_processed' step must carry dict config")

    current_query = str(config.get("query") or "")
    if mark_processed.get("type") == "document.mark_specialist_processed":
        return raw
    if "array_append" not in current_query or "hmk_extracted" not in current_query:
        raise ValueError("HMK 'mark_processed' no longer matches inline specialist mutation shape")

    connection_name = _preserve_connection(config)
    mark_processed["type"] = "document.mark_specialist_processed"
    mark_processed["config"] = {
        "connection": connection_name,
        "document_id": "{{ input.item.id }}",
        "specialist_name": "hmk_extracted",
    }
    mark_processed["params"] = {}
    raw["steps"] = steps
    store.save(raw, name=pipeline_name)
    return store.load_raw(pipeline_name)


def promote_hmk_to_document_persistence_bricks(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Ensure HMK uses the reusable document persistence brick family end-to-end."""

    active_db = db if db is not None else BrixDB()
    rewrite_hmk_save_results_to_persistence_brick(db=active_db, pipeline_name=pipeline_name)
    rewrite_hmk_mark_processed_to_specialist_brick(db=active_db, pipeline_name=pipeline_name)
    standardize_hmk_statement_output(db=active_db, pipeline_name=pipeline_name)
    return PipelineStore(db=active_db).load_raw(pipeline_name)


def standardize_hmk_statement_output(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Expose bundle-aware statement summary fields in the HMK pipeline output."""

    active_db = db if db is not None else BrixDB()
    store = PipelineStore(db=active_db)
    raw = store.load_raw(pipeline_name)

    output = raw.get("output")
    if not isinstance(output, dict):
        output = {}

    desired_output = {
        "doc_id": "{{ input.item.id }}",
        "status": "{{ 'ok' if save_results.output is defined else 'marked_only' }}",
        "is_statement_bundle": "{{ save_results.output.document_shape.is_bundle | default(false) }}",
        "statement_count": "{{ save_results.output.document_shape.statement_count | default(1) }}",
        "statement_numbers": "{{ save_results.output.document_shape.statement_numbers | default([]) }}",
        "period_from": "{{ save_results.output.document_shape.period_from | default('') }}",
        "period_to": "{{ save_results.output.document_shape.period_to | default('') }}",
    }
    if output == desired_output:
        return raw

    raw["output"] = desired_output
    store.save(raw, name=pipeline_name)
    return store.load_raw(pipeline_name)


def standardize_hmk_extract_flow(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Replace HMK ad hoc extract preparation and Daigestr sub-pipeline glue with standard bricks."""

    active_db = db if db is not None else BrixDB()
    store = PipelineStore(db=active_db)
    raw = store.load_raw(pipeline_name)
    steps = raw.get("steps")
    if not isinstance(steps, list):
        raise ValueError(f"Pipeline '{pipeline_name}' has no step list")

    extract_step = _step_by_id(steps, "extract")
    if extract_step.get("type") == "extract.document_with_daigestr":
        try:
            prepare_step = _step_by_id(steps, "prepare_extractable")
        except ValueError:
            prepare_step = _step_by_id(steps, "read_file_b64")
    else:
        prepare_step = _step_by_id(steps, "read_file_b64")

    if prepare_step.get("type") == "document.prepare_extractable_payload" and extract_step.get("type") == "extract.document_with_daigestr":
        return raw

    prepare_step["id"] = "prepare_extractable"
    prepare_step["type"] = "document.prepare_extractable_payload"
    prepare_step["config"] = {
        "file_bytes_path": "{{ download_save.output.file_bytes_path }}",
        "mime_type": "{{ get_download_url.output.file.mimeType | default('') }}",
        "filename": "{{ input.item.file_name | default('') }}",
        "language": "{{ input.language | default('de') }}",
        "include_base64": True,
        "metadata": {
            "source": "{{ input.item.source | default('') }}",
            "source_id": "{{ input.item.source_id | default('') }}",
            "document_id": "{{ input.item.id }}",
            "extension": "{{ input.item.extension | default('') }}",
            "doc_date": "{{ input.item.doc_date | default('') }}",
        },
    }
    prepare_step["params"] = {}
    prepare_step["when"] = "{{ download_save.output.extractable | default(false) }}"

    extract_step["type"] = "extract.document_with_daigestr"
    extract_step.pop("pipeline", None)
    extract_step["config"] = {
        "file_bytes_path": "{{ prepare_extractable.output.file_bytes_path }}",
        "base64": "{{ prepare_extractable.output.base64 }}",
        "filename": "{{ prepare_extractable.output.filename }}",
        "language": "{{ prepare_extractable.output.language | default('de') }}",
        "mime_type": "{{ prepare_extractable.output.mime_type | default('') }}",
        "metadata": "{{ prepare_extractable.output.metadata }}",
        "mode": "default",
        "retry_on_low_quality": True,
        "quality_retry_threshold": 0.75,
        "quality_retry_mode": "full",
    }
    extract_step["params"] = {}
    extract_step["when"] = "{{ prepare_extractable.output is defined }}"

    raw["steps"] = steps
    store.save(raw, name=pipeline_name)
    return store.load_raw(pipeline_name)


def standardize_hmk_download_flow(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Replace HMK helper-based download staging with the standard source brick."""

    active_db = db if db is not None else BrixDB()
    store = PipelineStore(db=active_db)
    raw = store.load_raw(pipeline_name)
    steps = raw.get("steps")
    if not isinstance(steps, list):
        raise ValueError(f"Pipeline '{pipeline_name}' has no step list")

    download_step = _step_by_id(steps, "download_save")
    if download_step.get("type") == "source.download_to_file":
        return raw

    helper_name = str(download_step.get("helper") or "")
    if helper_name != "att_onedrive_save":
        raise ValueError("HMK 'download_save' no longer matches helper-based download staging shape")

    download_step["type"] = "source.download_to_file"
    download_step.pop("helper", None)
    download_step["config"] = {
        "url": "{{ get_download_url.output['@microsoft.graph.downloadUrl'] | default('') }}",
        "filename": "{{ input.item.file_name | default('') }}",
    }
    download_step["params"] = {}
    raw["steps"] = steps
    store.save(raw, name=pipeline_name)
    return store.load_raw(pipeline_name)


def _ensure_knowledge_entity(
    db: BrixDB,
    *,
    entity_type: str,
    name: str,
    title: str,
    raw_text: str = "",
    summary: str = "",
    rationale: str = "",
    lifecycle_stage: str = "active",
    status: str = "",
    owner: str = "",
    project: str = "",
    tags: list[str] | None = None,
    content: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing = db.knowledge_entity_get(name)
    if existing is None:
        return db.knowledge_entity_add(
            entity_type,
            name,
            title,
            raw_text=raw_text,
            summary=summary,
            rationale=rationale,
            lifecycle_stage=lifecycle_stage,
            status=status,
            owner=owner,
            project=project,
            tags=tags or [],
            content=content or {},
        )
    return db.knowledge_entity_update(
        existing["id"],
        title=title,
        raw_text=raw_text,
        summary=summary,
        rationale=rationale,
        lifecycle_stage=lifecycle_stage,
        status=status,
        owner=owner,
        project=project,
        tags=tags or [],
        content=content or {},
    ) or existing


def _ensure_knowledge_link(
    db: BrixDB,
    *,
    source_entity_type: str,
    source_entity_id: str,
    relation_type: str,
    target_entity_type: str,
    target_entity_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_source = db._knowledge_link_entity_canonical_id(source_entity_type, source_entity_id)
    canonical_target = db._knowledge_link_entity_canonical_id(target_entity_type, target_entity_id)
    for link in db.knowledge_link_list(entity_type=source_entity_type, entity_id=source_entity_id):
        if (
            link.get("source_entity_type") == source_entity_type
            and link.get("source_entity_id") == canonical_source
            and link.get("relation_type") == relation_type
            and link.get("target_entity_type") == target_entity_type
            and link.get("target_entity_id") == canonical_target
        ):
            return link
    return db.knowledge_link_add(
        source_entity_type,
        source_entity_id,
        relation_type,
        target_entity_type,
        target_entity_id,
        metadata=metadata or {},
    )


def register_hmk_prior_case_metadata(
    *,
    db: BrixDB | None = None,
    pipeline_name: str = "buddy-hmk-extract-single",
) -> dict[str, Any]:
    """Register the HMK brick-first migration as reusable prior-case knowledge."""

    active_db = db if db is not None else BrixDB()
    pipeline = active_db.get_pipeline(pipeline_name)
    if pipeline is None:
        raise ValueError(f"Pipeline '{pipeline_name}' not found")

    prior_case = _ensure_knowledge_entity(
        active_db,
        entity_type="decision",
        name="hmk-anchor-refactor-prior-case",
        title="HMK anchor refactor proves brick-first orchestration",
        raw_text=(
            "HMK was reduced to orchestration by replacing helper and inline SQL workarounds with "
            "reusable download, extract-preparation, extraction, and persistence bricks."
        ),
        summary=(
            "Use HMK as the canonical prior case when a document pipeline must migrate from helper-heavy "
            "logic to DB-first, brick-first orchestration."
        ),
        rationale=(
            "The migration demonstrates that Brix should solve repeated document ingestion behavior through "
            "reusable bricks and metadata-backed orchestration, not pipeline-local workaround code."
        ),
        lifecycle_stage="active",
        status="accepted",
        owner="platform",
        project=str(pipeline.get("project") or "buddy"),
        tags=["hmk", "prior-case", "brick-first", "db-first", "regression"],
        content={
            "pipeline_name": pipeline_name,
            "migration_scope": [
                "source.download_to_file",
                "document.prepare_extractable_payload",
                "extract.document_with_daigestr",
                "document.persist_extraction_result",
                "document.mark_specialist_processed",
            ],
            "replaced_patterns": [
                "att_onedrive_save helper staging",
                "flow.pipeline Daigestr glue",
                "inline db.exec persistence SQL",
                "inline specialist mutation SQL",
            ],
            "task_ids": ["T-6.1.1", "T-6.1.2", "T-6.2.1", "T-6.2.2", "T-6.3.1", "T-6.3.2"],
        },
    )

    related_bricks = [
        "source.download_to_file",
        "document.prepare_extractable_payload",
        "extract.document_with_daigestr",
        "document.persist_extraction_result",
        "document.mark_specialist_processed",
    ]

    links = [
        _ensure_knowledge_link(
            active_db,
            source_entity_type="decision",
            source_entity_id=prior_case["id"],
            relation_type="documents",
            target_entity_type="pipeline",
            target_entity_id=pipeline_name,
            metadata={"role": "canonical prior case"},
        )
    ]
    for brick_name in related_bricks:
        links.append(
            _ensure_knowledge_link(
                active_db,
                source_entity_type="decision",
                source_entity_id=prior_case["id"],
                relation_type="candidate_for_reuse",
                target_entity_type="brick",
                target_entity_id=brick_name,
                metadata={"pipeline_name": pipeline_name},
            )
        )

    semantic_payload = {
        "title": prior_case["title"],
        "summary": prior_case["summary"],
        "rationale": prior_case["rationale"],
        "pipeline": pipeline_name,
        "bricks": related_bricks,
        "tags": prior_case["tags"],
        "replaced_patterns": prior_case["content"].get("replaced_patterns", []),
    }
    semantic_doc = active_db.semantic_document_upsert(
        entity_type="decision",
        entity_id=prior_case["id"],
        document_type="knowledge",
        title=prior_case["title"],
        text_content=json.dumps(semantic_payload, sort_keys=True),
        project=str(pipeline.get("project") or "buddy"),
        metadata={"name": prior_case["name"], "pipeline_name": pipeline_name},
    )
    sync_stats = sync_semantic_index(db=active_db)

    return {
        "entry": active_db.knowledge_entity_get(prior_case["id"]) or prior_case,
        "links": links,
        "semantic_document": semantic_doc,
        "sync_stats": sync_stats,
    }
