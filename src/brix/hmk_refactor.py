"""Targeted DB-first refactors for HMK pipelines."""
from __future__ import annotations

from typing import Any

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


def _step_by_id(steps: list[dict[str, Any]], step_id: str) -> dict[str, Any]:
    for step in steps:
        if isinstance(step, dict) and step.get("id") == step_id:
            return step
    raise ValueError(f"Step '{step_id}' not found")


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

    save_results["type"] = "document.persist_extraction_result"
    save_results["config"] = {
        "connection": "buddy-db",
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

    mark_processed["type"] = "document.mark_specialist_processed"
    mark_processed["config"] = {
        "connection": "buddy-db",
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
    return PipelineStore(db=active_db).load_raw(pipeline_name)
