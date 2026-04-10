"""Tests for systematic brick-candidate detection."""

from __future__ import annotations

import asyncio

from brix.brick_candidate_detector import (
    detect_brick_candidates,
    filter_brick_candidate_report,
    detect_sql_pattern_candidates,
    detect_step_sequence_candidates,
    normalize_sql_pattern,
)
from brix.db import BrixDB
from brix.mcp_handlers.helpers import _handle_list_helpers


def _pipeline_with_steps(db: BrixDB, name: str, steps: list[dict]) -> str:
    pipeline_id = db.upsert_pipeline(name=name, path=f"/tmp/{name}.yaml")
    for index, step in enumerate(steps):
        db.upsert_step(pipeline_id=pipeline_id, step_dict=step, step_order=index)
    return pipeline_id


def test_normalize_sql_pattern_collapses_literals() -> None:
    assert (
        normalize_sql_pattern("SELECT * FROM docs WHERE id = 123 AND name = 'Alice'")
        == "select * from docs where id = ? and name = ?"
    )


def test_detects_repeated_helper_usage_candidates(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "helpers.db")
    db.upsert_helper(
        name="extract_invoice_fields",
        script_path="",
        description="Extract invoice fields.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["extract"],
    )
    _pipeline_with_steps(
        db,
        "a",
        [{"id": "extract", "type": "script.python", "helper": "extract_invoice_fields"}],
    )
    _pipeline_with_steps(
        db,
        "b",
        [{"id": "extract", "type": "script.python", "helper": "extract_invoice_fields"}],
    )

    report = detect_brick_candidates(db).as_dict()
    candidates = [item for item in report["candidates"] if item["kind"] == "repeated_helper_usage"]

    assert candidates
    assert candidates[0]["suggested_brick"] == "extract.extract_invoice_fields"
    assert candidates[0]["evidence_count"] == 2


def test_detects_repeated_sql_pattern_candidates(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "sql.db")
    _pipeline_with_steps(
        db,
        "a",
        [{"id": "load", "type": "db.query", "config": {"query": "SELECT * FROM docs WHERE id = 1"}}],
    )
    _pipeline_with_steps(
        db,
        "b",
        [{"id": "load", "type": "db.query", "config": {"query": "SELECT * FROM docs WHERE id = 2"}}],
    )

    candidates = detect_sql_pattern_candidates(db)

    assert len(candidates) == 1
    assert candidates[0].kind == "repeated_sql_pattern"
    assert candidates[0].domain == "db"
    assert candidates[0].evidence_count == 2


def test_detects_repeated_step_sequence_candidates(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "sequence.db")
    sequence = [
        {"id": "download", "type": "mcp.call"},
        {"id": "extract", "type": "extract.document"},
        {"id": "persist", "type": "db.exec"},
    ]
    _pipeline_with_steps(db, "a", sequence)
    _pipeline_with_steps(db, "b", sequence)

    candidates = detect_step_sequence_candidates(db)

    assert len(candidates) == 1
    assert candidates[0].kind == "repeated_step_sequence"
    assert candidates[0].suggested_brick == "flow.mcp_call_extract_document_db_exec"


def test_list_helpers_can_include_reuse_candidates(monkeypatch, tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "mcp.db")
    db.upsert_helper(
        name="extract_invoice_fields",
        script_path="",
        description="Extract invoice fields.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["extract"],
    )
    _pipeline_with_steps(
        db,
        "a",
        [{"id": "extract", "type": "script.python", "helper": "extract_invoice_fields"}],
    )
    _pipeline_with_steps(
        db,
        "b",
        [{"id": "extract", "type": "script.python", "helper": "extract_invoice_fields"}],
    )

    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db)
    monkeypatch.setattr("brix.brick_candidate_detector.BrixDB", lambda: db)
    result = asyncio.run(_handle_list_helpers({"include_reuse_candidates": True}))

    assert result["success"] is True
    assert result["reuse_candidates"]["summary"]["repeated_helper_usage"] == 1


def test_filter_brick_candidate_report_scopes_helper_and_pipeline_evidence(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "scope.db")
    db.upsert_helper(
        name="extract_invoice_fields",
        script_path="",
        description="Extract invoice fields.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="buddy",
        tags=["extract"],
    )
    db.upsert_helper(
        name="classify_mail",
        script_path="",
        description="Classify mail.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        code="def run(data): return data",
        project="cody",
        tags=["classification"],
    )
    _pipeline_with_steps(
        db,
        "extract_a",
        [
            {"id": "helper", "type": "script.python", "helper": "extract_invoice_fields"},
            {"id": "load", "type": "db.query", "config": {"query": "SELECT * FROM docs WHERE id = 1"}},
        ],
    )
    _pipeline_with_steps(
        db,
        "extract_b",
        [
            {"id": "helper", "type": "script.python", "helper": "extract_invoice_fields"},
            {"id": "load", "type": "db.query", "config": {"query": "SELECT * FROM docs WHERE id = 2"}},
        ],
    )
    _pipeline_with_steps(
        db,
        "classify_a",
        [{"id": "helper", "type": "script.python", "helper": "classify_mail"}],
    )
    _pipeline_with_steps(
        db,
        "classify_b",
        [{"id": "helper", "type": "script.python", "helper": "classify_mail"}],
    )

    scoped = filter_brick_candidate_report(
        detect_brick_candidates(db),
        helper_names={"extract_invoice_fields"},
        pipeline_names={"extract_a", "extract_b"},
    ).as_dict()

    helpers = [candidate for candidate in scoped["candidates"] if candidate["kind"] == "repeated_helper_usage"]
    assert [candidate["suggested_brick"] for candidate in helpers] == ["extract.extract_invoice_fields"]
    assert helpers[0]["evidence_count"] == 2
    assert {item["helper"] for item in helpers[0]["evidence"]} == {"extract_invoice_fields"}
    assert scoped["summary"]["repeated_sql_pattern"] == 1
    sql_candidates = [candidate for candidate in scoped["candidates"] if candidate["kind"] == "repeated_sql_pattern"]
    assert {item["pipeline"] for item in sql_candidates[0]["evidence"]} == {"extract_a", "extract_b"}
