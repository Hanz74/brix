from __future__ import annotations

import json

import pytest

from brix.db import BrixDB
from brix.migrations import run_pending_migrations


@pytest.mark.asyncio
async def test_diagnose_run_includes_prior_cases_component_context_and_repair_plan(monkeypatch, tmp_path):
    from brix.mcp_handlers.insights import _handle_diagnose_run

    db_path = tmp_path / "diagnose-guidance.db"
    db = BrixDB(db_path=db_path)
    run_pending_migrations(db)

    db.upsert_pipeline(
        name="buddy-hmk-extract",
        path=str(tmp_path / "buddy-hmk-extract.yaml"),
        project="buddy",
    )
    reuse = db.knowledge_entity_add(
        "reuse",
        "reuse-pipeline-buddy-hmk-extract",
        "Reuse review for buddy-hmk-extract",
        summary="Prior reuse review",
        rationale="Existing extraction pipeline was reviewed.",
        lifecycle_stage="active",
        status="modified_existing_component",
        owner="team-brix",
        project="buddy",
    )
    db.knowledge_link_add("reuse", reuse["id"], "documents", "pipeline", "buddy-hmk-extract")

    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-prior-001",
                "buddy-hmk-extract",
                0,
                "2026-04-09T00:00:00+00:00",
                "2026-04-09T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps(
                    {
                        "save_results": {
                            "status": "error",
                            "error_message": "ModuleNotFoundError: No module named 'acme'",
                        }
                    }
                ),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-current-001",
                "buddy-hmk-extract",
                0,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps(
                    {
                        "save_results": {
                            "status": "error",
                            "error_message": "ModuleNotFoundError: No module named 'acme'",
                        }
                    }
                ),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )

    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", db_path)

    result = await _handle_diagnose_run({"run_id": "run-current-001"})

    assert result["success"] is True
    assert result["prior_cases"]
    assert result["prior_cases"][0]["run_id"] == "run-prior-001"
    assert result["component_context"]["related"]
    assert any(item["entity_type"] == "reuse" for item in result["component_context"]["related"])
    assert result["repair_plan"]
    assert result["repair_plan"][0]["step_id"] == "save_results"
    assert result["repair_plan"][0]["linked_prior_cases"]
    assert result["repair_plan"][0]["linked_components"]
