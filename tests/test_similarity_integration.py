import json

import pytest

from brix.db import BrixDB


@pytest.mark.asyncio
async def test_compose_and_plan_pipeline_include_similar_cases(monkeypatch):
    from brix.mcp_handlers.composer import _handle_compose_pipeline, _handle_plan_pipeline

    fake_matches = [
        {
            "entity_type": "intent",
            "entity_id": "intent-1",
            "document_type": "knowledge",
            "title": "Prior HMK intent",
            "score": 0.91,
        }
    ]

    def _fake_semantic_search(*args, **kwargs):
        return {"matches": fake_matches}

    monkeypatch.setattr("brix.mcp_handlers.composer.semantic_search", _fake_semantic_search)

    compose = await _handle_compose_pipeline({"goal": "Fix HMK parse failures"})
    plan = await _handle_plan_pipeline({"goal": "Fix HMK parse failures"})

    assert compose["similar_cases"] == fake_matches
    assert plan["similar_cases"] == fake_matches


@pytest.mark.asyncio
async def test_diagnose_run_includes_similar_cases(monkeypatch, tmp_path):
    from brix.mcp_handlers.insights import _handle_diagnose_run

    db_path = tmp_path / "diagnose.db"
    db = BrixDB(db_path=db_path)
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-sim-001",
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
                            "error_message": "HMK parse failure",
                        }
                    }
                ),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )

    fake_matches = [
        {
            "entity_type": "finding",
            "entity_id": "run-old-001:extract",
            "document_type": "incident",
            "title": "Similar HMK incident",
            "score": 0.88,
        }
    ]

    def _fake_semantic_search(*args, **kwargs):
        return {"matches": fake_matches}

    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", db_path)
    monkeypatch.setattr("brix.mcp_handlers.insights.semantic_search", _fake_semantic_search)

    result = await _handle_diagnose_run({"run_id": "run-sim-001"})

    assert result["similar_cases"] == fake_matches
    assert result["diagnoses"][0]["similar_cases"] == fake_matches


@pytest.mark.asyncio
async def test_diagnose_run_no_step_data_preserves_response_shape(monkeypatch, tmp_path):
    from brix.mcp_handlers.insights import _handle_diagnose_run

    db_path = tmp_path / "diagnose-empty.db"
    db = BrixDB(db_path=db_path)
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-sim-empty-001",
                "buddy-hmk-extract",
                1,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                None,
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )

    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", db_path)

    result = await _handle_diagnose_run({"run_id": "run-sim-empty-001"})

    assert result["diagnoses"] == []
    assert result["similar_cases"] == []
    assert result["total_failed_steps"] == 0
