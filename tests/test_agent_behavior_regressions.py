from __future__ import annotations

import pytest

from brix.history import RunHistory
from brix.models import ErrorConfig, Pipeline, Step
from brix.validator import PipelineValidator


def test_agent_regression_db_query_dml_points_to_supported_path() -> None:
    pipeline = Pipeline(
        name="agent-db-query-dml",
        steps=[
            Step(
                id="write",
                type="db.query",
                query="UPDATE users SET active = 1",
            )
        ],
        error_handling=ErrorConfig(),
        policy_level="strict",
    )

    result = PipelineValidator(lint_rules=[]).validate(pipeline, level="standard")
    payload = result.to_structured_payload()

    finding = next(item for item in payload["findings"] if item["code"] == "DB_QUERY_DML")
    assert finding["severity"] == "error"
    assert finding["hint"] == "Use db.exec for UPDATE/DELETE/INSERT"
    assert any("Use db.exec for UPDATE/DELETE/INSERT" in action for action in payload["next_actions"])
    assert all("workaround" not in action.lower() for action in payload["next_actions"])


def test_agent_regression_engine_error_exposes_phase_and_root_cause(tmp_path) -> None:
    history = RunHistory(db_path=tmp_path / "agent-regressions.db")
    history.record_start("run-1", "agent-engine-error")
    history.record_finish(
        "run-1",
        False,
        0.2,
        {
            "_engine_error": {
                "status": "error",
                "duration": 0.0,
                "errors": 1,
                "error_message": (
                    "Unhandled engine exception phase=execution boundary=engine "
                    "last_completed_step=prepare completed_steps=['prepare'] "
                    "root_exception=RuntimeError: synthetic render crash\nTraceback"
                ),
            }
        },
    )

    errors = history.get_run_errors(run_id="run-1")

    assert errors == [
        {
            "run_id": "run-1",
            "step_id": "_engine_error",
            "error_message": (
                "Unhandled engine exception phase=execution boundary=engine "
                "last_completed_step=prepare completed_steps=['prepare'] "
                "root_exception=RuntimeError: synthetic render crash\nTraceback"
            ),
            "hint": None,
            "phase": "execution",
            "root_cause": "RuntimeError: synthetic render crash",
        }
    ]
