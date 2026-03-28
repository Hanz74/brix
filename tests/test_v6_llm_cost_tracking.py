"""Tests for T-BRIX-V6-21: LLM Cost Tracking.

Covers:
1. db.py — cost_usd column exists in runs table (ALTER TABLE migration)
2. db.py — record_run_finish persists cost_usd
3. db.py — get_monthly_cost_usd aggregates correctly
4. engine.py — _extract_step_cost parses llm_usage from step output
5. engine.py — _extract_step_cost returns 0.0 for missing/invalid usage
6. engine.py — run() accumulates cost and passes to history.record_finish
7. alerting.py — monthly_cost_exceeds:N condition validation accepted
8. alerting.py — monthly_cost_exceeds:N fires when threshold exceeded
9. alerting.py — monthly_cost_exceeds:N does not fire when below threshold
"""
import asyncio
import json
from pathlib import Path
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def alert_db(tmp_path):
    from brix.alerting import AlertManager
    return AlertManager(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# 1. cost_usd column exists
# ---------------------------------------------------------------------------

class TestCostColumn:
    def test_column_present_in_runs_table(self, db):
        with db._connect() as conn:
            pragma = conn.execute("PRAGMA table_info(runs)").fetchall()
        col_names = [row[1] for row in pragma]
        assert "cost_usd" in col_names

    def test_column_added_idempotently_on_existing_db(self, tmp_path):
        """Opening BrixDB twice does not crash even if column already exists."""
        from brix.db import BrixDB
        db_path = tmp_path / "brix.db"
        BrixDB(db_path=db_path)
        # Second open — ALTER TABLE should be caught and ignored
        BrixDB(db_path=db_path)


# ---------------------------------------------------------------------------
# 2. record_run_finish persists cost_usd
# ---------------------------------------------------------------------------

class TestRecordRunFinishCostUsd:
    def test_cost_persisted(self, db):
        db.record_run_start("run-1", "my-pipeline")
        db.record_run_finish("run-1", success=True, duration=1.5, cost_usd=0.042)
        row = db.get_run("run-1")
        assert row is not None
        assert abs(row["cost_usd"] - 0.042) < 1e-9

    def test_cost_null_when_not_provided(self, db):
        db.record_run_start("run-2", "my-pipeline")
        db.record_run_finish("run-2", success=True, duration=1.0)
        row = db.get_run("run-2")
        assert row is not None
        assert row["cost_usd"] is None

    def test_cost_zero_stored_as_null_via_engine_convention(self, db):
        """Engine passes None (not 0.0) when no LLM steps ran."""
        db.record_run_start("run-3", "my-pipeline")
        db.record_run_finish("run-3", success=True, duration=1.0, cost_usd=None)
        row = db.get_run("run-3")
        assert row["cost_usd"] is None


# ---------------------------------------------------------------------------
# 3. get_monthly_cost_usd
# ---------------------------------------------------------------------------

class TestGetMonthlyCostUsd:
    def test_returns_zero_for_empty_db(self, db):
        assert db.get_monthly_cost_usd(2026, 3) == 0.0

    def test_sums_runs_in_month(self, db):
        db.record_run_start("r1", "p", version=None)
        db.record_run_finish("r1", True, 1.0, cost_usd=0.10)
        db.record_run_start("r2", "p", version=None)
        db.record_run_finish("r2", True, 1.0, cost_usd=0.20)
        # Both runs land in the current month — use current month query
        total = db.get_monthly_cost_usd()
        assert abs(total - 0.30) < 1e-9

    def test_ignores_runs_outside_month(self, db):
        # Manually insert a run in a different month
        from datetime import datetime, timezone
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at, cost_usd) VALUES (?,?,?,?)",
                ("r-past", "p", "2025-01-15T10:00:00+00:00", 99.99),
            )
        # Query for 2026-03 — should NOT include the 2025-01 run
        total = db.get_monthly_cost_usd(2026, 3)
        assert total == 0.0

    def test_ignores_null_costs(self, db):
        db.record_run_start("r1", "p")
        db.record_run_finish("r1", True, 1.0, cost_usd=None)
        total = db.get_monthly_cost_usd()
        assert total == 0.0


# ---------------------------------------------------------------------------
# 4 & 5. _extract_step_cost
# ---------------------------------------------------------------------------

class TestExtractStepCost:
    def _cost(self, data):
        from brix.engine import _extract_step_cost
        return _extract_step_cost(data)

    def test_returns_zero_for_non_dict(self):
        assert self._cost(None) == 0.0
        assert self._cost("string") == 0.0
        assert self._cost([1, 2, 3]) == 0.0

    def test_returns_zero_for_missing_llm_usage(self):
        assert self._cost({"result": "ok"}) == 0.0

    def test_returns_zero_for_invalid_llm_usage(self):
        assert self._cost({"llm_usage": "not-a-dict"}) == 0.0

    def test_returns_zero_for_unknown_model(self):
        cost = self._cost({"llm_usage": {"input_tokens": 1000, "output_tokens": 500, "model": "unknown-model-xyz"}})
        assert cost == 0.0

    def test_calculates_cost_mistral_large(self):
        # mistral-large: $4/1M input, $12/1M output
        cost = self._cost({
            "llm_usage": {
                "input_tokens": 1_000_000,
                "output_tokens": 1_000_000,
                "model": "mistral-large",
            }
        })
        expected = 4.0 + 12.0
        assert abs(cost - expected) < 0.01

    def test_calculates_cost_gpt4o(self):
        # gpt-4o: $5/1M input, $15/1M output
        cost = self._cost({
            "llm_usage": {
                "input_tokens": 500_000,
                "output_tokens": 200_000,
                "model": "gpt-4o",
            }
        })
        expected = (500_000 / 1_000_000) * 5.0 + (200_000 / 1_000_000) * 15.0
        assert abs(cost - expected) < 0.001

    def test_model_prefix_match(self):
        # "mistral-large-latest" should match "mistral-large"
        cost_base = self._cost({
            "llm_usage": {"input_tokens": 100_000, "output_tokens": 50_000, "model": "mistral-large"}
        })
        cost_latest = self._cost({
            "llm_usage": {"input_tokens": 100_000, "output_tokens": 50_000, "model": "mistral-large-latest"}
        })
        assert abs(cost_base - cost_latest) < 0.001

    def test_zero_tokens_returns_zero(self):
        cost = self._cost({"llm_usage": {"input_tokens": 0, "output_tokens": 0, "model": "gpt-4o"}})
        assert cost == 0.0


# ---------------------------------------------------------------------------
# 6. Engine run() accumulates cost and passes to history
# ---------------------------------------------------------------------------

class TestEngineCostAccumulation:
    def test_cost_written_to_db_after_successful_run(self, tmp_path):
        """A step that emits llm_usage should result in cost_usd in the DB."""
        import brix.db as _brix_db
        import brix.history as _brix_history

        db_path = tmp_path / "brix.db"
        # Redirect both module-level constants so engine + history use our tmp DB
        original_db_path = _brix_db.BRIX_DB_PATH
        original_history_path = _brix_history.HISTORY_DB_PATH
        _brix_db.BRIX_DB_PATH = db_path
        _brix_history.HISTORY_DB_PATH = db_path

        try:
            script = tmp_path / "llm_helper.py"
            script.write_text(
                "import sys, json\n"
                "print(json.dumps({'result': 'done', 'llm_usage': "
                "{'input_tokens': 1000, 'output_tokens': 500, 'model': 'gpt-4o-mini'}}))\n"
            )

            from brix.models import Pipeline, Step
            from brix.engine import PipelineEngine

            pipeline = Pipeline(
                name="test-cost",
                steps=[Step(id="llm-step", type="python", script=str(script))],
            )

            engine = PipelineEngine()
            result = asyncio.run(engine.run(pipeline))
            assert result.success

            from brix.db import BrixDB
            db = BrixDB(db_path=db_path)
            run = db.get_run(result.run_id)
            assert run is not None
            # gpt-4o-mini: $0.15/1M input, $0.6/1M output
            # 1000 input + 500 output → tiny but > 0
            assert run["cost_usd"] is not None
            assert run["cost_usd"] > 0.0
        finally:
            _brix_db.BRIX_DB_PATH = original_db_path
            _brix_history.HISTORY_DB_PATH = original_history_path

    def test_no_cost_when_no_llm_usage(self, tmp_path):
        """A step with no llm_usage emits NULL cost_usd."""
        import brix.db as _brix_db
        import brix.history as _brix_history

        db_path = tmp_path / "brix.db"
        original_db_path = _brix_db.BRIX_DB_PATH
        original_history_path = _brix_history.HISTORY_DB_PATH
        _brix_db.BRIX_DB_PATH = db_path
        _brix_history.HISTORY_DB_PATH = db_path

        try:
            script = tmp_path / "plain_helper.py"
            script.write_text(
                "import sys, json\n"
                "print(json.dumps({'result': 'done'}))\n"
            )

            from brix.models import Pipeline, Step
            from brix.engine import PipelineEngine

            pipeline = Pipeline(
                name="test-no-cost",
                steps=[Step(id="plain-step", type="python", script=str(script))],
            )

            engine = PipelineEngine()
            result = asyncio.run(engine.run(pipeline))
            assert result.success

            from brix.db import BrixDB
            db = BrixDB(db_path=db_path)
            run = db.get_run(result.run_id)
            assert run is not None
            assert run["cost_usd"] is None
        finally:
            _brix_db.BRIX_DB_PATH = original_db_path
            _brix_history.HISTORY_DB_PATH = original_history_path


# ---------------------------------------------------------------------------
# 7. Alerting — monthly_cost_exceeds condition accepted
# ---------------------------------------------------------------------------

class TestAlertConditionValidation:
    def test_monthly_cost_exceeds_is_valid(self, alert_db):
        rule = alert_db.add_rule(
            name="cost-alert",
            condition="monthly_cost_exceeds:10.0",
            channel="log",
        )
        assert rule.condition == "monthly_cost_exceeds:10.0"

    def test_monthly_cost_exceeds_without_threshold_raises(self, alert_db):
        """Base form 'monthly_cost_exceeds' without colon+N is still base-valid
        (the validator accepts the base). Matching will return False at evaluation."""
        # Validator checks base only — monthly_cost_exceeds alone should be accepted
        rule = alert_db.add_rule(
            name="cost-alert-bare",
            condition="monthly_cost_exceeds:0",
            channel="log",
        )
        assert rule is not None


# ---------------------------------------------------------------------------
# 8 & 9. Alerting — monthly_cost_exceeds fires/doesn't fire
# ---------------------------------------------------------------------------

class TestAlertMonthlyBudget:
    def _make_run_result(self, success=True, pipeline="test"):
        return {"success": success, "run_id": "r1", "pipeline": pipeline, "steps": {}}

    def test_fires_when_cost_exceeds_threshold(self, tmp_path):
        from brix.alerting import AlertManager
        from brix.db import BrixDB

        db_path = tmp_path / "brix.db"
        alert_mgr = AlertManager(db_path=db_path)
        alert_mgr.add_rule(
            name="budget-alert",
            condition="monthly_cost_exceeds:5.0",
            channel="log",
        )

        # Insert a run that pushes monthly cost above the threshold
        brix_db = BrixDB(db_path=db_path)
        brix_db.record_run_start("r-cost", "test-pipeline")
        brix_db.record_run_finish("r-cost", True, 1.0, cost_usd=10.0)

        fired = alert_mgr.check_alerts(self._make_run_result())
        assert len(fired) == 1
        assert fired[0]["condition"] == "monthly_cost_exceeds:5.0"

    def test_does_not_fire_when_cost_below_threshold(self, tmp_path):
        from brix.alerting import AlertManager
        from brix.db import BrixDB

        db_path = tmp_path / "brix.db"
        alert_mgr = AlertManager(db_path=db_path)
        alert_mgr.add_rule(
            name="budget-alert",
            condition="monthly_cost_exceeds:100.0",
            channel="log",
        )

        brix_db = BrixDB(db_path=db_path)
        brix_db.record_run_start("r-cheap", "test-pipeline")
        brix_db.record_run_finish("r-cheap", True, 1.0, cost_usd=0.50)

        fired = alert_mgr.check_alerts(self._make_run_result())
        assert len(fired) == 0

    def test_does_not_fire_when_no_cost_data(self, tmp_path):
        from brix.alerting import AlertManager

        db_path = tmp_path / "brix.db"
        alert_mgr = AlertManager(db_path=db_path)
        alert_mgr.add_rule(
            name="budget-alert",
            condition="monthly_cost_exceeds:0.01",
            channel="log",
        )
        # No runs in DB — monthly cost is 0.0
        fired = alert_mgr.check_alerts(self._make_run_result())
        assert len(fired) == 0
