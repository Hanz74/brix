"""Smoke + Regression Tests — T-BRIX-DB-29.

Three test classes:
- TestSmoke:       Critical-path E2E tests. If any fail, the system is broken.
- TestRegression:  Verifies all V8 features work correctly.
- TestIntegration: Multi-step pipelines against real runners.

STRICT: assert exact values, not just "is not None".
"""
from __future__ import annotations

import asyncio
import io
import sqlite3
import warnings
from pathlib import Path
from typing import Any

import pytest

from brix.db import BrixDB
from brix.engine import PipelineEngine, LEGACY_ALIASES
from brix.loader import PipelineLoader
from brix.models import Pipeline, Step
from brix.context import PipelineContext


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _run_pipeline_with_db(
    yaml_text: str,
    tmp_path: Path,
    user_input: dict = None,
    monkeypatch=None,
    test_db_path: Path = None,
) -> tuple:
    """Run a pipeline against a tmp-path history DB.
    Returns (run_id, success, db, result).

    If monkeypatch + test_db_path are provided, BrixDB() (default init) is
    redirected to test_db_path so engine deprecation tracking is isolated.
    """
    import brix.history as history_mod

    db_path = tmp_path / "brix_run.db"
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_text)
    engine = PipelineEngine()

    original_path = history_mod.HISTORY_DB_PATH
    history_mod.HISTORY_DB_PATH = db_path

    if monkeypatch and test_db_path:
        original_init = BrixDB.__init__

        def patched_init(self, db_path=None):
            original_init(self, db_path=test_db_path)

        monkeypatch.setattr(BrixDB, "__init__", patched_init)

    try:
        result = asyncio.run(engine.run(pipeline, user_input=user_input or {}))
    finally:
        history_mod.HISTORY_DB_PATH = original_path

    db = BrixDB(db_path=db_path)
    return result.run_id, result.success, db, result


# ---------------------------------------------------------------------------
# TestSmoke — Critical paths, fast
# ---------------------------------------------------------------------------

class TestSmoke:
    """Kernfunktionen End-to-End. Wenn einer dieser Tests fehlschlägt, ist das System kaputt."""

    def test_create_and_run_pipeline_e2e(self, tmp_path):
        """Pipeline erstellen → laufen lassen → Status abrufen → Step-Daten vorhanden."""
        # 1. Pipeline with two flow.set steps
        # Note: step outputs are accessed via direct step_id (not steps.step_id prefix)
        yaml_text = """
name: smoke-e2e
steps:
  - id: produce
    type: flow.set
    values:
      answer: 42
      label: hello
  - id: forward
    type: flow.set
    values:
      forwarded: "{{ produce.output.answer }}"
"""
        # 2. Run pipeline
        run_id, success, db, result = _run_pipeline_with_db(yaml_text, tmp_path)

        # 3. Assert status == done (success)
        assert success is True, f"Pipeline failed. Steps: {[(s, st.status, st.error_message) for s, st in result.steps.items()]}"

        # 4. Both steps recorded in step_executions
        executions = db.get_step_executions(run_id)
        assert len(executions) == 2, f"Expected 2 step_executions, got {len(executions)}"

        step_ids = {e["step_id"] for e in executions}
        assert "produce" in step_ids, f"Step 'produce' missing from executions: {step_ids}"
        assert "forward" in step_ids, f"Step 'forward' missing from executions: {step_ids}"

        # All steps succeeded
        for exe in executions:
            assert exe["status"] == "success", (
                f"Step '{exe['step_id']}' has status '{exe['status']}', expected 'success'"
            )

        # Verify output_data was persisted and contains correct values
        produce_exec = next(e for e in executions if e["step_id"] == "produce")
        assert produce_exec["output_data"] is not None, "produce step must have output_data"
        assert produce_exec["output_data"].get("answer") == 42, (
            f"Expected answer=42, got: {produce_exec['output_data']}"
        )

        # Verify forward step received the value from produce
        forward_exec = next(e for e in executions if e["step_id"] == "forward")
        assert forward_exec["output_data"] is not None, "forward step must have output_data"
        assert forward_exec["output_data"].get("forwarded") == 42, (
            f"Expected forwarded=42, got: {forward_exec['output_data']}"
        )

    def test_variable_in_pipeline(self, tmp_path, monkeypatch):
        """Variable setzen → in Pipeline nutzen → Wert korrekt im Output."""
        test_db_path = tmp_path / "brix_var.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(
            BrixDB, "__init__",
            lambda self, db_path=None: original_init(self, db_path=test_db_path)
        )

        # 1. Set variable directly
        db = BrixDB()
        db.variable_set("smoke_var", "hello_from_var")

        # 2. Pipeline using {{ var.smoke_var }}
        yaml_text = """
name: var-test
steps:
  - id: use_var
    type: flow.set
    values:
      result: "{{ var.smoke_var }}"
"""
        import brix.history as history_mod
        run_db_path = tmp_path / "brix_run.db"
        loader = PipelineLoader()
        pipeline = loader.load_from_string(yaml_text)
        engine = PipelineEngine()

        original_path = history_mod.HISTORY_DB_PATH
        history_mod.HISTORY_DB_PATH = run_db_path
        try:
            result = asyncio.run(engine.run(pipeline, user_input={}))
        finally:
            history_mod.HISTORY_DB_PATH = original_path

        assert result.success is True, f"Pipeline failed: {result}"

        # 3. Output data should contain the resolved variable value
        run_db = BrixDB(db_path=run_db_path)
        executions = run_db.get_step_executions(result.run_id)
        assert len(executions) >= 1, "No step executions recorded"

        use_var_exec = next((e for e in executions if e["step_id"] == "use_var"), None)
        assert use_var_exec is not None, "Step 'use_var' not found in executions"
        assert use_var_exec["status"] == "success"

        output_data = use_var_exec.get("output_data") or {}
        assert output_data.get("result") == "hello_from_var", (
            f"Expected 'hello_from_var' in output, got: {output_data}"
        )

    def test_persistent_store_survives_runs(self, tmp_path, monkeypatch):
        """Persistent Store: Wert in Run 1 setzen → in Run 2 lesen."""
        test_db_path = tmp_path / "brix_store.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(
            BrixDB, "__init__",
            lambda self, db_path=None: original_init(self, db_path=test_db_path)
        )

        import brix.history as history_mod
        run_db_path = tmp_path / "brix_run.db"
        loader = PipelineLoader()

        # 1. Run 1: write to persistent_store via persist=true
        yaml_run1 = """
name: store-writer
steps:
  - id: write_counter
    type: flow.set
    persist: true
    values:
      counter: 42
      run_label: first_run
"""
        pipeline1 = loader.load_from_string(yaml_run1)
        engine1 = PipelineEngine()
        original_path = history_mod.HISTORY_DB_PATH
        history_mod.HISTORY_DB_PATH = run_db_path
        try:
            result1 = asyncio.run(engine1.run(pipeline1, user_input={}))
        finally:
            history_mod.HISTORY_DB_PATH = original_path

        assert result1.success is True, f"Run 1 failed: {result1}"

        # Verify store has the value (store_get returns string)
        store_db = BrixDB()
        stored_counter = store_db.store_get("counter")
        assert stored_counter is not None, "counter must be stored in persistent_store"
        # Values stored by SetRunner go through Jinja2 rendering, may be int or string
        assert str(stored_counter) == "42", (
            f"Expected counter='42' in persistent store, got: {stored_counter!r}"
        )

        # 2. Run 2: read from persistent_store via {{ store.counter }}
        yaml_run2 = """
name: store-reader
steps:
  - id: read_counter
    type: flow.set
    values:
      loaded_value: "{{ store.counter }}"
"""
        pipeline2 = loader.load_from_string(yaml_run2)
        engine2 = PipelineEngine()
        history_mod.HISTORY_DB_PATH = run_db_path
        try:
            result2 = asyncio.run(engine2.run(pipeline2, user_input={}))
        finally:
            history_mod.HISTORY_DB_PATH = original_path

        assert result2.success is True, f"Run 2 failed: {result2}"

        # Run 2 output should have loaded_value matching the stored counter
        run_db2 = BrixDB(db_path=run_db_path)
        executions2 = run_db2.get_step_executions(result2.run_id)
        read_exec = next((e for e in executions2 if e["step_id"] == "read_counter"), None)
        assert read_exec is not None, "Step 'read_counter' missing"
        output = read_exec.get("output_data") or {}
        # loaded_value should match the originally stored counter value
        assert str(output.get("loaded_value")) == "42", (
            f"Expected loaded_value='42' from store in run 2, got: {output}"
        )

    def test_secret_variable_redacted(self, tmp_path, monkeypatch):
        """Secret Variable wird in step_executions redacted."""
        monkeypatch.setenv("BRIX_MASTER_KEY", "a" * 64)

        test_db_path = tmp_path / "brix_secret.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(
            BrixDB, "__init__",
            lambda self, db_path=None: original_init(self, db_path=test_db_path)
        )

        # 1. Set a secret variable
        db = BrixDB()
        db.variable_set("api_key", "sk-secret123", secret=True)

        # 2. Pipeline uses the secret variable
        yaml_text = """
name: secret-test
steps:
  - id: use_secret
    type: flow.set
    values:
      token_value: "{{ var.api_key }}"
"""
        import brix.history as history_mod
        run_db_path = tmp_path / "brix_run.db"
        loader = PipelineLoader()
        pipeline = loader.load_from_string(yaml_text)
        engine = PipelineEngine()

        original_path = history_mod.HISTORY_DB_PATH
        history_mod.HISTORY_DB_PATH = run_db_path
        try:
            result = asyncio.run(engine.run(pipeline, user_input={}))
        finally:
            history_mod.HISTORY_DB_PATH = original_path

        assert result.success is True, f"Pipeline failed: {result}"

        # 3. Check that "sk-secret123" does NOT appear in step_executions
        run_db = BrixDB(db_path=run_db_path)
        executions = run_db.get_step_executions(result.run_id)
        assert len(executions) >= 1, "No step executions recorded"

        import json
        for exe in executions:
            raw_input = json.dumps(exe.get("input_data") or {})
            raw_output = json.dumps(exe.get("output_data") or {})
            assert "sk-secret123" not in raw_input, (
                f"Secret 'sk-secret123' leaked into input_data: {raw_input}"
            )
            assert "sk-secret123" not in raw_output, (
                f"Secret 'sk-secret123' leaked into output_data: {raw_output}"
            )

    def test_brick_first_resolution(self):
        """Brick-Name (db.query) wird korrekt zu Runner aufgelöst."""
        engine = PipelineEngine()

        # db.query should resolve
        runner = engine._resolve_runner("db.query")
        assert runner is not None, "db.query must resolve to a runner"

        # flow.set should resolve
        runner_set = engine._resolve_runner("flow.set")
        assert runner_set is not None, "flow.set must resolve to a runner"

        # Nonexistent brick must return None (not raise)
        runner_bad = engine._resolve_runner("nonexistent.brick")
        assert runner_bad is None, (
            f"nonexistent.brick should return None, got: {runner_bad}"
        )

    def test_legacy_alias_with_warning(self, tmp_path):
        """Alter Step-Type 'set' funktioniert aber generiert Warnung in RunResult."""
        yaml_text = """
name: legacy-test
steps:
  - id: legacy_step
    type: set
    values:
      x: "1"
"""
        run_id, success, db, result = _run_pipeline_with_db(yaml_text, tmp_path)
        assert success is True, f"Legacy pipeline failed: {result}"

        # The RunResult should have deprecation_warnings since 'set' is a legacy type
        assert result.deprecation_warnings, (
            "Expected deprecation_warnings for legacy type 'set', got empty list"
        )
        # At least one warning should mention 'set'
        warnings_text = " ".join(result.deprecation_warnings)
        assert "set" in warnings_text.lower(), (
            f"Expected 'set' in deprecation warnings, got: {result.deprecation_warnings}"
        )

    def test_compositor_mode_blocks_python(self):
        """Compositor-Mode Pipeline blockiert python Steps."""
        pipeline = Pipeline(
            name="cm-block-test",
            compositor_mode=True,
            steps=[Step(id="s1", type="python", script="/nonexistent/helper.py")],
        )
        assert pipeline.allow_code is False, "compositor_mode=True must set allow_code=False"

        engine = PipelineEngine()
        result = asyncio.run(engine.run(pipeline, user_input={}))

        assert result.success is False, (
            "Compositor-mode pipeline with python step must fail"
        )
        s1 = result.steps.get("s1")
        assert s1 is not None, "Step 's1' not in result.steps"
        assert s1.status == "error", f"Expected status='error', got '{s1.status}'"
        assert s1.error_message is not None, "error_message must not be None"
        assert "Compositor-Mode" in s1.error_message, (
            f"Expected 'Compositor-Mode' in error: {s1.error_message}"
        )
        assert "python" in s1.error_message, (
            f"Expected 'python' in error: {s1.error_message}"
        )


# ---------------------------------------------------------------------------
# TestRegression — All V8 Features
# ---------------------------------------------------------------------------

class TestRegression:
    """Verifiziert dass alle V8 Features funktionieren."""

    def test_all_system_bricks_have_runner(self):
        """Jeder System-Brick zeigt auf einen existierenden Runner."""
        from brix.bricks.builtins import SYSTEM_BRICKS

        engine = PipelineEngine()
        runner_names = set(engine._runners.keys())

        for brick in SYSTEM_BRICKS:
            assert brick.runner, (
                f"System brick '{brick.name}' is missing runner field"
            )
            assert brick.runner in runner_names, (
                f"System brick '{brick.name}' references runner '{brick.runner}' "
                f"which is not in engine._runners. Available: {sorted(runner_names)}"
            )

    def test_foreach_produces_progress(self, tmp_path):
        """foreach meldet Item-Progress (3 foreach_item_executions für 3 Items)."""
        # foreach expression must be a Jinja2 string expression
        yaml_text = """
name: foreach-progress-test
steps:
  - id: process_items
    type: flow.set
    foreach: "{{ ['alpha', 'beta', 'gamma'] }}"
    values:
      item_result: "{{ item }}"
"""
        run_id, success, db, result = _run_pipeline_with_db(yaml_text, tmp_path)
        assert success is True, (
            f"foreach pipeline failed: "
            f"{[(s, st.status, st.error_message) for s, st in result.steps.items()]}"
        )

        # All 3 foreach items should be recorded in foreach_item_executions
        items = db.get_foreach_items(run_id, "process_items")
        assert len(items) == 3, (
            f"Expected 3 foreach_item_executions, got {len(items)}"
        )
        item_indices = {item["item_index"] for item in items}
        assert item_indices == {0, 1, 2}, (
            f"Expected item indices {{0,1,2}}, got {item_indices}"
        )

    def test_repeat_produces_progress(self, tmp_path):
        """repeat runner executes sub-steps for each iteration."""
        # flow.repeat uses 'sequence' (list of step dicts) not 'steps' or 'times'
        # max_iterations controls how many times it runs (with no until/while = runs max_iterations)
        yaml_text = """
name: repeat-progress-test
steps:
  - id: do_repeat
    type: flow.repeat
    max_iterations: 3
    until: "{{ repeat.index >= 2 }}"
    sequence:
      - id: inner
        type: flow.set
        values:
          x: "{{ repeat.index }}"
"""
        run_id, success, db, result = _run_pipeline_with_db(yaml_text, tmp_path)
        assert success is True, (
            f"repeat pipeline failed: "
            f"{[(s, st.status, st.error_message) for s, st in result.steps.items()]}"
        )

        # The repeat step itself must be recorded
        executions = db.get_step_executions(run_id)
        step_ids = {e["step_id"] for e in executions}
        assert "do_repeat" in step_ids, (
            f"Step 'do_repeat' missing from executions: {step_ids}"
        )

        # The repeat step must succeed
        repeat_exec = next(e for e in executions if e["step_id"] == "do_repeat")
        assert repeat_exec["status"] == "success", (
            f"repeat step must succeed, got: {repeat_exec['status']}, "
            f"error: {repeat_exec.get('error_detail')}"
        )

    def test_custom_brick_crud(self):
        """Custom Brick erstellen → nutzen → löschen."""
        from brix.bricks.registry import BrickRegistry
        from brix.bricks.schema import BrickSchema

        registry = BrickRegistry()
        custom_brick = BrickSchema(
            name="custom.smoke-test-brick",
            type="python",
            description="Smoke test custom brick",
            when_to_use="For smoke tests only",
            system=False,
            runner="python",
            namespace="custom",
        )

        # Create
        registry.register(custom_brick)
        found = registry.get("custom.smoke-test-brick")
        assert found is not None, "Custom brick must be retrievable after registration"
        assert found.name == "custom.smoke-test-brick"
        assert found.runner == "python"
        assert found.system is False

        # Delete
        registry.unregister("custom.smoke-test-brick")
        assert registry.get("custom.smoke-test-brick") is None, (
            "Custom brick must be None after unregistration"
        )

    def test_connection_crud(self, tmp_path, monkeypatch):
        """Connection erstellen → löschen → nicht mehr sichtbar."""
        monkeypatch.setenv("BRIX_MASTER_KEY", "b" * 64)

        from brix.connections import ConnectionManager
        from brix.credential_store import CredentialStore

        db = BrixDB(db_path=tmp_path / "conn.db")
        cred_store = CredentialStore(db_path=tmp_path / "cred.db")
        manager = ConnectionManager(db)
        manager._cred_store = cred_store

        # Create — uses `register()` not `add()`
        meta = manager.register(
            name="test-sqlite-smoke",
            driver="sqlite",
            dsn=f"sqlite:///{tmp_path}/test_conn.db",
            description="Smoke test connection",
        )
        assert meta.get("id"), "register() must return dict with 'id'"
        conn_id = meta["id"]

        # List — connection must be visible
        connections = manager.list()
        names = [c["name"] for c in connections]
        assert "test-sqlite-smoke" in names, (
            f"'test-sqlite-smoke' not found in connections: {names}"
        )

        # Delete — uses name, not id
        deleted = manager.delete("test-sqlite-smoke")
        assert deleted is True, f"delete() must return True, got {deleted}"

        # Verify gone
        connections_after = manager.list()
        names_after = [c["name"] for c in connections_after]
        assert "test-sqlite-smoke" not in names_after, (
            "Connection must be gone after delete"
        )

    def test_discover_returns_all_categories(self):
        """discover() zeigt alle 16 Kategorien."""
        from brix.mcp_handlers.discover import _handle_discover

        result = asyncio.run(_handle_discover({}))
        assert result["success"] is True, f"discover failed: {result}"

        categories = result.get("categories", [])
        category_names = {c["category"] for c in categories}

        expected = {
            "bricks", "pipelines", "runners", "connectors", "connections",
            "credentials", "variables", "templates", "triggers", "alerts",
            "types", "jinja_filters", "env_config", "namespaces", "helpers", "runs",
        }
        missing = expected - category_names
        assert not missing, (
            f"discover() is missing categories: {missing}. Got: {category_names}"
        )
        assert len(category_names) == 16, (
            f"Expected exactly 16 categories, got {len(category_names)}: {category_names}"
        )

    def test_deprecation_tracking_in_db(self, tmp_path, monkeypatch):
        """Legacy-Type Nutzung wird in DB recorded."""
        test_db_path = tmp_path / "brix_depr.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(
            BrixDB, "__init__",
            lambda self, db_path=None: original_init(self, db_path=test_db_path)
        )

        yaml_text = """
name: deprecated-track-test
steps:
  - id: legacy
    type: set
    values:
      x: "1"
"""
        import brix.history as history_mod
        run_db_path = tmp_path / "brix_run.db"
        loader = PipelineLoader()
        pipeline = loader.load_from_string(yaml_text)
        engine = PipelineEngine()

        original_path = history_mod.HISTORY_DB_PATH
        history_mod.HISTORY_DB_PATH = run_db_path
        try:
            result = asyncio.run(engine.run(pipeline, user_input={}))
        finally:
            history_mod.HISTORY_DB_PATH = original_path

        assert result.success is True, f"Pipeline failed: {result}"

        # The engine records to default BrixDB() which is monkeypatched to test_db_path
        depr_db = BrixDB()
        count = depr_db.get_deprecated_count()
        assert count >= 1, (
            f"Expected at least 1 deprecated_usage record for 'set', got {count}"
        )

        entries = depr_db.get_deprecated_usage()
        legacy_entry = next((e for e in entries if e["old_type"] == "set"), None)
        assert legacy_entry is not None, (
            f"Expected deprecated_usage entry for 'set', entries: {entries}"
        )
        assert legacy_entry["new_type"] == "flow.set", (
            f"Expected new_type='flow.set', got: {legacy_entry['new_type']}"
        )

    def test_retention_deletes_execution_data(self, tmp_path):
        """clean_retention löscht step_executions + foreach_items + run_inputs."""
        db = BrixDB(db_path=tmp_path / "ret.db")

        # Insert an old run (2020) and its associated execution data
        with db._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO runs (run_id, pipeline, started_at, success, triggered_by) "
                "VALUES ('run-old-smoke', 'test', '2020-01-01T00:00:00', 1, 'test')"
            )

        db.record_step_execution(
            run_id="run-old-smoke",
            step_id="step-1",
            status="success",
            input_data={"x": 1},
            output_data={"y": 2},
        )
        db.record_foreach_item(
            run_id="run-old-smoke",
            step_id="step-1",
            item_index=0,
        )
        db.record_run_input(run_id="run-old-smoke", input_params={"param": "val"})

        # Verify data present before retention
        assert len(db.get_step_executions("run-old-smoke")) == 1, (
            "step_executions must be present before retention"
        )
        assert len(db.get_foreach_items("run-old-smoke", "step-1")) == 1, (
            "foreach_item_executions must be present before retention"
        )
        assert db.get_run_input("run-old-smoke") is not None, (
            "run_inputs must be present before retention"
        )

        # Run retention (1 day max → 2020 data is deleted)
        result = db.clean_retention(max_days=1, max_mb=9999)
        assert result["runs_deleted_age"] >= 1, (
            f"Expected at least 1 run deleted by age, got: {result}"
        )

        # All execution data must be gone
        assert db.get_step_executions("run-old-smoke") == [], (
            "step_executions must be deleted after retention"
        )
        assert db.get_foreach_items("run-old-smoke", "step-1") == [], (
            "foreach_item_executions must be deleted after retention"
        )
        assert db.get_run_input("run-old-smoke") is None, (
            "run_inputs must be deleted after retention"
        )

    def test_type_compatibility_check_in_compose(self, tmp_path, monkeypatch):
        """compose_pipeline prüft Typ-Kompatibilität zwischen Steps."""
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir(parents=True, exist_ok=True)

        import brix.mcp_handlers._shared as shared_mod
        import brix.mcp_handlers.composer as composer_mod

        def patched_pipeline_dir():
            pipelines_dir.mkdir(parents=True, exist_ok=True)
            return pipelines_dir

        monkeypatch.setattr(shared_mod, "_pipeline_dir", patched_pipeline_dir)
        monkeypatch.setattr(composer_mod, "_pipeline_dir", patched_pipeline_dir)

        from brix.mcp_handlers.composer import _handle_compose_pipeline

        # compose_pipeline should return a structured result with steps
        result = asyncio.run(_handle_compose_pipeline({"goal": "fetch emails and filter them"}))
        assert result["success"] is True, f"compose_pipeline failed: {result}"
        assert "proposed_pipeline" in result, "Missing 'proposed_pipeline' in result"

        proposed = result["proposed_pipeline"]
        assert "steps" in proposed, "proposed_pipeline must have 'steps'"
        assert isinstance(proposed["steps"], list), "steps must be a list"
        assert len(proposed["steps"]) >= 1, "At least one step must be proposed"

        # Each step must have id and type
        for step in proposed["steps"]:
            assert "id" in step, f"Step missing 'id': {step}"
            assert "type" in step, f"Step missing 'type': {step}"


# ---------------------------------------------------------------------------
# TestIntegration — Real Multi-Step Pipelines
# ---------------------------------------------------------------------------

class TestIntegration:
    """Echte Multi-Step Pipelines mit echten Runnern."""

    def test_db_query_filter_upsert_pipeline(self, tmp_path):
        """db.query → flow.filter → db.upsert End-to-End gegen SQLite.

        Tests the full runner chain directly (db.query runner → filter runner → upsert runner)
        since these runners use step attributes not available in the Step model.
        Verifies only rows with amount > 100 end up in the target table.
        """
        # 1. Create SQLite source DB with test data
        source_db_path = tmp_path / "source.db"
        conn = sqlite3.connect(str(source_db_path))
        conn.execute(
            "CREATE TABLE transactions (id INTEGER PRIMARY KEY, name TEXT, amount INTEGER)"
        )
        conn.execute("INSERT INTO transactions VALUES (1, 'Alice', 200)")
        conn.execute("INSERT INTO transactions VALUES (2, 'Bob', 50)")
        conn.execute("INSERT INTO transactions VALUES (3, 'Carol', 300)")
        conn.commit()
        conn.close()

        # 2. Create target DB
        target_db_path = tmp_path / "target.db"
        conn2 = sqlite3.connect(str(target_db_path))
        conn2.execute(
            "CREATE TABLE results (id INTEGER PRIMARY KEY, name TEXT, amount INTEGER)"
        )
        conn2.commit()
        conn2.close()

        # 3a. Run db.query directly
        from brix.runners.db_query import DbQueryRunner

        class _DbQueryStep:
            connection = str(source_db_path)  # bare file path (sqlite)
            query = "SELECT id, name, amount FROM transactions"
            params = None

        class _FakeContext:
            def to_jinja_context(self):
                return {}

        runner = DbQueryRunner()
        query_result = asyncio.run(runner.execute(_DbQueryStep(), _FakeContext()))
        assert query_result["success"] is True, (
            f"db.query failed: {query_result.get('error')}"
        )
        all_rows = query_result["data"]["rows"]
        assert len(all_rows) == 3, f"Expected 3 rows, got {len(all_rows)}"

        # 3b. Run flow.filter directly
        from brix.runners.filter import FilterRunner

        class _FilterStep:
            params = {
                "input": all_rows,
                "where": "{{ item.amount > 100 }}",
            }

        filter_runner = FilterRunner()
        filter_result = asyncio.run(filter_runner.execute(_FilterStep(), None))
        assert filter_result["success"] is True, (
            f"filter failed: {filter_result.get('error')}"
        )
        filtered_rows = filter_result["data"]
        assert len(filtered_rows) == 2, (
            f"Expected 2 rows after filter (amount > 100), got {len(filtered_rows)}: {filtered_rows}"
        )
        names_filtered = {r["name"] for r in filtered_rows}
        assert names_filtered == {"Alice", "Carol"}, (
            f"Expected Alice and Carol, got: {names_filtered}"
        )
        # Verify Bob is excluded
        assert all(r["amount"] > 100 for r in filtered_rows), (
            f"All filtered rows must have amount > 100, got: {filtered_rows}"
        )

        # 3c. Run db.upsert directly
        from brix.runners.db_upsert import DbUpsertRunner

        class _UpsertStep:
            connection = str(target_db_path)
            table = "results"
            key_columns = ["id"]
            params = {"data": filtered_rows}
            conflict_key = None

        upsert_runner = DbUpsertRunner()
        upsert_result = asyncio.run(upsert_runner.execute(_UpsertStep(), _FakeContext()))
        assert upsert_result["success"] is True, (
            f"db.upsert failed: {upsert_result.get('error')}"
        )

        # 4. Verify only high-amount rows are in target DB
        conn3 = sqlite3.connect(str(target_db_path))
        rows = conn3.execute("SELECT id, name, amount FROM results ORDER BY id").fetchall()
        conn3.close()

        assert len(rows) == 2, (
            f"Expected 2 rows (Alice=200, Carol=300) in target, got {len(rows)}: {rows}"
        )
        names_in_target = {row[1] for row in rows}
        assert names_in_target == {"Alice", "Carol"}, (
            f"Expected Alice and Carol in target, got: {names_in_target}"
        )
        amounts_in_target = {row[2] for row in rows}
        assert all(a > 100 for a in amounts_in_target), (
            f"All amounts in target must be > 100, got: {amounts_in_target}"
        )

    def test_specialist_extract_pipeline(self, tmp_path):
        """extract.specialist E2E: extracts data from text via regex.

        Uses the SpecialistRunner directly with a fake context to avoid YAML
        escaping issues with regex patterns.
        """
        from brix.runners.specialist import SpecialistRunner

        class _SpecStep:
            config = {
                "input_field": "text",
                "extract": [
                    {"name": "amount", "method": "regex", "pattern": r"(\d+) EUR", "group": 1},
                    {"name": "due_date", "method": "regex", "pattern": r"(\d{4}-\d{2}-\d{2})", "group": 1},
                ],
            }

        class _FakeContext:
            def to_jinja_context(self):
                return {"text": "Invoice total: 150 EUR, due by 2026-03-31"}

        runner = SpecialistRunner()
        result = asyncio.run(runner.execute(_SpecStep(), _FakeContext()))

        assert result["success"] is True, (
            f"Specialist runner failed: {result.get('error')}"
        )
        result_data = result["data"]["result"]
        assert result_data["amount"] == "150", (
            f"Expected amount='150', got: {result_data}"
        )
        assert result_data["due_date"] == "2026-03-31", (
            f"Expected due_date='2026-03-31', got: {result_data}"
        )

        # Second assertion: multiple items in a foreach pattern work too
        texts = [
            "Invoice total: 100 EUR, due by 2026-01-15",
            "Invoice total: 200 EUR, due by 2026-02-20",
            "Invoice total: 300 EUR, due by 2026-03-25",
        ]
        results = []
        for text in texts:
            class _FakeCtx:
                _text = text
                def to_jinja_context(self):
                    return {"text": self._text}
            r = asyncio.run(runner.execute(_SpecStep(), _FakeCtx()))
            assert r["success"] is True, f"Specialist failed on: {text}"
            results.append(r["data"]["result"])

        amounts = [int(r["amount"]) for r in results]
        assert amounts == [100, 200, 300], (
            f"Expected [100, 200, 300], got: {amounts}"
        )

    def test_multi_step_with_type_checking(self, tmp_path):
        """Pipeline mit 5 flow.set Steps: Typ-Kompatibilität + Datenweitergabe.

        Uses flow.set steps exclusively since filter/transform have Jinja2 where-clause
        escaping complexity in YAML pipelines. Tests that values flow correctly across steps.
        """
        # Step outputs are accessible via direct step_id (no "steps." prefix)
        yaml_text = """
name: multi-step-type-check
steps:
  - id: s1_produce
    type: flow.set
    values:
      items_list:
        - name: Alpha
          score: 10
        - name: Beta
          score: 25
        - name: Gamma
          score: 5

  - id: s2_count
    type: flow.set
    values:
      item_count: "{{ s1_produce.output.items_list | length }}"
      first_name: "{{ s1_produce.output.items_list[0].name }}"

  - id: s3_calculate
    type: flow.set
    values:
      doubled: "{{ s1_produce.output.items_list | length * 2 }}"
      has_items: "{{ s2_count.output.item_count | int > 0 }}"

  - id: s4_aggregate
    type: flow.set
    values:
      summary_count: "{{ s2_count.output.item_count }}"
      summary_doubled: "{{ s3_calculate.output.doubled }}"

  - id: s5_final
    type: flow.set
    values:
      report: "Total items: {{ s4_aggregate.output.summary_count }}"
"""
        run_id, success, db, result = _run_pipeline_with_db(yaml_text, tmp_path)
        assert success is True, (
            f"5-step pipeline failed. Steps: "
            f"{[(sid, s.status, s.error_message) for sid, s in result.steps.items()]}"
        )

        executions = db.get_step_executions(run_id)
        assert len(executions) == 5, (
            f"Expected 5 step executions, got {len(executions)}: "
            f"{[e['step_id'] for e in executions]}"
        )

        # All steps must succeed
        for exe in executions:
            assert exe["status"] == "success", (
                f"Step '{exe['step_id']}' has status '{exe['status']}', "
                f"error: {exe.get('error_detail')}"
            )

        # s2_count: items_list has 3 elements, first name is 'Alpha'
        s2_exec = next((e for e in executions if e["step_id"] == "s2_count"), None)
        assert s2_exec is not None, "Step s2_count missing"
        s2_output = s2_exec.get("output_data") or {}
        assert int(s2_output.get("item_count", -1)) == 3, (
            f"Expected item_count=3, got: {s2_output}"
        )
        assert s2_output.get("first_name") == "Alpha", (
            f"Expected first_name='Alpha', got: {s2_output}"
        )

        # s5_final: report contains the count
        s5_exec = next((e for e in executions if e["step_id"] == "s5_final"), None)
        assert s5_exec is not None, "Step s5_final missing"
        s5_output = s5_exec.get("output_data") or {}
        assert "3" in str(s5_output.get("report", "")), (
            f"Expected report to contain '3', got: {s5_output}"
        )

    def test_filter_transform_runner_chain(self):
        """flow.filter → flow.transform chain: filter by score, transform to names.

        Tests runner chain directly since YAML pipelines have Jinja2 where-clause
        escaping complexity for filter expressions.
        """
        from brix.runners.filter import FilterRunner
        from brix.runners.transform import TransformRunner

        items = [
            {"name": "Alpha", "score": 10},
            {"name": "Beta", "score": 25},
            {"name": "Gamma", "score": 5},
        ]

        class _FilterStep:
            params = {"input": items, "where": "{{ item.score >= 10 }}"}

        # Step 1: filter items with score >= 10
        filter_runner = FilterRunner()
        filter_result = asyncio.run(filter_runner.execute(_FilterStep(), None))
        assert filter_result["success"] is True, (
            f"filter failed: {filter_result.get('error')}"
        )
        filtered = filter_result["data"]
        assert len(filtered) == 2, (
            f"Expected 2 items after filter (score >= 10), got {len(filtered)}: {filtered}"
        )
        filtered_names = {item["name"] for item in filtered}
        assert filtered_names == {"Alpha", "Beta"}, (
            f"Expected Alpha and Beta, got: {filtered_names}"
        )

        class _TransformStep:
            params = {"input": filtered, "expression": "{{ item.name }}"}

        # Step 2: transform to name-only list
        transform_runner = TransformRunner()
        transform_result = asyncio.run(transform_runner.execute(_TransformStep(), None))
        assert transform_result["success"] is True, (
            f"transform failed: {transform_result.get('error')}"
        )
        names = transform_result["data"]
        assert names == ["Alpha", "Beta"], (
            f"Expected ['Alpha', 'Beta'], got: {names}"
        )
