"""Regression tests for T-BRIX-BUG-101 list-safe step rendering."""

from __future__ import annotations

import sqlite3

import pytest

import brix.context as context_module
import brix.db as db_module
import brix.history as history_module
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.models import Step


def test_render_step_params_renders_list_items_individually() -> None:
    loader = PipelineLoader()
    step = Step(
        id="insert_user",
        type="db.exec",
        params=["{{ input.name }}", "{{ input.age }}"],
    )

    rendered = loader.render_step_params(step, {"input": {"name": "Alice", "age": 33}})

    assert rendered == ["Alice", 33]


@pytest.mark.asyncio
async def test_engine_run_db_exec_with_rendered_positional_params(tmp_path) -> None:
    db_module.BRIX_DB_PATH = tmp_path / "brix.db"
    history_module.HISTORY_DB_PATH = db_module.BRIX_DB_PATH
    context_module.WORKDIR_BASE = tmp_path / "runs"
    context_module.CACHE_BASE = tmp_path / "cache"

    db_path = tmp_path / "bug101.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE users (name TEXT, age INTEGER)")
        conn.commit()
    finally:
        conn.close()

    pipeline = PipelineLoader().load_from_string(
        f"""
name: bug101-list-rendering
input:
  name:
    type: string
  age:
    type: integer
steps:
  - id: prepare
    type: flow.set
    values:
      name: "{{{{ input.name }}}}"
      age: "{{{{ input.age }}}}"

  - id: insert_user
    type: db.exec
    connection: "sqlite:///{db_path}"
    query: "INSERT INTO users (name, age) VALUES (?, ?)"
    params:
      - "{{{{ input.name }}}}"
      - "{{{{ input.age }}}}"
"""
    )

    engine = PipelineEngine()
    engine.register_runner("flow.set", engine._runners["set"])
    engine.register_runner("db.exec", engine._runners["db_exec"])

    result = await engine.run(
        pipeline,
        user_input={"name": "Alice", "age": 33},
    )

    assert result.success is True
    assert result.steps["prepare"].status == "ok"
    assert result.steps["insert_user"].status == "ok"

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT name, age FROM users").fetchone()
    finally:
        conn.close()

    assert row == ("Alice", 33)
