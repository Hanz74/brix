from __future__ import annotations

import json
from pathlib import Path

from brix.db import BrixDB


def test_seed_data_includes_effective_step_tools() -> None:
    seed = json.loads(Path("seed-data.json").read_text())
    names = {entry["name"] for entry in seed["mcp_tool_schemas"]}

    assert "brix__materialize_step" in names
    assert "brix__inspect_effective_pipeline" in names


def test_migration_registers_effective_step_tool_schemas(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "effective_tools.db")

    materialize = db.mcp_tool_schemas_get("brix__materialize_step")
    inspect_pipeline = db.mcp_tool_schemas_get("brix__inspect_effective_pipeline")

    assert materialize is not None
    assert inspect_pipeline is not None
    assert materialize["description"]
    assert inspect_pipeline["description"]
