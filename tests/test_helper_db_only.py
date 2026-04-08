from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from brix.db import BrixDB
from brix.helper_registry import HelperRegistry
from brix.runners.python import PythonRunner
from brix.startup_sync import _sync_helpers, run_startup_sync


@pytest.fixture
def helper_db(tmp_path: Path) -> BrixDB:
    return BrixDB(db_path=tmp_path / "helpers.db")


@pytest.fixture
def db_registry(helper_db: BrixDB, monkeypatch: pytest.MonkeyPatch) -> HelperRegistry:
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    return HelperRegistry(db=helper_db)


@pytest.mark.asyncio
async def test_helper_code_read_from_db_not_disk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    db_registry: HelperRegistry,
) -> None:
    code = "import json\nprint(json.dumps({'source': 'db'}))\n"
    content_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    db_registry.register(
        name="db_only_helper",
        script="/app/helpers/does-not-exist.py",
        description="Helper stored only in database",
        input_schema={},
        output_schema={},
        code=code,
    )
    monkeypatch.setattr("brix.runners.python.HELPER_CACHE_DIR", tmp_path / "cache")

    step = SimpleNamespace(id="s1", helper="db_only_helper", params={}, timeout=None, progress=False)
    context = SimpleNamespace(credentials={}, workdir=None)

    result = await PythonRunner().execute(step, context)

    assert result["success"] is True
    assert result["data"] == {"source": "db"}
    assert (tmp_path / "cache" / f"db_only_helper_{content_hash}.py").exists()


@pytest.mark.asyncio
async def test_cache_file_created_with_hash_in_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    db_registry: HelperRegistry,
) -> None:
    code = "import json\nprint(json.dumps({'ok': True}))\n"
    content_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr("brix.runners.python.HELPER_CACHE_DIR", cache_dir)

    db_registry.register(
        name="hash_helper",
        description="Hash named helper cache file",
        input_schema={},
        output_schema={},
        code=code,
    )

    step = SimpleNamespace(id="s1", helper="hash_helper", params={}, timeout=None, progress=False)
    context = SimpleNamespace(credentials={}, workdir=None)
    result = await PythonRunner().execute(step, context)

    assert result["success"] is True
    assert cache_dir.glob("hash_helper_*.py")
    assert (cache_dir / f"hash_helper_{content_hash}.py").exists()


@pytest.mark.asyncio
async def test_create_helper_without_description_rejected(
    helper_db: BrixDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brix.mcp_handlers import helpers as hh

    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    monkeypatch.setattr("brix.db.BrixDB", lambda *args, **kwargs: helper_db)

    result = await hh._handle_create_helper(
        {
            "name": "missing_description",
            "code": "print('x')\n",
            "input_schema": {},
            "output_schema": {},
        }
    )

    assert result["success"] is False
    assert "description" in result["error"]


@pytest.mark.asyncio
async def test_create_helper_without_input_schema_rejected(
    helper_db: BrixDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brix.mcp_handlers import helpers as hh

    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    monkeypatch.setattr("brix.db.BrixDB", lambda *args, **kwargs: helper_db)

    result = await hh._handle_create_helper(
        {
            "name": "missing_input_schema",
            "code": "print('x')\n",
            "description": "Valid helper description",
            "output_schema": {},
        }
    )

    assert result["success"] is False
    assert "input_schema" in result["error"]


@pytest.mark.asyncio
async def test_create_helper_persists_empty_script_path(
    helper_db: BrixDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brix.mcp_handlers import helpers as hh

    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    monkeypatch.setattr("brix.db.BrixDB", lambda *args, **kwargs: helper_db)

    result = await hh._handle_create_helper(
        {
            "name": "db_only_script_path",
            "code": "print('x')\n",
            "description": "DB-only helper should not store a fake path",
            "input_schema": {},
            "output_schema": {},
        }
    )

    assert result["success"] is True
    helper = helper_db.get_helper("db_only_script_path")
    assert helper is not None
    assert helper["script_path"] == ""


@pytest.mark.asyncio
async def test_delete_helper_removes_cache_file(
    tmp_path: Path,
    helper_db: BrixDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brix.mcp_handlers import helpers as hh

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setattr("brix.mcp_handlers.helpers._HELPER_CACHE_DIR", cache_dir)
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    monkeypatch.setattr("brix.db.BrixDB", lambda *args, **kwargs: helper_db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._scan_pipelines_for_helper", lambda name: [])

    code = "print('cached')\n"
    content_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    helper_db.upsert_helper(
        name="delete_me",
        script_path="db://delete_me",
        description="Helper to delete from database",
        input_schema={},
        output_schema={},
        code=code,
        content_hash=content_hash,
    )
    cache_file = cache_dir / f"delete_me_{content_hash}.py"
    cache_file.write_text(code, encoding="utf-8")

    result = await hh._handle_delete_helper({"name": "delete_me"})

    assert result["success"] is True
    assert not cache_file.exists()
    assert helper_db.get_helper("delete_me") is None


@pytest.mark.asyncio
async def test_delete_helper_blocked_by_db_step_reference(
    helper_db: BrixDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brix.mcp_handlers import helpers as hh

    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    monkeypatch.setattr("brix.db.BrixDB", lambda *args, **kwargs: helper_db)
    monkeypatch.setattr("brix.mcp_handlers.helpers._scan_pipelines_for_helper", lambda name: [])

    helper_db.upsert_helper(
        name="still_used",
        script_path="db://still_used",
        description="Helper that is still referenced by a DB-backed step",
        input_schema={},
        output_schema={},
        code="print('still used')\n",
    )
    helper_db.upsert_pipeline(
        name="db-only-pipeline",
        path="/tmp/db-only-pipeline.yaml",
    )
    pipeline = helper_db.get_pipeline("db-only-pipeline")
    assert pipeline is not None
    helper_db.upsert_step(
        pipeline["id"],
        {"id": "call", "type": "script.python", "helper": "still_used"},
        step_order=0,
    )

    result = await hh._handle_delete_helper({"name": "still_used"})

    assert result["success"] is False
    assert result["affected_pipelines"] == ["db-only-pipeline"]
    assert helper_db.get_helper("still_used") is not None


def test_startup_sync_does_not_scan_disk_for_helpers(
    helper_db: BrixDB,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom() -> dict[str, Path]:
        raise AssertionError("helper disk scan should not run")

    monkeypatch.setattr("brix.startup_sync._scan_helper_files", _boom)

    assert _sync_helpers(helper_db) == 0
    result = run_startup_sync(helper_db)
    assert result["orphan_triggers"] >= 0
