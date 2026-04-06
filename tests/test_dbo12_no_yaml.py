from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


PIPELINE_DATA = {
    "name": "dbo12-pipeline",
    "version": "1.0.0",
    "description": "DB rows only",
    "steps": [
        {"id": "fetch", "type": "http.request", "url": "https://old.example.com"},
        {"id": "transform", "type": "flow.set", "values": {"ok": True}},
    ],
}


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = tmp_path / "dbo12.db"
    monkeypatch.setattr("brix.db.BRIX_DB_PATH", db_path)
    return BrixDB(db_path=db_path)


@pytest.fixture
def store(tmp_path, db):
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)


def _pipeline_row(db: BrixDB, name: str) -> dict:
    row = db.get_pipeline(name)
    assert row is not None
    return row


def test_save_does_not_write_yaml_content(store, db):
    store.save(dict(PIPELINE_DATA))

    with db._connect() as conn:
        row = conn.execute(
            "SELECT yaml_content FROM pipeline WHERE name=?",
            (PIPELINE_DATA["name"],),
        ).fetchone()

    assert row is not None
    assert row[0] in (None, "")


def test_load_reads_from_db_rows_when_yaml_content_is_stale(store, db):
    store.save(dict(PIPELINE_DATA))
    row = _pipeline_row(db, PIPELINE_DATA["name"])

    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET yaml_content=? WHERE id=?",
            ("name: stale\nsteps: []\n", row["id"]),
        )

    loaded = store.load(PIPELINE_DATA["name"])

    assert loaded.name == PIPELINE_DATA["name"]
    assert [step.id for step in loaded.steps] == ["fetch", "transform"]
    assert loaded.steps[0].url == "https://old.example.com"


@pytest.mark.asyncio
async def test_update_step_then_load_returns_current_data(tmp_path, monkeypatch, db):
    monkeypatch.setattr("brix.mcp_server.PIPELINE_DIR", tmp_path)

    from brix.mcp_server import _handle_create_pipeline, _handle_update_step

    pipeline_name = "dbo12-update-step"
    create_result = await _handle_create_pipeline(
        {
            "name": pipeline_name,
            "steps": [{"id": "fetch", "type": "http.request", "url": "https://old.example.com"}],
        }
    )
    assert create_result["success"] is True

    row = _pipeline_row(db, pipeline_name)
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET yaml_content=? WHERE id=?",
            ("name: stale\nsteps:\n  - id: fetch\n    type: flow.set\n", row["id"]),
        )

    result = await _handle_update_step(
        {
            "pipeline_name": pipeline_name,
            "step_id": "fetch",
            "updates": {"url": "https://new.example.com", "timeout": "30s"},
        }
    )
    reloaded = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db).load_raw(
        pipeline_name
    )

    assert result["success"] is True
    assert reloaded["steps"][0]["id"] == "fetch"
    assert reloaded["steps"][0]["url"] == "https://new.example.com"
    assert reloaded["steps"][0]["timeout"] == "30s"


def test_load_raw_returns_dict_from_db_rows(store, db):
    store.save(dict(PIPELINE_DATA))
    row = _pipeline_row(db, PIPELINE_DATA["name"])

    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET yaml_content=? WHERE id=?",
            ("name: stale\nsteps: []\n", row["id"]),
        )

    raw = store.load_raw(PIPELINE_DATA["name"])

    assert raw["name"] == PIPELINE_DATA["name"]
    assert raw["description"] == "DB rows only"
    assert [step["id"] for step in raw["steps"]] == ["fetch", "transform"]
