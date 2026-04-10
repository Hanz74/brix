import yaml

import pytest

from brix.db import BrixDB
from brix.models import Pipeline
from brix.pipeline_store import PipelineStore


PIPELINE_DATA = {
    "name": "db-pipeline",
    "version": "1.2.3",
    "description": "DB-backed pipeline",
    "project": "demo",
    "group": "ops",
    "tags": ["alpha", "beta"],
    "input": {
        "query": {"type": "string", "description": "Search query"},
        "limit": {"type": "integer", "default": 5, "description": "Result limit"},
    },
    "credentials": {
        "api_key": "API_KEY",
        "oauth": {"env": "OAUTH_TOKEN", "refresh": {"type": "oauth2"}},
    },
    "steps": [
        {"id": "prepare", "type": "flow.set", "values": {"ready": True}},
        {"id": "run", "type": "cli", "args": ["echo", "{{ input.query }}"]},
    ],
}


@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "db_only_store.db")


@pytest.fixture
def store(tmp_path, db):
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=db)


def _mark_migrated(db: BrixDB, name: str) -> dict:
    row = db.get_pipeline(name)
    assert row is not None
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET migration_status='v71_complete' WHERE id=?",
            (row["id"],),
        )
    updated = db.get_pipeline(name)
    assert updated is not None
    return updated


def test_load_with_v71_complete_loads_from_step_rows(store, db):
    yaml_only = {
        "name": "migrated-pipeline",
        "steps": [{"id": "yaml-step", "type": "flow.set", "values": {"source": "yaml"}}],
    }
    pipeline_id = db.upsert_pipeline(
        name="migrated-pipeline",
        path="/virtual/migrated-pipeline.yaml",
        yaml_content=yaml.dump(yaml_only, sort_keys=False),
    )
    db.upsert_step(
        pipeline_id,
        {"id": "db-step", "type": "flow.set", "values": {"source": "db"}},
        step_order=0,
    )
    _mark_migrated(db, "migrated-pipeline")

    pipeline = store.load("migrated-pipeline")

    assert pipeline.steps[0].id == "db-step"
    assert pipeline.steps[0].values == {"source": "db"}


def test_load_with_null_migration_status_still_reads_db_rows(store, db):
    pipeline_id = db.upsert_pipeline(
        name="legacy-pipeline",
        path="/virtual/legacy-pipeline.yaml",
        yaml_content=yaml.dump({
            "name": "legacy-pipeline",
            "steps": [{"id": "yaml-step", "type": "flow.set", "values": {"source": "yaml"}}],
        }, sort_keys=False),
    )
    db.upsert_step(
        pipeline_id,
        {"id": "db-step", "type": "flow.set", "values": {"source": "db"}},
        step_order=0,
    )

    pipeline = store.load("legacy-pipeline")

    assert pipeline.steps[0].id == "db-step"
    assert pipeline.steps[0].values == {"source": "db"}


def test_save_writes_step_rows_without_yaml_content_or_disk_file(store, db, tmp_path):
    path = store.save(PIPELINE_DATA)
    row = db.get_pipeline("db-pipeline")

    assert row is not None
    assert row["migration_status"] == "v71_complete"
    assert path == tmp_path / "db-pipeline.yaml"
    assert not path.exists()
    assert db.get_pipeline_yaml_content("db-pipeline") is None

    steps = db.get_steps(row["id"])
    credentials = db.get_pipeline_credentials(row["id"])
    inputs = db.get_pipeline_inputs(row["id"])

    assert [step["id"] for step in steps] == ["prepare", "run"]
    assert {item["name"] for item in credentials} == {"api_key", "oauth"}
    assert {item["name"] for item in inputs} == {"limit", "query"}


def test_load_raw_with_migrated_pipeline_returns_dict_from_db_rows(store, db):
    yaml_only = {
        "name": "raw-pipeline",
        "description": "yaml description",
        "steps": [{"id": "yaml-step", "type": "flow.set", "values": {"source": "yaml"}}],
    }
    pipeline_id = db.upsert_pipeline(
        name="raw-pipeline",
        path="/virtual/raw-pipeline.yaml",
        yaml_content=yaml.dump(yaml_only, sort_keys=False),
    )
    db.upsert_step(
        pipeline_id,
        {"id": "db-step", "type": "flow.set", "values": {"source": "db"}},
        step_order=0,
    )
    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET migration_status='v71_complete', description=? WHERE id=?",
            ("db description", pipeline_id),
        )

    raw = store.load_raw("raw-pipeline")

    assert raw["description"] == "db description"
    assert raw["steps"][0]["id"] == "db-step"
    assert raw["steps"][0]["type"] == "flow.set"
    assert raw["steps"][0]["values"] == {"source": "db"}


def test_brix_step_source_yaml_does_not_override_db_rows(monkeypatch, store, db):
    monkeypatch.setenv("BRIX_STEP_SOURCE", "yaml")
    yaml_only = {
        "name": "rollback-pipeline",
        "steps": [{"id": "yaml-step", "type": "flow.set", "values": {"source": "yaml"}}],
    }
    pipeline_id = db.upsert_pipeline(
        name="rollback-pipeline",
        path="/virtual/rollback-pipeline.yaml",
        yaml_content=yaml.dump(yaml_only, sort_keys=False),
    )
    db.upsert_step(
        pipeline_id,
        {"id": "db-step", "type": "flow.set", "values": {"source": "db"}},
        step_order=0,
    )
    _mark_migrated(db, "rollback-pipeline")

    pipeline = store.load("rollback-pipeline")

    assert pipeline.steps[0].id == "db-step"
    assert pipeline.steps[0].values == {"source": "db"}


def test_dual_mode_does_not_validate_against_yaml_content(monkeypatch, store, db):
    monkeypatch.setenv("BRIX_STEP_SOURCE", "dual")
    yaml_only = {
        "name": "dual-pipeline",
        "steps": [{"id": "yaml-step", "type": "flow.set", "values": {"source": "yaml"}}],
    }
    pipeline_id = db.upsert_pipeline(
        name="dual-pipeline",
        path="/virtual/dual-pipeline.yaml",
        yaml_content=yaml.dump(yaml_only, sort_keys=False),
    )
    db.upsert_step(
        pipeline_id,
        {"id": "db-step", "type": "flow.set", "values": {"source": "db"}},
        step_order=0,
    )
    _mark_migrated(db, "dual-pipeline")

    pipeline = store.load("dual-pipeline")

    assert pipeline.steps[0].id == "db-step"
    assert pipeline.steps[0].values == {"source": "db"}


def test_exists_ignores_filesystem_only_pipeline(tmp_path, store):
    (tmp_path / "filesystem-only.yaml").write_text(
        "name: filesystem-only\nsteps:\n  - id: s1\n    type: flow.set\n",
        encoding="utf-8",
    )

    assert store.exists("filesystem-only") is False


def test_roundtrip_save_then_load_returns_identical_pipeline(store):
    store.save(PIPELINE_DATA)

    loaded = store.load("db-pipeline")
    expected = Pipeline.model_validate(PIPELINE_DATA)

    assert loaded == expected
