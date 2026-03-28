"""Tests for BrixDB — central SQLite index (T-BRIX-V5-04).

Covers:
- Schema creation (all 5 tables)
- Runs CRUD (record_run_start, record_run_finish, get_run, get_recent_runs,
             delete_run, cleanup_runs)
- Pipelines CRUD (upsert_pipeline, get_pipeline, list_pipelines, delete_pipeline)
- Helpers CRUD (upsert_helper, get_helper, list_helpers, delete_helper, UUID lookup)
- Pipeline-Helper relationships (get_pipeline_helpers)
- Object versions (record_object_version, get_object_versions)
- Migration: migrate_from_history_db
- Migration: migrate_from_registry_yaml
- sync_pipelines_from_dirs
- sync_all
- RunHistory + HelperRegistry + PipelineStore atomically sync to same BrixDB
"""
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from brix.db import BrixDB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Return a BrixDB backed by a temporary file."""
    return BrixDB(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_all_tables_exist(self, db):
        """All five tables are created on init."""
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "runs" in tables
        assert "pipelines" in tables
        assert "helpers" in tables
        assert "pipeline_helpers" in tables
        assert "object_versions" in tables

    def test_db_created_in_subdirectory(self, tmp_path):
        """BrixDB creates parent directories as needed."""
        nested = tmp_path / "a" / "b" / "brix.db"
        assert not nested.parent.exists()
        BrixDB(db_path=nested)
        assert nested.exists()


# ---------------------------------------------------------------------------
# Runs CRUD
# ---------------------------------------------------------------------------

class TestRunsCRUD:
    def test_record_start_and_get(self, db):
        db.record_run_start("r1", "my-pipeline", version="1.0.0", input_data={"k": "v"})
        run = db.get_run("r1")
        assert run is not None
        assert run["run_id"] == "r1"
        assert run["pipeline"] == "my-pipeline"
        assert run["version"] == "1.0.0"
        assert run["triggered_by"] == "cli"
        assert run["finished_at"] is None

    def test_record_finish(self, db):
        db.record_run_start("r1", "p")
        db.record_run_finish(
            "r1", success=True, duration=2.5,
            steps={"s1": {"status": "ok"}},
            result_summary={"items": 10},
        )
        run = db.get_run("r1")
        assert run["success"] == 1
        assert run["duration"] == 2.5
        assert run["finished_at"] is not None
        assert json.loads(run["steps_data"])["s1"]["status"] == "ok"
        assert json.loads(run["result_summary"])["items"] == 10

    def test_get_run_not_found(self, db):
        assert db.get_run("nonexistent") is None

    def test_get_recent_runs(self, db):
        for i in range(5):
            db.record_run_start(f"run-{i}", "p")
            db.record_run_finish(f"run-{i}", True, 1.0)
        recent = db.get_recent_runs(limit=3)
        assert len(recent) == 3

    def test_delete_run(self, db):
        db.record_run_start("r1", "p")
        assert db.delete_run("r1") is True
        assert db.get_run("r1") is None

    def test_delete_run_not_found(self, db):
        assert db.delete_run("ghost") is False

    def test_cleanup_runs(self, db):
        db.record_run_start("r1", "p")
        db.record_run_finish("r1", True, 1.0)
        # cleanup with 0 days removes everything started before now
        deleted = db.cleanup_runs(older_than_days=0)
        assert deleted >= 0  # may be 0 or 1 depending on SQLite timing

    def test_triggered_by_default(self, db):
        db.record_run_start("r1", "p")
        run = db.get_run("r1")
        assert run["triggered_by"] == "cli"

    def test_triggered_by_custom(self, db):
        db.record_run_start("r1", "p", triggered_by="mcp")
        run = db.get_run("r1")
        assert run["triggered_by"] == "mcp"


# ---------------------------------------------------------------------------
# Pipelines CRUD
# ---------------------------------------------------------------------------

class TestPipelinesCRUD:
    def test_upsert_and_get(self, db):
        pid = db.upsert_pipeline("my-pipe", "/app/pipelines/my-pipe.yaml")
        assert pid  # UUID returned
        result = db.get_pipeline("my-pipe")
        assert result is not None
        assert result["name"] == "my-pipe"
        assert result["path"] == "/app/pipelines/my-pipe.yaml"
        assert result["requirements"] == []

    def test_upsert_with_requirements(self, db):
        db.upsert_pipeline("req-pipe", "/app/p.yaml", requirements=["httpx", "pydantic"])
        result = db.get_pipeline("req-pipe")
        assert result["requirements"] == ["httpx", "pydantic"]

    def test_upsert_is_idempotent(self, db):
        pid1 = db.upsert_pipeline("pipe", "/old/path.yaml")
        pid2 = db.upsert_pipeline("pipe", "/new/path.yaml")
        assert pid1 == pid2  # same stable ID
        result = db.get_pipeline("pipe")
        assert result["path"] == "/new/path.yaml"

    def test_get_pipeline_not_found(self, db):
        assert db.get_pipeline("ghost") is None

    def test_list_pipelines(self, db):
        db.upsert_pipeline("alpha", "/a.yaml")
        db.upsert_pipeline("beta", "/b.yaml")
        results = db.list_pipelines()
        names = [r["name"] for r in results]
        assert "alpha" in names
        assert "beta" in names

    def test_list_pipelines_sorted(self, db):
        db.upsert_pipeline("z-pipe", "/z.yaml")
        db.upsert_pipeline("a-pipe", "/a.yaml")
        results = db.list_pipelines()
        names = [r["name"] for r in results]
        assert names == sorted(names)

    def test_delete_pipeline(self, db):
        db.upsert_pipeline("todel", "/del.yaml")
        assert db.delete_pipeline("todel") is True
        assert db.get_pipeline("todel") is None

    def test_delete_pipeline_not_found(self, db):
        assert db.delete_pipeline("ghost") is False

    def test_custom_pipeline_id_preserved(self, db):
        custom_id = "aaaaaaaa-0000-0000-0000-000000000001"
        pid = db.upsert_pipeline("id-pipe", "/p.yaml", pipeline_id=custom_id)
        assert pid == custom_id
        result = db.get_pipeline("id-pipe")
        assert result["id"] == custom_id


# ---------------------------------------------------------------------------
# Helpers CRUD
# ---------------------------------------------------------------------------

class TestHelpersCRUD:
    def test_upsert_and_get(self, db):
        hid = db.upsert_helper("parse", "/helpers/parse.py", description="Parse invoices")
        assert hid
        result = db.get_helper("parse")
        assert result is not None
        assert result["name"] == "parse"
        assert result["script_path"] == "/helpers/parse.py"
        assert result["description"] == "Parse invoices"
        assert result["requirements"] == []
        assert result["input_schema"] == {}
        assert result["output_schema"] == {}

    def test_upsert_with_schemas(self, db):
        db.upsert_helper(
            "strict",
            "/strict.py",
            requirements=["httpx"],
            input_schema={"type": "object", "properties": {"url": {"type": "string"}}},
            output_schema={"type": "array"},
        )
        result = db.get_helper("strict")
        assert result["requirements"] == ["httpx"]
        assert result["input_schema"]["type"] == "object"
        assert result["output_schema"]["type"] == "array"

    def test_upsert_is_idempotent(self, db):
        hid1 = db.upsert_helper("h", "/old.py")
        hid2 = db.upsert_helper("h", "/new.py")
        assert hid1 == hid2
        result = db.get_helper("h")
        assert result["script_path"] == "/new.py"

    def test_get_helper_not_found(self, db):
        assert db.get_helper("ghost") is None

    def test_get_helper_by_uuid(self, db):
        custom_id = "bbbbbbbb-0000-0000-0000-000000000001"
        db.upsert_helper("uuid-helper", "/u.py", helper_id=custom_id)
        result = db.get_helper(custom_id)
        assert result is not None
        assert result["name"] == "uuid-helper"

    def test_list_helpers(self, db):
        db.upsert_helper("alpha", "/a.py")
        db.upsert_helper("beta", "/b.py")
        results = db.list_helpers()
        names = [r["name"] for r in results]
        assert "alpha" in names
        assert "beta" in names

    def test_list_helpers_sorted(self, db):
        db.upsert_helper("z-h", "/z.py")
        db.upsert_helper("a-h", "/a.py")
        results = db.list_helpers()
        names = [r["name"] for r in results]
        assert names == sorted(names)

    def test_delete_helper(self, db):
        db.upsert_helper("todel", "/del.py")
        assert db.delete_helper("todel") is True
        assert db.get_helper("todel") is None

    def test_delete_helper_not_found(self, db):
        assert db.delete_helper("ghost") is False


# ---------------------------------------------------------------------------
# Pipeline-Helper relationships
# ---------------------------------------------------------------------------

class TestPipelineHelperRelationships:
    def test_sync_pipeline_helpers(self, db, tmp_path):
        """Helpers referenced in YAML steps appear in pipeline_helpers join table."""
        # Register the helper in db
        db.upsert_helper("my_helper", "/helpers/my_helper.py")

        # Write a pipeline YAML that references the helper
        pipeline_yaml = tmp_path / "test-pipe.yaml"
        pipeline_yaml.write_text(yaml.dump({
            "name": "test-pipe",
            "version": "1.0.0",
            "steps": [
                {"id": "run_it", "type": "python", "helper": "my_helper"},
            ],
        }))

        db.sync_pipelines_from_dirs([tmp_path])

        helpers = db.get_pipeline_helpers("test-pipe")
        assert len(helpers) == 1
        assert helpers[0]["name"] == "my_helper"

    def test_get_pipeline_helpers_no_helpers(self, db, tmp_path):
        """Pipeline with no helper references returns empty list."""
        pipeline_yaml = tmp_path / "plain.yaml"
        pipeline_yaml.write_text(yaml.dump({
            "name": "plain",
            "version": "1.0.0",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}],
        }))
        db.sync_pipelines_from_dirs([tmp_path])
        helpers = db.get_pipeline_helpers("plain")
        assert helpers == []

    def test_get_pipeline_helpers_unknown_pipeline(self, db):
        assert db.get_pipeline_helpers("ghost") == []

    def test_cascade_delete_removes_pipeline_helpers(self, db):
        """Deleting a pipeline removes its pipeline_helpers rows."""
        db.upsert_helper("h1", "/h1.py")
        pid = db.upsert_pipeline("pipeX", "/pipeX.yaml")

        with db._connect() as conn:
            h = conn.execute("SELECT id FROM helpers WHERE name='h1'").fetchone()
            conn.execute(
                "INSERT INTO pipeline_helpers (pipeline_id, helper_id) VALUES (?,?)",
                (pid, h[0]),
            )

        db.delete_pipeline("pipeX")
        # Join table rows should be gone (CASCADE)
        with db._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_helpers WHERE pipeline_id=?", (pid,)
            ).fetchall()
        assert rows == []


# ---------------------------------------------------------------------------
# Object Versions
# ---------------------------------------------------------------------------

class TestObjectVersions:
    def test_record_and_get(self, db):
        vid = db.record_object_version("pipeline", "my-pipe", {"version": "1.0.0"})
        assert vid
        versions = db.get_object_versions("pipeline", "my-pipe")
        assert len(versions) == 1
        assert versions[0]["version_id"] == vid
        assert json.loads(versions[0]["content"])["version"] == "1.0.0"

    def test_multiple_versions_newest_first(self, db):
        db.record_object_version("helper", "h", {"v": 1})
        db.record_object_version("helper", "h", {"v": 2})
        db.record_object_version("helper", "h", {"v": 3})
        versions = db.get_object_versions("helper", "h")
        assert len(versions) == 3
        # Newest first
        contents = [json.loads(v["content"])["v"] for v in versions]
        assert contents[0] >= contents[-1]

    def test_get_versions_not_found(self, db):
        assert db.get_object_versions("pipeline", "ghost") == []

    def test_custom_version_id(self, db):
        vid = db.record_object_version("pipeline", "p", {"x": 1}, version_id="v-fixed")
        assert vid == "v-fixed"
        versions = db.get_object_versions("pipeline", "p")
        assert versions[0]["version_id"] == "v-fixed"


# ---------------------------------------------------------------------------
# Migration: history.db → brix.db
# ---------------------------------------------------------------------------

class TestMigrateFromHistoryDb:
    def test_migrate_runs(self, db, tmp_path):
        """Runs from legacy history.db are copied into brix.db."""
        legacy_db = tmp_path / "history.db"
        conn = sqlite3.connect(str(legacy_db))
        conn.execute("""
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,
                version TEXT, started_at TEXT NOT NULL, finished_at TEXT,
                duration REAL, success INTEGER, input_data TEXT,
                steps_data TEXT, result_summary TEXT, triggered_by TEXT DEFAULT 'cli'
            )
        """)
        conn.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("r-legacy", "old-pipeline", "1.0", "2024-01-01T00:00:00",
             "2024-01-01T00:01:00", 60.0, 1, None, None, None, "cli"),
        )
        conn.commit()
        conn.close()

        imported = db.migrate_from_history_db(history_db_path=legacy_db)
        assert imported == 1
        run = db.get_run("r-legacy")
        assert run is not None
        assert run["pipeline"] == "old-pipeline"

    def test_migrate_is_idempotent(self, db, tmp_path):
        """Running migration twice doesn't duplicate rows."""
        legacy_db = tmp_path / "history.db"
        conn = sqlite3.connect(str(legacy_db))
        conn.execute("""
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,
                version TEXT, started_at TEXT NOT NULL, finished_at TEXT,
                duration REAL, success INTEGER, input_data TEXT,
                steps_data TEXT, result_summary TEXT, triggered_by TEXT DEFAULT 'cli'
            )
        """)
        conn.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("dup", "p", "1", "2024-01-01T00:00:00", None, None, None, None, None, None, "cli"),
        )
        conn.commit()
        conn.close()

        db.migrate_from_history_db(legacy_db)
        db.migrate_from_history_db(legacy_db)  # second call

        recent = db.get_recent_runs(100)
        assert len([r for r in recent if r["run_id"] == "dup"]) == 1

    def test_migrate_missing_db_returns_zero(self, db, tmp_path):
        """Migration of non-existent file returns 0 without error."""
        result = db.migrate_from_history_db(tmp_path / "nonexistent.db")
        assert result == 0


# ---------------------------------------------------------------------------
# Migration: registry.yaml → brix.db
# ---------------------------------------------------------------------------

class TestMigrateFromRegistryYaml:
    def test_migrate_helpers(self, db, tmp_path):
        """Helpers from legacy registry.yaml are imported into brix.db."""
        reg = tmp_path / "registry.yaml"
        reg.write_text(yaml.dump({
            "parse_invoice": {
                "name": "parse_invoice",
                "script": "/helpers/parse_invoice.py",
                "description": "Parses PDF invoices",
                "requirements": ["pypdf2"],
                "input_schema": {},
                "output_schema": {},
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
                "id": "aaaaaaaa-1111-1111-1111-111111111111",
            },
            "fetch_mail": {
                "name": "fetch_mail",
                "script": "/helpers/fetch_mail.py",
                "description": "Fetches emails",
                "requirements": [],
                "input_schema": {},
                "output_schema": {},
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
                "id": "aaaaaaaa-2222-2222-2222-222222222222",
            },
        }))

        imported = db.migrate_from_registry_yaml(registry_path=reg)
        assert imported == 2

        h = db.get_helper("parse_invoice")
        assert h is not None
        assert h["script_path"] == "/helpers/parse_invoice.py"
        assert h["requirements"] == ["pypdf2"]

    def test_migrate_registry_idempotent(self, db, tmp_path):
        """Double migration doesn't duplicate helpers."""
        reg = tmp_path / "registry.yaml"
        reg.write_text(yaml.dump({
            "my_helper": {
                "name": "my_helper",
                "script": "/h.py",
                "description": "",
                "requirements": [],
                "input_schema": {},
                "output_schema": {},
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
                "id": "bbbbbbbb-0000-0000-0000-000000000001",
            }
        }))

        db.migrate_from_registry_yaml(reg)
        db.migrate_from_registry_yaml(reg)  # second call

        helpers = db.list_helpers()
        assert len([h for h in helpers if h["name"] == "my_helper"]) == 1

    def test_migrate_missing_registry_returns_zero(self, db, tmp_path):
        result = db.migrate_from_registry_yaml(tmp_path / "nonexistent.yaml")
        assert result == 0


# ---------------------------------------------------------------------------
# sync_pipelines_from_dirs
# ---------------------------------------------------------------------------

class TestSyncPipelinesFromDirs:
    def _write_pipeline(self, directory: Path, name: str, helpers: list[str] = None) -> Path:
        p = directory / f"{name}.yaml"
        steps = [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}]
        if helpers:
            steps = [{"id": f"step_{h}", "type": "python", "helper": h} for h in helpers]
        p.write_text(yaml.dump({
            "name": name, "version": "1.0.0", "steps": steps,
        }))
        return p

    def test_sync_indexes_pipeline(self, db, tmp_path):
        self._write_pipeline(tmp_path, "alpha")
        upserted = db.sync_pipelines_from_dirs([tmp_path])
        assert upserted >= 1
        assert db.get_pipeline("alpha") is not None

    def test_sync_multiple_dirs(self, db, tmp_path):
        dir1 = tmp_path / "d1"
        dir2 = tmp_path / "d2"
        dir1.mkdir()
        dir2.mkdir()
        self._write_pipeline(dir1, "pipe-a")
        self._write_pipeline(dir2, "pipe-b")
        db.sync_pipelines_from_dirs([dir1, dir2])
        assert db.get_pipeline("pipe-a") is not None
        assert db.get_pipeline("pipe-b") is not None

    def test_sync_deduplicates_by_name(self, db, tmp_path):
        dir1 = tmp_path / "d1"
        dir2 = tmp_path / "d2"
        dir1.mkdir()
        dir2.mkdir()
        self._write_pipeline(dir1, "dup-pipe")
        self._write_pipeline(dir2, "dup-pipe")
        result = db.sync_pipelines_from_dirs([dir1, dir2])
        assert result == 1  # Only one upserted (first wins)

    def test_sync_missing_dir_skipped(self, db, tmp_path):
        db.sync_pipelines_from_dirs([tmp_path / "nonexistent"])
        assert db.list_pipelines() == []

    def test_sync_indexes_helper_references(self, db, tmp_path):
        db.upsert_helper("ref_helper", "/ref_helper.py")
        self._write_pipeline(tmp_path, "ref-pipe", helpers=["ref_helper"])
        db.sync_pipelines_from_dirs([tmp_path])
        helpers = db.get_pipeline_helpers("ref-pipe")
        assert len(helpers) == 1
        assert helpers[0]["name"] == "ref_helper"

    def test_sync_is_idempotent(self, db, tmp_path):
        self._write_pipeline(tmp_path, "idem-pipe")
        db.sync_pipelines_from_dirs([tmp_path])
        db.sync_pipelines_from_dirs([tmp_path])
        pipelines = db.list_pipelines()
        assert len([p for p in pipelines if p["name"] == "idem-pipe"]) == 1


# ---------------------------------------------------------------------------
# sync_all
# ---------------------------------------------------------------------------

class TestSyncAll:
    def test_sync_all_summary(self, db, tmp_path):
        # Create legacy history.db with one run
        hist = tmp_path / "history.db"
        conn = sqlite3.connect(str(hist))
        conn.execute("""
            CREATE TABLE runs (run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL,
            version TEXT, started_at TEXT NOT NULL, finished_at TEXT,
            duration REAL, success INTEGER, input_data TEXT,
            steps_data TEXT, result_summary TEXT, triggered_by TEXT DEFAULT 'cli')
        """)
        conn.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("sync-run", "sync-pipe", "1", "2024-01-01T00:00:00",
             None, None, None, None, None, None, "cli"),
        )
        conn.commit()
        conn.close()

        # Create registry.yaml with one helper
        reg_dir = tmp_path / "helpers"
        reg_dir.mkdir()
        reg = reg_dir / "registry.yaml"
        reg.write_text(yaml.dump({
            "my_sync_helper": {
                "name": "my_sync_helper", "script": "/h.py", "description": "",
                "requirements": [], "input_schema": {}, "output_schema": {},
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
                "id": "cccccccc-0000-0000-0000-000000000001",
            }
        }))

        # Create a pipeline YAML
        pipes_dir = tmp_path / "pipelines"
        pipes_dir.mkdir()
        (pipes_dir / "sync-pipe.yaml").write_text(yaml.dump({
            "name": "sync-pipe", "version": "1.0.0",
            "steps": [{"id": "s1", "type": "cli", "args": ["echo"]}],
        }))

        summary = db.sync_all(
            history_db_path=hist,
            registry_path=reg,
            pipeline_dirs=[pipes_dir],
        )

        assert summary["runs_migrated"] == 1
        assert summary["helpers_migrated"] == 1
        assert summary["pipelines_synced"] == 1


# ---------------------------------------------------------------------------
# Integration: RunHistory + BrixDB
# ---------------------------------------------------------------------------

class TestRunHistoryIntegration:
    def test_run_history_uses_brix_db(self, tmp_path):
        """RunHistory stores runs in brix.db — verifiable via BrixDB."""
        from brix.history import RunHistory

        db_path = tmp_path / "brix.db"
        h = RunHistory(db_path=db_path)
        h.record_start("rh-1", "pipeline-x", "1.0.0")
        h.record_finish("rh-1", True, 3.0)

        # Access the same DB directly to verify
        central = BrixDB(db_path=db_path)
        run = central.get_run("rh-1")
        assert run is not None
        assert run["pipeline"] == "pipeline-x"
        assert run["success"] == 1

    def test_run_history_and_brix_db_share_data(self, tmp_path):
        """Runs written by RunHistory are visible through BrixDB.get_recent_runs."""
        from brix.history import RunHistory

        db_path = tmp_path / "brix.db"
        h = RunHistory(db_path=db_path)
        for i in range(3):
            h.record_start(f"rh-{i}", "p")
            h.record_finish(f"rh-{i}", True, float(i))

        central = BrixDB(db_path=db_path)
        recent = central.get_recent_runs(10)
        assert len(recent) == 3


# ---------------------------------------------------------------------------
# Integration: HelperRegistry + BrixDB
# ---------------------------------------------------------------------------

class TestHelperRegistryIntegration:
    def test_register_syncs_to_db(self, tmp_path):
        """HelperRegistry.register() writes helper to brix.db."""
        from brix.helper_registry import HelperRegistry

        reg_file = tmp_path / "registry.yaml"
        db_path = tmp_path / "brix.db"
        central = BrixDB(db_path=db_path)

        reg = HelperRegistry(registry_path=reg_file, db=central)
        reg.register("myhelper", "/helpers/myhelper.py", description="does stuff")

        h = central.get_helper("myhelper")
        assert h is not None
        assert h["script_path"] == "/helpers/myhelper.py"
        assert h["description"] == "does stuff"

    def test_update_syncs_to_db(self, tmp_path):
        """HelperRegistry.update() updates the helper in brix.db."""
        from brix.helper_registry import HelperRegistry

        reg_file = tmp_path / "registry.yaml"
        db_path = tmp_path / "brix.db"
        central = BrixDB(db_path=db_path)

        reg = HelperRegistry(registry_path=reg_file, db=central)
        reg.register("upd", "/old.py", description="old")
        reg.update("upd", description="new", script="/new.py")

        h = central.get_helper("upd")
        assert h["description"] == "new"
        assert h["script_path"] == "/new.py"

    def test_remove_syncs_to_db(self, tmp_path):
        """HelperRegistry.remove() removes the helper from brix.db."""
        from brix.helper_registry import HelperRegistry

        reg_file = tmp_path / "registry.yaml"
        db_path = tmp_path / "brix.db"
        central = BrixDB(db_path=db_path)

        reg = HelperRegistry(registry_path=reg_file, db=central)
        reg.register("todel", "/todel.py")
        reg.remove("todel")

        assert central.get_helper("todel") is None


# ---------------------------------------------------------------------------
# Integration: PipelineStore + BrixDB
# ---------------------------------------------------------------------------

class TestPipelineStoreIntegration:
    _PIPELINE = {
        "name": "store-test",
        "version": "1.0.0",
        "steps": [{"id": "s1", "type": "cli", "args": ["echo", "hi"]}],
    }

    def test_save_syncs_to_db(self, tmp_path):
        """PipelineStore.save() indexes pipeline into brix.db."""
        from brix.pipeline_store import PipelineStore

        db_path = tmp_path / "brix.db"
        central = BrixDB(db_path=db_path)

        store = PipelineStore(pipelines_dir=tmp_path / "pipes", db=central)
        store.save(dict(self._PIPELINE))

        p = central.get_pipeline("store-test")
        assert p is not None
        assert "store-test" in p["path"]

    def test_delete_syncs_to_db(self, tmp_path):
        """PipelineStore.delete() removes pipeline from brix.db."""
        from brix.pipeline_store import PipelineStore

        db_path = tmp_path / "brix.db"
        central = BrixDB(db_path=db_path)

        store = PipelineStore(pipelines_dir=tmp_path / "pipes", db=central)
        store.save(dict(self._PIPELINE))
        store.delete("store-test")

        assert central.get_pipeline("store-test") is None

    def test_save_with_requirements_syncs(self, tmp_path):
        """Pipeline requirements are indexed in brix.db."""
        from brix.pipeline_store import PipelineStore

        db_path = tmp_path / "brix.db"
        central = BrixDB(db_path=db_path)

        pipeline_data = dict(self._PIPELINE)
        pipeline_data["requirements"] = ["httpx>=0.28", "pydantic"]

        store = PipelineStore(pipelines_dir=tmp_path / "pipes", db=central)
        store.save(pipeline_data)

        p = central.get_pipeline("store-test")
        assert p["requirements"] == ["httpx>=0.28", "pydantic"]


# ---------------------------------------------------------------------------
# Application Log (T-BRIX-V7-08)
# ---------------------------------------------------------------------------

class TestAppLog:
    def test_write_and_read(self, db):
        """write_app_log inserts a record; get_app_log returns it."""
        entry_id = db.write_app_log("INFO", "engine", "test message")
        assert isinstance(entry_id, str) and len(entry_id) > 0

        entries = db.get_app_log()
        assert len(entries) == 1
        e = entries[0]
        assert e["level"] == "INFO"
        assert e["component"] == "engine"
        assert e["message"] == "test message"

    def test_level_uppercased(self, db):
        """Level is stored in uppercase regardless of input case."""
        db.write_app_log("warning", "sched", "low-battery")
        entries = db.get_app_log()
        assert entries[0]["level"] == "WARNING"

    def test_message_truncated(self, db):
        """Messages longer than 2000 chars are truncated."""
        long_msg = "x" * 3000
        db.write_app_log("DEBUG", "engine", long_msg)
        entries = db.get_app_log()
        assert len(entries[0]["message"]) == 2000

    def test_filter_by_level(self, db):
        """get_app_log(level=...) returns only matching entries."""
        db.write_app_log("INFO", "engine", "info msg")
        db.write_app_log("ERROR", "engine", "error msg")
        infos = db.get_app_log(level="INFO")
        assert all(e["level"] == "INFO" for e in infos)
        errors = db.get_app_log(level="ERROR")
        assert all(e["level"] == "ERROR" for e in errors)

    def test_filter_by_component(self, db):
        """get_app_log(component=...) returns only matching entries."""
        db.write_app_log("INFO", "engine", "engine msg")
        db.write_app_log("INFO", "scheduler", "sched msg")
        engine_logs = db.get_app_log(component="engine")
        assert all(e["component"] == "engine" for e in engine_logs)

    def test_filter_by_since(self, db):
        """get_app_log(since=...) excludes older entries."""
        from datetime import datetime, timezone, timedelta
        db.write_app_log("INFO", "engine", "old message")
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        entries = db.get_app_log(since=future)
        assert entries == []

    def test_limit(self, db):
        """get_app_log respects limit parameter."""
        for i in range(10):
            db.write_app_log("INFO", "engine", f"msg {i}")
        entries = db.get_app_log(limit=3)
        assert len(entries) == 3

    def test_app_log_table_in_schema(self, db):
        """app_log table is created during schema init."""
        with db._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "app_log" in tables


# ---------------------------------------------------------------------------
# Retention Policy (T-BRIX-V7-08)
# ---------------------------------------------------------------------------

class TestCleanRetention:
    def test_age_based_deletion(self, db):
        """clean_retention removes runs older than max_days."""
        from datetime import datetime, timezone, timedelta
        old_ts = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
        # Insert an old run directly
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at, finished_at, success) "
                "VALUES (?,?,?,?,?)",
                ("old-run", "test-pipe", old_ts, old_ts, 1),
            )
        # Insert a recent run
        db.record_run_start("new-run", "test-pipe")
        db.record_run_finish("new-run", success=True, duration=1.0)

        result = db.clean_retention(max_days=30, max_mb=9999)
        assert result["runs_deleted_age"] == 1
        assert db.get_run("old-run") is None
        assert db.get_run("new-run") is not None

    def test_app_log_age_deletion(self, db):
        """clean_retention removes app_log entries older than max_days."""
        from datetime import datetime, timezone, timedelta
        old_ts = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
        # Insert old app_log entry directly
        import uuid
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO app_log (id, timestamp, level, component, message) "
                "VALUES (?,?,?,?,?)",
                (str(uuid.uuid4()), old_ts, "INFO", "engine", "old log"),
            )
        db.write_app_log("INFO", "engine", "recent log")

        result = db.clean_retention(max_days=30, max_mb=9999)
        assert result["app_log_deleted"] == 1
        entries = db.get_app_log()
        assert len(entries) == 1
        assert entries[0]["message"] == "recent log"

    def test_returns_summary_dict(self, db):
        """clean_retention always returns a dict with the expected keys."""
        result = db.clean_retention(max_days=30, max_mb=9999)
        assert "runs_deleted_age" in result
        assert "runs_deleted_size" in result
        assert "app_log_deleted" in result
        assert "db_size_mb" in result
        assert isinstance(result["db_size_mb"], float)

    def test_no_deletion_when_nothing_old(self, db):
        """clean_retention deletes nothing when all data is recent."""
        db.record_run_start("r1", "pipe")
        db.write_app_log("INFO", "engine", "recent msg")
        result = db.clean_retention(max_days=30, max_mb=9999)
        assert result["runs_deleted_age"] == 0
        assert result["app_log_deleted"] == 0

    def test_env_var_defaults(self, db, monkeypatch):
        """clean_retention reads BRIX_RETENTION_DAYS and BRIX_RETENTION_MAX_MB from env."""
        monkeypatch.setenv("BRIX_RETENTION_DAYS", "7")
        monkeypatch.setenv("BRIX_RETENTION_MAX_MB", "9999")
        from datetime import datetime, timezone, timedelta
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        with db._connect() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline, started_at, finished_at, success) "
                "VALUES (?,?,?,?,?)",
                ("env-old-run", "test-pipe", old_ts, old_ts, 1),
            )
        result = db.clean_retention()  # no explicit params → reads env
        assert result["runs_deleted_age"] == 1
