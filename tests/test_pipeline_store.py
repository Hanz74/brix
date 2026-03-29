"""Tests for PipelineStore (T-BRIX-V2-08)."""
import pytest
import yaml
from pathlib import Path

from brix.db import BrixDB
from brix.pipeline_store import PipelineStore
from brix.models import Pipeline


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def isolated_db(tmp_path):
    """Return a BrixDB backed by a temporary database file."""
    return BrixDB(db_path=tmp_path / "test.db")


@pytest.fixture
def store(tmp_path, isolated_db):
    """Return a PipelineStore with isolated DB and filesystem."""
    return PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_PIPELINE = {
    "name": "test-pipeline",
    "version": "1.0.0",
    "description": "A test pipeline",
    "steps": [{"id": "step1", "type": "cli", "args": ["echo", "hello"]}],
}

PIPELINE_WITH_INPUT = {
    "name": "input-pipeline",
    "version": "1.2.0",
    "description": "Pipeline with input params",
    "input": {
        "query": {"type": "string", "description": "Search query"},
        "limit": {"type": "integer", "default": 10, "description": "Max results"},
    },
    "steps": [{"id": "run", "type": "cli", "args": ["echo", "{{ input.query }}"]}],
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPipelineStoreSaveLoad:
    """Save and load pipelines."""

    def test_save_and_load(self, store):
        """Save a pipeline and load it back as a Pipeline model."""
        path = store.save(MINIMAL_PIPELINE)
        assert path.exists()
        assert path.suffix == ".yaml"

        pipeline = store.load("test-pipeline")
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "test-pipeline"
        assert pipeline.version == "1.0.0"
        assert len(pipeline.steps) == 1

    def test_save_uses_pipeline_name_from_dict(self, store):
        """Save without explicit name uses pipeline_data['name']."""
        path = store.save(MINIMAL_PIPELINE)
        assert path.name == "test-pipeline.yaml"

    def test_save_with_explicit_name(self, store):
        """Save with explicit name overrides dict name."""
        path = store.save(MINIMAL_PIPELINE, name="custom-name")
        assert path.name == "custom-name.yaml"
        assert path.exists()

    def test_save_returns_path(self, store):
        """save() returns a Path object."""
        path = store.save(MINIMAL_PIPELINE)
        assert isinstance(path, Path)

    def test_load_yaml_extension(self, store):
        """load() finds .yaml files."""
        store.save(MINIMAL_PIPELINE, name="yaml-ext")
        pipeline = store.load("yaml-ext")
        assert pipeline.name == "test-pipeline"  # name from dict content

    def test_load_yml_extension(self, tmp_path, isolated_db):
        """load() finds a pipeline stored in DB (DB-only path, .yml extension no longer needed)."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        # Store via save() which persists to DB
        data = dict(MINIMAL_PIPELINE)
        data["name"] = "yml-ext"
        s.save(data)
        pipeline = s.load("yml-ext")
        assert pipeline.name == "yml-ext"


class TestPipelineStoreLoadRaw:
    """load_raw returns raw dict."""

    def test_save_and_load_raw(self, store):
        """load_raw returns a dict, not a Pipeline model."""
        store.save(MINIMAL_PIPELINE)
        raw = store.load_raw("test-pipeline")
        assert isinstance(raw, dict)
        assert raw["name"] == "test-pipeline"
        assert raw["version"] == "1.0.0"
        assert isinstance(raw["steps"], list)

    def test_load_raw_preserves_input_section(self, store):
        """load_raw preserves the input schema section."""
        store.save(PIPELINE_WITH_INPUT)
        raw = store.load_raw("input-pipeline")
        assert "input" in raw
        assert "query" in raw["input"]
        assert "limit" in raw["input"]

    def test_load_raw_not_found(self, store):
        """load_raw raises FileNotFoundError for missing pipeline."""
        with pytest.raises(FileNotFoundError):
            store.load_raw("nonexistent-pipeline")


class TestPipelineStoreListAll:
    """list_all() returns metadata for all pipelines."""

    def test_list_all_empty(self, store):
        """Empty store returns empty list."""
        result = store.list_all()
        assert result == []

    def test_list_all(self, store):
        """list_all returns metadata for each saved pipeline."""
        store.save(MINIMAL_PIPELINE)
        store.save(PIPELINE_WITH_INPUT)

        results = store.list_all()
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert "test-pipeline" in names
        assert "input-pipeline" in names

    def test_list_all_metadata_fields(self, store):
        """Each list entry has name, version, description, steps, path."""
        store.save(MINIMAL_PIPELINE)

        results = store.list_all()
        assert len(results) == 1
        entry = results[0]
        assert "name" in entry
        assert "version" in entry
        assert "description" in entry
        assert "steps" in entry
        assert "path" in entry

    def test_list_all_step_count(self, store):
        """list_all reports correct step count."""
        store.save(MINIMAL_PIPELINE)  # 1 step

        result = store.list_all()
        pipeline_entry = next(r for r in result if r["name"] == "test-pipeline")
        assert pipeline_entry["steps"] == 1

    def test_list_all_broken_pipeline_still_listed(self, tmp_path, isolated_db):
        """A broken YAML stored in DB still appears in list_all with error in description."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        # Store invalid YAML directly in DB
        isolated_db.upsert_pipeline(
            name="broken",
            path="",
            requirements=[],
            yaml_content="name: broken\nsteps: not-a-list\n",
        )

        results = s.list_all()
        assert len(results) == 1
        assert results[0]["name"] == "broken"
        assert "Error" in results[0]["description"]


class TestPipelineStoreExists:
    """exists() checks for pipeline files."""

    def test_exists_true(self, store):
        """exists() returns True when pipeline file exists."""
        store.save(MINIMAL_PIPELINE)
        assert store.exists("test-pipeline") is True

    def test_exists_false(self, store):
        """exists() returns False when pipeline file doesn't exist."""
        assert store.exists("nonexistent") is False

    def test_exists_yml(self, tmp_path, isolated_db):
        """exists() finds .yml files too."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        (tmp_path / "mypipe.yml").write_text(yaml.dump(MINIMAL_PIPELINE))
        assert s.exists("mypipe") is True


class TestPipelineStoreDelete:
    """delete() removes pipeline files."""

    def test_delete(self, store):
        """delete() removes the pipeline file and returns True."""
        store.save(MINIMAL_PIPELINE)
        assert store.exists("test-pipeline") is True

        result = store.delete("test-pipeline")
        assert result is True
        assert store.exists("test-pipeline") is False

    def test_delete_returns_false_for_missing(self, store):
        """delete() returns False when pipeline doesn't exist."""
        result = store.delete("nonexistent-pipeline")
        assert result is False

    def test_delete_yml(self, tmp_path, isolated_db):
        """delete() also removes .yml files."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        (tmp_path / "mypipe.yml").write_text(yaml.dump(MINIMAL_PIPELINE))
        result = s.delete("mypipe")
        assert result is True
        assert not (tmp_path / "mypipe.yml").exists()


class TestPipelineStoreLoadNotFound:
    """load() raises FileNotFoundError for missing pipelines."""

    def test_load_not_found(self, store):
        """load() raises FileNotFoundError for unknown pipeline name."""
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            store.load("nonexistent-pipeline")


class TestPipelineStoreTimestamps:
    """Save manages created_at and updated_at."""

    def test_save_sets_created_and_updated(self, store):
        """First save sets both created_at and updated_at."""
        store.save(MINIMAL_PIPELINE)
        raw = store.load_raw("test-pipeline")
        assert "created_at" in raw
        assert "updated_at" in raw

    def test_save_preserves_created_at(self, store):
        """Second save keeps created_at from first save, updates updated_at."""
        store.save(MINIMAL_PIPELINE)
        raw1 = store.load_raw("test-pipeline")
        created1 = raw1["created_at"]

        store.save(dict(MINIMAL_PIPELINE, version="2.0.0"))
        raw2 = store.load_raw("test-pipeline")
        assert raw2["created_at"] == created1
        assert raw2["updated_at"] >= raw1["updated_at"]

    def test_save_respects_explicit_created_at(self, store):
        """If pipeline_data already has created_at, it is preserved."""
        data = dict(MINIMAL_PIPELINE, created_at="2020-01-01T00:00:00Z")
        store.save(data)
        raw = store.load_raw("test-pipeline")
        assert raw["created_at"] == "2020-01-01T00:00:00Z"


class TestPipelineStoreVersion:
    """get_version returns the pipeline version string."""

    def test_get_version(self, store):
        store.save(MINIMAL_PIPELINE)
        assert store.get_version("test-pipeline") == "1.0.0"


class TestPipelineStoreResolve:
    """resolve() finds by name or UUID."""

    def test_resolve_by_name(self, store):
        store.save(MINIMAL_PIPELINE)
        assert store.resolve("test-pipeline") == "test-pipeline"

    def test_resolve_not_found(self, store):
        with pytest.raises(FileNotFoundError):
            store.resolve("nonexistent-pipeline-xyz")

    def test_resolve_by_id(self, store):
        data = dict(MINIMAL_PIPELINE, id="my-uuid-1234")
        store.save(data)
        assert store.resolve("my-uuid-1234") == "test-pipeline"


class TestPipelineStoreDBFirst:
    """DB-first specific tests."""

    def test_save_stores_yaml_content_in_db(self, store, isolated_db):
        """save() writes yaml_content to DB."""
        store.save(MINIMAL_PIPELINE)
        content = isolated_db.get_pipeline_yaml_content("test-pipeline")
        assert content is not None
        assert "test-pipeline" in content

    def test_load_from_db_without_file(self, tmp_path, isolated_db):
        """load() reads from DB even if file doesn't exist."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        s.save(MINIMAL_PIPELINE)
        # Remove the file
        (tmp_path / "test-pipeline.yaml").unlink()
        # Should still load from DB
        pipeline = s.load("test-pipeline")
        assert pipeline.name == "test-pipeline"

    def test_load_raw_from_db_without_file(self, tmp_path, isolated_db):
        """load_raw() reads from DB even if file doesn't exist."""
        s = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path], db=isolated_db)
        s.save(MINIMAL_PIPELINE)
        (tmp_path / "test-pipeline.yaml").unlink()
        raw = s.load_raw("test-pipeline")
        assert raw["name"] == "test-pipeline"
