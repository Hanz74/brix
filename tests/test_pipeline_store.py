"""Tests for PipelineStore (T-BRIX-V2-08)."""
import pytest
import yaml
from pathlib import Path

from brix.pipeline_store import PipelineStore
from brix.models import Pipeline


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

    def test_save_and_load(self, tmp_path):
        """Save a pipeline and load it back as a Pipeline model."""
        store = PipelineStore(pipelines_dir=tmp_path)
        path = store.save(MINIMAL_PIPELINE)
        assert path.exists()
        assert path.suffix == ".yaml"

        pipeline = store.load("test-pipeline")
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "test-pipeline"
        assert pipeline.version == "1.0.0"
        assert len(pipeline.steps) == 1

    def test_save_uses_pipeline_name_from_dict(self, tmp_path):
        """Save without explicit name uses pipeline_data['name']."""
        store = PipelineStore(pipelines_dir=tmp_path)
        path = store.save(MINIMAL_PIPELINE)
        assert path.name == "test-pipeline.yaml"

    def test_save_with_explicit_name(self, tmp_path):
        """Save with explicit name overrides dict name."""
        store = PipelineStore(pipelines_dir=tmp_path)
        path = store.save(MINIMAL_PIPELINE, name="custom-name")
        assert path.name == "custom-name.yaml"
        assert path.exists()

    def test_save_returns_path(self, tmp_path):
        """save() returns a Path object."""
        store = PipelineStore(pipelines_dir=tmp_path)
        path = store.save(MINIMAL_PIPELINE)
        assert isinstance(path, Path)

    def test_load_yaml_extension(self, tmp_path):
        """load() finds .yaml files."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(MINIMAL_PIPELINE, name="yaml-ext")
        pipeline = store.load("yaml-ext")
        assert pipeline.name == "test-pipeline"  # name from dict content

    def test_load_yml_extension(self, tmp_path):
        """load() also finds .yml files."""
        store = PipelineStore(pipelines_dir=tmp_path)
        yml_path = tmp_path / "yml-ext.yml"
        yml_path.write_text(yaml.dump(MINIMAL_PIPELINE))
        pipeline = store.load("yml-ext")
        assert pipeline.name == "test-pipeline"


class TestPipelineStoreLoadRaw:
    """load_raw returns raw dict."""

    def test_save_and_load_raw(self, tmp_path):
        """load_raw returns a dict, not a Pipeline model."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(MINIMAL_PIPELINE)
        raw = store.load_raw("test-pipeline")
        assert isinstance(raw, dict)
        assert raw["name"] == "test-pipeline"
        assert raw["version"] == "1.0.0"
        assert isinstance(raw["steps"], list)

    def test_load_raw_preserves_input_section(self, tmp_path):
        """load_raw preserves the input schema section."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(PIPELINE_WITH_INPUT)
        raw = store.load_raw("input-pipeline")
        assert "input" in raw
        assert "query" in raw["input"]
        assert "limit" in raw["input"]

    def test_load_raw_not_found(self, tmp_path):
        """load_raw raises FileNotFoundError for missing pipeline."""
        store = PipelineStore(pipelines_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            store.load_raw("nonexistent-pipeline")


class TestPipelineStoreListAll:
    """list_all() returns metadata for all pipelines."""

    def test_list_all_empty(self, tmp_path):
        """Empty store returns empty list."""
        # Use search_paths=[tmp_path] to isolate from ~/.brix/pipelines
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        result = store.list_all()
        assert result == []

    def test_list_all(self, tmp_path):
        """list_all returns metadata for each saved pipeline."""
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        store.save(MINIMAL_PIPELINE)
        store.save(PIPELINE_WITH_INPUT)

        results = store.list_all()
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert "test-pipeline" in names
        assert "input-pipeline" in names

    def test_list_all_metadata_fields(self, tmp_path):
        """Each list entry has name, version, description, steps, path."""
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        store.save(MINIMAL_PIPELINE)

        results = store.list_all()
        assert len(results) == 1
        entry = results[0]
        assert "name" in entry
        assert "version" in entry
        assert "description" in entry
        assert "steps" in entry
        assert "path" in entry

    def test_list_all_step_count(self, tmp_path):
        """list_all reports correct step count."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(MINIMAL_PIPELINE)  # 1 step

        result = store.list_all()
        pipeline_entry = next(r for r in result if r["name"] == "test-pipeline")
        assert pipeline_entry["steps"] == 1

    def test_list_all_broken_pipeline_still_listed(self, tmp_path):
        """A broken YAML still appears in list_all with error in description."""
        store = PipelineStore(pipelines_dir=tmp_path, search_paths=[tmp_path])
        # Write invalid YAML
        (tmp_path / "broken.yaml").write_text("name: broken\nsteps: not-a-list\n")

        results = store.list_all()
        assert len(results) == 1
        assert results[0]["name"] == "broken"
        assert "Error" in results[0]["description"]


class TestPipelineStoreExists:
    """exists() checks for pipeline files."""

    def test_exists_true(self, tmp_path):
        """exists() returns True when pipeline file exists."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(MINIMAL_PIPELINE)
        assert store.exists("test-pipeline") is True

    def test_exists_false(self, tmp_path):
        """exists() returns False when pipeline file doesn't exist."""
        store = PipelineStore(pipelines_dir=tmp_path)
        assert store.exists("nonexistent") is False

    def test_exists_yml(self, tmp_path):
        """exists() finds .yml files too."""
        store = PipelineStore(pipelines_dir=tmp_path)
        (tmp_path / "mypipe.yml").write_text(yaml.dump(MINIMAL_PIPELINE))
        assert store.exists("mypipe") is True


class TestPipelineStoreDelete:
    """delete() removes pipeline files."""

    def test_delete(self, tmp_path):
        """delete() removes the pipeline file and returns True."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(MINIMAL_PIPELINE)
        assert store.exists("test-pipeline") is True

        result = store.delete("test-pipeline")
        assert result is True
        assert store.exists("test-pipeline") is False

    def test_delete_returns_false_for_missing(self, tmp_path):
        """delete() returns False when pipeline doesn't exist."""
        store = PipelineStore(pipelines_dir=tmp_path)
        result = store.delete("nonexistent-pipeline")
        assert result is False

    def test_delete_yml(self, tmp_path):
        """delete() also removes .yml files."""
        store = PipelineStore(pipelines_dir=tmp_path)
        (tmp_path / "mypipe.yml").write_text(yaml.dump(MINIMAL_PIPELINE))
        result = store.delete("mypipe")
        assert result is True
        assert not (tmp_path / "mypipe.yml").exists()


class TestPipelineStoreLoadNotFound:
    """load() raises FileNotFoundError for missing pipelines."""

    def test_load_not_found(self, tmp_path):
        """load() raises FileNotFoundError for unknown pipeline name."""
        store = PipelineStore(pipelines_dir=tmp_path)
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            store.load("nonexistent")

    def test_load_error_contains_name(self, tmp_path):
        """FileNotFoundError message contains the pipeline name."""
        store = PipelineStore(pipelines_dir=tmp_path)
        with pytest.raises(FileNotFoundError) as exc_info:
            store.load("my-missing-pipeline")
        assert "my-missing-pipeline" in str(exc_info.value)


class TestPipelineStoreGetVersion:
    """get_version() returns pipeline version string."""

    def test_get_version(self, tmp_path):
        """get_version returns the version from pipeline YAML."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(MINIMAL_PIPELINE)
        version = store.get_version("test-pipeline")
        assert version == "1.0.0"

    def test_get_version_custom(self, tmp_path):
        """get_version returns custom version."""
        store = PipelineStore(pipelines_dir=tmp_path)
        store.save(PIPELINE_WITH_INPUT)
        version = store.get_version("input-pipeline")
        assert version == "1.2.0"


class TestPipelineStoreAutoCreate:
    """PipelineStore auto-creates the pipelines directory."""

    def test_creates_dir_on_init(self, tmp_path):
        """PipelineStore creates pipelines_dir if it doesn't exist."""
        new_dir = tmp_path / "new" / "nested" / "dir"
        assert not new_dir.exists()
        PipelineStore(pipelines_dir=new_dir)
        assert new_dir.exists()


# ---------------------------------------------------------------------------
# Multi-path search tests (T-BRIX-V2-19)
# ---------------------------------------------------------------------------

SECOND_PIPELINE = {
    "name": "second-pipeline",
    "version": "2.0.0",
    "description": "A pipeline in the second search path",
    "steps": [{"id": "s1", "type": "cli", "args": ["echo", "second"]}],
}


class TestPipelineStoreMultiPath:
    """PipelineStore searches multiple paths."""

    def test_search_multiple_paths(self, tmp_path):
        """Pipeline in second search path is found when not in first."""
        primary = tmp_path / "primary"
        secondary = tmp_path / "secondary"
        primary.mkdir()
        secondary.mkdir()

        # Write pipeline only to secondary
        (secondary / "second-pipeline.yaml").write_text(yaml.dump(SECOND_PIPELINE))

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, secondary])
        pipeline = store.load("second-pipeline")
        assert pipeline.name == "second-pipeline"
        assert pipeline.version == "2.0.0"

    def test_search_priority(self, tmp_path):
        """Pipeline in first search path takes priority over second."""
        primary = tmp_path / "primary"
        secondary = tmp_path / "secondary"
        primary.mkdir()
        secondary.mkdir()

        # Write different versions to each path — same name
        primary_data = dict(MINIMAL_PIPELINE)
        primary_data["version"] = "1.0.0"
        (primary / "test-pipeline.yaml").write_text(yaml.dump(primary_data))

        secondary_data = dict(MINIMAL_PIPELINE)
        secondary_data["version"] = "9.9.9"
        (secondary / "test-pipeline.yaml").write_text(yaml.dump(secondary_data))

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, secondary])
        pipeline = store.load("test-pipeline")
        assert pipeline.version == "1.0.0"  # primary wins

    def test_list_all_deduplicates(self, tmp_path):
        """Same pipeline name in both paths appears only once in list_all."""
        primary = tmp_path / "primary"
        secondary = tmp_path / "secondary"
        primary.mkdir()
        secondary.mkdir()

        (primary / "test-pipeline.yaml").write_text(yaml.dump(MINIMAL_PIPELINE))
        (secondary / "test-pipeline.yaml").write_text(yaml.dump(MINIMAL_PIPELINE))

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, secondary])
        results = store.list_all()
        names = [r["name"] for r in results]
        assert names.count("test-pipeline") == 1

    def test_list_all_includes_both_paths(self, tmp_path):
        """list_all returns pipelines from all search paths."""
        primary = tmp_path / "primary"
        secondary = tmp_path / "secondary"
        primary.mkdir()
        secondary.mkdir()

        (primary / "test-pipeline.yaml").write_text(yaml.dump(MINIMAL_PIPELINE))
        (secondary / "second-pipeline.yaml").write_text(yaml.dump(SECOND_PIPELINE))

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, secondary])
        results = store.list_all()
        names = {r["name"] for r in results}
        assert "test-pipeline" in names
        assert "second-pipeline" in names

    def test_save_goes_to_primary(self, tmp_path):
        """save() always writes to pipelines_dir, not to other search paths."""
        primary = tmp_path / "primary"
        secondary = tmp_path / "secondary"
        primary.mkdir()
        secondary.mkdir()

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, secondary])
        path = store.save(MINIMAL_PIPELINE)

        assert path.parent == primary
        assert (primary / "test-pipeline.yaml").exists()
        assert not (secondary / "test-pipeline.yaml").exists()

    def test_search_paths_missing_dir_skipped(self, tmp_path):
        """list_all skips non-existent directories gracefully."""
        primary = tmp_path / "primary"
        primary.mkdir()
        nonexistent = tmp_path / "does-not-exist"

        (primary / "test-pipeline.yaml").write_text(yaml.dump(MINIMAL_PIPELINE))

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, nonexistent])
        results = store.list_all()
        assert len(results) == 1
        assert results[0]["name"] == "test-pipeline"

    def test_exists_checks_all_paths(self, tmp_path):
        """exists() returns True if pipeline is in any search path."""
        primary = tmp_path / "primary"
        secondary = tmp_path / "secondary"
        primary.mkdir()
        secondary.mkdir()

        (secondary / "second-pipeline.yaml").write_text(yaml.dump(SECOND_PIPELINE))

        store = PipelineStore(pipelines_dir=primary, search_paths=[primary, secondary])
        assert store.exists("second-pipeline") is True
        assert store.exists("nonexistent") is False
