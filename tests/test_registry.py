"""Tests for BrixRegistry (T-BRIX-23)."""
import yaml
import pytest
from pathlib import Path

from brix.registry import BrixRegistry


# ---------------------------------------------------------------------------
# Pipeline listing
# ---------------------------------------------------------------------------

def test_list_pipelines_empty(tmp_path):
    """Returns empty list when pipelines dir does not exist."""
    reg = BrixRegistry(pipelines_dir=tmp_path / "nonexistent", bricks_dir=tmp_path / "bricks")
    assert reg.list_pipelines() == []


def test_list_pipelines_empty_dir(tmp_path):
    """Returns empty list when pipelines dir exists but has no YAML files."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    assert reg.list_pipelines() == []


def test_list_pipelines_with_files(tmp_path):
    """Returns metadata for valid pipeline YAML files."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    pipeline_data = {
        "name": "my-pipeline",
        "version": "1.0.0",
        "description": "A test pipeline",
        "steps": [
            {"id": "step1", "type": "python", "script": "run.py"},
            {"id": "step2", "type": "python", "script": "run2.py"},
        ],
    }
    (pipelines_dir / "my-pipeline.yaml").write_text(yaml.dump(pipeline_data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    pipelines = reg.list_pipelines()

    assert len(pipelines) == 1
    p = pipelines[0]
    assert p["name"] == "my-pipeline"
    assert p["version"] == "1.0.0"
    assert p["description"] == "A test pipeline"
    assert p["steps"] == 2
    assert "my-pipeline.yaml" in p["path"]


def test_list_pipelines_multiple_files(tmp_path):
    """Lists multiple pipeline files sorted alphabetically."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    for name in ["alpha", "beta", "gamma"]:
        data = {
            "name": name,
            "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
        }
        (pipelines_dir / f"{name}.yaml").write_text(yaml.dump(data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    pipelines = reg.list_pipelines()
    assert len(pipelines) == 3
    names = [p["name"] for p in pipelines]
    assert "alpha" in names
    assert "beta" in names
    assert "gamma" in names


def test_list_pipelines_invalid_yaml(tmp_path):
    """Falls back gracefully for invalid pipeline files."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    (pipelines_dir / "broken.yaml").write_text("not: valid: pipeline: yaml: [")

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    pipelines = reg.list_pipelines()
    assert len(pipelines) == 1
    assert pipelines[0]["name"] == "broken"
    assert pipelines[0]["description"] == "Error loading pipeline"
    assert pipelines[0]["steps"] == 0


def test_list_pipelines_yml_extension(tmp_path):
    """Picks up .yml files as well as .yaml."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    data = {
        "name": "yml-pipeline",
        "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
    }
    (pipelines_dir / "yml-pipeline.yml").write_text(yaml.dump(data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    pipelines = reg.list_pipelines()
    assert len(pipelines) == 1
    assert pipelines[0]["name"] == "yml-pipeline"


# ---------------------------------------------------------------------------
# Brick listing
# ---------------------------------------------------------------------------

def test_list_bricks_empty(tmp_path):
    """Returns empty list when bricks dir does not exist."""
    reg = BrixRegistry(pipelines_dir=tmp_path / "pipelines", bricks_dir=tmp_path / "nonexistent")
    assert reg.list_bricks() == []


def test_list_bricks_empty_dir(tmp_path):
    """Returns empty list when bricks dir exists but has no YAML files."""
    bricks_dir = tmp_path / "bricks"
    bricks_dir.mkdir()
    reg = BrixRegistry(pipelines_dir=tmp_path / "pipelines", bricks_dir=bricks_dir)
    assert reg.list_bricks() == []


def test_list_bricks_with_files(tmp_path):
    """Returns metadata for valid brick YAML files."""
    bricks_dir = tmp_path / "bricks"
    bricks_dir.mkdir()

    brick_data = {
        "name": "http-fetch",
        "type": "http",
        "description": "Fetches a URL",
        "version": "0.2.0",
        "tested": True,
    }
    (bricks_dir / "http-fetch.yaml").write_text(yaml.dump(brick_data))

    reg = BrixRegistry(pipelines_dir=tmp_path / "pipelines", bricks_dir=bricks_dir)
    bricks = reg.list_bricks()

    assert len(bricks) == 1
    b = bricks[0]
    assert b["name"] == "http-fetch"
    assert b["type"] == "http"
    assert b["description"] == "Fetches a URL"
    assert b["version"] == "0.2.0"
    assert b["tested"] is True


def test_list_bricks_defaults_for_missing_fields(tmp_path):
    """Uses sensible defaults when optional brick fields are absent."""
    bricks_dir = tmp_path / "bricks"
    bricks_dir.mkdir()
    (bricks_dir / "minimal.yaml").write_text(yaml.dump({}))

    reg = BrixRegistry(pipelines_dir=tmp_path / "pipelines", bricks_dir=bricks_dir)
    bricks = reg.list_bricks()

    assert len(bricks) == 1
    b = bricks[0]
    assert b["name"] == "minimal"  # falls back to stem
    assert b["type"] == "?"
    assert b["tested"] is False


def test_list_bricks_invalid_yaml(tmp_path):
    """Falls back gracefully for unreadable brick files."""
    bricks_dir = tmp_path / "bricks"
    bricks_dir.mkdir()
    (bricks_dir / "broken.yaml").write_text(": !!invalid")

    reg = BrixRegistry(pipelines_dir=tmp_path / "pipelines", bricks_dir=bricks_dir)
    bricks = reg.list_bricks()
    assert len(bricks) == 1
    assert bricks[0]["name"] == "broken"
    assert bricks[0]["description"] == "Error"


# ---------------------------------------------------------------------------
# get_pipeline_info
# ---------------------------------------------------------------------------

def test_get_pipeline_info(tmp_path):
    """Returns detailed info dict for a named pipeline."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    pipeline_data = {
        "name": "info-pipeline",
        "version": "2.0.0",
        "description": "Full info test",
        "input": {
            "query": {"type": "string", "description": "Search query"},
        },
        "credentials": {"token": {"env": "API_TOKEN"}},
        "steps": [
            {"id": "fetch", "type": "mcp", "server": "m365", "tool": "list-mail"},
            {"id": "process", "type": "python", "script": "process.py"},
        ],
    }
    (pipelines_dir / "info-pipeline.yaml").write_text(yaml.dump(pipeline_data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    info = reg.get_pipeline_info("info-pipeline")

    assert info is not None
    assert info["name"] == "info-pipeline"
    assert info["version"] == "2.0.0"
    assert info["description"] == "Full info test"
    assert "query" in info["input"]
    assert info["input"]["query"]["type"] == "string"
    assert "token" in info["credentials"]
    assert len(info["steps"]) == 2
    assert {"id": "fetch", "type": "mcp"} in info["steps"]
    assert "m365" in info["mcp_servers"]
    assert "info-pipeline.yaml" in info["path"]


def test_get_pipeline_info_by_stem(tmp_path):
    """Looks up pipeline by file stem when name differs."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    pipeline_data = {
        "name": "actual-name",
        "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
    }
    (pipelines_dir / "file-stem.yaml").write_text(yaml.dump(pipeline_data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    # Lookup by file stem
    info = reg.get_pipeline_info("file-stem")
    assert info is not None
    assert info["name"] == "actual-name"


def test_get_pipeline_info_not_found(tmp_path):
    """Returns None when pipeline name does not match any file."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    pipeline_data = {
        "name": "other-pipeline",
        "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
    }
    (pipelines_dir / "other.yaml").write_text(yaml.dump(pipeline_data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    assert reg.get_pipeline_info("nonexistent") is None


def test_get_pipeline_info_no_pipelines_dir(tmp_path):
    """Returns None when pipelines directory does not exist."""
    reg = BrixRegistry(pipelines_dir=tmp_path / "nonexistent", bricks_dir=tmp_path / "bricks")
    assert reg.get_pipeline_info("anything") is None


def test_get_pipeline_info_no_mcp_servers(tmp_path):
    """mcp_servers is empty when no MCP steps present."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    pipeline_data = {
        "name": "no-mcp",
        "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
    }
    (pipelines_dir / "no-mcp.yaml").write_text(yaml.dump(pipeline_data))

    reg = BrixRegistry(pipelines_dir=pipelines_dir, bricks_dir=tmp_path / "bricks")
    info = reg.get_pipeline_info("no-mcp")
    assert info is not None
    assert info["mcp_servers"] == []
