"""Tests for Pipeline Templates (T-BRIX-V2-14)."""
import pytest

from brix.templates.catalog import get_template, list_templates, TEMPLATES


def test_list_templates() -> None:
    """5 templates should be available."""
    templates = list_templates()
    assert len(templates) == 5
    names = [t["name"] for t in templates]
    assert "http-download" in names
    assert "mcp-fetch-process" in names
    assert "batch-convert" in names
    assert "filter-export" in names
    assert "multi-source-merge" in names


def test_list_templates_has_steps_count() -> None:
    """Each list entry should include name, description, steps count."""
    for t in list_templates():
        assert "name" in t
        assert "description" in t
        assert "steps" in t
        assert isinstance(t["steps"], int)
        assert t["steps"] > 0


def test_get_template_by_keyword_download() -> None:
    """'download' keyword should match http-download."""
    tmpl = get_template("download files from api")
    assert tmpl is not None
    assert tmpl["name"] == "http-download"


def test_get_template_email() -> None:
    """'email' keyword should match mcp-fetch-process."""
    tmpl = get_template("process email attachments")
    assert tmpl is not None
    assert tmpl["name"] == "mcp-fetch-process"


def test_get_template_convert() -> None:
    """'convert pdf' should match batch-convert."""
    tmpl = get_template("convert pdf files in folder")
    assert tmpl is not None
    assert tmpl["name"] == "batch-convert"


def test_get_template_filter() -> None:
    """'filter export' should match filter-export."""
    tmpl = get_template("filter and export results")
    assert tmpl is not None
    assert tmpl["name"] == "filter-export"


def test_get_template_merge() -> None:
    """'merge' keyword should match multi-source-merge."""
    tmpl = get_template("merge data from multiple sources")
    assert tmpl is not None
    assert tmpl["name"] == "multi-source-merge"


def test_get_template_not_found() -> None:
    """Unknown goal with no matching keywords should return None."""
    tmpl = get_template("xyzzy frobnicator")
    assert tmpl is None


def test_template_has_pipeline() -> None:
    """Each template must have a valid pipeline structure."""
    for name, tmpl in TEMPLATES.items():
        pipeline = tmpl.get("pipeline")
        assert pipeline is not None, f"Template {name!r} missing 'pipeline'"
        assert "name" in pipeline, f"Template {name!r} pipeline missing 'name'"
        assert "steps" in pipeline, f"Template {name!r} pipeline missing 'steps'"
        assert isinstance(pipeline["steps"], list), f"Template {name!r} steps is not a list"
        assert len(pipeline["steps"]) > 0, f"Template {name!r} has no steps"
        for step in pipeline["steps"]:
            assert "id" in step, f"Template {name!r} has a step without 'id'"
            assert "type" in step, f"Template {name!r} step {step.get('id')!r} missing 'type'"


def test_template_customization_points() -> None:
    """Each template must have a non-empty customization_points list."""
    for name, tmpl in TEMPLATES.items():
        cp = tmpl.get("customization_points")
        assert cp is not None, f"Template {name!r} missing 'customization_points'"
        assert isinstance(cp, list), f"Template {name!r} customization_points is not a list"
        assert len(cp) > 0, f"Template {name!r} has empty customization_points"


def test_template_keywords_non_empty() -> None:
    """Each template must have at least one keyword."""
    for name, tmpl in TEMPLATES.items():
        keywords = tmpl.get("keywords", [])
        assert len(keywords) > 0, f"Template {name!r} has no keywords"


def test_get_template_mcp_keyword() -> None:
    """'mcp' keyword should match mcp-fetch-process."""
    tmpl = get_template("get data from mcp server")
    assert tmpl is not None
    assert tmpl["name"] == "mcp-fetch-process"


def test_get_template_fetch_returns_pipeline_key() -> None:
    """get_template result must contain 'pipeline' key with steps."""
    tmpl = get_template("download")
    assert tmpl is not None
    assert "pipeline" in tmpl
    assert "steps" in tmpl["pipeline"]


def test_get_template_description_non_empty() -> None:
    """All templates have a non-empty description."""
    for name, tmpl in TEMPLATES.items():
        assert tmpl.get("description"), f"Template {name!r} has empty description"
