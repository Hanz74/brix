"""Tests for T-BRIX-V8-08: Pipeline-Templates — Parametrisierte Blueprints.

Covers:
1. Template erstellen (is_template=True mit blueprint_params)
2. list_templates findet nur Templates (is_template=True)
3. instantiate_template: alle Params → korrekte Pipeline
4. instantiate_template: fehlender required Param → Fehler
5. instantiate_template: Default-Werte werden eingesetzt
6. instantiate_template: enum Param mit ungültigem Wert → Fehler
7. Instanziierte Pipeline hat is_template=False
8. Jinja2-Rendering in Step-Parametern funktioniert
9. Nicht-Template-Pipeline bei instantiate abgelehnt
10. list_templates gibt leere Liste wenn keine Templates vorhanden
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_pipelines_dir(tmp_path, monkeypatch):
    """Redirect pipeline storage to a temp directory."""
    from brix.pipeline_store import PipelineStore

    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)

    import brix.mcp_server as mcp_mod
    import brix.mcp_handlers._shared as shared_mod
    import brix.mcp_handlers.templates as templates_mod

    monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", pipelines_dir)

    def patched_pipeline_dir():
        pipelines_dir.mkdir(parents=True, exist_ok=True)
        return pipelines_dir

    monkeypatch.setattr(shared_mod, "_pipeline_dir", patched_pipeline_dir)
    monkeypatch.setattr(templates_mod, "_pipeline_dir", patched_pipeline_dir)

    # Restrict PipelineStore search paths to just this temp dir
    original_init = PipelineStore.__init__

    def patched_store_init(self, pipelines_dir=None, search_paths=None):
        original_init(self, pipelines_dir=patched_pipeline_dir(), search_paths=[str(patched_pipeline_dir())])

    monkeypatch.setattr(PipelineStore, "__init__", patched_store_init)
    return pipelines_dir


def _make_template_dict(
    name: str = "email-intake-template",
    extra_params: list[dict] | None = None,
) -> dict:
    """Build a minimal valid template pipeline dict."""
    params = [
        {
            "name": "source",
            "description": "Email source (gmail or outlook)",
            "type": "enum",
            "required": True,
            "enum_values": ["gmail", "outlook"],
        },
        {
            "name": "folder",
            "description": "Folder to scan",
            "type": "string",
            "required": False,
            "default": "INBOX",
        },
    ]
    if extra_params:
        params.extend(extra_params)

    return {
        "name": name,
        "version": "1.0.0",
        "description": "Template for {{ tpl.source }} email intake",
        "is_template": True,
        "blueprint_params": params,
        "steps": [
            {
                "id": "fetch",
                "type": "mcp",
                "server": "{{ tpl.source }}-mcp",
                "tool": "list_messages",
                "params": {
                    "folder": "{{ tpl.folder }}",
                    "source": "{{ tpl.source }}",
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# Helper: save a raw pipeline dict via _save_pipeline_yaml
# ---------------------------------------------------------------------------

def _save_raw(name: str, data: dict):
    """Save a raw pipeline dict (bypasses model validation for template fields)."""
    import yaml
    from brix.mcp_handlers._shared import _pipeline_dir
    path = _pipeline_dir() / f"{name}.yaml"
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)


# ---------------------------------------------------------------------------
# 1. Template erstellen (is_template=True)
# ---------------------------------------------------------------------------

class TestCreateTemplate:
    def test_create_template_pipeline(self, tmp_pipelines_dir):
        """A pipeline saved with is_template=True is treated as a template."""
        from brix.mcp_handlers._shared import _pipeline_dir
        data = _make_template_dict()
        _save_raw("email-intake-template", data)

        raw_path = _pipeline_dir() / "email-intake-template.yaml"
        assert raw_path.exists()

        import yaml
        with open(raw_path) as f:
            loaded = yaml.safe_load(f)
        assert loaded["is_template"] is True
        assert len(loaded["blueprint_params"]) == 2


# ---------------------------------------------------------------------------
# 2. list_templates findet nur Templates
# ---------------------------------------------------------------------------

class TestListTemplates:
    def test_only_templates_returned(self, tmp_pipelines_dir):
        """list_templates must exclude non-template pipelines."""
        # Save one template and one regular pipeline
        _save_raw("email-intake-template", _make_template_dict())
        _save_raw("regular-pipeline", {
            "name": "regular-pipeline",
            "version": "1.0.0",
            "is_template": False,
            "steps": [{"id": "s1", "type": "set", "values": {"x": 1}}],
        })

        from brix.mcp_handlers.templates import _handle_list_templates
        result = asyncio.get_event_loop().run_until_complete(
            _handle_list_templates({})
        )
        assert result["success"] is True
        names = [t["name"] for t in result["templates"]]
        assert "email-intake-template" in names
        assert "regular-pipeline" not in names

    def test_empty_list_when_no_templates(self, tmp_pipelines_dir):
        """list_templates returns empty list when no templates exist."""
        _save_raw("only-regular", {
            "name": "only-regular",
            "version": "1.0.0",
            "steps": [{"id": "s1", "type": "set", "values": {"x": 1}}],
        })

        from brix.mcp_handlers.templates import _handle_list_templates
        result = asyncio.get_event_loop().run_until_complete(
            _handle_list_templates({})
        )
        assert result["success"] is True
        assert result["total"] == 0
        assert result["templates"] == []

    def test_template_exposes_blueprint_params(self, tmp_pipelines_dir):
        """list_templates should include blueprint_params in the response."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_list_templates
        result = asyncio.get_event_loop().run_until_complete(
            _handle_list_templates({})
        )
        assert result["success"] is True
        tpl = next(t for t in result["templates"] if t["name"] == "email-intake-template")
        param_names = [p["name"] for p in tpl["blueprint_params"]]
        assert "source" in param_names
        assert "folder" in param_names


# ---------------------------------------------------------------------------
# 3. instantiate_template: alle Params → korrekte Pipeline
# ---------------------------------------------------------------------------

class TestInstantiateTemplate:
    def test_full_instantiation(self, tmp_pipelines_dir):
        """All required params supplied → new pipeline created."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "gmail-intake",
                "params": {"source": "gmail", "folder": "Work"},
            })
        )
        assert result["success"] is True, result.get("error")
        assert result["instance_name"] == "gmail-intake"
        assert result["template_name"] == "email-intake-template"
        assert result["resolved_params"]["source"] == "gmail"
        assert result["resolved_params"]["folder"] == "Work"

    def test_instance_file_created(self, tmp_pipelines_dir):
        """instantiate_template must persist the new pipeline file."""
        from brix.mcp_handlers._shared import _pipeline_dir

        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "outlook-intake",
                "params": {"source": "outlook"},
            })
        )
        assert (_pipeline_dir() / "outlook-intake.yaml").exists()


# ---------------------------------------------------------------------------
# 4. instantiate_template: fehlender required Param → Fehler
# ---------------------------------------------------------------------------

class TestMissingRequiredParam:
    def test_missing_required_param_returns_error(self, tmp_pipelines_dir):
        """Missing required parameter must return an error, not crash."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "bad-instance",
                "params": {},  # 'source' is required but absent
            })
        )
        assert result["success"] is False
        assert "source" in result["error"]


# ---------------------------------------------------------------------------
# 5. Default-Werte werden eingesetzt
# ---------------------------------------------------------------------------

class TestDefaultValues:
    def test_optional_param_uses_default(self, tmp_pipelines_dir):
        """An optional param not supplied should use its declared default."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "gmail-default-folder",
                "params": {"source": "gmail"},  # 'folder' absent → default "INBOX"
            })
        )
        assert result["success"] is True, result.get("error")
        assert result["resolved_params"]["folder"] == "INBOX"

    def test_default_value_rendered_in_step(self, tmp_pipelines_dir):
        """The default value must actually appear in the rendered step params."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "gmail-default-folder2",
                "params": {"source": "gmail"},
            })
        )
        assert result["success"] is True
        step = result["pipeline"]["steps"][0]
        assert step["params"]["folder"] == "INBOX"


# ---------------------------------------------------------------------------
# 6. enum Param mit ungültigem Wert → Fehler
# ---------------------------------------------------------------------------

class TestEnumValidation:
    def test_invalid_enum_value_returns_error(self, tmp_pipelines_dir):
        """An enum param with an unlisted value must be rejected."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "bad-enum-instance",
                "params": {"source": "yahoo"},  # not in enum_values
            })
        )
        assert result["success"] is False
        assert "yahoo" in result["error"] or "source" in result["error"]

    def test_valid_enum_value_accepted(self, tmp_pipelines_dir):
        """A valid enum value must pass through without error."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "valid-enum-instance",
                "params": {"source": "outlook"},
            })
        )
        assert result["success"] is True


# ---------------------------------------------------------------------------
# 7. Instanziierte Pipeline hat is_template=False
# ---------------------------------------------------------------------------

class TestInstanceIsNotTemplate:
    def test_instance_has_is_template_false(self, tmp_pipelines_dir):
        """The created instance must have is_template=False."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "not-a-template",
                "params": {"source": "gmail"},
            })
        )
        assert result["success"] is True
        assert result["pipeline"]["is_template"] is False

    def test_instance_not_returned_by_list_templates(self, tmp_pipelines_dir):
        """Instantiated pipeline must NOT appear in list_templates results."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import (
            _handle_instantiate_template,
            _handle_list_templates,
        )
        asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "should-not-be-template",
                "params": {"source": "gmail"},
            })
        )
        list_result = asyncio.get_event_loop().run_until_complete(
            _handle_list_templates({})
        )
        names = [t["name"] for t in list_result["templates"]]
        assert "should-not-be-template" not in names
        assert "email-intake-template" in names


# ---------------------------------------------------------------------------
# 8. Jinja2-Rendering in Step-Parametern funktioniert
# ---------------------------------------------------------------------------

class TestJinja2Rendering:
    def test_tpl_placeholders_replaced_in_steps(self, tmp_pipelines_dir):
        """{{ tpl.X }} must be replaced with supplied values in step fields."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "rendered-instance",
                "params": {"source": "outlook", "folder": "Sent"},
            })
        )
        assert result["success"] is True
        step = result["pipeline"]["steps"][0]
        # server field: {{ tpl.source }}-mcp → outlook-mcp
        assert step["server"] == "outlook-mcp"
        # params.folder: {{ tpl.folder }} → Sent
        assert step["params"]["folder"] == "Sent"
        assert step["params"]["source"] == "outlook"

    def test_tpl_placeholder_in_description(self, tmp_pipelines_dir):
        """{{ tpl.X }} in the description top-level field is also rendered."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "desc-rendered",
                "params": {"source": "gmail"},
            })
        )
        assert result["success"] is True
        assert "gmail" in result["pipeline"].get("description", "")

    def test_template_not_mutated_after_instantiation(self, tmp_pipelines_dir):
        """Instantiating a template must not modify the original template file."""
        import yaml
        from brix.mcp_handlers._shared import _pipeline_dir

        _save_raw("email-intake-template", _make_template_dict())

        # Read template before
        with open(_pipeline_dir() / "email-intake-template.yaml") as f:
            before = yaml.safe_load(f)

        from brix.mcp_handlers.templates import _handle_instantiate_template
        asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "clone-check",
                "params": {"source": "gmail"},
            })
        )

        # Read template after
        with open(_pipeline_dir() / "email-intake-template.yaml") as f:
            after = yaml.safe_load(f)

        assert before == after  # Template unchanged


# ---------------------------------------------------------------------------
# 9. Nicht-Template-Pipeline bei instantiate abgelehnt
# ---------------------------------------------------------------------------

class TestNonTemplateRejected:
    def test_instantiate_non_template_rejected(self, tmp_pipelines_dir):
        """Calling instantiate_template on a non-template pipeline must fail."""
        _save_raw("regular-pipeline", {
            "name": "regular-pipeline",
            "version": "1.0.0",
            "steps": [{"id": "s1", "type": "set", "values": {"x": 1}}],
        })

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "regular-pipeline",
                "instance_name": "bad-instance",
                "params": {},
            })
        )
        assert result["success"] is False
        assert "not a template" in result["error"].lower() or "is_template" in result["error"]

    def test_instantiate_missing_template_returns_error(self, tmp_pipelines_dir):
        """Calling instantiate_template with a non-existent template name must fail."""
        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "does-not-exist",
                "instance_name": "any-name",
                "params": {},
            })
        )
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ---------------------------------------------------------------------------
# 10. Parameter validation edge cases
# ---------------------------------------------------------------------------

class TestParameterEdgeCases:
    def test_no_params_required_succeeds(self, tmp_pipelines_dir):
        """A template with no required params can be instantiated with empty params."""
        _save_raw("no-params-template", {
            "name": "no-params-template",
            "version": "1.0.0",
            "is_template": True,
            "blueprint_params": [],
            "steps": [{"id": "s1", "type": "set", "values": {"x": 1}}],
        })

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "no-params-template",
                "instance_name": "no-params-instance",
                "params": {},
            })
        )
        assert result["success"] is True

    def test_instance_name_in_pipeline_name_field(self, tmp_pipelines_dir):
        """The created instance pipeline must have instance_name as its name field."""
        _save_raw("email-intake-template", _make_template_dict())

        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
                "instance_name": "my-custom-intake",
                "params": {"source": "gmail"},
            })
        )
        assert result["success"] is True
        assert result["pipeline"]["name"] == "my-custom-intake"

    def test_missing_template_name_param_returns_error(self, tmp_pipelines_dir):
        """instantiate_template without template_name returns an error."""
        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "instance_name": "some-instance",
            })
        )
        assert result["success"] is False

    def test_missing_instance_name_param_returns_error(self, tmp_pipelines_dir):
        """instantiate_template without instance_name returns an error."""
        from brix.mcp_handlers.templates import _handle_instantiate_template
        result = asyncio.get_event_loop().run_until_complete(
            _handle_instantiate_template({
                "template_name": "email-intake-template",
            })
        )
        assert result["success"] is False
