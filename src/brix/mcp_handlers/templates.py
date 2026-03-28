"""Template handler — Pipeline-Blueprint instantiation (T-BRIX-V8-08).

Provides two MCP tool handlers:
- ``_handle_list_templates``: return all pipelines with is_template=True.
- ``_handle_instantiate_template``: render a template's {{ tpl.X }} placeholders
  with caller-supplied params and save the result as a new, runnable pipeline.
"""
from __future__ import annotations

import copy
import json
import uuid as _uuid_mod

import yaml
from jinja2 import ChainableUndefined
from jinja2.sandbox import SandboxedEnvironment

from brix.mcp_handlers._shared import (
    _audit_db,
    _extract_source,
    _source_summary,
    _load_pipeline_yaml,
    _save_pipeline_yaml,
    _pipeline_dir,
    _validate_pipeline_dict,
)
from brix.pipeline_store import PipelineStore


# ---------------------------------------------------------------------------
# Jinja2 environment for template rendering
# ---------------------------------------------------------------------------

def _make_jinja_env() -> SandboxedEnvironment:
    env = SandboxedEnvironment(undefined=ChainableUndefined)
    env.filters["tojson"] = json.dumps
    return env


def _render_value(env: SandboxedEnvironment, value, context: dict):
    """Recursively render ``{{ tpl.X }}`` placeholders in *value*."""
    if isinstance(value, str) and "{{" in value:
        rendered = env.from_string(value).render(context)
        # Try to parse back to native type (int/bool/list/dict)
        try:
            return json.loads(rendered)
        except (json.JSONDecodeError, ValueError):
            pass
        return rendered
    elif isinstance(value, dict):
        return {k: _render_value(env, v, context) for k, v in value.items()}
    elif isinstance(value, list):
        return [_render_value(env, item, context) for item in value]
    return value


# ---------------------------------------------------------------------------
# list_templates
# ---------------------------------------------------------------------------

async def _handle_list_templates(arguments: dict) -> dict:
    """Return all pipelines that have ``is_template: true``."""
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    all_info = store.list_all()

    templates = []
    for info in all_info:
        name = info["name"]
        try:
            raw = store.load_raw(name)
        except Exception:
            continue
        if not raw.get("is_template", False):
            continue
        params_raw = raw.get("blueprint_params", [])
        # Normalise — may be stored as list[dict]
        params_out = []
        for p in params_raw:
            if isinstance(p, dict):
                params_out.append({
                    "name": p.get("name", ""),
                    "description": p.get("description", ""),
                    "type": p.get("type", "string"),
                    "required": p.get("required", True),
                    "default": p.get("default"),
                    "enum_values": p.get("enum_values"),
                })
        templates.append({
            "name": name,
            "description": raw.get("description", ""),
            "blueprint_params": params_out,
            "step_count": len(raw.get("steps", [])),
        })

    return {
        "success": True,
        "templates": templates,
        "total": len(templates),
    }


# ---------------------------------------------------------------------------
# instantiate_template
# ---------------------------------------------------------------------------

async def _handle_instantiate_template(arguments: dict) -> dict:
    """Render a template with provided params and save as a new pipeline.

    Steps:
    1. Load template pipeline (must have is_template=True).
    2. Validate that all required blueprint_params are supplied.
    3. Validate enum params against enum_values.
    4. Fill defaults for optional missing params.
    5. Render {{ tpl.X }} in all step fields.
    6. Save new pipeline with instance_name (is_template=False).
    """
    template_name = (arguments.get("template_name") or "").strip()
    instance_name = (arguments.get("instance_name") or "").strip()
    params: dict = arguments.get("params") or {}
    source = _extract_source(arguments)

    if not template_name:
        return {"success": False, "error": "Parameter 'template_name' is required"}
    if not instance_name:
        return {"success": False, "error": "Parameter 'instance_name' is required"}

    # --- 1. Load template ---
    try:
        raw = _load_pipeline_yaml(template_name)
    except FileNotFoundError:
        return {"success": False, "error": f"Template '{template_name}' not found"}

    if not raw.get("is_template", False):
        return {
            "success": False,
            "error": (
                f"Pipeline '{template_name}' is not a template "
                "(is_template must be true)"
            ),
        }

    blueprint_params: list[dict] = raw.get("blueprint_params", [])

    # --- 2 & 3. Validate params ---
    resolved_params: dict = {}
    for param_def in blueprint_params:
        pname = param_def.get("name", "")
        required = param_def.get("required", True)
        default = param_def.get("default")
        ptype = param_def.get("type", "string")
        enum_values = param_def.get("enum_values")

        if pname in params:
            value = params[pname]
        elif not required:
            value = default
        else:
            return {
                "success": False,
                "error": f"Required parameter '{pname}' is missing",
            }

        # Enum validation
        if ptype == "enum" and enum_values and value not in enum_values:
            return {
                "success": False,
                "error": (
                    f"Parameter '{pname}' has invalid value '{value}'. "
                    f"Allowed values: {enum_values}"
                ),
            }

        resolved_params[pname] = value

    # --- 4. Render {{ tpl.X }} in step fields ---
    env = _make_jinja_env()
    render_context = {"tpl": resolved_params}

    # Deep-copy so we don't mutate the cached raw dict
    instance_raw = copy.deepcopy(raw)

    # Remove template-only fields from the instance
    instance_raw["is_template"] = False
    instance_raw.pop("blueprint_params", None)
    instance_raw["name"] = instance_name

    # Assign a fresh UUID for the instance
    instance_raw["id"] = str(_uuid_mod.uuid4())

    # Render all step fields recursively
    rendered_steps = _render_value(env, instance_raw.get("steps", []), render_context)
    instance_raw["steps"] = rendered_steps

    # Render other top-level string fields that may use {{ tpl.X }}
    for field in ("description", "version"):
        if isinstance(instance_raw.get(field), str) and "{{" in instance_raw[field]:
            instance_raw[field] = _render_value(env, instance_raw[field], render_context)

    # Render input / output / credentials dicts
    for field in ("input", "output", "credentials"):
        if field in instance_raw and isinstance(instance_raw[field], dict):
            instance_raw[field] = _render_value(env, instance_raw[field], render_context)

    # --- 5. Validate the resulting instance pipeline ---
    validation = _validate_pipeline_dict(instance_raw)

    # --- 6. Save ---
    _save_pipeline_yaml(instance_name, instance_raw)

    _audit_db.write_audit_entry(
        tool="brix__instantiate_template",
        source=source,
        arguments_summary=_source_summary(
            source,
            pipeline=instance_name,
            template=template_name,
        ),
    )

    return {
        "success": True,
        "instance_name": instance_name,
        "template_name": template_name,
        "resolved_params": resolved_params,
        "step_count": len(rendered_steps),
        "validated": validation["valid"],
        "validation": validation,
        "pipeline": instance_raw,
    }
