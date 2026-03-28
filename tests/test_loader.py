"""Tests for brix.loader module."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from brix.loader import PipelineLoader
from brix.models import Pipeline

# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------


def test_load_from_string_minimal():
    """Minimal valid pipeline YAML loads correctly."""
    yaml_str = """
name: test
steps:
  - id: s1
    type: python
    script: run.py
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert pipeline.name == "test"
    assert len(pipeline.steps) == 1
    assert pipeline.steps[0].id == "s1"


def test_load_from_string_full():
    """Full pipeline YAML with all fields parses correctly."""
    yaml_str = """
name: full-test
version: "1.0.0"
description: Full test pipeline
input:
  query:
    type: str
    default: "test"
credentials:
  token:
    env: BRIX_CRED_TOKEN
error_handling:
  on_error: continue
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
    params:
      filter: "{{ input.query }}"
  - id: process
    type: python
    script: helpers/process.py
    foreach: "{{ fetch.output }}"
    parallel: true
    concurrency: 5
output:
  result: "{{ process.output }}"
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert pipeline.version == "1.0.0"
    assert "query" in pipeline.input
    assert pipeline.error_handling.on_error == "continue"
    assert len(pipeline.steps) == 2


def test_load_from_file():
    """Load pipeline from an actual YAML file on disk."""
    yaml_content = """
name: file-test
steps:
  - id: s1
    type: cli
    args: ["echo", "hello"]
"""
    loader = PipelineLoader()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        pipeline = loader.load(path)
        assert pipeline.name == "file-test"
    finally:
        os.unlink(path)


def test_load_invalid_yaml():
    """Malformed YAML raises an exception."""
    loader = PipelineLoader()
    with pytest.raises(Exception):
        loader.load_from_string("name: [invalid yaml {{")


# ---------------------------------------------------------------------------
# render_template tests
# ---------------------------------------------------------------------------


def test_render_template_simple():
    """Simple variable substitution works."""
    loader = PipelineLoader()
    result = loader.render_template("Hello {{ name }}", {"name": "World"})
    assert result == "Hello World"


def test_render_template_nested():
    """Nested context access works."""
    loader = PipelineLoader()
    result = loader.render_template("{{ input.query }}", {"input": {"query": "test"}})
    assert result == "test"


def test_render_template_default_filter():
    """Jinja2 default filter provides a fallback for missing variables (D-16)."""
    loader = PipelineLoader()
    result = loader.render_template("{{ missing | default('fallback') }}", {})
    assert result == "fallback"


def test_render_template_default_list():
    """Default filter with an empty-list sentinel."""
    loader = PipelineLoader()
    result = loader.render_template("{{ items | default([]) }}", {})
    assert result == "[]"


def test_render_template_sandbox_blocks_imports():
    """SandboxedEnvironment blocks dangerous dunder access (D-13)."""
    loader = PipelineLoader()
    with pytest.raises(Exception):
        loader.render_template("{{ ''.__class__.__mro__[1].__subclasses__() }}", {})


# ---------------------------------------------------------------------------
# render_value tests
# ---------------------------------------------------------------------------


def test_render_value_plain_string():
    """Plain string without {{ }} passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value("hello", {}) == "hello"


def test_render_value_template_string():
    """String with {{ }} is rendered."""
    loader = PipelineLoader()
    assert loader.render_value("{{ name }}", {"name": "World"}) == "World"


def test_render_value_json_result():
    """Template that renders to valid JSON is parsed to the native type."""
    loader = PipelineLoader()
    ctx = {"data": json.dumps({"key": "value"})}
    result = loader.render_value("{{ data }}", ctx)
    assert result == {"key": "value"}


def test_render_value_dict():
    """Dict values are recursively rendered."""
    loader = PipelineLoader()
    result = loader.render_value({"a": "{{ x }}", "b": "plain"}, {"x": "rendered"})
    assert result == {"a": "rendered", "b": "plain"}


def test_render_value_list():
    """List values are recursively rendered."""
    loader = PipelineLoader()
    result = loader.render_value(["{{ x }}", "plain"], {"x": "rendered"})
    assert result == ["rendered", "plain"]


def test_render_value_non_string_int():
    """Integer passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value(42, {}) == 42


def test_render_value_non_string_bool():
    """Boolean passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value(True, {}) is True


def test_render_value_non_string_none():
    """None passes through unchanged."""
    loader = PipelineLoader()
    assert loader.render_value(None, {}) is None


# ---------------------------------------------------------------------------
# evaluate_condition tests
# ---------------------------------------------------------------------------


def test_evaluate_condition_true():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ flag }}", {"flag": True}) is True


def test_evaluate_condition_false():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ flag }}", {"flag": False}) is False


def test_evaluate_condition_empty_string():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ val }}", {"val": ""}) is False


def test_evaluate_condition_none_value():
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ val }}", {"val": None}) is False


def test_evaluate_condition_missing_var():
    """Missing variable renders to empty string → falsy (D-16 / Undefined)."""
    loader = PipelineLoader()
    assert loader.evaluate_condition("{{ missing }}", {}) is False


def test_evaluate_condition_no_condition_empty():
    """Empty condition string → always execute."""
    loader = PipelineLoader()
    assert loader.evaluate_condition("", {}) is True


def test_evaluate_condition_no_condition_none():
    """None condition → always execute."""
    loader = PipelineLoader()
    assert loader.evaluate_condition(None, {}) is True


# ---------------------------------------------------------------------------
# resolve_foreach tests
# ---------------------------------------------------------------------------


def test_resolve_foreach_list():
    loader = PipelineLoader()
    result = loader.resolve_foreach("{{ items }}", {"items": [1, 2, 3]})
    assert result == [1, 2, 3]


def test_resolve_foreach_json_string():
    """A template rendering to a JSON list string is parsed correctly."""
    loader = PipelineLoader()
    result = loader.resolve_foreach("{{ data }}", {"data": json.dumps([1, 2, 3])})
    assert result == [1, 2, 3]


def test_resolve_foreach_not_list_raises():
    """Non-list resolution raises ValueError with descriptive message."""
    loader = PipelineLoader()
    with pytest.raises(ValueError, match="did not resolve to a list"):
        loader.resolve_foreach("{{ val }}", {"val": "not-a-list"})


# ---------------------------------------------------------------------------
# T-BRIX-V3-09: unwrap_foreach filter tests
# ---------------------------------------------------------------------------


def test_unwrap_foreach_filter():
    """unwrap_foreach extracts data from successful foreach items."""
    loader = PipelineLoader()
    items = [
        {"success": True, "data": "a"},
        {"success": False, "error": "oops"},
        {"success": True, "data": "b"},
    ]
    result = loader.render_value("{{ items | unwrap_foreach }}", {"items": items})
    assert result == ["a", "b"]


def test_unwrap_foreach_filter_all_success():
    """unwrap_foreach with all-success items returns full data list."""
    loader = PipelineLoader()
    items = [{"success": True, "data": i} for i in range(3)]
    result = loader.render_value("{{ items | unwrap_foreach }}", {"items": items})
    assert result == [0, 1, 2]


def test_unwrap_foreach_filter_non_list_passthrough():
    """unwrap_foreach passes through non-list values unchanged."""
    loader = PipelineLoader()
    result = loader.render_value("{{ val | unwrap_foreach }}", {"val": "not-a-list"})
    assert result == "not-a-list"


# ---------------------------------------------------------------------------
# T-BRIX-V4-02: iif filter
# ---------------------------------------------------------------------------


def test_iif_filter_true():
    """iif with truthy value returns the true branch."""
    loader = PipelineLoader()
    result = loader.render_template("{{ flag | iif('yes', 'no') }}", {"flag": True})
    assert result == "yes"


def test_iif_filter_false():
    """iif with falsy value returns the false branch."""
    loader = PipelineLoader()
    result = loader.render_template("{{ flag | iif('yes', 'no') }}", {"flag": False})
    assert result == "no"


def test_iif_filter_default():
    """iif with falsy value and no false_val argument returns empty string."""
    loader = PipelineLoader()
    result = loader.render_template("{{ flag | iif('yes') }}", {"flag": False})
    assert result == ""


# ---------------------------------------------------------------------------
# T-BRIX-V4-17: include: mechanism — inline groups
# ---------------------------------------------------------------------------


def test_include_inline_group_expands_steps():
    """include: referencing an inline group expands its steps in place."""
    yaml_str = """
name: test-include
groups:
  auth_steps:
    - id: login
      type: python
      script: helpers/login.py
    - id: get_token
      type: python
      script: helpers/token.py
steps:
  - include: auth_steps
  - id: do_work
    type: python
    script: helpers/work.py
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert len(pipeline.steps) == 3
    assert pipeline.steps[0].id == "login"
    assert pipeline.steps[1].id == "get_token"
    assert pipeline.steps[2].id == "do_work"


def test_include_inline_group_is_deep_copied():
    """Each include expansion produces independent step copies."""
    yaml_str = """
name: test-copy
groups:
  auth:
    - id: login
      type: python
      script: helpers/login.py
steps:
  - include: auth
  - include: auth
  - id: final
    type: python
    script: helpers/final.py
"""
    loader = PipelineLoader()
    # Duplicate IDs would cause validation error; ensure deep copy does not
    # prevent parsing (note: duplicate ids are a validator concern, not loader's)
    raw = {"name": "test-copy", "groups": {"auth": [{"id": "login", "type": "python", "script": "run.py"}]}, "steps": [{"include": "auth"}, {"include": "auth"}]}
    result = loader.resolve_includes(raw)
    assert len(result["steps"]) == 2
    assert result["steps"][0]["id"] == "login"
    assert result["steps"][1]["id"] == "login"
    # Ensure they are distinct objects (deep copy)
    assert result["steps"][0] is not result["steps"][1]


def test_include_groups_preserved_in_model():
    """The groups section is preserved on the parsed Pipeline model."""
    yaml_str = """
name: test-groups-model
groups:
  setup:
    - id: init
      type: python
      script: helpers/init.py
steps:
  - include: setup
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert "setup" in pipeline.groups
    assert pipeline.groups["setup"][0]["id"] == "init"


def test_include_unknown_group_raises():
    """include: referencing an undefined group raises ValueError."""
    yaml_str = """
name: test-unknown
steps:
  - include: nonexistent_group
"""
    loader = PipelineLoader()
    with pytest.raises(ValueError, match="nonexistent_group"):
        loader.load_from_string(yaml_str)


def test_include_circular_detection():
    """Circular group references are detected and raise ValueError."""
    # We simulate a circular reference via nested include by calling
    # resolve_includes directly with a crafted raw dict.
    loader = PipelineLoader()
    # A group that references itself
    raw = {
        "name": "test-circular",
        "groups": {
            "loop": [{"include": "loop"}],
        },
        "steps": [{"include": "loop"}],
    }
    with pytest.raises(ValueError, match="[Cc]ircular"):
        loader.resolve_includes(raw)


def test_include_mixed_with_regular_steps():
    """Include can be mixed with regular steps at any position."""
    yaml_str = """
name: test-mixed
groups:
  cleanup:
    - id: remove_tmp
      type: cli
      args: ["rm", "-rf", "/tmp/work"]
steps:
  - id: fetch
    type: python
    script: helpers/fetch.py
  - include: cleanup
  - id: report
    type: python
    script: helpers/report.py
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert len(pipeline.steps) == 3
    assert pipeline.steps[0].id == "fetch"
    assert pipeline.steps[1].id == "remove_tmp"
    assert pipeline.steps[2].id == "report"


def test_include_nested_groups():
    """A group can include another group (nested include expansion)."""
    yaml_str = """
name: test-nested
groups:
  base:
    - id: s1
      type: python
      script: helpers/s1.py
  extended:
    - include: base
    - id: s2
      type: python
      script: helpers/s2.py
steps:
  - include: extended
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(yaml_str)
    assert len(pipeline.steps) == 2
    assert pipeline.steps[0].id == "s1"
    assert pipeline.steps[1].id == "s2"


def test_include_empty_group_produces_no_steps():
    """An include of an empty group inserts no steps."""
    loader = PipelineLoader()
    raw = {
        "name": "test-empty-group",
        "groups": {"nothing": []},
        "steps": [
            {"include": "nothing"},
            {"id": "real", "type": "python", "script": "run.py"},
        ],
    }
    result = loader.resolve_includes(raw)
    assert len(result["steps"]) == 1
    assert result["steps"][0]["id"] == "real"


# ---------------------------------------------------------------------------
# T-BRIX-V4-17: include: mechanism — external file references
# ---------------------------------------------------------------------------


def test_include_external_file_steps():
    """include: can reference an external YAML file with a steps: key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write the external file
        ext_file = Path(tmpdir) / "shared.yaml"
        ext_file.write_text("""
steps:
  - id: shared_step
    type: python
    script: helpers/shared.py
""")
        # Write the main pipeline
        main_file = Path(tmpdir) / "main.yaml"
        main_file.write_text(f"""
name: test-ext-file
steps:
  - include: shared.yaml
  - id: local_step
    type: python
    script: helpers/local.py
""")
        loader = PipelineLoader()
        pipeline = loader.load(str(main_file))
        assert len(pipeline.steps) == 2
        assert pipeline.steps[0].id == "shared_step"
        assert pipeline.steps[1].id == "local_step"


def test_include_external_file_group_fragment():
    """include: file.yaml#group_name selects a named group from an external file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ext_file = Path(tmpdir) / "groups.yaml"
        ext_file.write_text("""
groups:
  auth:
    - id: auth_step
      type: python
      script: helpers/auth.py
  cleanup:
    - id: cleanup_step
      type: python
      script: helpers/cleanup.py
""")
        main_file = Path(tmpdir) / "main.yaml"
        main_file.write_text("""
name: test-fragment
steps:
  - include: groups.yaml#auth
  - id: work
    type: python
    script: helpers/work.py
""")
        loader = PipelineLoader()
        pipeline = loader.load(str(main_file))
        assert len(pipeline.steps) == 2
        assert pipeline.steps[0].id == "auth_step"
        assert pipeline.steps[1].id == "work"


def test_include_external_file_not_found_raises():
    """include: referencing a missing file raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        main_file = Path(tmpdir) / "main.yaml"
        main_file.write_text("""
name: test-missing
steps:
  - include: nonexistent.yaml
""")
        loader = PipelineLoader()
        with pytest.raises(ValueError, match="not found"):
            loader.load(str(main_file))


def test_include_external_file_missing_fragment_raises():
    """include: file.yaml#missing_group raises ValueError when group not found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ext_file = Path(tmpdir) / "groups.yaml"
        ext_file.write_text("""
groups:
  auth:
    - id: auth_step
      type: python
      script: helpers/auth.py
""")
        main_file = Path(tmpdir) / "main.yaml"
        main_file.write_text("""
name: test-bad-fragment
steps:
  - include: groups.yaml#no_such_group
""")
        loader = PipelineLoader()
        with pytest.raises(ValueError, match="no_such_group"):
            loader.load(str(main_file))


def test_include_external_file_no_steps_no_fragment_raises():
    """include: file.yaml without steps: and no fragment raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ext_file = Path(tmpdir) / "groups.yaml"
        ext_file.write_text("""
groups:
  auth:
    - id: auth_step
      type: python
      script: helpers/auth.py
""")
        main_file = Path(tmpdir) / "main.yaml"
        main_file.write_text("""
name: test-no-steps
steps:
  - include: groups.yaml
""")
        loader = PipelineLoader()
        with pytest.raises(ValueError, match="no 'steps' key"):
            loader.load(str(main_file))


def test_resolve_includes_no_includes_passthrough():
    """resolve_includes with no include: entries returns raw dict unchanged."""
    loader = PipelineLoader()
    raw = {
        "name": "plain",
        "steps": [{"id": "s1", "type": "python", "script": "run.py"}],
    }
    result = loader.resolve_includes(raw)
    assert result["steps"] == raw["steps"]


def test_resolve_includes_non_dict_passthrough():
    """resolve_includes with a non-dict input returns it unchanged."""
    loader = PipelineLoader()
    assert loader.resolve_includes("not-a-dict") == "not-a-dict"


# ---------------------------------------------------------------------------
# render_step_params — body field rendering (INBOX-344)
# ---------------------------------------------------------------------------


def _make_step(**kwargs):
    """Minimal Step-like object for render_step_params tests."""
    from brix.models import Step
    defaults = {
        "id": "s1",
        "type": "http",
        "url": None,
        "command": None,
        "args": None,
        "headers": None,
        "body": None,
        "params": None,
        "values": None,
        "script": None,
    }
    defaults.update(kwargs)
    return Step(**{k: v for k, v in defaults.items() if k in Step.model_fields})


def test_render_step_params_body_string_template():
    """body string with {{ }} template is rendered into _body key."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: t
steps:
  - id: s1
    type: http
    url: https://example.com
    method: POST
    body: "gmail:{{ input.email }}"
""")
    step = pipeline.steps[0]
    ctx = {"input": {"email": "user@example.com"}}
    rendered = loader.render_step_params(step, ctx)
    assert "_body" in rendered
    assert rendered["_body"] == "gmail:user@example.com"


def test_render_step_params_body_dict_with_template():
    """body dict whose values contain {{ }} templates are rendered recursively."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: t
steps:
  - id: s1
    type: http
    url: https://example.com
    method: POST
    body:
      source: "gmail:{{ input.email }}"
      limit: 10
""")
    step = pipeline.steps[0]
    ctx = {"input": {"email": "user@example.com"}}
    rendered = loader.render_step_params(step, ctx)
    assert "_body" in rendered
    assert rendered["_body"]["source"] == "gmail:user@example.com"
    assert rendered["_body"]["limit"] == 10


def test_render_step_params_body_none_not_in_rendered():
    """When body is None, _body key must not appear in the rendered params."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: t
steps:
  - id: s1
    type: http
    url: https://example.com
""")
    step = pipeline.steps[0]
    ctx = {}
    rendered = loader.render_step_params(step, ctx)
    assert "_body" not in rendered


def test_render_step_params_body_static_dict_preserved():
    """body dict without templates is stored under _body unchanged."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: t
steps:
  - id: s1
    type: http
    url: https://example.com
    method: POST
    body:
      key: value
      number: 42
""")
    step = pipeline.steps[0]
    rendered = loader.render_step_params(step, {})
    assert "_body" in rendered
    assert rendered["_body"] == {"key": "value", "number": 42}


# ---------------------------------------------------------------------------
# T-BRIX-V6-18: extends — Pipeline-Template inheritance
# ---------------------------------------------------------------------------


def test_extends_basic_template_substitution():
    """Instance pipeline inherits steps from base template with {{ template.X }} substitution."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_file = Path(tmpdir) / "extract-base.yaml"
        base_file.write_text("""
kind: template
name: extract-base
steps:
  - id: extract
    type: python
    script: "{{ template.script }}"
""")
        loader = PipelineLoader()
        pipeline = loader.load_from_string(
            """
name: my-extract
extends: extract-base
template_params:
  script: helpers/buddy_extract_beihilfe.py
""",
            base_dir=tmpdir,
        )
        assert pipeline.name == "my-extract"
        assert len(pipeline.steps) == 1
        assert pipeline.steps[0].script == "helpers/buddy_extract_beihilfe.py"


def test_extends_template_params_multiple_placeholders():
    """Multiple {{ template.X }} placeholders are all substituted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_file = Path(tmpdir) / "base.yaml"
        base_file.write_text("""
kind: template
name: base
steps:
  - id: fetch
    type: python
    script: "{{ template.script }}"
    params:
      label: "{{ template.label }}"
""")
        loader = PipelineLoader()
        pipeline = loader.load_from_string(
            """
name: instance
extends: base
template_params:
  script: helpers/my_script.py
  label: my-label
""",
            base_dir=tmpdir,
        )
        assert pipeline.steps[0].script == "helpers/my_script.py"
        assert pipeline.steps[0].params["label"] == "my-label"


def test_extends_instance_fields_override_base():
    """Instance-level fields (description, version) override base template fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_file = Path(tmpdir) / "base.yaml"
        base_file.write_text("""
kind: template
name: base
version: "0.0.1"
description: Base description
steps:
  - id: s1
    type: python
    script: run.py
""")
        loader = PipelineLoader()
        pipeline = loader.load_from_string(
            """
name: instance
version: "1.2.3"
description: My description
extends: base
template_params: {}
""",
            base_dir=tmpdir,
        )
        assert pipeline.name == "instance"
        assert pipeline.version == "1.2.3"
        assert pipeline.description == "My description"


def test_extends_instance_steps_override_base_steps():
    """If the instance defines its own steps, they take precedence over the base."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_file = Path(tmpdir) / "base.yaml"
        base_file.write_text("""
kind: template
name: base
steps:
  - id: base_step
    type: python
    script: base.py
""")
        loader = PipelineLoader()
        pipeline = loader.load_from_string(
            """
name: instance
extends: base
template_params: {}
steps:
  - id: my_step
    type: python
    script: mine.py
""",
            base_dir=tmpdir,
        )
        assert len(pipeline.steps) == 1
        assert pipeline.steps[0].id == "my_step"


def test_extends_no_extends_unchanged():
    """Pipeline without extends is loaded normally (no regression)."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: plain
steps:
  - id: s1
    type: python
    script: run.py
""")
    assert pipeline.name == "plain"
    assert pipeline.steps[0].id == "s1"


def test_extends_template_not_found_raises():
    """extends: referencing a non-existent template raises ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = PipelineLoader()
        with pytest.raises(ValueError, match="nonexistent"):
            loader.load_from_string(
                """
name: instance
extends: nonexistent
template_params: {}
""",
                base_dir=tmpdir,
            )


def test_extends_from_file():
    """load() resolves extends relative to the pipeline file's directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_file = Path(tmpdir) / "base-template.yaml"
        base_file.write_text("""
kind: template
name: base-template
steps:
  - id: run
    type: python
    script: "{{ template.script }}"
""")
        instance_file = Path(tmpdir) / "instance.yaml"
        instance_file.write_text("""
name: from-file
extends: base-template
template_params:
  script: helpers/actual.py
""")
        loader = PipelineLoader()
        pipeline = loader.load(str(instance_file))
        assert pipeline.name == "from-file"
        assert pipeline.steps[0].script == "helpers/actual.py"


def test_extends_search_paths():
    """extends can find templates in search_paths when not in base_dir."""
    with tempfile.TemporaryDirectory() as templates_dir:
        with tempfile.TemporaryDirectory() as instance_dir:
            base_file = Path(templates_dir) / "shared-base.yaml"
            base_file.write_text("""
kind: template
name: shared-base
steps:
  - id: work
    type: python
    script: "{{ template.script }}"
""")
            loader = PipelineLoader()
            pipeline = loader.load_from_string(
                """
name: using-shared
extends: shared-base
template_params:
  script: helpers/worker.py
""",
                base_dir=instance_dir,
                search_paths=[templates_dir],
            )
            assert pipeline.steps[0].script == "helpers/worker.py"


def test_extends_kind_and_meta_not_propagated():
    """kind, extends, and template_params fields are stripped from the merged pipeline."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_file = Path(tmpdir) / "base.yaml"
        base_file.write_text("""
kind: template
name: base
steps:
  - id: s1
    type: python
    script: run.py
""")
        loader = PipelineLoader()
        pipeline = loader.load_from_string(
            """
name: instance
extends: base
template_params:
  foo: bar
""",
            base_dir=tmpdir,
        )
        # kind should be None (not "template"), extends should be None
        assert pipeline.kind is None
        assert pipeline.extends is None
        assert pipeline.template_params == {}
