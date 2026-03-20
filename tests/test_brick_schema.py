"""Tests for Brix v2 brick schema system."""

import pytest
from brix.bricks.schema import BrickParam, BrickSchema
from brix.bricks.builtins import ALL_BUILTINS, HTTP_GET, HTTP_POST, RUN_CLI


# ---------------------------------------------------------------------------
# BrickParam tests
# ---------------------------------------------------------------------------


def test_brick_param_required_fields():
    param = BrickParam(type="string", description="A URL")
    assert param.type == "string"
    assert param.description == "A URL"
    assert param.required is False
    assert param.default is None
    assert param.enum is None


def test_brick_param_with_default():
    param = BrickParam(type="string", description="Timeout", default="60s")
    assert param.default == "60s"


def test_brick_param_required_true():
    param = BrickParam(type="string", description="Required field", required=True)
    assert param.required is True


def test_brick_param_with_enum():
    param = BrickParam(type="string", description="Method", enum=["GET", "POST", "PUT"])
    assert param.enum == ["GET", "POST", "PUT"]


def test_brick_param_boolean_type():
    param = BrickParam(type="boolean", description="Binary mode", default=False)
    assert param.type == "boolean"
    assert param.default is False


def test_brick_param_array_type():
    param = BrickParam(type="array", description="List of items")
    assert param.type == "array"


def test_brick_param_object_type():
    param = BrickParam(type="object", description="Key-value map")
    assert param.type == "object"


# ---------------------------------------------------------------------------
# BrickSchema creation tests
# ---------------------------------------------------------------------------


def test_brick_schema_basic():
    schema = BrickSchema(
        name="test_brick",
        type="http",
        description="A test brick",
        when_to_use="Use this for testing.",
    )
    assert schema.name == "test_brick"
    assert schema.type == "http"
    assert schema.description == "A test brick"
    assert schema.when_to_use == "Use this for testing."
    assert schema.category == "general"
    assert schema.config_schema == {}
    assert schema.input_description == ""
    assert schema.output_description == ""


def test_brick_schema_with_config():
    schema = BrickSchema(
        name="my_brick",
        type="cli",
        description="Runs a command",
        when_to_use="When you need a CLI",
        category="cli",
        config_schema={
            "args": BrickParam(type="array", description="Arguments", required=True),
            "timeout": BrickParam(type="string", description="Timeout", default="30s"),
        },
    )
    assert "args" in schema.config_schema
    assert "timeout" in schema.config_schema
    assert schema.config_schema["args"].required is True
    assert schema.config_schema["timeout"].default == "30s"


def test_brick_schema_category():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        category="http",
    )
    assert schema.category == "http"


def test_brick_schema_input_output_descriptions():
    schema = BrickSchema(
        name="x",
        type="file",
        description="d",
        when_to_use="w",
        input_description="A file path",
        output_description="File contents",
    )
    assert schema.input_description == "A file path"
    assert schema.output_description == "File contents"


# ---------------------------------------------------------------------------
# to_json_schema() tests
# ---------------------------------------------------------------------------


def test_to_json_schema_empty():
    schema = BrickSchema(name="x", type="http", description="d", when_to_use="w")
    js = schema.to_json_schema()
    assert js["type"] == "object"
    assert js["properties"] == {}
    assert "required" not in js


def test_to_json_schema_with_required():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "url": BrickParam(type="string", description="The URL", required=True),
            "timeout": BrickParam(type="string", description="Timeout", default="60s"),
        },
    )
    js = schema.to_json_schema()
    assert "url" in js["properties"]
    assert "timeout" in js["properties"]
    assert js["required"] == ["url"]


def test_to_json_schema_no_required_when_none():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "timeout": BrickParam(type="string", description="Timeout", default="60s"),
        },
    )
    js = schema.to_json_schema()
    assert "required" not in js


def test_to_json_schema_property_structure():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "url": BrickParam(type="string", description="The URL to call", required=True),
        },
    )
    js = schema.to_json_schema()
    url_prop = js["properties"]["url"]
    assert url_prop["type"] == "string"
    assert url_prop["description"] == "The URL to call"


def test_to_json_schema_default_in_property():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "timeout": BrickParam(type="string", description="Timeout", default="60s"),
        },
    )
    js = schema.to_json_schema()
    assert js["properties"]["timeout"]["default"] == "60s"


def test_to_json_schema_no_default_when_none():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "url": BrickParam(type="string", description="URL", required=True),
        },
    )
    js = schema.to_json_schema()
    assert "default" not in js["properties"]["url"]


def test_to_json_schema_with_enum():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "method": BrickParam(type="string", description="HTTP method", enum=["GET", "POST"]),
        },
    )
    js = schema.to_json_schema()
    assert js["properties"]["method"]["enum"] == ["GET", "POST"]


def test_to_json_schema_multiple_required():
    schema = BrickSchema(
        name="x",
        type="http",
        description="d",
        when_to_use="w",
        config_schema={
            "url": BrickParam(type="string", description="URL", required=True),
            "content": BrickParam(type="string", description="Body", required=True),
            "timeout": BrickParam(type="string", description="Timeout", default="60s"),
        },
    )
    js = schema.to_json_schema()
    assert set(js["required"]) == {"url", "content"}


# ---------------------------------------------------------------------------
# Built-in brick tests
# ---------------------------------------------------------------------------


def test_all_builtins_count():
    assert len(ALL_BUILTINS) == 10


def test_all_builtins_are_brick_schemas():
    for brick in ALL_BUILTINS:
        assert isinstance(brick, BrickSchema), f"{brick} is not a BrickSchema"


def test_all_builtins_have_when_to_use():
    for brick in ALL_BUILTINS:
        assert brick.when_to_use, f"Brick '{brick.name}' has empty when_to_use"


def test_all_builtins_have_category():
    for brick in ALL_BUILTINS:
        assert brick.category, f"Brick '{brick.name}' has empty category"


def test_all_builtins_have_description():
    for brick in ALL_BUILTINS:
        assert brick.description, f"Brick '{brick.name}' has empty description"


def test_all_builtins_have_unique_names():
    names = [b.name for b in ALL_BUILTINS]
    assert len(names) == len(set(names)), "Duplicate brick names detected"


def test_all_builtins_json_schema_export():
    """Every built-in brick must export a valid JSON Schema dict."""
    for brick in ALL_BUILTINS:
        js = brick.to_json_schema()
        assert isinstance(js, dict), f"Brick '{brick.name}' returned non-dict from to_json_schema()"
        assert js["type"] == "object"
        assert "properties" in js


def test_http_get_schema():
    assert HTTP_GET.name == "http_get"
    assert HTTP_GET.type == "http"
    assert HTTP_GET.category == "http"
    assert "url" in HTTP_GET.config_schema
    assert HTTP_GET.config_schema["url"].required is True
    js = HTTP_GET.to_json_schema()
    assert "url" in js["required"]


def test_http_post_schema():
    assert HTTP_POST.name == "http_post"
    assert HTTP_POST.type == "http"
    js = HTTP_POST.to_json_schema()
    assert "url" in js["required"]
    assert "body" in js["properties"]


def test_run_cli_schema():
    assert RUN_CLI.name == "run_cli"
    assert RUN_CLI.type == "cli"
    assert RUN_CLI.category == "cli"
    assert RUN_CLI.config_schema["args"].type == "array"
    js = RUN_CLI.to_json_schema()
    assert "args" in js["required"]


def test_builtin_names():
    """Verify all 10 expected brick names are present."""
    expected = {
        "http_get", "http_post", "run_cli", "python_script",
        "file_read", "file_write", "mcp_call",
        "filter", "transform", "sub_pipeline",
    }
    actual = {b.name for b in ALL_BUILTINS}
    assert actual == expected


def test_builtin_types():
    """Verify brick types are valid."""
    valid_types = {"http", "cli", "python", "mcp", "filter", "transform", "file", "pipeline"}
    for brick in ALL_BUILTINS:
        assert brick.type in valid_types, f"Brick '{brick.name}' has invalid type '{brick.type}'"


def test_filter_brick():
    from brix.bricks.builtins import FILTER
    assert FILTER.type == "filter"
    assert FILTER.category == "transform"
    assert FILTER.config_schema["where"].required is True
    assert FILTER.config_schema["input"].required is True


def test_transform_brick():
    from brix.bricks.builtins import TRANSFORM
    assert TRANSFORM.type == "transform"
    assert TRANSFORM.config_schema["expression"].required is True


def test_sub_pipeline_brick():
    from brix.bricks.builtins import SUB_PIPELINE
    assert SUB_PIPELINE.type == "pipeline"
    assert SUB_PIPELINE.config_schema["pipeline"].required is True


def test_mcp_call_brick():
    from brix.bricks.builtins import MCP_CALL
    assert MCP_CALL.type == "mcp"
    assert MCP_CALL.config_schema["server"].required is True
    assert MCP_CALL.config_schema["tool"].required is True


def test_file_read_brick():
    from brix.bricks.builtins import FILE_READ
    assert FILE_READ.type == "file"
    assert FILE_READ.config_schema["path"].required is True
    assert FILE_READ.config_schema["binary"].default is False


def test_file_write_brick():
    from brix.bricks.builtins import FILE_WRITE
    assert FILE_WRITE.type == "file"
    js = FILE_WRITE.to_json_schema()
    assert set(js["required"]) == {"path", "content"}
