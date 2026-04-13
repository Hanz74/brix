"""Tests for T-BRIX-VAL-11 step output type compatibility checks."""

from unittest.mock import MagicMock, patch

import pytest

from brix.models import Pipeline, Step
from brix.validator import PipelineValidator


def _pipeline(steps):
    return Pipeline(name="val11-pipeline", steps=steps)


def _step(step_id, type="flow.set", **kwargs):
    return Step(id=step_id, type=type, **kwargs)


def _noop(self, *args, **kwargs):
    pass


_HEAVY_CHECKS = [
    "_check_sub_pipeline_existence",
    "_check_connection_existence",
    "_check_brick_config_schema",
    "_check_jinja_ast",
]


@pytest.fixture(autouse=True)
def _patch_heavy_checks():
    patches = [patch.object(PipelineValidator, name, _noop) for name in _HEAVY_CHECKS]
    for item in patches:
        item.start()
    yield
    for item in patches:
        item.stop()


def _brick_getter(output_types):
    def _get(name, _seen=None):
        output_type = output_types.get(name)
        if not output_type:
            return None
        brick = MagicMock()
        brick.output_type = output_type
        return brick

    return _get


def test_foreach_on_dict_output_step_warns():
    steps = [
        _step("source", type="flow.set"),
        _step("loop", type="flow.set", foreach="{{ source.output }}"),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "flow.set": "dict",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-06" in warning]
    assert len(warnings) == 1
    assert "source" in warnings[0]
    assert "dict" in warnings[0]


def test_foreach_on_list_output_step_has_no_warning():
    steps = [
        _step("source", type="db.query"),
        _step("loop", type="flow.set", foreach="{{ source.output }}"),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "db.query": "list[dict]",
        "flow.set": "dict",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-06" in warning]
    assert warnings == []


def test_db_exec_with_dict_params_has_no_warning():
    steps = [
        _step("source", type="flow.set"),
        _step(
            "write",
            type="db.exec",
            config={"connection": "main", "query": "UPDATE t SET x = ?", "params": "{{ source.output }}"},
        ),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "flow.set": "dict",
        "db.exec": "dict",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning]
    assert warnings == []


def test_db_query_with_list_params_warns():
    steps = [
        _step("source", type="db.query"),
        _step(
            "read",
            type="db.query",
            config={"connection": "main", "query": "SELECT * FROM t WHERE id = :id", "params": "{{ source.output }}"},
        ),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "db.query": "list[dict]",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning]
    assert len(warnings) == 1
    assert "db.query params" in warnings[0]
    assert "list[dict]" in warnings[0]


def test_db_upsert_with_single_dict_data_warns():
    steps = [
        _step("source", type="flow.set"),
        _step(
            "upsert",
            type="db.upsert",
            params={"data": "{{ source.output }}"},
        ),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "flow.set": "dict",
        "db.upsert": "dict",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning]
    assert len(warnings) == 1
    assert "db.upsert data" in warnings[0]
    assert "dict" in warnings[0]


def test_flow_filter_with_dict_input_warns():
    steps = [
        _step("source", type="flow.set"),
        _step(
            "filter",
            type="flow.filter",
            params={"input": "{{ source.output }}"},
        ),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "flow.set": "dict",
        "flow.filter": "list",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning]
    assert len(warnings) == 1
    assert "flow.filter input" in warnings[0]
    assert "dict" in warnings[0]


def test_flow_merge_with_templated_dict_input_warns():
    steps = [
        _step("source", type="flow.set"),
        _step("merge", type="flow.merge", inputs=["{{ source.output }}"]),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "flow.set": "dict",
        "flow.merge": "list",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning]
    assert len(warnings) == 1
    assert "flow.merge inputs" in warnings[0]
    assert "dict" in warnings[0]
    assert "list of lists" in warnings[0]


def test_flow_merge_with_list_inputs_has_no_warning():
    steps = [
        _step("left", type="source.fetch"),
        _step("right", type="flow.flatten"),
        _step("merge", type="flow.merge", inputs=["{{ left.output }}", "{{ right.output }}"]),
    ]

    with patch("brix.bricks.registry.BrickRegistry.get", side_effect=_brick_getter({
        "source.fetch": "list",
        "flow.flatten": "list",
        "flow.merge": "list",
    })):
        result = PipelineValidator().validate(_pipeline(steps), level="standard")

    warnings = [warning for warning in result.warnings if "T-BRIX-VAL-11" in warning]
    assert warnings == []
