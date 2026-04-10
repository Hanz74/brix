"""Tests for extended validator checks T-BRIX-VAL-01 through T-BRIX-VAL-10."""

from unittest.mock import patch, MagicMock
import pytest

from brix.models import Pipeline, Step
from brix.validator import PipelineValidator


def _pipeline(steps, **kwargs):
    """Helper to create a minimal Pipeline with given steps."""
    return Pipeline(name="test-pipeline", steps=steps, **kwargs)


def _step(id, type="flow.set", **kwargs):
    """Helper to create a minimal Step."""
    return Step(id=id, type=type, **kwargs)


def _validate_quick(pipeline):
    """Validate at quick level to skip brick-schema/sub-pipeline/connection checks."""
    v = PipelineValidator()
    # We run at standard but patch out the heavy checks that need real DB/registry
    return v.validate(pipeline, level="standard")


def _noop(self, *a, **kw):
    pass


# Patch out checks that require real DB access for all tests in this module
_HEAVY_CHECKS = [
    "_check_sub_pipeline_existence",
    "_check_connection_existence",
    "_check_brick_config_schema",
    "_check_jinja_ast",
]


@pytest.fixture(autouse=True)
def _patch_heavy_checks():
    """Disable heavy DB-dependent checks for all tests."""
    patches = [patch.object(PipelineValidator, name, _noop) for name in _HEAVY_CHECKS]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


class TestVAL01MissingOutput:
    """T-BRIX-VAL-01: Missing .output in step references."""

    def test_warns_on_missing_output(self):
        steps = [
            _step("fetch", type="http.request"),
            _step("process", type="flow.transform", params={"data": "{{ fetch.items }}"}),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val01_warnings = [w for w in result.warnings if "T-BRIX-VAL-01" in w]
        assert len(val01_warnings) == 1
        assert "fetch.items" in val01_warnings[0]
        assert "fetch.output.items" in val01_warnings[0]

    def test_no_warning_when_output_present(self):
        steps = [
            _step("fetch", type="http.request"),
            _step("process", type="flow.transform", params={"data": "{{ fetch.output.items }}"}),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val01_warnings = [w for w in result.warnings if "T-BRIX-VAL-01" in w]
        assert len(val01_warnings) == 0

    def test_ignores_input_references(self):
        steps = [
            _step("process", type="flow.transform", params={"data": "{{ input.query }}"}),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val01_warnings = [w for w in result.warnings if "T-BRIX-VAL-01" in w]
        assert len(val01_warnings) == 0


class TestVAL02TojsonOnString:
    """T-BRIX-VAL-02: tojson on already-string values."""

    def test_warns_on_tojson_for_string_output(self):
        steps = [
            _step("render", type="flow.transform"),
            _step("use", type="flow.set", params={"val": "{{ render.output | tojson }}"}),
        ]
        mock_brick = MagicMock()
        mock_brick.output_type = "string"

        with patch("brix.bricks.registry.BrickRegistry.get", return_value=mock_brick):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val02_warnings = [w for w in result.warnings if "T-BRIX-VAL-02" in w]
        assert len(val02_warnings) == 1
        assert "tojson" in val02_warnings[0]

    def test_no_warning_for_list_output(self):
        steps = [
            _step("fetch", type="db.query"),
            _step("use", type="flow.set", params={"val": "{{ fetch.output | tojson }}"}),
        ]
        mock_brick = MagicMock()
        mock_brick.output_type = "list"

        with patch("brix.bricks.registry.BrickRegistry.get", return_value=mock_brick):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val02_warnings = [w for w in result.warnings if "T-BRIX-VAL-02" in w]
        assert len(val02_warnings) == 0


class TestVAL03HelperWithoutCode:
    """T-BRIX-VAL-03: Helper without code."""

    def test_warns_on_helper_without_code(self):
        steps = [_step("run", type="script.python", helper="my_helper")]
        mock_entry = MagicMock()
        mock_entry.code = ""
        mock_entry.input_schema = {}

        with patch("brix.helper_registry.HelperRegistry.get", return_value=mock_entry):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val03_warnings = [w for w in result.warnings if "T-BRIX-VAL-03" in w]
        assert len(val03_warnings) == 1
        assert "has no code" in val03_warnings[0]

    def test_no_warning_when_code_exists(self):
        steps = [_step("run", type="script.python", helper="my_helper")]
        mock_entry = MagicMock()
        mock_entry.code = "print('hello')"
        mock_entry.input_schema = {}
        mock_entry.imports = []

        with patch("brix.helper_registry.HelperRegistry.get", return_value=mock_entry):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val03_warnings = [w for w in result.warnings if "T-BRIX-VAL-03" in w]
        assert len(val03_warnings) == 0


class TestVAL04UnusedSteps:
    """T-BRIX-VAL-04: Unused steps."""

    def test_info_on_unused_step(self):
        steps = [
            _step("unused", type="flow.set", values={"x": 1}),
            _step("used", type="flow.transform", params={"data": "hello"}),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val04_infos = [i for i in result.infos if "T-BRIX-VAL-04" in i]
        assert len(val04_infos) == 1
        assert "unused" in val04_infos[0]

    def test_no_info_when_step_referenced(self):
        steps = [
            _step("fetch", type="flow.set", values={"x": 1}),
            _step("use", type="flow.transform", params={"data": "{{ fetch.output }}"}),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val04_infos = [i for i in result.infos if "T-BRIX-VAL-04" in i]
        assert len(val04_infos) == 0

    def test_no_info_for_side_effect_steps(self):
        steps = [
            _step("notify", type="action.notify", params={"msg": "hi"}),
            _step("last", type="flow.set", values={"x": 1}),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val04_infos = [i for i in result.infos if "T-BRIX-VAL-04" in i]
        assert len(val04_infos) == 0


class TestVAL05SubPipelineOutputMismatch:
    """T-BRIX-VAL-05: Output-schema mismatch between sub-pipeline and parent."""

    def test_warns_on_missing_output_key(self):
        sub_pipeline = _pipeline(
            [_step("s1", type="flow.set")],
            output={"result": "{{ s1.output }}"},
        )
        sub_pipeline.name = "sub-pipe"

        steps = [
            _step("sub", type="flow.pipeline", pipeline="sub-pipe"),
            _step("use", type="flow.transform", params={"data": "{{ sub.output.missing_key }}"}),
        ]

        with patch("brix.pipeline_store.PipelineStore.load", return_value=sub_pipeline):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val05_warnings = [w for w in result.warnings if "T-BRIX-VAL-05" in w]
        assert len(val05_warnings) == 1
        assert "missing_key" in val05_warnings[0]

    def test_no_warning_when_key_exists(self):
        sub_pipeline = _pipeline(
            [_step("s1", type="flow.set")],
            output={"data": "{{ s1.output }}"},
        )
        sub_pipeline.name = "sub-pipe"

        steps = [
            _step("sub", type="flow.pipeline", pipeline="sub-pipe"),
            _step("use", type="flow.transform", params={"val": "{{ sub.output.data }}"}),
        ]

        with patch("brix.pipeline_store.PipelineStore.load", return_value=sub_pipeline):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val05_warnings = [w for w in result.warnings if "T-BRIX-VAL-05" in w]
        assert len(val05_warnings) == 0


class TestVAL06ForeachOnNonList:
    """T-BRIX-VAL-06: foreach on non-list expression."""

    def test_warns_on_dict_output_type(self):
        steps = [
            _step("transform", type="flow.transform"),
            _step("loop", type="flow.set", foreach="{{ transform.output }}"),
        ]
        mock_brick = MagicMock()
        mock_brick.output_type = "dict"

        with patch("brix.bricks.registry.BrickRegistry.get", return_value=mock_brick):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val06_warnings = [w for w in result.warnings if "T-BRIX-VAL-06" in w]
        assert len(val06_warnings) == 1
        assert "dict" in val06_warnings[0]

    def test_no_warning_on_list_output_type(self):
        steps = [
            _step("query", type="db.query"),
            _step("loop", type="flow.set", foreach="{{ query.output }}"),
        ]
        mock_brick = MagicMock()
        mock_brick.output_type = "list"

        with patch("brix.bricks.registry.BrickRegistry.get", return_value=mock_brick):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val06_warnings = [w for w in result.warnings if "T-BRIX-VAL-06" in w]
        assert len(val06_warnings) == 0


class TestD16ConditionalDefault:
    """D-16: conditional-step references should respect explicit guards."""

    def test_no_warning_when_reference_is_guarded_by_when_defined(self):
        steps = [
            _step("read_file_b64", type="file.read_base64", when="{{ download.output.extractable | default(false) }}"),
            _step(
                "extract",
                type="flow.pipeline",
                params={"base64_data": "{{ read_file_b64.output.base64 }}"},
                when="{{ read_file_b64.output is defined }}",
            ),
        ]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))

        d16_warnings = [w for w in result.warnings if "D-16" in w]
        assert d16_warnings == []


class TestVAL07DbQueryDML:
    """T-BRIX-VAL-07: db.query used for DML."""

    def test_warns_on_update(self):
        steps = [_step("q", type="db.query", query="UPDATE users SET active=false WHERE id=1")]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val07_warnings = [w for w in result.warnings if "T-BRIX-VAL-07" in w]
        assert len(val07_warnings) == 1
        assert "UPDATE" in val07_warnings[0]

    def test_warns_on_delete(self):
        steps = [_step("q", type="db.query", query="DELETE FROM users WHERE id=1")]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val07_warnings = [w for w in result.warnings if "T-BRIX-VAL-07" in w]
        assert len(val07_warnings) == 1
        assert "DELETE" in val07_warnings[0]

    def test_warns_on_insert(self):
        steps = [_step("q", type="db.query", query="INSERT INTO users (name) VALUES ('x')")]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val07_warnings = [w for w in result.warnings if "T-BRIX-VAL-07" in w]
        assert len(val07_warnings) == 1
        assert "INSERT" in val07_warnings[0]

    def test_no_warning_on_select(self):
        steps = [_step("q", type="db.query", query="SELECT * FROM users")]
        v = PipelineValidator()
        result = v.validate(_pipeline(steps))
        val07_warnings = [w for w in result.warnings if "T-BRIX-VAL-07" in w]
        assert len(val07_warnings) == 0


class TestVAL08DuplicateIdsAcrossSubPipelines:
    """T-BRIX-VAL-08: Duplicate step IDs across sub-pipelines."""

    def test_warns_on_collision(self):
        sub_pipeline = _pipeline([
            _step("shared_id", type="flow.set"),
        ])
        sub_pipeline.name = "sub-pipe"

        steps = [
            _step("shared_id", type="flow.set"),
            _step("sub", type="flow.pipeline", pipeline="sub-pipe"),
        ]

        with patch("brix.pipeline_store.PipelineStore.load", return_value=sub_pipeline):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val08_warnings = [w for w in result.warnings if "T-BRIX-VAL-08" in w]
        assert len(val08_warnings) == 1
        assert "shared_id" in val08_warnings[0]

    def test_no_warning_on_unique_ids(self):
        sub_pipeline = _pipeline([
            _step("sub_step", type="flow.set"),
        ])
        sub_pipeline.name = "sub-pipe"

        steps = [
            _step("parent_step", type="flow.set"),
            _step("sub", type="flow.pipeline", pipeline="sub-pipe"),
        ]

        with patch("brix.pipeline_store.PipelineStore.load", return_value=sub_pipeline):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val08_warnings = [w for w in result.warnings if "T-BRIX-VAL-08" in w]
        assert len(val08_warnings) == 0


class TestVAL09LargeHelperWithoutSchema:
    """T-BRIX-VAL-09: Large helper without input_schema."""

    def test_warns_on_large_helper_no_schema(self):
        steps = [_step("run", type="script.python", helper="big_helper")]
        mock_entry = MagicMock()
        mock_entry.code = "x" * 600  # > 500 chars
        mock_entry.input_schema = {}  # No properties
        mock_entry.imports = []

        with patch("brix.helper_registry.HelperRegistry.get", return_value=mock_entry), \
             patch("brix.helper_registry.HelperRegistry.list_all", return_value=[]):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val09_warnings = [w for w in result.warnings if "T-BRIX-VAL-09" in w]
        assert len(val09_warnings) == 1
        assert "600 chars" in val09_warnings[0]

    def test_no_warning_when_schema_present(self):
        steps = [_step("run", type="script.python", helper="big_helper")]
        mock_entry = MagicMock()
        mock_entry.code = "x" * 600
        mock_entry.input_schema = {"properties": {"param1": {"type": "string"}}}
        mock_entry.imports = []

        with patch("brix.helper_registry.HelperRegistry.get", return_value=mock_entry), \
             patch("brix.helper_registry.HelperRegistry.list_all", return_value=[]):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val09_warnings = [w for w in result.warnings if "T-BRIX-VAL-09" in w]
        assert len(val09_warnings) == 0

    def test_no_warning_when_small_helper(self):
        steps = [_step("run", type="script.python", helper="small_helper")]
        mock_entry = MagicMock()
        mock_entry.code = "print('hi')"  # < 500 chars
        mock_entry.input_schema = {}
        mock_entry.imports = []

        with patch("brix.helper_registry.HelperRegistry.get", return_value=mock_entry), \
             patch("brix.helper_registry.HelperRegistry.list_all", return_value=[]):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val09_warnings = [w for w in result.warnings if "T-BRIX-VAL-09" in w]
        assert len(val09_warnings) == 0


class TestVAL10CrossHelperImports:
    """T-BRIX-VAL-10: Cross-helper imports without imports field."""

    def test_warns_on_undeclared_import(self):
        steps = [_step("run", type="script.python", helper="main_helper")]

        main_entry = MagicMock()
        main_entry.name = "main_helper"
        main_entry.code = "from utils_helper import parse\nresult = parse(data)"
        main_entry.imports = []  # Does NOT declare utils_helper
        main_entry.input_schema = {}

        utils_entry = MagicMock()
        utils_entry.name = "utils_helper"

        with patch("brix.helper_registry.HelperRegistry.get", return_value=main_entry), \
             patch("brix.helper_registry.HelperRegistry.list_all", return_value=[main_entry, utils_entry]):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val10_warnings = [w for w in result.warnings if "T-BRIX-VAL-10" in w]
        assert len(val10_warnings) == 1
        assert "utils_helper" in val10_warnings[0]

    def test_no_warning_when_import_declared(self):
        steps = [_step("run", type="script.python", helper="main_helper")]

        main_entry = MagicMock()
        main_entry.name = "main_helper"
        main_entry.code = "from utils_helper import parse\nresult = parse(data)"
        main_entry.imports = ["utils_helper"]  # Properly declared
        main_entry.input_schema = {}

        utils_entry = MagicMock()
        utils_entry.name = "utils_helper"

        with patch("brix.helper_registry.HelperRegistry.get", return_value=main_entry), \
             patch("brix.helper_registry.HelperRegistry.list_all", return_value=[main_entry, utils_entry]):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val10_warnings = [w for w in result.warnings if "T-BRIX-VAL-10" in w]
        assert len(val10_warnings) == 0

    def test_ignores_stdlib_imports(self):
        steps = [_step("run", type="script.python", helper="main_helper")]

        main_entry = MagicMock()
        main_entry.name = "main_helper"
        main_entry.code = "import json\nimport os\nfrom sys import argv"
        main_entry.imports = []
        main_entry.input_schema = {}

        with patch("brix.helper_registry.HelperRegistry.get", return_value=main_entry), \
             patch("brix.helper_registry.HelperRegistry.list_all", return_value=[main_entry]):
            v = PipelineValidator()
            result = v.validate(_pipeline(steps))

        val10_warnings = [w for w in result.warnings if "T-BRIX-VAL-10" in w]
        assert len(val10_warnings) == 0
