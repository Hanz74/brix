"""Tests for sub-pipeline runner."""
import pytest
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader


def _write_yaml(path, content):
    with open(path, 'w') as f:
        f.write(content)


async def test_sub_pipeline_basic(tmp_path):
    """Sub-pipeline executes and returns result."""
    # Create sub-pipeline
    sub_yaml = tmp_path / "sub.yaml"
    _write_yaml(sub_yaml, """
name: sub-pipeline
steps:
  - id: echo_sub
    type: cli
    args: ["echo", "from-sub"]
""")

    # Create main pipeline referencing sub
    main_yaml = tmp_path / "main.yaml"
    _write_yaml(main_yaml, f"""
name: main-pipeline
steps:
  - id: call_sub
    type: pipeline
    pipeline: "{sub_yaml}"
""")

    loader = PipelineLoader()
    pipeline = loader.load(str(main_yaml))
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    assert result.success is True
    assert result.steps["call_sub"].status == "ok"


async def test_sub_pipeline_not_found():
    """Non-existent sub-pipeline returns error."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: test
steps:
  - id: missing
    type: pipeline
    pipeline: "nonexistent.yaml"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)
    assert result.success is False


async def test_sub_pipeline_no_ref():
    """Pipeline step without pipeline field returns error."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: test
steps:
  - id: no_ref
    type: pipeline
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)
    assert result.success is False


async def test_sub_pipeline_with_params(tmp_path):
    """Sub-pipeline receives params from parent."""
    sub_yaml = tmp_path / "sub.yaml"
    _write_yaml(sub_yaml, """
name: sub
steps:
  - id: echo_param
    type: cli
    args: ["echo", "ok"]
""")

    main_yaml = tmp_path / "main.yaml"
    _write_yaml(main_yaml, f"""
name: main
steps:
  - id: call_sub
    type: pipeline
    pipeline: "{sub_yaml}"
    params:
      key: value
""")

    loader = PipelineLoader()
    pipeline = loader.load(str(main_yaml))
    engine = PipelineEngine()
    result = await engine.run(pipeline)
    assert result.success is True
