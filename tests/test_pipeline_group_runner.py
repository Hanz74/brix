"""Tests for PipelineGroupRunner (T-BRIX-V6-17)."""
import pytest
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader


def _write_yaml(path, content):
    with open(path, "w") as f:
        f.write(content)


async def test_pipeline_group_basic(tmp_path):
    """Two sub-pipelines run in parallel and both succeed."""
    for name in ("alpha", "beta"):
        _write_yaml(
            tmp_path / f"{name}.yaml",
            f"""
name: {name}
steps:
  - id: echo_{name}
    type: cli
    args: ["echo", "{name}-done"]
""",
        )

    main_yaml = tmp_path / "main.yaml"
    _write_yaml(
        main_yaml,
        f"""
name: main
steps:
  - id: run_group
    type: pipeline_group
    pipelines:
      - "{tmp_path}/alpha.yaml"
      - "{tmp_path}/beta.yaml"
    concurrency: 2
""",
    )

    pipeline = PipelineLoader().load(str(main_yaml))
    result = await PipelineEngine().run(pipeline)

    assert result.success is True
    assert result.steps["run_group"].status == "ok"


async def test_pipeline_group_shared_params(tmp_path):
    """shared_params are passed to each sub-pipeline."""
    _write_yaml(
        tmp_path / "sub.yaml",
        """
name: sub
steps:
  - id: ok
    type: cli
    args: ["echo", "ok"]
""",
    )

    main_yaml = tmp_path / "main.yaml"
    _write_yaml(
        main_yaml,
        f"""
name: main
steps:
  - id: run_group
    type: pipeline_group
    pipelines:
      - "{tmp_path}/sub.yaml"
    shared_params:
      key: value
    concurrency: 3
""",
    )

    pipeline = PipelineLoader().load(str(main_yaml))
    result = await PipelineEngine().run(pipeline)

    assert result.success is True
    assert result.steps["run_group"].status == "ok"


async def test_pipeline_group_not_found(tmp_path):
    """Non-existent sub-pipeline causes the step to fail."""
    main_yaml = tmp_path / "main.yaml"
    _write_yaml(
        main_yaml,
        """
name: main
steps:
  - id: run_group
    type: pipeline_group
    pipelines:
      - "totally_nonexistent_pipeline_xyz.yaml"
    concurrency: 1
""",
    )

    pipeline = PipelineLoader().load(str(main_yaml))
    result = await PipelineEngine().run(pipeline)

    assert result.success is False


async def test_pipeline_group_empty_pipelines_list(tmp_path):
    """pipeline_group with no pipelines listed returns an error."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(
        """
name: test
steps:
  - id: empty_group
    type: pipeline_group
    pipelines: []
"""
    )
    result = await PipelineEngine().run(pipeline)
    assert result.success is False


async def test_pipeline_group_result_structure(tmp_path):
    """Result data contains results/errors/total/succeeded/failed keys."""
    _write_yaml(
        tmp_path / "ok_pipe.yaml",
        """
name: ok_pipe
steps:
  - id: ok
    type: cli
    args: ["echo", "ok"]
""",
    )

    main_yaml = tmp_path / "main.yaml"
    _write_yaml(
        main_yaml,
        f"""
name: main
steps:
  - id: run_group
    type: pipeline_group
    pipelines:
      - "{tmp_path}/ok_pipe.yaml"
    concurrency: 2
""",
    )

    pipeline = PipelineLoader().load(str(main_yaml))
    result = await PipelineEngine().run(pipeline)

    assert result.success is True
    data = result.result
    # result.result is the output of the last step
    assert isinstance(data, dict)
    assert "results" in data
    assert "errors" in data
    assert data["total"] == 1
    assert data["succeeded"] == 1
    assert data["failed"] == 0


async def test_pipeline_group_concurrency_default(tmp_path):
    """Default concurrency of 3 is used when not specified."""
    for i in range(5):
        _write_yaml(
            tmp_path / f"pipe{i}.yaml",
            f"""
name: pipe{i}
steps:
  - id: echo{i}
    type: cli
    args: ["echo", "p{i}"]
""",
        )

    refs = "\n".join(
        f'      - "{tmp_path}/pipe{i}.yaml"' for i in range(5)
    )
    main_yaml = tmp_path / "main.yaml"
    _write_yaml(
        main_yaml,
        f"""
name: main
steps:
  - id: run_group
    type: pipeline_group
    pipelines:
{refs}
""",
    )

    pipeline = PipelineLoader().load(str(main_yaml))
    result = await PipelineEngine().run(pipeline)

    assert result.success is True
    assert result.steps["run_group"].status == "ok"


async def test_pipeline_group_model_fields():
    """Step model accepts pipeline_group type with new fields."""
    from brix.models import Step
    step = Step(
        id="test_group",
        type="pipeline_group",
        pipelines=["pipe-a", "pipe-b"],
        shared_params={"key": "val"},
        concurrency=5,
    )
    assert step.type == "pipeline_group"
    assert step.pipelines == ["pipe-a", "pipe-b"]
    assert step.shared_params == {"key": "val"}
    assert step.concurrency == 5
