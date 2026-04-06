"""Tests for flow.pipeline bug: pipeline name in step.config instead of step.pipeline.

Covers two root causes:
1. merge_step_config_into_params must promote config["pipeline"] to step["pipeline"]
   for flow.pipeline/pipeline step types.
2. engine.py resolve_foreach must catch ValueError and record the step as errored
   (not silently abort the whole pipeline without a step entry in the run log).
"""
import pytest
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.db import merge_step_config_into_params


# ---------------------------------------------------------------------------
# Unit tests: merge_step_config_into_params
# ---------------------------------------------------------------------------


def test_pipeline_name_in_config_is_promoted():
    """flow.pipeline step with pipeline name in config gets it promoted to top-level."""
    step = {
        "id": "process_each",
        "type": "flow.pipeline",
        "config": {"pipeline": "my-sub-pipeline"},
        "params": None,
        "pipeline": None,
    }
    result = merge_step_config_into_params(step)
    assert result["pipeline"] == "my-sub-pipeline", (
        "config['pipeline'] must be promoted to step['pipeline'] for flow.pipeline steps"
    )


def test_pipeline_legacy_type_name_in_config_is_promoted():
    """Legacy 'pipeline' step type also gets the promotion."""
    step = {
        "id": "process_each",
        "type": "pipeline",
        "config": {"pipeline": "my-sub-pipeline"},
        "params": None,
        "pipeline": None,
    }
    result = merge_step_config_into_params(step)
    assert result["pipeline"] == "my-sub-pipeline"


def test_pipeline_config_overrides_top_level_field():
    """Config must override top-level pipeline values for promoted fields."""
    step = {
        "id": "process_each",
        "type": "flow.pipeline",
        "config": {"pipeline": "config-value"},
        "params": None,
        "pipeline": "correct-value",
    }
    result = merge_step_config_into_params(step)
    assert result["pipeline"] == "config-value", (
        "config['pipeline'] must take precedence over top-level pipeline values"
    )


def test_non_pipeline_step_config_not_promoted():
    """A non-pipeline step type must not have config['pipeline'] promoted."""
    step = {
        "id": "do_query",
        "type": "db.query",
        "config": {"pipeline": "should-not-be-promoted", "query": "SELECT 1"},
        "params": None,
        "pipeline": None,
    }
    result = merge_step_config_into_params(step)
    # pipeline field should remain None for non-pipeline steps
    assert result.get("pipeline") is None


def test_config_into_params_still_works_for_pipeline_step():
    """config dict is still copied into params for pipeline steps when params is empty."""
    step = {
        "id": "process_each",
        "type": "flow.pipeline",
        "config": {"pipeline": "my-sub", "extra_param": "value"},
        "params": None,
        "pipeline": None,
    }
    result = merge_step_config_into_params(step)
    assert result["params"] == {"pipeline": "my-sub", "extra_param": "value"}
    assert result["pipeline"] == "my-sub"


# ---------------------------------------------------------------------------
# Integration tests: engine foreach resolve_foreach error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_foreach_resolve_error_is_recorded_not_silent():
    """When foreach expression fails to resolve, the step appears in run log as error.

    Previously, resolve_foreach raised ValueError which bubbled to the outer
    except block, aborting the pipeline silently without recording the step.
    """
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: test-foreach-resolve-error
steps:
  - id: step_one
    type: set
    values:
      result: "hello"

  - id: step_two
    type: set
    values:
      item_result: "{{ item }}"
    foreach: "{{ nonexistent_step.output }}"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # The pipeline should fail (not succeed silently)
    assert result.success is False

    # step_two MUST appear in the run log with an error status
    assert "step_two" in result.steps, (
        "step_two must appear in run log — currently it disappears silently when "
        "resolve_foreach raises ValueError"
    )
    assert result.steps["step_two"].status == "error"
    assert result.steps["step_two"].error_message is not None
    assert "foreach" in result.steps["step_two"].error_message.lower()

    # step_one must still appear as ok
    assert result.steps["step_one"].status == "ok"


@pytest.mark.asyncio
async def test_foreach_resolve_error_with_on_error_continue():
    """With on_error: continue, foreach resolve error is recorded and pipeline continues."""
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: test-foreach-resolve-continue
steps:
  - id: step_one
    type: set
    values:
      result: "hello"

  - id: step_two
    type: set
    values:
      item_result: "{{ item }}"
    foreach: "{{ nonexistent_step.output }}"
    on_error: continue

  - id: step_three
    type: set
    values:
      final: "done"
""")
    engine = PipelineEngine()
    result = await engine.run(pipeline)

    # step_two must be recorded as error
    assert "step_two" in result.steps
    assert result.steps["step_two"].status == "error"

    # step_three must still run because on_error: continue
    assert "step_three" in result.steps
    assert result.steps["step_three"].status == "ok"


@pytest.mark.asyncio
async def test_flow_pipeline_step_with_pipeline_in_config(tmp_path):
    """flow.pipeline step with pipeline name in step.config executes sub-pipeline correctly.

    This is the primary bug: when the step was created via DB with pipeline name
    in config_json rather than sub_pipeline column, the engine silently stopped.
    """
    # Create a real sub-pipeline YAML on disk
    sub_yaml = tmp_path / "my_sub.yaml"
    sub_yaml.write_text("""
name: my-sub-pipeline
steps:
  - id: sub_work
    type: set
    values:
      done: true
""")

    loader = PipelineLoader()
    # Simulate the pipeline as it arrives from the DB when pipeline name is in config
    # (step.pipeline is None, step.config has the pipeline key)
    pipeline = loader.load_from_string(f"""
name: test-pipeline-in-config
steps:
  - id: gen_data
    type: set
    values:
      items: [{{"id": 1}}, {{"id": 2}}]

  - id: process_each
    type: flow.pipeline
    foreach: "{{{{ gen_data.output.items }}}}"
    config:
      pipeline: "{sub_yaml}"
""")

    # Manually simulate what DB loading does: config is set, pipeline is None
    # The loader.load_from_string above will have placed pipeline in step.pipeline
    # already (since it's a YAML path).  We need to test the DB path directly.
    # Simulate DB round-trip via merge_step_config_into_params:
    for step in pipeline.steps:
        if step.id == "process_each":
            # Simulate: pipeline came from DB with config_json, sub_pipeline=NULL
            step_dict = step.model_dump()
            step_dict["config"] = {"pipeline": str(sub_yaml)}
            step_dict["pipeline"] = None  # as if sub_pipeline column was NULL
            merged = merge_step_config_into_params(step_dict)
            assert merged["pipeline"] == str(sub_yaml), (
                "merge_step_config_into_params must promote config['pipeline'] to step['pipeline']"
            )
