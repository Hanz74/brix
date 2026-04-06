from brix.engine import _RenderedStep
from brix.loader import PipelineLoader
from brix.models import Step
from brix.runners.python import PythonRunner


async def test_python_runner_uses_rendered_step_params_not_raw_config_params():
    loader = PipelineLoader()
    step = Step(
        id="helper_step",
        type="script.python",
        script="tests/helpers/echo_params.py",
        params={"test_val": "{{ input.x }}"},
        config={"params": {"test_val": "{{ input.x }}"}},
    )

    jinja_ctx = {"input": {"x": "world"}}
    rendered_params = loader.render_step_params(step, jinja_ctx)
    rendered_step = _RenderedStep(step, rendered_params, loader, jinja_ctx)

    runner = PythonRunner()
    result = await runner.execute(rendered_step, context=None)

    assert result["success"] is True
    assert result["data"]["received"]["test_val"] == "world"
    assert result["data"]["received"]["test_val"] != "{{ input.x }}"


async def test_python_runner_unwraps_nested_params_payload_for_helpers():
    loader = PipelineLoader()
    step = Step(
        id="helper_nested",
        type="script.python",
        script="tests/helpers/echo_params.py",
        params={"helper": "demo_helper", "params": {"foo": "bar"}},
    )

    rendered_params = loader.render_step_params(step, {"input": {}})
    rendered_step = _RenderedStep(step, rendered_params, loader, {"input": {}})

    runner = PythonRunner()
    result = await runner.execute(rendered_step, context=None)

    assert result["success"] is True
    assert result["data"]["received"] == {"foo": "bar"}


async def test_python_runner_renders_nested_config_params_for_legacy_config_path():
    loader = PipelineLoader()
    step = Step(
        id="helper_config",
        type="script.python",
        config={
            "script": "tests/helpers/echo_params.py",
            "params": {"test_val": "{{ input.x }}"},
        },
    )

    jinja_ctx = {"input": {"x": "world"}}
    rendered_params = loader.render_step_params(step, jinja_ctx)
    rendered_step = _RenderedStep(step, rendered_params, loader, jinja_ctx)

    runner = PythonRunner()
    result = await runner.execute(rendered_step, context=None)

    assert result["success"] is True
    assert result["data"]["received"]["test_val"] == "world"
