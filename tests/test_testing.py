"""Tests for the pipeline testing framework."""
import pytest
import yaml

from brix.testing import TestFixture, PipelineTestRunner, MockRunner, _SelectiveMockRunner


def _write_file(path, content):
    with open(path, "w") as f:
        f.write(content)


async def test_mock_runner():
    """MockRunner returns pre-defined data."""
    runner = MockRunner(mocks={"step1": {"key": "value"}})

    class FakeStep:
        id = "step1"

    result = await runner.execute(FakeStep(), context=None)
    assert result["success"] is True
    assert result["data"] == {"key": "value"}


async def test_mock_runner_unknown_step():
    runner = MockRunner(mocks={})

    class FakeStep:
        id = "unknown"

    result = await runner.execute(FakeStep(), context=None)
    assert result["success"] is False


def test_fixture_load(tmp_path):
    fixture_content = {
        "pipeline": "test.yaml",
        "description": "Test fixture",
        "input": {"query": "test"},
        "mocks": {"fetch": [1, 2, 3]},
        "assertions": {"fetch": [{"status": "ok"}]},
    }
    fixture_path = tmp_path / "test.test.yaml"
    fixture_path.write_text(yaml.dump(fixture_content))

    fx = TestFixture.load(str(fixture_path))
    assert fx.pipeline_path == "test.yaml"
    assert fx.input_data == {"query": "test"}
    assert fx.mocks == {"fetch": [1, 2, 3]}
    assert fx.description == "Test fixture"


async def test_pipeline_test_runner(tmp_path):
    """Full pipeline test with mocks."""
    pipeline_yaml = tmp_path / "test.yaml"
    _write_file(
        pipeline_yaml,
        """
name: test-pipeline
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
  - id: process
    type: cli
    args: ["echo", "processed"]
""",
    )

    fx = TestFixture(
        pipeline_path=str(pipeline_yaml),
        mocks={
            "fetch": {"messages": [1, 2, 3]},
            "process": {"output": "processed"},
        },
        assertions={"process": [{"status": "ok"}]},
    )

    runner = PipelineTestRunner()
    result = await runner.run_test(fx)

    assert result["summary"]["steps_passed"] == 2
    assert result["summary"]["steps_total"] == 2


async def test_selective_mock_runner():
    """Mocked steps return mock data, others use real runner."""
    from brix.runners.cli import CliRunner

    mock = _SelectiveMockRunner(
        mocks={"mocked_step": {"data": "from mock"}},
        real_runners={"cli": CliRunner()},
    )

    class MockedStep:
        id = "mocked_step"
        type = "cli"

    result = await mock.execute(MockedStep(), context=None)
    assert result["success"] is True
    assert result["data"] == {"data": "from mock"}


async def test_selective_mock_runner_falls_through_to_real(tmp_path):
    """Unmocked steps use the real runner."""
    from brix.runners.cli import CliRunner

    mock = _SelectiveMockRunner(
        mocks={},
        real_runners={"cli": CliRunner()},
    )

    class RealStep:
        id = "real_step"
        type = "cli"
        args = ["echo", "hello"]
        command = None
        timeout = None
        shell = False

    result = await mock.execute(RealStep(), context=None)
    assert result["success"] is True


async def test_assertion_status_pass(tmp_path):
    """Assertion on step status passes when status matches."""
    pipeline_yaml = tmp_path / "pipeline.yaml"
    _write_file(
        pipeline_yaml,
        """
name: assert-test
steps:
  - id: step1
    type: mcp
    server: fake
    tool: fake-tool
""",
    )

    fx = TestFixture(
        pipeline_path=str(pipeline_yaml),
        mocks={"step1": {"result": "ok"}},
        assertions={"step1": [{"status": "ok"}]},
    )

    runner = PipelineTestRunner()
    result = await runner.run_test(fx)

    assert result["summary"]["assertions_passed"] == 1
    assert result["summary"]["assertions_total"] == 1
    assert result["success"] is True


async def test_assertion_status_fail(tmp_path):
    """Assertion fails when expected status doesn't match actual."""
    pipeline_yaml = tmp_path / "pipeline.yaml"
    _write_file(
        pipeline_yaml,
        """
name: assert-fail-test
steps:
  - id: step1
    type: mcp
    server: fake
    tool: fake-tool
""",
    )

    fx = TestFixture(
        pipeline_path=str(pipeline_yaml),
        mocks={"step1": {"result": "ok"}},
        assertions={"step1": [{"status": "error"}]},  # expects error, but step succeeds
    )

    runner = PipelineTestRunner()
    result = await runner.run_test(fx)

    assert result["summary"]["assertions_passed"] == 0
    assert result["success"] is False


def test_fixture_defaults():
    """TestFixture has sane defaults for optional fields."""
    fx = TestFixture(pipeline_path="some.yaml")
    assert fx.input_data == {}
    assert fx.mocks == {}
    assert fx.assertions == {}
    assert fx.description == ""
