"""Tests for the CLI entry point."""
import json
import os
import tempfile

from click.testing import CliRunner as ClickRunner

from brix.cli import main


def test_cli_version():
    runner = ClickRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "brix" in result.output


def test_cli_run_simple_pipeline():
    """Run a pipeline with a simple echo step."""
    yaml_content = """
name: test-cli
steps:
  - id: echo_step
    type: cli
    args: ["echo", "hello from brix"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path])
        assert result.exit_code == 0
        # Extract JSON object from output (stderr may be mixed in after the JSON block)
        import re
        json_match = re.search(r"(\{.*\})", result.output, re.DOTALL)
        assert json_match, f"No JSON found in output: {result.output!r}"
        output = json.loads(json_match.group(1))
        assert output["success"] is True
        assert "echo_step" in output["steps"]
    finally:
        os.unlink(path)


def test_cli_run_with_params():
    """Parameters are passed as -p key=value."""
    yaml_content = """
name: test-params
input:
  greeting:
    type: str
    default: hello
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "-p", "greeting=hi"])
        assert result.exit_code == 0
    finally:
        os.unlink(path)


def test_cli_run_nonexistent_file():
    runner = ClickRunner()
    result = runner.invoke(main, ["run", "/nonexistent/pipeline.yaml"])
    assert result.exit_code != 0


def test_cli_validate_valid():
    yaml_content = """
name: valid-pipeline
steps:
  - id: s1
    type: cli
    args: ["echo", "hello"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["validate", path])
        assert result.exit_code == 0
        # validate prints to stderr; in click 8.3+ stderr is mixed into output
        assert "VALID" in result.output
    finally:
        os.unlink(path)


def test_cli_validate_invalid():
    """A pipeline with no steps should fail validation at load time."""
    yaml_content = """
name: bad
steps: []
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["validate", path])
        assert result.exit_code != 0
    finally:
        os.unlink(path)


def test_cli_dry_run():
    yaml_content = """
name: dry-test
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
  - id: process
    type: python
    script: helpers/proc.py
    foreach: "{{ fetch.output }}"
    parallel: true
    concurrency: 5
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run"])
        assert result.exit_code == 0
        # Dry run output goes to stderr; in click 8.3+ it is mixed into output
        assert "fetch" in result.output
        assert "process" in result.output
    finally:
        os.unlink(path)
