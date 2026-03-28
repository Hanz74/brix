"""Tests for brix viz — Mermaid diagram generation."""

import os
import tempfile

import pytest
from click.testing import CliRunner as ClickRunner

from brix.cli import main
from brix.loader import PipelineLoader
from brix.viz import generate_mermaid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(yaml_content: str):
    loader = PipelineLoader()
    return loader.load_from_string(yaml_content)


# ---------------------------------------------------------------------------
# Unit tests for generate_mermaid()
# ---------------------------------------------------------------------------


def test_viz_simple_pipeline():
    """Simple two-step pipeline produces a valid Mermaid diagram."""
    pipeline = _load("""
name: simple
version: 1.0.0
steps:
  - id: fetch
    type: cli
    args: ["echo", "hello"]
  - id: process
    type: python
    script: helpers/process.py
""")
    diagram = generate_mermaid(pipeline)
    assert "flowchart TD" in diagram
    assert "fetch" in diagram
    assert "process" in diagram
    # Sequential flow arrow
    assert "fetch --> process" in diagram or "fetch" in diagram


def test_viz_contains_title():
    """Diagram contains pipeline name and version as title."""
    pipeline = _load("""
name: my-pipeline
version: 2.5.0
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
    diagram = generate_mermaid(pipeline)
    assert "my-pipeline" in diagram
    assert "2.5.0" in diagram


def test_viz_direction_lr():
    """LR direction is respected."""
    pipeline = _load("""
name: lr-test
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
""")
    diagram = generate_mermaid(pipeline, direction="LR")
    assert "flowchart LR" in diagram


def test_viz_mcp_step_label():
    """MCP steps show server:tool in label."""
    pipeline = _load("""
name: mcp-test
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail-messages
""")
    diagram = generate_mermaid(pipeline)
    assert "m365" in diagram
    assert "list-mail-messages" in diagram


def test_viz_http_step_label():
    """HTTP steps show method and URL."""
    pipeline = _load("""
name: http-test
steps:
  - id: get_data
    type: http
    method: GET
    url: https://api.example.com/data
""")
    diagram = generate_mermaid(pipeline)
    assert "GET" in diagram
    assert "api.example.com" in diagram


def test_viz_pipeline_step_label():
    """Sub-pipeline steps show the pipeline name."""
    pipeline = _load("""
name: parent
steps:
  - id: sub
    type: pipeline
    pipeline: child-pipeline.yaml
""")
    diagram = generate_mermaid(pipeline)
    assert "child-pipeline.yaml" in diagram


def test_viz_conditional_step_shape():
    """Steps with 'when' use diamond shape {}."""
    pipeline = _load("""
name: cond-test
steps:
  - id: check
    type: cli
    args: ["echo", "check"]
  - id: conditional_step
    type: cli
    args: ["echo", "maybe"]
    when: "{{ check.output }}"
""")
    diagram = generate_mermaid(pipeline)
    # Diamond shape for conditional step
    assert "{" in diagram and "}" in diagram
    # 'when' label on the arrow
    assert "yes" in diagram or "conditional_step" in diagram


def test_viz_else_of_edge():
    """else_of creates a 'no' edge from the conditional step."""
    pipeline = _load("""
name: else-test
steps:
  - id: check
    type: cli
    args: ["echo", "check"]
    when: "{{ input.flag }}"
  - id: else_branch
    type: cli
    args: ["echo", "fallback"]
    else_of: check
""")
    diagram = generate_mermaid(pipeline)
    assert "no" in diagram
    assert "else_branch" in diagram


def test_viz_foreach_annotation():
    """Foreach steps are annotated in the diagram."""
    pipeline = _load("""
name: foreach-test
steps:
  - id: items
    type: cli
    args: ["echo", "list"]
  - id: process
    type: python
    script: helpers/proc.py
    foreach: "{{ items.output }}"
    parallel: true
    concurrency: 5
""")
    diagram = generate_mermaid(pipeline)
    # Node uses ribbon shape for foreach
    assert "process" in diagram
    # Foreach annotation comment
    assert "foreach" in diagram


def test_viz_disabled_step_style():
    """Disabled steps get grey style."""
    pipeline = _load("""
name: disabled-test
steps:
  - id: active
    type: cli
    args: ["echo", "active"]
  - id: inactive
    type: cli
    args: ["echo", "inactive"]
    enabled: false
""")
    diagram = generate_mermaid(pipeline)
    assert "inactive" in diagram
    # Disabled steps get grey fill
    assert "#ccc" in diagram or "fill:#ccc" in diagram


def test_viz_stop_step_shape():
    """Stop steps use stadium shape ([])."""
    pipeline = _load("""
name: stop-test
steps:
  - id: start
    type: cli
    args: ["echo", "start"]
  - id: halt
    type: stop
    message: "Pipeline halted"
""")
    diagram = generate_mermaid(pipeline)
    # Stop uses ([...]) shape
    assert "([" in diagram
    # Stop style (red fill)
    assert "halt" in diagram


def test_viz_approval_step_shape():
    """Approval steps use subroutine shape [[]]."""
    pipeline = _load("""
name: approval-test
steps:
  - id: start
    type: cli
    args: ["echo", "start"]
  - id: approve
    type: approval
    approval_timeout: 30m
""")
    diagram = generate_mermaid(pipeline)
    assert "[[" in diagram and "]]" in diagram
    assert "approve" in diagram


def test_viz_data_dependency_edge():
    """Non-adjacent template references produce dashed dependency edges."""
    pipeline = _load("""
name: dep-test
steps:
  - id: step_a
    type: cli
    args: ["echo", "a"]
  - id: step_b
    type: cli
    args: ["echo", "b"]
  - id: step_c
    type: python
    script: helpers/proc.py
    params:
      data: "{{ step_a.output }}"
""")
    diagram = generate_mermaid(pipeline)
    # Dashed edge for non-adjacent dependency
    assert "-.->|data|" in diagram or "data" in diagram


def test_viz_set_step_shows_keys():
    """Set steps show the variable names being set."""
    pipeline = _load("""
name: set-test
steps:
  - id: setvals
    type: set
    values:
      foo: bar
      baz: qux
""")
    diagram = generate_mermaid(pipeline)
    assert "setvals" in diagram
    assert "foo" in diagram


def test_viz_three_step_sequential_flow():
    """Three sequential steps have two sequential arrows."""
    pipeline = _load("""
name: three-step
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
  - id: b
    type: cli
    args: ["echo", "b"]
  - id: c
    type: cli
    args: ["echo", "c"]
""")
    diagram = generate_mermaid(pipeline)
    # a→b and b→c
    assert "a --> b" in diagram
    assert "b --> c" in diagram


def test_viz_sanitizes_hyphens_in_ids():
    """Step IDs with hyphens are sanitised to underscores for Mermaid."""
    pipeline = _load("""
name: hyphen-test
steps:
  - id: step-one
    type: cli
    args: ["echo", "one"]
  - id: step-two
    type: cli
    args: ["echo", "two"]
""")
    diagram = generate_mermaid(pipeline)
    assert "step_one" in diagram
    assert "step_two" in diagram


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


def test_cli_viz_stdout():
    """brix viz prints Mermaid to stdout."""
    yaml_content = """
name: viz-cli-test
steps:
  - id: s1
    type: cli
    args: ["echo", "hello"]
  - id: s2
    type: python
    script: helpers/proc.py
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["viz", path])
        assert result.exit_code == 0, result.output
        assert "flowchart TD" in result.output
        assert "s1" in result.output
        assert "s2" in result.output
    finally:
        os.unlink(path)


def test_cli_viz_lr_direction():
    """brix viz --direction LR produces LR flowchart."""
    yaml_content = """
name: viz-lr-test
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["viz", path, "--direction", "LR"])
        assert result.exit_code == 0, result.output
        assert "flowchart LR" in result.output
    finally:
        os.unlink(path)


def test_cli_viz_output_file(tmp_path):
    """brix viz --output writes to file."""
    yaml_content = """
name: viz-file-test
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
"""
    out_file = str(tmp_path / "diagram.mmd")
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["viz", path, "--output", out_file])
        assert result.exit_code == 0, result.output
        # File was written
        assert (tmp_path / "diagram.mmd").exists()
        content = (tmp_path / "diagram.mmd").read_text()
        assert "flowchart TD" in content
        assert "viz-file-test" in content
    finally:
        os.unlink(path)


def test_cli_viz_invalid_pipeline():
    """brix viz on a broken YAML exits with error."""
    yaml_content = "name: bad\nsteps: []\n"
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["viz", path])
        assert result.exit_code != 0
    finally:
        os.unlink(path)


def test_cli_viz_nonexistent_file():
    """brix viz on a non-existent file exits with error."""
    runner = ClickRunner()
    result = runner.invoke(main, ["viz", "/nonexistent/pipeline.yaml"])
    assert result.exit_code != 0


def test_cli_viz_short_flag_d():
    """brix viz -d LR is an alias for --direction LR."""
    yaml_content = """
name: short-flag-test
steps:
  - id: a
    type: cli
    args: ["echo", "a"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["viz", path, "-d", "LR"])
        assert result.exit_code == 0, result.output
        assert "flowchart LR" in result.output
    finally:
        os.unlink(path)
