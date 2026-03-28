"""Tests for v1 backward compatibility in v2."""
import json
import os
import re
import subprocess
import tempfile
import pytest
from pathlib import Path
from click.testing import CliRunner as ClickRunner

from brix.cli import main
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.models import Pipeline, Step
from brix.history import RunHistory
from brix.context import PipelineContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_json(output: str) -> dict:
    """Extract first JSON object from mixed stdout/stderr CLI output."""
    match = re.search(r"(\{.*\})", output, re.DOTALL)
    assert match, f"No JSON found in output: {output!r}"
    return json.loads(match.group(1))


# ---------------------------------------------------------------------------
# CLI backward compatibility
# ---------------------------------------------------------------------------

class TestCliBackwardCompat:
    """v1 CLI commands still work in v2."""

    def test_brix_version(self):
        runner = ClickRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "brix" in result.output

    def test_brix_run_simple_pipeline(self, tmp_path):
        """v1-style brix run with YAML pipeline succeeds."""
        pipeline_yaml = tmp_path / "test.yaml"
        pipeline_yaml.write_text("""
name: v1-compat-test
steps:
  - id: echo_step
    type: cli
    args: ["echo", "v1-works"]
""")
        runner = ClickRunner()
        result = runner.invoke(main, ["run", str(pipeline_yaml)])
        assert result.exit_code == 0
        output = _extract_json(result.output)
        assert output["success"] is True

    def test_brix_validate(self, tmp_path):
        """brix validate still works for v1-style pipeline."""
        pipeline_yaml = tmp_path / "test.yaml"
        pipeline_yaml.write_text("""
name: v1-validate-test
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        runner = ClickRunner()
        result = runner.invoke(main, ["validate", str(pipeline_yaml)])
        assert result.exit_code == 0

    def test_brix_run_with_params(self, tmp_path):
        """v1-style -p key=value parameter passing works."""
        pipeline_yaml = tmp_path / "test.yaml"
        pipeline_yaml.write_text("""
name: param-test
input:
  greeting:
    type: str
    default: hello
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        runner = ClickRunner()
        result = runner.invoke(main, ["run", str(pipeline_yaml), "-p", "greeting=hi"])
        assert result.exit_code == 0

    def test_brix_dry_run(self, tmp_path):
        """brix run --dry-run still works."""
        pipeline_yaml = tmp_path / "test.yaml"
        pipeline_yaml.write_text("""
name: dry-test
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        runner = ClickRunner()
        result = runner.invoke(main, ["run", str(pipeline_yaml), "--dry-run"])
        assert result.exit_code == 0

    def test_brix_run_outputs_json_to_stdout(self, tmp_path):
        """brix run writes a JSON result to stdout (v1 contract)."""
        pipeline_yaml = tmp_path / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: json-output-test
steps:
  - id: step1
    type: cli
    args: ["echo", "hello"]
""")
        runner = ClickRunner()
        result = runner.invoke(main, ["run", str(pipeline_yaml)])
        assert result.exit_code == 0
        parsed = _extract_json(result.output)
        # v1 contract: success, run_id, steps, duration
        assert "success" in parsed
        assert "run_id" in parsed
        assert "steps" in parsed
        assert "duration" in parsed

    def test_brix_run_step_ids_in_result(self, tmp_path):
        """Step IDs from v1-style pipeline appear in result steps dict."""
        pipeline_yaml = tmp_path / "pipeline.yaml"
        pipeline_yaml.write_text("""
name: step-ids-test
steps:
  - id: fetch_data
    type: cli
    args: ["echo", "data"]
  - id: process_data
    type: cli
    args: ["echo", "done"]
""")
        runner = ClickRunner()
        result = runner.invoke(main, ["run", str(pipeline_yaml)])
        assert result.exit_code == 0
        parsed = _extract_json(result.output)
        assert "fetch_data" in parsed["steps"]
        assert "process_data" in parsed["steps"]


# ---------------------------------------------------------------------------
# v1 YAML pipeline format compatibility
# ---------------------------------------------------------------------------

class TestV1PipelineFormat:
    """v1 YAML pipeline format loads and runs correctly in v2."""

    def test_load_v1_pipeline_with_all_types(self):
        """v1 pipeline with mixed step types loads without error."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-all-types
version: "0.6.5"
steps:
  - id: cli_step
    type: cli
    args: ["echo", "hello"]
  - id: python_step
    type: python
    script: helpers/echo_params.py
  - id: mcp_step
    type: mcp
    server: m365
    tool: list-mail-messages
""")
        assert len(pipeline.steps) == 3
        assert pipeline.version == "0.6.5"

    def test_v1_foreach_parallel(self):
        """v1 foreach + parallel fields load correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-foreach
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
    foreach: "{{ items }}"
    parallel: true
    concurrency: 5
""")
        assert pipeline.steps[0].parallel is True
        assert pipeline.steps[0].concurrency == 5
        assert pipeline.steps[0].foreach == "{{ items }}"

    def test_v1_error_handling(self):
        """v1 error_handling block loads correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-errors
error_handling:
  on_error: retry
  retry:
    max: 3
    backoff: exponential
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
    on_error: continue
""")
        assert pipeline.error_handling.on_error == "retry"
        assert pipeline.error_handling.retry.max == 3
        assert pipeline.error_handling.retry.backoff == "exponential"
        assert pipeline.steps[0].on_error == "continue"

    def test_v1_output_field(self):
        """v1 output block at pipeline level loads correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-output
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
output:
  result: "{{ s1.output }}"
""")
        assert pipeline.output == {"result": "{{ s1.output }}"}

    def test_v1_when_condition(self):
        """v1 when field on steps loads correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-when
input:
  flag:
    type: bool
    default: false
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
    when: "{{ input.flag }}"
""")
        assert pipeline.steps[0].when == "{{ input.flag }}"

    def test_v1_pipeline_defaults(self):
        """A minimal v1 pipeline gets default values applied in v2."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: minimal
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        # v1 defaults that must remain stable
        assert pipeline.version == "0.1.0"
        assert pipeline.input == {}
        assert pipeline.credentials == {}
        assert pipeline.output is None
        assert pipeline.error_handling.on_error == "stop"

    def test_v1_input_params(self):
        """v1-style input parameter block with default and type loads."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-input
input:
  query:
    type: str
    default: inbox
  limit:
    type: int
    default: 10
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        assert "query" in pipeline.input
        assert pipeline.input["query"].default == "inbox"
        assert "limit" in pipeline.input
        assert pipeline.input["limit"].default == 10

    def test_v1_credentials_block(self):
        """v1-style credentials block loads correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-creds
credentials:
  token:
    env: MY_TOKEN
steps:
  - id: s1
    type: mcp
    server: myserver
    tool: my-tool
""")
        assert "token" in pipeline.credentials
        assert pipeline.credentials["token"].env == "MY_TOKEN"

    def test_v1_step_timeout(self):
        """v1 step timeout field loads correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-timeout
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
    timeout: "30s"
""")
        assert pipeline.steps[0].timeout == "30s"

    def test_v1_cli_step_with_command(self):
        """v1 cli step using 'command' string (not args list) loads correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-cli-command
steps:
  - id: s1
    type: cli
    command: "echo hello"
    shell: true
""")
        assert pipeline.steps[0].command == "echo hello"
        assert pipeline.steps[0].shell is True

    def test_v1_http_step(self):
        """v1 http step fields load correctly."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-http
steps:
  - id: s1
    type: http
    url: "https://example.com/api"
    method: POST
    body:
      key: value
""")
        assert pipeline.steps[0].url == "https://example.com/api"
        assert pipeline.steps[0].method == "POST"
        assert pipeline.steps[0].body == {"key": "value"}

    def test_v1_version_string(self):
        """Any version string from v1 era is preserved."""
        loader = PipelineLoader()
        for v in ["0.1.0", "0.6.5", "1.0.0", "2.6.0"]:
            pipeline = loader.load_from_string(f"""
name: v-{v.replace('.', '-')}
version: "{v}"
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
            assert pipeline.version == v


# ---------------------------------------------------------------------------
# v1 Engine behavior compatibility
# ---------------------------------------------------------------------------

class TestV1EngineCompat:
    """v1 engine features work in v2."""

    async def test_engine_sequential_run(self):
        """Engine runs steps sequentially and returns success."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-engine
steps:
  - id: s1
    type: cli
    args: ["echo", "hello"]
  - id: s2
    type: cli
    args: ["echo", "world"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.success is True
        assert len(result.steps) == 2
        assert result.steps["s1"].status == "ok"
        assert result.steps["s2"].status == "ok"

    async def test_engine_when_skip(self):
        """Steps with false when-condition are skipped (v1 behavior)."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-skip
input:
  run_it:
    type: bool
    default: false
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
    when: "{{ input.run_it }}"
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline, {"run_it": False})
        assert result.steps["s1"].status == "skipped"

    async def test_engine_result_has_run_id(self):
        """RunResult contains a run_id (v1 contract)."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-run-id
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.run_id.startswith("run-")

    async def test_engine_result_serializable(self):
        """RunResult.model_dump() produces JSON-serializable dict (v1 stdout contract)."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-serial
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        dumped = result.model_dump()
        # Must be JSON-serializable (as written to stdout by CLI)
        as_json = json.dumps(dumped, default=str)
        parsed = json.loads(as_json)
        assert parsed["success"] is True

    async def test_engine_on_error_continue(self):
        """on_error=continue keeps running after a failing step (v1 behavior)."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-continue
steps:
  - id: fail_step
    type: cli
    args: ["false"]
    on_error: continue
  - id: ok_step
    type: cli
    args: ["echo", "still-running"]
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        # ok_step still ran
        assert "ok_step" in result.steps
        assert result.steps["ok_step"].status == "ok"

    async def test_engine_output_block_renders(self):
        """pipeline.output block is evaluated and set on result.result (v1 behavior)."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: v1-output-render
steps:
  - id: s1
    type: cli
    args: ["echo", "hello"]
output:
  msg: "{{ s1.output }}"
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        assert result.success is True
        assert isinstance(result.result, dict)
        assert "msg" in result.result


# ---------------------------------------------------------------------------
# v1 History (SQLite) compatibility
# ---------------------------------------------------------------------------

class TestV1HistoryCompat:
    """v1 SQLite history works in v2."""

    def test_history_schema_compat(self, tmp_path):
        """v1-style record_start/record_finish calls work and are readable in v2."""
        db_path = tmp_path / "test.db"
        history = RunHistory(db_path=db_path)

        # v1-style recording
        history.record_start("run-v1-001", "test-pipeline", "0.6.5", {"query": "test"})
        history.record_finish("run-v1-001", True, 2.5, {"s1": {"status": "ok"}})

        # v2 can read it
        run = history.get_run("run-v1-001")
        assert run is not None
        assert run["pipeline"] == "test-pipeline"
        assert run["version"] == "0.6.5"
        assert run["success"] == 1  # SQLite stores as int

        stats = history.get_stats()
        assert stats["total_runs"] == 1

    def test_history_get_recent(self, tmp_path):
        """get_recent() returns list of run dicts (v1 CLI history command contract)."""
        db_path = tmp_path / "test.db"
        history = RunHistory(db_path=db_path)
        history.record_start("run-v1-002", "pipeline-a", "1.0.0", {})
        history.record_finish("run-v1-002", True, 1.0, {})

        runs = history.get_recent(10)
        assert len(runs) == 1
        run = runs[0]
        # v1 CLI history command accesses these keys
        assert "run_id" in run
        assert "pipeline" in run
        assert "success" in run
        assert "duration" in run

    def test_history_stats_empty(self, tmp_path):
        """get_stats() returns zero-counts dict when DB is empty (v1 contract)."""
        db_path = tmp_path / "empty.db"
        history = RunHistory(db_path=db_path)
        stats = history.get_stats()
        assert stats["total_runs"] == 0
        assert stats["success_rate"] == 0
        assert stats["avg_duration"] == 0

    def test_history_stats_keys(self, tmp_path):
        """get_stats() always returns the same v1 key set."""
        db_path = tmp_path / "test.db"
        history = RunHistory(db_path=db_path)
        history.record_start("run-v1-003", "p", "1.0.0", {})
        history.record_finish("run-v1-003", False, 3.0, {})

        stats = history.get_stats()
        for key in ("total_runs", "success_rate", "avg_duration"):
            assert key in stats

    def test_history_multiple_runs(self, tmp_path):
        """Multiple runs are stored and stats aggregate correctly."""
        db_path = tmp_path / "multi.db"
        history = RunHistory(db_path=db_path)
        for i in range(3):
            rid = f"run-multi-{i:03d}"
            history.record_start(rid, "pipe", "1.0.0", {})
            history.record_finish(rid, i % 2 == 0, float(i + 1), {})

        stats = history.get_stats()
        assert stats["total_runs"] == 3


# ---------------------------------------------------------------------------
# v1 Context compatibility
# ---------------------------------------------------------------------------

class TestV1ContextCompat:
    """PipelineContext behavior is backward compatible."""

    def test_context_from_pipeline_defaults(self):
        """from_pipeline fills missing input keys with defaults."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: ctx-test
input:
  folder:
    type: str
    default: inbox
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        ctx = PipelineContext.from_pipeline(pipeline)
        assert ctx.input["folder"] == "inbox"

    def test_context_user_input_overrides_default(self):
        """User-supplied input overrides defaults."""
        loader = PipelineLoader()
        pipeline = loader.load_from_string("""
name: ctx-override
input:
  folder:
    type: str
    default: inbox
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
""")
        ctx = PipelineContext.from_pipeline(pipeline, {"folder": "sent"})
        assert ctx.input["folder"] == "sent"

    def test_context_run_id_format(self):
        """run_id always starts with 'run-' (v1 contract)."""
        ctx = PipelineContext()
        assert ctx.run_id.startswith("run-")

    def test_context_set_and_get_output(self):
        """set_output / get_output round-trip (v1 step-chaining contract)."""
        ctx = PipelineContext()
        ctx.set_output("step_a", {"items": [1, 2, 3]})
        assert ctx.get_output("step_a") == {"items": [1, 2, 3]}

    def test_context_to_jinja_context_keys(self):
        """to_jinja_context() always exposes 'input' and 'credentials' (v1 template contract)."""
        ctx = PipelineContext(pipeline_input={"q": "test"}, credentials={"token": "abc"})
        jctx = ctx.to_jinja_context()
        assert "input" in jctx
        assert "credentials" in jctx
        assert jctx["input"]["q"] == "test"

    def test_context_step_output_accessible_in_template(self):
        """Step output is accessible as {{ step_id.output }} (v1 template contract)."""
        ctx = PipelineContext()
        ctx.set_output("fetch", ["a", "b"])
        jctx = ctx.to_jinja_context()
        assert "fetch" in jctx
        assert jctx["fetch"]["output"] == ["a", "b"]


# ---------------------------------------------------------------------------
# v1 Helper scripts compatibility
# ---------------------------------------------------------------------------

class TestV1HelpersCompat:
    """v1 helper scripts in tests/helpers/ still work."""

    def test_echo_params_helper(self):
        """tests/helpers/echo_params.py accepts JSON arg and echoes it back."""
        helper = Path(__file__).parent / "helpers" / "echo_params.py"
        result = subprocess.run(
            ["python3", str(helper), json.dumps({"key": "value"})],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        output = json.loads(result.stdout)
        assert output["received"] == {"key": "value"}

    def test_echo_params_helper_no_args(self):
        """echo_params.py with no arguments returns empty received dict."""
        helper = Path(__file__).parent / "helpers" / "echo_params.py"
        result = subprocess.run(
            ["python3", str(helper)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        output = json.loads(result.stdout)
        assert output["received"] == {}

    def test_list_items_helper(self):
        """tests/helpers/list_items.py returns a JSON list."""
        helper = Path(__file__).parent / "helpers" / "list_items.py"
        result = subprocess.run(
            ["python3", str(helper)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        output = json.loads(result.stdout)
        assert isinstance(output, list)
        assert len(output) > 0
