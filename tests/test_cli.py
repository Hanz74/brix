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


# --- Server Management Tests ---

def test_cli_server_add(tmp_path, monkeypatch):
    """brix server add registers a server."""
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    runner = ClickRunner()
    result = runner.invoke(main, [
        "server", "add", "m365",
        "--command", "node",
        "--args", "/app/index.js",
        "--env", "TOKEN=abc123",
    ])
    assert result.exit_code == 0
    assert "m365" in result.output

    # Verify file was created
    import yaml
    data = yaml.safe_load((tmp_path / "servers.yaml").read_text())
    assert "m365" in data["servers"]
    assert data["servers"]["m365"]["command"] == "node"


def test_cli_server_list(tmp_path, monkeypatch):
    """brix server list shows registered servers."""
    # Pre-create config
    import yaml
    config = {"servers": {"m365": {"command": "node", "args": ["/app"]}}}
    (tmp_path / "servers.yaml").write_text(yaml.dump(config))
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")

    runner = ClickRunner()
    result = runner.invoke(main, ["server", "list"])
    assert result.exit_code == 0
    assert "m365" in result.output


def test_cli_server_list_empty(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    runner = ClickRunner()
    result = runner.invoke(main, ["server", "list"])
    assert "No servers" in result.output


def test_cli_server_remove(tmp_path, monkeypatch):
    import yaml
    config = {"servers": {"m365": {"command": "node"}}}
    (tmp_path / "servers.yaml").write_text(yaml.dump(config))
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")

    runner = ClickRunner()
    result = runner.invoke(main, ["server", "remove", "m365"])
    assert result.exit_code == 0
    assert "removed" in result.output


def test_cli_server_remove_nonexistent(tmp_path, monkeypatch):
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    runner = ClickRunner()
    result = runner.invoke(main, ["server", "remove", "ghost"])
    assert result.exit_code != 0


def test_cli_server_test_valid(tmp_path, monkeypatch):
    import yaml
    config = {"servers": {"m365": {"command": "node", "args": ["/app"]}}}
    (tmp_path / "servers.yaml").write_text(yaml.dump(config))
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    # Also patch load_server_config to use our path
    monkeypatch.setattr("brix.runners.mcp.SERVERS_CONFIG_PATH", tmp_path / "servers.yaml")

    runner = ClickRunner()
    result = runner.invoke(main, ["server", "test", "m365"])
    assert result.exit_code == 0
    assert "valid" in result.output.lower() or "loaded" in result.output.lower()


# --- Dry-Run Extended Tests (T-BRIX-17) ---


def test_cli_dry_run_shows_credentials(monkeypatch, tmp_path):
    """Dry-run shows credential env-var status (set vs NOT SET)."""
    yaml_content = """
name: cred-test
credentials:
  m365_token:
    env: BRIX_M365_TOKEN
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
"""
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    # Ensure env var is NOT set
    monkeypatch.delenv("BRIX_M365_TOKEN", raising=False)

    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run"])
        assert result.exit_code == 0
        # Should show credential name and env var
        assert "BRIX_M365_TOKEN" in result.output
        assert "m365_token" in result.output
        # Should show NOT SET since env var is missing
        assert "NOT SET" in result.output
    finally:
        os.unlink(path)


def test_cli_dry_run_shows_credentials_set(monkeypatch, tmp_path):
    """Dry-run shows checkmark when credential env-var IS set."""
    yaml_content = """
name: cred-set-test
credentials:
  m365_token:
    env: BRIX_M365_TOKEN
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
"""
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    monkeypatch.setenv("BRIX_M365_TOKEN", "secret-value")

    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run"])
        assert result.exit_code == 0
        assert "BRIX_M365_TOKEN" in result.output
        # Should show ✓ (check mark) since env var is set
        assert "✓" in result.output
    finally:
        os.unlink(path)


def test_cli_dry_run_shows_servers(monkeypatch, tmp_path):
    """Dry-run shows MCP server registration status."""
    import yaml as _yaml
    yaml_content = """
name: server-test
steps:
  - id: fetch
    type: mcp
    server: m365
    tool: list-mail
  - id: store
    type: mcp
    server: onedrive
    tool: upload-file
"""
    # Register m365 but NOT onedrive
    servers_config = {"servers": {"m365": {"command": "node", "args": ["/app"]}}}
    servers_path = tmp_path / "servers.yaml"
    servers_path.write_text(_yaml.dump(servers_config))
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: servers_path)

    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run"])
        assert result.exit_code == 0
        assert "m365" in result.output
        assert "onedrive" in result.output
        # m365 is registered (✓), onedrive is not (NOT REGISTERED)
        assert "NOT REGISTERED" in result.output
    finally:
        os.unlink(path)


def test_cli_dry_run_shows_depends_on(tmp_path, monkeypatch):
    """Dry-run shows [depends on X.output] for cross-step template references."""
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    yaml_content = """
name: depends-test
steps:
  - id: fetch
    type: cli
    args: ["echo", "data"]
  - id: process
    type: cli
    args: ["echo", "{{ fetch.output }}"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run"])
        assert result.exit_code == 0
        assert "depends on fetch.output" in result.output
    finally:
        os.unlink(path)


def test_cli_dry_run_renders_input_params(tmp_path, monkeypatch):
    """Dry-run renders params that only depend on input.* (no step references)."""
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    yaml_content = """
name: input-render-test
input:
  folder:
    type: string
    default: Inbox
steps:
  - id: fetch
    type: cli
    args: ["echo", "{{ input.folder }}"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run", "-p", "folder=Sent"])
        assert result.exit_code == 0
        # Rendered value should appear in output
        assert "Sent" in result.output
    finally:
        os.unlink(path)


# --- Clean Command Tests ---

def test_cli_clean_no_args():
    runner = ClickRunner()
    result = runner.invoke(main, ["clean"])
    assert result.exit_code != 0


def test_cli_clean_dry_run_all(tmp_path, monkeypatch):
    import brix.context
    monkeypatch.setattr(brix.context, "WORKDIR_BASE", tmp_path)
    # Create some fake workdirs
    (tmp_path / "run-001").mkdir()
    (tmp_path / "run-002").mkdir()

    runner = ClickRunner()
    result = runner.invoke(main, ["clean", "--all", "--dry-run"])
    assert result.exit_code == 0
    assert "2 workdirs" in result.output


def test_cli_clean_run_id(tmp_path, monkeypatch):
    import brix.context
    monkeypatch.setattr(brix.context, "WORKDIR_BASE", tmp_path)
    (tmp_path / "run-test").mkdir()

    runner = ClickRunner()
    result = runner.invoke(main, ["clean", "--run-id", "run-test"])
    assert result.exit_code == 0
    assert "deleted" in result.output
    assert not (tmp_path / "run-test").exists()


def test_parse_duration_days():
    from brix.cli import _parse_duration_days
    assert _parse_duration_days("24h") == 1.0
    assert _parse_duration_days("7d") == 7.0
    assert _parse_duration_days("2w") == 14.0
    assert _parse_duration_days("30d") == 30.0
    assert _parse_duration_days("invalid") is None


# --- Stats Step-Level Analytics (T-BRIX-V4-16) ---

def test_cli_stats_no_args(tmp_path, monkeypatch):
    """brix stats without a pipeline name shows global stats."""
    from brix.history import RunHistory
    h = RunHistory(db_path=tmp_path / "stats.db")
    h.record_start("r1", "pipeline-a")
    h.record_finish("r1", True, 2.0)
    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", tmp_path / "stats.db")

    runner = ClickRunner()
    result = runner.invoke(main, ["stats"])
    assert result.exit_code == 0
    assert "Total runs" in result.output


def test_cli_stats_pipeline_arg(tmp_path, monkeypatch):
    """brix stats <pipeline> shows step-level analytics for that pipeline."""
    from brix.history import RunHistory
    h = RunHistory(db_path=tmp_path / "stats.db")
    h.record_start("r1", "my-pipeline")
    h.record_finish("r1", True, 3.0, {
        "fetch": {"status": "ok", "duration": 1.0, "items": 50, "errors": None},
        "transform": {"status": "ok", "duration": 2.0, "items": 50, "errors": None},
    })
    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", tmp_path / "stats.db")

    runner = ClickRunner()
    result = runner.invoke(main, ["stats", "my-pipeline"])
    assert result.exit_code == 0
    assert "Step analytics" in result.output
    assert "fetch" in result.output
    assert "transform" in result.output


def test_cli_stats_pipeline_option(tmp_path, monkeypatch):
    """brix stats --pipeline <name> is equivalent to positional arg."""
    from brix.history import RunHistory
    h = RunHistory(db_path=tmp_path / "stats.db")
    h.record_start("r1", "opt-pipeline")
    h.record_finish("r1", True, 1.5, {
        "step_a": {"status": "ok", "duration": 1.5, "items": None, "errors": None},
    })
    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", tmp_path / "stats.db")

    runner = ClickRunner()
    result = runner.invoke(main, ["stats", "--pipeline", "opt-pipeline"])
    assert result.exit_code == 0
    assert "Step analytics" in result.output
    assert "step_a" in result.output


def test_cli_stats_pipeline_no_runs(tmp_path, monkeypatch):
    """brix stats <pipeline> with no runs shows zero stats, no step table."""
    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", tmp_path / "stats.db")
    runner = ClickRunner()
    result = runner.invoke(main, ["stats", "nonexistent"])
    assert result.exit_code == 0
    assert "Total runs: 0" in result.output
    # No step analytics section since there are no runs
    assert "Step analytics" not in result.output


def test_cli_stats_step_table_columns(tmp_path, monkeypatch):
    """Step analytics table includes duration and items columns."""
    from brix.history import RunHistory
    h = RunHistory(db_path=tmp_path / "stats.db")
    h.record_start("r1", "col-pipeline")
    h.record_finish("r1", True, 2.0, {
        "load": {"status": "ok", "duration": 0.5, "items": 200, "errors": None},
    })
    h.record_start("r2", "col-pipeline")
    h.record_finish("r2", False, 1.0, {
        "load": {"status": "error", "duration": 1.0, "items": None, "errors": "oops"},
    })
    monkeypatch.setattr("brix.history.HISTORY_DB_PATH", tmp_path / "stats.db")

    runner = ClickRunner()
    result = runner.invoke(main, ["stats", "col-pipeline"])
    assert result.exit_code == 0
    # Table header columns
    assert "Avg" in result.output
    assert "Min" in result.output
    assert "Max" in result.output
    assert "AvgItems" in result.output
    # Step row
    assert "load" in result.output


# --- Profile Management CLI Tests (T-BRIX-V4-18) ---


def _patch_profiles_path(monkeypatch, tmp_path):
    """Patch ProfileManager to use a tmp_path profiles.yaml."""
    from brix.profiles import ProfileManager
    profiles_path = tmp_path / "profiles.yaml"

    original_init = ProfileManager.__init__

    def patched_init(self, path=None):
        original_init(self, path or profiles_path)

    monkeypatch.setattr(ProfileManager, "__init__", patched_init)
    return profiles_path


def test_cli_profile_list_empty(tmp_path, monkeypatch):
    """brix profile list shows message when no profiles exist."""
    _patch_profiles_path(monkeypatch, tmp_path)
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "list"])
    assert result.exit_code == 0
    assert "No profiles" in result.output


def test_cli_profile_add_and_list(tmp_path, monkeypatch):
    """brix profile add creates a profile; brix profile list shows it."""
    import yaml as _yaml
    _patch_profiles_path(monkeypatch, tmp_path)
    runner = ClickRunner()

    result = runner.invoke(main, [
        "profile", "add", "dev",
        "--env", "API_KEY=dev-secret",
        "--input", "limit=10",
    ])
    assert result.exit_code == 0
    assert "dev" in result.output

    # Verify it's in the file
    data = _yaml.safe_load((tmp_path / "profiles.yaml").read_text())
    assert "dev" in data["profiles"]
    assert data["profiles"]["dev"]["env"]["API_KEY"] == "dev-secret"
    assert data["profiles"]["dev"]["input_defaults"]["limit"] == 10

    # List should show it
    result2 = runner.invoke(main, ["profile", "list"])
    assert "dev" in result2.output


def test_cli_profile_add_set_default(tmp_path, monkeypatch):
    """brix profile add --set-default sets the profile as default."""
    import yaml as _yaml
    _patch_profiles_path(monkeypatch, tmp_path)
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "add", "prod", "--set-default"])
    assert result.exit_code == 0
    data = _yaml.safe_load((tmp_path / "profiles.yaml").read_text())
    assert data.get("default_profile") == "prod"


def test_cli_profile_show(tmp_path, monkeypatch):
    """brix profile show displays env keys and input defaults."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({
        "profiles": {
            "dev": {
                "env": {"MY_KEY": "value"},
                "input_defaults": {"folder": "Inbox"},
            }
        }
    }))
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "show", "dev"])
    assert result.exit_code == 0
    assert "MY_KEY" in result.output
    assert "folder" in result.output


def test_cli_profile_show_not_found(tmp_path, monkeypatch):
    """brix profile show exits with error for unknown profile."""
    _patch_profiles_path(monkeypatch, tmp_path)
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "show", "ghost"])
    assert result.exit_code != 0


def test_cli_profile_remove(tmp_path, monkeypatch):
    """brix profile remove deletes the profile."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({
        "profiles": {"dev": {}, "prod": {}}
    }))
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "remove", "dev"])
    assert result.exit_code == 0
    assert "removed" in result.output
    data = _yaml.safe_load(profiles_path.read_text())
    assert "dev" not in data["profiles"]
    assert "prod" in data["profiles"]


def test_cli_profile_remove_not_found(tmp_path, monkeypatch):
    """brix profile remove exits with error for unknown profile."""
    _patch_profiles_path(monkeypatch, tmp_path)
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "remove", "ghost"])
    assert result.exit_code != 0


def test_cli_profile_default_set(tmp_path, monkeypatch):
    """brix profile default <name> sets the default profile."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({"profiles": {"prod": {}}}))
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "default", "prod"])
    assert result.exit_code == 0
    data = _yaml.safe_load(profiles_path.read_text())
    assert data["default_profile"] == "prod"


def test_cli_profile_default_show(tmp_path, monkeypatch):
    """brix profile default (no args) shows current default."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({
        "default_profile": "dev",
        "profiles": {"dev": {}}
    }))
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "default"])
    assert result.exit_code == 0
    assert "dev" in result.output


def test_cli_profile_default_clear(tmp_path, monkeypatch):
    """brix profile default --clear removes the default profile."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({
        "default_profile": "dev",
        "profiles": {"dev": {}}
    }))
    runner = ClickRunner()
    result = runner.invoke(main, ["profile", "default", "--clear"])
    assert result.exit_code == 0
    data = _yaml.safe_load(profiles_path.read_text())
    assert "default_profile" not in data


def test_cli_run_with_profile_flag(tmp_path, monkeypatch):
    """brix run --profile <name> is accepted and validated."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({
        "profiles": {
            "dev": {"env": {}, "input_defaults": {}}
        }
    }))

    yaml_content = """
name: profile-run-test
steps:
  - id: echo_step
    type: cli
    args: ["echo", "hello"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--profile", "dev"])
        assert result.exit_code == 0
    finally:
        os.unlink(path)


def test_cli_run_with_invalid_profile_fails(tmp_path, monkeypatch):
    """brix run --profile <nonexistent> exits with error before running."""
    _patch_profiles_path(monkeypatch, tmp_path)

    yaml_content = """
name: bad-profile-test
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--profile", "nonexistent"])
        assert result.exit_code != 0
        assert "nonexistent" in result.output.lower() or "nonexistent" in (result.output or "").lower()
    finally:
        os.unlink(path)


def test_cli_dry_run_shows_profile(tmp_path, monkeypatch):
    """brix run --dry-run --profile shows the active profile in output."""
    import yaml as _yaml
    profiles_path = _patch_profiles_path(monkeypatch, tmp_path)
    profiles_path.write_text(_yaml.dump({
        "profiles": {
            "staging": {
                "env": {"STAGING_KEY": "val"},
                "input_defaults": {},
            }
        }
    }))

    yaml_content = """
name: dry-profile-test
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
"""
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")
    runner = ClickRunner()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name
    try:
        result = runner.invoke(main, ["run", path, "--dry-run", "--profile", "staging"])
        assert result.exit_code == 0
        assert "staging" in result.output
    finally:
        os.unlink(path)


def test_cli_dry_run_no_profile_shows_none(tmp_path, monkeypatch):
    """brix run --dry-run without a profile shows '(none)' in profile line."""
    from brix.profiles import BRIX_PROFILE_ENV
    _patch_profiles_path(monkeypatch, tmp_path)
    monkeypatch.delenv(BRIX_PROFILE_ENV, raising=False)
    monkeypatch.setattr("brix.cli._get_servers_path", lambda: tmp_path / "servers.yaml")

    yaml_content = """
name: no-profile-dry
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
        result = runner.invoke(main, ["run", path, "--dry-run"])
        assert result.exit_code == 0
        assert "(none)" in result.output
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# brix clean --retention  (T-BRIX-V7-08)
# ---------------------------------------------------------------------------

def test_cli_clean_retention_dry_run(tmp_path, monkeypatch):
    """brix clean --retention --dry-run prints a dry-run message and exits 0."""
    monkeypatch.setenv("BRIX_RETENTION_DAYS", "30")
    monkeypatch.setenv("BRIX_RETENTION_MAX_MB", "500")

    # Patch BrixDB inside brix.db so that the local import in clean() picks it up
    import brix.db as _db_mod
    _orig = _db_mod.BrixDB

    class _FakeDB:
        def clean_retention(self, max_days=None, max_mb=None):
            return {"runs_deleted_age": 0, "runs_deleted_size": 0, "app_log_deleted": 0, "db_size_mb": 1.0}

    _db_mod.BrixDB = _FakeDB
    try:
        runner = ClickRunner()
        result = runner.invoke(main, ["clean", "--retention", "--dry-run"])
    finally:
        _db_mod.BrixDB = _orig

    assert result.exit_code == 0
    assert "dry-run" in result.output.lower() or "Retention" in result.output or "retention" in result.output.lower()


def test_cli_clean_retention_runs(tmp_path, monkeypatch):
    """brix clean --retention applies retention policy via BrixDB.clean_retention."""
    captured = {}

    class _FakeDB:
        def clean_retention(self, max_days=None, max_mb=None):
            captured["called"] = True
            captured["max_days"] = max_days
            captured["max_mb"] = max_mb
            return {
                "runs_deleted_age": 5,
                "runs_deleted_size": 2,
                "app_log_deleted": 10,
                "db_size_mb": 12.5,
            }

    import brix.cli as _cli_mod
    import brix.db as _db_mod
    _orig_brixdb = _db_mod.BrixDB
    try:
        _db_mod.BrixDB = _FakeDB
        runner = ClickRunner()
        result = runner.invoke(main, ["clean", "--retention"])
    finally:
        _db_mod.BrixDB = _orig_brixdb

    assert result.exit_code == 0
    assert captured.get("called") is True
    assert "5" in result.output  # runs_deleted_age
    assert "10" in result.output  # app_log_deleted


def test_cli_clean_retention_with_explicit_params(tmp_path, monkeypatch):
    """brix clean --retention --retention-days --retention-max-mb pass through."""
    captured = {}

    class _FakeDB:
        def clean_retention(self, max_days=None, max_mb=None):
            captured["max_days"] = max_days
            captured["max_mb"] = max_mb
            return {
                "runs_deleted_age": 0,
                "runs_deleted_size": 0,
                "app_log_deleted": 0,
                "db_size_mb": 1.0,
            }

    import brix.db as _db_mod
    _orig_brixdb = _db_mod.BrixDB
    try:
        _db_mod.BrixDB = _FakeDB
        runner = ClickRunner()
        result = runner.invoke(
            main, ["clean", "--retention", "--retention-days", "14", "--retention-max-mb", "100"]
        )
    finally:
        _db_mod.BrixDB = _orig_brixdb

    assert result.exit_code == 0
    assert captured["max_days"] == 14
    assert captured["max_mb"] == 100.0
