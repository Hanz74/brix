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
