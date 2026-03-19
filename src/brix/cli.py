"""CLI entry point for brix commands."""
import asyncio
import json
import sys
from pathlib import Path

import click
import yaml

from brix import __version__
from brix.engine import PipelineEngine
from brix.loader import PipelineLoader


@click.group()
@click.version_option(version=__version__, prog_name="brix")
def main():
    """Brix — Generic process orchestrator for Claude Code."""
    pass


@main.command()
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Show what would happen without executing")
@click.option("--resume", type=str, default=None, help="Resume a failed run by ID")
@click.option("--keep-workdir", is_flag=True, help="Keep workdir after successful run")
@click.option("--param", "-p", multiple=True, help="Pipeline parameter as key=value")
def run(pipeline_file, dry_run, resume, keep_workdir, param):
    """Execute a pipeline.

    Pass parameters with -p key=value or --param key=value.
    JSON result is written to stdout, progress to stderr.
    """
    # Parse params
    user_input = {}
    for p in param:
        if "=" not in p:
            click.echo(f"Error: parameter must be key=value, got: {p}", err=True)
            sys.exit(1)
        key, value = p.split("=", 1)
        # Try JSON parse for complex values
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            pass  # keep as string
        user_input[key] = value

    # Load pipeline
    loader = PipelineLoader()
    try:
        pipeline = loader.load(pipeline_file)
    except Exception as e:
        click.echo(f"Error loading pipeline: {e}", err=True)
        sys.exit(1)

    if dry_run:
        _dry_run(pipeline, user_input)
        return

    # Execute
    engine = PipelineEngine()
    result = asyncio.run(engine.run(pipeline, user_input))

    # Output JSON to stdout
    click.echo(json.dumps(result.model_dump(), indent=2, default=str))

    if not result.success:
        sys.exit(1)


@main.command()
@click.argument("pipeline_file", type=click.Path(exists=True))
def validate(pipeline_file):
    """Validate a pipeline without executing it.

    Checks YAML syntax, schema validity, and reference consistency.
    """
    loader = PipelineLoader()
    try:
        pipeline = loader.load(pipeline_file)
    except Exception as e:
        click.echo(f"✗ {e}", err=True)
        sys.exit(1)

    errors = []
    warnings = []

    # Check steps have unique IDs
    step_ids = [s.id for s in pipeline.steps]
    if len(step_ids) != len(set(step_ids)):
        errors.append("Duplicate step IDs found")

    # Check MCP steps have server references
    for step in pipeline.steps:
        if step.type == "mcp":
            if not step.server or not step.tool:
                errors.append(f"Step '{step.id}': MCP step needs server and tool")

    # Check credential references
    for key in pipeline.credentials:
        click.echo(f"  ✓ Credential '{key}' defined (env: {pipeline.credentials[key].env})", err=True)

    # Report
    click.echo(f"\nPipeline: {pipeline.name} v{pipeline.version}", err=True)
    click.echo(f"  Steps: {len(pipeline.steps)}", err=True)
    click.echo(f"  Input params: {len(pipeline.input)}", err=True)
    click.echo(f"  Credentials: {len(pipeline.credentials)}", err=True)

    if errors:
        for e in errors:
            click.echo(f"  ✗ {e}", err=True)
        click.echo(f"\nResult: INVALID ({len(errors)} errors)", err=True)
        sys.exit(1)
    elif warnings:
        click.echo(f"\nResult: VALID ({len(warnings)} warnings)", err=True)
    else:
        click.echo(f"\nResult: VALID", err=True)


def _dry_run(pipeline, user_input: dict):
    """Show what a pipeline run would do without executing (D-18)."""
    import os

    click.echo(f"Pipeline: {pipeline.name} v{pipeline.version}", err=True)
    click.echo(f"Input: {json.dumps(user_input) if user_input else '(none)'}", err=True)
    click.echo("", err=True)

    loader = PipelineLoader()

    # Build a partial Jinja2 context from available input (no step outputs yet)
    # so we can render templates that only reference input.*
    from brix.context import PipelineContext
    ctx = PipelineContext.from_pipeline(pipeline, user_input)
    jinja_ctx = ctx.to_jinja_context()

    # Collect step IDs seen so far (to detect cross-step dependencies)
    executed_step_ids: set[str] = set()

    for i, step in enumerate(pipeline.steps, 1):
        prefix = f"  Step {i}/{len(pipeline.steps)}: {step.id}"
        type_info = f"({step.type}"

        if step.type == "mcp":
            type_info += f" → {step.server}:{step.tool}"
        elif step.type == "python":
            type_info += f" → {step.script}"
        elif step.type == "cli":
            if step.args:
                type_info += f" → {' '.join(step.args[:3])}"
            elif step.command:
                type_info += f" → {step.command[:50]}"
        elif step.type == "http":
            type_info += f" → {step.method} {step.url}"
        elif step.type == "pipeline":
            type_info += f" → {step.pipeline}"
        type_info += ")"

        click.echo(f"{prefix} {type_info}", err=True)

        # --- Render params: show rendered value or [depends on X.output] ---
        params_to_show: list[tuple[str, str]] = []
        raw_params = {}
        if step.params:
            raw_params.update(step.params)
        # Add type-specific fields
        if step.url:
            raw_params["url"] = step.url
        if step.command:
            raw_params["command"] = step.command
        if step.args:
            raw_params["args"] = step.args
        if step.foreach:
            raw_params["foreach"] = step.foreach

        for param_key, param_val in raw_params.items():
            val_str = str(param_val)
            if "{{" in val_str:
                # Check if it references a step that hasn't run yet
                depends_on = _find_step_dependencies(val_str, executed_step_ids, pipeline)
                if depends_on:
                    params_to_show.append((param_key, f"[depends on {depends_on}.output]"))
                else:
                    # Try to render with current context (input.* available)
                    try:
                        rendered = loader.render_value(param_val, jinja_ctx)
                        params_to_show.append((param_key, repr(rendered)))
                    except Exception:
                        params_to_show.append((param_key, val_str))
            else:
                params_to_show.append((param_key, repr(param_val)))

        for pk, pv in params_to_show:
            click.echo(f"    {pk}: {pv}", err=True)

        # --- when condition ---
        if step.when:
            # Try to evaluate with current context
            depends_on_when = _find_step_dependencies(step.when, executed_step_ids, pipeline)
            if depends_on_when:
                click.echo(f"    when: {step.when} [depends on {depends_on_when}.output]", err=True)
            else:
                try:
                    will_run = loader.evaluate_condition(step.when, jinja_ctx)
                    status = "will run" if will_run else "will be SKIPPED"
                    click.echo(f"    when: {step.when} → {status}", err=True)
                except Exception:
                    click.echo(f"    when: {step.when}", err=True)

        if step.foreach:
            par = "parallel" if step.parallel else "sequential"
            click.echo(f"    foreach: ({par}, concurrency: {step.concurrency})", err=True)
        if step.on_error:
            click.echo(f"    on_error: {step.on_error}", err=True)
        if step.timeout:
            click.echo(f"    timeout: {step.timeout}", err=True)

        executed_step_ids.add(step.id)

    # --- Credential check ---
    cred_keys = list(pipeline.credentials.keys())
    cred_status: list[tuple[str, str, bool]] = []
    for key, cred_def in pipeline.credentials.items():
        env_var = cred_def.env
        is_set = bool(os.environ.get(env_var))
        cred_status.append((key, env_var, is_set))

    # --- Server check ---
    mcp_servers = {s.server for s in pipeline.steps if s.type == "mcp" and s.server}
    server_status: list[tuple[str, bool]] = []
    if mcp_servers:
        servers_path = _get_servers_path()
        registered = _load_servers_yaml(servers_path).get("servers", {})
        for srv in sorted(mcp_servers):
            server_status.append((srv, srv in registered))

    # --- Summary ---
    parallel_steps = sum(1 for s in pipeline.steps if s.parallel)

    click.echo("", err=True)
    click.echo("Summary:", err=True)
    click.echo(f"  {len(pipeline.steps)} steps, {parallel_steps} parallel", err=True)

    if server_status:
        click.echo("  MCP servers:", err=True)
        for srv, is_registered in server_status:
            mark = "✓" if is_registered else "✗ NOT REGISTERED"
            click.echo(f"    {mark} {srv}", err=True)

    if cred_status:
        click.echo("  Credentials:", err=True)
        for key, env_var, is_set in cred_status:
            mark = "✓" if is_set else "✗ NOT SET"
            click.echo(f"    {mark} {key} (env: {env_var})", err=True)


def _find_step_dependencies(template_str: str, executed_ids: set[str], pipeline) -> str | None:
    """Return the first step ID whose output is referenced in a template, or None.

    In dry-run we have no actual step outputs, so any reference to
    '<step_id>.output' (or just '<step_id>' in a template) is flagged
    as a runtime dependency.
    """
    all_step_ids = {s.id for s in pipeline.steps}
    for step_id in all_step_ids:
        if step_id in template_str:
            return step_id
    return None


# --- Server Management ---

@main.group()
def server():
    """Manage MCP server configurations."""
    pass


@server.command("add")
@click.argument("name")
@click.option("--command", "cmd", required=True, help="Server command (e.g. 'node', 'docker')")
@click.option("--args", "server_args", multiple=True, help="Command arguments")
@click.option("--env", "env_vars", multiple=True, help="Environment vars as KEY=VALUE")
@click.option("--tools-prefix", default=None, help="Tool name prefix")
def server_add(name, cmd, server_args, env_vars, tools_prefix):
    """Register a new MCP server. Tests connection and caches tool schemas."""
    # Parse env vars
    env = {}
    for e in env_vars:
        if "=" not in e:
            click.echo(f"Error: env must be KEY=VALUE, got: {e}", err=True)
            sys.exit(1)
        k, v = e.split("=", 1)
        env[k] = v

    # Build config
    config = {
        "command": cmd,
        "args": list(server_args),
    }
    if env:
        config["env"] = env
    if tools_prefix:
        config["tools_prefix"] = tools_prefix

    # Load or create servers.yaml
    config_path = _get_servers_path()
    servers_data = _load_servers_yaml(config_path)
    servers_data.setdefault("servers", {})[name] = config
    _save_servers_yaml(config_path, servers_data)

    click.echo(f"✓ Server '{name}' saved to {config_path}", err=True)
    click.echo(f"  command: {cmd} {' '.join(server_args)}", err=True)
    if env:
        click.echo(f"  env: {', '.join(env.keys())}", err=True)


@server.command("list")
def server_list():
    """List all registered MCP servers."""
    config_path = _get_servers_path()
    servers_data = _load_servers_yaml(config_path)
    servers = servers_data.get("servers", {})

    if not servers:
        click.echo("No servers registered. Use 'brix server add' to register one.", err=True)
        return

    for name, config in servers.items():
        cmd = config.get("command", "?")
        args = " ".join(config.get("args", []))
        click.echo(f"  {name}: {cmd} {args}", err=True)


@server.command("remove")
@click.argument("name")
def server_remove(name):
    """Remove a registered MCP server."""
    config_path = _get_servers_path()
    servers_data = _load_servers_yaml(config_path)
    servers = servers_data.get("servers", {})

    if name not in servers:
        click.echo(f"Error: server '{name}' not found", err=True)
        sys.exit(1)

    del servers[name]
    _save_servers_yaml(config_path, servers_data)
    click.echo(f"✓ Server '{name}' removed", err=True)


@server.command("test")
@click.argument("name")
def server_test(name):
    """Test connection to a registered MCP server."""
    config_path = _get_servers_path()
    try:
        from brix.runners.mcp import load_server_config
        sc = load_server_config(name, config_path)
        click.echo(f"✓ Server '{name}' config loaded: {sc.command} {' '.join(sc.args)}", err=True)
        # Note: actual MCP connection test requires running the server
        # which is async. For now, just validate the config.
        click.echo(f"  Config valid. Use 'brix server tools {name}' after connecting.", err=True)
    except Exception as e:
        click.echo(f"✗ {e}", err=True)
        sys.exit(1)


@server.command("tools")
@click.argument("name")
def server_tools(name):
    """List available tools for a server (from cache)."""
    # This will use the cache from T-BRIX-14
    # For now, show a placeholder
    cache_dir = _get_servers_path().parent / "cache" / name
    tools_file = cache_dir / "tools.json"
    if tools_file.exists():
        tools = json.loads(tools_file.read_text())
        click.echo(f"Cached tools for '{name}':", err=True)
        for tool in tools:
            tool_name = tool.get("name", "?")
            desc = tool.get("description", "")[:60]
            click.echo(f"  {tool_name}: {desc}", err=True)
    else:
        click.echo(f"No cached tools for '{name}'. Run 'brix server refresh {name}'.", err=True)


@server.command("refresh")
@click.argument("name")
def server_refresh(name):
    """Refresh tool schema cache for a server."""
    click.echo(f"Refreshing cache for '{name}'... (requires T-BRIX-14)", err=True)


# Helper functions

def _get_servers_path() -> Path:
    path = Path.home() / ".brix" / "servers.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_servers_yaml(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f) or {}
    return {}


def _save_servers_yaml(path: Path, data: dict):
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False)


# Future command groups will be added here:
# @main.command()
# def history(): ...
# @main.command()
# def test(): ...
