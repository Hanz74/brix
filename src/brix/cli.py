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
from brix.config import config


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
@click.option(
    "--profile",
    default=None,
    envvar="BRIX_PROFILE",
    help="Environment profile to use (overrides BRIX_PROFILE env var and default_profile).",
)
@click.option(
    "--dry-run-steps",
    default=None,
    help="Comma-separated list of step IDs to skip (status=dry_run). E.g. --dry-run-steps classify,write",
)
def run(pipeline_file, dry_run, resume, keep_workdir, param, profile, dry_run_steps):
    """Execute a pipeline.

    Pass parameters with -p key=value or --param key=value.
    JSON result is written to stdout, progress to stderr.

    Use --profile (or BRIX_PROFILE env var) to select an environment profile
    that injects credentials and input defaults defined in ~/.brix/profiles.yaml.

    Use --dry-run-steps to skip specific steps without executing them (status=dry_run).
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

    # Parse dry_run_steps (comma-separated → list)
    parsed_dry_run_steps: list[str] | None = None
    if dry_run_steps:
        parsed_dry_run_steps = [s.strip() for s in dry_run_steps.split(",") if s.strip()]

    # Load pipeline
    loader = PipelineLoader()
    try:
        pipeline = loader.load(pipeline_file)
    except Exception as e:
        click.echo(f"Error loading pipeline: {e}", err=True)
        sys.exit(1)

    if dry_run:
        _dry_run(pipeline, user_input, profile=profile)
        return

    # Validate profile exists before starting execution
    if profile:
        from brix.profiles import ProfileManager, ProfileNotFoundError
        mgr = ProfileManager()
        try:
            mgr.load_profile(profile)
        except ProfileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
        click.echo(f"Using profile: {profile}", err=True)

    # Execute
    engine = PipelineEngine()
    result = asyncio.run(engine.run(pipeline, user_input, profile=profile, dry_run_steps=parsed_dry_run_steps))

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


def _dry_run(pipeline, user_input: dict, profile: str = None):
    """Show what a pipeline run would do without executing (D-18)."""
    import os
    from brix.profiles import ProfileManager, ProfileNotFoundError

    click.echo(f"Pipeline: {pipeline.name} v{pipeline.version}", err=True)
    click.echo(f"Input: {json.dumps(user_input) if user_input else '(none)'}", err=True)

    # Resolve and show active profile
    mgr = ProfileManager()
    active_profile = mgr.active_profile_name(override=profile)
    if active_profile:
        try:
            profile_config = mgr.load_profile(active_profile)
            click.echo(f"Profile: {active_profile}", err=True)
            env_keys = list(profile_config["env"].keys())
            if env_keys:
                click.echo(f"  Injects env: {', '.join(env_keys)}", err=True)
            input_defs = profile_config.get("input_defaults", {})
            if input_defs:
                click.echo(f"  Input defaults: {input_defs}", err=True)
        except ProfileNotFoundError as e:
            click.echo(f"  ✗ Profile error: {e}", err=True)
    else:
        click.echo("Profile: (none)", err=True)

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
def profile():
    """Manage environment profiles (dev/prod/staging).

    Profiles are stored in ~/.brix/profiles.yaml and define env vars and input
    defaults that apply when the profile is active.

    Activate a profile with --profile <name> or BRIX_PROFILE=<name>.
    """
    pass


@profile.command("list")
def profile_list():
    """List all defined environment profiles."""
    from brix.profiles import ProfileManager
    mgr = ProfileManager()
    profiles = mgr.list_profiles()
    default = mgr.get_default_profile()
    active = mgr.active_profile_name()

    if not profiles:
        click.echo("No profiles defined. Use 'brix profile add' to create one.", err=True)
        return

    for name in profiles:
        markers = []
        if name == default:
            markers.append("default")
        if name == active and active != default:
            markers.append("active via BRIX_PROFILE")
        mark = f" ({', '.join(markers)})" if markers else ""
        click.echo(f"  {name}{mark}", err=True)


@profile.command("show")
@click.argument("name")
def profile_show(name):
    """Show details of an environment profile."""
    from brix.profiles import ProfileManager, ProfileNotFoundError
    mgr = ProfileManager()
    try:
        config = mgr.load_profile(name)
    except ProfileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        import sys
        sys.exit(1)

    click.echo(f"Profile: {name}", err=True)
    env = config.get("env", {})
    if env:
        click.echo("  env:", err=True)
        for k, v in env.items():
            # Mask values that look like secrets
            display_val = v if len(v) <= 4 else v[:2] + "***" + v[-2:] if len(v) <= 12 else "***"
            click.echo(f"    {k}: {display_val}", err=True)
    else:
        click.echo("  env: (none)", err=True)

    input_defaults = config.get("input_defaults", {})
    if input_defaults:
        click.echo("  input_defaults:", err=True)
        for k, v in input_defaults.items():
            click.echo(f"    {k}: {v!r}", err=True)
    else:
        click.echo("  input_defaults: (none)", err=True)


@profile.command("add")
@click.argument("name")
@click.option("--env", "env_vars", multiple=True, help="Env var as KEY=VALUE (can be repeated)")
@click.option("--input", "input_defaults", multiple=True, help="Input default as key=value (can be repeated)")
@click.option("--set-default", is_flag=True, help="Set this as the default profile")
def profile_add(name, env_vars, input_defaults, set_default):
    """Add or update an environment profile."""
    from brix.profiles import ProfileManager
    import sys

    env = {}
    for e in env_vars:
        if "=" not in e:
            click.echo(f"Error: env must be KEY=VALUE, got: {e}", err=True)
            sys.exit(1)
        k, v = e.split("=", 1)
        env[k] = v

    inp = {}
    for d in input_defaults:
        if "=" not in d:
            click.echo(f"Error: input must be key=value, got: {d}", err=True)
            sys.exit(1)
        k, v = d.split("=", 1)
        # Try to parse as JSON for non-string values
        try:
            v = json.loads(v)
        except (json.JSONDecodeError, ValueError):
            pass
        inp[k] = v

    mgr = ProfileManager()
    mgr.save_profile(name, env=env, input_defaults=inp)
    click.echo(f"✓ Profile '{name}' saved", err=True)

    if set_default:
        from brix.profiles import ProfileNotFoundError
        try:
            mgr.set_default(name)
            click.echo(f"✓ '{name}' set as default profile", err=True)
        except ProfileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)


@profile.command("remove")
@click.argument("name")
def profile_remove(name):
    """Remove an environment profile."""
    from brix.profiles import ProfileManager, ProfileNotFoundError
    import sys
    mgr = ProfileManager()
    try:
        mgr.delete_profile(name)
        click.echo(f"✓ Profile '{name}' removed", err=True)
    except ProfileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@profile.command("default")
@click.argument("name", required=False)
@click.option("--clear", is_flag=True, help="Clear the default profile")
def profile_default(name, clear):
    """Set or clear the default environment profile."""
    from brix.profiles import ProfileManager, ProfileNotFoundError
    import sys
    mgr = ProfileManager()
    if clear:
        mgr.set_default(None)
        click.echo("✓ Default profile cleared", err=True)
    elif name:
        try:
            mgr.set_default(name)
            click.echo(f"✓ Default profile set to '{name}'", err=True)
        except ProfileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
    else:
        current = mgr.get_default_profile()
        if current:
            click.echo(f"Default profile: {current}", err=True)
        else:
            click.echo("No default profile set.", err=True)


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
    click.echo(f"Refreshing cache for '{name}'...", err=True)

    config_path = _get_servers_path()
    try:
        from brix.runners.mcp import load_server_config
        server_config = load_server_config(name, config_path)
    except (FileNotFoundError, KeyError) as e:
        click.echo(f"✗ {e}", err=True)
        sys.exit(1)

    try:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
    except ImportError:
        click.echo("✗ MCP SDK not installed. Run: pip install mcp", err=True)
        sys.exit(1)

    async def _fetch_tools():
        server_params = StdioServerParameters(
            command=server_config.command,
            args=server_config.args,
            env=server_config.env if server_config.env else None,
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.list_tools()
                return [
                    {
                        "name": t.name,
                        "description": t.description or "",
                        "inputSchema": t.inputSchema if hasattr(t, "inputSchema") else {},
                    }
                    for t in result.tools
                ]

    try:
        tools = asyncio.run(_fetch_tools())
    except Exception as e:
        click.echo(f"✗ Failed to connect to '{name}': {e}", err=True)
        sys.exit(1)

    from brix.cache import SchemaCache
    cache = SchemaCache()
    cache.save_tools(name, tools)
    click.echo(f"✓ Cached {len(tools)} tools for '{name}'", err=True)


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


# --- Dependency Management (T-BRIX-V4-BUG-11) ---

@main.group()
def deps():
    """Manage pipeline Python package dependencies."""
    pass


@deps.command("check")
@click.argument("pipeline_file", type=click.Path(exists=True))
def deps_check(pipeline_file):
    """Check whether all requirements for a pipeline are installed.

    Reads the ``requirements`` field from the pipeline YAML and reports
    which packages are present and which are missing.
    """
    loader = PipelineLoader()
    try:
        pipeline = loader.load(pipeline_file)
    except Exception as e:
        click.echo(f"Error loading pipeline: {e}", err=True)
        sys.exit(1)

    if not pipeline.requirements:
        click.echo("No requirements defined in pipeline.", err=True)
        return

    from brix.deps import check_requirements
    missing = check_requirements(pipeline.requirements)

    for req in pipeline.requirements:
        if req in missing:
            click.echo(f"  ✗ {req}  (not installed)", err=True)
        else:
            click.echo(f"  ✓ {req}", err=True)

    if missing:
        click.echo(f"\n{len(missing)} of {len(pipeline.requirements)} package(s) missing.", err=True)
        sys.exit(1)
    else:
        click.echo(f"\nAll {len(pipeline.requirements)} requirement(s) satisfied.", err=True)


@deps.command("install")
@click.argument("pipeline_file", type=click.Path(exists=True))
def deps_install(pipeline_file):
    """Install missing requirements for a pipeline.

    Only installs packages not already present in the environment.
    """
    loader = PipelineLoader()
    try:
        pipeline = loader.load(pipeline_file)
    except Exception as e:
        click.echo(f"Error loading pipeline: {e}", err=True)
        sys.exit(1)

    if not pipeline.requirements:
        click.echo("No requirements defined in pipeline.", err=True)
        return

    from brix.deps import check_requirements, install_requirements
    missing = check_requirements(pipeline.requirements)

    if not missing:
        click.echo(f"All {len(pipeline.requirements)} requirement(s) already installed.", err=True)
        return

    click.echo(f"Installing {len(missing)} package(s): {', '.join(missing)}", err=True)
    ok = install_requirements(missing)
    if ok:
        click.echo(f"✓ {len(missing)} package(s) installed successfully.", err=True)
    else:
        click.echo("✗ Installation failed. Check pip output above.", err=True)
        sys.exit(1)


@main.command("mcp")
@click.option(
    "--transport",
    type=click.Choice(["stdio", "http"]),
    default="stdio",
    show_default=True,
    help="Transport to use: stdio (default) or http (HTTP/SSE via uvicorn).",
)
@click.option(
    "--port",
    default=config.MCP_HTTP_PORT,
    show_default=True,
    help="Port for HTTP transport (ignored for stdio).",
)
@click.option(
    "--host",
    default=config.MCP_HOST,
    show_default=True,
    help="Host to bind for HTTP transport (ignored for stdio).",
)
def mcp_server(transport: str, port: int, host: str):
    """Start Brix as MCP server.

    Use --transport stdio (default) for Claude Desktop / claude code.
    Use --transport http to expose the server over HTTP/SSE on --host:--port.
    """
    from brix.mcp_server import run_mcp_server, run_mcp_http_server

    if transport == "stdio":
        asyncio.run(run_mcp_server())
    else:
        click.echo(f"Starting Brix MCP HTTP server on {host}:{port}", err=True)
        asyncio.run(run_mcp_http_server(host=host, port=port))


@main.command("api")
@click.option("--port", default=config.API_PORT, help="Port to listen on")
@click.option("--host", default=config.API_HOST, help="Host to bind to")
def api_server(port, host):
    """Start Brix REST API server.

    Exposes endpoints for pipeline execution via HTTP, webhooks, and cron.
    Set BRIX_API_KEY env var to enable authentication.
    """
    import uvicorn
    from brix.api import app
    click.echo(f"Starting Brix API on {host}:{port}", err=True)
    uvicorn.run(app, host=host, port=port, log_level="info")


@main.group()
def triggers():
    """Manage pipeline triggers."""
    pass


@triggers.command("list")
def triggers_list():
    """List configured triggers."""
    from brix.triggers.service import TriggerService
    svc = TriggerService()
    svc.load_triggers()
    if not svc._triggers:
        click.echo("No triggers configured.", err=True)
        return
    for t in svc._triggers:
        status = "✓" if t.enabled else "○"
        click.echo(f"  {status} {t.id:<20} {t.type:<15} {t.interval:<8} → {t.pipeline}")


@triggers.command("status")
def triggers_status():
    """Show trigger state."""
    from brix.triggers.state import TriggerState
    state = TriggerState()
    # Show recent events
    events = state.get_unprocessed_events()
    click.echo(f"  Unprocessed events: {len(events)}")


@main.command("scheduler")
def run_scheduler():
    """Start the cron scheduler.

    Reads schedule config from ~/.brix/schedules.yaml and runs
    pipelines on their configured intervals.
    """
    import asyncio
    from brix.scheduler import BrixScheduler
    scheduler = BrixScheduler()
    asyncio.run(scheduler.start())


@main.command()
@click.option("--limit", "-n", default=10, help="Number of runs to show")
def history(limit):
    """Show recent pipeline runs."""
    from brix.history import RunHistory
    h = RunHistory()
    runs = h.get_recent(limit)
    if not runs:
        click.echo("No runs recorded.", err=True)
        return
    for run in runs:
        status = "✓" if run.get("success") else "✗"
        dur = f"{run.get('duration', 0):.1f}s" if run.get("duration") else "?"
        click.echo(f"  {status} {run['run_id']:<20} {run['pipeline']:<25} {dur}", err=True)


@main.command("test")
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option("--fixture", "-f", type=click.Path(exists=True), help="Test fixture YAML file")
def test_pipeline(pipeline_file, fixture):
    """Run pipeline tests with mock data."""
    import asyncio
    from brix.testing import PipelineTestRunner, TestFixture

    if fixture:
        fx = TestFixture.load(fixture)
    else:
        # Auto-discover: tests/<pipeline-name>.test.yaml
        pipeline_name = Path(pipeline_file).stem
        auto_fixture = Path("tests") / f"{pipeline_name}.test.yaml"
        if auto_fixture.exists():
            fx = TestFixture.load(str(auto_fixture))
        else:
            click.echo(f"No fixture found. Create {auto_fixture} or use --fixture.", err=True)
            sys.exit(1)

    # Override pipeline path if relative
    if not Path(fx.pipeline_path).is_absolute():
        fx.pipeline_path = pipeline_file

    runner = PipelineTestRunner()
    result = asyncio.run(runner.run_test(fx))

    # Display results
    summary = result["summary"]
    click.echo(f"\nTest: {fx.description or pipeline_file}", err=True)
    for step_id, status in result["run_result"].steps.items():
        icon = "✓" if status.status == "ok" else ("○" if status.status == "skipped" else "✗")
        click.echo(f"  {icon} {step_id}: {status.status}", err=True)

    if result["assertions"]:
        click.echo("", err=True)
        for ar in result["assertions"]:
            icon = "✓" if ar.passed else "✗"
            click.echo(f"  {icon} assertion: {ar.assertion} ({ar.message})", err=True)

    click.echo(
        f"\n{summary['steps_passed']}/{summary['steps_total']} steps, "
        f"{summary['assertions_passed']}/{summary['assertions_total']} assertions",
        err=True,
    )

    if not result["success"]:
        sys.exit(1)


@main.command()
@click.argument("pipeline_name", required=False)
@click.option("--pipeline", "pipeline_filter", default=None,
              help="Show step-level analytics for a specific pipeline.")
def stats(pipeline_name, pipeline_filter):
    """Show pipeline statistics.

    Pass a pipeline name as argument or via --pipeline to include
    per-step analytics (avg/min/max duration, success rate per step).
    """
    from brix.history import RunHistory
    h = RunHistory()
    # Accept pipeline name either as positional arg or --pipeline option
    effective_pipeline = pipeline_filter or pipeline_name
    s = h.get_stats(effective_pipeline)
    click.echo(f"  Total runs: {s['total_runs']}", err=True)
    click.echo(f"  Success rate: {s['success_rate']}%", err=True)
    click.echo(f"  Avg duration: {s['avg_duration']}s", err=True)

    if effective_pipeline and s["total_runs"] > 0:
        step_stats = h.get_step_stats(effective_pipeline)
        if step_stats:
            click.echo("", err=True)
            click.echo(f"  Step analytics for '{effective_pipeline}':", err=True)
            # Header
            hdr = f"  {'Step':<28} {'Runs':>5} {'OK':>5} {'Fail':>5} {'Skip':>5}  {'Avg':>7}  {'Min':>7}  {'Max':>7}  {'AvgItems':>9}"
            click.echo(hdr, err=True)
            click.echo("  " + "-" * (len(hdr) - 2), err=True)
            for step in step_stats:
                avg_dur = f"{step['avg_duration']}s" if step['avg_duration'] is not None else "-"
                min_dur = f"{step['min_duration']}s" if step['min_duration'] is not None else "-"
                max_dur = f"{step['max_duration']}s" if step['max_duration'] is not None else "-"
                avg_items = str(step['avg_items']) if step['avg_items'] is not None else "-"
                line = (
                    f"  {step['step_id']:<28}"
                    f" {step['runs']:>5}"
                    f" {step['successes']:>5}"
                    f" {step['failures']:>5}"
                    f" {step['skips']:>5}"
                    f"  {avg_dur:>7}"
                    f"  {min_dur:>7}"
                    f"  {max_dur:>7}"
                    f"  {avg_items:>9}"
                )
                click.echo(line, err=True)


@main.command()
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option(
    "--direction",
    "-d",
    type=click.Choice(["TD", "LR"], case_sensitive=False),
    default="TD",
    show_default=True,
    help="Diagram direction: TD (top-down) or LR (left-right).",
)
@click.option(
    "--output", "-o",
    type=click.Path(),
    default=None,
    help="Write diagram to this file instead of stdout.",
)
def viz(pipeline_file, direction, output):
    """Generate a Mermaid flowchart from a pipeline.

    Outputs the Mermaid diagram to stdout (or --output file).
    Paste the result into https://mermaid.live to visualise it.
    """
    from brix.viz import generate_mermaid

    loader = PipelineLoader()
    try:
        pipeline = loader.load(pipeline_file)
    except Exception as e:
        click.echo(f"Error loading pipeline: {e}", err=True)
        sys.exit(1)

    diagram = generate_mermaid(pipeline, direction=direction.upper())

    if output:
        Path(output).write_text(diagram)
        click.echo(f"✓ Diagram written to {output}", err=True)
    else:
        click.echo(diagram)


@main.command()
@click.option("--older-than", type=str, help="Delete runs older than duration (e.g. '24h', '7d', '30d')")
@click.option("--run-id", type=str, help="Delete a specific run")
@click.option("--all", "clean_all", is_flag=True, help="Delete all runs")
@click.option("--dry-run", "clean_dry_run", is_flag=True, help="Show what would be deleted")
@click.option("--versions", "clean_versions", is_flag=True, help="Trim object version history (keep last 10 per object)")
@click.option("--keep", "versions_keep", type=int, default=10, show_default=True, help="Number of versions to keep per object (used with --versions)")
@click.option("--orphaned-runs", "clean_orphaned", is_flag=True, help="Mark runs without finished_at (older than 24h) as cancelled")
@click.option("--max-age-hours", type=int, default=24, show_default=True, help="Age threshold in hours for --orphaned-runs")
@click.option(
    "--retention",
    "clean_retention",
    is_flag=True,
    help=(
        "Apply retention policy: delete runs/app_log older than BRIX_RETENTION_DAYS "
        "(default 30) and enforce BRIX_RETENTION_MAX_MB DB size limit (default 500)."
    ),
)
@click.option(
    "--retention-days",
    type=int,
    default=None,
    help="Override BRIX_RETENTION_DAYS for --retention (days to keep).",
)
@click.option(
    "--retention-max-mb",
    type=float,
    default=None,
    help="Override BRIX_RETENTION_MAX_MB for --retention (max DB size in MB).",
)
def clean(older_than, run_id, clean_all, clean_dry_run, clean_versions, versions_keep, clean_orphaned, max_age_hours, clean_retention, retention_days, retention_max_mb):
    """Clean up old workdirs, history entries, and version archives."""
    import shutil
    from brix.context import WORKDIR_BASE
    from brix.history import RunHistory

    if clean_retention:
        from brix.db import BrixDB
        db = BrixDB()
        if clean_dry_run:
            days = retention_days
            mb = retention_max_mb
            if days is None:
                import os
                try:
                    days = int(os.environ.get("BRIX_RETENTION_DAYS", 30))
                except (ValueError, TypeError):
                    days = 30
            if mb is None:
                import os as _os
                try:
                    mb = float(_os.environ.get("BRIX_RETENTION_MAX_MB", 500))
                except (ValueError, TypeError):
                    mb = 500.0
            click.echo(
                f"Would apply retention policy: max_days={days}, max_mb={mb} (dry-run)",
                err=True,
            )
            return
        result = db.clean_retention(max_days=retention_days, max_mb=retention_max_mb)
        click.echo(
            f"✓ Retention applied: {result['runs_deleted_age']} runs (age), "
            f"{result['runs_deleted_size']} runs (size), "
            f"{result['app_log_deleted']} app_log entries deleted. "
            f"DB size: {result['db_size_mb']} MB",
            err=True,
        )
        return

    if clean_versions:
        from brix.db import BrixDB
        db = BrixDB()
        if clean_dry_run:
            click.echo(f"Would trim object versions to last {versions_keep} per object (dry-run)", err=True)
            return
        deleted = db.cleanup_all_versions(keep=versions_keep)
        click.echo(f"✓ {deleted} old object version(s) deleted (kept last {versions_keep} per object)", err=True)
        return

    if clean_orphaned:
        from brix.history import RunHistory as _RH
        h = _RH()
        if clean_dry_run:
            click.echo(
                f"Would mark unfinished runs older than {max_age_hours}h as cancelled (dry-run)",
                err=True,
            )
            return
        updated = h.clean_orphaned_runs(max_age_hours=max_age_hours)
        click.echo(
            f"✓ {updated} orphaned run(s) marked as cancelled (older than {max_age_hours}h)",
            err=True,
        )
        return

    if not any([older_than, run_id, clean_all, clean_retention, clean_versions, clean_orphaned]):
        click.echo("Specify --older-than, --run-id, --all, --retention, --versions, or --orphaned-runs", err=True)
        sys.exit(1)

    history = RunHistory()

    if run_id:
        # Delete specific run
        workdir = WORKDIR_BASE / run_id
        if clean_dry_run:
            click.echo(f"Would delete: {workdir} (exists: {workdir.exists()})", err=True)
            return
        if workdir.exists():
            shutil.rmtree(workdir)
            click.echo(f"✓ Workdir {run_id} deleted", err=True)
        else:
            click.echo(f"Workdir {run_id} not found", err=True)
        return

    if clean_all:
        if clean_dry_run:
            # Count workdirs
            count = 0
            if WORKDIR_BASE.exists():
                count = sum(1 for d in WORKDIR_BASE.iterdir() if d.is_dir())
            click.echo(f"Would delete: {count} workdirs + all history entries", err=True)
            return
        # Delete all workdirs
        deleted = 0
        if WORKDIR_BASE.exists():
            for d in WORKDIR_BASE.iterdir():
                if d.is_dir():
                    shutil.rmtree(d)
                    deleted += 1
        # Clean history
        history_deleted = history.cleanup(older_than_days=0)
        click.echo(f"✓ {deleted} workdirs deleted, {history_deleted} history entries removed", err=True)
        return

    if older_than:
        # Parse duration
        days = _parse_duration_days(older_than)
        if days is None:
            click.echo(f"Invalid duration: {older_than}. Use '24h', '7d', '30d'.", err=True)
            sys.exit(1)

        if clean_dry_run:
            click.echo(f"Would delete runs older than {days} days", err=True)
            return

        # Clean workdirs by age
        import time
        deleted_workdirs = 0
        if WORKDIR_BASE.exists():
            now = time.time()
            for d in WORKDIR_BASE.iterdir():
                if d.is_dir():
                    age_days = (now - d.stat().st_mtime) / 86400
                    if age_days > days:
                        shutil.rmtree(d)
                        deleted_workdirs += 1

        # Clean history
        history_deleted = history.cleanup(older_than_days=days)
        click.echo(f"✓ {deleted_workdirs} workdirs deleted, {history_deleted} history entries removed", err=True)


def _parse_duration_days(duration: str) -> float | None:
    """Parse duration string to days. '24h' → 1.0, '7d' → 7.0, '30d' → 30.0"""
    duration = duration.strip().lower()
    try:
        if duration.endswith('h'):
            return float(duration[:-1]) / 24
        elif duration.endswith('d'):
            return float(duration[:-1])
        elif duration.endswith('w'):
            return float(duration[:-1]) * 7
        else:
            return float(duration)  # assume days
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Alerting CLI (T-BRIX-V5-08)
# ---------------------------------------------------------------------------

@main.group()
def alerts():
    """Manage pipeline alert rules."""
    pass


@alerts.command("list")
def alerts_list():
    """List all configured alert rules."""
    from brix.alerting import AlertManager
    mgr = AlertManager()
    rules = mgr.list_rules()
    if not rules:
        click.echo("No alert rules configured.", err=True)
        return
    for r in rules:
        status = "✓" if r.enabled else "○"
        click.echo(
            f"  {status} {r.id[:8]}  {r.name:<30} cond={r.condition}  ch={r.channel}",
            err=True,
        )


@alerts.command("add")
@click.option("--name", required=True, help="Rule name")
@click.option("--condition", required=True, help="Alert condition (e.g. pipeline_failed)")
@click.option("--channel", required=True, type=click.Choice(["log", "mattermost"]), help="Notification channel")
@click.option("--webhook-url", default=None, help="Mattermost webhook URL (required for mattermost channel)")
def alerts_add(name, condition, channel, webhook_url):
    """Add a new alert rule."""
    from brix.alerting import AlertManager
    config: dict = {}
    if webhook_url:
        config["webhook_url"] = webhook_url
    mgr = AlertManager()
    try:
        rule = mgr.add_rule(name=name, condition=condition, channel=channel, config=config)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    click.echo(f"✓ Alert rule '{rule.name}' added (id: {rule.id})", err=True)


@alerts.command("delete")
@click.argument("rule_id")
def alerts_delete(rule_id):
    """Delete an alert rule by ID."""
    from brix.alerting import AlertManager
    mgr = AlertManager()
    deleted = mgr.delete_rule(rule_id)
    if deleted:
        click.echo(f"✓ Alert rule '{rule_id}' deleted", err=True)
    else:
        click.echo(f"Alert rule '{rule_id}' not found.", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# migrate-helpers — Copy /app/helpers/*.py → ~/.brix/helpers/ + register
# ---------------------------------------------------------------------------

@main.command("migrate-helpers")
@click.option(
    "--source",
    default="/app/helpers",
    show_default=True,
    help="Source directory containing legacy helper scripts.",
)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Show what would be migrated without writing anything.",
)
def migrate_helpers(source, dry_run):
    """Migrate legacy helper scripts to managed storage (~/.brix/helpers/).

    Copies all .py files from SOURCE (default: /app/helpers) to
    ~/.brix/helpers/ and registers any unregistered helpers in the
    Brix helper registry.  Already-registered helpers are updated with
    the new script path.  Idempotent — safe to run multiple times.
    """
    import shutil
    from brix.helper_registry import HelperRegistry

    source_dir = Path(source)
    target_dir = Path.home() / ".brix" / "helpers"

    if not source_dir.exists():
        click.echo(f"✗ Source directory not found: {source_dir}", err=True)
        sys.exit(1)

    scripts = sorted(source_dir.glob("*.py"))
    if not scripts:
        click.echo(f"No .py files found in {source_dir}", err=True)
        return

    if not dry_run:
        target_dir.mkdir(parents=True, exist_ok=True)

    registry = HelperRegistry()
    migrated = 0
    registered = 0
    updated_paths = 0

    for script_path in scripts:
        name = script_path.stem
        dest = target_dir / script_path.name

        if dry_run:
            existing = registry.get(name)
            status = "update-path" if (existing and existing.script != str(dest)) else ("register" if existing is None else "already-managed")
            click.echo(f"  {'WOULD COPY' if dest != script_path else 'SKIP'} {script_path} → {dest}  [{status}]", err=True)
            continue

        # Copy file
        if not dest.exists() or dest.read_bytes() != script_path.read_bytes():
            shutil.copy2(str(script_path), str(dest))
            migrated += 1

        # Register or update registry
        existing = registry.get(name)
        if existing is None:
            registry.register(name=name, script=str(dest))
            registered += 1
        elif existing.script != str(dest):
            registry.update(name, script=str(dest))
            updated_paths += 1

    if not dry_run:
        click.echo(
            f"✓ Migration complete: {migrated} file(s) copied, "
            f"{registered} new registrations, {updated_paths} path update(s)",
            err=True,
        )
        click.echo(f"  Helpers now in: {target_dir}", err=True)


# ---------------------------------------------------------------------------
# Bundle — Import / Export
# ---------------------------------------------------------------------------

@main.group()
def bundle():
    """Bundle pipelines for sharing and portability.

    Export a pipeline (with its helpers) to a single archive file, or
    import a previously exported bundle.
    """
    pass


@bundle.command("export")
@click.argument("pipeline_file", type=click.Path(exists=True))
@click.option(
    "--output", "-o",
    default=None,
    help="Output archive path (.brix.tar.gz). Defaults to <pipeline-name>.brix.tar.gz.",
)
@click.option(
    "--base-dir",
    default=None,
    type=click.Path(file_okay=False),
    help="Base directory for resolving helpers/ paths. Defaults to pipeline file's directory.",
)
@click.option(
    "--include-missing",
    is_flag=True,
    default=False,
    help="Skip missing helper scripts instead of raising an error.",
)
def bundle_export(pipeline_file, output, base_dir, include_missing):
    """Export a pipeline (with helpers) to a .brix.tar.gz bundle.

    The bundle includes the pipeline YAML and all helper scripts referenced
    by ``script: helpers/<name>.py`` fields.  Share the bundle file to
    move a pipeline between machines or environments.

    Example:

    \b
        brix bundle export pipelines/download-attachments.yaml
        brix bundle export pipelines/my-pipeline.yaml -o /tmp/my-pipeline.brix.tar.gz
    """
    from brix.bundle import export_bundle, BUNDLE_SUFFIX

    pipeline_path = Path(pipeline_file).resolve()
    base = Path(base_dir).resolve() if base_dir else None

    if output is None:
        output_path = pipeline_path.parent / (pipeline_path.stem + BUNDLE_SUFFIX)
    else:
        output_path = Path(output)

    try:
        manifest = export_bundle(
            pipeline_path,
            output_path,
            base_dir=base,
            include_missing=include_missing,
        )
    except FileNotFoundError as e:
        click.echo(f"✗ {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"✗ Export failed: {e}", err=True)
        sys.exit(1)

    click.echo(f"✓ Bundle created: {output_path}", err=True)
    click.echo(f"  Pipeline : {manifest.pipeline_name}", err=True)
    click.echo(f"  Helpers  : {len(manifest.helpers)}", err=True)
    for h in manifest.helpers:
        click.echo(f"    + {h}", err=True)
    if manifest.missing_helpers:
        click.echo(f"  Skipped (missing):", err=True)
        for h in manifest.missing_helpers:
            click.echo(f"    - {h}", err=True)
    click.echo(f"  Created  : {manifest.created_at}", err=True)
    click.echo(f"  Brix     : {manifest.brix_version}", err=True)


@bundle.command("import")
@click.argument("bundle_file", type=click.Path(exists=True))
@click.option(
    "--pipelines-dir",
    default=None,
    type=click.Path(file_okay=False),
    help="Directory to install the pipeline YAML. Defaults to ~/.brix/pipelines/.",
)
@click.option(
    "--helpers-dir",
    default=None,
    type=click.Path(file_okay=False),
    help="Directory to install helper scripts. Defaults to /app/helpers or ~/.brix/helpers.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing pipeline and helper files.",
)
def bundle_import(bundle_file, pipelines_dir, helpers_dir, overwrite):
    """Import a pipeline bundle (.brix.tar.gz).

    Installs the pipeline YAML and helper scripts to the appropriate
    directories.  By default the pipeline goes to ``~/.brix/pipelines/``
    and helpers go to the first existing helpers directory.

    Example:

    \b
        brix bundle import my-pipeline.brix.tar.gz
        brix bundle import bundle.brix.tar.gz --overwrite --helpers-dir /my/helpers
    """
    from brix.bundle import import_bundle

    bundle_path = Path(bundle_file).resolve()
    pd = Path(pipelines_dir) if pipelines_dir else None
    hd = Path(helpers_dir) if helpers_dir else None

    try:
        result = import_bundle(bundle_path, pipelines_dir=pd, helpers_dir=hd, overwrite=overwrite)
    except FileExistsError as e:
        click.echo(f"✗ {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"✗ Import failed: {e}", err=True)
        sys.exit(1)

    click.echo(f"✓ Bundle imported", err=True)
    if result.pipeline:
        click.echo(f"  Pipeline : {result.pipeline}", err=True)
    for h in result.helpers:
        click.echo(f"  Helper   : {h}", err=True)
    if result.manifest:
        m = result.manifest
        click.echo(f"  Brix     : {m.brix_version}", err=True)
        click.echo(f"  Created  : {m.created_at}", err=True)


@bundle.command("inspect")
@click.argument("bundle_file", type=click.Path(exists=True))
def bundle_inspect(bundle_file):
    """Show contents of a bundle without extracting it.

    Example:

    \b
        brix bundle inspect my-pipeline.brix.tar.gz
    """
    from brix.bundle import read_manifest

    bundle_path = Path(bundle_file).resolve()

    manifest = read_manifest(bundle_path)
    if manifest:
        click.echo(f"Pipeline : {manifest.pipeline_name}", err=True)
        click.echo(f"Brix     : {manifest.brix_version}", err=True)
        click.echo(f"Created  : {manifest.created_at}", err=True)
        if manifest.helpers:
            click.echo(f"Helpers  :", err=True)
            for h in manifest.helpers:
                click.echo(f"  {h}", err=True)
        if manifest.missing_helpers:
            click.echo(f"Missing  :", err=True)
            for h in manifest.missing_helpers:
                click.echo(f"  {h}", err=True)
    else:
        click.echo("(no manifest in bundle)", err=True)

    click.echo("", err=True)
    click.echo("Archive contents:", err=True)
    try:
        import tarfile as _tarfile
        with _tarfile.open(bundle_path, "r:gz") as tar:
            for member in tar.getmembers():
                size = f"{member.size:>8} B" if not member.isdir() else "     DIR"
                click.echo(f"  {size}  {member.name}", err=True)
    except Exception as e:
        click.echo(f"✗ Cannot read archive: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Credential management CLI (T-BRIX-V5-05)
# ---------------------------------------------------------------------------

@main.group()
def credential():
    """Manage encrypted credentials in the Brix credential store.

    Credentials are Fernet-encrypted and stored in ~/.brix/credentials.db.
    Use the returned UUID in pipeline YAML instead of plaintext env vars:

    \b
        # In pipeline YAML:
        credentials:
          MY_API_KEY: cred-<uuid>

    Set BRIX_MASTER_KEY (64-char hex) for secure encryption.
    """
    pass


@credential.command("add")
@click.argument("name")
@click.option("--type", "cred_type", default="api-key",
              type=click.Choice(["api-key", "oauth2", "basic-auth"]),
              show_default=True,
              help="Credential type.")
@click.option("--value", required=False, default=None,
              help="The secret value. If omitted, prompts interactively (recommended).")
def credential_add(name, cred_type, value):
    """Add a new encrypted credential.

    \b
    Examples:
        brix credential add my-openai-key --type api-key
        brix credential add smtp-pass --type basic-auth --value hunter2
    """
    from brix.credential_store import CredentialStore, CREDENTIAL_TYPES
    import sqlite3 as _sqlite3

    if value is None:
        value = click.prompt(f"Secret value for '{name}'", hide_input=True, confirmation_prompt=True)

    try:
        store = CredentialStore()
        cred_id = store.add(name, cred_type, value)
        click.echo(f"Credential added: {name}", err=True)
        click.echo(f"  UUID : {cred_id}")
        click.echo(f"  Type : {cred_type}", err=True)
        click.echo("Use in pipeline YAML:", err=True)
        click.echo(f"  credentials:", err=True)
        click.echo(f"    MY_KEY: {cred_id}", err=True)
    except _sqlite3.IntegrityError:
        click.echo(f"Error: A credential named '{name}' already exists.", err=True)
        click.echo("Use 'brix credential update' to change it.", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@credential.command("list")
def credential_list():
    """List all stored credentials (name, UUID, type, timestamps — no values)."""
    from brix.credential_store import CredentialStore

    try:
        store = CredentialStore()
        items = store.list()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if not items:
        click.echo("No credentials stored. Use 'brix credential add' to add one.", err=True)
        return

    click.echo(f"{'NAME':<30} {'TYPE':<12} {'UUID':<36} CREATED")
    click.echo("-" * 100)
    for item in items:
        created = item["created_at"][:19].replace("T", " ")
        click.echo(f"{item['name']:<30} {item['type']:<12} {item['id']:<36} {created}")


@credential.command("delete")
@click.argument("name_or_id")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
def credential_delete(name_or_id, yes):
    """Delete a credential by name or UUID.

    \b
    Example:
        brix credential delete my-openai-key
    """
    from brix.credential_store import CredentialStore

    if not yes:
        click.confirm(
            f"Delete credential '{name_or_id}'? "
            "Pipelines using this UUID will fail.",
            abort=True,
        )

    try:
        store = CredentialStore()
        deleted = store.delete(name_or_id)
        if deleted:
            click.echo(f"Credential '{name_or_id}' deleted.", err=True)
        else:
            click.echo(f"Credential '{name_or_id}' not found.", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
