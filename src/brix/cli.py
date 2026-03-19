"""CLI entry point for brix commands."""
import asyncio
import json
import sys
from pathlib import Path

import click

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
    """Show what a pipeline run would do without executing."""
    click.echo(f"Pipeline: {pipeline.name} v{pipeline.version}", err=True)
    click.echo(f"Input: {json.dumps(user_input) if user_input else '(none)'}", err=True)
    click.echo("", err=True)

    loader = PipelineLoader()

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

        if step.foreach:
            par = "parallel" if step.parallel else "sequential"
            click.echo(f"    foreach: {step.foreach} ({par}, concurrency: {step.concurrency})", err=True)
        if step.when:
            click.echo(f"    when: {step.when}", err=True)
        if step.on_error:
            click.echo(f"    on_error: {step.on_error}", err=True)
        if step.timeout:
            click.echo(f"    timeout: {step.timeout}", err=True)

    # Summary
    mcp_servers = {s.server for s in pipeline.steps if s.type == "mcp" and s.server}
    cred_keys = list(pipeline.credentials.keys())
    parallel_steps = sum(1 for s in pipeline.steps if s.parallel)

    click.echo("", err=True)
    click.echo(f"Summary:", err=True)
    click.echo(f"  {len(pipeline.steps)} steps, {parallel_steps} parallel", err=True)
    if mcp_servers:
        click.echo(f"  MCP servers: {', '.join(mcp_servers)}", err=True)
    if cred_keys:
        click.echo(f"  Credentials: {', '.join(cred_keys)}", err=True)


# Future command groups will be added here:
# @main.group()
# def server(): ...
# @main.command()
# def history(): ...
# @main.command()
# def test(): ...
