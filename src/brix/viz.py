"""Mermaid diagram generator for Brix pipelines."""

from __future__ import annotations

import re
from typing import Optional

from brix.models import Pipeline, Step


# Mermaid node shape helpers
def _node_label(step: Step) -> str:
    """Return a short human-readable label for a step node."""
    type_tag = step.type.upper()

    if step.type == "mcp":
        detail = f"{step.server}:{step.tool}"
    elif step.type == "python":
        script = step.script or ""
        # Shorten to filename only
        detail = script.rsplit("/", 1)[-1] if "/" in script else script
    elif step.type == "cli":
        if step.args:
            detail = " ".join(str(a) for a in step.args[:3])
            if len(step.args) > 3:
                detail += " …"
        elif step.command:
            detail = step.command[:40]
            if len(step.command) > 40:
                detail += "…"
        else:
            detail = ""
    elif step.type == "http":
        detail = f"{step.method} {step.url or ''}"[:50]
    elif step.type == "pipeline":
        detail = step.pipeline or ""
    elif step.type == "set":
        keys = list((step.values or {}).keys())
        detail = ", ".join(keys[:3])
        if len(keys) > 3:
            detail += ", …"
    elif step.type == "filter":
        detail = str(step.params or "")[:30]
    elif step.type == "transform":
        detail = str(step.params or "")[:30]
    elif step.type == "notify":
        detail = step.channel or step.to or ""
    elif step.type == "approval":
        detail = f"timeout:{step.approval_timeout}"
    elif step.type == "stop":
        detail = step.message or ""
    elif step.type == "choose":
        detail = f"{len(step.choices or [])} branches"
    elif step.type == "parallel":
        detail = f"{len(step.sub_steps or [])} steps"
    elif step.type == "repeat":
        detail = step.until or step.while_condition or ""
    else:
        detail = ""

    if detail:
        # Escape brackets that would break Mermaid
        detail = _escape_mermaid(detail)
        return f"{step.id}\\n[{type_tag}] {detail}"
    return f"{step.id}\\n[{type_tag}]"


def _escape_mermaid(text: str) -> str:
    """Escape characters that would break Mermaid label syntax."""
    # Replace double quotes with single quotes (Mermaid string delimiter)
    text = text.replace('"', "'")
    # Remove or replace characters that break node labels
    text = text.replace("[", "(").replace("]", ")")
    text = text.replace("{", "(").replace("}", ")")
    text = text.replace("\n", " ")
    return text


def _node_shape(step: Step) -> tuple[str, str]:
    """Return (open_bracket, close_bracket) for Mermaid node shape.

    Shapes:
    - Default step:    [label]   (rectangle)
    - Conditional:     {label}   (diamond / rhombus)
    - Foreach/loop:    >label]   (asymmetric / ribbon)
    - Stop:            ([label]) (stadium)
    - Approval:        [[label]] (subroutine)
    """
    if step.type == "stop":
        return "([", "])"
    if step.type == "approval":
        return "[[", "]]"
    if step.when or step.else_of:
        return "{", "}"
    if step.foreach or step.type in ("repeat", "parallel"):
        return ">", "]"
    return "[", "]"


def _sanitize_id(step_id: str) -> str:
    """Return a Mermaid-safe node ID (alphanumeric + underscore)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", step_id)


def _find_dependencies(step: Step, all_ids: set[str]) -> list[str]:
    """Find step IDs that this step's templates reference."""
    deps: list[str] = []
    # Collect all template strings in the step
    templates: list[str] = []
    if step.params:
        templates.extend(_collect_strings(step.params))
    if step.foreach:
        templates.append(step.foreach)
    if step.when:
        templates.append(step.when)
    if step.url:
        templates.append(step.url)
    if step.command:
        templates.append(step.command)
    if step.args:
        templates.extend(str(a) for a in step.args)
    if step.values:
        templates.extend(_collect_strings(step.values))

    seen: set[str] = set()
    for tmpl in templates:
        if "{{" in tmpl:
            for sid in all_ids:
                if sid in tmpl and sid not in seen:
                    deps.append(sid)
                    seen.add(sid)
    return deps


def _collect_strings(value: object) -> list[str]:
    """Recursively collect all string values from a dict/list/str."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        result: list[str] = []
        for v in value.values():
            result.extend(_collect_strings(v))
        return result
    if isinstance(value, list):
        result = []
        for item in value:
            result.extend(_collect_strings(item))
        return result
    return []


def generate_mermaid(pipeline: Pipeline, direction: str = "TD") -> str:
    """Generate a Mermaid flowchart from a pipeline.

    Args:
        pipeline: The loaded Pipeline model.
        direction: Mermaid direction — "TD" (top-down) or "LR" (left-right).

    Returns:
        A string containing the full Mermaid diagram.
    """
    lines: list[str] = [f"flowchart {direction}"]

    # Collect all step IDs for dependency detection
    all_ids = {s.id for s in pipeline.steps}

    # Build node definitions
    lines.append("")
    lines.append("    %% Node definitions")

    for step in pipeline.steps:
        node_id = _sanitize_id(step.id)
        label = _node_label(step)
        open_b, close_b = _node_shape(step)
        lines.append(f'    {node_id}{open_b}"{label}"{close_b}')

    # Apply styling: disabled steps → grey, conditional → yellow
    style_lines: list[str] = []
    for step in pipeline.steps:
        node_id = _sanitize_id(step.id)
        if not step.enabled:
            style_lines.append(f"    style {node_id} fill:#ccc,stroke:#999,color:#666")
        elif step.type == "stop":
            style_lines.append(f"    style {node_id} fill:#fdd,stroke:#f66")
        elif step.type == "approval":
            style_lines.append(f"    style {node_id} fill:#ddf,stroke:#66f")

    # Build edges — primary flow (sequential) + template dependencies
    lines.append("")
    lines.append("    %% Flow edges")

    # Step index map
    step_index = {s.id: i for i, s in enumerate(pipeline.steps)}

    # Track which edges have been added to avoid duplicates
    added_edges: set[tuple[str, str]] = set()

    # 1. Sequential flow: each step → next step (default)
    for i, step in enumerate(pipeline.steps[:-1]):
        nxt = pipeline.steps[i + 1]

        src = _sanitize_id(step.id)
        dst = _sanitize_id(nxt.id)

        # Check if next step is an else_of branch
        if nxt.else_of == step.id:
            # Don't add sequential arrow — the else branch is shown differently
            continue

        edge_label = ""
        if step.when:
            edge_label = " -->|yes| "
        else:
            edge_label = " --> "

        edge_key = (src, dst)
        if edge_key not in added_edges:
            lines.append(f"    {src}{edge_label}{dst}")
            added_edges.add(edge_key)

    # 2. else_of edges: show "no" arrow from the conditional step
    for step in pipeline.steps:
        if step.else_of:
            # Find the step we are the else of
            src = _sanitize_id(step.else_of)
            dst = _sanitize_id(step.id)
            edge_key = (src, dst)
            if edge_key not in added_edges:
                lines.append(f"    {src} -->|no| {dst}")
                added_edges.add(edge_key)

    # 3. Template dependency edges (dashed)
    lines.append("")
    lines.append("    %% Data dependencies (dashed)")
    for step in pipeline.steps:
        deps = _find_dependencies(step, all_ids)
        for dep_id in deps:
            dep_idx = step_index.get(dep_id, -1)
            step_idx = step_index.get(step.id, -1)
            # Only draw dependency edges for non-adjacent steps
            # (adjacent deps are already shown by sequential flow)
            if dep_idx >= 0 and step_idx >= 0 and abs(step_idx - dep_idx) > 1:
                src = _sanitize_id(dep_id)
                dst = _sanitize_id(step.id)
                edge_key = (src, dst)
                if edge_key not in added_edges:
                    lines.append(f"    {src} -.->|data| {dst}")
                    added_edges.add(edge_key)

    # 4. foreach indicator
    lines.append("")
    lines.append("    %% Annotations")
    for step in pipeline.steps:
        if step.foreach:
            node_id = _sanitize_id(step.id)
            par_label = f"parallel×{step.concurrency}" if step.parallel else "sequential"
            lines.append(f"    %% {step.id}: foreach ({par_label})")

    # 5. Style blocks
    if style_lines:
        lines.append("")
        lines.append("    %% Styles")
        lines.extend(style_lines)

    # 6. Pipeline title as a comment header
    header = [
        f"---",
        f"title: {pipeline.name} v{pipeline.version}",
        f"---",
    ]

    return "\n".join(header + [""] + lines)
