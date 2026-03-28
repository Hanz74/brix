"""Template catalog: predefined pipeline patterns."""
from typing import Optional

TEMPLATES: dict[str, dict] = {}  # Populated below


def get_template(goal: str) -> Optional[dict]:
    """Find a template matching a goal description."""
    goal_lower = goal.lower()
    for name, tmpl in TEMPLATES.items():
        keywords = tmpl.get("keywords", [])
        if any(kw in goal_lower for kw in keywords):
            return tmpl
    # Fallback: return first partial match on name/description
    for name, tmpl in TEMPLATES.items():
        if any(word in goal_lower for word in name.split("-")):
            return tmpl
    return None


def list_templates() -> list[dict]:
    """List all available templates."""
    return [
        {"name": t["name"], "description": t["description"], "steps": len(t["pipeline"]["steps"])}
        for t in TEMPLATES.values()
    ]


# Template 1: HTTP Download
TEMPLATES["http-download"] = {
    "name": "http-download",
    "description": "Fetch a list from an API, download each item in parallel.",
    "keywords": ["download", "http get", "http download", "api download"],
    "customization_points": ["steps[0].url", "steps[1].headers"],
    "pipeline": {
        "name": "http-download",
        "version": "1.0.0",
        "description": "Download files from an API endpoint",
        "input": {
            "url": {"type": "str", "description": "API endpoint URL"},
            "output_dir": {"type": "str", "default": "./downloads", "description": "Output directory"},
        },
        "steps": [
            {"id": "fetch_list", "type": "http", "url": "{{ input.url }}", "method": "GET"},
            {"id": "download", "type": "http", "foreach": "{{ fetch_list.output }}", "parallel": True, "concurrency": 5, "url": "{{ item.url }}", "on_error": "continue"},
            {"id": "save", "type": "python", "script": "helpers/save_file.py", "foreach": "{{ download.output.items }}", "params": {"content": "{{ item.data }}", "output_dir": "{{ input.output_dir }}"}},
        ],
    },
}

# Template 2: MCP Fetch + Process
TEMPLATES["mcp-fetch-process"] = {
    "name": "mcp-fetch-process",
    "description": "Fetch data from an MCP server, process with Python, save results.",
    "keywords": ["mcp", "m365", "email", "mail", "outlook", "fetch mcp", "mcp fetch"],
    "customization_points": ["steps[0].server", "steps[0].tool", "steps[1].script"],
    "pipeline": {
        "name": "mcp-fetch-process",
        "version": "1.0.0",
        "description": "Fetch and process data from MCP server",
        "input": {
            "server": {"type": "str", "description": "MCP server name"},
            "tool": {"type": "str", "description": "MCP tool to call"},
            "output_dir": {"type": "str", "default": "./output"},
        },
        "steps": [
            {"id": "fetch", "type": "mcp", "server": "{{ input.server }}", "tool": "{{ input.tool }}"},
            {"id": "process", "type": "python", "script": "helpers/process.py", "params": {"data": "{{ fetch.output }}"}},
            {"id": "save", "type": "python", "script": "helpers/save_file.py", "params": {"content": "{{ process.output }}", "output_dir": "{{ input.output_dir }}"}},
        ],
    },
}

# Template 3: Batch Convert
TEMPLATES["batch-convert"] = {
    "name": "batch-convert",
    "description": "Read files from a folder, convert each in parallel, save results.",
    "keywords": ["convert", "batch convert", "transform", "markitdown", "pdf", "markdown"],
    "customization_points": ["steps[1].command or steps[1].url"],
    "pipeline": {
        "name": "batch-convert",
        "version": "1.0.0",
        "description": "Batch convert files in a folder",
        "input": {
            "source_dir": {"type": "str", "description": "Source directory"},
            "output_dir": {"type": "str", "description": "Output directory"},
            "extensions": {"type": "str", "default": "pdf,docx", "description": "File extensions to convert"},
        },
        "steps": [
            {"id": "list_files", "type": "python", "script": "helpers/list_files.py", "params": {"source_dir": "{{ input.source_dir }}", "extensions": "{{ input.extensions }}"}},
            {"id": "convert", "type": "cli", "foreach": "{{ list_files.output }}", "parallel": True, "concurrency": 5, "args": ["echo", "convert", "{{ item.path }}"], "on_error": "continue"},
            {"id": "save", "type": "python", "script": "helpers/save_results.py", "params": {"results": "{{ convert.output | default({}) }}", "output_dir": "{{ input.output_dir }}"}},
        ],
    },
}

# Template 4: Filter + Export
TEMPLATES["filter-export"] = {
    "name": "filter-export",
    "description": "Fetch data, filter by condition, export results.",
    "keywords": ["filter", "export", "csv", "json export", "select"],
    "customization_points": ["steps[0].type", "steps[1].where"],
    "pipeline": {
        "name": "filter-export",
        "version": "1.0.0",
        "description": "Filter and export data",
        "input": {
            "filter_condition": {"type": "str", "description": "Filter expression"},
            "output_file": {"type": "str", "default": "./export.json"},
        },
        "steps": [
            {"id": "fetch", "type": "mcp", "server": "placeholder", "tool": "placeholder"},
            {"id": "filter", "type": "filter", "params": {"input": "{{ fetch.output }}", "where": "{{ input.filter_condition }}"}},
            {"id": "export", "type": "python", "script": "helpers/export.py", "params": {"data": "{{ filter.output }}", "output_file": "{{ input.output_file }}"}},
        ],
    },
}

# Template 5: Multi-Source Merge
TEMPLATES["multi-source-merge"] = {
    "name": "multi-source-merge",
    "description": "Fetch from multiple sources, merge results, generate report.",
    "keywords": ["merge", "combine", "multi-source", "aggregate", "multi source"],
    "customization_points": ["steps[0] and steps[1] sources"],
    "pipeline": {
        "name": "multi-source-merge",
        "version": "1.0.0",
        "description": "Merge data from multiple sources",
        "input": {
            "output_dir": {"type": "str", "default": "./merged"},
        },
        "steps": [
            {"id": "source_a", "type": "mcp", "server": "placeholder", "tool": "placeholder"},
            {"id": "source_b", "type": "mcp", "server": "placeholder", "tool": "placeholder"},
            {"id": "merge", "type": "python", "script": "helpers/merge.py", "params": {"a": "{{ source_a.output }}", "b": "{{ source_b.output }}"}},
            {"id": "report", "type": "python", "script": "helpers/report.py", "params": {"data": "{{ merge.output }}", "output_dir": "{{ input.output_dir }}"}},
        ],
    },
}
