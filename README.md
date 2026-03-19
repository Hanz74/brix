# Brix

**Generic process orchestrator for Claude Code.** Combines modular building blocks (Python, HTTP, CLI, MCP) into pipelines with a unified JSON interface.

## Why?

Every tool call in Claude Code sends the entire context back and forth. 10 tool calls = 10x context = massive token consumption.

Brix turns that into **a single call:**

```bash
# Instead of 10 sequential tool calls:
brix run download-attachments.yaml --query "invoice"
# → 1 bash call, everything handled internally, JSON result on stdout
```

## How It Works

```
Claude Code: brix run pipeline.yaml --query "..."
                │
                ▼
        ┌─────────────────────────┐
        │   Brix Runtime (Python)  │
        │                          │
        │   ├─ mcp  → MCP SDK     │
        │   ├─ http → httpx async  │
        │   ├─ cli  → subprocess   │
        │   ├─ python → script     │
        │                          │
        │   Parallel where marked  │
        │   Error handling + retry │
        │   JSON result → stdout   │
        └─────────────────────────┘
```

Claude controls Brix as a tool. Brix executes and returns results. Never the other way around.

## Pipeline Example

```yaml
name: download-attachments
description: Download email attachments from M365 Outlook

input:
  query: { type: string, default: "hasAttachments:true" }
  folder: { type: string, default: "Inbox" }
  convert: { type: bool, default: false }

steps:
  - id: fetch_mails
    type: mcp
    server: m365
    tool: list-mail-messages
    params:
      folder: "{{ input.folder }}"
      filter: "{{ input.query }}"

  - id: extract
    type: python
    script: helpers/extract_urls.py
    params:
      messages: "{{ fetch_mails.output }}"

  - id: download
    type: http
    foreach: "{{ extract.output }}"
    parallel: true
    concurrency: 5
    url: "{{ item.download_url }}"
    on_error: continue

  - id: convert
    when: "{{ input.convert }}"
    type: cli
    foreach: "{{ download.output }}"
    parallel: true
    command: "markitdown '{{ item.path }}'"
```

## Building Blocks

| Type | Description | Execution |
|------|------------|-----------|
| `python` | Logic, transformation | subprocess / importlib |
| `http` | REST/API calls | httpx (async, parallel) |
| `cli` | Shell commands | asyncio subprocess |
| `mcp` | MCP tool calls | stdio via [MCP SDK](https://github.com/modelcontextprotocol/python-sdk) |

Every building block: **JSON in → JSON out.**

## Features

- **Parallelization** — `foreach` + `parallel: true` with concurrency limits
- **Data flow** — Jinja2 references: `{{ step.output }}`, `{{ item }}`, `{{ input.x }}`
- **Error handling** — `on_error: stop | continue | retry` (global + per step)
- **Sub-pipelines** — Pipelines can call other pipelines
- **MCP server management** — `brix server add/test/list` with schema caching
- **Resume** — Continue after failure: `brix run --resume <run-id>`
- **Dry-run** — `brix run --dry-run` shows what would happen
- **Testing** — Pipeline tests with mock data and snapshot tests
- **Monitoring** — Run history, statistics, token savings estimation

## CLI

```bash
brix run pipeline.yaml [--dry-run] [--resume <id>]   # Execute pipeline
brix validate pipeline.yaml                            # Validate without execution
brix test pipeline.yaml                                # Test with mock data
brix list pipelines|bricks                             # List available pipelines/bricks
brix info <pipeline>                                   # Pipeline details
brix server add|test|list|remove|refresh               # Manage MCP servers
brix history                                           # Run history
brix stats [pipeline]                                  # Statistics
```

## Tech Stack

- Python 3.11+
- asyncio + httpx
- Pydantic
- Jinja2
- PyYAML
- Click
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

## Status

🚧 **Work in progress** — Architecture and spec are defined, implementation pending.

## Documentation

Detailed documentation with all architecture decisions: [Outline Wiki](https://wiki.kuhlen.dev/doc/brix-D9aLp2qhE5)

## License

MIT
