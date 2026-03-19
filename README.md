# Brix

**Generic process orchestrator for Claude Code.** Combines modular building blocks (Python, HTTP, CLI, MCP) into pipelines with a unified JSON interface.

## The Problem

AI coding assistants like Claude Code are powerful — but they're inefficient at multi-step workflows. Downloading 10 email attachments means 10 sequential tool calls. Each tool call sends the **entire conversation context** back and forth. That's:

- 10 requests, each carrying the full context
- 10 responses, each added to the growing context
- Massive token consumption for what is essentially a batch job

The same applies to any multi-step process: fetch data from an API, transform it, save files, convert formats, generate reports. Each step is a separate tool call, and the token cost scales linearly.

## The Solution

Brix wraps multi-step workflows into a single call:

```bash
# Instead of 10 sequential tool calls:
brix run download-attachments.yaml --query "invoice"
# → 1 bash call. Everything handled internally. JSON result on stdout.
```

Brix is a **Python runtime** that Claude invokes via a single Bash command. It reads a pipeline definition (YAML), executes all steps internally — parallel where possible — and returns a compact JSON result. Claude sees one tool call and one result. The token savings are proportional to the number of steps avoided.

## How It Works

```
Claude Code: brix run pipeline.yaml --query "..."
                │
                ▼
        ┌──────────────────────────────┐
        │     Brix Runtime (Python)     │
        │                               │
        │  Pipeline Engine (asyncio)    │
        │  ├─ YAML + Jinja2 loader     │
        │  ├─ Step execution:           │
        │  │   mcp:    → MCP SDK stdio  │
        │  │   http:   → httpx async    │
        │  │   cli:    → subprocess     │
        │  │   python: → script/module  │
        │  │                            │
        │  ├─ Parallel where marked     │
        │  ├─ Error handling + retry    │
        │  └─ JSON result → stdout      │
        └──────────────────────────────┘
```

**The direction is always Claude → Brix, never the other way around.** Brix is a tool, not an agent. Claude decides what to run. Brix executes and reports back.

## Pipeline Format

Pipelines are defined as YAML files with Jinja2 templates for data flow between steps:

```yaml
name: download-attachments
description: Download email attachments from M365 Outlook

input:
  query: { type: string, default: "hasAttachments:true" }
  folder: { type: string, default: "Inbox" }
  output_dir: { type: string, default: "./attachments" }
  convert: { type: bool, default: false }

credentials:
  m365_token: { env: "BRIX_CRED_M365_TOKEN" }

error_handling:
  on_error: stop
  retry: { max: 3, backoff: exponential }

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
    headers:
      Authorization: "Bearer {{ credentials.m365_token }}"
    on_error: continue

  - id: save
    type: python
    foreach: "{{ download.output }}"
    parallel: true
    script: helpers/structured_save.py
    params:
      content: "{{ item.body }}"
      metadata: "{{ item.metadata }}"
      output_dir: "{{ input.output_dir }}"

  - id: convert
    when: "{{ input.convert }}"
    type: cli
    foreach: "{{ save.output }}"
    parallel: true
    concurrency: 3
    command: "markitdown '{{ item.path }}'"
    timeout: 30s
```

### Data Flow

Steps reference each other through Jinja2 expressions:

| Reference | Description |
|-----------|------------|
| `{{ input.param }}` | Pipeline input (from user) |
| `{{ step_id.output }}` | Output of a previous step |
| `{{ item }}` | Current element in a `foreach` loop |
| `{{ credentials.name }}` | Credential value (from env) |

### Parallelization

Mark steps with `foreach` + `parallel: true` to process items concurrently. The `concurrency` parameter limits how many run at once — important for respecting API rate limits:

```yaml
- id: download
  type: http
  foreach: "{{ urls.output }}"
  parallel: true
  concurrency: 5         # max 5 simultaneous requests
```

### Error Handling

Configurable globally or per step:

```yaml
# Global default
error_handling:
  on_error: stop

# Per-step override
- id: download
  on_error: continue     # skip individual failures, keep going
```

Three strategies: `stop` (abort on first error), `continue` (log and move on), `retry` (with configurable backoff).

### Sub-Pipelines

Pipelines can call other pipelines for composition and reuse:

```yaml
- id: batch_download
  type: pipeline
  pipeline: http-batch-download
  params:
    urls: "{{ extract.output.urls }}"
```

## Building Blocks

Brix has four runner types. Each implements the same interface: **JSON in → JSON out.**

| Type | What it does | How it runs | Examples |
|------|-------------|-------------|---------|
| `python` | Logic, transformation, aggregation | subprocess or importlib | Extract URLs, rename files, generate reports |
| `http` | REST and API calls | httpx (async, parallel) | Graph API downloads, webhook triggers |
| `cli` | Shell commands | asyncio subprocess | ffmpeg, pandoc, docker exec, any CLI tool |
| `mcp` | MCP tool calls | stdio via [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) | M365 mail, Docker management, n8n workflows |

### Why these four?

We evaluated additional types (`transform` for JSON mapping, `db` for SQL queries, `docker` for container lifecycle) and decided against them. `python` covers transformation, `cli` covers `docker exec`, and `db` can be a Python script. Fewer types = simpler. Specialization comes when the pain arrives, not before.

## MCP Integration

Brix communicates with MCP servers via the standard **stdio protocol** using the official [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk). This means Brix works with any MCP server out of the box.

### Server Management

Claude provisions Brix with MCP server configurations. Brix receives, stores, tests, and gives feedback:

```bash
$ brix server add m365 \
    --command "node" \
    --args "/path/to/m365-mcp/dist/index.js" \
    --env ACCESS_TOKEN=xxx

✓ Server "m365" reachable
✓ 23 tools found: list-mail-messages, get-mail-message, ...
✓ Saved to ~/.brix/servers.yaml
```

This is a **one-time setup**, not something that happens on every run. Brix caches tool schemas locally so it can validate pipelines and discover available tools without starting servers.

### Schema Caching

When a server is registered, Brix fetches and caches all tool schemas. This enables:

- **Offline validation:** `brix validate pipeline.yaml` checks tool names and parameters against the cache — no server needed
- **Faster startup:** No `tools/list` call on every run
- **Discovery:** `brix server tools m365` shows available tools from cache

Cache invalidation happens on server update, manual refresh, schema hash mismatch, or TTL expiry.

### Connection Pooling

When a pipeline has multiple MCP steps against the same server, Brix starts the server once and holds the connection for the pipeline's lifetime. No repeated startup overhead.

## Output

Brix strictly separates result from logs:

- **stdout:** Only the final JSON result — this is what Claude consumes
- **stderr:** Progress, warnings, debug info — for humans and log files

```json
{
  "success": true,
  "run_id": "run-2026-0319-abc",
  "steps": {
    "fetch_mails": { "status": "ok", "duration": 1.8, "items": 3 },
    "download":    { "status": "ok", "duration": 2.4, "items": 7, "errors": 0 },
    "save":        { "status": "ok", "duration": 0.3, "items": 7 }
  },
  "result": {
    "total_files": 7,
    "total_size": "12.4 MB",
    "files": [
      { "path": "./attachments/2026-03-15_Invoice_March.pdf", "size": 524288 },
      { "path": "./attachments/2026-03-10_Contract.docx", "size": 1048576 }
    ]
  },
  "duration": 4.7
}
```

### Workdir and File References

Each pipeline run gets a temporary working directory (`/tmp/brix/<run-id>/`). Large data (downloaded files, generated artifacts) is stored as files — steps pass **file paths**, not inline content. This prevents the JSON data chain from exploding on large payloads.

### Resume

Step outputs are persisted in the workdir. If step 3 of 5 fails, steps 1-2 are preserved:

```bash
brix run pipeline.yaml --resume run-abc123   # picks up at step 3
```

## CLI Reference

```bash
# Execution
brix run pipeline.yaml [--dry-run] [--resume <id>]   # Execute or preview a pipeline
brix validate pipeline.yaml                            # Validate without execution
brix test pipeline.yaml                                # Run with mock data

# Discovery
brix list pipelines                                    # Available pipelines
brix list bricks                                       # Available building blocks
brix info <pipeline>                                   # Pipeline details + requirements

# MCP Servers
brix server add <name> --command ... --env ...         # Register + test a server
brix server test <name>                                # Test connection
brix server list                                       # Show registered servers
brix server tools <name>                               # List available tools (cached)
brix server refresh <name>                             # Re-fetch schemas
brix server remove <name>                              # Unregister

# Monitoring
brix history                                           # Recent runs
brix stats [pipeline]                                  # Success rate, avg duration, etc.
```

## Testing

Pipelines can be tested with mock data without hitting real servers:

```yaml
# tests/download-attachments.test.yaml
pipeline: download-attachments.yaml

mocks:
  fetch_mails:
    output:
      - subject: "Invoice March"
        attachments: [{ name: "invoice.pdf", contentUrl: "https://..." }]

assertions:
  save:
    - item_count: 1
    - each:
        has_keys: [path, size, filename]
```

```bash
$ brix test download-attachments.yaml

✓ fetch_mails: mocked (1 message)
✓ extract: ran → 1 attachment
✓ download: mocked
✓ save: ran → 1 file saved
  ✓ assertion: item_count = 1
  ✓ assertion: has_keys [path, size, filename]

6/6 steps passed, 2/2 assertions passed
```

Snapshot tests capture a successful run's output for regression detection after updates.

## The Gap Brix Fills

We evaluated existing tools (as of March 2026):

| Tool | Stars | Why it doesn't fit |
|------|-------|--------------------|
| **pypyr** | 1.7k | Closest match, but no JSON contract between steps, no REST/MCP support |
| **Netflix Conductor** | 11k | Server-based, too heavyweight for a CLI tool |
| **Airflow / Prefect / Dagster** | 30k+ | Enterprise data pipelines — massive overkill |

The gap: **multiple step types + strict JSON in/out + lightweight + Claude Code integration + MCP native support.** Nothing covers all of these.

## Tech Stack

- **Python 3.11+** — runtime, CLI, all runners
- **asyncio + httpx** — parallel HTTP, async subprocess
- **Pydantic** — pipeline schemas, input/output validation
- **Jinja2** — template references in pipeline YAML
- **PyYAML** — pipeline definition parsing
- **Click** — CLI framework
- **[MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)** — stdio communication with MCP servers

## Status

🚧 **Work in progress** — Architecture and spec are fully defined, implementation is next.

## License

MIT
