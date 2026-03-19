# Brix

**A skill runtime for Claude Code.** Turn multi-step workflows into reusable slash commands backed by real pipelines — not prompt chains.

Brix combines modular building blocks (Python, HTTP, CLI, MCP) into pipelines with a unified JSON interface. Each pipeline can be exposed as a Claude Code [custom slash command](https://docs.anthropic.com/en/docs/claude-code/tutorials/custom-slash-commands) — giving Claude powerful, token-efficient skills that run as native processes.

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

## Skills — From Pipeline to Slash Command

Every Brix pipeline can become a Claude Code [custom slash command](https://docs.anthropic.com/en/docs/claude-code/tutorials/custom-slash-commands). Users type `/download-attachments`, Claude reads a skill prompt, calls `brix run`, and presents the result. The pipeline handles the heavy lifting; Claude handles the conversation.

### Skill Structure

```
~/.claude/commands/download-attachments.md    # Skill prompt
~/.brix/pipelines/download-attachments.yaml   # Pipeline definition
~/.brix/helpers/extract_urls.py               # Helper scripts
```

### Skill Prompt (`download-attachments.md`)

```markdown
Download email attachments from Outlook.

Ask the user for search criteria if not provided as arguments.
Then run:

\`\`\`bash
brix run download-attachments.yaml --query "$ARGUMENTS" --output-dir "./attachments"
\`\`\`

Show the user: number of files downloaded, file names, total size.
If any downloads failed, list which ones and why.
```

### The Full Picture

```
User types:  /download-attachments invoices from last week
                │
                ▼
Claude reads:   download-attachments.md (skill prompt)
                │
                ▼
Claude runs:    brix run download-attachments.yaml \
                  --query "invoices from last week"
                │
                ▼
Brix executes:  5 steps internally (fetch → extract → download → save → report)
                Parallel downloads, error handling, retries — all invisible to Claude
                │
                ▼
Claude gets:    {"success": true, "result": {"total_files": 7, ...}}
                │
                ▼
User sees:      "Downloaded 7 attachments (12.4 MB) to ./attachments/"
```

This is where Brix shines: the skill prompt is simple and readable, the pipeline handles all complexity, and Claude's context stays clean. **One tool call instead of ten.**

### Building a Skill Library

Over time, you build up a library of skills backed by tested, versioned pipelines:

```bash
/download-attachments    # M365 email attachments → local files
/convert-documents       # Batch convert via MarkItDown
/sync-workflows          # n8n workflow backup
/deploy-stack            # Docker compose deployment pipeline
/ingest-data             # ETL: fetch → transform → load
```

Each skill is a thin prompt layer over a Brix pipeline. The pipelines are reusable, testable, and version-controlled. The skills are discoverable via Claude Code's `/` menu.

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

When `on_error: continue` is used with `foreach`, the output includes all items with a `success` flag:

```json
{
  "items": [
    { "success": true, "data": { "path": "/tmp/file1.pdf" } },
    { "success": false, "error": "404 Not Found", "input": { "url": "..." } }
  ],
  "summary": { "total": 3, "succeeded": 2, "failed": 1 }
}
```

Downstream steps receive all items and can filter as needed.

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

### Security: CLI Runner

The CLI runner supports two modes — safe by default:

```yaml
# Default: args list (shell=False) — no injection possible
- type: cli
  args: ["markitdown", "{{ item.path }}"]

# Opt-in: shell string — when piping/globbing is needed
- type: cli
  command: "cat {{ item.path }} | grep 'pattern'"
  shell: true
```

User input is never processed as a Jinja2 template — only pipeline-internal references go through the template engine. This eliminates template injection by design.

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

## Deployment

Brix runs as a Docker container, consistent with a containerized infrastructure:

```yaml
# docker-compose.yml
services:
  brix:
    build: .
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock  # access other containers
      - ~/.brix:/root/.brix                         # persistent config
    env_file: .env
```

A wrapper script at `/usr/local/bin/brix` provides transparent CLI access:

```bash
#!/bin/bash
exec docker exec brix brix "$@"
```

## Tech Stack

- **Python 3.12** — runtime, CLI, all runners
- **asyncio + httpx** — parallel HTTP, async subprocess
- **Pydantic** — pipeline schemas, input/output validation
- **Jinja2** (`SandboxedEnvironment`) — template references in pipeline YAML
- **PyYAML** — pipeline definition parsing
- **Click** — CLI framework
- **SQLite** — run history and statistics (stdlib)
- **[MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)** — stdio communication with MCP servers, built-in `ClientSessionGroup` for connection pooling
- **Hatchling** — build system
- **Docker** — deployment

## Design Decisions

21 architecture decisions are documented in [`docs/decisions.md`](docs/decisions.md), covering security (Jinja2 sandboxing, shell injection prevention), data flow contracts (partial-success schemas, skipped step handling), execution model (sub-pipeline scope, dry-run semantics), and infrastructure choices.

Four expert reviews informed these decisions — all available in [`docs/`](docs/):
- [`review-spec-alex.md`](docs/review-spec-alex.md) — Requirements review (3 critical, 7 high findings)
- [`review-packaging-bruckner.md`](docs/review-packaging-bruckner.md) — DevOps & packaging review
- [`research-mcp-sdk.md`](docs/research-mcp-sdk.md) — MCP Python SDK deep dive
- [`research-claude-code-skills.md`](docs/research-claude-code-skills.md) — Claude Code slash commands research

## Claude Code Integration

Brix is designed to be discovered and used by Claude Code automatically. Two mechanisms ensure every Claude session knows about Brix:

### 1. CLAUDE.md (automatic)

The project's `CLAUDE.md` tells Claude that `brix` is available as a CLI tool. When working in the Brix repo (or any repo with Brix in the context), Claude will prefer `brix run` over multiple individual tool calls.

### 2. Skills (slash commands)

Skills live in `.claude/commands/` (project-scoped) and `~/.claude/commands/` (global). Brix ships two skills:

- **`/download-attachments`** — Download email attachments from M365 Outlook via Brix pipeline
- **`/brix-run`** — Run any Brix pipeline by name or path

To make Brix available globally (all projects):

```bash
cp .claude/commands/*.md ~/.claude/commands/
```

### 3. Path Convention

Brix runs in a Docker container. Host filesystem is mounted at `/host/root/`:

```bash
# Host path /root/documents → Brix path /host/root/documents
brix run pipeline.yaml -p output_dir=/host/root/documents
```

### 4. MCP Server Registration

Register MCP servers once. Claude and Brix share the same servers:

```bash
brix server add m365 \
  --command docker \
  --args exec --args -i --args m365 --args ms-365-mcp-server
```

## E2E Results

Tested with real M365 Outlook data:

```
$ brix run download-attachments.yaml \
    -p "query=hasAttachments eq true and contains(subject, 'Rechnung')" \
    -p top=50 -p output_dir=/host/root/dev/attachments

✓ fetch_mails:      2.9s   50 mails found
✓ get_attachments: 73.3s   attachments fetched (parallel in v0.6.4)
✓ flatten:          0.3s   PDF filter applied
✓ save_files:       2.0s   files saved to host
✓ report:           0.0s   summary generated

Total: 78.8s | 6.9 MB | 56 files | 1 tool call
Without Brix: ~164 tool calls | ~656,000 tokens
With Brix:    1 tool call     | ~5,000 tokens
```

## Status

**v0.6.4** — Implementation complete, E2E validated.

- 274 tests passing
- 27 tasks completed (26 epic + 1 bugfix)
- 6 waves, 21 architecture decisions, 4 expert reviews
- 8 integration learnings documented
- [Detailed documentation in Outline Wiki](https://wiki.kuhlen.dev/doc/brix-D9aLp2qhE5)

## License

MIT
