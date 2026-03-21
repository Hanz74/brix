# Brix

**A universal pipeline orchestrator accessible via MCP.** Combine Python, HTTP, CLI and MCP building blocks into pipelines — executable by any MCP client, REST API, webhook, or cron trigger.

Brix exposes pipelines as MCP tools that any AI agent can discover and call: Claude Code, Claude Desktop, Cursor, Cline, n8n, or custom clients. Each saved pipeline automatically becomes an MCP tool (`mcp__brix__pipeline__<name>`). No YAML writing, no scripting — the agent configures pipelines through structured MCP calls.

## The Problem

AI coding assistants like Claude Code are powerful — but they're wasteful at multi-step workflows. Consider a real task: **"Download the last 50 invoice PDFs from my Outlook."**

Without Brix, Claude does this:

```
1.  MCP: list-mail-messages (filter=Rechnung)          →  wait, context grows
2.  MCP: list-mail-attachments (mail #1)                →  wait, context grows
3.  MCP: list-mail-attachments (mail #2)                →  wait, context grows
    ... 48 more times ...
50. MCP: list-mail-attachments (mail #50)               →  wait, context grows
51. MCP: get-mail-attachment (attachment #1)             →  wait, context grows
    ... for every attachment ...
    Python: save file, generate report
```

That's **~164 tool calls**. Each one sends the entire conversation context back and forth. At ~4,000 tokens per round-trip, that's **~656,000 tokens** consumed — for what is essentially a batch job.

## The Solution

With Brix, Claude does this:

```bash
brix run download-attachments-broad.yaml \
  -p keywords="Rechnung,Invoice" -p top=200 \
  -p output_dir=/host/root/dev/invoices
```

**One call. 6.7 seconds. 79 PDFs. 17 MB on disk.**

### Real-world performance (measured)

We tested two strategies against a real Microsoft 365 mailbox:

| Strategy | What it does | API calls | Time | Result |
|----------|-------------|-----------|------|--------|
| **Without Brix** | Claude makes each call individually | ~164 | ~10 min+ | Fragile, context overflow risk |
| **Targeted** | OData filter on server, sequential attachment fetch | 1 + 50 | 78s | Works, but slow |
| **Targeted + parallel** | Same, but parallel attachment fetching | 1 + 10 batches | ~15s | Better |
| **Broad (recommended)** | Fetch 200 mails, filter locally, parallel attachments | 1 + 5 | **6.7s** | 11x faster than targeted |

Token comparison:

| | Tool calls | Tokens consumed |
|---|-----------|----------------|
| Without Brix | ~164 | ~656,000 |
| With Brix | **1** | **~5,000** |
| **Savings** | **99.4%** | **99.2%** |

The broad strategy is fastest because it avoids the N+1 query problem: instead of asking the server to filter (which still requires one attachment-listing call per mail), it fetches many mails at once, filters locally in milliseconds, and only makes attachment calls for actual matches.

### Before and after

**Before (Claude alone):**
- 10+ minutes of sequential tool calls
- Context window fills up, risk of overflow
- No parallelization possible
- No retry/error handling
- Results lost if session disconnects

**After (Claude + Brix):**
- 6.7 seconds, one command
- Parallel downloads (concurrency: 5)
- Automatic retry with exponential backoff
- PDF-only filtering, structured filenames
- Files on host disk, JSON report for Claude

Brix is a **Python runtime** that Claude invokes via a single Bash command. It reads a pipeline definition (YAML), executes all steps internally — parallel where possible — and returns a compact JSON result. Claude sees one tool call and one result.

## What It Looks Like

Here's the actual user experience. The user types a natural language request, Claude figures out the rest:

**User:**
```
Give me the last 50 invoice PDFs from my Outlook, save them to ~/dev/invoices/
```

**Claude (with Brix):**
```
I'll use the broad strategy — fetching recent mails with attachments and
filtering locally for invoices. This is faster than server-side filtering
for common search terms.

$ brix run download-attachments-broad.yaml \
    -p keywords="Rechnung,Invoice,Abrechnung" \
    -p top=200 \
    -p output_dir=/host/root/dev/invoices

✓ fetch_mails:      4.0s   200 mails fetched
✓ filter_mails:     0.0s   23 mails match keywords
✓ get_attachments:  9.8s   attachments fetched (parallel, concurrency: 5)
✓ flatten:          0.1s   24 PDFs extracted (non-PDF filtered out)
✓ save_files:       0.8s   saved to /root/dev/invoices/
✓ report:           0.0s   summary generated

Downloaded 24 PDF invoices (3.2 MB) to ~/dev/invoices/:

  2026-03-19  Kabel-Rechnung März 2026                    52 KB
  2026-02-28  Mistral AI invoice-032                      77 KB
  2026-02-19  Kabel-Rechnung Februar 2026                 52 KB
  2026-02-13  Kontoaufladung Rechnung                    245 KB
  2025-12-05  Mistral AI invoice-001                      55 KB
  ... and 19 more

That's 24 of 50 requested. Fetching the next batch...

$ brix run download-attachments-broad.yaml \
    -p keywords="Rechnung,Invoice,Abrechnung" \
    -p top=200 -p skip=200 \
    -p output_dir=/host/root/dev/invoices

✓ 26 more PDFs downloaded (4.1 MB)

Total: 50 invoice PDFs (7.3 MB) saved to ~/dev/invoices/
```

The user typed one sentence. Claude chose the strategy, ran two pipeline batches, filtered for PDFs, and presented a clean summary. Total time: ~30 seconds. Without Brix, this would have been 10+ minutes of sequential tool calls.

### Example 2: Batch convert PDFs to Markdown

**User:**
```
Convert all those PDFs to Markdown using MarkItDown
```

**Claude (with Brix):**
```
$ brix run convert-folder.yaml \
    -p source_dir=/host/root/dev/invoices \
    -p output_dir=/host/root/dev/invoices/md

✓ list_files:    0.1s   80 PDFs found
✓ convert:      61.2s   80/80 converted (parallel, concurrency: 5)
✓ save_markdown:  0.8s   80 .md files saved
✓ report:         0.0s   summary generated

Converted 80 PDFs to Markdown in 63 seconds.
All files saved to ~/dev/invoices/md/
```

Without Brix, Claude attempted this via individual MCP calls — managed 4 of 50 files in 4 minutes before being stopped. Brix completed all 80 in 63 seconds.

| Method | Files completed | Time |
|--------|----------------|------|
| Claude alone (MCP calls) | 4 / 50 | ~4 min (stopped) |
| **Brix pipeline** | **80 / 80** | **63 seconds** |

Pipelines are composable — the output of one becomes the input of the next. Download invoices, then convert them, all through natural language.

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

## Creating New Pipelines

No container rebuild needed — `pipelines/` and `helpers/` are volume-mounted.

### Helper Script Convention

Every helper script **must** use this input pattern:

```python
#!/usr/bin/env python3
"""What this helper does."""
import json
import sys

def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    # Your logic here
    result = {"key": "value"}
    print(json.dumps(result))

if __name__ == "__main__":
    main()
```

Why both argv and stdin? The Python runner passes small payloads (<100KB) via `sys.argv[1]` and large payloads via stdin. Your script must handle both.

### Pipeline YAML Rules

```yaml
# concurrency MUST be an integer — no Jinja2 templates!
concurrency: 5          # ✓ correct
concurrency: "{{ x }}"  # ✗ Pydantic validation error

# Host paths use /host/root/ prefix
output_dir: "/host/root/dev/output"   # ✓ writes to host /root/dev/output

# Reference conditional steps with | default()
data: "{{ optional_step.output | default([]) }}"

# Validate before running
# brix validate → brix run --dry-run → brix run
```

### Workflow

```bash
# 1. Write pipeline YAML
vim pipelines/my-pipeline.yaml

# 2. Write helper scripts
vim helpers/my_helper.py

# 3. Validate (catches schema errors, missing refs)
brix validate /app/pipelines/my-pipeline.yaml

# 4. Preview (shows rendered params without executing)
brix run --dry-run /app/pipelines/my-pipeline.yaml -p key=value

# 5. Run
brix run /app/pipelines/my-pipeline.yaml -p key=value

# No docker compose build needed!
```

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

### 2. MCP Server (v2 — recommended)

Register Brix as an MCP server. Claude sees all `mcp__brix__*` tools automatically:

```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

Then start a new session. Claude can now:
- `mcp__brix__run_pipeline` — execute any pipeline
- `mcp__brix__list_pipelines` — discover available pipelines
- `mcp__brix__get_tips` — get conventions and available bricks
- `mcp__brix__pipeline__<name>` — auto-exposed pipeline tools
- 10+ more tools for building, validating, and managing pipelines

**Prerequisites:**
```bash
# Brix containers must be running
docker compose up -d

# Pipelines must be in ~/.brix/pipelines/
cp pipelines/*.yaml ~/.brix/pipelines/
```

### 3. Skills (slash commands)

Skills live in `.claude/commands/` (project-scoped) and `~/.claude/commands/` (global). Brix ships two skills:

- **`/download-attachments`** — Download email attachments from M365 Outlook via Brix pipeline
- **`/brix-run`** — Run any Brix pipeline by name or path

To make Brix available globally (all projects), **three steps are required**:

```bash
# 1. Skills globally available (slash commands)
cp .claude/commands/*.md ~/.claude/commands/

# 2. Global CLAUDE.md — tells Claude about brix in EVERY session
# Add a "Brix" section to your root CLAUDE.md (~/CLAUDE.md or /root/CLAUDE.md)
# documenting: available commands, pipelines, path convention, when to use

# 3. Wrapper script (CLI access)
cat > /usr/local/bin/brix << 'EOF'
#!/bin/bash
exec docker exec brix brix "$@"
EOF
chmod +x /usr/local/bin/brix
```

**Important:** Skills alone are not enough. Claude only uses skills when the user types `/`. The global CLAUDE.md is the only way to make Claude **proactively** suggest Brix for multi-step tasks. Without it, Claude in other projects won't know Brix exists.

### 4. Path Convention

Brix runs in a Docker container. Host filesystem is mounted at `/host/root/`:

```bash
# Host path /root/documents → Brix path /host/root/documents
brix run pipeline.yaml -p output_dir=/host/root/documents
```

### 5. MCP Server Registration

Register MCP servers once. Claude and Brix share the same servers:

```bash
brix server add m365 \
  --command docker \
  --args exec --args -i --args m365 --args ms-365-mcp-server
```

## E2E Results

Tested with real M365 Outlook data — same use case across all methods:

| Method | Duration | Tool calls | Tokens (est.) | PDFs | Size |
|--------|----------|-----------|---------------|------|------|
| **Without Brix** (Claude alone) | ~10 min+ | ~164 | ~656,000 | fragile | — |
| **v1 CLI** (brix run via Bash) | ~35s | 1 | ~5,000 | 50 | 5.7 MB |
| **v2 MCP** (native MCP call) | **35.4s** | **1** | **~3,000** | **50** | **5.7 MB** |

v2 MCP has zero measurable overhead vs v1 CLI — plus lower token consumption because MCP responses are structured (no JSON parsing from stdout needed).

## Status

**v2.6.2** — MCP Server, REST API, Webhooks, Cron — all implemented and tested.

- 589 tests passing
- 45 tasks completed (27 v1 + 18 v2)
- 14 MCP tools + dynamic `pipeline__*` tools
- 10 built-in bricks + MCP auto-discovery
- 5 trigger types (MCP stdio, MCP HTTP, REST API, Webhook, Cron)
- 5 pipeline templates
- 7 expert reviews, 15 integration learnings
- All architecture decisions and reviews in [`docs/`](docs/)

## License

MIT
