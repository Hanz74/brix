# Brix

**DB-First Pipeline Orchestrator — konfigurieren statt coden.**

The pipeline runtime built for AI agents. ~3800 tests. 81 pipelines. 59 bricks. Built for Claude Code.

---

## Why Brix Exists

### Token Burn

Every tool call in Claude Code sends the entire conversation context back and forth. 10 sequential steps means 10x context. A real-world mail import with 50 PDF attachments requires roughly 164 calls — at ~4,000 tokens per round-trip, that's **~656,000 tokens** for what a batch job would finish in 35 seconds.

### Context Death

But the token cost is not the real problem. The growing context triggers Context-Compact. The agent loses focus, forgets what it was doing, and the tokens that should have been used for actual decisions were burned on repetitive API calls. A task that should take 30 seconds turns into a fragmented session spanning hours.

### No Reproducibility

The workflow ran yesterday. Today it doesn't. Why? No log, no diff, no debug trail. The agent has to start over — burning more tokens on the same setup it already did. There is no way to resume from where it stopped, no way to replay a single failed step, and no way to compare what changed between runs.

### No Coordination

Two Claude sessions work on the same project. Neither knows what the other is doing. Credentials flow through pipeline inputs as plaintext. There is no audit trail of who changed what and when.

---

## What Brix Is

Brix bundles multi-step workflows into a single call. The pipeline runs internally — parallel, with retry, with checkpoints. The agent sees only the result.

But Brix is more than a batch runner. It is the operating system for AI-agent workflows: a **DB-First** architecture where everything lives in `brix.db` — pipelines, helpers, bricks, connectors, tools, help topics. No code files to manage, no YAML to sync. The database is the single source of truth. Encrypted credential store, distributed locking for concurrent sessions, shared blackboard state between agents, circuit breakers, rate limiters, caches, saga rollbacks, event bus, queues, secret variables, and a self-documenting help system so the agent never has to guess how something works.

> "n8n is for humans who build workflows. Brix is for Claude who executes pipelines."
> — Alex (Spec Review, March 2026)

---

## The Numbers

Tested against a real Microsoft 365 mailbox. Task: download 50 invoice PDFs, save to disk, generate a report.

| | Without Brix | With Brix |
|---|---|---|
| Tool calls | ~164 | **1** |
| Time | ~10 min+ (sequential, fragile) | **~35 seconds** |
| Tokens consumed | ~656,000 | **~5,000** |
| Savings | — | **99.2% fewer tokens** |

The agent typed one call. The pipeline fetched mails in parallel, filtered for PDFs, downloaded attachments with retry and rate-limit handling, saved to disk with structured filenames, and returned a JSON summary.

---

## What It Looks Like

**User:**
```
Give me the last 50 invoice PDFs from my Outlook, save them to ~/dev/invoices/
```

**Claude (with Brix):**
```
$ brix run download-attachments-broad.yaml \
    -p keywords="Rechnung,Invoice,Abrechnung" \
    -p top=200 \
    -p output_dir=/host/root/dev/invoices

✓ fetch_mails:      4.0s   200 mails fetched
✓ filter_mails:     0.0s   23 mails match keywords
✓ get_attachments:  9.8s   attachments fetched (parallel, concurrency: 5)
✓ flatten:          0.1s   24 PDFs extracted
✓ save_files:       0.8s   saved to /root/dev/invoices/
✓ report:           0.0s   summary generated

Downloaded 24 PDF invoices (3.2 MB) to ~/dev/invoices/
```

The user typed one sentence. Claude chose the strategy, ran the pipeline, and presented a clean summary. Total: ~35 seconds. Without Brix: 164 tool calls and ~656,000 tokens.

---

## DB-First Architecture

Everything lives in `brix.db`. Pipelines, helpers, bricks, connectors, tools, help topics — all stored, versioned, and queryable in SQLite. Code files have been removed; the database is the single source of truth (Hard Cut). Schema migrations run automatically on startup.

### Bricks

59 Bricks organized in 10 namespaces. Instead of writing a Python helper script for each step, you pick a Brick and configure it:

| Namespace | What it covers |
|-----------|---------------|
| `source.*` | Fetch data — emails, files, HTTP endpoints, connectors |
| `db.*` | Database operations — query, upsert |
| `llm.*` | LLM inference — batch classification, extraction |
| `extract.*` | Declarative field extraction — regex, JSON path, templates |
| `flow.*` | Control flow — filter, transform, aggregate, merge, dedup, parallel, repeat, choose, switch, validate, wait, diff, flatten, set, error_handler, pipeline, pipeline_group |
| `action.*` | Side effects — notify (Mattermost/webhook), approval, respond, emit, queue |
| `http.*` | HTTP requests — GET, POST, any method |
| `mcp.*` | MCP tool calls — any registered MCP server |
| `script.*` | Code execution — Python scripts, CLI commands |
| `markitdown.*` | Document conversion — PDF/DOCX/HTML to Markdown |

Custom Bricks can be registered at runtime. The Universal Registry (`discover()`) finds bricks, connectors, helpers, and tools from a single entry point. Referential integrity ensures no brick can be deleted while pipelines reference it.

### unwrap_json — Nested JSON Responses

The `unwrap_json` feature handles nested JSON responses automatically. When a step returns a JSON string inside a JSON field (a common pattern with MCP servers), Brix unwraps it transparently so downstream steps receive the actual object, not the escaped string.

### org_registry — Projekt/Tag/Group Definitions

A dedicated `org_registry` stores project, tag, and group definitions. All 15 entity types carry org fields (`project`, `tags`, `group`, `description`) — required fields emit a warning when omitted. The registry is the single source of truth for organizational taxonomy across pipelines and helpers.

### Discover Bricks

```python
# Find what exists
mcp__brix__list_bricks()                          # all bricks
mcp__brix__search_bricks(query="email")           # by keyword
mcp__brix__get_brick_schema(name="llm.batch")     # full schema + params

# Plan and build
mcp__brix__plan_pipeline(goal="classify emails by category")
mcp__brix__compose_pipeline(goal="classify emails by category")
```

`plan_pipeline` returns a step-by-step plan using real Brick names. `compose_pipeline` returns a ready-to-run pipeline definition. `discover` what's available before you build — the system documents itself.

---

## Quick Start

**1. Start the container:**
```bash
docker compose up -d
```

**2. Register Brix as an MCP server in Claude Code:**
```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

**3. Register a MCP server (one-time setup):**
```bash
brix server add m365 \
  --command docker --args exec --args -i --args m365 --args ms-365-mcp-server
# ✓ Server "m365" reachable — 23 tools found
```

**4. Discover available Bricks:**
```python
mcp__brix__get_tips()          # conventions + Brick namespaces
mcp__brix__list_bricks()       # all built-in Bricks
```

**5. Create and run a pipeline:**
```python
mcp__brix__compose_pipeline(goal="fetch invoice emails from Outlook and save PDFs")
# → returns pipeline definition, ready to run

mcp__brix__run_pipeline(
    pipeline_id="fetch-invoices",
    input={"output_dir": "/host/root/dev/invoices"}
)
```

No YAML file to write. No container rebuild. The pipeline is live immediately.

---

## Brick Reference

### source namespace

| Brick | What it does |
|-------|-------------|
| `source.fetch` | Fetch data from a configured connector (Outlook, Gmail, OneDrive, local files, HTTP) |

### db namespace

| Brick | What it does |
|-------|-------------|
| `db.query` | Execute a SQL query, return rows as list of dicts |
| `db.upsert` | Upsert records into the Brix database |

### llm namespace

| Brick | What it does |
|-------|-------------|
| `llm.batch` | Run LLM inference in batch mode — classify, extract, or transform a list of items |

### extract namespace

| Brick | What it does |
|-------|-------------|
| `extract.specialist` | Declarative field extraction: regex, JSON path, split, template — no Python needed |

### flow namespace

| Brick | What it does |
|-------|-------------|
| `flow.filter` | Keep items matching a Jinja2 condition |
| `flow.transform` | Reshape each item with a Jinja2 mapping |
| `flow.aggregate` | Group items and compute statistics (count, sum, avg) |
| `flow.merge` | Merge multiple step outputs into one |
| `flow.dedup` | Deduplicate a list |
| `flow.diff` | Compute difference between two lists or datasets |
| `flow.flatten` | Flatten a list of lists to a single list |
| `flow.set` | Assign computed variables for downstream steps |
| `flow.choose` | Conditional branching (if/else) |
| `flow.switch` | Multi-branch switch/case |
| `flow.parallel` | Run independent sub-steps concurrently |
| `flow.repeat` | Loop until condition is met (poll, retry) |
| `flow.wait` | Wait for a duration or condition |
| `flow.validate` | Enforce data contracts at runtime |
| `flow.error_handler` | Handle errors from previous steps |
| `flow.pipeline` | Call a sub-pipeline |
| `flow.pipeline_group` | Fan-out: run multiple sub-pipelines in parallel |

### action namespace

| Brick | What it does |
|-------|-------------|
| `action.notify` | Send a notification (Mattermost, webhook, log) |
| `action.approval` | Pause for human sign-off; exposes REST endpoint |
| `action.respond` | Send a response back to the caller |
| `action.emit` | Publish an event to the event bus |
| `action.queue` | Buffer items for asynchronous processing |

### http namespace

| Brick | What it does |
|-------|-------------|
| `http.request` | Make an HTTP request (any method, headers, body) |

### mcp namespace

| Brick | What it does |
|-------|-------------|
| `mcp.call` | Call a tool on any registered MCP server via stdio protocol |

### script namespace

| Brick | What it does |
|-------|-------------|
| `script.python` | Run a Python script (reads JSON from stdin/argv, writes JSON to stdout) |
| `script.cli` | Execute a CLI command (shell=False by default) |

### markitdown namespace

| Brick | What it does |
|-------|-------------|
| `markitdown.convert` | Convert PDF, DOCX, HTML, or any supported format to Markdown |

---

## Connectors

6 connectors abstract authentication and API differences for common data sources:

| Connector | What it provides |
|-----------|----------------|
| `outlook` | Microsoft 365 email via M365 MCP — fetch, filter, read emails and attachments |
| `gmail` | Gmail via IMAP — direct connection, no MCP server needed |
| `onedrive` | OneDrive / SharePoint via M365 MCP — list, download, process files |
| `paypal` | PayPal transaction history via REST API + OAuth2 |
| `sparkasse` | German bank account via FinTS/HBCI — transactions and balances |
| `local_files` | Local filesystem — read files from a directory, filter by glob |

Use connectors with `source.fetch`:

```yaml
- id: fetch_emails
  type: source.fetch
  config:
    connector: outlook
    folder: Inbox
    filter: hasAttachments:true
    top: 50
```

---

## Feature Highlights

### `get_help` + `get_tips` — Brix explains itself to LLMs

```python
mcp__brix__get_tips()                    # quick orientation: Bricks, conventions
mcp__brix__get_help(topic="foreach")     # deep dive on a specific topic
```

Built-in help topics: `quick-start`, `step-referenzen`, `foreach`, `debugging`, `error-patterns`, `credentials`, `triggers`, `dag`, `templates`, `helpers`, `registries`, `sdk`, `anti-patterns`, `beispiele`, `tools`, `lessons-learned`. No documentation lookup needed — the runtime documents itself.

### `plan_pipeline` + `compose_pipeline` — Pipelines from goals

```python
mcp__brix__plan_pipeline(
    goal="classify incoming emails by topic and move to folders"
)
# → [{step: 1, brick: "source.fetch", purpose: "..."}, ...]

mcp__brix__compose_pipeline(
    goal="classify incoming emails by topic and move to folders"
)
# → full pipeline definition, ready to run
```

### `diagnose_run` + `auto_fix_step` — Pipelines that debug themselves

```python
mcp__brix__diagnose_run(run_id="run-2026-0325-abc")
# → [{step_id, error, hint, fix_suggestion, pipeline_context}, ...]

mcp__brix__auto_fix_step(run_id="run-...", step_id="classify")
# → {fixed: true, action: "installed pdfplumber", rerun_hint: "..."}
```

`auto_fix_step` handles the three most common failure classes automatically: missing Python packages (`ModuleNotFoundError` → installs the package), undefined Jinja2 references (adds `| default('')`), and timeouts (doubles the step timeout).

### Breakpoints + Step Replay — Interactive debugging

```yaml
- id: review_intermediate
  type: flow.pipeline
  breakpoint: true   # pauses here, waits for continue_run(run_id)
```

```python
mcp__brix__inspect_context(run_id="run-abc")   # full Jinja2 context while paused
mcp__brix__replay_step(run_id="run-abc", step_id="classify")  # re-run one step
```

### Pin/Mock Testing — n8n-style data pinning

```python
mcp__brix__test_pipeline(
    pipeline_id="classify-emails",
    pin={"fetch": [{"subject": "Invoice", "body": "..."}]},
    mock={"notify": {"status": "sent"}}
)
```

Pin step inputs with known data and mock external calls for deterministic, offline testing. Like n8n's data pinning, but for pipelines.

### Secret Variables — Fernet-encrypted

```python
mcp__brix__set_variable(name="OPENAI_KEY", value="sk-...", secret=True)
# → stored Fernet-encrypted, referenced as {{ vars.OPENAI_KEY }}
```

Secret variables are encrypted at rest, decrypted only at runtime, and never appear in logs or API responses. Distinct from the credential store (which uses UUID references).

### Credential Store — Encrypted secrets, never in plaintext

```python
mcp__brix__credential_add(name="openai-key", value="sk-...", type="api-key")
# → {"uuid": "cred-a1b2c3d4-..."}
```

```yaml
credentials:
  OPENAI_KEY: "cred-a1b2c3d4-..."   # resolved at runtime, never in list/get responses
```

Fernet encryption. Full CRUD: `credential_add`, `credential_list`, `credential_get`, `credential_update`, `credential_delete`, `credential_rotate`, `credential_search`.

### Persistent Store — Data survives runs

```python
mcp__brix__store_set(key="last_import_date", value="2026-03-28")
mcp__brix__store_get(key="last_import_date")    # across runs, across sessions
```

Distinct from `state_set`/`state_get` (ephemeral agent blackboard). The persistent store survives container restarts.

### Profiles, Mixins + Dynamic Dispatch

```python
mcp__brix__list_pipelines(profile="prod")   # prod-specific overrides
```

Profiles override pipeline parameters for different environments (dev/staging/prod) without modifying the pipeline itself. Mixins allow sharing common step sequences across pipelines. Dynamic Dispatch routes steps to different implementations based on runtime conditions. Brick inheritance lets domain bricks extend system bricks with additional behavior.

### DAG Execution — Parallel steps from dependency graph

```yaml
dag: true

steps:
  - id: fetch_invoices
  - id: fetch_contracts
  - id: merge
    depends_on: [fetch_invoices, fetch_contracts]
  - id: classify
    depends_on: [merge]
```

The engine resolves the dependency graph and runs all independent steps concurrently.

### Foreach Primitives

| Feature | What it does |
|---------|-------------|
| `parallel: true` + `concurrency: N` | Process items concurrently |
| `batch_size: N` | Split input into chunks |
| `flatten: true` | Flatten list-of-lists to a single list |
| `fetch_all_pages: true` | Follow pagination automatically (tested at 33k+ items) |
| `--resume <run_id>` | Resume from item-level checkpoint |

### Resilience

| Feature | What it does |
|---------|-------------|
| `on_error: stop / continue / retry` | Per-step error strategy |
| Rate-limit handling | Detects 429, reads Retry-After, waits automatically |
| Circuit breaker | Opens after N failures, half-opens after cooldown |
| Rate limiter | Token-bucket per step/pipeline |
| Step-level cache | Content-addressed, optional TTL |
| Saga pattern | Compensating transactions for multi-step rollback |
| Idempotency key | Prevents duplicate runs within 24h |

### Advanced Flow

| Feature | What it does |
|---------|-------------|
| Event bus | Pipelines emit and consume events between runs |
| Queue | Buffer items for asynchronous processing |
| Debounce | Coalesce rapid-fire triggers into single execution |
| Streaming | Stream step outputs to the caller as they complete |

### Agent Coordination

| Feature | What it does |
|---------|-------------|
| `save_agent_context` / `restore_agent_context` | Session state across context compacts |
| `claim_resource` / `release_resource` | Distributed locking (prevents parallel agent conflicts) |
| `state_set` / `state_get` | Shared blackboard KV-store between sessions |
| `store_set` / `store_get` | Persistent KV-store that survives restarts |

### Observability

| Feature | What it does |
|---------|-------------|
| `persist_output: true` | Writes step output to disk for replay |
| `breakpoint: true` | Pauses pipeline, waits for `continue_run` |
| `replay_step` | Re-execute one step using persisted inputs |
| `inspect_context` | Live Jinja2 context while the pipeline runs |
| `diff_runs` | Compare two runs step-by-step |
| Resource timeline | Peak RSS + duration per step in `get_run_status` |
| `get_insights` | Slow steps, failure patterns, dead helpers, resource regressions |

### Health Check

```python
mcp__brix__server_health()    # all registered MCP servers
```

Returns reachability, tool count, and last-seen for each server. The `brix__health` tool provides a comprehensive system health report including database status, migration state, and brick registry integrity. System pipelines (`_system/`) run automatically on startup to verify connectivity.

### Backup / Restore

```python
mcp__brix__run_pipeline(pipeline_id="system:backup")    # full DB backup
mcp__brix__run_pipeline(pipeline_id="system:restore")   # restore from backup
```

MCP tools for database backup and restore. Backups include all pipelines, helpers, bricks, connectors, credentials, variables, and run history.

### Triggers

| Type | What it does |
|------|-------------|
| `mail` | Fires on new matching emails (M365) |
| `file` | Fires on filesystem events (created, modified) |
| `http_poll` | Polls an endpoint on interval, fires on change |
| `pipeline` | Fires when another pipeline completes |
| `webhook` | `POST /webhook/<name>` — async, HMAC auth, idempotency |

Trigger groups: start/stop multiple triggers together with one call.

### Auto-Tagging + Auto-Version-Bump

Pipelines and helpers are auto-tagged based on their content and org-field hints. Every save triggers an automatic version bump, so the version history is always in sync without manual intervention.

### SSE Transport — MCP Server (Cody-Bridge)

The MCP server supports SSE transport in addition to stdio. This enables persistent connections from browser-based clients and the Cody-Bridge integration. SSE transport runs on a configurable port alongside the stdio server.

### PII Scan Integration

The PII scan tool (`mcp__cody__pii_scan`) is integrated into the Gatekeeper pipeline. Every pipeline and helper saved to the registry is scanned for personally identifiable information (emails, IBANs, phone numbers, credentials, addresses). Findings at BLOCK severity prevent the commit.

### Schema Migrations

Automatic schema migrations on container startup. Each migration is versioned and idempotent. The migration system tracks which migrations have been applied and runs pending ones in order. No manual SQL needed.

---

## Pipeline Format

```yaml
name: classify-emails
description: Classify incoming emails by topic

input:
  folder: { type: string, default: "Inbox" }
  top: { type: integer, default: 50 }

steps:
  - id: fetch
    type: source.fetch
    config:
      connector: outlook
      folder: "{{ input.folder }}"
      top: "{{ input.top }}"

  - id: classify
    type: llm.batch
    input: "{{ fetch.output }}"
    config:
      prompt: "Classify this email: {{ item.subject }} — {{ item.body_preview }}"
      output_field: category
      categories: [invoice, contract, newsletter, other]

  - id: filter_invoices
    type: flow.filter
    input: "{{ classify.output }}"
    config:
      where: "{{ item.category == 'invoice' }}"

  - id: notify
    type: action.notify
    config:
      channel: "#invoices"
      message: "{{ filter_invoices.output | length }} new invoice emails"
```

### Data Flow

| Reference | Description |
|-----------|------------|
| `{{ input.param }}` | Pipeline input (from caller) |
| `{{ step_id.output }}` | Output of a previous step |
| `{{ item }}` | Current element in a `foreach` loop |
| `{{ credentials.name }}` | Credential value (resolved at runtime) |
| `{{ vars.name }}` | Variable value (secret vars decrypted at runtime) |

---

## Architecture

```
Claude Code / MCP Client
        │
        │  mcp__brix__run_pipeline(...)
        ▼
┌─────────────────────────────────────────────┐
│              Brix MCP Server                 │
│                                              │
│  MCP Tools · SSE + stdio transport           │
│  30 Helpers · 59 Bricks · 81 Pipelines      │
│                                              │
│  Pipeline Engine (asyncio)                   │
│  ├─ DB-First: all objects in brix.db        │
│  ├─ Brick Registry (59 bricks, 10 NS)      │
│  ├─ Universal Registry (discover())         │
│  ├─ DAG resolver + parallel executor        │
│  ├─ foreach checkpoints + item resume       │
│  ├─ Rate-limit handling (429 + Retry-After) │
│  ├─ Auto-pagination (follows nextLink)      │
│  ├─ Resilience (CB, RL, Cache, Saga)       │
│  ├─ Advanced Flow (Queue, Events, Stream)  │
│  ├─ unwrap_json + org_registry + PII scan  │
│  ├─ Auto-tagging + Auto-version-bump       │
│  └─ JSON result → MCP response              │
│                                              │
│  Storage (SQLite — brix.db)                  │
│  ├─ Pipelines · Helpers · Bricks · Tools    │
│  ├─ Connectors · Help Topics               │
│  ├─ Object versions (rollback/diff)         │
│  ├─ Credentials (Fernet-encrypted)          │
│  ├─ Secret Variables (Fernet-encrypted)     │
│  ├─ Triggers · Alerts · Scheduler          │
│  ├─ Persistent Store · Profiles            │
│  ├─ Circuit Breaker · Rate Limiter · Cache  │
│  ├─ Saga · Queue · Event Bus               │
│  ├─ Schema Migrations (auto on startup)    │
│  └─ Agent state · Blackboard · Locks        │
└─────────────────────────────────────────────┘
        │
        │  stdio (MCP Python SDK)
        ▼
  Registered MCP Servers
  (m365, markitdown, n8n, docker, ...)
```

**The direction is always Claude → Brix → MCP servers, never the other way around.** Brix is a tool, not an agent. Claude decides what to run. Brix executes and reports back.

---

## Versioning + History

| Feature | What it does |
|---------|-------------|
| Object versioning | Every save of a pipeline or helper writes a version snapshot |
| `get_versions` | Show version history for any pipeline or helper |
| `rollback` | Restore a previous version |
| `diff_versions` | Unified diff between any two versions |
| Run history | Full run log with step timing, errors, and MCP traces |

---

## Security

| Feature | What it does |
|---------|-------------|
| Credential Store | Fernet encryption, UUID references, values never in responses |
| Secret Variables | Fernet encryption, decrypted only at runtime |
| Default-deny API | `BRIX_API_KEY` required for remote access; localhost-only without it |
| Webhook HMAC | Per-pipeline `BRIX_WEBHOOK_SECRET_<NAME>`, constant-time comparison |
| CLI runner | `shell=False` by default; opt-in `shell: true` |
| Jinja2 sandbox | `SandboxedEnvironment` — template injection prevented by design |

---

## Tooling

| Command | What it does |
|---------|-------------|
| `brix validate` | Validate schema, tool names, Jinja2 refs |
| `brix run --dry-run` | Preview rendered params without executing |
| `brix viz` | Generate Mermaid diagram of a pipeline |
| `brix stats --pipeline` | Step-level analytics across all runs |
| `brix bundle export/import` | Package pipeline + helpers as `.brix` archive |
| `brix profile list/add/default` | Environment profiles (dev/staging/prod) |

---

## Claude Code Integration

### 1. MCP Server (recommended)

```bash
claude mcp add brix -- docker exec -i brix-mcp brix mcp
```

Claude now sees all `mcp__brix__*` tools automatically. Every saved pipeline also appears as `mcp__brix__pipeline__<name>`.

### 2. Skills (slash commands)

```bash
cp .claude/commands/*.md ~/.claude/commands/
```

Available skills: `/download-attachments`, `/brix-run`

### Path Convention

Brix runs in Docker. Host filesystem is mounted at `/host/root/`:

```bash
# Host /root/documents → Brix /host/root/documents
```

---

## Output Format

Brix strictly separates result from logs:
- **stdout:** Only the final JSON result — what Claude and MCP clients consume
- **stderr:** Progress, warnings, debug info — for humans and log files

---

## Repo

- `src/brix/` — Engine, Brick Registry, runners, MCP server (stdio + SSE)
- `pipelines/` — 81 pipelines: buddy (34), cody (36), utility (7), system (4) — volume-mounted
- `helpers/` — 30 Python helper scripts (volume-mounted)
- `docs/` — Architecture decisions, integration learnings, expert reviews
- `tests/` — ~3800+ tests
