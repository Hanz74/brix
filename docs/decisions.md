# Architecture Decisions

**Date:** 2026-03-19
**Status:** All decisions finalized, ready for implementation planning

This document consolidates all architecture decisions made during the ideation phase, incorporating findings from four expert reviews:
- **Alex (Spec-Wächter):** Requirements review — security, data flow contracts, edge cases
- **Prof. Dr. Bruckner (DevOps):** Packaging, Docker integration, CLI setup
- **MCP SDK Research:** stdio client API, ClientSessionGroup, error handling
- **Claude Code Skills Research:** Slash command format, arguments, tool permissions

---

## Confirmed Decisions

### D-01: Token savings as primary driver

Brix is a Python runtime, not a prompt library. Claude calls `brix run pipeline.yaml` once. Brix handles everything internally. 10 sequential tool calls → 1 tool call.

**Rejected alternative:** YAML-only approach where Claude reads the recipe and orchestrates itself. No token savings — Claude still makes every tool call individually.

### D-02: Direction is always Claude → Brix

Brix is a tool, not an agent. Claude provisions Brix (server configs, credentials), starts pipelines, consumes results. Brix never initiates communication toward Claude.

### D-03: Docker container deployment

Brix runs as a Docker container with socket mounting for `docker exec -i` access to other containers.

```yaml
services:
  brix:
    build: .
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ~/.brix:/root/.brix
```

A wrapper script at `/usr/local/bin/brix` provides transparent CLI access.

**Rejected alternative:** Direct host installation. Inconsistent with existing infrastructure where everything runs in Docker.

**Source:** Prof. Bruckner review + user preference.

### D-04: Four runner types — python, http, cli, mcp

| Type | Execution |
|------|-----------|
| `python` | subprocess (always isolated) |
| `http` | httpx async |
| `cli` | asyncio subprocess |
| `mcp` | stdio via MCP Python SDK |

Plus `pipeline` as a meta-type for sub-pipeline composition.

**Rejected types:**
- `transform` — python covers it
- `db` — python script covers it
- `docker` — `cli` with `docker exec` covers it

**Source:** Initial design + Alex review (H-7: subprocess only for Python runner).

### D-05: MCP integration via stdio protocol

Using the official MCP Python SDK (`mcp` package). The SDK's built-in `ClientSessionGroup` handles multi-server connection pooling — no custom implementation needed.

```python
async with ClientSessionGroup() as group:
    await group.connect_to_server(StdioServerParameters(command="server-a"))
    await group.connect_to_server(StdioServerParameters(command="server-b"))
    result = await group.call_tool("tool-name", {...})
```

For container-based MCP servers, `docker exec -i` provides transparent stdio bridging:

```yaml
# servers.yaml
m365:
  command: docker
  args: ["exec", "-i", "m365-mcp", "node", "/app/index.js"]
```

**Rejected alternatives:**
- HTTP direct (duplicate work — MCP servers already wrap APIs)
- MetaMCP gateway (unclear API, unnecessary dependency)
- Brix as MCP server (multiple tool calls again, no token savings)

**Source:** Initial design + MCP SDK research + Bruckner review.

### D-06: YAML + Jinja2 pipeline format

Pipelines defined as YAML. Jinja2 for step-to-step data flow references (`{{ step.output }}`). Rendering is **lazy** — templates resolve when the step executes, not at load time.

**Rejected alternatives:**
- Python DSL (harder to read, validate, generate)
- JSON (no comments, poor readability)

### D-07: Build system — Hatchling + uv

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

Dependencies use `>=` lower bounds only. `uv.lock` handles reproducibility.

**Source:** Prof. Bruckner review.

### D-08: CLI framework — Click

Click over Typer for complex command hierarchies. Async bridge: Click (sync) → `asyncio.run()` → async pipeline engine.

**Source:** Prof. Bruckner review.

### D-09: History storage — SQLite

`~/.brix/history.db` using Python's stdlib `sqlite3`. Queryable for `brix history` and `brix stats`. No new dependency.

**Rejected alternative:** YAML/JSON flat files (not queryable, no atomic writes).

**Source:** Prof. Bruckner review.

### D-10: MCP schema caching

Tool schemas cached locally after `brix server add`. Enables offline validation and faster startup.

Invalidation triggers: server update, manual refresh (`brix server refresh`), schema hash mismatch, TTL expiry (configurable, default 7 days).

### D-11: Credentials from environment variables

`BRIX_CRED_*` env vars. No built-in credential management, token refresh, or OAuth flows in v1. Extensible later.

### D-12: Skills integration via Claude Code slash commands

Skills use frontmatter with `allowed-tools: Bash(brix:*)` for pre-approved execution. Arguments via `$ARGUMENTS`. JSON output parsed by Claude natively.

```yaml
---
description: Download M365 attachments via Brix pipeline
argument-hint: <search-query> [--output DIR]
allowed-tools: Bash(brix:*)
---
```

Existing `/download-attachments` skill at `~/.claude/commands/download-attachments.md` serves as first migration target.

**Source:** Claude Code Skills research.

---

## New Decisions (from expert reviews)

### D-13: Jinja2 security — user input is never templated

User input (`$ARGUMENTS`, `--query`, etc.) is passed as raw strings into the pipeline context. Only pipeline-internal references (`{{ step.output }}`, `{{ input.x }}`) go through Jinja2. User-controlled values are never processed as Jinja2 templates.

This eliminates template injection entirely without needing `SandboxedEnvironment`.

**Source:** Alex review C-1. Defense in depth: `SandboxedEnvironment` is still used as an additional safety layer, but the primary protection is that user input bypasses Jinja2.

### D-14: CLI runner — args list by default, shell string opt-in

Two modes in pipeline YAML:

```yaml
# Safe default: args list (shell=False)
- id: convert
  type: cli
  args: ["markitdown", "{{ item.path }}"]

# Opt-in: shell string (shell=True) — when piping/globbing needed
- id: process
  type: cli
  command: "cat {{ item.path }} | grep 'pattern'"
  shell: true
```

Default is `args` (safe). `command` + `shell: true` is explicit opt-in with documented risk.

**Source:** Alex review C-2.

### D-15: Partial-success output schema for foreach

When `on_error: continue` is set, all items are included in output with a `success` flag:

```json
{
  "items": [
    { "success": true, "data": { "path": "/tmp/file1.pdf" } },
    { "success": true, "data": { "path": "/tmp/file2.pdf" } },
    { "success": false, "error": "404 Not Found", "input": { "url": "..." } }
  ],
  "summary": { "total": 3, "succeeded": 2, "failed": 1 }
}
```

Downstream steps receive all items and can filter. This is the defined contract.

**Source:** Alex review H-1.

### D-16: Skipped steps return default value

When a step is skipped (via `when: false`), referencing its output requires Jinja2's `| default()` filter:

```yaml
converted: "{{ convert.output | default([]) }}"
```

Without `| default()`, referencing a skipped step's output raises a validation error. `brix validate` catches this.

**Source:** Alex review H-6.

### D-17: Sub-pipelines run in same process

Sub-pipelines execute in the same asyncio event loop, sharing the `ClientSessionGroup` connection pool. This is faster and allows MCP connection reuse.

Sub-pipeline output is available as `{{ sub_step_id.output.result }}` — mapping to the sub-pipeline's final result.

**Source:** Alex review H-2.

### D-18: Dry-run shows rendered parameters without executing

`brix run --dry-run` resolves Jinja2 templates with provided input, lists all steps with their rendered parameters, required servers, and credentials — but executes nothing.

```
Step 1: fetch_mails (mcp → m365:list-mail-messages)
  params: { folder: "Inbox", filter: "invoices" }
Step 2: extract (python → helpers/extract_urls.py)
  input: fetch_mails.output [not yet available]
Step 3: download (http GET, foreach, parallel:5)
  url: [depends on extract.output]
...
Requires: MCP server "m365", credential "m365_token"
```

Steps that depend on previous outputs show `[depends on X.output]` since the data doesn't exist yet.

**Source:** Alex review H-5.

### D-19: Python runner always uses subprocess

The Python runner always executes scripts via `subprocess` — never `importlib`. This provides process isolation, prevents scripts from affecting Brix internals, and makes error handling predictable.

Convention: scripts read JSON from `sys.argv[1]` or stdin, write JSON to stdout.

**Rejected alternative:** `importlib` (shared memory, no isolation, security risk).

**Source:** Alex review H-7.

### D-20: Workdir cleanup policy

| Condition | Action |
|-----------|--------|
| Successful run | Auto-delete workdir |
| Failed run | Keep 24h for `--resume` and debugging |
| `--keep-workdir` flag | Never auto-delete |
| Manual | `brix clean --older-than 24h` |

**Source:** Alex review C-3 + Bruckner review.

### D-21: Pipeline output field

Pipelines declare an explicit `output` mapping:

```yaml
output:
  files: "{{ save.output }}"
  converted: "{{ convert.output | default([]) }}"
```

This defines what appears in the `result` field of the JSON response. Without `output`, the last step's output is used as the result.

**Source:** Alex review M-7.

---

## Summary

| Category | Count | Key themes |
|----------|-------|------------|
| Core architecture | 6 | Token savings, direction, runners, pipeline format, MCP, skills |
| Infrastructure | 4 | Docker, Hatchling, Click, SQLite |
| Security | 3 | Jinja2 sandboxing, CLI escaping, subprocess isolation |
| Data contracts | 3 | Partial-success, skipped steps, pipeline output |
| Execution model | 3 | Sub-pipelines, dry-run, workdir cleanup |
| Integration | 2 | Schema caching, credentials |

**Total: 21 decisions documented.** All finalized, ready for implementation planning.
