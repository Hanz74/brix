# Brix v2 Review — Alex (Spec) + Flowstein (n8n-Architekt)

**Date:** 2026-03-20
**Scope:** MCP Server approach, scope, risks, n8n differentiation

## Verdict

**MCP-Server approach: YES — but not yet.**

Build 3-5 more pipelines in v1 first. Recognize the patterns. Then abstract.
Premature abstraction is more expensive than YAML boilerplate.

## n8n Differentiation

> "n8n is for humans who build workflows. Brix is for Claude who executes pipelines."

No competition. Completely different target audiences.

| | n8n | Brix |
|---|-----|------|
| Target | Business users, ops teams | Claude Code (an agent, not a human) |
| Interface | Browser, visual canvas | MCP tools, JSON in/out |
| Trigger | Webhooks, schedule, events | Claude calls run_pipeline |
| Auth | Credentials vault, OAuth | ENV variables |
| Scale | Multi-user, teams, enterprise | Single-session, local |

## v2.0 Minimal Scope (when ready)

### 6 MCP Tools
```
brix__list_bricks        — Available step types
brix__get_brick_schema   — What does brick X expect?
brix__create_pipeline    — New pipeline (name + description)
brix__add_step           — Add step to pipeline
brix__run_pipeline       — Execute pipeline
brix__get_run_status     — Last run status
```

### 6 P0 Built-in Bricks
| Brick | Priority |
|-------|----------|
| `http_get` / `http_post` | P0 |
| `run_cli` | P0 |
| `python_eval` | P0 |
| `file_read` / `file_write` | P0 |
| `mcp_call` | P0 |

### Later (v2.1+)
- `json_transform`, `filter`, `parallel` (P1)
- `docker_exec`, `db_query` (P2)
- list_pipelines, clone, validate, run_history
- Sub-pipelines, webhook triggers, UI

## Key Architecture Insight (Flowstein)

The core investment is the **Schema System**, not the builder.
Pydantic models as canonical brick schemas, auto-exportable as JSON Schema.
This is what makes Claude configure bricks correctly — not documentation.

## Risks

1. **"Rebuilding n8n, only worse"** — Every feature must have a Claude Code use case
2. **MCP Server complexity** — 200-500 LOC boilerplate + registry + store + executor
3. **Execution engine scope** — v2.0 sequential only, parallel in v2.1

## Market Research (March 2026)

Nothing fills the Brix niche:
- **MCPStack** — MCP-only tool chaining, no Python/CLI/HTTP, no persistence
- **mcp-agent** — Agent framework, agent decides (not tool-driven)
- **n8n** — UI-first visual builder for business users

The gap: **No tool lets Claude Code build and execute pipelines via MCP calls
with a registry of configurable building blocks.**

## Implementation Order (when ready)
1. Brick Schema System (Pydantic → JSON Schema)
2. Registry (list_bricks, get_brick_schema)
3. Pipeline Store (YAML files in ~/.brix/pipelines/)
4. 6 P0 bricks
5. MCP Server wrapper (thin layer)
6. create_pipeline + add_step + run_pipeline
