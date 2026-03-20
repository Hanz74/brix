# DX Review Brix v2 — Prof. Dr. Lisa Bergmann (UX/CX)

**Date:** 2026-03-20

## Key Insight

> "The v2 interface succeeds when Claude can build and run a pipeline in
> 2 tool calls — without ever calling list_bricks or get_brick_schema.
> Templates + inline steps in create_pipeline + instant validation make
> this possible."

## P0 Recommendations

### 1. create_pipeline accepts steps inline + validates immediately
Primary flow: one call to create + validate. Not 5 sequential add_step calls.

### 2. get_pipeline tool is a v2.0 blocker
Claude needs to inspect half-built pipelines. Without this, build state is a black box.

### 3. Agent-optimized tool descriptions
Each tool description must answer in one sentence: What does it do? When do I need it? What do I get back?

### 4. Dual-Layer Error Schema
```json
{
  "recoverable": true,
  "agent_actions": ["retry_step", "skip_step", "abort_pipeline"],
  "resume_command": "brix__run_pipeline({resume_from: 'download'})"
}
```

## P1 Recommendations

- search_bricks with keyword + category (100+ bricks need search)
- when_to_use field per brick ("use when you need to call a REST API")
- Anomaly detection: >50% foreach failure = config problem, not item problem

## P2 Recommendations

- get_template: 5-6 quickstart templates covering 80% of use cases
- Structured progress stream for human observers
- check_pipeline_health / schema drift detection
