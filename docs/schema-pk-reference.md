# Schema Primary Key Reference

Extracted from `src/brix/db.py` (+ `credential_store.py`, `api.py`).

## All Tables by PK Pattern

### Pattern A: `id TEXT` PK + `UNIQUE name`

Stable UUID as PK, human-readable name with UNIQUE constraint. **Recommended pattern.**

| Table | PK Column | UNIQUE Column |
|-------|-----------|---------------|
| `pipelines` | `id TEXT` | `name TEXT UNIQUE` |
| `helpers` | `id TEXT` | `name TEXT UNIQUE` |
| `triggers` | `id TEXT` | `name TEXT UNIQUE` |
| `trigger_groups` | `id TEXT` | `name TEXT UNIQUE` |
| `connections` | `id TEXT` | `name TEXT UNIQUE` |
| `credentials` | `id TEXT` | `name TEXT UNIQUE` |
| `registry_templates` | `id TEXT` | `name TEXT UNIQUE` |
| `registry_patterns` | `id TEXT` | `name TEXT UNIQUE` |
| `registry_schemas` | `id TEXT` | `name TEXT UNIQUE` |
| `registry_error_patterns` | `id TEXT` | `name TEXT UNIQUE` |
| `registry_best_practices` | `id TEXT` | `name TEXT UNIQUE` |
| `registry_lessons_learned` | `id TEXT` | `name TEXT UNIQUE` |
| `org_registry` | `id TEXT` | `UNIQUE(entry_type, name)` |

### Pattern B: `name TEXT` PK (no separate id)

Name is the primary key directly. Renaming requires delete+recreate.

| Table | PK Column | Notes |
|-------|-----------|-------|
| `brick_definitions` | `name TEXT` | e.g. `flow.filter` |
| `connector_definitions` | `name TEXT` | e.g. `outlook` |
| `mcp_tool_schemas` | `name TEXT` | tool name as PK |
| `help_topics` | `name TEXT` | topic slug as PK |
| `variables` | `name TEXT` | variable name as PK |
| `profiles` | `name TEXT` | profile name as PK |

### Pattern C: Compound PK

Two or more columns form the primary key together.

| Table | PK Columns | Notes |
|-------|------------|-------|
| `pipeline_helpers` | `(pipeline_id, helper_id)` | junction table |
| `trigger_state` | `(trigger_id, dedupe_key)` | dedup tracking |
| `deprecated_usage` | `(pipeline_name, step_id)` | migration tracking |
| `keyword_taxonomies` | `(category, keyword)` | lookup table |
| `type_compatibility` | `(output_type, compatible_input)` | lookup table |
| `step_pins` | `(pipeline_name, step_id)` | test data pins |

### Pattern D: Domain-specific PK

PK uses a domain-specific column name (not `id` or `name`).

| Table | PK Column | Notes |
|-------|-----------|-------|
| `runs` | `run_id TEXT` | UUID run identifier |
| `run_inputs` | `run_id TEXT` | 1:1 extension of `runs` |
| `agent_sessions` | `session_id TEXT` | agent session UUID |
| `resource_locks` | `resource_id TEXT` | distributed lock |
| `shared_state` | `key TEXT` | KV store |
| `persistent_store` | `key TEXT` | KV store |
| `trigger_meta` | `trigger_id TEXT` | 1:1 extension of `triggers` |
| `circuit_breaker_state` | `brick_name TEXT` | FK-like to brick_definitions |
| `rate_limiter_state` | `brick_name TEXT` | FK-like to brick_definitions |
| `brick_cache` | `cache_key TEXT` | TTL cache |
| `queue_buffer` | `queue_name TEXT` | named queue |
| `debounce_state` | `trigger_name TEXT` | debounce tracking |
| `idempotency_keys` | `key TEXT` | SSE idempotency (separate DB) |

### Pattern E: `id TEXT` PK, no UNIQUE name

Auto-generated UUID PK, but no unique name constraint.

| Table | PK Column | Notes |
|-------|-----------|-------|
| `object_versions` | `id TEXT` | version snapshots |
| `audit_log` | `id TEXT` | append-only log |
| `app_log` | `id TEXT` | append-only log |
| `step_outputs` | `id TEXT` | run output data |
| `alert_rules` | `id TEXT` | has `name` but NOT UNIQUE |
| `alert_history` | `id TEXT` | append-only history |
| `step_executions` | `id TEXT` | execution records |
| `foreach_item_executions` | `id TEXT` | execution records |
| `event_bus` | `id TEXT` | transient events |

### Pattern F: `INTEGER PRIMARY KEY AUTOINCREMENT`

| Table | PK Column | Notes |
|-------|-----------|-------|
| `pipeline_events` | `id INTEGER AUTOINCREMENT` | only table with auto-increment |

### Separate DB: `schema_migrations`

| Table | PK Column | Notes |
|-------|-----------|-------|
| `schema_migrations` | `version INTEGER` | migration tracking |

## Summary

| Pattern | Count | Tables |
|---------|-------|--------|
| A: `id` PK + UNIQUE name | 13 | Core entities with stable UUIDs |
| B: `name` PK | 6 | Definition/config tables |
| C: Compound PK | 6 | Junction/lookup tables |
| D: Domain-specific PK | 13 | State/extension tables |
| E: `id` PK, no UNIQUE name | 9 | Log/execution tables |
| F: AUTOINCREMENT | 1 | `pipeline_events` |
| **Total** | **48** | |

## Recommendation

Long-term, all **core entity tables** (Patterns B and the `alert_rules` anomaly in E) should migrate to **Pattern A** (`id TEXT PRIMARY KEY` + `UNIQUE name`):

1. **Pattern B tables** (`brick_definitions`, `connector_definitions`, `mcp_tool_schemas`, `help_topics`, `variables`, `profiles`) -- adding a UUID `id` column as PK and demoting `name` to UNIQUE would allow renaming without breaking foreign-key references (e.g. `circuit_breaker_state.brick_name`, `rate_limiter_state.brick_name`).

2. **`alert_rules`** -- already has `id TEXT PK` but `name` lacks a UNIQUE constraint. Adding `UNIQUE` on `name` would align it with Pattern A.

3. **Pattern C, D, E, F tables** are fine as-is -- junction tables, KV stores, append-only logs, and extension tables have legitimate reasons for their PK patterns.

**Migration priority:** `brick_definitions` and `connector_definitions` first (most referenced by other tables), then `variables` and `profiles` (user-facing), then `mcp_tool_schemas` and `help_topics` (low-risk).
