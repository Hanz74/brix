# v3 Agent Reviews — Consolidated Findings

**Date:** 2026-03-22
**Context:** 33K mail import for Buddy project exposed scalability limits

## Alex + Flowstein: Architecture

### P0 — Breaking if not done now
1. **MCP Connection Pooling**: 99K subprocess-spawns at 33K mails. Move session lifecycle from Runner to Engine/Context scope. ~33 min wasted on connection overhead alone.
2. **Step-Output Memory Model**: 33K items as in-memory dict + JSON blob in SQLite + Jinja2 context = explosion. Need lazy loading via JSONL files.

### P1 — Hard to retrofit later
3. **foreach-Checkpoint**: Item-level resume, not just step-level. JSONL checkpoint per foreach step.
4. **Rate-Limit-Handling**: 429 + Retry-After header. fetchAllPages without this = guaranteed crash.

### P2 — Natural extensions
5. **batch_size primitive**: `batch_size: 100` on foreach — chunks + checkpoint after each batch.
6. **if/else syntax**: Complementary when as sugar. Decide now before YAML syntax is frozen.

### P3 — Optimization
7. **Jinja2 Context Cache**: Lazy rebuild on set_output, not rebuild-all per step.

## Lisa + Bruckner: DX + Ops

### Blockers
1. **Workdir /tmp → ~/.brix/runs/**: /tmp dies on container restart. Resume broken.
2. **Async dispatch**: run_pipeline must return run_id immediately. No 6h HTTP/MCP timeout.

### High
3. **Live run status**: get_run_status reads run.json from workdir, not just SQLite (which only has post-mortem data).
4. **foreach-Results as JSONL**: 33MB blob in steps_data column kills brix history.

### Medium
5. **foreach_progress is dead code**: ProgressReporter.foreach_progress never called in engine.
6. **resume_run_id in MCP**: Resume feature exists but not exposed via MCP tools.
7. **Docker log rotation**: 6h run fills default 10MB limit.
8. **Heartbeat in run.json**: Distinguish hung vs running (last_heartbeat_at timestamp).
