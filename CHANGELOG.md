# Changelog

## [2.1.0] — 2026-03-20

### Added
- MCP Server Grundgerüst (stdio transport) — `src/brix/mcp_server.py`
- 13 tool stubs registered: brix__get_tips, brix__list_bricks, brix__search_bricks, brix__get_brick_schema, brix__create_pipeline, brix__get_pipeline, brix__add_step, brix__remove_step, brix__validate_pipeline, brix__run_pipeline, brix__get_run_status, brix__get_run_history, brix__list_pipelines
- `brix mcp` CLI command to launch MCP server mode
- Docker dual-mode: CLI (default) + MCP via BRIX_MODE=mcp environment variable
- `brix-mcp` service in docker-compose.yml for dedicated MCP server container

## [2.0.1] — 2026-03-20

### Added
- BrickRegistry: central registry with built-in loading, register/unregister, search, category filter
- MCP auto-discovery: `discover_mcp_bricks` + `discover_all_mcp_servers` from SchemaCache
- 26 tests covering all registry operations including MCP schema-to-BrickParam mapping

## [2.0.0] — 2026-03-20

### Added
- Brix v2: Brick Schema System (Pydantic → JSON Schema)
- 10 built-in brick type definitions with agent-optimized descriptions
- BrickSchema with when_to_use field for AI agent discovery

## [0.6.5] — 2026-03-19

### Added
- Broad fetch strategy pipeline (download-attachments-broad)
- Keyword filter helper for local mail filtering
- Skill-prompt with strategy selection (targeted vs broad)

## [0.6.4] — 2026-03-19

### Added
- PDF-only filter for download-attachments
- Parallel attachment fetching (5x speedup)

## [0.6.2] — 2026-03-19

### Added
- Skill-prompt migration: /download-attachments
- Generic /brix-run skill

This completes the Brix v0.6.x implementation — all 26 tasks done.

## [0.6.0] — 2026-03-19

### Added
- First complete pipeline: /download-attachments
- Helper scripts: extract URLs, structured save, summary report
- Test fixture with mock M365 data

## [0.5.2] — 2026-03-19

### Added
- Testing framework: mock fixtures, assertions, brix test command

## [0.5.0] — 2026-03-19

### Added
- SQLite run history (D-09)
- brix history and brix stats commands
- Run tracking integrated into pipeline engine

## [0.4.2] — 2026-03-19

### Added
- Dry-run: credential env-var checks (✓/NOT SET), server registration checks (D-18)
- Dry-run: rendered parameters where possible, [depends on X.output] for cross-step refs
- Dry-run: when-condition evaluation with current input context
- Conditional steps: complex Jinja2 expressions fully supported ({{ input.count > 5 }}, {{ items | length > 0 }}) (D-16)
- Explicit pipeline output field mapping with Jinja2 rendering (D-21)

## [0.4.0] — 2026-03-19

### Added
- Sub-pipeline runner: pipeline composition (D-17)
- Same-process execution with shared connection pool

## [0.3.1] — 2026-03-19

### Added
- MCP schema caching with TTL-based invalidation
- brix server add/list/remove/test/tools/refresh commands

## [0.3.0] — 2026-03-19

### Added
- MCP runner: tool calls via stdio protocol (MCP Python SDK)
- Server configuration loading from ~/.brix/servers.yaml
- docker exec -i support for container-based MCP servers

## [0.2.3] — 2026-03-19

### Added
- Retry logic with linear/exponential backoff (`on_error: retry`, `RetryConfig`)
- Default 60s timeout for steps without explicit timeout (via `parse_timeout`)

## [0.2.2] — 2026-03-19

### Added
- foreach + parallel execution with concurrency limits (D-15 partial-success)

## [0.2.0] — 2026-03-19

### Added
- Python runner: script execution via subprocess (D-19)

## [0.1.2] — 2026-03-19

### Added
- Click CLI with `brix run`, `brix validate`, `--dry-run`
- Parameter passing via `-p key=value`
- Pipeline execution with JSON output on stdout
- Dry-run mode showing step details

## [0.1.1] — 2026-03-19

### Changed
- Implemented Pydantic models for Pipeline, Step, StepResult, RunResult, ServerConfig

## [0.1.0] — 2026-03-19

### Added
- Initial project structure
- pyproject.toml with Hatchling build system
- Source layout under src/brix/
- Test infrastructure with pytest + pytest-asyncio
