# Changelog

## [2.6.4] — 2026-03-20

### Added (T-BRIX-V2-20)
- Warning when unknown input parameters are passed to `run_pipeline` (MCP + REST API)
- `mcp_server.py` `_handle_run_pipeline`: detects params not in `pipeline.input` schema, appends to `warnings` list in result dict
- `api.py` `run_pipeline`: same check, `warnings` field added to JSON response
- 1 new test in `tests/test_mcp_server.py` — `test_run_pipeline_unknown_params_warning`

## [2.6.2] — 2026-03-20

### Added
- E2E tests validating full v2 workflow
- MCP → REST API → Pipeline Auto-Exposure chain tested

Brix v2 complete — 18/18 tasks done.

## [2.6.0] — 2026-03-20

### Added (Wave 7 — T-BRIX-V2-16)
- `src/brix/security.py` — `SecurityConfig` class with YAML-based allowlists and shell enforcement
- Allowlist checks: `check_container`, `check_executable`, `check_script_path`, `check_shell_mode`
- Configurable via `~/.brix/security.yaml` (optional — open by default when absent)
- `enforce_shell_false: true` default — blocks `shell=True` unless explicitly disabled in config
- Empty allowlists treated as "no restriction" (open by default)
- 10 new tests in `tests/test_security.py` covering all check methods and edge cases

## [2.5.0] — 2026-03-20

### Added
- V2-14 Pipeline Templates: `src/brix/templates/` package with `catalog.py`
- 5 predefined pipeline templates covering ~80% of common use cases:
  - `http-download` — Fetch list from API, download each item in parallel
  - `mcp-fetch-process` — Fetch from MCP server, process with Python, save
  - `batch-convert` — Read files from folder, convert in parallel, save results
  - `filter-export` — Fetch data, filter by condition, export results
  - `multi-source-merge` — Fetch from multiple sources, merge, generate report
- `get_template(goal)` — Keyword-based template lookup by goal description
- `list_templates()` — Returns all templates with name, description, step count
- `brix__get_template` MCP tool — Accepts optional `goal` param; returns matching template or lists all
- 14 new tests in `tests/test_templates.py`

## [2.4.0] — 2026-03-20

### Added
- V2-12 MCP HTTP/SSE Transport: `run_mcp_http_server(host, port)` in `src/brix/mcp_server.py`
- Starlette ASGI app with two transport paths:
  - `GET/POST /mcp` — StreamableHTTP (primary, via `StreamableHTTPSessionManager`)
  - `GET /sse` + `POST /messages` — Legacy SSE transport (for older MCP clients)
- `brix mcp --transport http --port 8091 --host 0.0.0.0` CLI option (extends existing `brix mcp` command)
- `brix-mcp` service in `docker-compose.yml` now exposes port 8091 with `BRIX_MCP_TRANSPORT=stdio` env var
- 3 new tests in `tests/test_mcp_http.py`: `test_http_transport_available`, `test_http_server_imports`, `test_http_tool_call_via_lifespan`

## [2.3.0] — 2026-03-20

### Added
- V2-10 REST API: `src/brix/api.py` — Starlette app with endpoints: `GET /health`, `GET /pipelines`, `POST /run/{name}`, `GET /status/{run_id}`, `POST /webhook/{name}`
- V2-11 Webhook + Cron: Webhook endpoint with optional per-pipeline secret (`BRIX_WEBHOOK_SECRET_<NAME>`), `src/brix/scheduler.py` — `BrixScheduler` with interval parsing, YAML-based schedule config (`~/.brix/schedules.yaml`)
- `brix api` CLI command to start REST API server (default port 8090)
- `brix scheduler` CLI command to start cron scheduler
- `brix-api` service in `docker-compose.yml` (port 8090)
- Optional `BRIX_API_KEY` header auth for all protected endpoints
- 27 new tests: `tests/test_api.py` (18) + `tests/test_scheduler.py` (17)
- `starlette>=0.40` and `uvicorn>=0.30` added as explicit dependencies

## [2.2.0] — 2026-03-20

### Added
- V2-08 Pipeline Store: `src/brix/pipeline_store.py` — `PipelineStore` class with `save`, `load`, `load_raw`, `exists`, `list_all`, `delete`, `get_version`
- V2-09 Auto-Exposure: Saved pipelines automatically registered as `brix__pipeline__<name>` MCP tools (dynamic, no server restart needed)
- `_build_pipeline_tools()` — builds MCP tool definitions from pipeline input schemas
- `_handle_pipeline_tool()` — executes a named pipeline via its auto-exposed tool
- `PIPELINE_TOOL_PREFIX` constant (`brix__pipeline__`)
- `create_server(store=...)` now accepts an optional `PipelineStore` (for testing/injection)
- 17 new tests: `tests/test_pipeline_store.py` (13) + appended to `tests/test_mcp_server.py` (4 classes, 13 tests)

## [2.1.1] — 2026-03-20

### Added
- V2-05 Discovery: `_handle_get_tips`, `_handle_list_bricks`, `_handle_search_bricks`, `_handle_get_brick_schema` — real implementations replacing stubs
- V2-06 Builder: `_handle_create_pipeline` (with inline steps + immediate validation, Lisa P0), `_handle_get_pipeline`, `_handle_add_step` (with `position` support), `_handle_remove_step`, `_handle_validate_pipeline`
- V2-07 Execution: `_handle_run_pipeline` (dual-layer error schema), `_handle_get_run_status`, `_handle_get_run_history` (with pipeline filter), `_handle_list_pipelines` (custom directory support)
- 26 new integration tests across `TestDiscoveryHandlers`, `TestBuilderHandlers`, `TestExecutionHandlers`
- Pipeline persistence in `~/.brix/pipelines/*.yaml`

### Changed
- Removed `TestStubReturnsNotImplemented` test class (stubs replaced by real handlers)

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
