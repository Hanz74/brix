# Legacy Classification

Scan scope:
- `src/brix/engine.py`
- `src/brix/engine_types.py`
- `src/brix/startup_sync.py`
- `src/brix/seed.py`
- `src/brix/bricks/builtins.py`
- compat views in `src/brix/migrations.py`
- CLI commands in `src/brix/cli.py`

Notes:
- `src/brix/engine_types.py` does not define `LEGACY_ALIASES`.
- Status meanings:
  - `COMPATIBLE`: keep working as a supported compatibility surface
  - `DEPRECATED`: still works, but code already warns or clearly signals migration
  - `REMOVE`: dead shim or migration-only surface that should go in the next major

| Legacy Element | Type | Status | Action |
|---|---|---|---|
| `python` -> `script.python` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `script.python`. |
| `http` -> `http.request` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `http.request`. |
| `mcp` -> `mcp.call` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `mcp.call`. |
| `cli` -> `script.cli` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `script.cli`. |
| `filter` -> `flow.filter` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.filter`. |
| `transform` -> `flow.transform` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.transform`. |
| `set` -> `flow.set` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.set`. |
| `repeat` -> `flow.repeat` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.repeat`. |
| `choose` -> `flow.choose` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.choose`. |
| `parallel` -> `flow.parallel` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.parallel`. |
| `pipeline` -> `flow.pipeline` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.pipeline`. |
| `pipeline_group` -> `flow.pipeline_group` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.pipeline_group`. |
| `validate` -> `flow.validate` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.validate`. |
| `notify` -> `action.notify` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `action.notify`. |
| `approval` -> `action.approval` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `action.approval`. |
| `specialist` -> `extract.specialist` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `extract.specialist`. |
| `db_query` -> `db.query` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `db.query`. |
| `db_upsert` -> `db.upsert` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `db.upsert`. |
| `db_exec` -> `db.exec` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `db.exec`. |
| `llm_batch` -> `llm.batch` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `llm.batch`. |
| `markitdown` -> `markitdown.convert` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `markitdown.convert`. |
| `source` -> `source.fetch` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `source.fetch`. |
| `switch` -> `flow.switch` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.switch`. |
| `merge` -> `flow.merge` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.merge`. |
| `error_handler` -> `flow.error_handler` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.error_handler`. |
| `wait` -> `flow.wait` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.wait`. |
| `dedup` -> `flow.dedup` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.dedup`. |
| `aggregate` -> `flow.aggregate` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.aggregate`. |
| `flatten` -> `flow.flatten` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.flatten`. |
| `diff` -> `flow.diff` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `flow.diff`. |
| `respond` -> `action.respond` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `action.respond`. |
| `file_read` -> `file.read` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `file.read`. |
| `file_read_base64` -> `file.read_base64` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `file.read_base64`. |
| `file_write` -> `file.write` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `file.write`. |
| `file_list` -> `file.list` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `file.list`. |
| `file_load_json` -> `file.load_json` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `file.load_json`. |
| `keyword_filter` -> `filter.keyword` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `filter.keyword`. |
| `extract_url` -> `extract.url` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `extract.url`. |
| `extract_ics` -> `extract.ics` | Engine alias | DEPRECATED | Keep resolving, but migrate pipelines to `extract.ics`. |
| `Path("/app/pipelines")` in `_PIPELINE_SEARCH_PATHS` | Startup legacy path | COMPATIBLE | Keep as a compatibility import/scan location until container users fully stop relying on mounted pipeline YAMLs. |
| `_migrate_pipeline_steps()` | Startup compat wrapper | REMOVE | Delete in the next major; normalization already happens in `_sync_pipelines()`. |
| `_backfill_descriptions()` | Startup compat wrapper | REMOVE | Delete in the next major; descriptions now live on the pipeline row. |
| `_seed_from_code()` | Seed legacy fallback | REMOVE | Remove the dead code path; `seed_if_empty()` now hard-requires `seed-data.json`. |
| `_seed_brick_definitions()` | Seed legacy fallback helper | REMOVE | Remove with `_seed_from_code()`. |
| `_seed_connector_definitions()` | Seed legacy fallback helper | REMOVE | Remove with `_seed_from_code()`. |
| `_seed_mcp_tool_schemas()` | Seed legacy fallback helper | REMOVE | Remove with `_seed_from_code()`. |
| `_seed_help_topics()` | Seed legacy fallback helper | REMOVE | Remove with `_seed_from_code()`. |
| `_seed_keyword_taxonomies()` | Seed legacy fallback helper | REMOVE | Remove with `_seed_from_code()`. |
| `_seed_type_compatibility()` | Seed legacy fallback helper | REMOVE | Remove with `_seed_from_code()`. |
| `LEGACY_STEP_TYPE_MAP` | Seed migration map | REMOVE | Delete in the next major; the migration entrypoint is already a no-op. |
| `_migrate_steps_in_list()` | Seed migration helper | REMOVE | Delete in the next major; no active caller should rely on YAML step rewriting here. |
| `migrate_legacy_step_types()` | Seed migration shim | REMOVE | Delete in the next major; function is explicitly deprecated and always returns `0`. |
| `Path("/app/pipelines")` in `_PIPELINE_IMPORT_DIRS` | Seed legacy import path | COMPATIBLE | Keep as a one-way import source until all deployments are DB-only. |
| `Path("/app/helpers")` in `_HELPER_IMPORT_DIRS` | Seed legacy import path | COMPATIBLE | Keep as a one-way import source until all helper code is DB-backed or managed in `~/.brix/helpers`. |
| `http_get` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer canonical namespaced HTTP bricks for new definitions. |
| `http_post` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer canonical namespaced HTTP bricks for new definitions. |
| `run_cli` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `script.cli` for new definitions. |
| `python_script` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `script.python` for new definitions. |
| `file_read` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `file.read` for new definitions. |
| `file_write` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `file.write` for new definitions. |
| `mcp_call` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `mcp.call` for new definitions. |
| `filter` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `flow.filter` for new definitions. |
| `transform` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `flow.transform` for new definitions. |
| `sub_pipeline` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `flow.pipeline` for new definitions. |
| `specialist` | Legacy flat builtin brick | COMPATIBLE | Keep resolving, but prefer `extract.specialist` for new definitions. |
| `agent_sessions` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `alert_rules` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `brick_definitions` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `connections` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `connector_definitions` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `foreach_item_executions` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `help_topics` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `helpers` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `keyword_taxonomies` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `mcp_tool_schemas` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `object_versions` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `pipeline_events` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `pipeline_helpers` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `pipelines` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `profiles` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `registry_best_practices` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `registry_error_patterns` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `registry_lessons_learned` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `registry_patterns` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `registry_schemas` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `registry_templates` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `resource_locks` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `run_inputs` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `runs` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `schema_migrations` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `step_executions` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `step_outputs` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `step_pins` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `tips` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `trigger_groups` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `triggers` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `variables` | Compat view | COMPATIBLE | Keep view for old plural-table readers. |
| `brix migrate-helpers` | Legacy CLI command | REMOVE | Remove in the next major after `/app/helpers` migration is no longer supported. |
