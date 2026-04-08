# Post-DBOnly Audit

Date: 2026-04-06
Workspace: `/root/docker/brix`

Note: the requested `docker exec ...` commands could not be run in this session because Docker socket access is denied. I reproduced the same checks locally with `PYTHONPATH=src` and direct reads from `/root/.brix/brix.db`, which is present in this environment.

## Executive Summary

High-severity findings:

1. `merge`, `switch`, `error_handler`, and part of `repeat` are still not DB-safe.
   Their runner config depends on top-level step fields that are not declared on `Step` and are not persisted in `pipeline_step`. Those fields are dropped on save/load roundtrip.

2. Brick registry still contains two broken file bricks.
   `brick_definition` rows `file_read` and `file_write` point to runner `file`, but there is no `file` runner.

3. `pipeline` and `pipeline_group` runners are still disk/YAML-path oriented.
   They resolve sub-pipelines via filesystem lookup instead of DB-backed `PipelineStore` lookup first. That is a remaining DB-only consistency gap.

4. `brix__validate_step_migration` is now noisy/useless as a parity signal.
   It reported `96/96` mismatches, but `store_load_matches_db=true` in sampled results. It is comparing DB-native pipeline dicts against legacy/minimal `yaml_content` mirrors without normalizing both sides.

5. `startup_sync` is not actually independent of `yaml_content`.
   `startup_sync._sync_pipelines()` calls `_normalize_pipeline_steps_common()`, and that function still reads `pipeline.yaml_content`. Startup is safe only because `yaml_content` is still present in the live schema.

6. Help topics still contain stale pre-DB-only and pre-consolidated-tool examples.
   Tips are mostly updated; help topics are not.

## Audit 1: Config vs Params, All Runners

Scope note:

- Audited runner modules with actual runner classes in `src/brix/runners/*.py`.
- Utility/non-runner modules excluded from the table: `__init__.py`, `_subprocess.py`, `base.py`.
- `create_pipeline` and `add_step` both normalize `config -> params` for non-specialist steps via `_normalize_steps()` / `_normalize_step_config()`.
- Therefore a runner is generally safe when it reads `step.params`, or reads typed `Step` fields that actually exist on `Step`.

### Runner Table

| Runner | File | Reads Config From | Status / Potential Issue |
| --- | --- | --- | --- |
| `aggregate` | `src/brix/runners/aggregate.py` | `step.params` only | OK for normal `create_pipeline` path. Top-level `operations/group_by/input` would be dropped if a caller bypasses normalization. |
| `approval` | `src/brix/runners/approval.py` | top-level attrs: `message`, `on_timeout`, `channel`, `to`, `approval_timeout` | Works with current `Step` model. Registry schema for `action.approval` is empty, so brick/tool guidance is incomplete. |
| `choose` | `src/brix/runners/choose.py` | top-level attrs: `choices`, `default_steps` | OK. Fields exist on `Step`. |
| `cli` | `src/brix/runners/cli.py` | top-level attrs: `args`, `command`, `shell`, timeout | OK. Fields exist on `Step`. |
| `convert_batch` | `src/brix/runners/convert_batch.py` | `step.params` only | OK for normalized path. |
| `db_exec` | `src/brix/runners/db_exec.py` | `step.params`, fallback `step.config`, and direct attrs for `connection/query` | Compatible. No create-vs-read mismatch found. |
| `db_query` | `src/brix/runners/db_query.py` | `step.params`, fallback `step.config`, then direct attrs | Compatible. No create-vs-read mismatch found. |
| `db_upsert` | `src/brix/runners/db_upsert.py` | mostly `step.params`, fallback direct attrs for some fields | Compatible. |
| `dedup` | `src/brix/runners/dedup.py` | `step.params` only | OK for normalized path. |
| `diff` | `src/brix/runners/diff.py` | `step.params` only | OK for normalized path. |
| `emit` | `src/brix/runners/emit.py` | top-level attrs `event/data`, fallback `step.params` | Works. `event`/`data` exist on `Step`. |
| `error_handler` | `src/brix/runners/error_handler.py` | top-level attrs `try_step`, `handler_step` | Broken for DB-backed persistence. `try_step` and `handler_step` are not on `Step` and not in `pipeline_step`; they are dropped. |
| `extract_ics` | `src/brix/runners/extract_ics.py` | `step.params` only | OK for normalized path. |
| `extract_url` | `src/brix/runners/extract_url.py` | `step.params` only | OK for normalized path. |
| `file_read` | `src/brix/runners/file_io.py` | `step.params` only | OK for normalized path. |
| `file_read_base64` | `src/brix/runners/file_io.py` | `step.params` only | OK for normalized path. |
| `file_write` | `src/brix/runners/file_io.py` | `step.params` only | OK for normalized path. |
| `file_list` | `src/brix/runners/file_io.py` | `step.params` only | OK for normalized path. |
| `file_load_json` | `src/brix/runners/file_io.py` | `step.params` only | OK for normalized path. |
| `filter` | `src/brix/runners/filter.py` | `step.params` only | OK for normalized path. |
| `flatten` | `src/brix/runners/flatten.py` | `step.params` only | OK for normalized path. |
| `http` | `src/brix/runners/http.py` | `step.params`, fallback `step.config`, then direct attrs `url/method/headers/body` | Compatible. |
| `keyword_filter` | `src/brix/runners/keyword_filter.py` | `step.params` only | OK for normalized path. |
| `llm_batch` | `src/brix/runners/llm_batch.py` | `step.params` first, then top-level fallback | Safe through `create_pipeline` because config becomes params. Top-level `system_prompt/user_template/...` would be dropped if a caller bypasses normalization. |
| `llm_batch_poll` | `src/brix/runners/llm_batch_poll.py` | `step.params` only | OK for normalized path. |
| `markitdown` | `src/brix/runners/markitdown.py` | `step.params` first, then top-level fallback | Safe through normalized path. Top-level-only config would be lost on DB roundtrip. |
| `mcp` | `src/brix/runners/mcp.py` | top-level attrs `server/tool/timeout`, arguments from `step.params` | Compatible. `server` and `tool` exist on `Step`. |
| `merge` | `src/brix/runners/merge.py` | top-level attrs `inputs`, `mode`, `key` | Broken for DB-backed persistence. None of these fields exist on `Step` or `pipeline_step`; they are dropped. |
| `notify` | `src/brix/runners/notify.py` | top-level attrs `channel`, `to`; message from `step.params.message` or `step.message` | Internally works if step fields are written in the expected shape. But `action.notify` brick schema uses `message_template` and `target`, which do not match runner field names. |
| `parallel` | `src/brix/runners/parallel_runner.py` | top-level attrs `sub_steps`, `concurrency` | OK. Fields exist on `Step`. |
| `pipeline` | `src/brix/runners/pipeline.py` | top-level attr `pipeline`; forwarded args from `step.params` | Config shape is fine, but runner still resolves sub-pipelines via filesystem paths instead of DB lookup first. Remaining DB-only gap. |
| `pipeline_group` | `src/brix/runners/pipeline_group.py` | top-level attrs `pipelines`, `shared_params`, `concurrency` | Config shape is fine, but runner still resolves sub-pipelines from disk paths only. Remaining DB-only gap. |
| `python` | `src/brix/runners/python.py` | top-level attr `script`; runtime args from `step.params` | Compatible. |
| `queue` | `src/brix/runners/queue.py` | top-level attrs `queue_name`, `collect_until`, `collect_for`, `flush_to`; fallback `params.queue_name` | Compatible. Fields exist on `Step`. |
| `repeat` | `src/brix/runners/repeat.py` | top-level attrs `sequence`, `until`, `while_condition`, `max_iterations`, `timeout`, `delay` | Mostly OK, but `delay` is not on `Step` and not persisted in `pipeline_step`; that value is lost. |
| `respond` | `src/brix/runners/respond.py` | `step.params` only | Runner itself is fine. But `action.respond` brick schema is empty, so tool/brick guidance is incomplete. |
| `set` | `src/brix/runners/set.py` | top-level attr `values`, fallback `step.params` | Compatible. |
| `source` | `src/brix/runners/source.py` | collects top-level attrs `connector/path/pattern/recursive/folder/filter/limit`, then merges `config/params` | Safe through normalized path because `params` are read. Top-level-only config would be dropped on DB roundtrip. |
| `specialist` | `src/brix/runners/specialist.py` | `step.config` only | Compatible by design. This is the one step type intentionally exempt from `config -> params` normalization. |
| `switch` | `src/brix/runners/switch.py` | top-level attrs `field`, `cases`, `default` | Broken for DB-backed persistence. These fields are not on `Step` and not in `pipeline_step`; they are dropped. |
| `transform` | `src/brix/runners/transform.py` | `step.params` only | OK for normalized path. |
| `util_load_dir` | `src/brix/runners/util_load_dir.py` | `step.params` only | OK for normalized path. |
| `util_wait` | `src/brix/runners/util_wait.py` | `step.params` only | OK for normalized path. |
| `validate` | `src/brix/runners/validate.py` | top-level attr `rules` | OK. `rules` exists on `Step`. |
| `wait` | `src/brix/runners/wait.py` | top-level attrs like `until`, `timeout`; internal wait config | Compatible with current `Step` fields. |

### Concrete Config-vs-Model Breakages

These are not theoretical:

- `merge`
  - sample step: `{'id':'mg','type':'merge','inputs':['a','b'],'mode':'lookup','key':'id'}`
  - `step_dict_to_row(...)` persisted only `step_key` and `step_type`
  - everything the runner needs was dropped

- `switch`
  - sample step: `{'id':'sw','type':'switch','field':'{{ x }}','cases':{'a':'s1'},'default':'s2'}`
  - persisted only `step_key` and `step_type`

- `error_handler`
  - sample step: `{'id':'eh','type':'error_handler','try_step':'a','handler_step':'b'}`
  - persisted only `step_key` and `step_type`

- `repeat.delay`
  - `sequence` persists via `sequence_json`
  - `delay` is dropped because there is no `delay` field/column

### Create-vs-Read Conclusion

For the normal CRUD path (`create_pipeline`, `add_step`, `PipelineStore.save`):

- Safe pattern: runner reads `params`, or reads top-level fields that exist on `Step`.
- Unsafe pattern: runner reads top-level ad hoc fields that are not on `Step` and not mapped in `pipeline_step`.

That leaves four concrete DB-only inconsistencies:

1. `merge`: broken
2. `switch`: broken
3. `error_handler`: broken
4. `repeat.delay`: dropped

Secondary consistency gaps:

1. `pipeline` and `pipeline_group` still resolve sub-pipelines from disk, not DB first.
2. `notify` brick schema does not match `NotifyRunner` field names.

## Audit 2: DB Schema vs Code

### `pipeline_step` Columns Used vs Orphaned

Observed `pipeline_step` column count: `76`

Columns that are actively mapped to/from `Step`:

- all non-structural mapped columns in `_STEP_FIELD_TO_COLUMN`
- all `*_json` columns in `_STEP_JSON_COLUMNS`
- all boolean columns in `_STEP_BOOL_COLUMNS`
- direct same-name columns present on `Step`

Columns that appear structural/internal only:

- `id`
- `pipeline_id`
- `parent_step_id`
- `container`
- `branch_key`
- `branch_when`
- `position`
- `created_at`
- `updated_at`

Assessment:

- `id`, `pipeline_id`, `position`, timestamps are expected DB internals.
- `parent_step_id`, `container`, `branch_key`, `branch_when` currently look orphaned or at least unused in the live write/read path.
  - `upsert_step()` never writes nested rows into these columns.
  - `get_steps()` / `step_row_to_dict()` ignore them.
  - nested step containers are currently stored inline in JSON columns (`sequence_json`, `choices_json`, `default_steps_json`, `sub_steps_json`) instead.

So the schema still carries a not-currently-used normalization design for nested containers.

### `step_dict_to_row`

Definition location: `src/brix/db.py`

Result:

- It maps every current `Step` model field to either:
  - a renamed column from `_STEP_FIELD_TO_COLUMN`
  - a JSON column in `_STEP_JSON_COLUMNS`
  - a bool column in `_STEP_BOOL_COLUMNS`
  - a same-name direct column
- `Unmapped Step fields: []`

But this is only true for fields declared on `Step`.

### `step_row_to_dict`

Definition location: `src/brix/db.py`

Result:

- It reverses all mapped non-structural columns correctly.
- For a fully populated sample containing every declared `Step` field, the roundtrip was lossless:
  - `missing []`
  - `extra []`
  - `diffs []`

### Fields Lost in Roundtrip

Declared `Step` fields: no loss found.

Undeclared runner-specific fields: lost.

Lost on save/load because they are neither in `Step` nor in `pipeline_step`:

- `merge`: `inputs`, `mode`, `key`
- `switch`: `field`, `cases`, `default`
- `error_handler`: `try_step`, `handler_step`
- `repeat`: `delay`

Potentially risky if callers set them as top-level attrs instead of `params`:

- `source`: `connector`, `path`, `pattern`, `recursive`, `folder`, `filter`, `limit`
- `llm_batch`: `system_prompt`, `user_template`, `temperature`, `max_tokens`, `items`, `model`
- `markitdown`: `input`, `filename`, `auto_extract`, `language`, `template`
- `aggregate`: `operations`, `group_by`, `input`

These are less severe because those runners also work when the config lives in `params`, which is what `create_pipeline` normally writes after normalization.

## Audit 3: Brick Registry vs Runners

Local equivalent of requested checks:

- `discover_runners()` result count: `45`
- `brick_definition` row count: `69`

### Exact Results

Bricks whose `runner` column does not exist in discovered runners:

1. `file_read` -> `runner='file'`
2. `file_write` -> `runner='file'`

Runners with no `brick_definition` row pointing at them:

1. `db_exec`
2. `emit`
3. `file_read`
4. `file_read_base64`
5. `queue`

Interpretation:

- `db_exec` is discoverable as a runner but only brick alias `db.exec` is missing in DB. DB has `db.ingest`, `db.query`, `db.upsert`, but not `db.exec`.
- `emit`, `queue`, `file_read_base64` have no matching brick rows at all.
- `file_read` runner exists, but the legacy brick row `file_read` points to nonexistent `file` instead of `file_read`.

### Exact Name Compare

If comparing exact names only, almost everything mismatches because brick names are mostly canonical dot-notation aliases and runner names are flat Python module names. That exact-name comparison is not useful by itself.

The meaningful compare is `brick_definition.runner` vs discovered runner names, and that produced the concrete mismatches listed above.

### Additional Brick Schema Gaps

Brick definitions with empty `config_schema`: `28`

Notable empty-schema bricks:

- `flow.switch`
- `flow.error_handler`
- `flow.repeat`
- `flow.pipeline`
- `flow.pipeline_group`
- `action.approval`
- `action.respond`
- `http.request`
- `mcp.call`
- `script.python`
- `script.cli`
- `source.fetch`

This does not always break runtime because `get_brick_schema` falls back to the runner `config_schema()` when DB schema is empty, but the DB itself is incomplete.

### Brick-vs-Runner Field Mismatch

`action.notify` is semantically inconsistent:

- brick schema fields: `channel`, `message_template`, `target`
- runner reads: `channel`, `to`, `message`

So a brick-generated step can be shaped differently from what `NotifyRunner` expects unless another layer translates names.

## Audit 4: MCP Tool Schemas Completeness

Local equivalent of requested handler/schema check:

- Missing schemas: `0`
- Empty schemas: `1`

Empty tool schema:

- `brix__validate_step_migration`

Additional quality note:

- `35` tool schemas still have placeholder descriptions like `MCP tool: ...`
- This is not a completeness failure, but it is a metadata quality gap.

## Audit 5: Tips and Help Topics

### Tips

Tips are mostly aligned with DB-only persistence.

Good/current examples:

- `KERN-REGEL`
- `DB-only Pipeline Persistence`
- `BRIX_STEP_SOURCE Toggle`
- `TOP-5 ANTI-PATTERNS`

I did not find a tip still telling users to do normal pipeline CRUD by editing YAML files on disk.

### Help Topics With Stale Content

These help topics still contain stale examples or old tool names:

1. `quick-start`
   - still shows inline step `config` blocks with legacy `type: http` / `type: python`
   - still shows `brix__run_pipeline(name="hello-world")` instead of current `pipeline_id`

2. `beispiele`
   - uses `params=[...]` at pipeline level instead of `input_schema`
   - uses inline `config` blocks with legacy `python/http`

3. `credentials`
   - references nonexistent tools `brix__credential_add` / `brix__credential_rotate`
   - current tool is consolidated `brix__credential(action=...)`

4. `tools`
   - same stale `brix__credential_add` / `brix__credential_rotate` naming

5. `triggers`
   - says `schedules.yaml` is deprecated but also talks about it as an active migration path
   - needs review for post-DB-only wording

6. `templates`, `dag`, `foreach`, `helpers`
   - still show old step shapes centered on legacy `config` and raw `python` examples

### DBO-11 Verification

Using `brix__validate_step_migration` logic locally:

- pipelines checked: `96`
- mismatch count: `96`

Observed behavior:

- sampled mismatches had `store_load_matches_db = true`
- sampled mismatches had `store_load_matches_yaml = false`

Interpretation:

- this tool is now reporting structural/key-set differences between DB-native pipeline dicts and older `yaml_content`
- it is not a reliable indicator of broken migration anymore
- it needs normalization on both sides, or a narrower comparison target

## Audit 6: Startup Integrity

### Does `startup_sync` still work after DBO-12 / `yaml_content` removal?

Current answer: it works only because `yaml_content` has not actually been removed.

Evidence:

- all `96` pipelines still have non-empty `yaml_content`
- `startup_sync._sync_pipelines()` calls `_normalize_pipeline_steps_common()`
- `_normalize_pipeline_steps_common()` selects `id, name, yaml_content, migration_status FROM pipeline`

So:

- `startup_sync.py` itself no longer directly parses YAML files for pipeline normalization
- but the normalization function it calls is still `yaml_content`-dependent
- if `yaml_content` were really removed from schema/data today, this path would break

### Dead / Live `yaml_content` References

Requested grep shows non-migration references still present in live code:

- `src/brix/db.py`
- `src/brix/seed.py`
- `src/brix/integrity.py`
- `src/brix/mcp_handlers/health.py`
- `src/brix/bundle.py`

Assessment:

- not all of these are dead
- several are still active fallback/validation/export paths
- the codebase is still dual-mode, not fully free of `yaml_content`

### Other Startup-Adjacent Legacy Paths

There are still explicit references to legacy disk/YAML persistence around schedules and pipeline files:

- `src/brix/scheduler.py` is deprecated but still reads `~/.brix/schedules.yaml`
- `src/brix/runners/pipeline.py` resolves sub-pipelines from disk paths
- `src/brix/runners/pipeline_group.py` resolves sub-pipelines from disk paths

These are not necessarily startup blockers, but they are inconsistent with a strict DB-only model.

## Recommended Fix Order

1. Fix persistence holes first.
   - add `merge.inputs/mode/key` to `Step` + `pipeline_step`
   - add `switch.field/cases/default`
   - add `error_handler.try_step/handler_step`
   - add `repeat.delay`

2. Fix brick registry/runtime mismatches.
   - replace broken `file_read` / `file_write` brick rows pointing at `file`
   - add missing brick rows for `db.exec`, `queue`, `emit`, `file.read_base64`

3. Make sub-pipeline runners DB-first.
   - `pipeline` and `pipeline_group` should resolve by pipeline name through `PipelineStore`/DB before falling back to raw file paths

4. Repair schema metadata.
   - fill empty brick schemas for `flow.switch`, `flow.error_handler`, `flow.repeat`, `flow.pipeline`, `flow.pipeline_group`, `action.approval`, `action.respond`, `source.fetch`
   - fix `action.notify` schema field names or add translation layer
   - add non-empty schema for `brix__validate_step_migration`

5. Rework migration parity check.
   - normalize DB and YAML forms before comparing
   - otherwise DBO-11 stays permanently red/noisy

6. Clean help topics.
   - replace stale consolidated-tool names
   - remove pre-DB-only CRUD language
   - update examples away from legacy `config` + flat runner names where possible

