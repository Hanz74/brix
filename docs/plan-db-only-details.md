# DB-Only Pipeline Migration Details

This document fills the three open gaps in the existing DB-only migration plan in [docs/plan-db-only-pipelines.md](/root/docker/brix/docs/plan-db-only-pipelines.md).

Scope used for this analysis:

- model source: [src/brix/models.py](/root/docker/brix/src/brix/models.py)
- current persistence/runtime behavior: [src/brix/db.py](/root/docker/brix/src/brix/db.py), [src/brix/pipeline_store.py](/root/docker/brix/src/brix/pipeline_store.py), [src/brix/loader.py](/root/docker/brix/src/brix/loader.py)
- tests reviewed:
  - [tests/test_engine.py](/root/docker/brix/tests/test_engine.py)
  - [tests/test_mcp_server.py](/root/docker/brix/tests/test_mcp_server.py)
  - [tests/test_crud_complete.py](/root/docker/brix/tests/test_crud_complete.py)
  - [tests/test_v5_crud_gaps.py](/root/docker/brix/tests/test_v5_crud_gaps.py)
  - [tests/test_config_params_mapping.py](/root/docker/brix/tests/test_config_params_mapping.py)
  - [tests/test_add_step_params_bug12.py](/root/docker/brix/tests/test_add_step_params_bug12.py)
  - [tests/test_pipeline_helpers.py](/root/docker/brix/tests/test_pipeline_helpers.py)

Important baseline observation:

- Current Brix runtime is still DB-first, but not DB-only.
- The authoritative executable definition is still `pipeline.yaml_content`, loaded back through `PipelineLoader.load_from_string()`.
- The normalized tables proposed in the existing plan do not exist yet.
- The existing `pipeline_step` draft in `docs/plan-db-only-pipelines.md` is incomplete relative to `models.Step`.

## Gap 1: Exact Column Mapping

## 1.1 Summary

The target DB-only model should use:

- `pipeline` for pipeline-level metadata
- `pipeline_input` for `Pipeline.input`
- `pipeline_credential` for `Pipeline.credentials`
- `pipeline_step` for all executable `Step` rows

The existing draft in [docs/plan-db-only-pipelines.md](/root/docker/brix/docs/plan-db-only-pipelines.md) already covers many `Step` fields, but it is missing these required columns:

- `persist INTEGER NOT NULL DEFAULT 0`
- `success_on_stop INTEGER NOT NULL DEFAULT 1`
- `channel TEXT`
- `recipient TEXT`
- `until_expr TEXT`
- `while_expr TEXT`
- `max_iterations INTEGER NOT NULL DEFAULT 100`
- `depends_on_json TEXT NOT NULL DEFAULT '[]'`

Without those additions, the normalized schema cannot faithfully represent `models.Step`.

## 1.2 Canonical `pipeline_input` mapping

Note: the model field is `Pipeline.input`, not `Pipeline.inputs`.

Recommended table:

```sql
CREATE TABLE pipeline_input (
    pipeline_id TEXT NOT NULL,
    input_key TEXT NOT NULL,
    type TEXT NOT NULL,
    default_json TEXT,
    description TEXT,
    PRIMARY KEY (pipeline_id, input_key),
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE
);
```

Field mapping:

| field_name | python_type | db_column | db_type | is_json | default_value |
| --- | --- | --- | --- | --- | --- |
| `input` map key | `str` | `input_key` | `TEXT` | `false` | none |
| `InputParam.type` | `str` | `type` | `TEXT NOT NULL` | `false` | none |
| `InputParam.default` | `Any` | `default_json` | `TEXT` | `true` | `null` |
| `InputParam.description` | `Optional[str]` | `description` | `TEXT` | `false` | `null` |

## 1.3 Canonical `pipeline_credential` mapping

Recommended table:

```sql
CREATE TABLE pipeline_credential (
    pipeline_id TEXT NOT NULL,
    alias TEXT NOT NULL,
    env_ref TEXT NOT NULL,
    refresh_json TEXT,
    PRIMARY KEY (pipeline_id, alias),
    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE
);
```

Field mapping:

| field_name | python_type | db_column | db_type | is_json | default_value |
| --- | --- | --- | --- | --- | --- |
| `credentials` map key | `str` | `alias` | `TEXT` | `false` | none |
| `CredentialRef.env` | `str` | `env_ref` | `TEXT NOT NULL` | `false` | none |
| `CredentialRef.refresh` | `Optional[dict]` | `refresh_json` | `TEXT` | `true` | `null` |

Implementation note:

- `Pipeline.coerce_credentials()` already normalizes shorthand string credentials into `{env: ...}`.
- The DB representation should store only the canonical form.

## 1.4 Exact `Step` to `pipeline_step` mapping

Recommended final `pipeline_step` shape:

```sql
CREATE TABLE pipeline_step (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,

    step_key TEXT NOT NULL,
    parent_step_id TEXT,
    container TEXT NOT NULL DEFAULT 'steps',
    branch_key TEXT,
    branch_when TEXT,
    position INTEGER NOT NULL,

    step_type TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,

    script TEXT,
    helper TEXT,
    url TEXT,
    method TEXT,
    headers_json TEXT,
    body_json TEXT,
    command TEXT,
    args_json TEXT,
    shell INTEGER NOT NULL DEFAULT 0,
    server TEXT,
    tool TEXT,
    pipeline_ref TEXT,
    pipelines_json TEXT,
    shared_params_json TEXT NOT NULL DEFAULT '{}',
    values_json TEXT,
    persist INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    success_on_stop INTEGER NOT NULL DEFAULT 1,
    channel TEXT,
    recipient TEXT,
    approval_timeout TEXT,
    on_timeout TEXT,
    until_expr TEXT,
    while_expr TEXT,
    max_iterations INTEGER NOT NULL DEFAULT 100,
    params_json TEXT,
    foreach_expr TEXT,
    parallel INTEGER NOT NULL DEFAULT 0,
    concurrency INTEGER NOT NULL DEFAULT 10,
    batch_size INTEGER NOT NULL DEFAULT 0,
    flat_output INTEGER NOT NULL DEFAULT 0,
    when_expr TEXT,
    else_of TEXT,
    on_error TEXT,
    retry_profile TEXT,
    timeout TEXT,
    fetch_all_pages INTEGER NOT NULL DEFAULT 0,
    progress INTEGER NOT NULL DEFAULT 0,
    requirements_json TEXT NOT NULL DEFAULT '[]',
    input_schema_json TEXT NOT NULL DEFAULT '{}',
    output_schema_json TEXT NOT NULL DEFAULT '{}',
    rules_json TEXT,
    config_json TEXT,
    depends_on_json TEXT NOT NULL DEFAULT '[]',
    cache_json TEXT,
    circuit_breaker_json TEXT,
    rate_limit_json TEXT,
    compensate_json TEXT,
    persist_output INTEGER NOT NULL DEFAULT 0,
    pause_before INTEGER NOT NULL DEFAULT 0,
    persist_data INTEGER NOT NULL DEFAULT 1,
    profile TEXT,
    queue_name TEXT,
    collect_until INTEGER,
    collect_for TEXT,
    flush_to TEXT,
    event TEXT,
    data_json TEXT,
    stream INTEGER NOT NULL DEFAULT 0,
    unwrap_json INTEGER,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_step_id) REFERENCES pipeline_step(id) ON DELETE CASCADE,
    UNIQUE (pipeline_id, step_key)
);
```

Mapping rules:

- `step_key` is the model-visible `Step.id`.
- `id` is a surrogate row UUID, not the model field.
- nested containers are structural, not YAML blobs:
  - top level: `container='steps'`
  - repeat children: `container='sequence'`
  - parallel children: `container='sub_steps'`
  - choose default children: `container='default_steps'`
  - choose branch children: `container='choice_steps'`, with `branch_key` and `branch_when`

Exact field mapping:

| field_name | python_type | db_column | db_type | is_json | default_value |
| --- | --- | --- | --- | --- | --- |
| `id` | `str` | `step_key` | `TEXT NOT NULL` | `false` | none |
| `type` | `Literal[...]` | `step_type` | `TEXT NOT NULL` | `false` | none |
| `enabled` | `bool` | `enabled` | `INTEGER NOT NULL` | `false` | `True` |
| `script` | `Optional[str]` | `script` | `TEXT` | `false` | `None` |
| `helper` | `Optional[str]` | `helper` | `TEXT` | `false` | `None` |
| `url` | `Optional[str]` | `url` | `TEXT` | `false` | `None` |
| `method` | `str` | `method` | `TEXT` | `false` | `"GET"` |
| `headers` | `Optional[dict[str, str]]` | `headers_json` | `TEXT` | `true` | `None` |
| `body` | `Any` | `body_json` | `TEXT` | `true` | `None` |
| `command` | `Optional[str]` | `command` | `TEXT` | `false` | `None` |
| `args` | `Optional[list[str]]` | `args_json` | `TEXT` | `true` | `None` |
| `shell` | `bool` | `shell` | `INTEGER NOT NULL` | `false` | `False` |
| `server` | `Optional[str]` | `server` | `TEXT` | `false` | `None` |
| `tool` | `Optional[str]` | `tool` | `TEXT` | `false` | `None` |
| `pipeline` | `Optional[str]` | `pipeline_ref` | `TEXT` | `false` | `None` |
| `pipelines` | `Optional[list[str]]` | `pipelines_json` | `TEXT` | `true` | `None` |
| `shared_params` | `dict[str, Any]` | `shared_params_json` | `TEXT NOT NULL` | `true` | `{}` |
| `values` | `Optional[dict[str, Any]]` | `values_json` | `TEXT` | `true` | `None` |
| `persist` | `bool` | `persist` | `INTEGER NOT NULL` | `false` | `False` |
| `message` | `Optional[str]` | `message` | `TEXT` | `false` | `None` |
| `success_on_stop` | `bool` | `success_on_stop` | `INTEGER NOT NULL` | `false` | `True` |
| `channel` | `Optional[str]` | `channel` | `TEXT` | `false` | `None` |
| `to` | `Optional[str]` | `recipient` | `TEXT` | `false` | `None` |
| `approval_timeout` | `str` | `approval_timeout` | `TEXT` | `false` | `"24h"` |
| `on_timeout` | `str` | `on_timeout` | `TEXT` | `false` | `"stop"` |
| `choices` | `Optional[list[dict]]` | `parent_step_id + container='choice_steps' + branch_key + branch_when + position` | `STRUCTURAL CHILD ROWS` | `false` | `None` |
| `default_steps` | `Optional[list[dict]]` | `parent_step_id + container='default_steps' + position` | `STRUCTURAL CHILD ROWS` | `false` | `None` |
| `sub_steps` | `Optional[list[dict]]` | `parent_step_id + container='sub_steps' + position` | `STRUCTURAL CHILD ROWS` | `false` | `None` |
| `until` | `Optional[str]` | `until_expr` | `TEXT` | `false` | `None` |
| `while_condition` | `Optional[str]` | `while_expr` | `TEXT` | `false` | `None` |
| `max_iterations` | `int` | `max_iterations` | `INTEGER NOT NULL` | `false` | `100` |
| `sequence` | `Optional[list[dict]]` | `parent_step_id + container='sequence' + position` | `STRUCTURAL CHILD ROWS` | `false` | `None` |
| `params` | `Optional[dict[str, Any]]` | `params_json` | `TEXT` | `true` | `None` |
| `foreach` | `Optional[str]` | `foreach_expr` | `TEXT` | `false` | `None` |
| `parallel` | `bool` | `parallel` | `INTEGER NOT NULL` | `false` | `False` |
| `concurrency` | `int` | `concurrency` | `INTEGER NOT NULL` | `false` | `10` |
| `batch_size` | `int` | `batch_size` | `INTEGER NOT NULL` | `false` | `0` |
| `flat_output` | `bool` | `flat_output` | `INTEGER NOT NULL` | `false` | `False` |
| `when` | `Optional[str]` | `when_expr` | `TEXT` | `false` | `None` |
| `else_of` | `Optional[str]` | `else_of` | `TEXT` | `false` | `None` |
| `on_error` | `Optional[Literal["stop","continue","retry"]]` | `on_error` | `TEXT` | `false` | `None` |
| `retry_profile` | `Optional[str]` | `retry_profile` | `TEXT` | `false` | `None` |
| `timeout` | `Optional[str]` | `timeout` | `TEXT` | `false` | `None` |
| `fetch_all_pages` | `bool` | `fetch_all_pages` | `INTEGER NOT NULL` | `false` | `False` |
| `progress` | `bool` | `progress` | `INTEGER NOT NULL` | `false` | `False` |
| `requirements` | `list[str]` | `requirements_json` | `TEXT NOT NULL` | `true` | `[]` |
| `input_schema` | `dict` | `input_schema_json` | `TEXT NOT NULL` | `true` | `{}` |
| `output_schema` | `dict` | `output_schema_json` | `TEXT NOT NULL` | `true` | `{}` |
| `rules` | `Optional[list[dict]]` | `rules_json` | `TEXT` | `true` | `None` |
| `config` | `Optional[dict]` | `config_json` | `TEXT` | `true` | `None` |
| `depends_on` | `list[str]` | `depends_on_json` | `TEXT NOT NULL` | `true` | `[]` |
| `cache` | `Union[bool, dict, None]` | `cache_json` | `TEXT` | `true` | `False` |
| `circuit_breaker` | `Optional[dict]` | `circuit_breaker_json` | `TEXT` | `true` | `None` |
| `rate_limit` | `Optional[dict]` | `rate_limit_json` | `TEXT` | `true` | `None` |
| `compensate` | `Optional[dict]` | `compensate_json` | `TEXT` | `true` | `None` |
| `persist_output` | `bool` | `persist_output` | `INTEGER NOT NULL` | `false` | `False` |
| `pause_before` | `bool` | `pause_before` | `INTEGER NOT NULL` | `false` | `False` |
| `persist_data` | `bool` | `persist_data` | `INTEGER NOT NULL` | `false` | `True` |
| `profile` | `Optional[str]` | `profile` | `TEXT` | `false` | `None` |
| `queue_name` | `Optional[str]` | `queue_name` | `TEXT` | `false` | `None` |
| `collect_until` | `Optional[int]` | `collect_until` | `INTEGER` | `false` | `None` |
| `collect_for` | `Optional[str]` | `collect_for` | `TEXT` | `false` | `None` |
| `flush_to` | `Optional[str]` | `flush_to` | `TEXT` | `false` | `None` |
| `event` | `Optional[str]` | `event` | `TEXT` | `false` | `None` |
| `data` | `Any` | `data_json` | `TEXT` | `true` | `None` |
| `stream` | `bool` | `stream` | `INTEGER NOT NULL` | `false` | `False` |
| `unwrap_json` | `Optional[bool]` | `unwrap_json` | `INTEGER` | `false` | `None` |

## 1.5 Pipeline metadata fields that remain on `pipeline`

For completeness, these `Pipeline` fields stay on `pipeline` instead of separate tables:

- `name`
- `version`
- `description`
- `brix_version`
- `kind`
- `extends`
- `template_params_json`
- `is_template`
- `blueprint_params_json`
- `error_handling_json`
- `retry_profiles_json`
- `notify_json`
- `idempotency_key`
- `compositor_mode`
- `allow_code`
- `strict_bricks`
- `test_mode`
- `requirements_json`
- `groups_json`
- `output_json`
- `output_slots_json`
- legacy/compatibility: `path`, `yaml_content`, `persistence_version`

## Gap 2: Test Strategy

## 2.1 Current baseline from targeted `pytest` runs

I ran each requested file individually in the current workspace. Results:

| file | test_count | current_result |
| --- | ---: | --- |
| `tests/test_engine.py` | 97 | 3 passed, 94 failed |
| `tests/test_mcp_server.py` | 163 | 69 passed, 94 failed |
| `tests/test_crud_complete.py` | 22 | 16 passed, 6 errors |
| `tests/test_v5_crud_gaps.py` | 78 | 70 passed, 8 failed |
| `tests/test_config_params_mapping.py` | 12 | 8 passed, 4 failed |
| `tests/test_add_step_params_bug12.py` | 9 | 0 passed, 9 failed |
| `tests/test_pipeline_helpers.py` | 18 | 18 passed |

These failures are the current repository baseline, not the migration impact by themselves.

Important point:

- migration planning should classify tests by coupling to YAML/file persistence
- not by whether they are already red today for unrelated reasons

## 2.2 File-by-file migration impact

### `tests/test_engine.py`

- Test count: 97
- YAML-dependent break risk: low
- Why:
  - almost every test constructs a `Pipeline` via `PipelineLoader().load_from_string(...)`
  - tests exercise engine execution on an in-memory `Pipeline` object
  - they do not depend on `PipelineStore`, `pipeline.yaml_content`, or pipeline files on disk
- Expected to pass after DB-only migration:
  - conceptually all 97 should remain valid
  - only helper tests around upstream loading would need updates if engine entrypoints change
- What new tests are needed:
  - repository path: load `Pipeline` from normalized DB rows and run engine unchanged
  - fallback path: when `storage_mode=dual` and rows are absent, loader falls back to `yaml_content`
  - parity path: same pipeline loaded from YAML vs row assembly yields equivalent `Pipeline.model_dump()`
  - nested step assembly: repeat / choose / parallel reconstructed from rows execute identically

### `tests/test_mcp_server.py`

- Test count: 163
- YAML-dependent break risk: high
- Why:
  - large parts of the file monkeypatch `brix.mcp_server.PIPELINE_DIR`
  - many tests assert `.yaml` file creation or read raw YAML files back from `tmp_path`
  - several tests rely on `PipelineStore.save()` or filesystem scans
- Expected to break under DB-only persistence:
  - pipeline CRUD tests that assume disk YAML side effects
  - `list_pipelines_custom_dir`
  - create/update tests that read generated YAML for `error_handling`, `groups`, `output`
  - rename pipeline tests that assert file rename behavior
  - pipeline-tool discovery tests that use `PipelineStore.save()` only because save still mirrors to files today
- Expected to continue conceptually:
  - tool registration and schema tests
  - generic handler registry tests
  - help/tips tests
  - run-history/status tests not tied to file persistence
- Rewrite recommendation:
  - split this file into:
    - API contract tests for MCP handlers
    - legacy YAML compatibility tests
  - remove direct assertions on filesystem paths from the DB-only contract suite
- What new tests are needed:
  - `create_pipeline` inserts `pipeline`, `pipeline_input`, `pipeline_credential`, `pipeline_step`
  - `get_pipeline` assembles from normalized rows only
  - `add_step` inserts one row or one parent plus child rows for nested inputs
  - `remove_step` deletes target row and cascades child rows
  - `update_step` updates a single row without YAML re-emission
  - `rename_pipeline` changes metadata only in DB-only mode and does not require disk rename
  - `list_pipelines` reads from DB rows without directory scanning
  - compatibility-mode test: in `storage_mode=dual`, `yaml_content` mirror still regenerates

### `tests/test_crud_complete.py`

- Test count: 22
- YAML-dependent break risk: none
- Why:
  - these tests cover connections, trigger groups, triggers, variables, profiles, alert rules
  - they use dedicated DB-backed managers and do not depend on pipeline YAML persistence
- Expected to pass after DB-only migration:
  - all 22
- What new tests are needed:
  - none for the pipeline DB-only change itself
  - only regression smoke coverage to ensure unrelated CRUD handlers are unaffected

### `tests/test_v5_crud_gaps.py`

- Test count: 78
- YAML-dependent break risk: partial
- YAML-coupled subset:
  - `TestSearchPipelines` writes pipeline YAML files into a temp directory and points the handler at that directory
  - those tests assume file-backed pipeline discovery
- Not YAML-coupled:
  - alert update
  - credential rotate/search
  - run annotate/search
  - server manager tests
  - tips content tests
- Expected to break under DB-only persistence:
  - the 4 pipeline search tests that currently build temp YAML files:
    - name match
    - description match
    - case insensitive
    - no match
- Expected to pass:
  - the other 74 tests conceptually
- What new tests are needed:
  - `search_pipelines` over DB-only metadata and reconstructed descriptions
  - search by `pipeline.description`
  - search still works when `path` is null and `yaml_content` is empty
  - dual-mode search fallback from rows to `yaml_content`

### `tests/test_config_params_mapping.py`

- Test count: 12
- YAML-dependent break risk: partial
- Expected to pass unchanged:
  - 8 unit tests around `_normalize_step_config()` and `_normalize_steps()`
- Expected to break or require rewrite:
  - 4 integration tests using `PIPELINE_DIR` and `PipelineStore.load_raw()`
  - those tests verify what got serialized into stored YAML
- DB-only replacement:
  - assert stored `pipeline_step.params_json` vs `config_json`
  - assert assembled `get_pipeline` output still exposes the same step shape
- What new tests are needed:
  - non-specialist `config` is normalized into `params_json`
  - specialist `config` remains in `config_json`
  - reconstruction to `get_pipeline()` preserves old external contract
  - no `yaml_content` rewrite is required for these semantics in DB-only mode

### `tests/test_add_step_params_bug12.py`

- Test count: 9
- YAML-dependent break risk: low to medium
- Why:
  - tests use MCP handlers, not raw YAML inspection
  - assertions are about step shape returned from `get_pipeline` and `get_step`
  - fixture still monkeypatches `PIPELINE_DIR`, which may disappear from the handler implementation
- Expected to pass conceptually after DB-only migration:
  - all 9 should remain valid as contract tests
- Expected rewrite scope:
  - fixture/setup only, if handler no longer uses `PIPELINE_DIR`
- What new tests are needed:
  - `add_step` promotes recognized fields directly into columns rather than leaving them in `params_json`
  - custom keys remain in `params_json`
  - JSON-string params are parsed before row persistence
  - `get_step` reassembles from a single DB row with no YAML round-trip

### `tests/test_pipeline_helpers.py`

- Test count: 18
- YAML-dependent break risk: partial and explicit
- Expected to keep passing:
  - 12 unit tests for `_extract_helper_refs()`
- Expected to break:
  - 6 integration/migration tests that currently depend on `yaml_content`:
    - `test_creates_join_rows`
    - `test_removes_stale_rows`
    - `test_unknown_helpers_ignored`
    - `test_unknown_pipeline_noop`
    - `test_nested_refs_in_repeat`
    - `test_backfill_populates_existing_pipelines`
- Why:
  - current implementation of `refresh_pipeline_deps()` reads `pipeline.yaml_content`
  - DB-only migration explicitly moves helper extraction to `pipeline_step` rows
- What new tests are needed:
  - helper extraction from `pipeline_step.helper`
  - helper extraction from `pipeline_step.script`
  - recursive helper discovery from child step rows (`sequence`, `sub_steps`, `choice_steps`, `default_steps`)
  - backfill job that converts old `yaml_content` into rows and then populates `pipeline_helper`
  - no-op behavior when a pipeline has metadata but no step rows

## 2.3 Recommended migration test matrix

Add these dedicated test files instead of overloading the legacy YAML suites:

- `tests/test_pipeline_repository_db_rows.py`
  - assemble `Pipeline` from `pipeline`, `pipeline_input`, `pipeline_credential`, `pipeline_step`
  - verify exact nested reconstruction
- `tests/test_pipeline_migration_from_yaml_content.py`
  - one pipeline with nested steps, inputs, credentials
  - migrate to rows
  - compare reconstructed raw dict to source raw dict
- `tests/test_pipeline_storage_modes.py`
  - `yaml`, `dual`, `db`
  - verify reads and writes under each mode
- `tests/test_pipeline_step_crud_db_only.py`
  - create/add/update/remove/get step without YAML rewrite
- `tests/test_pipeline_deps_from_step_rows.py`
  - helper and sub-pipeline dependency extraction from normalized rows
- `tests/test_pipeline_rollback_restore.py`
  - delete/break rows, restore from `yaml_content`, verify successful reassembly

## Gap 3: Rollback Plan

## 3.1 How to detect failed migration

Detect at both pipeline level and rollout level.

Per-pipeline failure signals:

1. `pipeline.persistence_version = 3` but no `pipeline_step` rows exist.
2. `pipeline_step` rows exist but reconstructing a raw dict and calling `Pipeline.model_validate(raw)` fails.
3. nested row integrity violation:
   - duplicate `(pipeline_id, parent_step_id, container, position)`
   - orphan `parent_step_id`
   - `choice_steps` rows missing `branch_key`
4. required metadata mismatch:
   - row-assembled `input` differs from source `yaml_content`
   - row-assembled `credentials` differs from source `yaml_content`
5. structural parity mismatch:
   - canonicalized source dict hash != canonicalized reconstructed dict hash

Recommended validation query/checks:

```sql
SELECT p.name
FROM pipeline p
LEFT JOIN pipeline_step s ON s.pipeline_id = p.id
WHERE p.persistence_version = 3
GROUP BY p.id
HAVING COUNT(s.id) = 0;
```

Runtime validation step after migration:

1. read source `yaml_content`
2. migrate to rows
3. reconstruct raw dict from rows
4. normalize both dicts
5. compare hashes
6. call `Pipeline.model_validate()` on reconstructed dict
7. only then mark `persistence_version = 3`

Rollout-level failure signals:

- more than N pipelines fail migration validation
- engine load failures increase after switching to DB mode
- MCP CRUD operations start producing partial pipelines or missing nested steps

## 3.2 How to restore from `yaml_content` backup

Use `pipeline.yaml_content` as the immediate rollback source until a full release cycle has passed.

Per-pipeline restore procedure:

1. Force runtime mode to `yaml`.
2. Fetch source blob:

```sql
SELECT id, name, yaml_content
FROM pipeline
WHERE name = ?;
```

3. Validate the blob with the existing YAML loader path:
   - `PipelineLoader.load_from_string(yaml_content)`
4. Delete normalized child rows for that pipeline:
   - `DELETE FROM pipeline_input WHERE pipeline_id=?`
   - `DELETE FROM pipeline_credential WHERE pipeline_id=?`
   - `DELETE FROM pipeline_step WHERE pipeline_id=?`
5. Re-run the migration for that single pipeline if the issue was transient, or keep it on YAML fallback.
6. If abandoning DB rows for that pipeline, reset:

```sql
UPDATE pipeline
SET persistence_version = 2
WHERE id = ?;
```

Emergency full rollback:

1. switch system storage mode to `yaml`
2. ignore normalized rows entirely
3. continue loading from `yaml_content`
4. later rebuild rows offline after fixes

Recommended helper API:

- `restore_pipeline_rows_from_yaml(pipeline_name)`
- `restore_all_pipeline_rows_from_yaml()`

Pseudo-code:

```python
def restore_pipeline_rows_from_yaml(name: str):
    row = db.get_pipeline(name)
    yaml_content = db.get_pipeline_yaml_content(name)
    raw = yaml.safe_load(yaml_content) or {}
    Pipeline.model_validate(raw)
    db.replace_pipeline_inputs(row["id"], raw.get("input", {}))
    db.replace_pipeline_credentials(row["id"], raw.get("credentials", {}))
    db.replace_pipeline_steps(row["id"], raw.get("steps", []))
    db.mark_pipeline_persistence_version(row["id"], 3)
```

## 3.3 Runtime toggle between YAML and DB-rows mode

Add a single runtime switch rather than scattered conditionals.

Recommended config:

- env var: `BRIX_PIPELINE_STORAGE_MODE`
- allowed values:
  - `yaml`
  - `dual`
  - `db`

Behavior:

### `yaml`

- reads:
  - always use `yaml_content`
  - disk YAML only as final legacy fallback
- writes:
  - legacy behavior
  - optional backfill to rows disabled or best-effort only
- use for:
  - emergency rollback
  - pre-migration compatibility

### `dual`

- reads:
  - prefer rows only when `persistence_version >= 3` and validation passes
  - otherwise fall back to `yaml_content`
- writes:
  - primary write to normalized rows
  - regenerate `yaml_content` mirror
- use for:
  - migration window
  - burn-in period

### `db`

- reads:
  - rows only
  - `yaml_content` used only by restore/export tools
- writes:
  - rows only
  - optional on-demand YAML export, not runtime persistence
- use for:
  - post-cutover steady state

Recommended repository boundary:

```python
class PipelineRepository:
    def load_pipeline(self, name: str) -> Pipeline:
        mode = config.BRIX_PIPELINE_STORAGE_MODE
        if mode == "yaml":
            return self._load_from_yaml_content(name)
        if mode == "dual":
            return self._load_from_rows_or_yaml(name)
        if mode == "db":
            return self._load_from_rows_only(name)
```

Important rule:

- the toggle should live at repository/load-save boundaries
- the engine should continue receiving `Pipeline` objects and remain unaware of storage mode

## 3.4 What to back up before migration

Minimum required backups:

1. Full SQLite backup of `brix.db`
   - safest option is a SQLite online backup or the existing backup tool
2. Raw `pipeline` table snapshot including:
   - `id`
   - `name`
   - `path`
   - `yaml_content`
   - `updated_at`
   - `requirements_json`
   - `project`
   - `tags`
   - `group_name`
   - `description`
3. `object_version` rows for `type='pipeline'`
4. Filesystem pipeline YAML directory if it still exists and is still mirrored

Recommended additional backups:

5. `helper` and `pipeline_helper`
   - because dependency refresh behavior changes from YAML scan to row scan
6. export bundle of the whole project
7. migration manifest file containing:
   - timestamp
   - pipeline count
   - pipelines with non-empty `yaml_content`
   - checksum per pipeline for source `yaml_content`

Recommended pre-migration backup checklist:

1. Run DB backup.
2. Record count of pipelines with non-empty `yaml_content`.
3. Export all pipeline blobs to a tarball or bundle archive.
4. Copy mirrored `~/.brix/pipelines/*.yaml` if present.
5. Record pre-migration checksums of each pipeline's canonical raw dict.

## 3.5 Recommended rollback-ready rollout sequence

1. Add new tables and repository code.
2. Deploy with `BRIX_PIPELINE_STORAGE_MODE=dual`.
3. Backfill rows from `yaml_content`.
4. Validate each migrated pipeline before setting `persistence_version = 3`.
5. Run DB-only contract tests and parity checks.
6. Keep `yaml_content` mirror writes enabled for at least one stable release cycle.
7. Switch to `BRIX_PIPELINE_STORAGE_MODE=db`.
8. Keep restore tools and `yaml_content` until rollback is no longer needed.

## Bottom Line

The existing DB-only plan is directionally correct, but it is not complete until:

- the `pipeline_step` schema includes the 8 missing `Step` fields listed above
- MCP CRUD tests are split into DB-only contract tests vs YAML compatibility tests
- rollout ships with an explicit storage-mode toggle and a per-pipeline restore path from `yaml_content`

That is the minimum needed to make the migration reversible, testable, and lossless.
