# Bug Analysis: `step_dict_to_row()` / `pipeline_step` column mismatch

## Root cause

The failure is caused by two pieces of code in [src/brix/db.py](/root/docker/brix/src/brix/db.py):

1. [src/brix/db.py:171](/root/docker/brix/src/brix/db.py#L171) to [src/brix/db.py:175](/root/docker/brix/src/brix/db.py#L175)
   `step_dict_to_row()` maps known fields, but for any unknown key it falls back to `column = key`.

```python
column = _STEP_FIELD_TO_COLUMN.get(key)
if column is None and f"{key}_json" in _STEP_JSON_COLUMNS:
    column = f"{key}_json"
if column is None:
    column = key
```

2. [src/brix/db.py:2397](/root/docker/brix/src/brix/db.py#L2397) to [src/brix/db.py:2405](/root/docker/brix/src/brix/db.py#L2405)
   `upsert_step()` then inserts every key returned by `step_dict_to_row()` directly into `pipeline_step`.

```python
cols = list(row.keys())
...
INSERT INTO pipeline_step ({",".join(cols)})
```

That means any extra key in `step_dict` becomes a SQL column name. If `step_dict` contains `success`, the insert tries to write `pipeline_step.success`, but the DDL only defines `success_on_stop`.

## Where `success` comes from

It does not come from the `Step` model.

- [src/brix/models.py:185](/root/docker/brix/src/brix/models.py#L185) defines `success_on_stop: bool = True`
- There is no `success` field on `Step`

`success` therefore comes from an unvalidated/raw step dict passed into `upsert_step()`, not from Pydantic model serialization.

Relevant raw-dict write paths:

- [src/brix/pipeline_store.py:248](/root/docker/brix/src/brix/pipeline_store.py#L248) to [src/brix/pipeline_store.py:249](/root/docker/brix/src/brix/pipeline_store.py#L249)
- [src/brix/mcp_handlers/steps.py:328](/root/docker/brix/src/brix/mcp_handlers/steps.py#L328) to [src/brix/mcp_handlers/steps.py:329](/root/docker/brix/src/brix/mcp_handlers/steps.py#L329)

Both pass raw `step` dicts to `upsert_step()` without filtering unknown keys.

## `Step` model vs `pipeline_step` DDL

### `Step` model fields relevant to DB rows

The `Step` model maps cleanly to the `pipeline_step` table columns.

- Model field `id` maps to DB column `step_key`
- Model field `type` maps to DB column `step_type`
- Model field `pipeline` maps to DB column `sub_pipeline`
- Model field `to` maps to DB column `notify_to`
- Model field `when` maps to DB column `when_expr`
- Model field `until` maps to DB column `until_expr`
- Model field `foreach` maps to DB column `foreach_expr`
- Model field `while_condition` maps to DB column `while_expr`
- JSON-backed fields map to `*_json` columns

There is no `success` field in the model. The correct stop-runner field is `success_on_stop`.

## `pipeline_step` DDL: all columns

From [src/brix/db.py:234](/root/docker/brix/src/brix/db.py#L234) to [src/brix/db.py:310](/root/docker/brix/src/brix/db.py#L310), the table columns are:

- `id`
- `pipeline_id`
- `step_key`
- `parent_step_id`
- `container`
- `branch_key`
- `branch_when`
- `position`
- `step_type`
- `enabled`
- `script`
- `helper`
- `url`
- `method`
- `headers_json`
- `body_json`
- `command`
- `args_json`
- `shell`
- `server`
- `tool`
- `sub_pipeline`
- `pipelines_json`
- `shared_params_json`
- `values_json`
- `persist`
- `message`
- `success_on_stop`
- `channel`
- `notify_to`
- `approval_timeout`
- `on_timeout`
- `choices_json`
- `default_steps_json`
- `until_expr`
- `while_expr`
- `max_iterations`
- `sequence_json`
- `sub_steps_json`
- `params_json`
- `foreach_expr`
- `parallel`
- `concurrency`
- `batch_size`
- `flat_output`
- `when_expr`
- `else_of`
- `on_error`
- `retry_profile`
- `timeout`
- `fetch_all_pages`
- `progress`
- `requirements_json`
- `input_schema_json`
- `output_schema_json`
- `rules_json`
- `config_json`
- `depends_on_json`
- `cache_json`
- `circuit_breaker_json`
- `rate_limit_json`
- `compensate_json`
- `persist_output`
- `pause_before`
- `persist_data`
- `profile`
- `queue_name`
- `collect_until`
- `collect_for`
- `flush_to`
- `event`
- `data_json`
- `stream`
- `unwrap_json`
- `created_at`
- `updated_at`

## All mismatches

### Invalid output keys from `Step` model through `step_dict_to_row()`

None.

When the input is a valid `Step` model dump, the produced DB keys are all valid `pipeline_step` columns.

### DDL columns not produced by `Step.model_dump()` through `step_dict_to_row()`

These are expected structural columns, not bugs:

- `id`
- `pipeline_id`
- `parent_step_id`
- `container`
- `branch_key`
- `branch_when`
- `position`
- `created_at`
- `updated_at`

These are populated separately or reserved for nested/branched step storage.

### Actual mismatch class causing the bug

Any unknown input key becomes a DB column name.

Known observed example:

- input key `success`
- produced row key `success`
- DDL column does not exist

So the real mismatch is not between the `Step` model and the DDL. It is between arbitrary raw step dicts and the DDL, because `step_dict_to_row()` does not restrict output to allowed columns.

## Exact fix needed

### Required code change

Change [src/brix/db.py:167](/root/docker/brix/src/brix/db.py#L167) to [src/brix/db.py:182](/root/docker/brix/src/brix/db.py#L182) so `step_dict_to_row()` only emits valid `pipeline_step` columns.

Recommended behavior:

- define an allowed set of DB columns for step payloads
- after applying field-name remapping and `*_json` remapping, reject or ignore unknown columns
- do not fall back to `column = key` for unknown keys unless that key is in the allowed column set

In practice:

- keep the explicit mappings in `_STEP_FIELD_TO_COLUMN`
- keep JSON detection for known `*_json` columns
- add an `_STEP_ALLOWED_COLUMNS` set derived from the `pipeline_step` schema, excluding structural write-managed fields like `id`, `pipeline_id`, `position`, `created_at`, `updated_at`
- in `step_dict_to_row()`, if a resolved column is not in `_STEP_ALLOWED_COLUMNS`, skip it or raise `ValueError`

### Minimal safe fix

In [src/brix/db.py:174](/root/docker/brix/src/brix/db.py#L174) to [src/brix/db.py:175](/root/docker/brix/src/brix/db.py#L175), replace the blind fallback:

```python
if column is None:
    column = key
```

with guarded logic such as:

```python
if column is None:
    if key in _STEP_ALLOWED_COLUMNS:
        column = key
    else:
        continue
```

or, better for debuggability:

```python
if column is None:
    if key in _STEP_ALLOWED_COLUMNS:
        column = key
    else:
        raise ValueError(f"Unknown pipeline_step field: {key}")
```

### Optional hardening

Also harden the callers that currently pass raw dicts:

- [src/brix/pipeline_store.py:249](/root/docker/brix/src/brix/pipeline_store.py#L249)
- [src/brix/mcp_handlers/steps.py:329](/root/docker/brix/src/brix/mcp_handlers/steps.py#L329)

They can validate with `Step.model_validate(step)` before calling `upsert_step()`, but the primary fix still belongs in `step_dict_to_row()` because that is the DB boundary and currently trusts arbitrary keys.

## Bottom line

- `pipeline_step` has `success_on_stop`, not `success`
- `Step` defines `success_on_stop`, not `success`
- `success` is coming from a raw/unvalidated step dict
- the exact bug is the fallback in `step_dict_to_row()` plus the blind insert in `upsert_step()`
- there are no other model-to-DDL column mismatches beyond the expected structural DDL-only columns listed above
