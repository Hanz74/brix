# Plan: DB-Only Pipelines With Normalized Step Rows

## Executive Summary

The current persistence model is still YAML-centric even though the DB already stores a `pipeline` row plus a `yaml_content` blob. The practical source of truth is the serialized whole-pipeline document:

- MCP CRUD handlers load the whole pipeline as a raw dict, mutate it in memory, and rewrite the entire YAML blob.
- `PipelineStore.save()` writes YAML to disk and mirrors the same string into `pipeline.yaml_content`.
- `update_step` is not a row update. It is a full-document read/modify/write path, so null/partial merge bugs are structural, not incidental.
- The engine executes `Pipeline`/`Step` Pydantic objects, but those objects are assembled by reparsing YAML text instead of loading relational data.

The migration target should be:

- `pipeline` row holds pipeline metadata only.
- `pipeline_step` rows hold all executable steps, including nested steps.
- `pipeline_credential` rows hold credential definitions.
- Optional support tables hold pipeline inputs and choice-branch metadata.
- Runtime loads a `Pipeline` object from DB rows directly, without YAML parsing.
- YAML becomes import/export compatibility, not runtime persistence.

This removes full-document rewrites, makes `update_step` a single `UPDATE`, and removes the need to keep disk YAML synchronized with DB state.

---

## Current Architecture

### Persistence Shape Today

- Disk:
  - Pipelines are written to `~/.brix/pipelines/<name>.yaml`.
- DB:
  - `pipeline` table stores metadata plus `yaml_content TEXT` in [`src/brix/db.py:94`](/root/docker/brix/src/brix/db.py#L94).
- Runtime:
  - CRUD code edits raw pipeline dicts.
  - `PipelineStore.load()` reparses `yaml_content` into a `Pipeline` model in [`src/brix/pipeline_store.py:126`](/root/docker/brix/src/brix/pipeline_store.py#L126).
  - The engine executes the model but does not own persistence.

### Core Structural Problems

1. `update_step` is whole-document persistence

- `steps._handle_update_step()` loads the full raw pipeline, finds the step recursively, mutates the dict, bumps version, then calls `store.save(raw, name)` in [`src/brix/mcp_handlers/steps.py:457`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L457).
- That save path serializes the whole pipeline YAML again in [`src/brix/pipeline_store.py:107`](/root/docker/brix/src/brix/pipeline_store.py#L107).

2. Step CRUD cannot be atomic at the DB row level

- There is no step table.
- Ordering, nesting, and branch structure live only inside YAML arrays.

3. Dependency refresh reparses YAML again

- `refresh_pipeline_deps()` reads `pipeline.yaml_content`, YAML-parses it, then rescans helper refs in [`src/brix/db.py:1637`](/root/docker/brix/src/brix/db.py#L1637).

4. Backward-compatibility paths still depend on filesystem scanning

- Similarity checks and explicit-directory listing still read `.yaml` files directly in [`src/brix/mcp_handlers/_shared.py:354`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L354) and [`src/brix/mcp_handlers/pipelines.py:616`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L616).

5. `yaml_content` is acting as a pseudo-document database

- DB reads are still string-to-YAML-to-dict or string-to-YAML-to-Pydantic conversions rather than relational loads.

---

## File-By-File Analysis

## 1. `src/brix/db.py`

### YAML on Disk Reads/Writes

Reads YAML from disk:

- Startup pipeline sync scans `*.yaml` / `*.yml` and parses each file in [`src/brix/db.py:1475`](/root/docker/brix/src/brix/db.py#L1475), specifically:
  - directory scan in [`src/brix/db.py:1501`](/root/docker/brix/src/brix/db.py#L1501)
  - `yaml_file.read_text()` + `yaml.safe_load()` in [`src/brix/db.py:1503`](/root/docker/brix/src/brix/db.py#L1503)

No direct pipeline YAML disk writes in this file.

### `yaml_content` DB Reads/Writes

Schema:

- `pipeline.yaml_content TEXT DEFAULT ''` in [`src/brix/db.py:94`](/root/docker/brix/src/brix/db.py#L94)

Writes:

- `upsert_pipeline(..., yaml_content=...)` supports writing the blob in [`src/brix/db.py:1980`](/root/docker/brix/src/brix/db.py#L1980)
- conditional column handling in [`src/brix/db.py:2018`](/root/docker/brix/src/brix/db.py#L2018)

Reads:

- `refresh_pipeline_deps()` selects `id, yaml_content` in [`src/brix/db.py:1644`](/root/docker/brix/src/brix/db.py#L1644)
- blob is YAML-parsed in [`src/brix/db.py:1661`](/root/docker/brix/src/brix/db.py#L1661)
- `get_pipeline_yaml_content()` reads the blob in [`src/brix/db.py:2440`](/root/docker/brix/src/brix/db.py#L2440)
- `count_pipelines_with_content()` counts populated blobs in [`src/brix/db.py:2469`](/root/docker/brix/src/brix/db.py#L2469)

### What Must Change

- Add first-class step persistence tables.
- Stop using `yaml_content` as the source for helper dependency refresh.
- Replace `sync_pipelines_from_dirs()` with:
  - one-time import from YAML for migration
  - optional compatibility importer, not startup sync
- Extend `pipeline` metadata storage so the engine can assemble a `Pipeline` model without reparsing YAML.
- Move helper dependency extraction to step-row scanning.

### DB Responsibilities After Migration

- `pipeline` becomes metadata only.
- `pipeline_step` becomes the canonical step source.
- `pipeline_credential` becomes the canonical credential source.
- Optional `pipeline_input` and `pipeline_branch` tables eliminate residual YAML parsing.

---

## 2. `src/brix/mcp_handlers/pipelines.py`

### YAML on Disk Reads/Writes

Reads:

- `_handle_create_pipeline()` checks existing YAML via `_load_pipeline_yaml(name)` in [`src/brix/mcp_handlers/pipelines.py:143`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L143)
- `_handle_get_pipeline()` loads raw pipeline via `_load_pipeline_yaml(name)` in [`src/brix/mcp_handlers/pipelines.py:258`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L258)
- `_handle_validate_pipeline()` uses `_load_pipeline_yaml(name)` in [`src/brix/mcp_handlers/pipelines.py:601`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L601)
- explicit-directory list scans YAML files in [`src/brix/mcp_handlers/pipelines.py:625`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L625), parsing each in [`src/brix/mcp_handlers/pipelines.py:632`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L632)
- `_get_current_content_str()` reconstructs current pipeline content via `_load_pipeline_yaml(name)` in [`src/brix/mcp_handlers/pipelines.py:797`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L797)
- `test_pipeline` resolves a YAML file path from search paths in [`src/brix/mcp_handlers/pipelines.py:1014`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L1014)

Writes:

- `_handle_create_pipeline()` persists through `_save_pipeline_yaml(name, pipeline_data)` in [`src/brix/mcp_handlers/pipelines.py:193`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L193)
- `_handle_update_pipeline()` saves through `store.save(raw, name)` in [`src/brix/mcp_handlers/pipelines.py:386`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L386)
- `_handle_rename_pipeline()` saves the renamed pipeline with `store.save(data, name=new_name)` in [`src/brix/mcp_handlers/pipelines.py:565`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L565)
- `_handle_rollback()` restores archived content with `store.save(raw, name)` in [`src/brix/mcp_handlers/pipelines.py:873`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L873)

### `yaml_content` DB Reads/Writes

Reads indirectly through `PipelineStore`:

- `_handle_update_pipeline()` loads raw via `store.load_raw(name)` in [`src/brix/mcp_handlers/pipelines.py:328`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L328)
- validation after save uses `store.load(name)` in [`src/brix/mcp_handlers/pipelines.py:421`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L421)
- list/search use `store.list_all()` in [`src/brix/mcp_handlers/pipelines.py:693`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L693) and [`src/brix/mcp_handlers/pipelines.py:746`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L746)
- rename loads old raw via `store.load_raw(old_name)` in [`src/brix/mcp_handlers/pipelines.py:543`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L543)

Writes indirectly through `PipelineStore`:

- create via `_save_pipeline_yaml()` in [`src/brix/mcp_handlers/pipelines.py:193`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L193)
- update via `store.save()` in [`src/brix/mcp_handlers/pipelines.py:386`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L386)
- rename via `store.save()` in [`src/brix/mcp_handlers/pipelines.py:565`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L565)
- rollback via `store.save()` in [`src/brix/mcp_handlers/pipelines.py:874`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L874)

Direct metadata upserts to `pipeline` row:

- org-field-only `upsert_pipeline(...)` in [`src/brix/mcp_handlers/pipelines.py:207`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L207)
- same on update in [`src/brix/mcp_handlers/pipelines.py:400`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L400)

### What Must Change

- `create_pipeline` should:
  - insert pipeline metadata row
  - insert input rows
  - insert credential rows
  - insert step rows
  - never call `_save_pipeline_yaml`
- `get_pipeline` should:
  - assemble from DB rows
  - optionally support `format="yaml"` by reconstructing YAML from DB, not reading disk
- `update_pipeline` should:
  - update pipeline metadata tables only
  - never load the whole pipeline document unless explicitly needed for compatibility export
- `rename_pipeline` should:
  - update `pipeline.name`
  - not rename disk files in DB-only mode
- `list_pipelines` explicit-dir branch should be demoted to import-only tooling.
- `test_pipeline` should stop depending on a YAML file path and instead accept a `Pipeline` object or DB repository load.

---

## 3. `src/brix/mcp_handlers/steps.py`

### YAML on Disk Reads/Writes

Reads:

- `add_step` loads raw pipeline via `_load_pipeline_yaml(name)` in [`src/brix/mcp_handlers/steps.py:208`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L208)
- `remove_step` loads raw pipeline via `_load_pipeline_yaml(name)` in [`src/brix/mcp_handlers/steps.py:413`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L413)
- `update_step` loads raw pipeline via `store.load_raw(name)` in [`src/brix/mcp_handlers/steps.py:466`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L466)
- `get_step` loads raw pipeline via `store.load_raw(pipeline_name)` in [`src/brix/mcp_handlers/steps.py:535`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L535)

Writes:

- `add_step` persists full pipeline through `_save_pipeline_yaml(name, data)` in [`src/brix/mcp_handlers/steps.py:285`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L285)
- `remove_step` persists full pipeline through `_save_pipeline_yaml(name, data)` in [`src/brix/mcp_handlers/steps.py:434`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L434)
- `update_step` persists full pipeline through `store.save(raw, name)` in [`src/brix/mcp_handlers/steps.py:489`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L489)

### `yaml_content` DB Reads/Writes

Reads indirectly through `PipelineStore`:

- `update_step` load in [`src/brix/mcp_handlers/steps.py:466`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L466)
- `get_step` load in [`src/brix/mcp_handlers/steps.py:535`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L535)

Writes indirectly through `PipelineStore` / `_save_pipeline_yaml`:

- `add_step` in [`src/brix/mcp_handlers/steps.py:285`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L285)
- `remove_step` in [`src/brix/mcp_handlers/steps.py:434`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L434)
- `update_step` in [`src/brix/mcp_handlers/steps.py:489`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L489)

### What Must Change

This file benefits the most from normalization.

- `add_step` becomes `INSERT INTO pipeline_step ...`
- `remove_step` becomes `DELETE FROM pipeline_step WHERE ...`
- `get_step` becomes `SELECT ... FROM pipeline_step WHERE pipeline_id=? AND step_key=?`
- `update_step` becomes a simple row update plus optional branch/order updates

The current null-reversion bug exists because the handler rewrites the whole document. With step rows:

- `{"timeout": null}` can map to `timeout = NULL`
- `{"params": {"foo": null}}` can update JSON payload deterministically
- unrelated sibling and parent steps are untouched

### Required Structural Change

Nested steps must no longer require recursive in-memory traversal of a YAML tree. Each step needs:

- a stable row id
- a pipeline foreign key
- optional parent step foreign key
- a container marker (`steps`, `sequence`, `sub_steps`, `default_steps`, `choice`)
- an order index

---

## 4. `src/brix/mcp_handlers/_shared.py`

### YAML on Disk Reads/Writes

Reads:

- `_load_pipeline_yaml()` delegates to `PipelineStore.load_raw()` in [`src/brix/mcp_handlers/_shared.py:129`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L129)
- `_validate_pipeline_dict()` converts dict to YAML string and reparses through `PipelineLoader.load_from_string()` in [`src/brix/mcp_handlers/_shared.py:147`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L147)
- `_find_similar_pipelines()` scans search paths for YAML files and parses them in [`src/brix/mcp_handlers/_shared.py:354`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L354), especially [`src/brix/mcp_handlers/_shared.py:368`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L368) and [`src/brix/mcp_handlers/_shared.py:377`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L377)
- `_scan_pipelines_for_helper()` and `_scan_pipelines_for_sub_pipeline()` dump reconstructed raw data back to YAML strings for substring search in [`src/brix/mcp_handlers/_shared.py:396`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L396) and [`src/brix/mcp_handlers/_shared.py:411`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L411)

Writes:

- `_save_pipeline_yaml()` delegates to `PipelineStore.save()` in [`src/brix/mcp_handlers/_shared.py:141`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L141)

### `yaml_content` DB Reads/Writes

Indirect only through `PipelineStore`:

- `_load_pipeline_yaml()` in [`src/brix/mcp_handlers/_shared.py:136`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L136)
- `_save_pipeline_yaml()` in [`src/brix/mcp_handlers/_shared.py:143`](/root/docker/brix/src/brix/mcp_handlers/_shared.py#L143)

### What Must Change

- Rename the abstraction. `_load_pipeline_yaml` / `_save_pipeline_yaml` should become repository-level helpers, e.g. `_load_pipeline_raw` / `_save_pipeline_graph`, or be deleted.
- Similarity and cross-reference scans should use DB queries:
  - descriptions from `pipeline.description`
  - helper usage from `pipeline_step.helper` / `pipeline_step.script`
  - sub-pipeline usage from `pipeline_step.pipeline_ref`
- Validation should stop converting dicts to YAML strings; validate directly from an assembled `Pipeline` object or from structured DB/raw dict data.

---

## 5. `src/brix/engine.py`

### YAML on Disk Reads/Writes

Pipeline execution path in this file does not read pipeline YAML directly.

The only YAML file read here is unrelated server config:

- `~/.brix/servers.yaml` is parsed in [`src/brix/engine.py:1498`](/root/docker/brix/src/brix/engine.py#L1498)

### `yaml_content` DB Reads/Writes

No direct `yaml_content` access in `engine.py`.

Important execution boundary:

- `PipelineEngine.run()` accepts a `Pipeline` object, not a pipeline name, in [`src/brix/engine.py:446`](/root/docker/brix/src/brix/engine.py#L446)
- `PipelineContext.from_pipeline()` is called in [`src/brix/engine.py:484`](/root/docker/brix/src/brix/engine.py#L484)

### What Must Change

The engine itself does not need a major execution rewrite. The key change is how callers obtain the `Pipeline` object.

Recommended direction:

- Introduce a DB-backed repository/assembler:
  - `PipelineRepository.load_pipeline(name) -> Pipeline`
- Keep `PipelineEngine.run(pipeline: Pipeline, ...)` unchanged.

This minimizes blast radius:

- templating/rendering logic stays in the engine/loader layer
- persistence stops leaking into runtime execution

Secondary engine improvements enabled by normalized step rows:

- resolve helper usage directly from step rows before run
- optionally resolve only step-local credential aliases instead of populating all pipeline credentials into every step context

---

## 6. `src/brix/loader.py`

### YAML on Disk Reads/Writes

Reads YAML:

- `load(path)` reads a YAML file in [`src/brix/loader.py:42`](/root/docker/brix/src/brix/loader.py#L42)
- `load_from_string(yaml_string)` parses YAML text in [`src/brix/loader.py:51`](/root/docker/brix/src/brix/loader.py#L51)
- include resolution reads external YAML files in [`src/brix/loader.py:176`](/root/docker/brix/src/brix/loader.py#L176)
- template inheritance reads template YAML files later in the file, beginning from the `extends` workflow in [`src/brix/loader.py:216`](/root/docker/brix/src/brix/loader.py#L216)

No pipeline YAML writes in this file.

### `yaml_content` DB Reads/Writes

None directly. It only parses strings or files handed to it.

### What Must Change

`PipelineLoader` should stop being the persistence loader for normal runtime pipelines.

Recommended split:

- Keep `PipelineLoader` for:
  - YAML import
  - compatibility export/import
  - include / extends / template workflows if those remain YAML-defined authoring features
- Add a new assembler:
  - `PipelineAssembler.from_db_rows(...) -> Pipeline`

This avoids contaminating relational runtime loading with YAML-specific concerns like:

- `include`
- `extends`
- external file fragments

If template/include features remain author-time features, they should resolve during create/import, not during every runtime load.

---

## 7. `src/brix/models.py`

### YAML on Disk Reads/Writes

None.

### `yaml_content` DB Reads/Writes

None.

### What Must Change

The Pydantic models are already suitable runtime shapes, but they currently mirror YAML document structure.

Relevant runtime model fields:

- `Pipeline.credentials` in [`src/brix/models.py:386`](/root/docker/brix/src/brix/models.py#L386)
- `Pipeline.groups` in [`src/brix/models.py:448`](/root/docker/brix/src/brix/models.py#L448)
- `Pipeline.steps` in [`src/brix/models.py:449`](/root/docker/brix/src/brix/models.py#L449)
- `Step` nested containers:
  - `choices` in [`src/brix/models.py:196`](/root/docker/brix/src/brix/models.py#L196)
  - `default_steps` in [`src/brix/models.py:197`](/root/docker/brix/src/brix/models.py#L197)
  - `sub_steps` in [`src/brix/models.py:200`](/root/docker/brix/src/brix/models.py#L200)
  - `sequence` in [`src/brix/models.py:206`](/root/docker/brix/src/brix/models.py#L206)

Recommended changes:

- Do not force runtime model redesign immediately.
- Keep `Pipeline` and `Step` as execution models.
- Build them from normalized rows instead of YAML.

Longer term:

- consider widening `Step.type` from a giant `Literal[...]` to plain `str`
- current Literal-based enumeration makes DB-native extensibility harder than necessary

---

## 8. `src/brix/context.py`

### YAML on Disk Reads/Writes

No pipeline YAML reads/writes.

This file persists run-state JSON, not pipeline definition YAML.

### `yaml_content` DB Reads/Writes

None.

### What Must Change

Credential handling is concentrated here:

- `PipelineContext.from_pipeline()` resolves `pipeline.credentials` in [`src/brix/context.py:114`](/root/docker/brix/src/brix/context.py#L114)
- it loops through `pipeline.credentials.items()` in [`src/brix/context.py:153`](/root/docker/brix/src/brix/context.py#L153)
- refresh behavior is implemented in `_refresh_credential()` in [`src/brix/context.py:179`](/root/docker/brix/src/brix/context.py#L179)

This should remain the runtime resolution layer, but the source of credential definitions should change from YAML-derived `Pipeline.credentials` to DB-assembled pipeline credential objects.

Recommended change:

- keep the runtime contract: `Pipeline.credentials: dict[str, CredentialRef]`
- load that dict from `pipeline_credential` rows
- optionally narrow runtime exposure to per-step aliases

Credential values should still never be persisted in step rows or run metadata.

---

## 9. `src/brix/pipeline_store.py`

### YAML on Disk Reads/Writes

Reads:

- existing DB blob is YAML-parsed for `created_at` preservation in [`src/brix/pipeline_store.py:76`](/root/docker/brix/src/brix/pipeline_store.py#L76)
- fallback filesystem read for existing file in [`src/brix/pipeline_store.py:90`](/root/docker/brix/src/brix/pipeline_store.py#L90)

Writes:

- whole-pipeline YAML serialization in [`src/brix/pipeline_store.py:107`](/root/docker/brix/src/brix/pipeline_store.py#L107)
- disk write in [`src/brix/pipeline_store.py:109`](/root/docker/brix/src/brix/pipeline_store.py#L109)

### `yaml_content` DB Reads/Writes

Writes:

- `upsert_pipeline(..., yaml_content=yaml_content)` in [`src/brix/pipeline_store.py:117`](/root/docker/brix/src/brix/pipeline_store.py#L117)

Reads:

- `load()` reads blob in [`src/brix/pipeline_store.py:133`](/root/docker/brix/src/brix/pipeline_store.py#L133)
- `load_raw()` reads blob in [`src/brix/pipeline_store.py:143`](/root/docker/brix/src/brix/pipeline_store.py#L143)
- `list_all()` repeatedly reads blob in [`src/brix/pipeline_store.py:174`](/root/docker/brix/src/brix/pipeline_store.py#L174)

### What Must Change

This file should be replaced or substantially narrowed.

Current role:

- hybrid YAML serializer
- DB blob mirror
- lookup/resolve helper

Target role:

- repository facade over relational persistence

Recommended refactor:

- replace `PipelineStore` with `PipelineRepository`
- repository methods:
  - `create_pipeline(raw_or_model)`
  - `load_pipeline(name) -> Pipeline`
  - `load_pipeline_raw(name) -> dict`
  - `list_pipelines()`
  - `delete_pipeline(name)`
  - `find_by_id(id)`
- no YAML serialization in normal CRUD paths

Compatibility-only methods can remain separately:

- `export_pipeline_yaml(name) -> str`
- `import_pipeline_yaml(yaml_text)`

---

## 10. `src/brix/bundle.py`

### YAML on Disk Reads/Writes

Reads:

- single-pipeline export reads the pipeline YAML file directly in [`src/brix/bundle.py:150`](/root/docker/brix/src/brix/bundle.py#L150)
- helper ref scan parses YAML text in [`src/brix/bundle.py:90`](/root/docker/brix/src/brix/bundle.py#L90)
- import writes `pipeline.yaml` to disk in [`src/brix/bundle.py:326`](/root/docker/brix/src/brix/bundle.py#L326)
- project import reads extracted YAML files from archive in [`src/brix/bundle.py:792`](/root/docker/brix/src/brix/bundle.py#L792)

### `yaml_content` DB Reads/Writes

Reads:

- project export gets blob for credential ref extraction in [`src/brix/bundle.py:559`](/root/docker/brix/src/brix/bundle.py#L559)
- project export gets blob again for archive writing in [`src/brix/bundle.py:599`](/root/docker/brix/src/brix/bundle.py#L599)
- project import checks existing blob in [`src/brix/bundle.py:796`](/root/docker/brix/src/brix/bundle.py#L796)

Writes:

- project import upserts pipeline with `yaml_content` in [`src/brix/bundle.py:808`](/root/docker/brix/src/brix/bundle.py#L808)

### What Must Change

Bundles should remain portable, but the source should become reconstructed YAML or JSON from DB rows.

Recommended direction:

- Keep archive format compatible for now:
  - still export `pipelines/<name>.yaml`
- Generate that YAML from DB rows, not from `yaml_content`
- Add a manifest version flag indicating `persistence_format=db-normalized`
- Import should:
  - parse YAML or JSON bundle content
  - populate normalized rows
  - not store imported `yaml_content` except optional compatibility mirror

Longer term:

- support a DB-native bundle representation:
  - `pipeline.json`
  - `steps.json`
  - `credentials.json`
- still optionally emit YAML for external portability

---

## Proposed DB Schema

## Goals

- First-class step rows
- support nested step trees
- support stable step updates without full-pipeline rewrites
- retain enough structure to rebuild the existing `Pipeline`/`Step` models
- avoid over-normalizing every sparse runner field into dozens of tiny tables

## Recommended Tables

### 1. `pipeline`

Keep and extend the existing table.

Recommended columns:

```sql
CREATE TABLE pipeline (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0.0',
    description TEXT DEFAULT '',
    project TEXT DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    group_name TEXT DEFAULT '',

    kind TEXT,
    extends TEXT,
    is_template INTEGER NOT NULL DEFAULT 0,

    brix_version TEXT,
    compositor_mode INTEGER NOT NULL DEFAULT 0,
    allow_code INTEGER NOT NULL DEFAULT 1,
    strict_bricks INTEGER NOT NULL DEFAULT 0,
    test_mode INTEGER NOT NULL DEFAULT 0,

    requirements_json TEXT NOT NULL DEFAULT '[]',
    error_handling_json TEXT NOT NULL DEFAULT '{}',
    retry_profiles_json TEXT NOT NULL DEFAULT '{}',
    notify_json TEXT NOT NULL DEFAULT '{}',
    output_json TEXT,
    output_slots_json TEXT NOT NULL DEFAULT '{}',
    template_params_json TEXT NOT NULL DEFAULT '{}',
    blueprint_params_json TEXT NOT NULL DEFAULT '[]',
    groups_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT,

    path TEXT,
    yaml_content TEXT,
    persistence_version INTEGER NOT NULL DEFAULT 2
);
```

Notes:

- `path` becomes nullable/legacy.
- `yaml_content` remains temporary for rollback/export compatibility during migration.
- `groups_json` can remain JSON unless you want to normalize include/template authoring too.

### 2. `pipeline_input`

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

### 3. `pipeline_credential`

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

This maps directly to `Pipeline.credentials: dict[str, CredentialRef]`.

### 4. `pipeline_step`

This is the core normalized step table.

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

    helper TEXT,
    script TEXT,
    url TEXT,
    method TEXT,
    command TEXT,
    shell INTEGER,
    server TEXT,
    tool TEXT,
    pipeline_ref TEXT,
    message TEXT,
    approval_timeout TEXT,
    on_timeout TEXT,
    foreach_expr TEXT,
    when_expr TEXT,
    else_of TEXT,
    on_error TEXT,
    retry_profile TEXT,
    timeout TEXT,
    queue_name TEXT,
    collect_until INTEGER,
    collect_for TEXT,
    flush_to TEXT,
    event TEXT,

    parallel INTEGER NOT NULL DEFAULT 0,
    concurrency INTEGER NOT NULL DEFAULT 10,
    batch_size INTEGER NOT NULL DEFAULT 0,
    flat_output INTEGER NOT NULL DEFAULT 0,
    fetch_all_pages INTEGER NOT NULL DEFAULT 0,
    progress INTEGER NOT NULL DEFAULT 0,
    persist_output INTEGER NOT NULL DEFAULT 0,
    pause_before INTEGER NOT NULL DEFAULT 0,
    persist_data INTEGER NOT NULL DEFAULT 1,
    stream INTEGER NOT NULL DEFAULT 0,
    unwrap_json INTEGER,

    profile TEXT,

    args_json TEXT,
    headers_json TEXT,
    body_json TEXT,
    pipelines_json TEXT,
    shared_params_json TEXT NOT NULL DEFAULT '{}',
    values_json TEXT,
    params_json TEXT,
    requirements_json TEXT NOT NULL DEFAULT '[]',
    input_schema_json TEXT NOT NULL DEFAULT '{}',
    output_schema_json TEXT NOT NULL DEFAULT '{}',
    rules_json TEXT,
    config_json TEXT,
    cache_json TEXT,
    circuit_breaker_json TEXT,
    rate_limit_json TEXT,
    compensate_json TEXT,
    data_json TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    FOREIGN KEY (pipeline_id) REFERENCES pipeline(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_step_id) REFERENCES pipeline_step(id) ON DELETE CASCADE,
    UNIQUE (pipeline_id, step_key)
);

CREATE INDEX idx_pipeline_step_pipeline_position
    ON pipeline_step (pipeline_id, parent_step_id, container, position);
```

Rationale:

- normalized row per step
- stable nesting via `parent_step_id`
- stable ordering via `position`
- stable branch modeling via `container` + `branch_key` + `branch_when`
- sparse runner-specific data stays in JSON columns where normalization would add little value

### 5. Optional: `step_credential_binding`

This is recommended, not mandatory.

```sql
CREATE TABLE step_credential_binding (
    step_id TEXT NOT NULL,
    credential_alias TEXT NOT NULL,
    usage TEXT DEFAULT '',
    PRIMARY KEY (step_id, credential_alias),
    FOREIGN KEY (step_id) REFERENCES pipeline_step(id) ON DELETE CASCADE
);
```

Use cases:

- least-privilege runtime resolution
- better dependency analysis
- credential impact reporting for bundles/audits

If this is too much for phase 1, infer bindings from Jinja strings and keep runtime behavior unchanged.

---

## How Nested Steps Map To Rows

Represent every executable step as one row.

Examples:

- top-level step:
  - `parent_step_id = NULL`
  - `container = 'steps'`
  - `position = 0..N`

- `repeat.sequence` child:
  - `parent_step_id = <repeat row id>`
  - `container = 'sequence'`
  - `position = ...`

- `parallel.sub_steps` child:
  - `parent_step_id = <parallel row id>`
  - `container = 'sub_steps'`

- `choose.default_steps` child:
  - `parent_step_id = <choose row id>`
  - `container = 'default_steps'`

- `choose.choices[i].steps` child:
  - `parent_step_id = <choose row id>`
  - `container = 'choice_steps'`
  - `branch_key = '<i>'`
  - `branch_when = '<choice.when>'`

This avoids a separate branch table in phase 1.

Tradeoff:

- `branch_when` is duplicated across steps in the same branch.

That duplication is acceptable initially because:

- it keeps writes simple
- it preserves branch identity without join-heavy reconstruction
- branch metadata changes are rare relative to step field updates

If branch metadata later needs more structure, introduce `pipeline_step_branch` in phase 2.

---

## Migration Plan

## Phase 0: Preparation

1. Add new tables behind feature flags.
2. Add repository/assembler code that can:
   - load from normalized tables
   - fall back to `yaml_content`
3. Add a raw-to-rows migrator that can flatten nested steps.

No handler behavior changes yet.

## Phase 1: One-Time Backfill From `yaml_content`

Source of truth for migration input:

- first choice: `pipeline.yaml_content`
- fallback: disk YAML only if blob is empty and file still exists

Migration algorithm per pipeline:

1. Read pipeline row.
2. Parse `yaml_content` with existing YAML parser once.
3. Extract pipeline metadata into `pipeline` columns.
4. Extract `input` into `pipeline_input`.
5. Extract `credentials` into `pipeline_credential`.
6. Recursively flatten `steps` into `pipeline_step`.
7. Optionally infer `step_credential_binding`.
8. Mark pipeline row as `persistence_version = 3`.

Pseudocode:

```python
def migrate_pipeline(raw: dict, pipeline_id: str):
    upsert_pipeline_metadata(...)
    replace_pipeline_inputs(...)
    replace_pipeline_credentials(...)
    replace_pipeline_steps(...)

def replace_pipeline_steps(raw_steps):
    delete existing step rows for pipeline
    walk(raw_steps, parent_step_id=None, container="steps", branch_key=None)

def walk(step_list, parent_step_id, container, branch_key, branch_when=None):
    for position, step in enumerate(step_list):
        row_id = uuid4()
        insert_step_row(...)
        if step.get("sequence"):
            walk(step["sequence"], row_id, "sequence", None)
        if step.get("sub_steps"):
            walk(step["sub_steps"], row_id, "sub_steps", None)
        if step.get("default_steps"):
            walk(step["default_steps"], row_id, "default_steps", None)
        for idx, choice in enumerate(step.get("choices", [])):
            walk(choice.get("steps", []), row_id, "choice_steps", str(idx), choice.get("when"))
```

## Phase 2: Dual Read, Dual Write

Reads:

- prefer normalized tables when `persistence_version >= 3` and step rows exist
- fall back to `yaml_content`

Writes:

- write normalized rows as primary
- optionally regenerate `yaml_content`
- optionally keep writing disk YAML for compatibility tools only

This phase reduces rollback risk.

## Phase 3: Switch Runtime Load To DB-Native

- `PipelineStore.load()` or new `PipelineRepository.load_pipeline()` assembles `Pipeline` from rows.
- engine callers stop touching YAML.
- step CRUD uses row updates/inserts/deletes only.

## Phase 4: Remove YAML As Runtime Persistence

- stop writing disk YAML on normal CRUD
- stop updating `yaml_content` on normal CRUD
- keep export/import compatibility only

## Phase 5: Drop Legacy Storage

After all reads are DB-native and compatibility tooling no longer depends on the blob:

- make `pipeline.path` optional/legacy-only
- delete YAML startup sync
- eventually drop `yaml_content`

Do this only after at least one stable release cycle with telemetry/checks.

---

## How `update_step` Becomes a Simple DB Update

## Current Path

Current flow in [`src/brix/mcp_handlers/steps.py:457`](/root/docker/brix/src/brix/mcp_handlers/steps.py#L457):

1. load full raw pipeline
2. recursively find target step
3. mutate dict
4. bump version
5. serialize whole pipeline
6. write disk YAML
7. write DB blob

## Target Path

New flow:

1. resolve `pipeline_id`
2. `SELECT` target step row by `(pipeline_id, step_key)`
3. apply patch to row columns / JSON columns only
4. `UPDATE pipeline_step ...`
5. bump `pipeline.updated_at` and optionally `pipeline.version`
6. refresh helper/sub-pipeline dependency tables from rows if needed

Example:

```sql
UPDATE pipeline_step
SET
    timeout = ?,
    params_json = ?,
    updated_at = ?
WHERE pipeline_id = ? AND step_key = ?;
```

Patch handling rules:

- scalar keys map directly to columns
- `None` maps to SQL `NULL`
- JSON-backed fields are fully replaced unless you explicitly support JSON-merge semantics
- do not reload sibling/parent rows unless order/nesting changes

Recommended API behavior:

- `update_step` remains a replacement patch, not recursive merge magic
- if callers need nested JSON patch semantics later, add that deliberately

This removes the current null-reversion class of bugs because no YAML merge/re-emission occurs.

---

## How The Engine Loads Pipelines From DB

## Current Boundary

- engine already runs a `Pipeline` object in [`src/brix/engine.py:446`](/root/docker/brix/src/brix/engine.py#L446)
- the problem is upstream assembly, not execution

## Target Assembly Path

Add a repository/assembler layer:

```python
class PipelineRepository:
    def load_pipeline(self, name: str) -> Pipeline:
        pipeline_row = ...
        input_rows = ...
        credential_rows = ...
        step_rows = ...
        raw = self._assemble_raw_dict(...)
        return Pipeline.model_validate(raw)
```

Assembly rules:

1. load pipeline metadata row
2. load input rows into `input`
3. load credential rows into `credentials`
4. load all step rows for pipeline ordered by `(parent_step_id, container, position)`
5. rebuild nested step dicts recursively
6. create `Pipeline` model via `Pipeline.model_validate(raw)`

Important point:

- no YAML string creation
- no YAML parse
- existing engine execution code remains intact

## Loader Role Afterward

- `PipelineLoader` remains for:
  - YAML import
  - template/include expansion during import or authoring
- DB runtime load bypasses `PipelineLoader.load_from_string()`

---

## Credentials At Step Level

## Current Behavior

- pipeline-level aliases are defined in `Pipeline.credentials`
- `PipelineContext.from_pipeline()` resolves all aliases eagerly in [`src/brix/context.py:151`](/root/docker/brix/src/brix/context.py#L151)
- resolved credentials are exposed in the Jinja context under `credentials.*` in [`src/brix/context.py:499`](/root/docker/brix/src/brix/context.py#L499)

## Recommended DB Model

Primary definition stays pipeline-scoped:

- `pipeline_credential(alias, env_ref, refresh_json)`

Optional per-step usage:

- `step_credential_binding(step_id, credential_alias, usage)`

## Runtime Recommendation

Phase 1:

- preserve current semantics
- load all pipeline credential aliases from `pipeline_credential`
- resolve them exactly as today in `PipelineContext.from_pipeline()`

Phase 2:

- resolve only aliases referenced by the current step
- derive needed aliases from:
  - explicit `step_credential_binding`
  - or inferred Jinja references in step fields

Benefits of step-level usage tracking:

- less secret exposure in runtime context
- better auditability
- more precise bundle/export manifests

What should not change:

- secrets remain unresolved at rest
- step rows never store plaintext values
- refresh logic stays in `context.py`

---

## What YAML Files Can Be Removed

## Can Be Removed From Runtime Persistence

Once DB-native load/write is complete, these pipeline YAML artifacts are no longer needed for runtime CRUD/execution:

- `~/.brix/pipelines/<name>.yaml` as normal persistence output
- startup directory scans in `db.sync_pipelines_from_dirs()`
- search-path-based existence checks for runtime CRUD

## Should Remain As Compatibility Features

Keep YAML support only for:

- import from legacy files
- export for humans or external tools
- bundle portability
- authoring-time template/include workflows if you keep those

## Recommended Policy

- normal MCP CRUD: DB-only
- `export_pipeline_yaml` / bundle export: reconstruct YAML on demand
- `import_pipeline_yaml`: parse YAML and populate DB rows

This is cleaner than preserving mirror files forever.

---

## Backward Compatibility Strategy

## Read Strategy

During migration window:

1. if normalized rows exist for a pipeline, use them
2. else if `yaml_content` exists, parse and optionally auto-backfill
3. else if disk YAML exists, import it once and backfill
4. else not found

## Write Strategy

Phase A:

- primary write: normalized tables
- secondary mirror:
  - regenerate `yaml_content`
  - optionally regenerate disk YAML

Phase B:

- primary write: normalized tables only
- export YAML only on demand

## Operational Guardrails

- add a migration status check:
  - count pipelines with step rows
  - count pipelines still relying on `yaml_content`
- add integrity validation:
  - reconstructed `Pipeline.model_validate(raw)` must succeed
  - step order uniqueness per parent/container
  - no orphan nested rows
- keep rollback path:
  - reconstruct YAML from rows and rehydrate `yaml_content` if needed

## Bundle Compatibility

Short term:

- keep exporting YAML inside bundles

Medium term:

- add manifest metadata noting normalized persistence source
- support importing either YAML bundle or DB-native bundle

---

## Concrete Code Change Plan

## A. DB Layer

1. Add tables:
   - `pipeline_input`
   - `pipeline_credential`
   - `pipeline_step`
   - optional `step_credential_binding`
2. Add CRUD methods:
   - `replace_pipeline_inputs()`
   - `replace_pipeline_credentials()`
   - `replace_pipeline_steps()`
   - `get_pipeline_inputs()`
   - `get_pipeline_credentials()`
   - `get_pipeline_steps()`
   - `get_step()`
   - `update_step_row()`
   - `insert_step_row()`
   - `delete_step_row()`
3. Rewrite `refresh_pipeline_deps()` to scan step rows, not `yaml_content`.

## B. Repository Layer

1. Introduce `PipelineRepository`.
2. Move `PipelineStore` callers over to repository methods.
3. Keep `PipelineStore` as a thin compatibility wrapper initially.

## C. MCP Handlers

1. `create_pipeline`
   - insert metadata + normalized rows
   - stop calling `_save_pipeline_yaml`
2. `get_pipeline`
   - assemble from DB
3. `update_pipeline`
   - metadata-only updates against `pipeline`, `pipeline_input`, `pipeline_credential`
4. `add_step`
   - `INSERT pipeline_step`
5. `remove_step`
   - `DELETE pipeline_step`
6. `update_step`
   - single-row update
7. `get_step`
   - single-row load

## D. Engine

1. Add DB-native pipeline load path upstream of `PipelineEngine.run()`.
2. Keep engine execution contract unchanged.

## E. Bundle / Export / Import

1. export from reconstructed DB model
2. import into normalized tables
3. keep YAML archive payload short term

## F. Cleanup

1. delete startup YAML sync
2. delete runtime reliance on pipeline file paths
3. drop `yaml_content` after one stable migration cycle

---

## Highest-Risk Areas

1. Nested step reconstruction

- `choose`, `parallel`, `repeat`, `default_steps`, and `compensate` must round-trip exactly.

2. Versioning semantics

- current version bumps are tied to whole-document saves
- decide whether row-level updates still auto-bump patch/minor the same way

3. Template/include authoring

- if unresolved YAML features are still allowed at runtime, DB-native assembly becomes more complex
- strongly prefer resolving these at import/create time

4. Test infrastructure

- parts of the test path still assume a YAML file path in [`src/brix/mcp_handlers/pipelines.py:1001`](/root/docker/brix/src/brix/mcp_handlers/pipelines.py#L1001)

5. Bundle portability

- external users may still expect YAML bundles even if internal persistence is DB-native

---

## Recommended Rollout Order

1. Add normalized tables and migration script.
2. Backfill from `yaml_content`.
3. Add DB-native repository loader.
4. Switch `get_pipeline` and engine callers to DB-native reads.
5. Switch `update_step` to direct row updates.
6. Switch `add_step` / `remove_step` to row CRUD.
7. Stop writing disk YAML in normal CRUD.
8. Stop writing `yaml_content` except optional compatibility mirror.
9. Remove mirror entirely after a deprecation window.

---

## Bottom Line

The main architectural issue is not just “YAML is on disk”. It is that the system still treats the whole pipeline as a serialized document blob even inside the DB. The correct migration is:

- normalize steps into rows
- keep pipeline metadata relational
- keep credentials relational
- assemble runtime `Pipeline` models from DB rows
- demote YAML to import/export compatibility

That gives you:

- deterministic step CRUD
- atomic updates
- no full-document merge bugs
- no YAML parse on runtime load
- simpler dependency analysis
- a clean path to drop disk files entirely
