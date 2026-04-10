# Step Field Migration Policy

## Purpose
Runner-specific top-level `Step` fields are historical compatibility inputs. The target model is brick-first: runner semantics belong in each brick schema and therefore in `effective_config`, not in the global `Step` model.

## Current Compatibility Surface
Brix still accepts fields such as `connection`, `query`, `server`, `tool`, `script`, `pipeline`, `url`, `command`, `values`, `choices`, and `sequence` at top level. These fields are not deleted because existing DB rows, imports, and bundles may still contain them.

## Canonical Home
For new or updated definitions, runner-specific fields should be stored under `config.<field>` and validated through the brick schema. Examples:

- `connection` -> `config.connection`
- `query` -> `config.query`
- `server` -> `config.server`
- `tool` -> `config.tool`
- `script` -> `config.script`
- `pipeline` -> `config.pipeline`

Generic orchestration fields remain valid at top level: `id`, `type`, `enabled`, `params`, `foreach`, `when`, `on_error`, `timeout`, `requirements`, `input_schema`, and `output_schema`.

## Compatibility Rules
Existing top-level runner fields remain readable. During materialization they are treated as compatibility inputs and surfaced in provenance. When a field exists both at top level and in `config`, the effective-step rules in `03a-effective-step-spec.md` apply: the brick schema and `config.<field>` define the canonical direction, and compatibility behavior must remain explicit.

The validator emits informational findings for explicitly supplied top-level runner fields. It does not flag Pydantic defaults, because defaults are not migration debt.

## Deprecation Roadmap
Phase 1 records the policy in code and documentation while keeping runtime compatibility.

Phase 2 should extend MCP create/update tools to prefer `config` for runner-specific inputs and include guidance when callers submit top-level fields.

Phase 3 should add drift and migration reports that identify persisted rows still using top-level runner fields.

Phase 4 may restrict new brick definitions from introducing additional global `Step` fields unless they are orchestration primitives rather than runner semantics.

## Design Rule
Adding a reusable brick must not require adding another global runner-specific field to `Step`. If a brick needs a new parameter, it belongs in the brick config schema.
