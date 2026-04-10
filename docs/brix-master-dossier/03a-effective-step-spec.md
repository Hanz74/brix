# Canonical Effective Step Specification

## Purpose
This document is the normative specification for step semantics in Brix. It defines the only acceptable semantic target for validator, engine, MCP inspection, drift detection, and debugging.

Brix may continue to ingest historical shapes, legacy aliases, and DB-backed compatibility fields, but those are all inputs into one canonical model. They are not competing truths.

## Normative Statement
Every executable step in Brix must be understood through exactly three layers:

1. `RawStep`
2. `EffectiveStep`
3. `ExecutionPayload`

No major surface may invent an alternative semantic reconstruction once these layers exist.

## Layer 1: `RawStep`

### Definition
`RawStep` is the exact persisted or submitted step shape before canonicalization.

Sources:
- DB step row after JSON decoding
- MCP CRUD payload
- import/bundle payload
- historical compatibility input

### Properties
- preserves historical fields
- may contain legacy type aliases
- may mix top-level fields with nested `config`
- may contain wrapper keys, compatibility keys, or stale overlap
- is suitable for persistence and history
- is not suitable as the canonical execution truth

### Required Behavior
`RawStep` must be preserved for:
- provenance
- debugging
- drift detection
- migration tooling

`RawStep` must not be consumed directly by:
- runner execution
- runner config validation
- policy validation
- MCP “effective behavior” inspection

## Layer 2: `EffectiveStep`

### Definition
`EffectiveStep` is the single canonical internal representation of a step after promotion, alias resolution, config normalization, and provenance capture.

This is the semantic truth for:
- validator
- engine preparation
- MCP inspection
- drift scanning
- debugging surfaces

### Required Fields
- `step_id`
- `raw_type`
- `effective_type`
- `raw_config`
- `effective_config`
- `raw_params`
- `effective_params`
- `promoted_fields`
- `defaulted_fields`
- `policy_flags`
- `dependency_refs`
- `provenance`
- `rendering_warnings`

### Intended Shape
The exact class or storage layout may differ, but semantically the model must expose:

```python
{
    "step_id": "save_results",
    "raw_type": "db.exec",
    "effective_type": "db.exec",
    "raw_config": {...},
    "effective_config": {...},
    "raw_params": [...],
    "effective_params": [...],
    "promoted_fields": {
        "connection": {
            "source": "config.connection",
            "target": "step.connection",
            "reason": "brick-field-promotion",
        }
    },
    "defaulted_fields": {
        "on_error": {
            "value": "stop",
            "source": "step-default",
        }
    },
    "policy_flags": {
        "uses_legacy_alias": False,
        "has_shape_drift": False,
        "contains_workaround_pattern": False,
    },
    "dependency_refs": {
        "pipeline": None,
        "helper": None,
        "connection": "buddy-db",
        "server": None,
        "tool": None,
    },
    "provenance": {
        "persisted_from": "db.pipeline_step",
        "normalizers": [
            "step_row_to_dict",
            "merge_step_config_into_params",
            "materialize_step",
        ],
    },
    "rendering_warnings": [],
}
```

### Responsibilities
`EffectiveStep` must answer:
- what brick will actually run
- which config values actually apply
- which params actually apply
- which top-level values were promoted from config
- which values were defaulted or inferred
- which dependencies the step refers to
- which policy and drift flags already attach to the step

### Non-Responsibilities
`EffectiveStep` is not yet the runner payload.

It must not:
- contain rendered runtime values that depend on a specific execution context
- hide provenance
- flatten away the distinction between persisted and promoted values

## Layer 3: `ExecutionPayload`

### Definition
`ExecutionPayload` is the runner-facing view after runtime rendering and step-local preparation.

This is the final execution-ready structure consumed by:
- `runner.validate_config(...)`
- `runner.execute(...)`

### Properties
- context-rendered
- execution-local
- safe for runner consumption
- contract-checkable against brick schema
- may differ from `EffectiveStep` because templates, defaults, and runtime context have been applied

### Required Behavior
`ExecutionPayload` must preserve a traceable link back to `EffectiveStep`, even if only indirectly through stored provenance or wrapper metadata.

## Promotion and Precedence Invariants

### Invariant 1: Alias Resolution Precedes Semantic Validation
Legacy type aliases must be resolved before validator policy, brick schema matching, or engine preparation make semantic decisions.

Example:
- raw `type = "pipeline"`
- effective `type = "flow.pipeline"`

### Invariant 2: `EffectiveStep` Carries Both Raw and Effective Values
Canonicalization must never destroy the original persisted values.

Reason:
- drift detection
- migration decisions
- observability

### Invariant 3: Config Promotion Must Be Explicit
If a value moves from `config.<field>` to `step.<field>`, the promotion must be represented in provenance.

The system must never silently behave as though the field always lived at top level.

### Invariant 4: Brick Schema Wins Over Historical Shape
If a brick schema and a historical top-level convention disagree, the brick schema defines the target model. Compatibility is transitional.

### Invariant 5: `EffectiveParams` Must Be Shape-Preserving
If persisted params are list-backed, the effective params remain list-backed.

If persisted params are dict-backed, the effective params remain dict-backed.

No surface may downcast list params into a dict-shaped compatibility structure as its canonical truth.

### Invariant 6: Wrapper Keys Are Transport, Not Business Semantics
Keys such as `_config`, `_values`, `_pipeline`, and similar wrappers may exist as transport artifacts during rendering, but they are not the canonical semantic model.

They must not become the effective truth seen by validator, MCP inspection, or policy logic.

### Invariant 7: Engine and Validator Must Materialize the Same Step
The engine must not execute a meaningfully different step than the validator inspected.

If a divergence exists, that is a product bug.

### Invariant 8: Failure Before Execution Is Still a Step Failure
If a step cannot be materialized or rendered into an `ExecutionPayload`, the failed step still exists as a failed semantic step and must be surfaced accordingly.

## Canonical Precedence Rules

### Type Precedence
1. persisted `step.type`
2. legacy alias resolution
3. brick registry lookup on effective type

There is no later stage where a different semantic type should emerge implicitly.

### Config Precedence
1. raw persisted `config`
2. compatibility normalization
3. brick-schema-aware canonicalization into `effective_config`
4. runtime rendering into execution payload

### Top-Level Field Precedence
Top-level runner-facing fields remain historical compatibility inputs until brick schema contracts fully dominate. If a field is supported both top-level and inside `config`, the effective model must choose one canonical source and record the promotion or override.

Current direction:
- canonical semantic home should move toward brick schema fields in `effective_config`
- compatibility promotions remain visible in provenance until retired

Deterministic rule for overlapping runner-facing fields:
1. if the effective brick schema defines the field as part of config, `config.<field>` is the canonical source
2. the top-level field is treated as a compatibility input only
3. if both are present and differ, `config.<field>` wins in `EffectiveStep`
4. the losing top-level value must remain visible in provenance as an overridden compatibility value
5. if only the top-level field exists, the materialization layer may promote it into `effective_config`, but that promotion must be recorded explicitly

Examples of fields governed by this rule include:
- `connection`
- `query`
- `pipeline`
- `helper`
- `script`
- `server`
- `tool`
- other runner-facing fields historically allowed at top level

The migration policy for these fields is defined in
[04b-step-field-migration-policy.md](/root/docker/brix/docs/brix-master-dossier/04b-step-field-migration-policy.md).
The practical rule is strict: new reusable brick semantics must enter through
brick config schemas, not by adding more global `Step` fields.

### Params Precedence
1. raw persisted `params`
2. `config.params` compatibility merge rules
3. shape-preserving canonical `effective_params`
4. runtime rendering into final bind values

For list-backed params:
- `config.params` as list replaces positional params canonically

For dict-backed params:
- `config.params` merges with top-level params according to existing DB-first merge semantics until those semantics are retired behind the materialization layer

## Provenance Model

### Why Provenance Is Mandatory
Without provenance, Brix cannot explain:
- why a field appears where it does
- why validator and engine agree or disagree
- whether a field was persisted, promoted, or defaulted
- whether a shape difference is drift or intended normalization

### Minimum Provenance Fields
- raw source location
- applied alias mapping
- applied promotion rules
- applied default rules
- normalization path
- policy annotations

## Existing Merge and Reconstruction Points
The current platform reconstructs step meaning in several places. These are the existing merge points that must converge onto `materialize_step(...)`.

### DB Readback and Merge
Files:
- [db.py](/root/docker/brix/src/brix/db.py)

Current responsibilities:
- `step_row_to_dict(...)`
- `merge_step_config_into_params(...)`
- promotion of selected config fields back to top-level step fields
- compatibility merging of `config.params` into `step.params`

Future role:
- produce `RawStep`
- hand off semantic normalization to `materialize_step(...)`

### Step Model Construction
Files:
- [models.py](/root/docker/brix/src/brix/models.py)

Current responsibilities:
- `Step.from_db_row(...)`
- partial config/params compatibility behavior

Future role:
- instantiate permissive raw model
- avoid re-owning semantic merge decisions beyond raw compatibility admission

### Loader Rendering
Files:
- [loader.py](/root/docker/brix/src/brix/loader.py)

Current responsibilities:
- `render_step_params(...)`
- wrapper-key transport for `_config`, `_values`, `_pipeline`
- context rendering

Future role:
- consume `EffectiveStep`
- produce `ExecutionPayload`
- stop being an implicit semantic merge authority

### Engine Wrapper Reconstruction
Files:
- [engine_types.py](/root/docker/brix/src/brix/engine_types.py)
- [engine_step.py](/root/docker/brix/src/brix/engine_step.py)

Current responsibilities:
- `_RenderedStep`
- `_step_config_dict(...)`
- fallback reconstruction for validation and execution

Future role:
- consume materialized effective state plus rendered execution payload
- stop reconstructing business semantics from wrapper artifacts

### Validator Normalization
Files:
- [validator.py](/root/docker/brix/src/brix/validator.py)

Current responsibilities:
- `StepAnalysis`
- ad hoc `effective_type`
- config/list normalization
- schema matching and policy checks over partially normalized state

Future role:
- operate on `EffectiveStep`
- stop carrying private normalization logic that can diverge from runtime

### MCP Diagnostic Normalization
Files:
- [mcp_handlers/steps.py](/root/docker/brix/src/brix/mcp_handlers/steps.py)
- [mcp_handlers/_shared.py](/root/docker/brix/src/brix/mcp_handlers/_shared.py)

Current responsibilities:
- `_normalize_step_config(...)`
- `diagnose_step`
- raw/config/promoted field explanation

Future role:
- expose `RawStep`, `EffectiveStep`, and `ExecutionPayload` directly
- stop being another independent semantic reconstruction site

## Canonical Target Mapping

| Existing Surface | Today | Target |
|---|---|---|
| DB readback | compatibility merge authority | raw-state hydration only |
| Step model | partial semantic normalization | permissive raw admission |
| Loader | render plus wrapper-based semantic transport | render-only from `EffectiveStep` |
| Engine wrappers | semantic fallback reconstruction | execution consumption of canonical payload |
| Validator | private normalization + policy | policy over `EffectiveStep` |
| MCP step diagnostics | custom explanatory normalization | inspect canonical raw/effective/rendered trio |

## Acceptance Criteria for This Specification
This task is considered complete only if the following statements are true:

1. Brix has a single written normative specification for step semantics.
2. The specification distinguishes `RawStep`, `EffectiveStep`, and `ExecutionPayload`.
3. Promotion, precedence, provenance, and wrapper-key rules are explicit.
4. Existing merge points are mapped to the target model.
5. The dossier treats this document as the only semantic target for future implementation work.

## Consequence for All Later Work
Every later task in `E1`, `E2`, `E4`, and `E5` must be evaluated against this specification.

If implementation behavior disagrees with this document, either:
- the code must change, or
- this specification must be intentionally revised

Silent coexistence of multiple semantic truths is no longer acceptable.
