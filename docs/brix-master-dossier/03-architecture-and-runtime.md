# Architecture and Runtime

## Problem Today
Brix still reconstructs step meaning across multiple layers:

- DB readback and merge helpers
- loader render logic
- validator normalization
- step tool normalization
- engine execution wrappers

This creates semantic duplication and therefore drift.

## Target State
Brix should have a single canonical internal representation of a step:

- raw persisted step shape
- materialized effective step shape
- runtime execution payload

Every major system layer should consume the same materialized step view.

The normative specification for this model lives in:
- [03a-effective-step-spec.md](/root/docker/brix/docs/brix-master-dossier/03a-effective-step-spec.md)

## Canonical Internal Shapes

### Raw Step Shape
The exact persisted DB record or MCP payload.

Properties:
- historical
- may contain legacy fields
- may contain config and top-level overlap
- not execution-ready

### Materialized Effective Step Shape
The single canonical internal representation used by validator, MCP inspection, and engine preparation.

Properties:
- effective brick type
- effective config
- effective params
- promoted and inherited values
- provenance metadata
- policy flags

### Execution Payload
The exact runner-facing structure after context rendering.

Properties:
- fully rendered
- step-local
- execution-safe
- contract-checked against brick schema

## Architectural Roles

### DB Layer
Responsibilities:
- persistence
- versioning
- retrieval
- historical storage
- canonical data ownership

Must not:
- re-invent higher-level runtime semantics repeatedly

### Brick Registry
Responsibilities:
- capability catalog
- schemas
- contracts
- lifecycle
- metadata

Must not:
- depend on ad hoc step-field conventions as the primary source of truth

### Materialization Layer
Responsibilities:
- transform raw DB step shape into effective canonical step shape
- expose promotion decisions
- unify config and runner-facing fields

This should become the semantic heart of Brix.

### Engine
Responsibilities:
- scheduling
- execution
- retries
- error surfacing
- context propagation

Must not:
- own hidden config merge semantics

### Validator and Advice Layer
Responsibilities:
- analyze effective step shape
- detect drift, policy violations, anti-patterns, and likely runtime mismatches
- guide repair

Must not:
- infer semantics differently than runtime

### MCP Layer
Responsibilities:
- expose product truth
- provide CRUD and diagnostics
- provide guided repair flows

Must not:
- drift from runtime and DB semantics

## Required Central Capability: Step Materialization

### `materialize_step(step, context=None, mode=...)`
The platform should introduce one explicit materialization service or function that returns:

- raw source shape
- effective type
- effective config
- effective params
- promoted fields
- defaulted fields
- policy annotations
- dependency references
- rendering warnings

Every important surface should use this:
- validator
- step preview tools
- engine preflight
- diagnose/repair tools

This materialization capability must produce the `EffectiveStep` defined in:
- [03a-effective-step-spec.md](/root/docker/brix/docs/brix-master-dossier/03a-effective-step-spec.md)

## Runtime Error Model
The runtime must always identify:
- last successful step
- first failed or non-materializable step
- failure phase
- root exception
- relevant config/rendering context

Minimum failure phases:
- `load`
- `materialize`
- `render`
- `pre_execute`
- `runner`
- `persist`
- `finalize`

## Drift Classes

### Shape Drift
Raw DB state and runtime-effective state diverge.

### Schema Drift
Brick schema and persisted config diverge.

### Help Drift
Help topics describe outdated types or wrong usage.

### Policy Drift
Validators, tips, and runtime disagree on allowed practices.

### Reuse Drift
Equivalent logic exists in helpers or pipelines instead of reusable bricks.

## Architectural Directives

### Directive 1
Minimize global runner-specific Step fields over time.

### Directive 2
Move semantic authority toward brick schemas and materialized step state.

### Directive 3
Treat file mirrors as export products, not authoring truth.

### Directive 4
Make all non-trivial merge logic observable through MCP inspection.

### Directive 5
Treat silent or hidden failure boundaries as product bugs.
