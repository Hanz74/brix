# Migration and Remediation Strategy

## Goal
Move Brix from a hybrid improving system into a coherent DB-first and brick-first platform without normalizing historical debt.

## Strategic Migration Tracks

### Track A: Canonical Semantics
Create one materialized effective step model and move all major surfaces onto it.

### Track B: DB Truth Hardening
Reduce file mirrors to export and bundle roles. Move all authoring and repair assumptions toward DB-first behavior.

Normative boundary:
- [03b-file-mirror-policy.md](/root/docker/brix/docs/brix-master-dossier/03b-file-mirror-policy.md)

### Track C: Helper-to-Brick Migration
Identify helper families and pipeline-level special logic that should become reusable bricks.

### Track D: Knowledge Layer Introduction
Introduce MetaDB, graph relationships, semantic retrieval, and intent capture.

### Track E: Governance Enforcement
Introduce mandatory metadata, reuse checks, and anti-workaround enforcement.

## Migration Rules

### Rule 1
Do not silently reinterpret legacy state. Annotate and migrate it explicitly.

### Rule 2
Do not migrate by copying debt into the new model. Normalize meaning, not just storage shape.

### Rule 3
Do not leave helpers or patterns “temporarily” unclassified.

### Rule 4
Do not add new MCP features without aligning them with DB truth and governance rules.

### Rule 5
Do not let a file mirror suppress a DB integrity issue. A mirror may be migration input, but live repair must update DB-owned state.

## Existing Debt Classes
- helper overuse
- raw `mcp.call` overuse
- inline SQL workaround paths
- duplicated materialization semantics
- partial metadata
- hybrid help and mirror behaviors

## Migration Order

### Phase 1
Semantics and truth:
- canonical step materialization
- drift scanner
- runtime and validator parity

### Phase 2
Brick ecosystem hardening:
- helper inventory
- brick candidate generation
- contract standardization

### Phase 3
Knowledge system:
- MetaDB schema
- graph projection
- similarity search
- intent capture

### Phase 4
Governance hardening:
- required metadata enforcement
- reuse gate
- anti-workaround gates

### Phase 5
Authoring and agent guidance:
- MCP CRUD
- guided creation flows
- onboarding support

## HMK as Migration Anchor
HMK should be treated as a proving ground:
- remove inline SQL workarounds
- standardize persistence
- standardize document extract preparation
- keep orchestration at pipeline level
- move logic to reusable bricks or stable compositional primitives

## Success Criteria
- no important drift classes remain silent
- new components cannot be created without required metadata
- repeated pipeline logic is visible as reusable candidates
- prior cases are retrievable
- LLMs are guided toward reuse before invention
