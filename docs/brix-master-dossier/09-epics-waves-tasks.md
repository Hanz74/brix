# Epics, Waves, and Tasks

## Execution Model
This plan assumes:
- `Epic = major`
- `Wave = minor`
- `Task = patch`

Each task should be:
- fully prepared
- test-backed
- gatekeeper-reviewed
- committed immediately when green
- versioned immediately
- recorded in the DB changelog immediately

This document is intentionally not compressed. It is the explicit implementation program derived from the full architecture dossier.

## Epic E1: Canonical Semantics and DB Truth

### Goal
Make Brix semantics singular, explicit, and DB-anchored.

### Wave W1.1: Effective Step Shape

#### Task T-1.1.1: Define canonical effective step representation
Goal:
- Specify the single materialized step representation used across validator, engine, MCP, and debugging.

Deliverables:
- architecture spec for raw step, effective step, execution payload
- invariants for promotion, config precedence, and provenance
- mapping of existing merge points to the new model

Done when:
- the model is documented and accepted as the only semantic target

#### Task T-1.1.2: Implement central materialization service
Goal:
- Introduce a single materialization path replacing duplicated shape reconstruction.

Deliverables:
- central `materialize_step(...)`
- explicit provenance output
- tests for config, params, legacy aliases, wrapper keys, conditional refs

Done when:
- validator and engine can consume the same effective structure

#### Task T-1.1.3: Expose materialized step through MCP
Goal:
- Make effective step semantics directly inspectable.

Deliverables:
- `materialize_step`
- `inspect_effective_pipeline`
- raw vs effective comparison output

Done when:
- a user or LLM can inspect exactly how a step will execute

### Wave W1.2: DB Truth Hardening

#### Task T-1.2.1: Reclassify file mirrors as export products
Goal:
- Remove architectural ambiguity around file-backed truth.

Deliverables:
- explicit policy and implementation boundaries
- docs and tooling changes that frame files as export/mirror only

Done when:
- DB is the only authoritative authoring truth

#### Task T-1.2.2: Align pipeline store and loader with DB-first semantics
Goal:
- Eliminate hybrid assumptions in store/load paths.

Deliverables:
- clarified persistence flow
- reduced file-first assumptions
- regression tests for DB-authored components

Done when:
- authoring, readback, and execution all assume DB-first behavior

#### Task T-1.2.3: Extend drift scanner to semantic parity checks
Goal:
- detect persisted state that no longer matches brick schemas, help, or runtime shape.

Deliverables:
- parity checks for brick schema vs config
- parity checks for help vs current brick types
- parity checks for raw step vs effective step

Done when:
- semantic drift is visible before production use

## Epic E2: Brick Ecosystem and Reuse Architecture

### Goal
Move reusable logic out of helpers and pipelines into a coherent brick ecosystem.

### Wave W2.1: Brick Contracts

#### Task T-2.1.1: Define standard inter-brick contracts
Goal:
- Introduce standard domain payload contracts for common operations.

Deliverables:
- contract catalog for file, extract, db mutation, classification, download
- validator integration points

Done when:
- common compositions no longer rely on ad hoc shapes

#### Task T-2.1.2: Reduce global runner-specific Step fields
Goal:
- move semantic emphasis from Step fields to brick schema contracts.

Deliverables:
- migration design for runner-specific top-level fields
- compatibility rules
- deprecation roadmap

Done when:
- brick schemas dominate semantics instead of global Step fields

### Wave W2.2: Helper-to-Brick Migration

#### Task T-2.2.1: Build helper inventory and clustering model
Goal:
- classify helpers by family, domain, and migration candidacy.

Deliverables:
- inventory
- family clusters
- brick-candidate classifications

Done when:
- every helper has a strategic category

#### Task T-2.2.2: Implement brick-candidate detector
Goal:
- automatically detect repeated logic that should become bricks.

Deliverables:
- repeated helper usage detection
- repeated SQL pattern detection
- repeated step sequence detection

Done when:
- reuse candidates are generated systematically

#### Task T-2.2.3: Establish helper governance
Goal:
- ensure new helpers require explicit justification.

Deliverables:
- metadata requirement for helpers
- validator and gatekeeper rules

Done when:
- no new helper can be introduced silently as normal product design

### Wave W2.3: Strategic Brick Families

#### Task T-2.3.1: Introduce document persistence brick family
Goal:
- standardize saving extraction results and specialist state.

Deliverables:
- `document.persist_extraction_result`
- `document.mark_specialist_processed`

Done when:
- pipelines no longer rely on repeated inline persistence logic

#### Task T-2.3.2: Introduce source/download standard bricks
Goal:
- standardize remote fetch-and-store patterns.

Deliverables:
- bricks for remote download and local persistence contracts

Done when:
- repeated download-save logic is reusable

#### Task T-2.3.3: Introduce extraction preparation family
Goal:
- standardize extractable payload preparation and daigestr execution contracts.

Deliverables:
- `document.prepare_extractable_payload`
- `extract.document_with_daigestr`

Done when:
- pipelines no longer encode repeated extract-preparation logic ad hoc

## Epic E3: Knowledge Layer, Intent, and Similarity

### Goal
Make Brix capable of understanding relationships, origins, prior cases, and intent.

### Wave W3.1: MetaDB Foundation

#### Task T-3.1.1: Implement knowledge entities and metadata schema
Goal:
- introduce persistent entities for intent, task, decision, workaround, and reuse.

Deliverables:
- schema additions
- storage model
- lifecycle rules

Done when:
- required knowledge entities are first-class

#### Task T-3.1.2: Link product entities to knowledge entities
Goal:
- connect pipelines, bricks, helpers, runs, findings, and changelog to the knowledge layer.

Deliverables:
- link tables or equivalent structures
- referential validation

Done when:
- components and knowledge are queryable together

### Wave W3.2: Graph and Similarity

#### Task T-3.2.1: Build graph projection
Goal:
- expose structural relationships explicitly.

Deliverables:
- graph model or equivalent projection layer
- component relationship queries

Done when:
- dependency and context traversal is possible

#### Task T-3.2.2: Build semantic retrieval over intents, incidents, and docs
Goal:
- support similarity-driven reuse and incident lookup.

Deliverables:
- embedding/index strategy
- retrieval API
- similarity scoring model

Done when:
- the system can answer “have we seen something like this before?”

#### Task T-3.2.3: Integrate similar-case retrieval into diagnostics and authoring
Goal:
- make similarity operational, not passive.

Deliverables:
- integration with create flows
- integration with diagnose flows

Done when:
- reuse and prior-case context are surfaced proactively

## Epic E4: Governance, Required Metadata, and Enforcement

### Goal
Make metadata and reuse requirements mandatory product behavior.

### Wave W4.1: Required Metadata

#### Task T-4.1.1: Define required metadata per entity type
Goal:
- formalize mandatory fields and statuses.

Deliverables:
- metadata matrix
- enforcement severity mapping

Done when:
- required metadata is explicit and enforceable

#### Task T-4.1.2: Enforce metadata in create and update flows
Goal:
- make incomplete creation impossible or visibly draft-only.

Deliverables:
- blocking rules
- draft-state handling
- repair prompts

Done when:
- active components cannot exist without required metadata

### Wave W4.2: Reuse and Anti-Workaround Enforcement

#### Task T-4.2.1: Require reuse check before new component creation
Goal:
- force an explicit answer to “what existing thing did we compare against?”

Deliverables:
- reuse-search integration
- recorded decision outcome

Done when:
- no new pipeline or brick is created without reuse evidence

#### Task T-4.2.2: Implement workaround pattern registry and detection
Goal:
- make workaround patterns first-class and enforceable.

Deliverables:
- pattern catalog
- validator integration
- gatekeeper integration

Done when:
- repeated workaround forms are system-visible

#### Task T-4.2.3: Require workaround annotation or replacement plan
Goal:
- prevent hidden workaround normalization.

Deliverables:
- workaround metadata
- expiry/replacement tracking

Done when:
- any temporary workaround is explicit and owned

## Epic E5: MCP Product Surface and Agent Guidance

### Goal
Turn the full architecture into an operable surface for users and LLMs.

### Wave W5.1: MCP CRUD Expansion

#### Task T-5.1.1: Add intent and decision CRUD
Goal:
- make knowledge capture operational.

#### Task T-5.1.2: Add component-context and relationship inspection tools
Goal:
- let users and LLMs inspect context directly.

#### Task T-5.1.3: Add metadata repair and reuse-check tools
Goal:
- let the system guide missing information completion.

### Wave W5.2: Guidance Surfaces

#### Task T-5.2.1: Evolve `get_tips` into an enforcement-guidance surface
Goal:
- move from generic advice to actionable metadata and reuse guidance.

#### Task T-5.2.2: Evolve `validate_pipeline` into a policy-aware authoring gate
Goal:
- include metadata, workaround, similarity, and contract checks.

#### Task T-5.2.3: Evolve `diagnose_run` into prior-case-aware repair guidance
Goal:
- combine runtime evidence, history, and similarity.

## Epic E6: HMK as Anchor Refactor

### Goal
Use HMK as a concrete proving ground for the architectural model.

### Wave W6.1: Workaround Elimination

#### Task T-6.1.1: Remove interpolated SQL persistence from HMK
Goal:
- eliminate persistence workarounds in `buddy-hmk-extract-single`.

#### Task T-6.1.2: Replace HMK-specific specialist marking with reusable persistence behavior
Goal:
- stop embedding repeated state mutation patterns in HMK.

### Wave W6.2: Reusable Brick Introduction

#### Task T-6.2.1: Introduce document persistence brick in HMK
Goal:
- use HMK as first adoption site for persistence bricks.

#### Task T-6.2.2: Introduce extraction preparation or daigestr execution standardization
Goal:
- remove repeated pre-extract logic from HMK.

### Wave W6.3: Architectural Validation

#### Task T-6.3.1: Prove HMK can be reduced to orchestration and configuration
Goal:
- demonstrate the intended Brix design principle in a live case.

#### Task T-6.3.2: Add HMK-specific regression and prior-case metadata
Goal:
- ensure the migration itself becomes retrievable knowledge.
