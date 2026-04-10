# Brix Master Architecture Dossier

## Purpose
This dossier defines the target operating model for Brix as a strict `db-first`, `brick-first`, `reuse-first`, and `intent-aware` system. It is not a quick-win plan, not an MVP framing, and not a workaround catalog. It is the comprehensive target architecture, governance model, and execution program for evolving Brix into a coherent product platform.

## Why This Dossier Exists
Brix has moved materially toward a DB-centric and brick-centric system, but the real platform still shows hybrid traces:

- multiple step shapes are reconstructed across layers
- runtime truth, validator truth, MCP truth, and persisted truth can drift
- helpers and subpipelines still absorb logic that should live in reusable bricks
- component metadata is incomplete and therefore weak for onboarding, reuse, and agent guidance
- user intent and historical solution context are not captured as first-class assets

These gaps produce repeated failure modes:

- the same patterns are re-implemented instead of reused
- LLMs fall back to workarounds because the correct path is not locally obvious enough
- debugging requires reconstructing relationships that should already be explicit
- product knowledge is distributed across DB rows, Python, MCP behavior, docs, and chat history

## Non-Negotiable Principles

### 1. DB-First
The DB is the source of truth for operational entities. File mirrors exist only as export, backup, bundle, or debugging artifacts. No architectural decision should assume file state is primary.

### 2. Brick-First
Pipelines are orchestration and configuration layers. Reusable logic belongs in bricks. Helpers and ad hoc template logic are transitional or exceptional, not the strategic center.

### 3. Reuse-First
Before a new pipeline pattern, helper, or inline SQL fragment is introduced, Brix should be able to answer whether an equivalent or near-equivalent solution already exists.

### 4. Intent-Aware
The system must capture why a component exists, not only what it does. User intent, implementation decisions, related components, and observed outcomes are first-class knowledge.

### 5. No Silent Drift
If runtime shape, schema shape, help text, brick schema, persisted state, or validator semantics diverge, the system must surface that drift explicitly.

### 6. No Workaround Normalization
Workarounds may appear temporarily during debugging, but the platform must treat them as product defects or migration debt, not as acceptable steady-state design.

### 7. Mandatory Metadata
No important component may exist without sufficient metadata for reuse, governance, and onboarding.

## Canonical Problem Statement
The current platform has grown around several valid advances:

- DB-backed persistence
- stronger brick registry
- structured validator findings
- MCP as the primary product surface

But it still carries systemic debt:

- too many step semantics live in more than one place
- too much business logic remains in pipelines, helpers, and template expressions
- too many relationships are implicit rather than explicit
- too much historical context is irretrievable without manual archaeology

## Target Outcomes

### Product Outcomes
- Brix becomes the authoritative system for structured workflow composition and governance.
- Bricks become the primary reuse vehicle, comparable to node ecosystems such as n8n.
- Pipelines become shorter, more declarative, and easier to validate.

### Developer Outcomes
- Engineers can answer “what exists, why it exists, what it depends on, and what it affects” without manual archaeology.
- Drift becomes detectable before it reaches production work.
- Repeated helper or SQL logic becomes visible as a brick candidate instead of remaining hidden.

### Agent Outcomes
- LLMs are guided toward correct reuse and correct metadata completion.
- Missing component context becomes actionable through MCP CRUD, not a dead end.
- Similar incidents, patterns, and solutions become retrievable through graph and semantic search.

## Dossier Structure
- [01-coverage-matrix.md](/root/docker/brix/docs/brix-master-dossier/01-coverage-matrix.md)
- [02-domain-model.md](/root/docker/brix/docs/brix-master-dossier/02-domain-model.md)
- [03-architecture-and-runtime.md](/root/docker/brix/docs/brix-master-dossier/03-architecture-and-runtime.md)
- [03a-effective-step-spec.md](/root/docker/brix/docs/brix-master-dossier/03a-effective-step-spec.md)
- [03b-file-mirror-policy.md](/root/docker/brix/docs/brix-master-dossier/03b-file-mirror-policy.md)
- [04-brick-ecosystem.md](/root/docker/brix/docs/brix-master-dossier/04-brick-ecosystem.md)
- [04a-standard-brick-contracts.md](/root/docker/brix/docs/brix-master-dossier/04a-standard-brick-contracts.md)
- [04b-step-field-migration-policy.md](/root/docker/brix/docs/brix-master-dossier/04b-step-field-migration-policy.md)
- [05-knowledge-graph-intent-layer.md](/root/docker/brix/docs/brix-master-dossier/05-knowledge-graph-intent-layer.md)
- [06-governance-and-metadata-policy.md](/root/docker/brix/docs/brix-master-dossier/06-governance-and-metadata-policy.md)
- [07-mcp-surface-and-crud-spec.md](/root/docker/brix/docs/brix-master-dossier/07-mcp-surface-and-crud-spec.md)
- [08-migration-and-remediation-strategy.md](/root/docker/brix/docs/brix-master-dossier/08-migration-and-remediation-strategy.md)
- [09-epics-waves-tasks.md](/root/docker/brix/docs/brix-master-dossier/09-epics-waves-tasks.md)
- [10-hmk-brick-first-analysis.md](/root/docker/brix/docs/brix-master-dossier/10-hmk-brick-first-analysis.md)
- [11-component-landscape-and-optimization.md](/root/docker/brix/docs/brix-master-dossier/11-component-landscape-and-optimization.md)
- [12-agent-guidance-and-onboarding.md](/root/docker/brix/docs/brix-master-dossier/12-agent-guidance-and-onboarding.md)

## Decision Rules
When conflicts arise, these rules apply in order:

1. Prefer product consistency over local convenience.
2. Prefer bricks over helpers when the pattern is reusable.
3. Prefer DB truth over file mirrors.
4. Prefer explicit metadata over implicit tribal knowledge.
5. Prefer surfaced failure over silent fallback.
6. Prefer guided reuse over new component creation.

## Explicit Non-Goals
- preserving hybrid semantics indefinitely
- adding more helper-based flexibility as a substitute for missing bricks
- tolerating incomplete metadata for “speed”
- treating graph, RAG, and intent capture as optional nice-to-haves
- prioritizing local workarounds over product repairs
