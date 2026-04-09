# Coverage Matrix

This matrix exists to prevent architectural blind spots. Every major topic must be addressed across every relevant system dimension. No theme is considered complete until it is either covered or explicitly marked as not applicable.

## Topic Matrix

| Topic | DB | Runtime | Validator | MCP | Governance | Migration | Agent Guidance | Notes |
|---|---|---|---|---|---|---|---|---|
| Canonical step shape | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Central systemic dependency |
| Brick schemas and contracts | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Must dominate over legacy step-field semantics |
| DB-first truth model | Yes | Yes | Yes | Yes | Yes | Yes | Yes | File mirror only as export |
| Brick-first reuse model | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Must reduce helper/pipeline invention |
| Drift detection | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Includes help, changelog, registry, steps |
| Anti-workaround system | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Hard detection and guided repair |
| MetaDB and required metadata | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Foundational for intent and reuse |
| Graph relationships | Yes | Partial | Partial | Yes | Yes | Yes | Yes | Graph is a knowledge layer over product entities |
| RAG / semantic retrieval | Partial | No | Partial | Yes | Yes | Partial | Yes | Built over graph and document corpora |
| Similarity / prior case retrieval | Yes | Partial | Yes | Yes | Yes | Partial | Yes | Must use runs, findings, components, intent |
| Component lifecycle | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Draft, active, deprecated, replaced, archived |
| Brick ecosystem composition | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Comparable to n8n node ecosystem |
| MCP CRUD surface | Yes | Partial | Partial | Yes | Yes | Yes | Yes | Product surface for all metadata and repair |
| Help / tips / changelog parity | Yes | Partial | Yes | Yes | Yes | Yes | Yes | One truth, many surfaces |
| HMK and other live migration examples | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Must be validated against real cases |

## Coverage Requirements

### Architecture
Every topic must identify:
- today’s problem
- target state
- invariant boundaries
- the canonical place where semantics live

### Runtime
Every topic that affects execution must specify:
- pre-execution semantics
- render/materialization semantics
- execution semantics
- error surfacing semantics

### Validator
Every topic that affects component correctness must specify:
- what is statically checkable
- what is policy-relevant
- what becomes blocking vs warning vs informational

### MCP
Every topic that affects system operation must specify:
- what should be readable
- what should be creatable/updatable/deletable
- what should be repairable or diagnosable

### Governance
Every topic that affects consistency must specify:
- required metadata
- hard blocks
- required review or policy gates

### Migration
Every topic must specify:
- how existing state transitions into the target model
- whether legacy entities remain supported, mirrored, transformed, or blocked

### Agent Guidance
Every topic must specify:
- what an LLM should do first
- what an LLM must not do
- how missing information is requested or repaired

## Areas Requiring Special Discipline

### No Compression of Intent
Intent capture must store the original user phrasing or equivalent raw intent artifact, not only a normalized summary.

### No Silent Downcasting of Metadata
If a component lacks required metadata, that is a visible state, not a hidden empty default.

### No Helper Expansion Without Brick Review
Any new helper or repeated helper pattern must trigger a brick-candidate review.

### No New Pipeline Without Reuse Check
Pipeline creation must include an explicit search for similar bricks, pipelines, intents, or prior cases.
