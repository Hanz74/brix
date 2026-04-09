# Governance and Metadata Policy

## Policy Goal
Brix should enforce consistency. It should not merely suggest better behavior after the fact.

## Core Governance Rule
No important component may become active without required metadata and reuse review.

## Required Metadata by Component Type

### Pipeline
Required:
- project
- description
- purpose
- owner
- source intent
- status

### Brick
Required:
- description
- owner
- input contract
- output contract
- anti-pattern notes
- examples

### Helper
Required:
- description
- owner
- reason_not_a_brick or brick_candidate reference

### Connection
Required:
- description
- project
- owner
- usage scope

### Help Topic
Required:
- owner
- version relevance
- linked brick or topic domain

## Enforcement Levels

### Error / Block
Used when a component should not be created or activated.

Examples:
- missing project
- missing owner
- missing description
- missing input/output contract for a stable brick
- new helper without brick justification

### Warning
Used when the component may exist temporarily but must be repaired.

Examples:
- partial examples missing
- incomplete similarity links
- missing replacement note on deprecated component

### Info
Used for guidance and enrichment opportunities.

## Reuse Check Policy
Before creating a new component, Brix should require:
- similar intents search
- similar pipelines search
- related brick search
- repeated pattern search

The result should be explicitly recorded:
- reused existing component
- modified existing component
- new component justified

## Workaround Policy

### Mandatory Detection
Known workaround patterns must be detectable by validator and drift tooling.

### Mandatory Recording
Any approved temporary workaround must be linked to:
- reason
- expiration condition
- replacement plan
- owning task

### Mandatory Escalation
Repeated workaround patterns become architecture debt, not local pipeline detail.

## LLM Guidance Policy

### Required LLM Behavior
When mandatory metadata is missing, the system should instruct the LLM to ask for it or help infer it.

### Required System Behavior
MCP tools should surface missing metadata as actionable deficits.

### Joint Resolution Model
User and LLM should be able to agree on missing information through dedicated CRUD flows.

## Gatekeeper Scope
A true gatekeeper review should include:
- metadata completeness
- reuse check completeness
- workaround pattern detection
- policy compliance
- drift risk
- contract completeness

## Lifecycle Governance
No component should silently remain active when superseded.

Required lifecycle operations:
- deprecate
- replace
- archive
- annotate reason
- link replacement

## Policy Questions Brix Must Be Able to Answer
- who owns this component?
- why does it exist?
- was reuse checked before it was created?
- what similar things exist?
- is it carrying a workaround?
- what replaced it?
