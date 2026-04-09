# Knowledge Graph, MetaDB, and Intent Layer

## Purpose
Brix should not only know components. It should know relationships, origins, decisions, reuse patterns, and prior solutions. This requires a dedicated knowledge layer built on structured metadata plus graph relationships plus semantic retrieval.

## Why Graph and RAG Both Matter

### Graph
Best for:
- explicit dependency and relationship traversal
- impact analysis
- reuse analysis
- drift analysis
- onboarding maps

### RAG
Best for:
- semantic similarity
- prior incident retrieval
- decision retrieval
- help, docs, and changelog retrieval
- user-intent matching

### Combined Model
The graph is the structural backbone.
RAG sits over graph nodes and linked documents.

## MetaDB Purpose
The MetaDB should store first-class knowledge that today is fragmented or missing:
- intents
- tasks
- decisions
- ownership
- lifecycle
- relationships
- prior incidents
- known workaround patterns

## Intent as a First-Class Entity

### Required Principle
Original user intent should be stored with minimal compression.

At minimum:
- raw request text
- normalized summary
- domain classification
- linked project
- linked task(s)
- linked resulting components

### Why This Matters
It enables questions such as:
- have we solved something like this before?
- which pipeline or brick was built for this kind of request?
- which past fixes produced stable outcomes vs workarounds?

## Similarity and Prior Case Retrieval
The system should support queries like:
- “I have this problem. Was something similar solved before?”
- “Which components resemble this requested capability?”
- “Which past incidents look like this failure?”

### Signal Inputs
- raw intent embeddings
- normalized intent
- component metadata
- run failures and diagnoses
- workaround patterns
- changelog entries
- help and docs

## Required Entity Groups

### Structural Entities
- pipeline
- step
- brick
- helper
- connection
- variable

### Operational Entities
- run
- finding
- drift finding
- repair action

### Knowledge Entities
- intent
- task
- decision
- lesson
- workaround pattern
- reuse candidate
- help topic
- changelog entry

## Required Relationship Types
- `resembles`
- `created_for`
- `documents`
- `uses`
- `depends_on`
- `replaces`
- `failed_at`
- `fixed_by`
- `matches_pattern`
- `candidate_for_reuse`

## Mandatory Metadata Policy
Every significant entity should be blocked or downgraded if key metadata is missing.

Examples:
- a brick without purpose, owner, and contracts
- a pipeline without intent, purpose, and project
- a helper without brick-candidate status
- a decision without rationale

## Knowledge-Layer Use Cases

### Onboarding
“Show me everything relevant to HMK.”

### Change Planning
“Which pipelines use this helper or pattern?”

### Reuse Guidance
“Should this request become a new brick or reuse an existing one?”

### Incident Analysis
“Have we seen this failure pattern before?”

### Product Strategy
“Which user intents repeatedly produce similar workaround-heavy implementations?”

## Governance Integration
The knowledge layer must not be optional. It should feed:
- validator
- tips
- creation workflows
- repair workflows
- gatekeeper checks

## Anti-Entropy Mechanisms
- periodic missing-metadata scan
- periodic orphan relationship scan
- periodic duplicate intent scan
- periodic reuse candidate generation

## Suggested Storage Strategy

### Relational Base
Store core entities and normalized metadata in DB tables.

### Graph Projection
Project entities and relationships into a queryable graph form.

### Semantic Index
Embed intent, decision, help, changelog, findings, and lessons for similarity search.

This allows:
- structured joins
- graph traversals
- semantic retrieval

all over the same conceptual domain.
