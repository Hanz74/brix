# MCP Surface and CRUD Specification

## Purpose
If knowledge, intent, reuse, and governance are to be real product features, they need first-class MCP support. Otherwise they remain advisory documents instead of operable system behavior.

## MCP Design Principles

### Principle 1
Everything important should be inspectable.

### Principle 2
Missing metadata should be repairable through MCP, not only by direct DB surgery.

### Principle 3
Guidance tools should point to concrete next actions.

### Principle 4
Creation flows should enforce metadata and reuse checks.

## Required MCP Domains

### Intent CRUD
Examples:
- `create_intent`
- `get_intent`
- `search_intents`
- `update_intent`
- `link_intent`

### Decision CRUD
Examples:
- `create_decision`
- `get_decision`
- `search_decisions`
- `link_decision`

### Knowledge Graph Inspection
Examples:
- `get_component_context`
- `get_related_components`
- `get_dependency_graph`
- `get_component_history`

### Similarity / Prior Case Retrieval
Examples:
- `search_similar_cases`
- `search_similar_intents`
- `search_similar_failures`
- `find_reuse_candidates`

### Governance and Metadata Repair
Examples:
- `get_missing_metadata`
- `repair_component_metadata`
- `validate_component_metadata`
- `record_workaround`
- `record_reuse_decision`

### Drift and Anti-Workaround
Examples:
- `scan_drift`
- `scan_workaround_patterns`
- `diagnose_component_drift`
- `suggest_brick_candidate`

### Runtime Shape Inspection
Examples:
- `materialize_step`
- `inspect_effective_pipeline`
- `compare_raw_vs_effective_step`

## Creation Workflow Expectations

### New Pipeline Creation
Should require:
- project
- intent
- purpose
- owner
- reuse search result

### New Brick Creation
Should require:
- purpose
- input contract
- output contract
- domain
- owner
- anti-pattern guidance

### New Helper Creation
Should require:
- explicit “why not a brick?”
- candidate migration status

## `get_tips` Evolution
`get_tips` should become a guided enforcement surface, not just general advice.

Examples:
- “Pipeline missing project and source intent.”
- “This pattern resembles existing brick X.”
- “This helper family is a brick candidate.”
- “This component is blocked until metadata Y is supplied.”

## `validate_pipeline` Evolution
In addition to syntax and schema:
- detect missing metadata
- detect workaround patterns
- detect duplicate or similar existing solutions
- identify missing contract alignment

## `diagnose_run` Evolution
Should combine:
- runtime failure data
- historical similarity
- component context
- workaround pattern knowledge

## MCP Response Model
Every important analysis response should support:
- findings
- why
- suggested next action
- linked components
- linked prior cases
- policy severity

This keeps the surface usable by both humans and LLMs.
