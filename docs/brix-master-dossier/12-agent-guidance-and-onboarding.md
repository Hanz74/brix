# Agent Guidance and Onboarding

## Purpose
This document describes how Brix should guide both human users and LLMs toward correct platform usage.

## Current Problem
Even when the correct path exists, it is not always the shortest or most obvious local path. That causes:
- helper invention
- pipeline-local special logic
- workarounds
- incomplete metadata
- duplicated implementations

## Target State
Brix should actively steer authors toward:
- existing reusable components
- complete metadata
- safe runtime contracts
- prior solved cases
- explicit policy compliance

## Required Authoring Flow

### Step 1: Intent Capture
Before creating a new component, capture:
- raw request
- normalized summary
- project
- owner
- expected outcome
- key constraints

### Step 2: Reuse Search
The system should check:
- similar intents
- existing pipelines
- existing bricks
- known helper families
- prior incidents and fixes

### Step 3: Creation or Reuse Decision
The user and LLM should then explicitly decide:
- reuse existing component
- modify existing component
- create new component with justification

### Step 4: Metadata Completion
If required metadata is missing, the system should ask for it before activation.

### Step 5: Policy Check
The system should enforce:
- metadata completeness
- reuse check completion
- no disallowed workaround patterns

## `get_tips` as Guidance Surface
`get_tips` should eventually behave as a dynamic authoring coach.

Examples:
- “This project has similar pipelines already.”
- “This helper family likely belongs in a brick.”
- “This new component is missing owner and source intent.”
- “This pattern matches known workaround W-004.”

## Onboarding Queries Brix Should Support
- what are the main components in project X?
- why does pipeline Y exist?
- what replaced helper Z?
- what similar cases exist for this request?
- which components are known workaround carriers?
- where does this brick appear across projects?

## Required MCP Experiences
- `get_component_context`
- `get_related_components`
- `search_similar_intents`
- `search_similar_failures`
- `get_missing_metadata`
- `repair_component_metadata`
- `record_reuse_decision`

## LLM Guardrails

### Must Do
- search for reuse before inventing
- surface missing metadata
- prefer bricks over helpers
- prefer DB truth over file mirrors
- treat workaround patterns as debt
- treat files as export, backup, bundle, debug, or legacy-import artifacts only

### Must Not Do
- invent pipeline-local business logic when a reusable component is appropriate
- introduce new helpers without justification
- leave missing ownership or purpose metadata unresolved
- trust legacy help text over current registry/runtime truth
- repair live pipeline state by editing YAML mirrors directly

## Human and LLM Collaboration
The system should support a guided conversation:
- Brix identifies missing information
- the LLM explains why it matters
- the user provides or approves the missing data
- Brix stores it via CRUD

This makes metadata completion part of normal work, not an afterthought.
