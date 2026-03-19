# Strategy & Filter Review — Alex (Requirements Engineer)

**Date:** 2026-03-19
**Scope:** Pipeline strategies + filter brick type evaluation

## Concept 1: Pipeline Strategies (Loop/Pagination)

### Recommendation: No Brix-Core feature. Claude decides strategy.

D-01 and D-02 are decisive: Brix executes, Claude plans. Strategy selection
("broad fetch + local filter" vs "targeted OData filter") is a semantic
decision that depends on data volume, filter selectivity, and user intent.
That's Claude's intelligence, not Brix's job.

**How it works:**
- Claude chooses strategy based on user request
- Claude orchestrates multiple `brix run` calls if needed (pagination)
- Brix executes each call atomically with clean JSON results
- Skill-prompt documents two pipeline variants, Claude picks one

**Rejected:**
- `until` syntax in pipeline YAML — uncontrollable state, impossible to validate
- Accumulating state across runs — violates stateless RunResult model
- Internal loop engine — Claude can't debug hidden loops

### Implementation
Skill-prompt gets two pipeline variants:
- `download-attachments-targeted.yaml` — OData filter (precise, fewer results)
- `download-attachments-broad.yaml` — broad fetch, local filter (faster for large volumes)

Claude selects based on user intent. Transparent, testable, debuggable.

## Concept 2: Filter/Transform Runner Type

### Recommendation: No for v1, revisit in v2.

D-04 precedent: `transform` was rejected with "python covers it". `filter` is
a subset of `transform` — same logic applies.

The concrete pain (flatten_attachments.py) is not pure filtering — it's
flatten + filter + enrich. A `filter` type would cover only one of three
operations. The flatten logic remains imperative regardless.

**When to revisit:** If Brix gets visual pipeline representation (`brix describe`,
UI), `type: filter` adds semantic clarity. For pure execution, it's irrelevant.

## Summary

| Concept | Verdict | Rationale |
|---------|---------|-----------|
| Loop/Pagination in engine | No | Violates D-01/D-02 |
| `until` syntax | No | Uncontrollable state |
| Strategy in skill-prompt | Yes | Correct responsibility split |
| `filter` runner type | Later (v2) | D-04 precedent, insufficient pain |
