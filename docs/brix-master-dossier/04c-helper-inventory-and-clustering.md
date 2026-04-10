# Helper Inventory and Clustering

## Purpose
Helper inventory makes helper-to-brick migration explicit. Helpers remain supported, but Brix must know whether each helper is narrow utility, transitional debt, legacy residue, or a brick candidate.

## Source of Truth
The inventory is DB-first. It reads the `helper` table and DB-backed pipeline-helper relationships. Filesystem helper paths are treated only as legacy metadata and as a signal for review.

## Strategic Categories
- `brick_candidate`: domain logic that should be considered for a reusable brick.
- `stable_helper`: narrow helper with complete metadata and no strong brick signal.
- `legacy_review`: helper still depends on legacy script-path semantics or lacks DB code.
- `metadata_required`: helper cannot be evaluated because required metadata is incomplete.

Every helper receives exactly one strategic category.

## Family Clusters
Helpers are clustered into reusable families:

- `extraction`
- `classification`
- `persistence`
- `source_transfer`
- `conversion`
- `notification`
- `validation`
- `orchestration`
- `utility`

Family classification uses helper name, description, tags, script path metadata, and stored DB code. The result is heuristic by design, but deterministic and inspectable.

## Brick-Candidate Signals
Signals include:

- repeated pipeline usage
- domain logic family
- missing metadata
- missing DB code
- legacy script path
- no DB pipeline usage

High-value domain helpers become brick candidates. Metadata gaps do not hide the helper; they are surfaced as part of the inventory.

## MCP Guidance
`list_helpers(include_inventory=true)` enriches each helper with family, domain, strategic category, migration candidacy, usage count, missing metadata, and signals. `list_helpers(include_reuse_candidates=true)` adds systematic brick-candidate detection for repeated helper usage, repeated SQL patterns, and repeated step sequences. `get_tips` summarizes helper inventory so agents see brick-candidate pressure before inventing new helpers.

## Brick-Candidate Detector
The detector lives in `src/brix/brick_candidate_detector.py` and reads only DB-backed helpers, pipelines, and `pipeline_step` rows.

It emits three candidate kinds:

- `repeated_helper_usage`: a DB helper is reused across pipelines and belongs to a domain family.
- `repeated_sql_pattern`: normalized SQL templates repeat across `db.query`, `db.exec`, or `db.upsert` steps.
- `repeated_step_sequence`: contiguous step-type windows repeat across pipelines.

Each candidate includes evidence, confidence, signals, and a suggested brick name. The detector does not create bricks automatically; it generates structured migration pressure for later governance and implementation decisions.

## Boundary to Later Tasks
This task classifies helpers and detects repeated logic as brick-candidate pressure. Mandatory helper justification and enforcement belongs to `T-2.2.3`.

## Helper Governance
New helpers must not enter the system as silent product design. Helper creation and update flows therefore evaluate governance metadata:

- complete description
- input and output schemas
- project
- tags
- either `reason_not_a_brick` or `brick_candidate_ref`

Helpers that do not meet this bar remain `draft` and emit governance warnings. The validator also warns when a pipeline references a helper with incomplete governance metadata. This keeps existing legacy helpers usable while making new helper debt visible and actionable.
