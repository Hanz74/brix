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
`list_helpers(include_inventory=true)` enriches each helper with family, domain, strategic category, migration candidacy, usage count, missing metadata, and signals. `get_tips` summarizes helper inventory so agents see brick-candidate pressure before inventing new helpers.

## Boundary to Later Tasks
This task classifies helpers. Repeated SQL and step-sequence detection belongs to `T-2.2.2`. Mandatory helper justification and enforcement belongs to `T-2.2.3`.
