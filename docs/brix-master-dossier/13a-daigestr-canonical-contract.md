# Daigestr Canonical Contract

## Purpose
This document freezes the Brix-side interpretation of Daigestr responses.

Brix must consume Daigestr results through exactly three canonical payload regions:

- `raw.meta`
- `raw.extracted`
- `raw.normalized`

Anything outside those regions is non-canonical and must not be treated as an authoritative source.

## Canonical Sources

### Metadata
Brix reads technical and document-level metadata only from `raw.meta`.

Examples:
- `document_type`
- `document_type_confidence`
- `template_used`
- `template_version`
- `quality_score`
- `quality_grade`
- `retry_applied`
- `retry_reason`
- `initial_mode`
- `final_mode`
- `initial_quality_score`
- `final_quality_score`
- `retry_threshold_used`
- `request_id`
- `attempt_number`
- `attempt_count`
- `attempt_mode`
- `pipeline_steps`

### Business Extraction
Brix reads extracted business payloads from:

- `raw.extracted`
- `raw.normalized`

`raw.extracted` contains the raw template/schema result.
`raw.normalized` contains the normalization result used for downstream persistence and routing.

## Non-canonical Inputs

The following are explicitly non-canonical and must not override canonical values:

- logs
- top-level `document_type`
- top-level `quality_score`
- top-level `_quality_score`
- top-level `template`
- mirrored metadata in `normalized` or `extracted` when the field belongs to `raw.meta`

## Brix Output Rules

When Brix mirrors selected Daigestr fields into its own result envelope:

- `document_type` must come from `raw.meta.document_type`
- `quality_score` and `_quality_score` must come from `raw.meta.quality_score`
  or, if absent, the documented retry-aware fallback inside `raw.meta`
- `_meta.template` must come from `raw.meta.template_used`

Brix may provide convenience mirrors, but those mirrors must always derive from canonical fields.

## Compatibility Rule

If a Daigestr response omits canonical metadata, Brix must degrade explicitly instead of inventing values from
non-canonical mirrors.

That means:
- missing canonical fields may become empty or `null`
- Brix must not silently recover the value from deprecated top-level mirrors

## Persistence Gate

When Brix persists Daigestr outputs through `document.persist_extraction_result`, the payload must already satisfy
the canonical contract.

At minimum, the persisted result must contain:
- `raw.meta.document_type`
- `raw.meta.template_used`
- `raw.meta.quality_score`
- `raw.extracted`
- `raw.normalized`

If those fields are missing, persistence must fail explicitly with contract violations instead of storing an
underspecified payload in `documents.raw_structured`.

## Rationale

This prevents:

- log-driven debugging as an implicit product contract
- ambiguity between old mirrors and current authoritative fields
- brittle fallback behavior when Daigestr evolves
