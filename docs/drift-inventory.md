# Drift Inventory

Generated from live `mcp__brix__brix__get_tips` output plus direct DB inspection on 2026-04-09.

## Summary

- Total findings: 8
- Auto-fixed findings: 0

## Count Per Issue Type

| Issue type | Count |
| --- | ---: |
| `NO_STEP_ROWS` | 1 |
| `UNKNOWN_HELPER_REF` | 8 |
| `HELP_LEGACY_TYPE` | 14 |
| `MISSING_DESCRIPTION` | 5 |

## Categorized Inventory

### Missing Rows

- `NO_STEP_ROWS` (1)
  - Pipeline: `buddy-test-pipe`
  - Pipeline ID: `5015c088-788b-4ed1-ac87-6eb558d2b2e7`
  - Finding: pipeline row exists without any `pipeline_step` rows

### Unknown Helper References

- `UNKNOWN_HELPER_REF` (8)
  - `apply-template-updates/apply:helper=apply_template_updates`
  - `buddy-extract-all/llm-structured:helper=buddy_extract_structured_llm`
  - `buddy-extract-all/merge:helper=buddy_extract_merge_validate`
  - `buddy-process-attachments-v2/find:helper=att_find_candidates`
  - `buddy-process-attachments-v2/process:helper=att_process_single`
  - `convert-pdf/read_file:helper=file_to_base64`
  - `enrich-markitdown-templates/enrich:helper=enrich_markitdown_templates`
  - `import-markitdown-templates/import:helper=import_templates_to_markitdown`

### Legacy Help Content

- `HELP_LEGACY_TYPE` (14)
  - `anti-patterns:mcp,python`
  - `beispiele:http,mcp,python`
  - `credentials:python`
  - `dag:python`
  - `debugging:python`
  - `error-patterns:filter,python`
  - `foreach:python`
  - `helpers:mcp,python`
  - `lessons-learned:set`
  - `quick-start:http,python`
  - `registries:mcp`
  - `sdk:http,mcp,set`
  - `templates:http,python`
  - `triggers:filter,mcp`

### Missing Metadata

- `MISSING_DESCRIPTION` (5)
  - Helper: `cody_build_params`
  - Helper: `cody_diff_summarizer`
  - Helper: `cody_epic_completion_check`
  - Helper: `cody_gk_verdict_router`
  - Helper: `cody_inbox_triage_filter`

## Affected Entities

### Pipelines

- `buddy-test-pipe`
- `apply-template-updates`
- `buddy-extract-all`
- `buddy-process-attachments-v2`
- `convert-pdf`
- `enrich-markitdown-templates`
- `import-markitdown-templates`

### Helpers

- Unknown helper refs
  - `apply_template_updates`
  - `buddy_extract_structured_llm`
  - `buddy_extract_merge_validate`
  - `att_find_candidates`
  - `att_process_single`
  - `file_to_base64`
  - `enrich_markitdown_templates`
  - `import_templates_to_markitdown`
- Helpers missing description metadata
  - `cody_build_params`
  - `cody_diff_summarizer`
  - `cody_epic_completion_check`
  - `cody_gk_verdict_router`
  - `cody_inbox_triage_filter`

## Verification Basis

- `mcp__brix__brix__get_tips` reported 8 integrity problems on 2026-04-09.
- Direct DB inspection confirmed:
  - the zero-step pipeline row,
  - the 8 stale helper references,
  - the 5 helpers without descriptions.
- Help-topic legacy matches were reproduced against the live help content using the same legacy-step-type pattern used by `src/brix/integrity.py`.
