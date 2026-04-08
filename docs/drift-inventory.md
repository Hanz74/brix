# Drift Inventory

Generated from `run_integrity_checks(db)` on 2026-04-08.

## Summary

- Total findings: 7
- Auto-fixed findings: 0

## Count Per Issue Type

| Issue type | Count |
| --- | ---: |
| `NO_STEP_ROWS` | 1 |
| `UNKNOWN_HELPER_REF` | 1 |
| `MISSING_DESCRIPTION` | 5 |

## Categorized Inventory

### Missing Rows

- `NO_STEP_ROWS` (1)
  - Pipeline: `buddy-test-pipe`
  - Finding: pipeline has 0 step rows in DB

### Unknown Refs

- `UNKNOWN_HELPER_REF` (8 affected step references across 1 finding)
  - `apply-template-updates/apply:helper=apply_template_updates`
  - `buddy-extract-all/llm-structured:helper=buddy_extract_structured_llm`
  - `buddy-extract-all/merge:helper=buddy_extract_merge_validate`
  - `buddy-process-attachments-v2/find:helper=att_find_candidates`
  - `buddy-process-attachments-v2/process:helper=att_process_single`
  - `convert-pdf/read_file:helper=file_to_base64`
  - `enrich-markitdown-templates/enrich:helper=enrich_markitdown_templates`
  - `import-markitdown-templates/import:helper=import_templates_to_markitdown`

### Missing Metadata

- `MISSING_DESCRIPTION` (5)
  - Helper: `cody_build_params`
  - Helper: `cody_diff_summarizer`
  - Helper: `cody_epic_completion_check`
  - Helper: `cody_gk_verdict_router`
  - Helper: `cody_inbox_triage_filter`

### Stale Artifacts

- None found in the current integrity run.

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

## Raw Finding Notes

- `NO_STEP_ROWS`: 1 pipeline has 0 step rows in DB.
- `UNKNOWN_HELPER_REF`: 8 step references point to helpers that are not currently known to integrity checks / registry state.
- `MISSING_DESCRIPTION`: 5 helper records exist without description metadata.
