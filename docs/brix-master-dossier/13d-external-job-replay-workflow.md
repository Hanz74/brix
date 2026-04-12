# External Job Replay Workflow

## Purpose
This workflow lets Brix replay known external-job responses without calling the upstream service again.

## Supported Replay Sources
- `replay_fixture_path`
  - points to a persisted fixture JSON such as `tests/fixtures/daigestr/*.json`
  - fixture files may wrap the usable payload under `extraction_result`
- `replay_response_path`
  - points to a persisted `response.json` artifact under a run workdir
  - relative paths resolve against the current run workdir first

## Execution Rules
- Replay uses the same canonicalization path as live execution.
- Brix must not make an HTTP call when a replay source is provided.
- Replay outputs must still expose the canonical Daigestr contract:
  - `raw.meta`
  - `raw.extracted`
  - `raw.normalized`
- Replay metadata is attached under:
  - `_meta.replay`
  - `raw.meta.replay`
  - top-level `replay`

## Operator Value
- reproduce regressions without another upstream OCR/LLM call
- inspect persisted `response.json` artifacts through the normal runner path
- keep fixture-driven tests on the same transformation logic as production
