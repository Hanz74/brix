# External Job Recovery Policy

## Purpose
This policy defines how Brix resumes long-running external steps after interruption, timeout, or process restart.

## Recovery Rules
- If `external_job_artifacts/<step_id>/response.json` exists for the resumed run, Brix must replay that response instead of calling the external service again.
- If only partial artifacts exist, such as:
  - `request.json`
  - `attempt_history.json`
  - `markdown.md`
  and no final `response.json` exists, Brix must treat the prior attempt as incomplete and restart the external call from scratch.
- Explicit replay inputs always win over automatic resume recovery:
  - `replay_fixture_path`
  - `replay_response_path`

## Rationale
- `response.json` is the only persisted artifact that proves a complete external response was already received.
- Partial artifacts are diagnostically useful but not sufficient proof of a finished external result.
- This avoids ambiguous half-resume behavior where Brix would otherwise guess whether an upstream call finished.

## Operator Expectations
- completed external responses are reused deterministically
- incomplete attempts are restarted explicitly
- replay and resume share the same canonical transformation path
