# External Job Artifact Policy

## Purpose
This policy defines how Brix persists replay-relevant external-job artifacts without duplicating large upstream payloads unnecessarily.

## Storage Layout
- Artifacts live under `external_job_artifacts/<step_id>/` inside the run workdir.
- Canonical files:
  - `request.json`
  - `response.json`
  - `attempt_history.json`
  - `markdown.md` when textual stage output exists

## Retention Rules
- Raw request `base64` and `content` are not duplicated into persisted request artifacts.
- Persisted request artifacts store only metadata plus byte counts for large payload fields.
- Large textual outputs such as Markdown are stored as dedicated files, not duplicated inline into every JSON artifact.
- JSON artifacts may reference large sidecar files by relative path.

## Replay Goal
- A failed or suspicious external run should be inspectable from workdir artifacts alone.
- Replaying the final response or inspecting the request/attempt history must not require another upstream call.
