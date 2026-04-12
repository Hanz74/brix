# T-9.1.2 Unified External Job Progress Surface

## Purpose
This task turns scattered progress fields into one operator- and LLM-friendly MCP surface.

Before this task, `get_run_status` exposed runtime details across several places:

- `current_progress`
- `step_progress`
- `step_progress_history`

Those fields remain available, but they were not a single stable contract for understanding:

- current attempt
- retry state
- current page/item progress
- recent attempt transitions

## What Changed

`get_run_status` now emits a unified `external_job_progress` object whenever Brix can identify an external job.

The surface is derived from:

- the freshest known current progress snapshot
- per-step progress snapshots
- persisted progress history

## Exposed Fields

The unified surface can include:

- `service`
- `step_id`
- `job_id`
- `request_id`
- `status`
- `stage`
- `current_stage`
- `attempt`
- `attempt_count`
- `mode`
- `retry_state`
- `retry_reason`
- `page_current`
- `page_total`
- `processed`
- `total`
- `percent`
- `progress_kind`
- `upstream_attempt`
- `metadata`
- `message`

It also exposes summarized attempt history as:

- `attempts`

Each attempt entry can include:

- `attempt`
- `attempt_count`
- `mode`
- `status`
- `retry_state`
- `retry_reason`
- `page_current`
- `page_total`
- `processed`
- `total`
- `percent`
- `stage`
- `request_id`
- `job_id`

## Why This Matters

This is the first Brix runtime surface that lets an agent answer:

- What is the job doing right now?
- Which attempt is active?
- Was a retry triggered?
- How far is the current page/item progress?
- What happened in earlier attempts?

without stitching together multiple raw status fragments manually.

## Verification

- regression test for service-backed progress polling
- regression test for unified multi-attempt retry/page progress surface
- existing `get_run_status`, `get_run_errors`, and intra-step progress tests kept green
