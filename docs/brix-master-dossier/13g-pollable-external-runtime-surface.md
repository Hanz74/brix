# T-9.1.1 Pollable External Runtime Surface

## Purpose
This task introduces the first real service-backed runtime polling path for external jobs in Brix.

The immediate reference integration is Daigestr. Brix now prefers the explicit async contract when available:

- `POST /v1/convert/async`
- `GET /v1/jobs/{job_id}`
- `GET /v1/jobs/{job_id}/result`

When the service does not return a usable `job_id`, Brix falls back to the existing synchronous request path.

## What Changed

### Config
New env-first runtime settings:

- `BRIX_DAIGESTR_ASYNC_CONVERT_ENDPOINT`
- `BRIX_DAIGESTR_USE_ASYNC_JOBS`
- `BRIX_DAIGESTR_JOB_STATUS_ENDPOINT_TEMPLATE`
- `BRIX_DAIGESTR_JOB_RESULT_ENDPOINT_TEMPLATE`
- `BRIX_DAIGESTR_JOB_POLL_INTERVAL_SECONDS`

### Runner behavior
`extract.document_with_daigestr` now supports a dual path:

1. start async job
2. poll canonical progress by `job_id`
3. fetch final result after completion

Fallback behavior:

1. if async start is unsupported, unavailable, or returns no `job_id`
2. use the previous direct conversion call

### Progress contract
Canonical progress now preserves service-backed runtime fields:

- `service`
- `status`
- `current_stage`
- `job_id`
- `request_id`
- `attempt`
- `attempt_count`
- `mode`
- `page_current`
- `page_total`
- `upstream_attempt`
- `metadata`

### MCP status surface
`get_run_status` now polls the external service when the persisted progress payload contains:

- `service=daigestr`
- `job_id=<value>`

This replaces stale local-only progress snapshots with fresher service-backed runtime state when available.

## Why This Matters
This task removes one major source of drift: operators and agents no longer have to infer live job state from logs when the external service already exposes a pollable contract.

It is also backward-compatible:

- services with polling support get richer runtime visibility
- services without polling support continue to work through the old path

## Verification
- targeted runner regressions for async path and sync fallback
- service-backed `get_run_status` regression
- existing intra-step progress and MCP run-status regressions kept green
