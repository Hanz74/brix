# T-9.2.1 External Service Capability Handshake

## Purpose
This task makes runtime expectations explicit before Brix uses advanced external-service features.

The immediate reference service is Daigestr. Brix now performs a lightweight capability handshake instead of assuming that async jobs and pollable status exist.

## Handshake Inputs

Brix reads:

- health endpoint
- tips/contract endpoint

Configured env-first via:

- `BRIX_DAIGESTR_HEALTH_ENDPOINT`
- `BRIX_DAIGESTR_TIPS_ENDPOINT`

## Handshake Output

The Daigestr capability model now captures:

- `service`
- `version`
- `supports_async_jobs`
- `supports_job_status`
- `supports_job_result`
- `job_progress_fields`

It also retains raw source payloads:

- `raw_health`
- `raw_tips`

## Runtime Behavior

When `extract.document_with_daigestr` prefers async jobs:

1. Brix fetches service capabilities
2. if async runtime support is confirmed, Brix uses the async start/poll/result path
3. if support is missing, Brix falls back to the synchronous path

This fallback is explicit and visible in step progress metadata instead of being implicit behavior.

## Why This Matters

This reduces drift between:

- what Brix thinks the service can do
- what the live service actually exposes

It is the first layer needed for the later compatibility gates in `W9.2`.

## Verification

- capability parser regression for supported async contract
- capability parser regression for missing async contract
- runner regression for capability-confirmed async path
- runner regression for capability-driven sync fallback
