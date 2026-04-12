# T-9.1.3 Service-Aware Hang Detection

## Purpose
This task stops Brix from treating every stale heartbeat as a hang when an external service can still prove that work is active.

The previous behavior used only:

- `last_heartbeat`

That was too weak for long-running external jobs, especially when:

- the Brix-side heartbeat lagged
- the external service still reported active processing
- operators saw false hang warnings for slow but healthy work

## What Changed

`get_run_status` now evaluates stale-running detection in two layers:

1. heartbeat age
2. service-backed runtime state when available

If the heartbeat is stale but the external service still reports:

- `queued`
- `processing`
- `running`
- `retrying`

then Brix no longer marks the run as hung.

If service polling is unavailable, Brix falls back to the old heartbeat-based hang heuristic.

## Resulting Behavior

### False-hang protection
Stale heartbeat plus live service status now produces:

- `suspected_hang = false`
- a hint that active service work is still in progress

### Fallback preservation
Stale heartbeat without service-backed runtime evidence still produces:

- `suspected_hang = true`
- the existing operational hint to inspect logs or cancel the run

## Why This Matters

This is the first step toward runtime-state-based hang detection instead of missing-end-state guessing.

It reduces noise for:

- long OCR jobs
- retry-heavy external processing
- services with their own queue and attempt lifecycle

without weakening the fallback behavior for integrations that still have no pollable runtime surface.

## Verification

- regression for stale heartbeat + active service progress => not hung
- regression for stale heartbeat without service progress => still hung
- existing `get_run_status` hang tests kept green
