# T-9.2.3 External Runtime Drift Gate

## Purpose
This task converts runtime drift into an explicit, tested failure mode.

The target problem is not a simple missing feature. It is partial or misleading runtime support, for example:

- async start endpoint exists but result endpoint is missing
- status polling exists but required progress fields are incomplete
- service version is missing, so compatibility cannot be reasoned about safely

## Drift Issues

The Daigestr capability handshake now classifies runtime drift explicitly:

- `missing_service_version`
- `async_contract_incomplete`
- `job_progress_fields_incomplete`

These issues are surfaced in the capability model itself and preserved in compatibility failures.

## Runtime Rule

If a step explicitly requires the async path and the handshake shows drift, Brix now fails before the expensive live call starts.

This makes drift:

- visible
- actionable
- reproducible in tests

instead of silently degrading or failing only after document upload.

## Why This Matters

This is the durable bugfix gate for the capability/contract layer.

It ensures later regressions in external runtime shape become:

- test failures
- structured compatibility errors

not surprise production behavior.

## Verification

- regression for incomplete async runtime contract
- regression for missing core progress fields in an advertised polling contract
- runner regression for explicit async requirement failing on runtime drift
