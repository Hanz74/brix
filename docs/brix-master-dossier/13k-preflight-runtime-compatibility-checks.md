# T-9.2.2 Preflight Runtime Compatibility Checks

## Purpose
This task turns capability knowledge into an actionable compatibility gate before expensive external calls start.

The key rule is:

- soft preference may fall back
- explicit requirement must fail fast

## Runtime Rule

For Daigestr async jobs:

- if async usage is only a default preference, Brix may fall back to sync when the service does not support async jobs
- if a step explicitly requires `use_async_jobs=true`, Brix now fails before the document upload when the service handshake says async jobs are unsupported

## Diagnostic Shape

The failure is surfaced as a structured external-job compatibility error:

- `error_type = external_job_capability_error`
- includes `service_capabilities`
- includes an actionable message describing how to recover

## Why This Matters

This makes drift visible before the expensive path starts.

Without this check, Brix would silently degrade or only fail later after document upload and service work had already begun.

With this check:

- soft compatibility remains pragmatic
- hard requirements become explicit and debuggable

## Verification

- runner regression for async capability present
- runner regression for soft fallback to sync when async support is absent
- runner regression for explicit async requirement failing fast when the service contract is incompatible
