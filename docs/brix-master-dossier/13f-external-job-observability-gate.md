# External Job Observability Gate

## Purpose
This gate locks in the runtime-observability behaviors that previously regressed for long-running external jobs.

## Gate Checks
- finished run history must override stale live `run.json` state
- completed runs must still expose persisted `current_progress` and `step_progress_history`
- structured external-job failure detail must survive into:
  - `steps_data`
  - `get_run_errors`
  - `get_run_log`
- retry chains must remain visible as machine-readable `attempt_history`

## Failure Meaning
If this gate fails, Brix is drifting back toward one of these anti-patterns:
- ghost-running after completion
- invisible retries
- opaque final failures without attempt context
- progress history only reconstructable from logs
