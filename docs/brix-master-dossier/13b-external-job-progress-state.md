# Canonical External Job Progress State

## Purpose
This document defines the canonical live-progress shape for long-running external jobs in Brix. It separates evolving runtime state from final extracted business results.

## Canonical Fields
- `processed`: current item progress when the job reports item-level work
- `total`: total item count when known
- `percent`: normalized completion percentage
- `progress_kind`: one of `item`, `page`, or `generic`
- `message`: human-readable progress message
- `stage`: canonical stage name such as `ocr`, `extract`, `validate`, or `persist`
- `attempt`: current attempt number
- `attempt_count`: total attempt count when known
- `mode`: current execution mode such as `default` or `full`
- `retry_state`: one of `none`, `eligible`, `pending`, or `applied`
- `retry_reason`: optional retry explanation
- `request_id`: external job request identifier
- `page_current`: current page for page-based jobs
- `page_total`: total page count for page-based jobs

## Mapping Rules
- Item-based progress maps to `processed` and `total`.
- Page-based progress maps to `page_current` and `page_total`.
- `percent` is derived from `processed/total` when possible.
- If no item counts exist, `percent` falls back to `page_current/page_total`.
- If neither item nor page counts exist, `percent` falls back to an explicit `percent` or `pct` value.
- `retry_state` is canonicalized from explicit retry state first, then from retry flags or mode transitions.

## Separation of Concerns
- Live progress belongs in step progress snapshots and run inspection surfaces.
- Final business output belongs in the step result payload.
- Brix must not infer final extraction quality or retry success from progress snapshots alone.
