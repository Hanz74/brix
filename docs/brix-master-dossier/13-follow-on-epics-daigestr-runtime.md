# Follow-on Epics: Daigestr Integration and Long-Running Runtime

## Purpose
This document defines the next Brix epics derived from the live Daigestr integration work after `E1` through `E6`.

The original six epics are complete as a transformation program. The remaining work is no longer about foundational DB-first or brick-first migration. It is about:

- hardening the Daigestr integration contract
- fixing real bundled-document result defects
- making long-running external jobs observable and replayable
- eliminating log-parsing as the primary source of truth for runtime state

These epics are intentionally explicit. They combine bugfix work and product hardening work because the current remaining problems sit exactly at that boundary.

## Current Confirmed Findings

### Brix-side improvements already complete
- live containers run current Brix code
- `raw.meta` is treated as the canonical metadata source
- `document_type`, `quality_score`, `template`, and retry metadata are persisted correctly into `raw_structured`
- long-running HMK extraction runs finalize correctly in Brix
- run history is preferred over stale live status when appropriate

### Remaining cross-system defect
- bundled bank statements still do not land as a complete structured result
- `raw_structured.markdown` contains multiple statements
- `raw_structured.normalized.kontoauszuege` is still empty
- `raw_structured.normalized.zeitraum` is still empty

### Remaining Brix product gaps
- no pollable external-job progress surface without log parsing
- no persisted intermediate snapshots for long-running external jobs
- no replay path for external-job attempts and intermediate artifacts

## Execution Model

These follow-on epics follow the same execution rules as the original program:

- `Epic = major`
- `Wave = minor`
- `Task = patch`

Each task must be:

- implemented end-to-end
- regression-tested
- gatekeeper-reviewed
- committed immediately when green
- versioned immediately
- recorded in the DB changelog immediately

## Epic E7: Daigestr Integration Hardening

### Goal
Make the Daigestr integration contract explicit, stable, and usable by Brix without inference, log inspection, or fallback guessing.

### Wave W7.1: Canonical Contract Enforcement

#### Task T-7.1.1: Freeze the Brix-side canonical Daigestr response contract
Goal:
- codify exactly which Daigestr fields Brix reads and which it ignores

Deliverables:
- explicit contract spec for `raw.meta`, `raw.extracted`, `raw.normalized`
- banned field list for non-canonical mirrors and log-derived assumptions
- tests proving Brix ignores deprecated or misleading mirrors

Done when:
- Brix has a single documented and enforced Daigestr response contract

#### Task T-7.1.2: Add contract validation for persisted Daigestr outputs
Goal:
- surface malformed or incomplete Daigestr payloads before downstream use

Deliverables:
- validation layer for required and nullable integration fields
- targeted findings for missing canonical metadata
- regression tests for incomplete and stale payload shapes

Done when:
- malformed Daigestr outputs are visible as explicit validation defects

#### Task T-7.1.3: Persist real Daigestr regression fixtures
Goal:
- capture real integration cases so fixes do not depend on repeated live calls

Deliverables:
- fixture set for short invoices/receipts and long bundled statements
- deterministic replay-ready payload corpus
- tests using persisted fixtures instead of live services

Done when:
- the main Daigestr integration cases are reproducible offline

### Wave W7.2: Bundled Document and Multi-Statement Handling

#### Task T-7.2.1: Define Brix handling for bundled statements
Goal:
- make downstream Brix behavior explicit when `normalized.kontoauszuege` exists

Deliverables:
- Brix-side handling rules for multi-statement bank documents
- persistence and downstream consumption rules
- tests for single-statement vs multi-statement payloads

Done when:
- Brix can consume bundled statement results without collapsing them to a single statement

#### Task T-7.2.2: Fix downstream assumptions that expect a single statement
Goal:
- remove hard assumptions that bank statements are always singular

Deliverables:
- HMK and generic statement handling updates
- compatibility rules for single-result and multi-result cases
- regression coverage for statement arrays and aggregated views

Done when:
- downstream flows no longer silently discard bundled statement structure

#### Task T-7.2.3: Add bundled-document bugfix gate
Goal:
- ensure the previously observed bundled-statement defect cannot regress silently

Deliverables:
- regression tests for the known 52-page bundled statement case
- checks for `kontoauszuege`, `zeitraum`, and complete booking coverage
- gatekeeper checklist for bundled-document correctness

Done when:
- the bundled-statement defect is covered by durable automated checks

### Wave W7.3: Integration Bugfix Closure

#### Task T-7.3.1: Harden nullability and partial-field behavior
Goal:
- ensure Brix handles partial but still valid Daigestr payloads safely

Deliverables:
- explicit handling for optional canonical fields
- no silent fallback to non-canonical mirrors
- regression tests for nullable integration fields

Done when:
- Brix degrades safely without inventing values

#### Task T-7.3.2: Mirror quality and retry metadata consistently
Goal:
- ensure the quality and retry signals used by Brix are always available in one place

Deliverables:
- final metadata mapping cleanup
- tests for `quality_score`, retry fields, and mode transitions
- verification against real fixtures

Done when:
- Brix reads quality and retry state consistently across all supported result classes

#### Task T-7.3.3: Close the HMK integration bug loop
Goal:
- convert the live HMK findings into permanent regression protection

Deliverables:
- targeted HMK regression suite
- bundle-aware assertions
- live-vs-fixture parity checks where feasible

Done when:
- the known HMK integration failures are reproducible and guarded

## Epic E8: Long-Running Job Observability and Replay

### Goal
Make long-running external jobs in Brix observable, inspectable, and replayable without reliance on log scraping.

### Wave W8.1: Canonical External Job Progress Model

#### Task T-8.1.1: Define canonical external-job progress state
Goal:
- formalize the runtime state model Brix expects from long-running external jobs

Deliverables:
- progress state spec including stage, attempt, mode, retry state, and percent
- canonical mapping rules for page/item progress
- separation of live progress from final result summary

Done when:
- Brix has a documented state model for long-running external jobs

#### Task T-8.1.2: Persist progress snapshots during execution
Goal:
- store evolving external-job state while a step is still running

Deliverables:
- persisted progress snapshots in run workdir and DB-backed run state
- latest-known progress per step
- tests for state updates during long-running execution

Done when:
- a running job can be inspected without waiting for final completion

#### Task T-8.1.3: Expose progress through MCP run inspection
Goal:
- make long-running progress visible through Brix MCP tools

Deliverables:
- `get_run_status` enhancements for external-job progress
- progress visibility in run inspection surfaces
- tests for running-job observability

Done when:
- agents can inspect real progress without reading service logs

### Wave W8.2: Intermediate State and Attempt History

#### Task T-8.2.1: Persist external request and attempt metadata
Goal:
- retain enough data to reconstruct what happened in each attempt

Deliverables:
- persisted `request_id`, attempt counters, modes, retry triggers
- stable storage shape for external attempt history
- tests for attempt metadata persistence

Done when:
- Brix can explain the history of a long-running external step

#### Task T-8.2.2: Persist intermediate artifacts and stage outputs
Goal:
- allow debugging without repeating expensive upstream calls

Deliverables:
- storage policy for intermediate payloads and snapshots
- retention rules for large artifacts
- tests for storing and loading stage outputs

Done when:
- critical external-job intermediate state survives after completion or failure

#### Task T-8.2.3: Surface retry and failure history cleanly
Goal:
- make retry chains and soft failures visible without log reconstruction

Deliverables:
- run history extensions for retry visibility
- MCP output that distinguishes upstream retry from final failure
- regression tests for chained retry cases

Done when:
- long-running failures are diagnosable from persisted state alone

### Wave W8.3: Replay and Recovery

#### Task T-8.3.1: Add replay path for external-job fixtures and artifacts
Goal:
- replay problem cases from persisted payloads instead of live upstream calls

Deliverables:
- replayable fixture loading for external-job outputs
- tooling and tests for deterministic replay
- documentation for replay workflow

Done when:
- a known external problem case can be replayed offline

#### Task T-8.3.2: Define resume and recovery rules for long-running steps
Goal:
- avoid ambiguous behavior after interruption, timeout, or restart

Deliverables:
- explicit resume semantics for long-running external steps
- recovery policy for partially completed attempts
- regression tests for interrupted and resumed runs

Done when:
- Brix can resume or restart long-running work predictably

#### Task T-8.3.3: Add observability bugfix gate
Goal:
- lock in protection against ghost-running, stale status, and invisible retries

Deliverables:
- regression suite for historical observability failures
- gatekeeper checks for run status integrity
- tests proving persisted progress and history remain coherent

Done when:
- the known runtime-observability defects are guarded

## Epic E9: External Service Runtime Contracts

### Goal
Reduce drift between Brix and external services by treating runtime capabilities and progress surfaces as explicit contracts rather than incidental behavior.

### Wave W9.1: Pollable Runtime Surface

#### Task T-9.1.1: Integrate pollable job status for external services
Goal:
- replace log parsing with service-backed runtime polling where available

Deliverables:
- Brix-side polling interface for external service job state
- compatibility handling for services with and without polling
- tests for service-backed progress retrieval

Done when:
- Brix can query a running external job through a defined interface

#### Task T-9.1.2: Expose attempt, retry, and page progress in one surface
Goal:
- unify the progress data Brix presents for external jobs

Deliverables:
- pollable fields for stage, attempt, retry status, and page/item progress
- MCP exposure of those fields
- tests for multi-attempt progress reporting

Done when:
- the runtime surface is rich enough for LLMs and operators to understand a live job

#### Task T-9.1.3: Base hang detection on service runtime state
Goal:
- stop inferring hangs from missing end states alone

Deliverables:
- hang detection rules informed by external service state
- fallback rules when polling is unavailable
- tests for false-hang and real-hang differentiation

Done when:
- Brix distinguishes slow work from stuck work more reliably

### Wave W9.2: Capability and Version Contracts

#### Task T-9.2.1: Add capability/version handshake for external services
Goal:
- make feature expectations explicit instead of implicit

Deliverables:
- service capability model
- version/capability checks at integration points
- tests for capability negotiation and mismatch detection

Done when:
- Brix knows whether the connected service supports the required runtime features

#### Task T-9.2.2: Validate contract compatibility before live use
Goal:
- detect incompatible service versions or missing features before expensive runs start

Deliverables:
- preflight compatibility checks
- actionable diagnostics for incompatible service states
- regression tests for drift scenarios

Done when:
- incompatible runtime contracts are visible before production execution

#### Task T-9.2.3: Add external-runtime drift bugfix gate
Goal:
- keep Brix and external runtime behavior aligned over time

Deliverables:
- drift checks for runtime capabilities and expected response contract
- test coverage for versioned integration drift
- gatekeeper checks for external-runtime compatibility

Done when:
- service-runtime drift is a tested and visible failure mode instead of a surprise

## Recommended Execution Order

The recommended order is:

1. `E7`
2. `E8`
3. `E9`

Reasoning:
- `E7` addresses the active contract and bundled-document defect line
- `E8` makes long-running failures diagnosable and reproducible
- `E9` builds a cleaner long-term runtime contract once the first two are in place

## Non-goals

These epics do not authorize:

- rebuilding Daigestr domain logic inside Brix
- parsing service logs as a permanent product strategy
- inventing new non-canonical mirrors outside `raw.meta`, `raw.extracted`, and `raw.normalized`
- shipping workaround logic as a final state
