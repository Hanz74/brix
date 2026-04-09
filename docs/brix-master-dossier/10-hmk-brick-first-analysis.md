# HMK Brick-First Analysis

## Scope
- `buddy-hmk-extract`
- `buddy-hmk-extract-single`

## Current State

### `buddy-hmk-extract`
This pipeline is structurally healthy as an orchestrator:
- find candidates in DB
- invoke a per-document subpipeline

That is acceptable pipeline responsibility.

### `buddy-hmk-extract-single`
This pipeline contains the architectural debt:
- raw source fetch
- helper-mediated download and save
- conditional file loading
- subpipeline extraction
- inline persistence logic
- specialist state mutation

## Key Workaround Signals

### `save_results`
The strongest workaround signal.

Problems:
- values rendered directly into SQL text
- manual quote escaping
- direct ID interpolation
- persistence semantics hidden inside pipeline SQL

Interpretation:
- this is a historical escape hatch around an unstable persistence path
- it is not the desired brick-first operating mode

### `mark_processed`
Less complex but still too specialized to remain inline forever.

### Conditional Fragility
`extract` depends on `read_file_b64.output`, but the predecessor is conditional and should be protected through a reusable contract rather than ad hoc template discipline.

## What Should Move Out of HMK

### Reusable Document Persistence
Candidate brick:
- `document.persist_extraction_result`

Responsibilities:
- set `raw_structured`
- set `doc_type`
- set `content_hash`
- set `file_path`
- append specialist or extraction status

### Reusable Specialist State Mutation
Candidate brick:
- `document.mark_specialist_processed`

### Reusable Extractable Payload Preparation
Candidate brick:
- `document.prepare_extractable_payload`

### Reusable Source Fetch and Store
Candidate brick or compositional primitive:
- `source.onedrive_fetch_and_store`

### Reusable Daigestr Execution Primitive
Candidate brick:
- `extract.document_with_daigestr`

## Target HMK Shape

### Target `buddy-hmk-extract`
- query candidate rows
- iterate
- invoke document-processing subpipeline or composite brick

### Target `buddy-hmk-extract-single`
- fetch and store file
- prepare extractable payload
- run standard extraction brick
- persist standard extraction result

That is a declarative orchestration pipeline, not a hidden mini-application.

## Why HMK Matters
HMK is not only a local pipeline cleanup opportunity. It is a platform anchor:
- it contains the exact kind of workaround pressure Brix should be designed to eliminate
- it is simple enough to refactor clearly
- it can prove that Brix is capable of turning workaround-heavy pipelines back into configuration-first pipelines

## Required HMK Outcome
After migration:
- no persistence workarounds remain inline
- no repeated extractability logic remains ad hoc
- no HMK-only mutation logic remains where a reusable brick should exist
- HMK becomes a reference example for brick-first workflow design
