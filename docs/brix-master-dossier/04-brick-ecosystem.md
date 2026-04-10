# Brick Ecosystem

## Why This Matters
Brix is named after bricks. The platform should therefore behave like a node ecosystem, not like a pipeline collection with many embedded mini-programs.

Today the real step distribution shows that reuse is still often happening at the wrong abstraction level:

- many `script.python` steps
- many raw `mcp.call` steps
- recurring helper patterns
- pipeline-specific business logic embedded directly in step config or SQL

## Target Model

### Bricks are the Primary Reuse Unit
Reusable logic belongs in bricks.

### Pipelines are Composition and Policy
Pipelines should be short, declarative, and domain-specific orchestrators.

### Helpers are Transitional or Exceptional
Helpers remain valid only for:
- genuinely narrow domain logic
- temporary migration debt
- capabilities not yet stabilized enough for a brick

Every helper should carry an explicit answer to:
- why is this not a brick?
- when should it become one?

## Brick Classes

### Primitive Bricks
Low-level operations:
- file read/write
- db query
- db exec
- http request
- mcp call

### Structured Domain Bricks
Higher-level reusable capabilities:
- extract tax ids
- parse ICS birthdays
- persist document extraction result
- mark specialist processed

### Composite Bricks or Standardized Subpipelines
Reusable end-to-end patterns:
- fetch and store remote file
- load file and prepare extractable payload
- perform daigestr extraction with standard contracts

## Standard Contracts
Bricks should interoperate through standard domain contracts, not ad hoc object shapes. The canonical catalog is defined in code at `src/brix/bricks/contracts.py` and documented in [04a-standard-brick-contracts.md](/root/docker/brix/docs/brix-master-dossier/04a-standard-brick-contracts.md).

### Initial Contract Set
- `file_blob`
- `remote_download_result`
- `document_extract_input`
- `document_extraction_result`
- `db_query_result`
- `db_mutation_result`
- `classification_result`

## Current Ecosystem Smells

### Too Many Helper-Based Reuse Paths
This signals missing or weak bricks.

### Too Many Raw MCP Calls
This signals missing typed bricks above raw MCP operations.

### Too Much Pipeline-Embedded SQL and Template Logic
This signals that business operations are not yet first-class reusable building blocks.

## Brick Candidate Detection
Brix should actively detect candidates for new bricks.

Signal types:
- repeated SQL templates
- repeated helper invocations with similar params
- repeated step sequences across pipelines
- repeated conditional download/extract/persist chains
- repeated normalization logic before or after MCP calls

## Brick Lifecycle

### Proposed States
- `draft`
- `experimental`
- `stable`
- `deprecated`
- `replaced`

### Mandatory Brick Metadata
- purpose
- domain
- owner
- input contract
- output contract
- examples
- anti-patterns
- replacement recommendations

## Strategic Brick Themes

### Persistence Bricks
Examples:
- `document.persist_extraction_result`
- `document.mark_specialist_processed`
- `document.persist_remote_file`

### Source and Transfer Bricks
Examples:
- `source.onedrive_fetch_and_store`
- `source.gmail_fetch_message_batch`
- `source.outlook_fetch_attachment_set`

### Extraction Bricks
Examples:
- `extract.document_with_daigestr`
- `extract.document_prepare_payload`
- `extract.reference_bundle`

### Governance Bricks
Examples:
- `policy.require_component_metadata`
- `drift.scan_component`
- `reuse.find_candidates`

## Helper-to-Brick Migration Strategy

### Step 1
Inventory helpers and cluster by purpose.

### Step 2
Identify repeated helper families across projects.

### Step 3
Create stable contracts and promote high-value patterns into bricks.

### Step 4
Retag helpers as:
- `stable helper`
- `brick candidate`
- `legacy helper`

### Step 5
Add governance so new helpers require justification.

## HMK as Example
HMK currently shows exactly the migration direction Brix should take:
- keep orchestration in the top-level pipeline
- remove embedded persistence logic from the per-document pipeline
- introduce reusable bricks for persistence and specialist marking
- standardize the daigestr preparation and execution path
