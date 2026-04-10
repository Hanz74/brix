# Standard Brick Contracts

## Purpose
Standard contracts define stable payload names for brick inputs and outputs. They prevent pipelines from depending on implicit dict shapes such as “whatever the download step returned” and give validator, composer, and MCP tooling a shared language for compatibility.

## Canonical Source
The executable catalog lives in `src/brix/bricks/contracts.py`. Dossier prose explains the intent, but runtime behavior must be derived from the code catalog and the DB-backed type compatibility registry.

## Contract Catalog

### `file_blob`
Local file payload prepared for extraction or persistence. Required field: `file_bytes_path`. Common optional fields: `file_size`, `mime_type`, `content_hash`, `name`, `extension`, `base64`.

### `remote_download_result`
Result of downloading a remote object to a managed local file. Required field: `file_bytes_path`. It can feed `file_blob` and `document_extract_input` consumers.

### `document_extract_input`
Normalized payload passed into a document extraction brick. Required field: `file_bytes_path`. Optional enrichment includes `mime_type`, `base64`, `markdown`, `language`, and `metadata`.

### `document_extraction_result`
Structured extraction result with normalized fields and quality metadata. Required field: `normalized`. Persistence bricks should consume it through explicit mapped params, not through opaque SQL templates.

### `db_query_result`
Rows returned by a read-only database query. It is list-like and compatible with `list[dict]`, `list[object]`, and `list[*]` consumers.

### `db_mutation_result`
Database write result for `INSERT`, `UPDATE`, `DELETE`, or `UPSERT`. Required field: `affected_rows`. Optional fields may include returned rows and error details.

### `classification_result`
Classification output with at least `category`. Optional fields include `confidence`, `rationale`, `labels`, and metadata.

## Validator Integration
The validator treats object contracts as dict-like and `db_query_result` as list-like. This keeps existing checks such as foreach/list checks, `db.query` param checks, and `db.upsert` data checks aligned with domain contracts instead of generic container names only.

## Governance Rule
New reusable bricks should declare these contract names where they apply. A new ad hoc object shape is only acceptable when the task also proposes whether it should become a standard contract.
