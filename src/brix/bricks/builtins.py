"""All built-in brick definitions for Brix v2."""

from brix.bricks.schema import BrickParam, BrickSchema

HTTP_GET = BrickSchema(
    name="http_get",
    type="http",
    description="Make an HTTP GET request to a URL and return the response.",
    when_to_use="Fetching data from REST APIs, downloading JSON, polling status endpoints.",
    category="http",
    config_schema={
        "url": BrickParam(type="string", description="The URL to request", required=True),
        "headers": BrickParam(type="object", description="HTTP headers as key-value pairs"),
        "timeout": BrickParam(type="string", description="Timeout duration (e.g. '30s')", default="60s"),
    },
    input_description="URL and optional headers",
    output_description="Response body (JSON parsed if possible, otherwise text)",
)

HTTP_POST = BrickSchema(
    name="http_post",
    type="http",
    description="Make an HTTP POST request with a JSON or text body.",
    when_to_use="Sending data to REST APIs, triggering webhooks, uploading content.",
    category="http",
    config_schema={
        "url": BrickParam(type="string", description="The URL to request", required=True),
        "headers": BrickParam(type="object", description="HTTP headers"),
        "body": BrickParam(type="object", description="Request body (sent as JSON)"),
        "timeout": BrickParam(type="string", description="Timeout duration", default="60s"),
    },
)

RUN_CLI = BrickSchema(
    name="run_cli",
    type="cli",
    description="Execute a shell command and return stdout. Always uses argument list (shell=False) for security.",
    when_to_use="Running system commands: ffmpeg, pandoc, docker exec, any CLI tool.",
    category="cli",
    config_schema={
        "args": BrickParam(
            type="array",
            description="Command and arguments as list (e.g. ['ffmpeg', '-i', 'input.mp4'])",
            required=True,
        ),
        "timeout": BrickParam(type="string", description="Timeout duration", default="60s"),
    },
    input_description="Command arguments as array (shell=False enforced)",
    output_description="stdout as string or parsed JSON",
)

PYTHON_SCRIPT = BrickSchema(
    name="python_script",
    type="python",
    description="Run a Python script file. Script reads JSON from argv[1] or stdin, writes JSON to stdout.",
    when_to_use="Data transformation, filtering, file processing — anything that needs Python logic.",
    category="python",
    config_schema={
        "script": BrickParam(type="string", description="Path to Python script file", required=True),
        "params": BrickParam(type="object", description="Parameters passed as JSON to the script"),
        "timeout": BrickParam(type="string", description="Timeout duration", default="60s"),
    },
)

FILE_READ = BrickSchema(
    name="file_read",
    type="file",
    description="Read a file and return its content.",
    when_to_use="Loading configuration files, reading input data, accessing local files.",
    category="file",
    config_schema={
        "path": BrickParam(type="string", description="File path to read", required=True),
        "encoding": BrickParam(type="string", description="Text encoding", default="utf-8"),
        "binary": BrickParam(type="boolean", description="Read as binary (base64 output)", default=False),
    },
)

FILE_WRITE = BrickSchema(
    name="file_write",
    type="file",
    description="Write content to a file.",
    when_to_use="Saving results, exporting data, creating output files.",
    category="file",
    config_schema={
        "path": BrickParam(type="string", description="File path to write", required=True),
        "content": BrickParam(type="string", description="Content to write", required=True),
        "encoding": BrickParam(type="string", description="Text encoding", default="utf-8"),
    },
)

MCP_CALL = BrickSchema(
    name="mcp_call",
    type="mcp",
    description="Call a tool on a registered MCP server via stdio protocol.",
    when_to_use="Interacting with M365, Docker, n8n, or any MCP-compatible service.",
    category="mcp",
    config_schema={
        "server": BrickParam(type="string", description="MCP server name from servers.yaml", required=True),
        "tool": BrickParam(type="string", description="Tool name to call", required=True),
        "params": BrickParam(type="object", description="Tool parameters"),
        "timeout": BrickParam(type="string", description="Timeout duration", default="60s"),
    },
)

FILTER = BrickSchema(
    name="filter",
    type="filter",
    description="Filter a list using a Jinja2 boolean expression. No Python script needed.",
    when_to_use="Filtering lists by condition: only PDFs, only items matching a keyword, removing empty entries.",
    category="transform",
    config_schema={
        "input": BrickParam(type="array", description="List to filter", required=True),
        "where": BrickParam(
            type="string",
            description="Jinja2 expression that evaluates to true/false per item (use 'item' variable)",
            required=True,
        ),
    },
    input_description="A list of items",
    output_description="Filtered list (only items where expression is true)",
)

TRANSFORM = BrickSchema(
    name="transform",
    type="transform",
    description="Transform data using a Jinja2 expression. Maps, renames, restructures.",
    when_to_use="Renaming fields, extracting nested values, reshaping data between steps.",
    category="transform",
    config_schema={
        "input": BrickParam(type="object", description="Data to transform"),
        "expression": BrickParam(type="string", description="Jinja2 expression for transformation", required=True),
    },
)

SUB_PIPELINE = BrickSchema(
    name="sub_pipeline",
    type="pipeline",
    description="Run another saved pipeline as a sub-step.",
    when_to_use="Composing complex workflows from reusable pipeline building blocks.",
    category="pipeline",
    config_schema={
        "pipeline": BrickParam(type="string", description="Pipeline name or path", required=True),
        "params": BrickParam(type="object", description="Input parameters for the sub-pipeline"),
    },
)

SPECIALIST = BrickSchema(
    name="specialist",
    type="specialist",
    description=(
        "Declarative data extraction: apply regex / json_path / split / template rules "
        "to extract fields from text or structured data, validate the result, and return "
        "it in dict / list / flat format. Replaces 600-line Python helper scripts with "
        "a compact YAML config block."
    ),
    when_to_use=(
        "Extracting structured fields from unstructured text (invoice numbers, IBANs, "
        "dates, amounts), reshaping nested JSON without a Python helper, or any "
        "declarative field extraction + validation pipeline step."
    ),
    category="transform",
    config_schema={
        "input_field": BrickParam(
            type="string",
            description="Dot-notation path into the pipeline context (default: 'text')",
            default="text",
        ),
        "extract": BrickParam(
            type="array",
            description=(
                "List of extraction rules. Each rule: "
                "{name, method, pattern?, template?, default?, group?, findall?}. "
                "method: regex | json_path | split | template"
            ),
            required=True,
        ),
        "checks": BrickParam(
            type="array",
            description=(
                "Optional validation rules. Each rule: "
                "{field, rule, value?, on_fail?}. "
                "rule: required | min_length | max_length | regex | type. "
                "on_fail: warn | skip | error"
            ),
        ),
        "output_format": BrickParam(
            type="string",
            description="Output format: dict (default), list, flat",
            default="dict",
            enum=["dict", "list", "flat"],
        ),
    },
    input_description=(
        "Any value reachable via the pipeline context using dot-notation "
        "(e.g. 'text', 'steps.fetch.data.body', 'input.raw')"
    ),
    output_description=(
        "{'result': extracted_data, 'warnings': [...], 'skipped': bool}"
    ),
)


# ---------------------------------------------------------------------------
# Atomic domain bricks — T-BRIX-V8-05
# ---------------------------------------------------------------------------

SOURCE_FETCH_EMAILS = BrickSchema(
    name="source.fetch_emails",
    type="mcp",
    description="Fetch emails from Outlook or Gmail via MCP.",
    when_to_use=(
        "When you need to read emails from a mailbox at the start of a pipeline. "
        "Use as the first step when the pipeline processes email content or attachments."
    ),
    when_NOT_to_use=(
        "When you already have the emails as input from a previous step. "
        "When you only need attachments from a known message ID (use mcp_call directly). "
        "When the pipeline receives emails via webhook/trigger."
    ),
    category="source",
    aliases=[
        "get emails", "read inbox", "fetch mail", "mails abrufen", "postfach lesen",
        "outlook mails", "gmail fetch", "e-mails holen", "posteingang", "inbox lesen",
        "read emails", "fetch emails", "mails lesen", "nachrichten abrufen",
    ],
    input_type="none",
    output_type="list[email]",
    config_schema={
        "provider": BrickParam(
            type="string",
            description="Mail provider to use",
            required=True,
            enum=["outlook", "gmail"],
        ),
        "folder": BrickParam(
            type="string",
            description="Folder name (e.g. 'inbox', 'Sent Items')",
            default="inbox",
        ),
        "filter": BrickParam(
            type="string",
            description="OData filter (Outlook) or IMAP search string (Gmail), e.g. 'isRead eq false'",
        ),
        "limit": BrickParam(
            type="integer",
            description="Maximum number of emails to fetch",
            default=50,
        ),
    },
    input_description="No input required — configured via provider/folder/filter/limit",
    output_description="List of email objects with id, subject, from, to, body, receivedDateTime, hasAttachments",
    examples=[
        {
            "goal": "Fetch unread emails from Outlook inbox",
            "config": {"provider": "outlook", "filter": "isRead eq false", "limit": 100},
        },
        {
            "goal": "Fetch all emails from Gmail for the last week",
            "config": {"provider": "gmail", "filter": "SINCE 7d", "limit": 200},
        },
    ],
    related_connector="outlook",
)

SOURCE_FETCH_FILES = BrickSchema(
    name="source.fetch_files",
    type="mcp",
    description="Fetch files from OneDrive or a local directory, with optional glob-pattern filtering.",
    when_to_use=(
        "When you need a list of files to process (PDFs, DOCX, images) from cloud storage or disk. "
        "Use as the first step in document-processing pipelines."
    ),
    when_NOT_to_use=(
        "When you already have file paths as input from a previous step. "
        "When you need to read file *content* directly (use file_read for that). "
        "When the source is an email attachment (use source.fetch_emails + attachment download instead)."
    ),
    category="source",
    aliases=[
        "list files", "scan folder", "dateien abrufen", "dateien holen", "dateien auflisten",
        "onedrive files", "local files", "ordner scannen", "verzeichnis lesen",
        "get files", "fetch files", "find files", "dateien finden", "dateiliste",
    ],
    input_type="none",
    output_type="list[file_ref]",
    config_schema={
        "provider": BrickParam(
            type="string",
            description="Storage provider to use",
            required=True,
            enum=["onedrive", "local"],
        ),
        "path": BrickParam(
            type="string",
            description="Root path or OneDrive folder path to scan",
            required=True,
        ),
        "pattern": BrickParam(
            type="string",
            description="Glob pattern to filter files, e.g. '*.pdf' or '**/*.docx'",
            default="*",
        ),
        "recursive": BrickParam(
            type="boolean",
            description="Whether to scan subdirectories recursively",
            default=False,
        ),
    },
    input_description="No input required — configured via provider/path/pattern/recursive",
    output_description="List of file references with name, path, size, modified_at",
    examples=[
        {
            "goal": "List all PDFs in OneDrive /Dokumente folder",
            "config": {"provider": "onedrive", "path": "/Dokumente", "pattern": "*.pdf", "recursive": True},
        },
        {
            "goal": "Scan local /host/root/dev/input for all files",
            "config": {"provider": "local", "path": "/host/root/dev/input", "pattern": "*"},
        },
    ],
    related_connector="onedrive",
)

SOURCE_HTTP_FETCH = BrickSchema(
    name="source.http_fetch",
    type="http",
    description="Make an HTTP GET or POST request to fetch data from an external API or webhook.",
    when_to_use=(
        "When the data source is a REST API, web service, or webhook endpoint. "
        "Suitable for JSON APIs, paginated endpoints, and status checks."
    ),
    when_NOT_to_use=(
        "When the data source is a registered MCP server (use mcp_call instead — more reliable, typed). "
        "When you need to download binary files like PDFs (use file_read or a dedicated download step). "
        "When the API requires OAuth2 flows that are already wrapped in an MCP connector."
    ),
    category="source",
    aliases=[
        "http get", "api call", "rest api", "fetch url", "http request", "api anfrage",
        "webhook", "json api", "endpoint", "http fetch", "url abrufen", "api abrufen",
    ],
    input_type="none",
    output_type="object|list",
    config_schema={
        "url": BrickParam(
            type="string",
            description="The URL to request",
            required=True,
        ),
        "method": BrickParam(
            type="string",
            description="HTTP method",
            default="GET",
            enum=["GET", "POST", "PUT", "PATCH", "DELETE"],
        ),
        "headers": BrickParam(
            type="object",
            description="HTTP headers as key-value pairs",
        ),
        "body": BrickParam(
            type="object",
            description="Request body (sent as JSON) — only used for POST/PUT/PATCH",
        ),
        "timeout": BrickParam(
            type="string",
            description="Timeout duration (e.g. '30s', '2m')",
            default="60s",
        ),
    },
    input_description="URL, method, optional headers and body",
    output_description="Response body parsed as JSON if possible, otherwise raw text string",
    examples=[
        {
            "goal": "Fetch JSON data from a public API",
            "config": {"url": "https://api.example.com/items", "method": "GET"},
        },
        {
            "goal": "Post data to a webhook",
            "config": {"url": "https://hooks.example.com/trigger", "method": "POST", "body": {"event": "new_document"}},
        },
    ],
)

CONVERT_TO_MARKDOWN = BrickSchema(
    name="convert.to_markdown",
    type="http",
    description=(
        "Convert any document (PDF, DOCX, XLSX, HTML, image) to clean Markdown text "
        "via the markitdown service."
    ),
    when_to_use=(
        "When you need to extract readable text from a document before LLM processing or classification. "
        "Works best for PDF invoices, Word documents, Excel sheets, and scanned pages."
    ),
    when_NOT_to_use=(
        "When the file is already plain text or Markdown. "
        "When you need structured table data from Excel (use convert.to_json instead). "
        "When the document is an image and you need OCR with spatial layout (use convert.extract_text with method=ocr)."
    ),
    category="convert",
    aliases=[
        "pdf to markdown", "docx to markdown", "to markdown", "dokument konvertieren",
        "markitdown", "pdf text", "dokument zu text", "convert document", "pdf lesen",
        "excel to markdown", "word to markdown", "html to markdown", "zu markdown",
    ],
    input_type="file_path",
    output_type="string (markdown)",
    config_schema={
        "input_path": BrickParam(
            type="string",
            description="Absolute path to the file to convert (inside the Brix container, e.g. /host/root/...)",
            required=True,
        ),
    },
    input_description="Absolute path to the source document (PDF, DOCX, XLSX, HTML, image)",
    output_description="Document content as Markdown string",
    examples=[
        {
            "goal": "Convert a PDF invoice to Markdown for LLM extraction",
            "config": {"input_path": "/host/root/dev/input/rechnung.pdf"},
        },
    ],
    related_connector="markitdown",
)

CONVERT_TO_JSON = BrickSchema(
    name="convert.to_json",
    type="python",
    description="Parse structured data files (CSV, XML, YAML) and return them as a JSON object or list.",
    when_to_use=(
        "When you need to load structured data from a file format into the pipeline context. "
        "Ideal for CSV exports, config files, and XML data feeds."
    ),
    when_NOT_to_use=(
        "When the input is already JSON — just use file_read. "
        "When the input is a PDF or Word document (use convert.to_markdown first). "
        "When you need only selected columns from a large CSV (write a Python helper instead)."
    ),
    category="convert",
    aliases=[
        "csv to json", "xml to json", "yaml to json", "parse csv", "csv parsen",
        "structured data", "csv lesen", "xml parsen", "config laden", "datei parsen",
        "convert csv", "convert xml", "convert yaml", "strukturierte daten",
    ],
    input_type="file_path",
    output_type="list|object",
    config_schema={
        "input_path": BrickParam(
            type="string",
            description="Absolute path to the input file",
            required=True,
        ),
        "input_format": BrickParam(
            type="string",
            description="Source file format",
            required=True,
            enum=["csv", "xml", "yaml"],
        ),
        "delimiter": BrickParam(
            type="string",
            description="Field delimiter for CSV files",
            default=",",
        ),
        "encoding": BrickParam(
            type="string",
            description="File encoding",
            default="utf-8",
        ),
    },
    input_description="Absolute path to CSV/XML/YAML file",
    output_description="Parsed data as a list of dicts (CSV/XML) or a dict (YAML)",
    examples=[
        {
            "goal": "Parse a CSV export into a list of records",
            "config": {"input_path": "/host/root/dev/export.csv", "input_format": "csv"},
        },
        {
            "goal": "Load a YAML config file",
            "config": {"input_path": "/host/root/dev/config.yaml", "input_format": "yaml"},
        },
    ],
)

CONVERT_EXTRACT_TEXT = BrickSchema(
    name="convert.extract_text",
    type="python",
    description="Extract plain text from PDF, DOCX, or image files using native parsing or OCR.",
    when_to_use=(
        "When you need raw text from documents, especially scanned PDFs or images where "
        "native text extraction is unavailable. Choose method=ocr for scans."
    ),
    when_NOT_to_use=(
        "When you need structured Markdown with headers and tables (use convert.to_markdown). "
        "When the file is already plain text. "
        "When you need to process Excel sheets (use convert.to_json with input_format=csv after export)."
    ),
    category="convert",
    aliases=[
        "extract text", "text extrahieren", "ocr", "pdf text extrahieren", "text aus pdf",
        "text aus bild", "image ocr", "bild zu text", "docx text", "text lesen",
        "plain text", "raw text", "texterkennung",
    ],
    input_type="file_path",
    output_type="string",
    config_schema={
        "input_path": BrickParam(
            type="string",
            description="Absolute path to the input file (PDF, DOCX, PNG, JPG, TIFF)",
            required=True,
        ),
        "method": BrickParam(
            type="string",
            description="Extraction method: 'native' for text-based PDFs/DOCX, 'ocr' for scans/images",
            default="native",
            enum=["native", "ocr"],
        ),
        "language": BrickParam(
            type="string",
            description="OCR language hint (ISO 639-1 code, e.g. 'de', 'en') — only used when method=ocr",
            default="de",
        ),
    },
    input_description="Absolute path to PDF, DOCX, or image file",
    output_description="Extracted plain text string",
    examples=[
        {
            "goal": "Extract text from a native PDF",
            "config": {"input_path": "/host/root/dev/doc.pdf", "method": "native"},
        },
        {
            "goal": "OCR a scanned invoice image",
            "config": {"input_path": "/host/root/dev/scan.png", "method": "ocr", "language": "de"},
        },
    ],
)

LLM_EXTRACT = BrickSchema(
    name="llm.extract",
    type="python",
    description=(
        "Use an LLM to extract structured fields from unstructured text based on a prompt template "
        "and an output schema."
    ),
    when_to_use=(
        "When you need to pull specific fields (amounts, dates, names, IBANs) from free-form text "
        "and regex/json_path cannot reliably handle the variation. "
        "Ideal after convert.to_markdown or convert.extract_text."
    ),
    when_NOT_to_use=(
        "When the data is already structured JSON — use transform or filter instead. "
        "When a simple regex pattern is sufficient (use the specialist brick). "
        "When you need binary classification — use llm.classify instead. "
        "When the text is too long for a single LLM context window without chunking."
    ),
    category="llm",
    aliases=[
        "llm extraction", "ki extraktion", "daten extrahieren", "felder extrahieren",
        "structured extraction", "field extraction", "extract fields", "llm parsen",
        "ki auslesen", "gpt extract", "claude extract", "daten auslesen",
    ],
    input_type="string (text)",
    output_type="object (extracted fields)",
    config_schema={
        "prompt_template": BrickParam(
            type="string",
            description=(
                "Jinja2 prompt template. Use {{ input.text }} to reference the input text. "
                "Should instruct the LLM to return JSON."
            ),
            required=True,
        ),
        "output_schema": BrickParam(
            type="object",
            description="JSON Schema describing the expected output fields",
            required=True,
        ),
        "model": BrickParam(
            type="string",
            description="LLM model to use",
            default="claude-3-5-haiku-latest",
            enum=["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest", "claude-opus-4-5", "gpt-4o-mini", "gpt-4o"],
        ),
        "temperature": BrickParam(
            type="string",
            description="LLM temperature (0.0 = deterministic, 1.0 = creative)",
            default="0.0",
        ),
    },
    input_description="Text string to extract from (passed as input.text in the template)",
    output_description="Dict of extracted fields matching the output_schema",
    examples=[
        {
            "goal": "Extract invoice number, date, and total amount from invoice text",
            "config": {
                "prompt_template": "Extract the invoice number, date, and total from this invoice:\n\n{{ input.text }}\n\nReturn JSON.",
                "output_schema": {"type": "object", "properties": {"invoice_number": {"type": "string"}, "date": {"type": "string"}, "total": {"type": "number"}}},
                "model": "claude-3-5-haiku-latest",
            },
        },
    ],
)

LLM_CLASSIFY = BrickSchema(
    name="llm.classify",
    type="python",
    description=(
        "Use an LLM to classify text or a document into one of several predefined categories."
    ),
    when_to_use=(
        "When you need to sort items into categories based on content semantics: "
        "email routing, document tagging, intent detection. "
        "Best for multi-class classification where a simple keyword match is insufficient."
    ),
    when_NOT_to_use=(
        "When the categories can be determined by regex or keyword matching (use filter or specialist). "
        "When you need to extract multiple fields — use llm.extract instead. "
        "When the classification needs a confidence score — add confidence to the output_schema of llm.extract. "
        "When you have a training dataset — consider a fine-tuned classifier instead."
    ),
    category="llm",
    aliases=[
        "classify", "kategorisieren", "klassifizieren", "einordnen", "document classification",
        "email classification", "intent detection", "tagging", "kategorien", "klasse bestimmen",
        "llm classify", "ki klassifizierung", "sort emails", "dokument einordnen",
    ],
    input_type="string (text)",
    output_type="object (category + rationale)",
    config_schema={
        "categories": BrickParam(
            type="array",
            description="List of possible category names (e.g. ['invoice', 'contract', 'newsletter'])",
            required=True,
        ),
        "prompt_template": BrickParam(
            type="string",
            description=(
                "Jinja2 prompt template. Use {{ input.text }} for the text and "
                "{{ config.categories | join(', ') }} for categories. "
                "Should instruct the LLM to return JSON with 'category' and 'rationale' fields."
            ),
        ),
        "model": BrickParam(
            type="string",
            description="LLM model to use",
            default="claude-3-5-haiku-latest",
            enum=["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest", "claude-opus-4-5", "gpt-4o-mini", "gpt-4o"],
        ),
    },
    input_description="Text to classify (passed as input.text)",
    output_description="Dict with 'category' (string from categories list) and 'rationale' (explanation)",
    examples=[
        {
            "goal": "Classify an email as invoice, contract, or newsletter",
            "config": {
                "categories": ["invoice", "contract", "newsletter", "other"],
                "model": "claude-3-5-haiku-latest",
            },
        },
    ],
)

DB_INGEST = BrickSchema(
    name="db.ingest",
    type="python",
    description="Write one or more records into a database table, with optional upsert (insert-or-update).",
    when_to_use=(
        "When you need to persist structured data into SQLite or PostgreSQL at the end of a pipeline. "
        "Use upsert_key to avoid duplicates when re-running the same pipeline."
    ),
    when_NOT_to_use=(
        "When you only need to write to a file (use file_write instead). "
        "When the data needs complex transformation before insert (run a transform step first). "
        "When writing to external SaaS databases via API — use mcp_call for that."
    ),
    category="db",
    aliases=[
        "datenbank schreiben", "db insert", "db upsert", "in datenbank speichern",
        "datensatz speichern", "record speichern", "insert", "upsert", "ingest",
        "daten persistieren", "daten speichern", "tabelle befüllen", "write to db",
    ],
    input_type="list[object] | object",
    output_type="object (insert_count, upsert_count, errors)",
    config_schema={
        "table": BrickParam(
            type="string",
            description="Target table name",
            required=True,
        ),
        "columns": BrickParam(
            type="array",
            description="List of column names to write. If omitted, all keys of the input records are used.",
        ),
        "upsert_key": BrickParam(
            type="string",
            description="Column name to use as upsert key. If a row with this key value exists, it is updated.",
        ),
        "connection": BrickParam(
            type="string",
            description="Database connection string or credential name (e.g. 'sqlite:///data.db' or 'POSTGRES_URL')",
            default="sqlite:///brix_data.db",
        ),
    },
    input_description="A single record dict or a list of record dicts to write",
    output_description="{'insert_count': N, 'upsert_count': N, 'error_count': N, 'errors': [...]}",
    examples=[
        {
            "goal": "Insert extracted invoice data into the invoices table",
            "config": {"table": "invoices", "upsert_key": "invoice_number", "connection": "sqlite:///buddy.db"},
        },
    ],
)

DB_QUERY = BrickSchema(
    name="db.query",
    type="python",
    runner="db_query",
    system=True,
    namespace="db",
    description="Execute a SQL query against a database and return the results as a list of dicts.",
    when_to_use=(
        "When you need to read existing data from a database for enrichment, deduplication checks, "
        "or reporting within a pipeline."
    ),
    when_NOT_to_use=(
        "When you need to write data (use db.ingest instead). "
        "When the data is in a file (use file_read or convert.to_json). "
        "When you need a single lookup by ID — consider passing the value via pipeline params instead."
    ),
    category="db",
    aliases=[
        "datenbank abfragen", "db query", "sql query", "sql abfrage", "datenbankabfrage",
        "daten lesen", "tabelle abfragen", "select", "datensätze abrufen", "query db",
        "read from db", "datenbank lesen", "aus datenbank lesen",
    ],
    input_type="none",
    output_type="list[object]",
    config_schema={
        "query": BrickParam(
            type="string",
            description="SQL SELECT query to execute. Use :param_name for named parameters.",
            required=True,
        ),
        "params": BrickParam(
            type="object",
            description="Named parameters for the query (e.g. {'status': 'open'})",
        ),
        "connection": BrickParam(
            type="string",
            description="Database connection string or credential name",
            default="sqlite:///brix_data.db",
        ),
    },
    input_description="No pipeline input required — query and params are fully configured",
    output_description="List of row dicts matching the query result",
    examples=[
        {
            "goal": "Find all unprocessed invoices",
            "config": {
                "query": "SELECT * FROM invoices WHERE processed = :done",
                "params": {"done": False},
                "connection": "sqlite:///buddy.db",
            },
        },
    ],
)

ACTION_NOTIFY = BrickSchema(
    name="action.notify",
    type="mcp",
    runner="notify",
    system=True,
    namespace="action",
    description=(
        "Send a notification via Mattermost, email, or log output at the end of a pipeline step."
    ),
    when_to_use=(
        "When you need to alert a user or channel about pipeline results, errors, or completion. "
        "Use at the end of pipelines or in error handlers."
    ),
    when_NOT_to_use=(
        "When you need to send a full formatted email with attachments (use mcp_call with the M365 server). "
        "When the target channel does not exist yet (create it first). "
        "When you only need debug output — use log channel and the pipeline's built-in step logging instead."
    ),
    category="action",
    aliases=[
        "benachrichtigung", "notify", "notification", "alert", "mattermost", "senden",
        "nachricht senden", "message senden", "kanal benachrichtigen", "send notification",
        "send message", "benachrichtigen", "meldung", "ping",
    ],
    input_type="none",
    output_type="object (sent_at, channel, status)",
    config_schema={
        "channel": BrickParam(
            type="string",
            description="Notification channel to use",
            default="log",
            enum=["mattermost", "email", "log"],
        ),
        "message_template": BrickParam(
            type="string",
            description=(
                "Jinja2 message template. Use {{ steps.<id>.result }} to reference step results. "
                "Markdown is supported for mattermost."
            ),
            required=True,
        ),
        "target": BrickParam(
            type="string",
            description=(
                "For mattermost: channel name or user handle (e.g. '@hans' or '#general'). "
                "For email: recipient email address."
            ),
        ),
    },
    input_description="No pipeline input required — message is rendered from the template",
    output_description="{'sent_at': ISO timestamp, 'channel': used channel, 'status': 'ok'|'error'}",
    examples=[
        {
            "goal": "Notify Mattermost channel when pipeline completes",
            "config": {
                "channel": "mattermost",
                "target": "#buddy-notifications",
                "message_template": "Pipeline finished: {{ steps.ingest.result.insert_count }} records imported.",
            },
        },
        {
            "goal": "Log completion to pipeline output",
            "config": {
                "channel": "log",
                "message_template": "Done. Processed {{ steps.fetch.result | length }} emails.",
            },
        },
    ],
    related_connector="mattermost",
)

ACTION_MOVE_FILE = BrickSchema(
    name="action.move_file",
    type="cli",
    description="Move or copy a file to a new location on the local filesystem or OneDrive.",
    when_to_use=(
        "When you need to archive processed files, organise output into folders, or stage files "
        "for downstream processing. Use operation=move to remove the source after copying."
    ),
    when_NOT_to_use=(
        "When the source is a cloud file that needs a download first (use source.fetch_files + file_read). "
        "When you need to rename a OneDrive file without downloading it — use mcp_call with the M365 server. "
        "When bulk moving thousands of files — write a Python helper with shutil.copytree instead."
    ),
    category="action",
    aliases=[
        "datei verschieben", "datei kopieren", "move file", "copy file", "archivieren",
        "file move", "file copy", "verschieben", "kopieren", "umbenennen",
        "rename file", "datei umbenennen", "datei archivieren", "in ordner verschieben",
    ],
    input_type="none",
    output_type="object (source, destination, operation, success)",
    config_schema={
        "source": BrickParam(
            type="string",
            description="Absolute source file path (inside Brix container, e.g. /host/root/...)",
            required=True,
        ),
        "destination": BrickParam(
            type="string",
            description="Absolute destination path (file or directory). If a directory, the filename is preserved.",
            required=True,
        ),
        "operation": BrickParam(
            type="string",
            description="'move' removes the source after copying; 'copy' keeps the source",
            default="move",
            enum=["move", "copy"],
        ),
        "overwrite": BrickParam(
            type="boolean",
            description="Whether to overwrite the destination if it already exists",
            default=False,
        ),
    },
    input_description="No pipeline input required — source and destination are fully configured",
    output_description="{'source': path, 'destination': final_path, 'operation': 'move'|'copy', 'success': bool}",
    examples=[
        {
            "goal": "Archive a processed PDF to the done/ folder",
            "config": {
                "source": "/host/root/dev/input/rechnung.pdf",
                "destination": "/host/root/dev/done/",
                "operation": "move",
            },
        },
        {
            "goal": "Copy a report to a backup location",
            "config": {
                "source": "/host/root/dev/output/report.json",
                "destination": "/host/root/backup/report.json",
                "operation": "copy",
                "overwrite": True,
            },
        },
    ],
)


# ---------------------------------------------------------------------------
# System Bricks — T-BRIX-DB-05c
# One system brick per runner. These are the Brick-First Engine bridge between
# the dot-notation brick names (db.query) and the runner (db_query).
# system=True means these bricks cannot be deleted via the registry.
# ---------------------------------------------------------------------------

SYSTEM_SCRIPT_PYTHON = BrickSchema(
    name="script.python",
    type="python",
    description="Run a Python script (Brick-First canonical name for the python runner).",
    when_to_use="Use when you need Python logic, data processing, or a custom script.",
    runner="python",
    system=True,
    namespace="script",
    category="system",
)

SYSTEM_HTTP_REQUEST = BrickSchema(
    name="http.request",
    type="http",
    description="Make an HTTP request (Brick-First canonical name for the http runner).",
    when_to_use="Use when you need to call a REST API or HTTP endpoint.",
    runner="http",
    system=True,
    namespace="http",
    category="system",
)

SYSTEM_MCP_CALL = BrickSchema(
    name="mcp.call",
    type="mcp",
    description="Call a tool on a registered MCP server (Brick-First canonical name for the mcp runner).",
    when_to_use="Use when you need to interact with an MCP-compatible service.",
    runner="mcp",
    system=True,
    namespace="mcp",
    category="system",
)

SYSTEM_SCRIPT_CLI = BrickSchema(
    name="script.cli",
    type="cli",
    description="Execute a CLI command (Brick-First canonical name for the cli runner).",
    when_to_use="Use when you need to run a shell command or CLI tool.",
    runner="cli",
    system=True,
    namespace="script",
    category="system",
)

SYSTEM_FLOW_FILTER = BrickSchema(
    name="flow.filter",
    type="filter",
    description="Filter a list using a Jinja2 expression (Brick-First canonical name for the filter runner).",
    when_to_use="Use to filter items in a foreach or list step.",
    runner="filter",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_TRANSFORM = BrickSchema(
    name="flow.transform",
    type="transform",
    description="Transform data using a Jinja2 expression (Brick-First canonical name for the transform runner).",
    when_to_use="Use to reshape or remap data between pipeline steps.",
    runner="transform",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_SET = BrickSchema(
    name="flow.set",
    type="set",
    description="Set context variables (Brick-First canonical name for the set runner).",
    when_to_use="Use to assign computed or static values to context variables.",
    runner="set",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_REPEAT = BrickSchema(
    name="flow.repeat",
    type="repeat",
    description="Repeat a sequence of steps until a condition is met (Brick-First canonical name for the repeat runner).",
    when_to_use="Use for polling loops or iteration until convergence.",
    runner="repeat",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_CHOOSE = BrickSchema(
    name="flow.choose",
    type="choose",
    description="Conditional branching (Brick-First canonical name for the choose runner).",
    when_to_use="Use when different step sequences should run depending on a condition.",
    runner="choose",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_PARALLEL = BrickSchema(
    name="flow.parallel",
    type="parallel",
    description="Run sub-steps in parallel (Brick-First canonical name for the parallel runner).",
    when_to_use="Use to execute independent steps concurrently.",
    runner="parallel",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_PIPELINE = BrickSchema(
    name="flow.pipeline",
    type="pipeline",
    description="Run a sub-pipeline (Brick-First canonical name for the pipeline runner).",
    when_to_use="Use to compose complex workflows from reusable pipeline building blocks.",
    runner="pipeline",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_PIPELINE_GROUP = BrickSchema(
    name="flow.pipeline_group",
    type="pipeline_group",
    description="Run multiple sub-pipelines in parallel (Brick-First canonical name for the pipeline_group runner).",
    when_to_use="Use to fan-out across multiple pipelines and collect their results.",
    runner="pipeline_group",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_VALIDATE = BrickSchema(
    name="flow.validate",
    type="validate",
    description="Validate data quality rules (Brick-First canonical name for the validate runner).",
    when_to_use="Use to assert data integrity before proceeding.",
    runner="validate",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_ACTION_APPROVAL = BrickSchema(
    name="action.approval",
    type="approval",
    description="Request human approval before continuing (Brick-First canonical name for the approval runner).",
    when_to_use="Use when a pipeline needs a human decision before proceeding.",
    runner="approval",
    system=True,
    namespace="action",
    category="system",
)

SYSTEM_EXTRACT_SPECIALIST = BrickSchema(
    name="extract.specialist",
    type="specialist",
    description="Declarative data extraction using rules (Brick-First canonical name for the specialist runner).",
    when_to_use="Use to extract structured fields from unstructured text without Python scripts.",
    runner="specialist",
    system=True,
    namespace="extract",
    category="system",
)

SYSTEM_DB_UPSERT = BrickSchema(
    name="db.upsert",
    type="db_upsert",
    description="Upsert records into the Brix database (Brick-First canonical name for the db_upsert runner).",
    when_to_use="Use to insert or update records in the application database.",
    runner="db_upsert",
    system=True,
    namespace="db",
    category="system",
)

SYSTEM_LLM_BATCH = BrickSchema(
    name="llm.batch",
    type="llm_batch",
    description="Run LLM inference in batch mode (Brick-First canonical name for the llm_batch runner).",
    when_to_use="Use to classify, extract or summarize many items using an LLM in a single batch.",
    runner="llm_batch",
    system=True,
    namespace="llm",
    category="system",
)

SYSTEM_MARKITDOWN_CONVERT = BrickSchema(
    name="markitdown.convert",
    type="markitdown",
    description="Convert documents to Markdown via markitdown (Brick-First canonical name for the markitdown runner).",
    when_to_use="Use to convert PDF, DOCX, XLSX, HTML, or images to Markdown text.",
    runner="markitdown",
    system=True,
    namespace="markitdown",
    category="system",
)

SYSTEM_SOURCE_FETCH = BrickSchema(
    name="source.fetch",
    type="source",
    description="Fetch data from a source connector (Brick-First canonical name for the source runner).",
    when_to_use="Use as the first step in a pipeline to fetch emails, files, or other source data.",
    runner="source",
    system=True,
    namespace="source",
    category="system",
)

SYSTEM_FLOW_SWITCH = BrickSchema(
    name="flow.switch",
    type="switch",
    description="Multi-branch switch/case control flow (Brick-First canonical name for the switch runner).",
    when_to_use="Use when you need to route to one of several branches based on a value.",
    runner="switch",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_MERGE = BrickSchema(
    name="flow.merge",
    type="merge",
    description="Merge multiple step outputs into one (Brick-First canonical name for the merge runner).",
    when_to_use="Use after parallel branches to consolidate their results.",
    runner="merge",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_ERROR_HANDLER = BrickSchema(
    name="flow.error_handler",
    type="error_handler",
    description="Handle errors from previous steps (Brick-First canonical name for the error_handler runner).",
    when_to_use="Use to catch and handle errors in a pipeline gracefully.",
    runner="error_handler",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_WAIT = BrickSchema(
    name="flow.wait",
    type="wait",
    description="Wait for a specified duration or condition (Brick-First canonical name for the wait runner).",
    when_to_use="Use to add delays or wait for external events between pipeline steps.",
    runner="wait",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_DEDUP = BrickSchema(
    name="flow.dedup",
    type="dedup",
    description="Deduplicate a list of items (Brick-First canonical name for the dedup runner).",
    when_to_use="Use to remove duplicate items from a list based on a key.",
    runner="dedup",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_AGGREGATE = BrickSchema(
    name="flow.aggregate",
    type="aggregate",
    description="Aggregate items by group and compute statistics (Brick-First canonical name for the aggregate runner).",
    when_to_use="Use to group and summarize lists of items.",
    runner="aggregate",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_FLATTEN = BrickSchema(
    name="flow.flatten",
    type="flatten",
    description="Flatten a list of lists into a single list (Brick-First canonical name for the flatten runner).",
    when_to_use="Use after foreach or parallel steps that produce nested lists.",
    runner="flatten",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_FLOW_DIFF = BrickSchema(
    name="flow.diff",
    type="diff",
    description="Compute the difference between two lists or datasets (Brick-First canonical name for the diff runner).",
    when_to_use="Use to find new, changed, or removed items between two snapshots.",
    runner="diff",
    system=True,
    namespace="flow",
    category="system",
)

SYSTEM_ACTION_RESPOND = BrickSchema(
    name="action.respond",
    type="respond",
    description="Send a response back to a caller or user (Brick-First canonical name for the respond runner).",
    when_to_use="Use as the final step in a pipeline to return a structured response.",
    runner="respond",
    system=True,
    namespace="action",
    category="system",
)

# All system bricks (one per runner)
SYSTEM_BRICKS: list[BrickSchema] = [
    SYSTEM_SCRIPT_PYTHON,
    SYSTEM_HTTP_REQUEST,
    SYSTEM_MCP_CALL,
    SYSTEM_SCRIPT_CLI,
    SYSTEM_FLOW_FILTER,
    SYSTEM_FLOW_TRANSFORM,
    SYSTEM_FLOW_SET,
    SYSTEM_FLOW_REPEAT,
    SYSTEM_FLOW_CHOOSE,
    SYSTEM_FLOW_PARALLEL,
    SYSTEM_FLOW_PIPELINE,
    SYSTEM_FLOW_PIPELINE_GROUP,
    SYSTEM_FLOW_VALIDATE,
    SYSTEM_ACTION_APPROVAL,
    SYSTEM_EXTRACT_SPECIALIST,
    SYSTEM_DB_UPSERT,
    SYSTEM_LLM_BATCH,
    SYSTEM_MARKITDOWN_CONVERT,
    SYSTEM_SOURCE_FETCH,
    SYSTEM_FLOW_SWITCH,
    SYSTEM_FLOW_MERGE,
    SYSTEM_FLOW_ERROR_HANDLER,
    SYSTEM_FLOW_WAIT,
    SYSTEM_FLOW_DEDUP,
    SYSTEM_FLOW_AGGREGATE,
    SYSTEM_FLOW_FLATTEN,
    SYSTEM_FLOW_DIFF,
    SYSTEM_ACTION_RESPOND,
]


# All built-in bricks
ALL_BUILTINS: list[BrickSchema] = [
    HTTP_GET,
    HTTP_POST,
    RUN_CLI,
    PYTHON_SCRIPT,
    FILE_READ,
    FILE_WRITE,
    MCP_CALL,
    FILTER,
    TRANSFORM,
    SUB_PIPELINE,
    SPECIALIST,
    # T-BRIX-V8-05: 12 atomic domain bricks
    SOURCE_FETCH_EMAILS,
    SOURCE_FETCH_FILES,
    SOURCE_HTTP_FETCH,
    CONVERT_TO_MARKDOWN,
    CONVERT_TO_JSON,
    CONVERT_EXTRACT_TEXT,
    LLM_EXTRACT,
    LLM_CLASSIFY,
    DB_INGEST,
    DB_QUERY,
    ACTION_NOTIFY,
    ACTION_MOVE_FILE,
    # T-BRIX-DB-05c: System bricks (one per runner, Brick-First Engine)
    *SYSTEM_BRICKS,
]
