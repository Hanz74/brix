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
]
