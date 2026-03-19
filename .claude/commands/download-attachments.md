---
description: Download M365 email attachments via Brix pipeline
argument-hint: <search-query> [--folder Inbox] [--convert]
allowed-tools: Bash(brix:*)
---

# Download Attachments

Download email attachments from Microsoft 365 Outlook using the Brix pipeline engine.

## Arguments

Parse `$ARGUMENTS` for:
- First argument or `--search`: Mail search query (e.g. "invoices from last week")
- `--folder`: Mail folder (default: Inbox)
- `--convert`: If present, convert attachments via MarkItDown
- `--output` or `--dir`: Output directory (default: ./attachments)

## Execution

First, validate the pipeline:
```bash
brix validate pipelines/download-attachments.yaml
```

Then execute:
```bash
brix run pipelines/download-attachments.yaml \
  -p query="<parsed search query>" \
  -p folder="<parsed folder or Inbox>" \
  -p convert=<true if --convert, else false> \
  -p output_dir="<parsed output dir or ./attachments>"
```

## Result Presentation

Parse the JSON output and present to the user:
- **Summary**: "Downloaded X files (Y MB) to <output_dir>"
- **File list**: Table with filename, size, original name
- If any downloads failed: list which ones and why
- If convert was true: list converted files
- If the pipeline failed entirely: show the error and suggest fixes

## Error Handling

- If `brix validate` fails: show the validation error, suggest checking server registration
- If `brix run` fails: show step-by-step status, identify the failing step
- If credentials are missing: suggest `export BRIX_CRED_M365_TOKEN=<token>`
- If MCP server not registered: suggest `brix server add m365 --command ...`
