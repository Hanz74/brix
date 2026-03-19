---
description: Download M365 email attachments via Brix pipeline
argument-hint: <search-query> [--folder Inbox] [--broad] [--top N]
allowed-tools: Bash(brix:*)
---

# Download Attachments

Download email attachments from Microsoft 365 Outlook using Brix pipelines.

## Strategy Selection

Choose the optimal strategy based on the user's request:

### Targeted (default)
Use when: specific OData-compatible filter, few expected results, precise query.
Pipeline: `download-attachments.yaml`
- OData filter on server side (fast for selective queries)
- Example: "mails with subject containing 'Rechnung' that have attachments"

### Broad
Use when: common keywords, many expected results, or when OData filter would be complex.
Pipeline: `download-attachments-broad.yaml`
- Fetches many mails at once (200+), filters locally by keywords
- Faster when the filter term is common (avoids per-mail API calls for non-matches)
- Use `--broad` flag or when keywords are simple text searches

## Arguments

Parse `$ARGUMENTS` for:
- Search terms: the main query (e.g. "Rechnungen", "invoices from last week")
- `--broad`: force broad strategy
- `--top N`: max mails to fetch (default: 10 targeted, 200 broad)
- `--folder`: mail folder (default: Inbox)
- `--output` or `--dir`: output directory (default: ./attachments)

## Execution

### Targeted Strategy
```bash
brix run /app/pipelines/download-attachments.yaml \
  -p "query=hasAttachments eq true and contains(subject, '<search term>')" \
  -p top=<N> \
  -p output_dir=/host/root/<target-path>
```

### Broad Strategy
```bash
brix run /app/pipelines/download-attachments-broad.yaml \
  -p "keywords=<comma-separated keywords>" \
  -p top=200 \
  -p output_dir=/host/root/<target-path>
```

### Pagination (if needed)
If the user wants more results than one batch provides, run the pipeline multiple times.
Brix handles each run atomically — accumulate results across runs.

## Result Presentation

Parse the JSON output and present to the user:
- **Summary**: "Downloaded X PDF files (Y MB) to <dir>"
- **File list**: filename, size, original mail subject
- If any downloads failed: list which ones and why
- If fewer results than expected: suggest running with --broad or higher --top

## Path Convention

Host paths must use `/host/root/` prefix:
- User says `/root/dev/test/` → use `/host/root/dev/test/`
- User says `~/documents/` → use `/host/root/documents/`

## Error Handling

- If `brix validate` fails: show error, suggest checking server registration
- If credentials missing: suggest `export BRIX_CRED_M365_TOKEN=<token>`
- If MCP server not registered: suggest `brix server add m365 --command docker --args exec --args -i --args m365 --args ms-365-mcp-server`
