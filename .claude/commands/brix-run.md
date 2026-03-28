---
description: Run any Brix pipeline
argument-hint: <pipeline-name> [params...]
allowed-tools: Bash(brix:*)
---

# Brix Pipeline Runner

Run a Brix pipeline by name or path.

## Arguments

Parse `$ARGUMENTS` for:
- First argument: pipeline name or path (e.g. "download-attachments" or "pipelines/custom.yaml")
- Remaining arguments: pipeline parameters as key=value pairs

## Execution

1. Resolve pipeline path:
   - If argument is a name: look in `pipelines/<name>.yaml`
   - If argument is a path: use directly

2. Validate first:
```bash
brix validate <pipeline-path>
```

3. Execute:
```bash
brix run <pipeline-path> <params as -p key=value>
```

## Result

Parse JSON output and present a clean summary to the user.
Show step-by-step status if there were errors.
